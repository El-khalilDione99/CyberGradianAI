"""
engine/anomaly/train.py
────────────────────────
Entraînement de l'Isolation Forest (IA-5) et versionnage dans MinIO/S3.

Appel typique :
    from engine.anomaly.train import train
    from engine.anomaly.dataset import build_dataset

    dataset = build_dataset(events=liste_events)
    result  = train(dataset)
    print(result.model_key)   # "anomaly/isolation_forest_20260801.pkl"

Ce module ne connaît pas l'API FastAPI ni Kafka.
Il reçoit un DatasetResult, entraîne, sauvegarde, retourne un TrainResult.

Versionnage :
    cg-models/
      anomaly/
        isolation_forest_<YYYYMMDD>.pkl   ← modèle sérialisé
        isolation_forest_<YYYYMMDD>_metrics.json
        production/
          current.json                    ← pointe vers la version active
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import RobustScaler

from engine.anomaly.dataset import DatasetResult, FEATURE_NAMES
from interfaces.store import get_object_store, BUCKET_MODELS

logger = logging.getLogger(__name__)

# ── Paramètres Isolation Forest ───────────────────────────────
# contamination : proportion de fraudes estimée dans le trafic global.
# Avec 20% de comptes frauduleux mais des scénarios rares, on estime
# ~1-3% de transactions frauduleuses dans le flux total.
IF_N_ESTIMATORS  = int(os.getenv("IF_N_ESTIMATORS",  "200"))
IF_CONTAMINATION = float(os.getenv("IF_CONTAMINATION", "0.02"))
IF_MAX_SAMPLES   = os.getenv("IF_MAX_SAMPLES", "auto")
SEED             = int(os.getenv("SEED", "42"))

MODEL_PREFIX = "anomaly/isolation_forest"
PRODUCTION_KEY = "anomaly/production/current.json"


# ════════════════════════════════════════════════════════════
#  Résultat d'entraînement
# ════════════════════════════════════════════════════════════

@dataclass
class TrainResult:
    model_key:    str             # clé S3 du fichier .pkl
    metrics_key:  str             # clé S3 du fichier _metrics.json
    version:      str             # YYYYMMDD
    metrics:      dict[str, Any] = field(default_factory=dict)
    is_champion:  bool = False    # True si promu en production


# ════════════════════════════════════════════════════════════
#  Point d'entrée
# ════════════════════════════════════════════════════════════

def train(
    dataset: DatasetResult,
    store=None,
    promote: bool = True,
) -> TrainResult:
    """
    Entraîne l'Isolation Forest sur dataset.X_train (légitimes purs).

    Paramètres
    ----------
    dataset : DatasetResult produit par build_dataset()
    store   : ObjectStore injecté pour les tests (None = auto)
    promote : si True, copie le modèle vers production/current.json
              après l'entraînement (comportement par défaut)

    Retourne
    --------
    TrainResult avec les clés S3 et les métriques d'entraînement.
    """
    obj_store = store or get_object_store()
    version   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    logger.info(
        "Entraînement IF — n_train=%d, n_features=%d, "
        "n_estimators=%d, contamination=%.3f",
        dataset.n_train, len(FEATURE_NAMES),
        IF_N_ESTIMATORS, IF_CONTAMINATION,
    )

    # ── 1. Normalisation (RobustScaler résiste aux outliers) ──
    scaler = RobustScaler()
    X_train_scaled = scaler.fit_transform(dataset.X_train)

    # ── 2. Entraînement ───────────────────────────────────────
    max_samples = (
        IF_MAX_SAMPLES if IF_MAX_SAMPLES == "auto"
        else int(IF_MAX_SAMPLES)
    )
    model = IsolationForest(
        n_estimators  = IF_N_ESTIMATORS,
        contamination = IF_CONTAMINATION,
        max_samples   = max_samples,
        random_state  = SEED,
        n_jobs        = -1,
    )
    model.fit(X_train_scaled)
    logger.info("Isolation Forest entraîné")

    # ── 3. Score sur le train (sanity check) ──────────────────
    train_scores = model.score_samples(X_train_scaled)
    train_anomaly_rate = float((model.predict(X_train_scaled) == -1).mean())
    logger.info(
        "Taux d'anomalie sur train : %.3f (attendu ≈ %.3f)",
        train_anomaly_rate, IF_CONTAMINATION,
    )

    # ── 4. Métriques d'entraînement ───────────────────────────
    metrics = {
        "version":           version,
        "n_train":           dataset.n_train,
        "n_test":            dataset.n_test,
        "n_fraud_test":      dataset.meta.get("n_fraud_test", 0),
        "n_features":        len(FEATURE_NAMES),
        "feature_names":     FEATURE_NAMES,
        "if_n_estimators":   IF_N_ESTIMATORS,
        "if_contamination":  IF_CONTAMINATION,
        "if_max_samples":    str(max_samples),
        "train_anomaly_rate": round(train_anomaly_rate, 4),
        "train_score_mean":  round(float(train_scores.mean()), 4),
        "train_score_std":   round(float(train_scores.std()), 4),
        "seed":              SEED,
        "trained_at":        datetime.now(timezone.utc).isoformat(),
        "dataset_meta":      dataset.meta,
    }

    # ── 5. Sérialisation dans MinIO/S3 ────────────────────────
    # On emballe (model, scaler) ensemble pour que le detector
    # n'ait besoin que d'un seul fichier.
    bundle = {"model": model, "scaler": scaler, "feature_names": FEATURE_NAMES}

    model_key   = f"{MODEL_PREFIX}_{version}.pkl"
    metrics_key = f"{MODEL_PREFIX}_{version}_metrics.json"

    obj_store.save_model(BUCKET_MODELS, model_key, bundle)
    obj_store.save_json(BUCKET_MODELS, metrics_key, metrics)
    logger.info("Modèle sauvegardé → %s/%s", BUCKET_MODELS, model_key)

    # ── 6. Promotion en production ────────────────────────────
    is_champion = False
    if promote:
        is_champion = _promote(obj_store, model_key, metrics_key, metrics)

    return TrainResult(
        model_key   = model_key,
        metrics_key = metrics_key,
        version     = version,
        metrics     = metrics,
        is_champion = is_champion,
    )


# ════════════════════════════════════════════════════════════
#  Promotion champion / challenger
# ════════════════════════════════════════════════════════════

def _promote(
    store,
    model_key:   str,
    metrics_key: str,
    metrics:     dict,
) -> bool:
    """
    Promeut le modèle en production si :
    - Aucun champion n'existe encore → promotion automatique
    - Le taux d'anomalie sur le train est dans la plage attendue
      [contamination × 0.5, contamination × 2.0]

    Écrit production/current.json avec le pointeur vers le nouveau modèle.
    Retourne True si promotion effectuée.
    """
    # Vérifier si un champion existe déjà
    current: dict[str, Any] = {}
    try:
        current = store.load_json(BUCKET_MODELS, PRODUCTION_KEY)
    except Exception:
        pass  # pas encore de champion → promotion automatique

    train_anomaly_rate = metrics.get("train_anomaly_rate", 0.0)
    low  = IF_CONTAMINATION * 0.5
    high = IF_CONTAMINATION * 2.0
    rate_ok = low <= train_anomaly_rate <= high

    if not current:
        logger.info("Aucun champion existant — promotion automatique")
        _write_current(store, model_key, metrics_key, metrics)
        return True

    if rate_ok:
        logger.info(
            "Taux d'anomalie %.3f dans la plage [%.3f, %.3f] — champion promu",
            train_anomaly_rate, low, high,
        )
        _write_current(store, model_key, metrics_key, metrics)
        return True

    logger.warning(
        "Taux d'anomalie %.3f hors plage [%.3f, %.3f] — pas de promotion",
        train_anomaly_rate, low, high,
    )
    return False


def _write_current(store, model_key: str, metrics_key: str, metrics: dict) -> None:
    current_info = {
        "model_key":   model_key,
        "metrics_key": metrics_key,
        "version":     metrics.get("version", "unknown"),
        "promoted_at": datetime.now(timezone.utc).isoformat(),
        "n_train":     metrics.get("n_train", 0),
        "train_anomaly_rate": metrics.get("train_anomaly_rate", 0.0),
    }
    store.save_json(BUCKET_MODELS, PRODUCTION_KEY, current_info)
    logger.info("production/current.json mis à jour → %s", model_key)
