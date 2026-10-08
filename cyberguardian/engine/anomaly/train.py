"""
engine/anomaly/train.py
────────────────────────
Entraînement de l'Isolation Forest (IA-5) et versionnage dans MinIO/S3.

  1. RobustScaler ajusté sur X_train (trafic légitime NORMAL uniquement)
  2. Réglage des hyperparamètres sur le jeu de VALIDATION (jamais le test)
  3. Entraînement final sur X_train
  4. Calibration score → 0-100 à partir des scores train
  5. Sauvegarde versionnée : models/anomaly/isolation_forest_<version>.pkl
     + _metrics.json, puis mise à jour du pointeur production/current.json
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import numpy as np
import sklearn
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import RobustScaler
from sklearn.metrics import roc_curve

from engine.anomaly.dataset import DatasetResult
from interfaces.store import get_object_store, BUCKET_MODELS

logger = logging.getLogger(__name__)

# ── Paramètres Isolation Forest ───────────────────────────────
IF_N_ESTIMATORS  = int(os.getenv("IF_N_ESTIMATORS",  "300"))
IF_CONTAMINATION = float(os.getenv("IF_CONTAMINATION", "0.05"))
IF_MAX_SAMPLES   = os.getenv("IF_MAX_SAMPLES", "256")
IF_MAX_FEATURES  = float(os.getenv("IF_MAX_FEATURES", "0.7"))
SEED             = int(os.getenv("SEED", "42"))

MODEL_PREFIX   = "anomaly/isolation_forest"
PRODUCTION_KEY = "anomaly/production/current.json"


@dataclass
class TrainResult:
    model_key:    str
    metrics_key:  str
    version:      str
    metrics:      dict[str, Any] = field(default_factory=dict)
    is_champion:  bool = False


def _recall_at_fpr(y: np.ndarray, scores: np.ndarray, target_fpr: float = 0.01) -> float:
    fpr, tpr, _ = roc_curve(y, scores)
    mask = fpr <= target_fpr
    return float(tpr[mask].max()) if mask.any() else 0.0


def _default_params() -> dict:
    max_samples: int | str = int(IF_MAX_SAMPLES) if IF_MAX_SAMPLES.isdigit() else IF_MAX_SAMPLES
    return {"n_estimators": IF_N_ESTIMATORS, "max_samples": max_samples,
            "max_features": IF_MAX_FEATURES}


def _auto_tune(X_train: np.ndarray, X_val: np.ndarray, y_val: np.ndarray) -> tuple[dict, list[dict]]:
    """
    Grille max_samples × max_features ; retient la combinaison qui maximise
    le rappel à 1 % de faux positifs sur la VALIDATION.
    Retourne (meilleurs paramètres, tableau de tous les essais).
    """
    best_recall, best_params = -1.0, _default_params()
    essais: list[dict] = []
    for max_samp in [128, 256, 512]:
        for max_feat in [0.6, 0.7, 0.85, 1.0]:
            m = IsolationForest(
                n_estimators=IF_N_ESTIMATORS, max_samples=max_samp,
                max_features=max_feat, random_state=SEED, n_jobs=-1,
            )
            m.fit(X_train)
            recall_1pct = _recall_at_fpr(y_val, -m.score_samples(X_val), 0.01)
            essais.append({"max_samples": max_samp, "max_features": max_feat,
                           "val_recall_at_1pct_fpr": round(recall_1pct, 4)})
            if recall_1pct > best_recall:
                best_recall = recall_1pct
                best_params = {"n_estimators": IF_N_ESTIMATORS, "max_samples": max_samp, "max_features": max_feat}
    logger.info("Auto-tuning IF (validation) — meilleurs params : %s (recall@1%%FPR=%.4f)",
                best_params, best_recall)
    return best_params, essais


def train(
    dataset: DatasetResult,
    store=None,
    promote: bool = True,
) -> TrainResult:
    obj_store = store or get_object_store()
    version   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    logger.info(
        "Entraînement IF — n_train=%d, n_features=%d",
        dataset.n_train, len(dataset.feature_names),
    )

    # ── 1. Normalisation (ajustée sur le train uniquement) ─────
    scaler = RobustScaler()
    X_train_scaled = scaler.fit_transform(dataset.X_train)

    # ── 2. Réglage sur la validation (le test n'est jamais regardé) ──
    y_val = dataset.y_val
    has_val = len(y_val) > 0 and 0 < int(y_val.sum()) < len(y_val)
    if has_val:
        X_val_scaled = scaler.transform(dataset.X_val)
        best_params, tuning_trials = _auto_tune(X_train_scaled, X_val_scaled, y_val)
        # contamination n'influence que model.predict() (pas score_samples,
        # donc pas le score 0-100) : on la cale sur le taux de fraude validation.
        contamination = float(np.clip(y_val.mean(), 0.01, 0.15))
    else:
        logger.warning("Pas de jeu de validation exploitable — paramètres par défaut, sans auto-tuning")
        best_params, tuning_trials = _default_params(), []
        contamination = IF_CONTAMINATION

    model = IsolationForest(
        n_estimators  = best_params["n_estimators"],
        contamination = contamination,
        max_samples   = best_params["max_samples"],
        max_features  = best_params["max_features"],
        random_state  = SEED,
        n_jobs        = -1,
    )
    model.fit(X_train_scaled)
    logger.info("Isolation Forest entraîné")

    # ── 3. Score sur le train ───────────────────────────────────
    train_scores = model.score_samples(X_train_scaled)
    train_anomaly_rate = float((model.predict(X_train_scaled) == -1).mean())

    # ── Calibration quantile_piecewise (UNIQUEMENT sur train) ──
    train_anomaly_scores = -train_scores
    q50  = float(np.quantile(train_anomaly_scores, 0.50))
    q90  = float(np.quantile(train_anomaly_scores, 0.90))
    q95  = float(np.quantile(train_anomaly_scores, 0.95))
    q99  = float(np.quantile(train_anomaly_scores, 0.99))
    q999 = float(np.quantile(train_anomaly_scores, 0.999))

    score_calibration = {
        "type": "quantile_piecewise",
        "points": [[q50, 0], [q90, 30], [q95, 50], [q99, 90], [q999, 100]],
        "source": "train_only",
    }

    val_recall_1pct = (
        _recall_at_fpr(y_val, -model.score_samples(X_val_scaled), 0.01) if has_val else None
    )

    # ── 4. Métriques ─────────────────────────────────────────────
    metrics = {
        "version":             version,
        "n_train":             dataset.n_train,
        "n_val":               dataset.n_val,
        "n_fraud_val":         int(y_val.sum()) if len(y_val) else 0,
        "val_recall_at_1pct_fpr": None if val_recall_1pct is None else round(val_recall_1pct, 4),
        "tuning_trials":       tuning_trials,
        "n_test":              dataset.n_test,
        "n_fraud_test":        dataset.meta.get("n_fraud_test", 0),
        "sklearn_version":     sklearn.__version__,
        "n_features":          len(dataset.feature_names),
        "feature_names":       dataset.feature_names,
        "if_n_estimators":     best_params["n_estimators"],
        "if_contamination":    contamination,
        "if_max_samples":      str(best_params["max_samples"]),
        "if_max_features":     best_params["max_features"],
        "train_anomaly_rate":  round(train_anomaly_rate, 4),
        "train_score_mean":    round(float(train_scores.mean()), 4),
        "train_score_std":     round(float(train_scores.std()), 4),
        "score_calibration":   score_calibration,
        "calibration_source":  score_calibration["source"],
        "seed":                SEED,
        "trained_at":          datetime.now(timezone.utc).isoformat(),
        "dataset_meta":        dataset.meta,
    }

    # ── 5. Sérialisation ──────────────────────────────────────────
    bundle = {
        "model":             model,
        "scaler":            scaler,
        "feature_names":     dataset.feature_names,
        "score_calibration": score_calibration,
        "version":           version,
        "sklearn_version":   sklearn.__version__,   # le pickle dépend de cette version
    }

    model_key   = f"{MODEL_PREFIX}_{version}.pkl"
    metrics_key = f"{MODEL_PREFIX}_{version}_metrics.json"

    obj_store.save_model(BUCKET_MODELS, model_key, bundle)
    obj_store.save_json(BUCKET_MODELS, metrics_key, metrics)
    logger.info("Modèle sauvegardé → %s/%s", BUCKET_MODELS, model_key)

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


def _promote(store, model_key: str, metrics_key: str, metrics: dict) -> bool:
    current: dict[str, Any] = {}
    try:
        current = store.load_json(BUCKET_MODELS, PRODUCTION_KEY)
    except Exception:
        pass
    if not current:
        logger.info("Aucun champion existant — promotion automatique")
        _write_current(store, model_key, metrics_key, metrics)
        return True
    _write_current(store, model_key, metrics_key, metrics)
    return True


def _write_current(store, model_key: str, metrics_key: str, metrics: dict) -> None:
    current_info = {
        "model_key":          model_key,
        "metrics_key":        metrics_key,
        "version":            metrics.get("version", "unknown"),
        "promoted_at":        datetime.now(timezone.utc).isoformat(),
        "n_train":            metrics.get("n_train", 0),
        "train_anomaly_rate": metrics.get("train_anomaly_rate", 0.0),
        "calibration_source": metrics.get("calibration_source", "unknown"),
        "val_recall_at_1pct_fpr": metrics.get("val_recall_at_1pct_fpr"),
        "sklearn_version":    metrics.get("sklearn_version"),
    }
    store.save_json(BUCKET_MODELS, PRODUCTION_KEY, current_info)
    logger.info("production/current.json mis à jour → %s", model_key)