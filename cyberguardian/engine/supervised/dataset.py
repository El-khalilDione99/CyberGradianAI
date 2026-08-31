"""
engine/supervised/dataset.py
─────────────────────────────
Construction du dataset supervisé pour XGBoost (IA-6).

Différence fondamentale avec IA-5 (Isolation Forest) :
  - IA-5 : entraînement sur LÉGITIMES PURS uniquement (non supervisé)
  - IA-6 : entraînement sur TOUTES LES CLASSES (fraudes + légitimes)
           Le double filtre NORMAL/label=0 de IA-5 est SUPPRIMÉ ici.

Flux :
  1. Charger les profils Redis (ou override pour tests)
  2. Charger les événements (fournis ou depuis MinIO dump)
  3. Calculer compute_features() pour chaque transaction
  4. Produire X (features), y (labels 0/1), et scale_pos_weight
  5. Split stratifié 80/20 par abonné (même logique que IA-5)
  6. Optionnel : exporter le dataset en Parquet vers S3 pour SageMaker

scale_pos_weight = n_légitimes / n_fraudes
  Compense le déséquilibre de classes sans sur-échantillonnage.
  XGBoost pondère chaque fraude comme si elle comptait N fois plus.

Retourne :
  SupervisedDatasetResult avec X_train, y_train, X_test, y_test,
  scale_pos_weight, feature_names, metadata
"""

from __future__ import annotations

import json
import logging
import os
import random
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import numpy as np

from engine.rules.features import compute_features
from interfaces.store import get_object_store, BUCKET_DATASETS

logger = logging.getLogger(__name__)

# ── Features IA-6 ─────────────────────────────────────────────
# On inclut hours_since_sim_swap (exclu de IA-5 car inf pollue RobustScaler,
# mais les arbres XGBoost tolèrent les valeurs extrêmes / NaN imputés).
# On ajoute aussi solde (signal de contexte financier utile en supervisé).
XGB_FEATURE_NAMES: list[str] = [
    # Features comportementales instantanées
    "amount_ratio",
    "zscore_montant",
    "hours_since_sim_swap",   # ← ajouté par rapport à IA-5 (inf → 9999.0)
    "new_device",
    "new_beneficiary",
    "is_roaming",
    # Compteurs de fenêtres glissantes
    "otp_count_1h",
    "nb_tx_1h",
    "nb_tx_24h",
    "nb_tx_7j",
    "nb_otp_24h",
    "nb_swaps_30j",
    # Contexte temporel
    "is_active_hour",
    "hour_of_day",
    # Profil abonné (stabilisé par Welford)
    "montant_moyen",
    "ecart_type_montant",
    "solde",                  # ← ajouté : signal de contexte financier
]

SEED        = int(os.getenv("SEED", "42"))
TRAIN_RATIO = 0.80

# Clé S3 du dump JSON des événements
EVENTS_DUMP_KEY  = "datasets/transactions_dump.json"
# Clé S3 du dataset Parquet exporté pour SageMaker
PARQUET_TRAIN_KEY = "datasets/xgboost_train.parquet"
PARQUET_TEST_KEY  = "datasets/xgboost_test.parquet"

# Valeur de substitution pour hours_since_sim_swap = inf (aucun swap)
INF_HOURS_SUBSTITUTE = 9999.0


# ════════════════════════════════════════════════════════════
#  Résultat supervisé
# ════════════════════════════════════════════════════════════

@dataclass
class SupervisedDatasetResult:
    X_train:          np.ndarray
    y_train:          np.ndarray       # 0 = légitime, 1 = fraude
    X_test:           np.ndarray
    y_test:           np.ndarray
    scale_pos_weight: float            # n_légitimes_train / n_fraudes_train
    feature_names:    list[str]
    meta: dict[str, Any] = field(default_factory=dict)

    @property
    def n_train(self) -> int:
        return len(self.X_train)

    @property
    def n_test(self) -> int:
        return len(self.X_test)

    @property
    def fraud_rate_train(self) -> float:
        if len(self.y_train) == 0:
            return 0.0
        return float(self.y_train.sum() / len(self.y_train))

    @property
    def fraud_rate_test(self) -> float:
        if len(self.y_test) == 0:
            return 0.0
        return float(self.y_test.sum() / len(self.y_test))


# ════════════════════════════════════════════════════════════
#  Point d'entrée
# ════════════════════════════════════════════════════════════

def build_dataset(
    events: list[dict[str, Any]] | None = None,
    store=None,
    profiles_override: dict[str, dict] | None = None,
    export_parquet: bool = False,
) -> SupervisedDatasetResult:
    """
    Construit le dataset supervisé pour XGBoost.

    Paramètres
    ----------
    events            : événements transactions. Si None, charge depuis MinIO.
    store             : ObjectStore injecté (tests). None = auto.
    profiles_override : dict {id_compte → profil} pour les tests sans Redis.
    export_parquet    : si True, exporte train/test en Parquet dans S3
                        (requis pour SageMaker Training Jobs).

    Retourne
    --------
    SupervisedDatasetResult avec X_train/y_train, X_test/y_test,
    scale_pos_weight, feature_names.
    """
    obj_store = store or get_object_store()

    # ── 1. Charger les événements ─────────────────────────────
    if events is None:
        events = _load_events_from_store(obj_store)
    logger.info("Supervisé — %d événements bruts chargés", len(events))

    # ── 2. Charger les profils ─────────────────────────────────
    profiles = profiles_override if profiles_override is not None else _load_profiles()
    logger.info("Supervisé — %d profils chargés", len(profiles))

    # ── 3. Calculer les features pour TOUTES les classes ──────
    # Pas de filtre ici — on prend fraudes ET légitimes (tous scénarios).
    all_rows: list[dict] = []
    for ev in events:
        if "id_transaction" not in ev:
            continue
        row = _process_event(ev, profiles)
        if row is not None:
            all_rows.append(row)

    n_fraude  = sum(1 for r in all_rows if r["_label"] == 1)
    n_legitime = sum(1 for r in all_rows if r["_label"] == 0)

    logger.info(
        "Supervisé — %d lignes total (fraudes=%d, légitimes=%d)",
        len(all_rows), n_fraude, n_legitime,
    )

    if n_fraude == 0:
        raise ValueError(
            "Aucune fraude dans le dataset — XGBoost supervisé impossible. "
            "Vérifier que le simulateur a généré des scénarios frauduleux "
            "avec type_scenario et label_fraude dans les payloads."
        )
    if n_legitime == 0:
        raise ValueError("Aucun légitime dans le dataset.")

    # ── 4. Split stratifié par abonné ──────────────────────────
    # Stratification : on s'assure que chaque split contient
    # des fraudes ET des légitimes, en séparant par abonné.
    train_rows, test_rows = _stratified_split_by_subscriber(
        all_rows, TRAIN_RATIO, SEED
    )

    # ── 5. Construire les matrices numpy ───────────────────────
    X_train, y_train = _rows_to_Xy(train_rows)
    X_test,  y_test  = _rows_to_Xy(test_rows)

    # ── 6. Calculer scale_pos_weight ──────────────────────────
    n_fraud_train  = int(y_train.sum())
    n_legit_train  = int((y_train == 0).sum())
    scale_pos_weight = (
        float(n_legit_train) / float(n_fraud_train)
        if n_fraud_train > 0 else 1.0
    )

    logger.info(
        "Supervisé — train=%d (fraudes=%d, légitimes=%d, spw=%.1f) | test=%d",
        len(X_train), n_fraud_train, n_legit_train,
        scale_pos_weight, len(X_test),
    )

    meta = {
        "n_events_bruts":    len(events),
        "n_profiles":        len(profiles),
        "n_train":           len(X_train),
        "n_test":            len(X_test),
        "n_fraud_train":     n_fraud_train,
        "n_fraud_test":      int(y_test.sum()),
        "fraud_rate_train":  round(float(y_train.mean()), 4),
        "fraud_rate_test":   round(float(y_test.mean()), 4),
        "scale_pos_weight":  round(scale_pos_weight, 2),
        "feature_names":     XGB_FEATURE_NAMES,
        "seed":              SEED,
        "built_at":          datetime.now(timezone.utc).isoformat(),
    }

    result = SupervisedDatasetResult(
        X_train          = X_train,
        y_train          = y_train,
        X_test           = X_test,
        y_test           = y_test,
        scale_pos_weight = scale_pos_weight,
        feature_names    = XGB_FEATURE_NAMES,
        meta             = meta,
    )

    # ── 7. Export Parquet pour SageMaker (optionnel) ───────────
    if export_parquet:
        _export_parquet(obj_store, X_train, y_train, X_test, y_test)

    return result


# ════════════════════════════════════════════════════════════
#  Helpers internes
# ════════════════════════════════════════════════════════════

def _process_event(
    ev: dict[str, Any],
    profiles: dict[str, dict],
) -> dict[str, Any] | None:
    """Calcule les features d'une transaction et retourne la ligne."""
    id_compte    = ev.get("id_compte", "")
    label_fraude = int(ev.get("label_fraude", 0))

    profile = profiles.get(id_compte, {})
    if not profile:
        return None

    try:
        features = compute_features(ev, profile)
    except Exception as exc:
        logger.debug("Erreur compute_features %s : %s", id_compte[:12], exc)
        return None

    row: dict[str, Any] = {}
    for f in XGB_FEATURE_NAMES:
        val = features.get(f, 0.0)
        # hours_since_sim_swap = inf → remplacer par constante
        if f == "hours_since_sim_swap" and (
            val == float("inf") or val != val  # inf ou NaN
        ):
            val = INF_HOURS_SUBSTITUTE
        row[f] = float(val) if not isinstance(val, bool) else float(int(val))

    row["_id_compte"] = id_compte
    row["_label"]     = label_fraude
    return row


def _stratified_split_by_subscriber(
    rows: list[dict],
    train_ratio: float,
    seed: int,
) -> tuple[list[dict], list[dict]]:
    """
    Split 80/20 stratifié par abonné ET par classe.

    Stratégie :
      - Séparer les abonnés en deux groupes : ceux qui ont eu des fraudes
        et ceux qui n'en ont pas.
      - Pour chaque groupe, 80% des abonnés → train, 20% → test.
      - Garantit que le test contient des fraudes ET des légitimes.
    """
    # Grouper par abonné
    by_subscriber: dict[str, list[dict]] = {}
    for row in rows:
        by_subscriber.setdefault(row["_id_compte"], []).append(row)

    # Séparer abonnés frauduleux et non-frauduleux
    fraud_subscribers   = [acc for acc, r in by_subscriber.items()
                           if any(x["_label"] == 1 for x in r)]
    legit_subscribers   = [acc for acc, r in by_subscriber.items()
                           if all(x["_label"] == 0 for x in r)]

    rng = random.Random(seed)
    fraud_subscribers.sort()
    legit_subscribers.sort()
    rng.shuffle(fraud_subscribers)
    rng.shuffle(legit_subscribers)

    cut_fraud = max(1, int(len(fraud_subscribers) * train_ratio))
    cut_legit = max(1, int(len(legit_subscribers) * train_ratio))

    train_ids = set(fraud_subscribers[:cut_fraud]) | set(legit_subscribers[:cut_legit])

    train_rows = [r for r in rows if r["_id_compte"] in train_ids]
    test_rows  = [r for r in rows if r["_id_compte"] not in train_ids]

    return train_rows, test_rows


def _rows_to_Xy(rows: list[dict]) -> tuple[np.ndarray, np.ndarray]:
    """Convertit une liste de rows en (X, y) numpy."""
    if not rows:
        return (
            np.empty((0, len(XGB_FEATURE_NAMES)), dtype=np.float32),
            np.empty(0, dtype=np.int8),
        )
    X = np.array(
        [[row[f] for f in XGB_FEATURE_NAMES] for row in rows],
        dtype=np.float32,
    )
    y = np.array([row["_label"] for row in rows], dtype=np.int8)
    return X, y


def _export_parquet(
    store,
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_test:  np.ndarray,
    y_test:  np.ndarray,
) -> None:
    """
    Exporte les datasets en Parquet vers S3/MinIO.
    Requis pour SageMaker Training Jobs (ingestion S3 Parquet).
    """
    try:
        import pandas as pd  # type: ignore
        import io

        def _to_parquet_bytes(X: np.ndarray, y: np.ndarray) -> bytes:
            df = pd.DataFrame(X, columns=XGB_FEATURE_NAMES)
            df["label"] = y.astype(int)
            buf = io.BytesIO()
            df.to_parquet(buf, index=False, engine="pyarrow")
            return buf.getvalue()

        store.upload(BUCKET_DATASETS, PARQUET_TRAIN_KEY,
                     _to_parquet_bytes(X_train, y_train))
        store.upload(BUCKET_DATASETS, PARQUET_TEST_KEY,
                     _to_parquet_bytes(X_test, y_test))
        logger.info(
            "Parquet exporté → %s/%s et %s",
            BUCKET_DATASETS, PARQUET_TRAIN_KEY, PARQUET_TEST_KEY,
        )
    except Exception as exc:
        logger.warning("Export Parquet échoué : %s", exc)


def _load_profiles() -> dict[str, dict]:
    """Charge tous les profils depuis Redis."""
    try:
        import redis as _redis
        client = _redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            decode_responses=True,
        )
        keys = client.keys("profile:*")
        profiles = {}
        for key in keys:
            raw = client.get(key)
            if raw:
                profiles[key.replace("profile:", "")] = json.loads(raw)
        return profiles
    except Exception as exc:
        logger.warning("Impossible de charger les profils Redis : %s", exc)
        return {}


def _load_events_from_store(store) -> list[dict[str, Any]]:
    """Charge le dump des événements depuis MinIO/S3."""
    try:
        raw = store.download(BUCKET_DATASETS, EVENTS_DUMP_KEY)
        events = json.loads(raw.decode("utf-8"))
        logger.info("Events chargés depuis store : %d", len(events))
        return events
    except Exception as exc:
        raise ValueError(
            f"Aucun événement fourni et chargement depuis MinIO échoué : {exc}"
        ) from exc
