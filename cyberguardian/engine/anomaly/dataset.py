"""
engine/anomaly/dataset.py
──────────────────────────
Construction du dataset d'entraînement pour l'Isolation Forest (IA-5).

Flux :
  1. Lire tous les profils depuis Redis (clés profile:*)
  2. Consommer les événements Kafka (topic transactions) déjà publiés
     OU recharger depuis un dump JSON si disponible dans MinIO
  3. Appliquer le double filtre :
       - type_scenario == "NORMAL"   (pur trafic normal, pas de scénario scripté)
       - label_fraude  == 0          (redondant mais défensif)
  4. Reconstruire les features via compute_features(event, profile)
  5. Splitter en train/test stratifié par abonné (80/20, seed=42)
     pour garantir qu'un même abonné n'apparaît pas dans les deux parties

Retourne :
  DatasetResult avec X_train, X_test, y_test, feature_names, metadata
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
from interfaces.store import get_feature_store, get_object_store, BUCKET_DATASETS

logger = logging.getLogger(__name__)

# ── Features retenues pour l'entraînement ────────────────────
# On exclut les features instables sur les nouveaux comptes
# (hours_since_sim_swap = inf, nb_beneficiaires_1h = approximation).
# On exclut aussi montant_courant (valeur brute, redondant avec amount_ratio).
FEATURE_NAMES: list[str] = [
    "amount_ratio",
    "zscore_montant",
    "new_device",
    "new_beneficiary",
    "is_roaming",
    "otp_count_1h",
    "nb_tx_1h",
    "is_active_hour",
    "hour_of_day",
    "montant_moyen",
    "ecart_type_montant",
    "nb_swaps_30j",
    "nb_otp_24h",
    "nb_tx_24h",
    "nb_tx_7j",
]

# hours_since_sim_swap est gardé séparément — utilisé par le z-score override
# dans le detector mais pas dans le vecteur IF (inf pollue la normalisation).

SEED = int(os.getenv("SEED", "42"))
TRAIN_RATIO = 0.80

# Clé S3 du dump JSON des événements (produit par le simulateur)
EVENTS_DUMP_KEY = "datasets/transactions_dump.json"


# ════════════════════════════════════════════════════════════
#  Résultat
# ════════════════════════════════════════════════════════════

@dataclass
class DatasetResult:
    X_train:       np.ndarray
    X_test:        np.ndarray
    y_test:        np.ndarray          # 0 = légitime, 1 = fraude
    feature_names: list[str]
    meta: dict[str, Any] = field(default_factory=dict)

    @property
    def n_train(self) -> int:
        return len(self.X_train)

    @property
    def n_test(self) -> int:
        return len(self.X_test)

    @property
    def fraud_rate_test(self) -> float:
        if len(self.y_test) == 0:
            return 0.0
        return float(self.y_test.sum() / len(self.y_test))


# ════════════════════════════════════════════════════════════
#  Point d'entrée principal
# ════════════════════════════════════════════════════════════

def build_dataset(
    events: list[dict[str, Any]] | None = None,
    store=None,
    profiles_override: dict[str, dict] | None = None,
) -> DatasetResult:
    """
    Construit le dataset d'entraînement et de test pour IA-5.

    Paramètres
    ----------
    events : liste d'événements transactions (dicts). Si None, tente de
             charger depuis MinIO (EVENTS_DUMP_KEY) puis lève ValueError.
    store  : ObjectStore injecté pour les tests (None = auto-détection).

    Retourne
    --------
    DatasetResult avec train (légitimes purs) et test (légitimes + fraudes).
    """
    # ── 1. Charger les événements ─────────────────────────────
    if events is None:
        events = _load_events_from_store(store)

    logger.info("Dataset — %d événements bruts chargés", len(events))

    # ── 2. Charger les profils Redis (ou utiliser l'override injecté) ──
    profiles = profiles_override if profiles_override is not None else _load_profiles()
    logger.info("Dataset — %d profils chargés", len(profiles))

    # ── 3. Calculer les features pour chaque événement ────────
    rows_normal: list[dict]  = []   # type_scenario=NORMAL, label=0 → train + test légitime
    rows_fraud:  list[dict]  = []   # label=1 → test fraude
    rows_atypical: list[dict] = []  # légitime atypique (scénarios non-NORMAL) → test fp

    for ev in events:
        if ev.get("stream", "") == "transactions" or "id_transaction" in ev:
            _process_event(ev, profiles, rows_normal, rows_fraud, rows_atypical)

    logger.info(
        "Dataset — normal=%d, fraud=%d, atypical=%d",
        len(rows_normal), len(rows_fraud), len(rows_atypical),
    )

    if len(rows_normal) < 10:
        raise ValueError(
            f"Pas assez d'événements normaux ({len(rows_normal)}) pour entraîner. "
            "Lance d'abord le simulateur."
        )

    # ── 4. Split train/test stratifié par abonné ──────────────
    X_train, X_test_legit = _split_by_subscriber(rows_normal, TRAIN_RATIO, SEED)

    # ── 5. Constituer le jeu de test complet ──────────────────
    # test = légitimes purs (20%) + légitimes atypiques + fraudes
    test_rows   = X_test_legit + rows_atypical + rows_fraud
    y_test_vals = (
        [0] * len(X_test_legit)
        + [0] * len(rows_atypical)
        + [1] * len(rows_fraud)
    )

    # Mélanger pour éviter que les fraudes soient toutes en fin de liste
    rng = random.Random(SEED)
    combined = list(zip(test_rows, y_test_vals))
    rng.shuffle(combined)
    test_rows, y_test_vals = zip(*combined) if combined else ([], [])

    # ── 6. Convertir en numpy ─────────────────────────────────
    X_train_np = _rows_to_matrix(list(X_train))
    X_test_np  = _rows_to_matrix(list(test_rows))
    y_test_np  = np.array(list(y_test_vals), dtype=np.int8)

    meta = {
        "n_events_bruts":   len(events),
        "n_profiles":       len(profiles),
        "n_train":          len(X_train_np),
        "n_test":           len(X_test_np),
        "n_fraud_test":     int(y_test_np.sum()),
        "n_atypical_test":  len(rows_atypical),
        "fraud_rate_test":  round(float(y_test_np.mean()), 4) if len(y_test_np) else 0.0,
        "feature_names":    FEATURE_NAMES,
        "seed":             SEED,
        "built_at":         datetime.now(timezone.utc).isoformat(),
    }
    logger.info(
        "Dataset final — train=%d | test=%d (fraudes=%d, fp_potentiels=%d)",
        len(X_train_np), len(X_test_np), int(y_test_np.sum()), len(rows_atypical),
    )
    return DatasetResult(
        X_train=X_train_np,
        X_test=X_test_np,
        y_test=y_test_np,
        feature_names=FEATURE_NAMES,
        meta=meta,
    )


# ════════════════════════════════════════════════════════════
#  Helpers internes
# ════════════════════════════════════════════════════════════

def _process_event(
    ev: dict[str, Any],
    profiles: dict[str, dict],
    rows_normal: list,
    rows_fraud: list,
    rows_atypical: list,
) -> None:
    """Calcule les features d'un événement et le classe dans le bon bucket."""
    id_compte     = ev.get("id_compte", "")
    label_fraude  = int(ev.get("label_fraude", 0))
    type_scenario = ev.get("type_scenario", "NORMAL")

    profile = profiles.get(id_compte, {})
    if not profile:
        return  # profil inconnu → on ignore (pas assez de contexte)

    try:
        features = compute_features(ev, profile)
    except Exception as exc:
        logger.debug("Erreur compute_features pour %s : %s", id_compte[:12], exc)
        return

    row = {f: features.get(f, 0.0) for f in FEATURE_NAMES}
    row["_id_compte"]     = id_compte
    row["_label_fraude"]  = label_fraude
    row["_type_scenario"] = type_scenario

    # Double filtre entraînement : NORMAL ET label=0
    if label_fraude == 0 and type_scenario == "NORMAL":
        rows_normal.append(row)
    elif label_fraude == 1:
        rows_fraud.append(row)
    else:
        # Légitime atypique (SWAP_LEGITIME, NOUVEAU_DEVICE_LEGITIME, etc.)
        rows_atypical.append(row)


def _split_by_subscriber(
    rows: list[dict],
    train_ratio: float,
    seed: int,
) -> tuple[list[dict], list[dict]]:
    """
    Split 80/20 stratifié par abonné.
    Pour chaque abonné, 80% de ses transactions vont en train, 20% en test.
    Garantit qu'un même abonné n'est PAS partagé entre train et test —
    ce qui éviterait une évaluation trop optimiste.
    """
    # Regrouper par abonné
    by_subscriber: dict[str, list[dict]] = {}
    for row in rows:
        acc = row["_id_compte"]
        by_subscriber.setdefault(acc, []).append(row)

    rng = random.Random(seed)
    subscriber_ids = sorted(by_subscriber.keys())  # tri pour reproductibilité
    rng.shuffle(subscriber_ids)

    cut = int(len(subscriber_ids) * train_ratio)
    train_ids = set(subscriber_ids[:cut])

    train_rows = [r for r in rows if r["_id_compte"] in train_ids]
    test_rows  = [r for r in rows if r["_id_compte"] not in train_ids]

    return train_rows, test_rows


def _rows_to_matrix(rows: list[dict]) -> np.ndarray:
    """Convertit une liste de dicts en matrice numpy (n_samples, n_features)."""
    if not rows:
        return np.empty((0, len(FEATURE_NAMES)), dtype=np.float32)
    matrix = []
    for row in rows:
        vec = [float(row.get(f, 0.0)) for f in FEATURE_NAMES]
        matrix.append(vec)
    return np.array(matrix, dtype=np.float32)


def _load_profiles() -> dict[str, dict]:
    """Charge tous les profils depuis Redis (clés profile:*)."""
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
                id_compte = key.replace("profile:", "")
                profiles[id_compte] = json.loads(raw)
        return profiles
    except Exception as exc:
        logger.warning("Impossible de charger les profils Redis : %s", exc)
        return {}


def _load_events_from_store(store=None) -> list[dict[str, Any]]:
    """Charge le dump des événements depuis MinIO/S3."""
    try:
        obj_store = store or get_object_store()
        raw = obj_store.download(BUCKET_DATASETS, EVENTS_DUMP_KEY)
        events = json.loads(raw.decode("utf-8"))
        logger.info("Events chargés depuis MinIO : %d", len(events))
        return events
    except Exception as exc:
        raise ValueError(
            f"Aucun événement fourni et chargement depuis MinIO échoué : {exc}\n"
            "Fournir events= ou lancer le simulateur avec --dump-events."
        ) from exc
