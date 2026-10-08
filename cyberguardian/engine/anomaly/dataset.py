"""
engine/anomaly/dataset.py
──────────────────────────
Construction du dataset d'entraînement pour l'Isolation Forest (IA-5).

Flux :
  1. Charger tous les événements (transactions + sim-events + otp-events)
  2. Reconstruire les profils par REPLAY CHRONOLOGIQUE STRICT — chaque
     transaction est scorée avec le profil tel qu'il existait AVANT elle,
     jamais avec un état final incluant des événements futurs.
  3. Appliquer le double filtre :
       - type_scenario == "NORMAL"   (pur trafic normal, pas de scénario scripté)
       - label_fraude  == 0          (redondant mais défensif)
  4. Reconstruire les features via compute_features(event, profile)
  5. Découper PAR ABONNÉ en train / validation / test (70/10/20, seed=42) :
       - train      : transactions NORMAL des abonnés train (aucune fraude)
       - validation : toutes les transactions des abonnés validation
                      → sert UNIQUEMENT au réglage des hyperparamètres
       - test       : toutes les transactions des abonnés test
                      → sert UNIQUEMENT à mesurer les performances finales
     Un abonné n'apparaît que dans une seule partie. Les fraudes/atypiques
     des abonnés train sont écartées (l'IF a déjà vu leur comportement).

Retourne :
  DatasetResult avec X_train, X_val, y_val, X_test, y_test, feature_names, meta
"""

from __future__ import annotations

import copy
import json
import logging
import os
import random
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import numpy as np

from engine.rules.features import compute_features
from engine.features.updater import apply_transaction, apply_sim_event, apply_otp_event
from interfaces.store import get_object_store, BUCKET_DATASETS

logger = logging.getLogger(__name__)

# ── Features retenues pour l'entraînement ────────────────────
# On exclut les features instables sur les nouveaux comptes
# (hours_since_sim_swap = inf, nb_beneficiaires_1h = approximation).
# On exclut aussi montant_courant (valeur brute, redondant avec amount_ratio).
FEATURE_NAMES: list[str] = [
    "nb_tx_1h",
    "new_device",
    "amount_ratio",
    "nb_tx_7j",
    "is_roaming",
    "ecart_type_montant",
    "zscore_montant",
    "hours_since_sim_swap",
    "otp_count_1h",
    "new_beneficiary",
    "nb_beneficiaires_1h",
    "nb_swaps_30j",
    "nb_tx_24h",
    "nb_otp_24h",
    "nb_tx_depuis_swap",
    "montant_cumule_depuis_swap",
]

# hours_since_sim_swap est plafonné à 2160h (90 j) dans compute_features,
# il n'y a donc plus de valeur inf qui perturberait la normalisation.

SEED = int(os.getenv("SEED", "42"))
TRAIN_RATIO = 0.70
VAL_RATIO   = 0.10   # le reste (20 %) va au test

# Clé S3 du dump JSON des événements (produit par le simulateur)
EVENTS_DUMP_KEY = "datasets/transactions_dump.json"


# ════════════════════════════════════════════════════════════
#  Résultat
# ════════════════════════════════════════════════════════════

@dataclass
class DatasetResult:
    X_train:       np.ndarray
    X_test:        np.ndarray
    y_test:        np.ndarray
    feature_names: list[str]
    train_subscriber_ids: np.ndarray = field(default_factory=lambda: np.empty(0, dtype=object))
    nb_transactions_test: np.ndarray = field(default_factory=lambda: np.empty(0, dtype=np.int32))
    test_type_scenario: np.ndarray = field(default_factory=lambda: np.empty(0, dtype=object))
    meta: dict[str, Any] = field(default_factory=dict)
    X_val: np.ndarray = field(default_factory=lambda: np.empty((0, len(FEATURE_NAMES)), dtype=np.float32))
    y_val: np.ndarray = field(default_factory=lambda: np.empty(0, dtype=np.int8))
    val_subscriber_ids:  np.ndarray = field(default_factory=lambda: np.empty(0, dtype=object))
    test_subscriber_ids: np.ndarray = field(default_factory=lambda: np.empty(0, dtype=object))

    @property
    def n_train(self) -> int:
        return len(self.X_train)

    @property
    def n_val(self) -> int:
        return len(self.X_val)

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

    Les profils sont reconstruits par REPLAY CHRONOLOGIQUE STRICT :
    pour chaque transaction, le profil utilisé pour calculer les features
    ne contient QUE les événements survenus AVANT cette transaction —
    jamais d'information future (même logique que le scoring en production).

    Paramètres
    ----------
    events : liste d'événements (transactions + sim-events + otp-events).
             Si None, tente de charger depuis MinIO (EVENTS_DUMP_KEY) puis
             lève ValueError.
    store  : ObjectStore injecté pour les tests (None = auto-détection).
    profiles_override : état de départ des profils AVANT le replay (optionnel).
             Si None, chaque profil part de zéro (dict vide) et se construit
             uniquement par replay des événements fournis.

    Retourne
    --------
    DatasetResult avec train (légitimes purs) et test (légitimes + fraudes).
    """
    # ── 1. Charger les événements ─────────────────────────────
    if events is None:
        events = _load_events_from_store(store)

    logger.info("Dataset — %d événements bruts chargés", len(events))

    # ── 2. Reconstruction chronologique des profils ───────────
    # On ne charge plus l'état final Redis : chaque profil est reconstruit
    # par replay strict, exactement comme en production temps réel.
    profiles_live: dict[str, dict] = (
        # copie PROFONDE : les listes du profil (devices, bénéficiaires…) sont
        # modifiées pendant le replay et ne doivent pas altérer celles de l'appelant
        copy.deepcopy(profiles_override)
        if profiles_override is not None
        else {}
    )

    rows_normal: list[dict]   = []   # type_scenario=NORMAL, label=0 → train + test légitime
    rows_fraud:  list[dict]   = []   # label=1 → test fraude
    rows_atypical: list[dict] = []   # légitime atypique (scénarios non-NORMAL) → test fp

    events_sorted = sorted(events, key=lambda e: e.get("horodatage", ""))

    for ev in events_sorted:
        id_compte = ev.get("id_compte", "")
        if not id_compte:
            continue
        if id_compte not in profiles_live:
            profiles_live[id_compte] = {}

        stream = ev.get("stream", "")
        is_tx  = stream == "transactions" or "id_transaction" in ev
        is_sim = stream == "sim-events"
        is_otp = stream == "otp-events"

        if is_tx:
            _process_event(ev, profiles_live, rows_normal, rows_fraud, rows_atypical)
        elif is_sim:
            apply_sim_event(profiles_live[id_compte], ev)
        elif is_otp:
            apply_otp_event(profiles_live[id_compte], ev)

    profiles = profiles_live  # pour compatibilité avec le reste du code (meta n_profiles, etc.)

    logger.info(
        "Dataset — normal=%d, fraud=%d, atypical=%d, profils reconstruits=%d",
        len(rows_normal), len(rows_fraud), len(rows_atypical), len(profiles),
    )

    if len(rows_normal) < 10:
        raise ValueError(
            f"Pas assez d'événements normaux ({len(rows_normal)}) pour entraîner. "
            "Lance d'abord le simulateur."
        )

    # ── 4. Découpage par abonné train / validation / test ─────
    all_rows = rows_normal + rows_atypical + rows_fraud
    train_ids, val_ids, test_ids = _split_subscribers(
        {r["_id_compte"] for r in all_rows}, TRAIN_RATIO, VAL_RATIO, SEED
    )

    # Transactions sans aucun historique (1ʳᵉ transaction du compte) : en
    # production le détecteur leur attribue 0 sans consulter l'IF, et leurs
    # features sont dégénérées (référence de montant = 1 FCFA → amount_ratio
    # énorme). On ne les utilise donc ni pour apprendre ni pour régler.
    # Elles restent dans le test ; evaluate() leur applique la même règle.
    has_history = lambda r: r["_nb_tx"] > 0

    # Train : uniquement le trafic légitime NORMAL des abonnés train.
    train_rows = [r for r in rows_normal if r["_id_compte"] in train_ids and has_history(r)]
    # Validation / test : toutes les transactions de leurs abonnés.
    val_rows   = [r for r in all_rows if r["_id_compte"] in val_ids and has_history(r)]
    test_rows  = [r for r in all_rows if r["_id_compte"] in test_ids]
    n_sans_historique_exclus = (
        sum(1 for r in rows_normal if r["_id_compte"] in train_ids and not has_history(r))
        + sum(1 for r in all_rows if r["_id_compte"] in val_ids and not has_history(r))
    )

    # Les fraudes/atypiques des abonnés train ne sont utilisées nulle part :
    # l'IF a déjà appris le comportement normal de ces abonnés.
    n_atypical_excluded = sum(1 for r in rows_atypical if r["_id_compte"] in train_ids)
    n_fraud_excluded    = sum(1 for r in rows_fraud    if r["_id_compte"] in train_ids)

    # Mélanger val/test pour éviter que les fraudes soient groupées
    rng = random.Random(SEED)
    rng.shuffle(val_rows)
    rng.shuffle(test_rows)

    # ── 5. Convertir en numpy ─────────────────────────────────
    X_train_np = _rows_to_matrix(train_rows)
    X_val_np   = _rows_to_matrix(val_rows)
    X_test_np  = _rows_to_matrix(test_rows)
    y_val_np   = np.array([r["_label_fraude"] for r in val_rows],  dtype=np.int8)
    y_test_np  = np.array([r["_label_fraude"] for r in test_rows], dtype=np.int8)

    meta = {
        "n_events_bruts":   len(events),
        "n_profiles":       len(profiles),
        "n_subscribers_train": len(train_ids),
        "n_subscribers_val":   len(val_ids),
        "n_subscribers_test":  len(test_ids),
        "n_subscribers_overlap": len((train_ids & val_ids) | (train_ids & test_ids) | (val_ids & test_ids)),
        "n_atypical_excluded_overlap": n_atypical_excluded,
        "n_fraud_excluded_overlap":    n_fraud_excluded,
        "n_sans_historique_exclus":    n_sans_historique_exclus,
        "n_train":          len(X_train_np),
        "n_val":            len(X_val_np),
        "n_fraud_val":      int(y_val_np.sum()),
        "n_test":           len(X_test_np),
        "n_fraud_test":     int(y_test_np.sum()),
        "n_atypical_test":  sum(1 for r in test_rows
                                if r["_label_fraude"] == 0 and r["_type_scenario"] != "NORMAL"),
        "fraud_rate_val":   round(float(y_val_np.mean()), 4) if len(y_val_np) else 0.0,
        "fraud_rate_test":  round(float(y_test_np.mean()), 4) if len(y_test_np) else 0.0,
        "split_ratios":     {"train": TRAIN_RATIO, "val": VAL_RATIO,
                             "test": round(1 - TRAIN_RATIO - VAL_RATIO, 2)},
        "feature_names":    FEATURE_NAMES,
        "seed":             SEED,
        "built_at":         datetime.now(timezone.utc).isoformat(),
        "profile_reconstruction": "replay_chronologique_strict",
    }
    logger.info(
        "Dataset final — train=%d | val=%d (fraudes=%d) | test=%d (fraudes=%d)",
        len(X_train_np), len(X_val_np), int(y_val_np.sum()),
        len(X_test_np), int(y_test_np.sum()),
    )
    return DatasetResult(
        X_train=X_train_np,
        X_test=X_test_np,
        y_test=y_test_np,
        feature_names=FEATURE_NAMES,
        train_subscriber_ids=np.array(sorted(train_ids), dtype=object),
        nb_transactions_test=np.array([r["_nb_tx"] for r in test_rows], dtype=np.int32),
        test_type_scenario=np.array([r["_type_scenario"] for r in test_rows], dtype=object),
        meta=meta,
        X_val=X_val_np,
        y_val=y_val_np,
        val_subscriber_ids=np.array(sorted(val_ids), dtype=object),
        test_subscriber_ids=np.array(sorted(test_ids), dtype=object),
    )


# ════════════════════════════════════════════════════════════
#  Helpers internes
# ════════════════════════════════════════════════════════════

def _process_event(
    ev: dict[str, Any],
    profiles_live: dict[str, dict],
    rows_normal: list,
    rows_fraud: list,
    rows_atypical: list,
) -> None:
    """
    Calcule les features d'une transaction avec le profil AVANT cette
    transaction, classe la ligne dans le bon bucket, PUIS met à jour
    le profil avec cette transaction (pour les événements suivants).
    """
    id_compte     = ev.get("id_compte", "")
    label_fraude  = int(ev.get("label_fraude", 0))
    type_scenario = ev.get("type_scenario", "NORMAL")

    profile = profiles_live.get(id_compte, {})

    try:
        features = compute_features(ev, profile)  # profil AVANT cette transaction
    except Exception as exc:
        logger.debug("Erreur compute_features pour %s : %s", id_compte[:12], exc)
        return

    row = {f: features.get(f, 0.0) for f in FEATURE_NAMES}
    row["_id_compte"]     = id_compte
    row["_label_fraude"]  = label_fraude
    row["_type_scenario"] = type_scenario
    row["_nb_tx"]         = int(profile.get("nb_transactions", 0))

    # Double filtre entraînement : NORMAL ET label=0
    if label_fraude == 0 and type_scenario == "NORMAL":
        rows_normal.append(row)
    elif label_fraude == 1:
        rows_fraud.append(row)
    else:
        # Légitime atypique (SWAP_LEGITIME, NOUVEAU_DEVICE_LEGITIME, etc.)
        rows_atypical.append(row)

    # Mettre à jour le profil APRES avoir calculé les features,
    # pour que les transactions suivantes voient cette transaction-ci.
    apply_transaction(profiles_live[id_compte], ev)


def _split_subscribers(
    subscriber_ids: set[str],
    train_ratio: float,
    val_ratio: float,
    seed: int,
) -> tuple[set[str], set[str], set[str]]:
    """Répartit les abonnés (et non les transactions) en train / val / test."""
    ids = sorted(subscriber_ids)
    random.Random(seed).shuffle(ids)
    cut_train = int(len(ids) * train_ratio)
    cut_val   = cut_train + int(len(ids) * val_ratio)
    return set(ids[:cut_train]), set(ids[cut_train:cut_val]), set(ids[cut_val:])


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
    """
    Charge tous les profils depuis Redis (clés profile:*).
    CONSERVÉ pour compatibilité/debug uniquement — n'est plus utilisé
    par build_dataset(), qui reconstruit désormais les profils par
    replay chronologique. Utile pour inspecter l'état courant de Redis
    indépendamment de la construction du dataset.
    """
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