"""
engine/supervised/dataset.py
─────────────────────────────
Construction du dataset supervisé pour XGBoost (IA-6).

Différence fondamentale avec IA-5 (Isolation Forest) :
  - IA-5 : entraînement sur LÉGITIMES PURS uniquement (non supervisé)
  - IA-6 : entraînement sur TOUTES LES CLASSES (fraudes + légitimes)
           Le double filtre NORMAL/label=0 de IA-5 est SUPPRIMÉ ici.

Flux :
  1. Charger les événements (fournis ou depuis MinIO dump)
  2. Rejouer chronologiquement les 3 flux en partant des profils initiaux
     (profiles_override = profils écrits dans Redis au démarrage) et calculer
     compute_features() pour chaque transaction — EXACTEMENT le calcul fait
     en production par XGBoostDetector.predict() (parité entraînement / inférence)
  3. Labels : label du simulateur, remplacé par le label analyste (écran W-4)
     quand il existe (labels_override)
  4. Produire X (features), y (labels 0/1), et scale_pos_weight
  5. Split stratifié 80/20 par abonné (même logique que IA-5)
  6. Export Parquet versionné vers S3 pour SageMaker (export_dataset_parquet)

scale_pos_weight = n_légitimes / n_fraudes
  Compense le déséquilibre de classes sans sur-échantillonnage.
  XGBoost pondère chaque fraude comme si elle comptait N fois plus.

Retourne :
  SupervisedDatasetResult avec X_train, y_train, X_test, y_test,
  scale_pos_weight, feature_names, metadata
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
    groups_train:        np.ndarray = field(default_factory=lambda: np.empty(0, dtype=object))  # abonné de chaque ligne train (CV groupée)
    train_subscriber_ids: np.ndarray = field(default_factory=lambda: np.empty(0, dtype=object))
    test_subscriber_ids:  np.ndarray = field(default_factory=lambda: np.empty(0, dtype=object))
    test_type_scenario:   np.ndarray = field(default_factory=lambda: np.empty(0, dtype=object))

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
    labels_override: dict[str, int] | None = None,
) -> SupervisedDatasetResult:
    """
    Construit le dataset supervisé pour XGBoost.

    Paramètres
    ----------
    events            : événements transactions. Si None, charge depuis MinIO.
    store             : ObjectStore injecté (tests). None = auto.
    profiles_override : dict {id_compte → profil} : état des profils AVANT le
                        replay. En production Redis est initialisé avec
                        simulator.profiles.build_initial_profile() : passer ces
                        profils ici garantit la parité entraînement / inférence.
    export_parquet    : si True, exporte train/test en Parquet dans S3
                        (requis pour SageMaker Training Jobs).
    labels_override   : {id_transaction → label} saisis par les analystes
                        (écran W-4, table labels). Remplacent le label simulateur.

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

    # ── 2. Reconstruction chronologique des profils ───────────
    # CORRECTIF : l'ancienne version chargeait les profils Redis figés
    # (état final, sans rejouer l'historique). Résultat : toutes les features
    # de fenêtre glissante (nb_tx_1h, nb_otp_24h, nb_swaps_30j, etc.)
    # étaient à 0, et hours_since_sim_swap était toujours à 9999.0 (cap).
    # Le modèle ne pouvait pas apprendre de ces features → 7 features mortes
    # sur 17, seul new_device discriminait réellement.
    #
    # On reproduit le même replay chronologique strict que anomaly/dataset.py :
    #   - chaque transaction est scorée avec le profil tel qu'il existait
    #     AVANT cette transaction (pas d'information future)
    #   - sim-events et otp-events chauffent le profil pour les suivants
    #
    # profiles_override reste disponible pour les tests unitaires.
    from engine.features.updater import (
        apply_transaction as _apply_tx,
        apply_sim_event   as _apply_sim,
        apply_otp_event   as _apply_otp,
    )

    profiles_live: dict[str, dict] = (
        # copie PROFONDE : les listes du profil (devices, bénéficiaires…) sont
        # modifiées pendant le replay et ne doivent pas altérer celles de l'appelant
        copy.deepcopy(profiles_override)
        if profiles_override is not None
        else {}
    )

    events_sorted = sorted(events, key=lambda e: e.get("horodatage", ""))

    # ── 3. Calculer les features pour TOUTES les classes ──────
    # Pas de filtre ici — on prend fraudes ET légitimes (tous scénarios).
    all_rows: list[dict] = []
    for ev in events_sorted:
        id_compte = ev.get("id_compte", "")
        if not id_compte:
            continue
        if id_compte not in profiles_live:
            profiles_live[id_compte] = {}

        stream = ev.get("stream", "")
        is_tx  = "id_transaction" in ev or stream == "transactions"
        is_sim = stream == "sim-events"
        is_otp = stream == "otp-events"

        if is_tx:
            # Scorer avec le profil AVANT cette transaction
            row = _process_event(ev, profiles_live, labels_override)
            if row is not None:
                all_rows.append(row)
            # Mettre à jour le profil APRÈS le scoring
            _apply_tx(profiles_live[id_compte], ev)
        elif is_sim:
            _apply_sim(profiles_live[id_compte], ev)
        elif is_otp:
            _apply_otp(profiles_live[id_compte], ev)

    n_profiles = len(profiles_live)
    logger.info("Supervisé — %d profils reconstruits par replay", n_profiles)

    n_fraude  = sum(1 for r in all_rows if r["_label"] == 1)
    n_legitime = sum(1 for r in all_rows if r["_label"] == 0)
    n_labels_analyste = sum(1 for r in all_rows if r["_label_source"] == "analyste")

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
    train_rows, test_rows, n_overlap = _stratified_split_by_subscriber(
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
        "n_events_bruts":        len(events),
        "n_profiles":            n_profiles,
        "profile_reconstruction": "replay_chronologique_strict",
        "n_subscribers_overlap": n_overlap,
        "n_train":               len(X_train),
        "n_test":                len(X_test),
        "n_fraud_train":         n_fraud_train,
        "n_fraud_test":          int(y_test.sum()),
        "fraud_rate_train":      round(float(y_train.mean()), 4),
        "fraud_rate_test":       round(float(y_test.mean()), 4),
        "scale_pos_weight":      round(scale_pos_weight, 2),
        "n_labels_analyste":     n_labels_analyste,
        "profils_initiaux":      profiles_override is not None,
        "feature_names":         XGB_FEATURE_NAMES,
        "seed":                  SEED,
        "built_at":              datetime.now(timezone.utc).isoformat(),
    }

    result = SupervisedDatasetResult(
        X_train          = X_train,
        y_train          = y_train,
        X_test           = X_test,
        y_test           = y_test,
        scale_pos_weight = scale_pos_weight,
        feature_names    = XGB_FEATURE_NAMES,
        meta             = meta,
        groups_train     = np.array([r["_id_compte"] for r in train_rows], dtype=object),
        train_subscriber_ids = np.array(sorted({r["_id_compte"] for r in train_rows}), dtype=object),
        test_subscriber_ids  = np.array(sorted({r["_id_compte"] for r in test_rows}), dtype=object),
        test_type_scenario   = np.array([r["_type_scenario"] for r in test_rows], dtype=object),
    )

    # ── 7. Export Parquet pour SageMaker (optionnel) ───────────
    if export_parquet:
        export_dataset_parquet(result, obj_store)

    return result


def export_dataset_parquet(
    ds: SupervisedDatasetResult,
    store,
    version: str | None = None,
) -> dict[str, str]:
    """
    Exporte train/test en Parquet dans S3 (bucket datasets), sous une clé
    versionnée : datasets/xgboost/<version>/{train,test}.parquet

    Format SageMaker XGBoost intégré : la colonne `label` est la PREMIÈRE,
    suivie des features dans l'ordre XGB_FEATURE_NAMES.

    Retourne {"version", "train_key", "test_key", "train_sha256", "test_sha256"}.
    L'empreinte SHA-256 identifie de façon unique le dataset d'origine d'un
    modèle (enregistrée dans la table models).
    """
    import hashlib
    version = version or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    prefix  = f"datasets/xgboost/{version}"
    out = {"version": version}
    for part, X, y in [("train", ds.X_train, ds.y_train), ("test", ds.X_test, ds.y_test)]:
        data = _to_parquet_bytes(X, y)
        key  = f"{prefix}/{part}.parquet"
        store.upload(BUCKET_DATASETS, key, data)
        out[f"{part}_key"]    = key
        out[f"{part}_sha256"] = hashlib.sha256(data).hexdigest()
    logger.info("Dataset Parquet exporté → %s/%s", BUCKET_DATASETS, prefix)
    return out


def read_parquet(store, key: str):
    """Relit un Parquet exporté → (X, y)."""
    import io
    import pandas as pd  # type: ignore
    df = pd.read_parquet(io.BytesIO(store.download(BUCKET_DATASETS, key)))
    return df[XGB_FEATURE_NAMES].to_numpy(dtype=np.float32), df["label"].to_numpy(dtype=np.int8)


def _to_parquet_bytes(X: np.ndarray, y: np.ndarray) -> bytes:
    import io
    import pandas as pd  # type: ignore
    df = pd.DataFrame(X, columns=XGB_FEATURE_NAMES)
    df.insert(0, "label", y.astype(int))       # label en 1re colonne (convention SageMaker)
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    return buf.getvalue()


# ════════════════════════════════════════════════════════════
#  Helpers internes
# ════════════════════════════════════════════════════════════

def _process_event(
    ev: dict[str, Any],
    profiles: dict[str, dict],
    labels_override: dict[str, int] | None = None,
) -> dict[str, Any] | None:
    """Calcule les features d'une transaction et retourne la ligne."""
    id_compte    = ev.get("id_compte", "")
    id_tx        = ev.get("id_transaction", "")
    label_fraude = int(ev.get("label_fraude", 0))
    label_source = "simulateur"
    if labels_override and id_tx in labels_override:
        label_fraude = int(labels_override[id_tx])
        label_source = "analyste"

    # Avec le replay chronologique, profiles est profiles_live et contient
    # déjà la clé id_compte (initialisée à {} avant l'appel).
    # On accepte les profils vides — compute_features retourne des valeurs
    # de repli sûres (0 pour les compteurs, 9999 pour hours_since_sim_swap).
    profile = profiles.get(id_compte, {})

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

    row["_id_compte"]     = id_compte
    row["_label"]         = label_fraude
    row["_label_source"]  = label_source
    scen = ev.get("type_scenario", "NORMAL")
    row["_type_scenario"] = getattr(scen, "value", scen)
    return row


def _stratified_split_by_subscriber(
    rows: list[dict],
    train_ratio: float,
    seed: int,
) -> tuple[list[dict], list[dict], int]:
    """
    Split 80/20 **strictement par abonné** : un abonné est entièrement
    en train OU entièrement en test — jamais dans les deux.

    Stratégie de stratification :
      1. Séparer les abonnés en deux groupes selon qu'ils ont eu des fraudes
         (fraud_subscribers) ou non (legit_subscribers).
      2. Appliquer le split 80/20 indépendamment sur chaque groupe pour
         garantir des fraudes ET des légitimes dans le test.
      3. Vérifier explicitement l'absence de chevauchement.

    Retourne
    --------
    train_rows, test_rows, n_overlap
      n_overlap est TOUJOURS 0 par construction — il est dans les retours
      uniquement pour être reporté dans meta et déclencher une alerte si
      jamais une régression introduit un bug.
    """
    # ── Grouper toutes les lignes par abonné ──────────────────
    by_subscriber: dict[str, list[dict]] = {}
    for row in rows:
        by_subscriber.setdefault(row["_id_compte"], []).append(row)

    # ── Séparer abonnés frauduleux / non-frauduleux ───────────
    fraud_subscribers = sorted([
        acc for acc, r in by_subscriber.items()
        if any(x["_label"] == 1 for x in r)
    ])
    legit_subscribers = sorted([
        acc for acc, r in by_subscriber.items()
        if all(x["_label"] == 0 for x in r)
    ])

    rng = random.Random(seed)
    rng.shuffle(fraud_subscribers)
    rng.shuffle(legit_subscribers)

    cut_fraud = max(1, int(len(fraud_subscribers) * train_ratio))
    cut_legit = max(1, int(len(legit_subscribers) * train_ratio))

    train_fraud_ids = set(fraud_subscribers[:cut_fraud])
    train_legit_ids = set(legit_subscribers[:cut_legit])
    train_ids       = train_fraud_ids | train_legit_ids

    test_fraud_ids = set(fraud_subscribers[cut_fraud:])
    test_legit_ids = set(legit_subscribers[cut_legit:])
    test_ids       = test_fraud_ids | test_legit_ids

    # ── Vérification stricte : aucun abonné dans les deux ─────
    overlap = train_ids & test_ids   # doit toujours être vide
    if overlap:
        logger.error(
            "ALERTE : %d abonnés présents en train ET en test — "
            "split compromis : %s",
            len(overlap), list(overlap)[:5],
        )

    train_rows = [r for r in rows if r["_id_compte"] in train_ids]
    test_rows  = [r for r in rows if r["_id_compte"] in test_ids]

    logger.info(
        "Split supervisé — train=%d rows (%d abonnés, %d fraudes) | "
        "test=%d rows (%d abonnés, %d fraudes) | overlap=%d",
        len(train_rows), len(train_ids),
        sum(1 for r in train_rows if r["_label"] == 1),
        len(test_rows),  len(test_ids),
        sum(1 for r in test_rows  if r["_label"] == 1),
        len(overlap),
    )

    return train_rows, test_rows, len(overlap)


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
