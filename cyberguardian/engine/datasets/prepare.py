"""
engine/datasets/prepare.py
──────────────────────────
Transforme les features point-in-time en DATASETS PRÊTS À ENTRAÎNER,
un par modèle, stockés et versionnés sur l'ObjectStore.

  prepare_anomaly()     → cg-datasets/anomaly/<v>/
      X_train.parquet   légitimes purs (type_scenario == NORMAL & label == 0)
      X_test.parquet    20% légit purs + légit atypiques + fraudes
      y_test.parquet    colonne `label` (0/1)
      scaler.pkl        RobustScaler ajusté sur X_train
      meta.json         features, split, stats EDA, seed, git sha, manifest brut

  prepare_supervised()  → cg-datasets/supervised/<v>/
      X_train / y_train / X_test / y_test .parquet   toutes classes
      meta.json         + scale_pos_weight

Split stratifié PAR ABONNÉ : un même abonné n'apparaît jamais dans train ET
test (sinon évaluation trop optimiste — le modèle « reconnaît » l'abonné).
"""

from __future__ import annotations

import json
import logging
import random
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from sklearn.preprocessing import RobustScaler

from interfaces.store import get_object_store, BUCKET_DATASETS
from engine.datasets._io import save_parquet, git_sha
from engine.datasets.raw import load_raw, RawDump
from engine.datasets.pit_features import replay_features, META_COLS

from engine.anomaly.dataset import FEATURE_NAMES as ANOMALY_FEATURES
from engine.supervised.dataset import (
    XGB_FEATURE_NAMES as SUPERVISED_FEATURES,
    INF_HOURS_SUBSTITUTE,
)

logger = logging.getLogger(__name__)

BUCKET      = BUCKET_DATASETS
TRAIN_RATIO = 0.80


# ════════════════════════════════════════════════════════════
#  Helpers communs
# ════════════════════════════════════════════════════════════

def _resolve_features(
    raw: RawDump | None,
    feat_df: pd.DataFrame | None,
    raw_version: str,
    store,
) -> tuple[RawDump, pd.DataFrame]:
    """Charge le dump + rejoue les features si l'appelant ne les fournit pas."""
    if raw is None:
        raw = load_raw(raw_version, store)
    if feat_df is None:
        feat_df = replay_features(raw.events, raw.profiles_init)
    return raw, feat_df


def _booleans_to_int(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.columns:
        if df[col].dtype == bool:
            df[col] = df[col].astype("int8")
    return df


def _split_by_subscriber(
    df: pd.DataFrame, train_ratio: float, seed: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """80/20 par abonné (aucun abonné partagé train/test)."""
    ids = sorted(df["_id_compte"].unique().tolist())
    rng = random.Random(seed)
    rng.shuffle(ids)
    cut = int(len(ids) * train_ratio)
    train_ids = set(ids[:cut])
    mask = df["_id_compte"].isin(train_ids)
    return df[mask].copy(), df[~mask].copy()


def _stratified_split_by_subscriber(
    df: pd.DataFrame, train_ratio: float, seed: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    80/20 par abonné ET par classe : on répartit séparément les abonnés
    « frauduleux » et « légitimes » pour garantir des fraudes dans chaque split.
    """
    fraud_ids = sorted(df.loc[df["_label"] == 1, "_id_compte"].unique().tolist())
    all_ids   = set(df["_id_compte"].unique().tolist())
    legit_ids = sorted(all_ids - set(fraud_ids))

    rng = random.Random(seed)
    rng.shuffle(fraud_ids)
    rng.shuffle(legit_ids)

    cut_f = max(1, int(len(fraud_ids) * train_ratio))
    cut_l = max(1, int(len(legit_ids) * train_ratio))
    train_ids = set(fraud_ids[:cut_f]) | set(legit_ids[:cut_l])

    mask = df["_id_compte"].isin(train_ids)
    return df[mask].copy(), df[~mask].copy()


def _split_chronological(
    df: pd.DataFrame, train_ratio: float, ts_col: str = "_horodatage",
) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    """
    Split TEMPOREL : les `train_ratio` premières transactions (triées par
    horodatage) → train, le reste (la période la plus récente) → test.

    Contrairement au split par abonné, un même abonné apparaît normalement
    des DEUX côtés — c'est volontaire et représentatif du déploiement réel
    (la base d'abonnés ne se renouvelle pas entre deux périodes ; on entraîne
    sur le passé, on score le futur, pour les mêmes clients). Retourne aussi
    l'horodatage de coupure (str), utile pour l'audit dans meta.json.
    """
    ordered = df.sort_values(ts_col).reset_index(drop=True)
    cut = int(len(ordered) * train_ratio)
    cutoff_ts = str(ordered.iloc[min(cut, len(ordered) - 1)][ts_col])
    return ordered.iloc[:cut].copy(), ordered.iloc[cut:].copy(), cutoff_ts


def _describe_json(X: pd.DataFrame) -> dict:
    """Stats descriptives (moyenne, std, quartiles…) sérialisables — pour l'EDA."""
    return json.loads(X.describe().round(6).to_json())


def _write_meta(store, kind: str, version: str, meta: dict, promote: bool) -> None:
    store.save_json(BUCKET, f"{kind}/{version}/meta.json", meta)
    if promote:
        store.save_json(BUCKET, f"{kind}/latest.json", {"version": version})


def _split_info(train_df: pd.DataFrame, test_df: pd.DataFrame) -> tuple[dict, int]:
    """Sidecar d'audit du split + nb d'abonnés partagés (doit être 0)."""
    train_subs = sorted(train_df["_id_compte"].unique().tolist())
    test_subs  = sorted(test_df["_id_compte"].unique().tolist())
    overlap    = sorted(set(train_subs) & set(test_subs))
    sidecar = {"train_subscribers": train_subs, "test_subscribers": test_subs,
               "overlap": overlap}
    return sidecar, len(overlap)


def _feature_matrix(df: pd.DataFrame, feature_names: list[str]) -> pd.DataFrame:
    """Sous-ensemble de colonnes → float32, sentinelle sur hours_since_sim_swap."""
    X = df.reindex(columns=feature_names).copy()
    if "hours_since_sim_swap" in X.columns:
        X["hours_since_sim_swap"] = (
            X["hours_since_sim_swap"]
            .replace([np.inf, -np.inf], INF_HOURS_SUBSTITUTE)
            .fillna(INF_HOURS_SUBSTITUTE)
        )
    X = X.astype("float32")
    if X.isna().any().any():
        bad = X.columns[X.isna().any()].tolist()
        raise ValueError(f"NaN dans les features après préparation : {bad}")
    return X


# ════════════════════════════════════════════════════════════
#  Couche 2 — Isolation Forest (non supervisé)
# ════════════════════════════════════════════════════════════

def prepare_anomaly(
    raw_version: str = "latest",
    *,
    raw: RawDump | None = None,
    feat_df: pd.DataFrame | None = None,
    store=None,
    train_ratio: float = TRAIN_RATIO,
    seed: int | None = None,
    version: str | None = None,
    promote: bool = True,
    split_strategy: str = "subscriber",
) -> str:
    """
    Construit le dataset d'anomalie et l'écrit dans cg-datasets/anomaly/<v>/.

    split_strategy :
      "subscriber"     (défaut) — split par abonné (§ docstring module).
      "chronological"  — train = légitimes purs de la période ancienne,
                          test = TOUTES les classes de la période récente.
                          Un abonné peut apparaître des deux côtés (voulu :
                          simule "j'entraîne sur le passé, je score le futur,
                          mêmes clients").

    Retourne la version.
    """
    store   = store or get_object_store()
    raw, feat_df = _resolve_features(raw, feat_df, raw_version, store)
    seed    = seed if seed is not None else int(raw.manifest.get("seed", 42))
    version = version or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    df = _booleans_to_int(feat_df)
    cutoff_ts = None

    if split_strategy == "chronological":
        early, late, cutoff_ts = _split_chronological(df, train_ratio)
        legit_pure = early[(early["_label"] == 0) & (early["_type_scenario"] == "NORMAL")]
        atypical   = df[(df["_label"] == 0) & (df["_type_scenario"] != "NORMAL")]   # info seulement
        fraud      = df[df["_label"] == 1]                                          # info seulement

        if len(legit_pure) < 100:
            raise ValueError(f"Trop peu de légitimes purs ({len(legit_pure)}) pour entraîner l'IF.")

        train_df = legit_pure
        test_df  = late   # toutes classes de la période récente, pas seulement légitime
        split_sidecar = {"method": "chronological", "cutoff_horodatage": cutoff_ts,
                          "n_train_rows": int(len(train_df)), "n_test_rows": int(len(test_df))}
        n_overlap = len(set(train_df["_id_compte"]) & set(test_df["_id_compte"]))  # informatif, pas une erreur
    else:
        # ── Sous-populations ─────────────────────────────────────
        legit_pure = df[(df["_label"] == 0) & (df["_type_scenario"] == "NORMAL")]
        atypical   = df[(df["_label"] == 0) & (df["_type_scenario"] != "NORMAL")]
        fraud      = df[df["_label"] == 1]

        if len(legit_pure) < 100:
            raise ValueError(f"Trop peu de légitimes purs ({len(legit_pure)}) pour entraîner l'IF.")

        # ── Split par abonné sur les légitimes purs ──────────────
        train_df, test_legit_df = _split_by_subscriber(legit_pure, train_ratio, seed)

        # ── Jeu de test = légit purs (20%) + atypiques + fraudes ──
        test_df = pd.concat([test_legit_df, atypical, fraud], ignore_index=True)
        test_df = test_df.sample(frac=1, random_state=seed).reset_index(drop=True)

        split_sidecar, n_overlap = _split_info(train_df, test_df)

    X_train = _feature_matrix(train_df, ANOMALY_FEATURES)
    X_test  = _feature_matrix(test_df,  ANOMALY_FEATURES)
    y_test  = test_df["_label"].astype("int8")

    # ── Normalisation : RobustScaler ajusté sur le train ─────
    scaler = RobustScaler().fit(X_train.values)

    # ── Écriture ─────────────────────────────────────────────
    pfx = f"anomaly/{version}"
    save_parquet(store, BUCKET, f"{pfx}/X_train.parquet", X_train)
    save_parquet(store, BUCKET, f"{pfx}/X_test.parquet",  X_test)
    save_parquet(store, BUCKET, f"{pfx}/y_test.parquet",  y_test.to_frame("label"))
    store.save_model(BUCKET, f"{pfx}/scaler.pkl", scaler)
    store.save_json(BUCKET, f"{pfx}/split.json", split_sidecar)

    meta = {
        "kind":              "anomaly",
        "version":           version,
        "raw_version":       raw.version,
        "point_in_time":     True,
        "feature_names":     list(ANOMALY_FEATURES),
        "n_train":           int(len(X_train)),
        "n_test":            int(len(X_test)),
        "n_fraud_test":      int(y_test.sum()),
        "n_atypical_test":   int(len(atypical)),
        "fraud_rate_test":   round(float(y_test.mean()), 4) if len(y_test) else 0.0,
        "train_ratio":       train_ratio,
        "seed":              seed,
        "scaler":            "RobustScaler",
        "split_strategy":    split_strategy,
        "cutoff_horodatage": cutoff_ts,   # None si split_strategy == "subscriber"
        # Recouvrement d'abonnés train/test :
        #  - split "subscriber" : attendu > 0 quand même (un abonné qui a aussi
        #    fraudé a ses tx NORMALES potentiellement en train et ses fraudes en
        #    test) — sans impact, l'IF n'apprend que le comportement légitime.
        #  - split "chronological" : recouvrement NORMAL et voulu (même base
        #    d'abonnés avant/après la coupure, comme en production).
        "n_subscriber_overlap": n_overlap,
        "class_balance": {
            "legit_pure": int(len(legit_pure)),
            "atypical":   int(len(atypical)),
            "fraud":      int(len(fraud)),
        },
        "eda": {"train_describe": _describe_json(X_train)},
        "raw_manifest":      raw.manifest,
        "git_sha":           git_sha(),
        "created_at":        datetime.now(timezone.utc).isoformat(),
    }
    _write_meta(store, "anomaly", version, meta, promote)

    logger.info(
        "prepare_anomaly %s — train=%d (légit purs) | test=%d (fraudes=%d, atypiques=%d)",
        version, len(X_train), len(X_test), int(y_test.sum()), len(atypical),
    )
    return version


# ════════════════════════════════════════════════════════════
#  Couche 3 — XGBoost (supervisé)
# ════════════════════════════════════════════════════════════

def prepare_supervised(
    raw_version: str = "latest",
    *,
    raw: RawDump | None = None,
    feat_df: pd.DataFrame | None = None,
    store=None,
    train_ratio: float = TRAIN_RATIO,
    seed: int | None = None,
    version: str | None = None,
    promote: bool = True,
    split_strategy: str = "subscriber",
) -> str:
    """
    Construit le dataset supervisé et l'écrit dans cg-datasets/supervised/<v>/.

    split_strategy :
      "subscriber"     (défaut) — split par abonné, aucun recouvrement.
      "chronological"  — train = période ancienne, test = période récente
                          (toutes classes des deux côtés). Un abonné peut
                          apparaître des deux côtés — voulu, cf. docstring
                          de `_split_chronological`.

    Retourne la version.
    """
    store   = store or get_object_store()
    raw, feat_df = _resolve_features(raw, feat_df, raw_version, store)
    seed    = seed if seed is not None else int(raw.manifest.get("seed", 42))
    version = version or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    df = _booleans_to_int(feat_df)
    cutoff_ts = None

    n_fraud = int((df["_label"] == 1).sum())
    if n_fraud == 0:
        raise ValueError("Aucune fraude dans les features — XGBoost supervisé impossible.")

    if split_strategy == "chronological":
        train_df, test_df, cutoff_ts = _split_chronological(df, train_ratio)
        n_overlap = len(set(train_df["_id_compte"]) & set(test_df["_id_compte"]))  # informatif
        split_sidecar = {"method": "chronological", "cutoff_horodatage": cutoff_ts,
                          "n_train_rows": int(len(train_df)), "n_test_rows": int(len(test_df))}
        if int((test_df["_label"] == 1).sum()) == 0:
            raise ValueError(
                "Aucune fraude dans la période de test (chronological) — "
                "augmente train_ratio ou le volume de données."
            )
    else:
        train_df, test_df = _stratified_split_by_subscriber(df, train_ratio, seed)
        split_sidecar, n_overlap = _split_info(train_df, test_df)
        if n_overlap:
            raise RuntimeError(
                f"Fuite : {n_overlap} abonnés présents dans train ET test "
                f"(split stratifié par abonné cassé)."
            )

    X_train = _feature_matrix(train_df, SUPERVISED_FEATURES)
    X_test  = _feature_matrix(test_df,  SUPERVISED_FEATURES)
    y_train = train_df["_label"].astype("int8")
    y_test  = test_df["_label"].astype("int8")

    n_fraud_train = int(y_train.sum())
    n_legit_train = int((y_train == 0).sum())
    scale_pos_weight = (n_legit_train / n_fraud_train) if n_fraud_train else 1.0

    pfx = f"supervised/{version}"
    save_parquet(store, BUCKET, f"{pfx}/X_train.parquet", X_train)
    save_parquet(store, BUCKET, f"{pfx}/X_test.parquet",  X_test)
    save_parquet(store, BUCKET, f"{pfx}/y_train.parquet", y_train.to_frame("label"))
    save_parquet(store, BUCKET, f"{pfx}/y_test.parquet",  y_test.to_frame("label"))
    store.save_json(BUCKET, f"{pfx}/split.json", split_sidecar)

    meta = {
        "kind":              "supervised",
        "version":           version,
        "raw_version":       raw.version,
        "point_in_time":     True,
        "feature_names":     list(SUPERVISED_FEATURES),
        "n_train":           int(len(X_train)),
        "n_test":            int(len(X_test)),
        "n_fraud_train":     n_fraud_train,
        "n_fraud_test":      int(y_test.sum()),
        "fraud_rate_train":  round(float(y_train.mean()), 4) if len(y_train) else 0.0,
        "fraud_rate_test":   round(float(y_test.mean()), 4) if len(y_test) else 0.0,
        "scale_pos_weight":  round(scale_pos_weight, 4),
        "train_ratio":       train_ratio,
        "seed":              seed,
        "split_strategy":    split_strategy,
        "cutoff_horodatage": cutoff_ts,   # None si split_strategy == "subscriber"
        # "subscriber" : garanti 0 (sinon RuntimeError plus haut).
        # "chronological" : recouvrement normal et voulu (même base d'abonnés).
        "n_subscriber_overlap": n_overlap,
        "eda": {
            "train_describe": _describe_json(X_train),
            "fraud_vs_legit_mean": json.loads(
                X_train.assign(_label=y_train.values)
                       .groupby("_label").mean().round(4).to_json()
            ),
        },
        "raw_manifest":      raw.manifest,
        "git_sha":           git_sha(),
        "created_at":        datetime.now(timezone.utc).isoformat(),
    }
    _write_meta(store, "supervised", version, meta, promote)

    logger.info(
        "prepare_supervised %s — train=%d (fraudes=%d, spw=%.1f) | test=%d (fraudes=%d)",
        version, len(X_train), n_fraud_train, scale_pos_weight,
        len(X_test), int(y_test.sum()),
    )
    return version
