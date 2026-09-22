"""
engine/datasets/load.py
───────────────────────
Chargement des datasets préparés depuis l'ObjectStore.

    ds = load_prepared("anomaly")        # dernière version promue
    ds = load_prepared("supervised", "20260910_120000")

Les objets retournés exposent les mêmes attributs que les anciens
`DatasetResult` / `SupervisedDatasetResult` → `engine/*/train.py` change à peine.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np

from interfaces.store import get_object_store, BUCKET_DATASETS
from engine.datasets._io import load_parquet

BUCKET = BUCKET_DATASETS


# ════════════════════════════════════════════════════════════
#  Objets retournés
# ════════════════════════════════════════════════════════════

@dataclass
class PreparedAnomalyDataset:
    X_train:       np.ndarray
    X_test:        np.ndarray
    y_test:        np.ndarray          # 0 = légitime, 1 = fraude
    scaler:        Any                 # RobustScaler déjà ajusté sur X_train
    feature_names: list[str]
    meta:          dict[str, Any] = field(default_factory=dict)

    @property
    def n_train(self) -> int:
        return len(self.X_train)

    @property
    def n_test(self) -> int:
        return len(self.X_test)

    @property
    def fraud_rate_test(self) -> float:
        return float(self.y_test.mean()) if len(self.y_test) else 0.0


@dataclass
class PreparedSupervisedDataset:
    X_train:          np.ndarray
    y_train:          np.ndarray
    X_test:           np.ndarray
    y_test:           np.ndarray
    scale_pos_weight: float
    feature_names:    list[str]
    meta:             dict[str, Any] = field(default_factory=dict)

    @property
    def n_train(self) -> int:
        return len(self.X_train)

    @property
    def n_test(self) -> int:
        return len(self.X_test)

    @property
    def fraud_rate_train(self) -> float:
        return float(self.y_train.mean()) if len(self.y_train) else 0.0

    @property
    def fraud_rate_test(self) -> float:
        return float(self.y_test.mean()) if len(self.y_test) else 0.0


# ════════════════════════════════════════════════════════════
#  Chargement
# ════════════════════════════════════════════════════════════

def resolve_version(kind: str, version: str = "latest", store=None) -> str:
    store = store or get_object_store()
    if version == "latest":
        return store.load_json(BUCKET, f"{kind}/latest.json")["version"]
    return version


def load_prepared(
    kind: str,
    version: str = "latest",
    store=None,
) -> PreparedAnomalyDataset | PreparedSupervisedDataset:
    """
    kind : "anomaly" | "supervised"
    """
    store   = store or get_object_store()
    version = resolve_version(kind, version, store)
    pfx     = f"{kind}/{version}"

    meta = store.load_json(BUCKET, f"{pfx}/meta.json")
    fn   = meta["feature_names"]

    def _X(name: str) -> np.ndarray:
        return load_parquet(store, BUCKET, f"{pfx}/{name}.parquet")[fn].to_numpy(dtype="float32")

    def _y(name: str) -> np.ndarray:
        return load_parquet(store, BUCKET, f"{pfx}/{name}.parquet")["label"].to_numpy(dtype="int8")

    if kind == "anomaly":
        return PreparedAnomalyDataset(
            X_train      = _X("X_train"),
            X_test       = _X("X_test"),
            y_test       = _y("y_test"),
            scaler       = store.load_model(BUCKET, f"{pfx}/scaler.pkl"),
            feature_names= fn,
            meta         = meta,
        )

    if kind == "supervised":
        return PreparedSupervisedDataset(
            X_train          = _X("X_train"),
            y_train          = _y("y_train"),
            X_test           = _X("X_test"),
            y_test           = _y("y_test"),
            scale_pos_weight = float(meta["scale_pos_weight"]),
            feature_names    = fn,
            meta             = meta,
        )

    raise ValueError(f"kind inconnu : {kind!r} (attendu 'anomaly' ou 'supervised')")
