"""
engine/datasets/
────────────────
Pipeline de données CyberGuardian AI — de la simulation au dataset prêt à
entraîner, stocké et versionné sur MinIO / S3.

    build_raw_dump()      →  cg-datasets/raw/<v>/          (trafic brut figé)
            │
    replay_features()     →  features point-in-time (aucune fuite temporelle)
            │
    prepare_anomaly()     →  cg-datasets/anomaly/<v>/      (Couche 2 : IsolationForest)
    prepare_supervised()  →  cg-datasets/supervised/<v>/   (Couche 3 : XGBoost)
            │
    load_prepared(kind)   →  dataset prêt, consommé par engine/*/train.py

Les scripts d'entraînement ne régénèrent plus rien : ils appellent
`load_prepared(...)` et entraînent. Toute la préparation est ici, testable
et rejouable en CI.
"""

from engine.datasets.raw import build_raw_dump, load_raw, RawDump
from engine.datasets.pit_features import replay_features
from engine.datasets.prepare import prepare_anomaly, prepare_supervised
from engine.datasets.load import (
    load_prepared,
    PreparedAnomalyDataset,
    PreparedSupervisedDataset,
)

__all__ = [
    "build_raw_dump",
    "load_raw",
    "RawDump",
    "replay_features",
    "prepare_anomaly",
    "prepare_supervised",
    "load_prepared",
    "PreparedAnomalyDataset",
    "PreparedSupervisedDataset",
]
