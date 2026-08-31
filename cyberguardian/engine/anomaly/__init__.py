"""
engine/anomaly/__init__.py
───────────────────────────
Couche 2 du moteur de scoring — Détection d'anomalies (IA-5).

Exports publics :
  AnomalyDetector : predict() + rechargement à chaud depuis MinIO/S3
  build_dataset   : construction du dataset d'entraînement depuis Redis
  train           : entraînement Isolation Forest + versionnage MinIO
  evaluate        : AUC-PR, rappel @1% FPR, distribution des scores
"""

from engine.anomaly.detector import AnomalyDetector
from engine.anomaly.dataset  import build_dataset
from engine.anomaly.train    import train
from engine.anomaly.evaluate import evaluate

__all__ = ["AnomalyDetector", "build_dataset", "train", "evaluate"]
