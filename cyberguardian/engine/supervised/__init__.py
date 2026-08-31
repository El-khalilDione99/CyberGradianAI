"""
engine/supervised/__init__.py
──────────────────────────────
Couche 3 du moteur de scoring — Modèle supervisé XGBoost (IA-6).

Exports publics :
  XGBoostDetector : predict() + SHAP top-3 + rechargement à chaud
  build_dataset   : dataset supervisé (toutes classes, scale_pos_weight)
  train           : entraînement local ou via SageMaker Training Job
  evaluate        : AUC-PR, rappel@1%FPR, SHAP importance, rapport S3
"""

from engine.supervised.detector import XGBoostDetector
from engine.supervised.dataset  import build_dataset
from engine.supervised.train    import train
from engine.supervised.evaluate import evaluate

__all__ = ["XGBoostDetector", "build_dataset", "train", "evaluate"]
