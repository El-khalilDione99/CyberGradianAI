"""
engine/supervised/detector.py
──────────────────────────────
XGBoostDetector — Couche 3 du moteur de scoring (IA-6).

Responsabilités :
  - Charger le bundle XGBoost depuis MinIO/S3 au démarrage
  - Calculer les features via compute_features() (IA-4)
  - Prédire la probabilité de fraude → score 0-100
  - Extraire les top-3 features SHAP pour l'explicabilité
  - Rechargement à chaud (reload_model) sans redémarrage du conteneur
  - Thread-safe (RLock, même pattern que RuleEngine et AnomalyDetector)

RÈGLE : pas d'appel à SageMaker à l'inférence.
  Le modèle est chargé en mémoire depuis S3 au démarrage.
  predict() est 100% local, sans réseau.

Explicabilité SHAP :
  On utilise un TreeExplainer qui exploite la structure des arbres XGBoost
  directement, sans ré-entraînement. Résultat : valeurs SHAP pour chaque
  feature, top-3 retournés dans la réponse pour l'analyste.
  Le background dataset est un sous-ensemble de l'entraînement (100 lignes).
"""

from __future__ import annotations

import logging
import os
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import numpy as np

from engine.supervised.dataset import XGB_FEATURE_NAMES, INF_HOURS_SUBSTITUTE
from engine.rules.features import compute_features
from interfaces.store import get_object_store, BUCKET_MODELS

logger = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────
XGB_MODEL_BUCKET     = os.getenv("XGB_MODEL_BUCKET", BUCKET_MODELS)
XGB_MODEL_S3_KEY     = os.getenv("XGB_MODEL_S3_KEY", "xgboost/production/current.json")
XGB_MODEL_LOCAL_PATH = os.getenv("XGB_MODEL_PATH", "")

# Seuil de décision (is_fraud = score >= threshold)
XGB_THRESHOLD = int(os.getenv("XGB_THRESHOLD", "50"))

# Nombre de features SHAP à retourner dans la réponse
SHAP_TOP_N = int(os.getenv("SHAP_TOP_N", "3"))


# ════════════════════════════════════════════════════════════
#  Résultat de prédiction
# ════════════════════════════════════════════════════════════

@dataclass
class SupervisedResult:
    """Résultat de la Couche 3 pour une transaction."""
    score:          int                      # 0-100
    probability:    float                    # probabilité brute XGBoost [0-1]
    is_fraud:       bool                     # score >= XGB_THRESHOLD
    shap_top3:      list[dict[str, Any]]     # top-3 features par contribution SHAP
    model_version:  str
    features:       dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "score":         self.score,
            "probability":   round(self.probability, 4),
            "is_fraud":      self.is_fraud,
            "shap_top3":     self.shap_top3,
            "model_version": self.model_version,
            "features_snapshot": {
                k: self.features.get(k)
                for k in ["amount_ratio", "zscore_montant", "hours_since_sim_swap",
                          "new_device", "new_beneficiary", "is_roaming",
                          "otp_count_1h", "nb_tx_1h"]
                if k in self.features
            },
        }


# ════════════════════════════════════════════════════════════
#  Détecteur principal
# ════════════════════════════════════════════════════════════

class XGBoostDetector:
    """
    Couche 3 — Détection supervisée par XGBoost + explicabilité SHAP.

    Même pattern que RuleEngine (IA-4) et AnomalyDetector (IA-5) :
    - Injection du store pour les tests
    - Rechargement à chaud via reload_model()
    - Thread-safe (RLock)
    - status() pour /health

    Usage :
        detector = XGBoostDetector()
        result   = detector.predict(event=tx_dict, profile=profile_dict)
        print(result.score, result.shap_top3)
    """

    def __init__(self, store=None) -> None:
        self._store       = store
        self._bundle      = None    # {"model": xgb, "feature_names": [...], "version": ...}
        self._explainer   = None    # shap.TreeExplainer (lazy-init)
        self._version     = "unknown"
        self._lock        = threading.RLock()
        self._loaded_from = "none"
        self.reload_model()

    # ── Chargement ───────────────────────────────────────────

    def reload_model(self) -> str:
        """
        Recharge le bundle XGBoost depuis :
          1. Fichier local si XGB_MODEL_PATH est défini
          2. MinIO/S3 : lit production/current.json → charge le .pkl
          3. Mode dégradé (score = 0, shap_top3 = [])

        Thread-safe.
        """
        # Tentative fichier local
        if XGB_MODEL_LOCAL_PATH and os.path.isfile(XGB_MODEL_LOCAL_PATH):
            try:
                bundle, version = self._load_from_file(XGB_MODEL_LOCAL_PATH)
                with self._lock:
                    self._bundle      = bundle
                    self._explainer   = None  # reset
                    self._version     = version
                    self._loaded_from = f"file:{XGB_MODEL_LOCAL_PATH}"
                msg = f"XGBoost chargé depuis {XGB_MODEL_LOCAL_PATH} (v{version})"
                logger.info(msg)
                return msg
            except Exception as exc:
                logger.warning("Chargement fichier local échoué : %s", exc)

        # Tentative MinIO/S3
        try:
            bundle, version, source = self._load_from_store()
            with self._lock:
                self._bundle      = bundle
                self._explainer   = None  # reset à chaque reload
                self._version     = version
                self._loaded_from = source
            msg = f"XGBoost chargé depuis {source} (v{version})"
            logger.info(msg)
            return msg
        except Exception as exc:
            logger.warning(
                "Impossible de charger XGBoost : %s — mode dégradé", exc
            )

        # Mode dégradé
        with self._lock:
            self._bundle      = None
            self._explainer   = None
            self._version     = "degraded"
            self._loaded_from = "none"
        return "Mode dégradé — XGBoost non disponible (score=0)"

    def _load_from_file(self, path: str):
        import pickle
        with open(path, "rb") as f:
            bundle = pickle.load(f)
        return bundle, bundle.get("version", os.path.basename(path))

    def _load_from_store(self):
        obj_store = self._store or get_object_store()
        current   = obj_store.load_json(XGB_MODEL_BUCKET, XGB_MODEL_S3_KEY)
        model_key = current["model_key"]
        version   = current.get("version", "unknown")
        bundle    = obj_store.load_model(XGB_MODEL_BUCKET, model_key)
        return bundle, version, f"s3:{XGB_MODEL_BUCKET}/{model_key}"

    # ── Prédiction ───────────────────────────────────────────

    def predict(
        self,
        event:   dict[str, Any],
        profile: dict[str, Any],
    ) -> SupervisedResult:
        """
        Calcule le score de fraude XGBoost pour une transaction.

        Paramètres
        ----------
        event   : dict — transaction en cours (horodatage, montant, device_id…)
        profile : dict — profil abonné depuis Redis/DynamoDB

        Retourne
        --------
        SupervisedResult avec score 0-100, probabilité, top-3 SHAP.
        """
        features = compute_features(event, profile)

        with self._lock:
            bundle    = self._bundle
            version   = self._version
            explainer = self._explainer

        # Mode dégradé
        if bundle is None:
            return SupervisedResult(
                score=0, probability=0.0,
                is_fraud=False, shap_top3=[],
                model_version="degraded", features=features,
            )

        # Construire le vecteur de features
        vec = self._build_vector(features)

        # Score XGBoost
        probability = self._score_xgboost(vec, bundle)
        score       = int(round(probability * 100))
        score       = max(0, min(100, score))

        # SHAP top-3
        if explainer is None:
            explainer = self._build_explainer(bundle)
            with self._lock:
                self._explainer = explainer

        shap_top3 = self._compute_shap_top3(vec, explainer)

        return SupervisedResult(
            score        = score,
            probability  = probability,
            is_fraud     = score >= XGB_THRESHOLD,
            shap_top3    = shap_top3,
            model_version = version,
            features     = features,
        )

    def _build_vector(self, features: dict[str, Any]) -> np.ndarray:
        """Construit le vecteur numpy depuis les features calculées."""
        vec = []
        for f in XGB_FEATURE_NAMES:
            val = features.get(f, 0.0)
            # Remplacer inf par la constante de substitution
            if f == "hours_since_sim_swap" and (
                val == float("inf") or val != val
            ):
                val = INF_HOURS_SUBSTITUTE
            vec.append(float(val) if not isinstance(val, bool) else float(int(val)))
        return np.array([vec], dtype=np.float32)

    def _score_xgboost(
        self, vec: np.ndarray, bundle: dict
    ) -> float:
        """
        Calcule la probabilité de fraude.
        Supporte les deux formats de bundle :
          - xgb.XGBClassifier (mode local)
          - xgb.Booster       (mode SageMaker)
        """
        import xgboost as xgb  # type: ignore

        model = bundle["model"]

        if isinstance(model, xgb.XGBClassifier):
            return float(model.predict_proba(vec)[0, 1])

        # xgb.Booster (SageMaker)
        feature_names = bundle.get("feature_names", XGB_FEATURE_NAMES)
        dmat = xgb.DMatrix(vec, feature_names=feature_names)
        return float(model.predict(dmat)[0])

    def _build_explainer(self, bundle: dict):
        """
        Construit le TreeExplainer SHAP.
        Lazy-init : créé au premier appel predict() après un reload.
        ThreadSafe : l'appelant tient le _lock avant d'écrire self._explainer.
        """
        try:
            import shap  # type: ignore
            model = bundle["model"]
            return shap.TreeExplainer(model)
        except Exception as exc:
            logger.warning("Impossible de créer le TreeExplainer SHAP : %s", exc)
            return None

    def _compute_shap_top3(
        self,
        vec:       np.ndarray,
        explainer,
    ) -> list[dict[str, Any]]:
        """
        Calcule les valeurs SHAP et retourne les top-N features
        par valeur absolue de contribution.

        Format de retour :
          [{"feature": "amount_ratio", "value": 9.25, "shap": 0.42}, ...]
        """
        if explainer is None:
            return []

        try:
            import shap  # type: ignore

            shap_values = explainer.shap_values(vec)

            # Pour les classifieurs binaires, shap_values peut être une liste
            # [shap_class0, shap_class1] — on prend la classe 1 (fraude)
            if isinstance(shap_values, list):
                sv = shap_values[1][0] if len(shap_values) > 1 else shap_values[0][0]
            else:
                sv = shap_values[0]

            # Trier par valeur absolue décroissante
            indices = np.argsort(np.abs(sv))[::-1][:SHAP_TOP_N]

            result = []
            for idx in indices:
                fname   = XGB_FEATURE_NAMES[idx]
                fval    = float(vec[0, idx])
                shapval = float(sv[idx])
                result.append({
                    "feature":    fname,
                    "value":      round(fval, 4),
                    "shap":       round(shapval, 4),
                    "direction":  "fraud" if shapval > 0 else "legitimate",
                })
            return result

        except Exception as exc:
            logger.debug("Calcul SHAP échoué : %s", exc)
            return []

    # ── Introspection ─────────────────────────────────────────

    @property
    def is_ready(self) -> bool:
        with self._lock:
            return self._bundle is not None

    @property
    def version(self) -> str:
        with self._lock:
            return self._version

    def status(self) -> dict[str, Any]:
        """Résumé pour /health."""
        with self._lock:
            return {
                "ready":        self._bundle is not None,
                "version":      self._version,
                "loaded_from":  self._loaded_from,
                "threshold":    XGB_THRESHOLD,
                "shap_top_n":   SHAP_TOP_N,
                "n_features":   len(XGB_FEATURE_NAMES),
            }
