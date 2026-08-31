"""
engine/anomaly/detector.py
───────────────────────────
AnomalyDetector — Couche 2 du moteur de scoring (IA-5).

Responsabilités :
  - Charger le bundle (IsolationForest + RobustScaler) depuis MinIO/S3
  - Calculer les features dérivées via compute_features() (IA-4)
  - Appliquer la formule hybride IF + z-score pour retourner un score 0-100
  - Supporter le rechargement à chaud (reload_model) sans redémarrage
  - Thread-safe (RLock, même pattern que RuleEngine)

Formule de fusion (décidée en amont) :
  score_if      = normaliser(score_isolation_forest → 0-100)
  score_couche2 = score_if
    si zscore_montant > Z_OVERRIDE_SOFT (3.0) → score = max(score_if, 75)
    si zscore_montant > Z_OVERRIDE_HARD (5.0) → score = max(score_if, 90)

  Justification : l'IF est le modèle principal (vision multi-dimensionnelle).
  Le z-score est un garde-fou interprétable sur le montant uniquement.
  On n'utilise pas le max brut pour éviter qu'un z-score élevé sur un
  compte peu actif (std Welford instable) ne déclenche à tort.
"""

from __future__ import annotations

import logging
import os
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import numpy as np

from engine.anomaly.dataset import FEATURE_NAMES
from engine.rules.features import compute_features
from interfaces.store import get_object_store, BUCKET_MODELS

logger = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────
MODEL_BUCKET     = os.getenv("MODEL_BUCKET",     BUCKET_MODELS)
MODEL_S3_KEY     = os.getenv("MODEL_S3_KEY",     "anomaly/production/current.json")
MODEL_LOCAL_PATH = os.getenv("ANOMALY_MODEL_PATH", "")

# Seuils z-score pour l'override
Z_OVERRIDE_SOFT = float(os.getenv("Z_OVERRIDE_SOFT", "3.0"))  # → plancher 75
Z_OVERRIDE_HARD = float(os.getenv("Z_OVERRIDE_HARD", "5.0"))  # → plancher 90

# Seuil minimum de transactions pour que le z-score soit fiable
# En-dessous, le std Welford est instable → on ignore l'override z-score
Z_MIN_TRANSACTIONS = int(os.getenv("Z_MIN_TRANSACTIONS", "10"))


# ════════════════════════════════════════════════════════════
#  Résultat de prédiction
# ════════════════════════════════════════════════════════════

@dataclass
class AnomalyResult:
    """Résultat de la Couche 2 pour une transaction."""
    score:          int             # 0-100, score final hybride
    score_if:       float           # score brut Isolation Forest (normalisé 0-100)
    zscore_montant: float           # z-score du montant (de compute_features)
    is_anomaly:     bool            # score >= seuil_anomalie (défaut 50)
    override_active: bool           # True si le z-score a relevé le score IF
    model_version:  str
    features:       dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "score":           self.score,
            "score_if":        round(self.score_if, 2),
            "zscore_montant":  round(self.zscore_montant, 4),
            "is_anomaly":      self.is_anomaly,
            "override_active": self.override_active,
            "model_version":   self.model_version,
            "features_snapshot": {
                k: self.features.get(k)
                for k in ["amount_ratio", "zscore_montant", "nb_tx_1h",
                          "otp_count_1h", "is_roaming", "new_device"]
            },
        }


# ════════════════════════════════════════════════════════════
#  Détecteur principal
# ════════════════════════════════════════════════════════════

class AnomalyDetector:
    """
    Couche 2 — Détection d'anomalies par Isolation Forest + z-score.

    Suit exactement le même pattern que RuleEngine (IA-4) :
    - Injection du store pour les tests
    - Rechargement à chaud via reload_model()
    - Thread-safe (RLock)
    - status() pour /health

    Usage :
        detector = AnomalyDetector()
        result   = detector.predict(event=tx_dict, profile=profile_dict)
        print(result.score, result.is_anomaly)
    """

    ANOMALY_THRESHOLD = 50  # score >= 50 → is_anomaly = True

    def __init__(self, store=None) -> None:
        self._store   = store
        self._bundle  = None          # {"model": IF, "scaler": RS, "feature_names": [...]}
        self._version = "unknown"
        self._lock    = threading.RLock()
        self._loaded_from = "none"
        self.reload_model()

    # ── Chargement / rechargement ────────────────────────────

    def reload_model(self) -> str:
        """
        Recharge le bundle (IF + scaler) depuis :
          1. Fichier local si ANOMALY_MODEL_PATH est défini
          2. MinIO/S3 : lit production/current.json → charge le .pkl pointé
          3. Mode dégradé si rien de disponible (score = 0 toujours)

        Retourne un message décrivant la source utilisée. Thread-safe.
        """
        # ── Tentative 1 : fichier local ───────────────────────
        if MODEL_LOCAL_PATH and os.path.isfile(MODEL_LOCAL_PATH):
            try:
                bundle, version = self._load_from_file(MODEL_LOCAL_PATH)
                with self._lock:
                    self._bundle      = bundle
                    self._version     = version
                    self._loaded_from = f"file:{MODEL_LOCAL_PATH}"
                msg = f"Modèle anomalie chargé depuis {MODEL_LOCAL_PATH} (v{version})"
                logger.info(msg)
                return msg
            except Exception as exc:
                logger.warning("Chargement fichier local échoué : %s", exc)

        # ── Tentative 2 : MinIO / S3 ─────────────────────────
        try:
            bundle, version, source = self._load_from_store()
            with self._lock:
                self._bundle      = bundle
                self._version     = version
                self._loaded_from = source
            msg = f"Modèle anomalie chargé depuis {source} (v{version})"
            logger.info(msg)
            return msg
        except Exception as exc:
            logger.warning(
                "Impossible de charger le modèle d'anomalie : %s — mode dégradé", exc
            )

        # ── Mode dégradé ──────────────────────────────────────
        with self._lock:
            self._bundle      = None
            self._version     = "degraded"
            self._loaded_from = "none"
        return "Mode dégradé — aucun modèle d'anomalie disponible (score=0)"

    def _load_from_file(self, path: str):
        import pickle
        with open(path, "rb") as f:
            bundle = pickle.load(f)
        version = bundle.get("version", os.path.basename(path))
        return bundle, version

    def _load_from_store(self):
        """
        1. Lit production/current.json pour connaître la clé du modèle actif
        2. Charge le .pkl correspondant
        """
        obj_store = self._store or get_object_store()

        # Lire le pointeur de production
        current = obj_store.load_json(MODEL_BUCKET, MODEL_S3_KEY)
        model_key = current["model_key"]
        version   = current.get("version", "unknown")

        bundle = obj_store.load_model(MODEL_BUCKET, model_key)
        source = f"s3:{MODEL_BUCKET}/{model_key}"
        return bundle, version, source

    # ── Prédiction ───────────────────────────────────────────

    def predict(
        self,
        event:   dict[str, Any],
        profile: dict[str, Any],
    ) -> AnomalyResult:
        """
        Calcule le score d'anomalie pour une transaction.

        Paramètres
        ----------
        event   : dict — transaction en cours (horodatage, montant, device_id…)
        profile : dict — profil abonné depuis Redis/DynamoDB

        Retourne
        --------
        AnomalyResult avec score 0-100, détail IF et z-score.
        """
        # ── Features dérivées ────────────────────────────────
        features = compute_features(event, profile)
        zscore   = features.get("zscore_montant", 0.0)
        nb_tx    = int(profile.get("nb_transactions", 0))

        with self._lock:
            bundle  = self._bundle
            version = self._version

        # ── Mode dégradé ─────────────────────────────────────
        if bundle is None:
            return AnomalyResult(
                score=0, score_if=0.0,
                zscore_montant=zscore, is_anomaly=False,
                override_active=False, model_version="degraded",
                features=features,
            )

        # ── Score Isolation Forest ────────────────────────────
        score_if_norm = self._score_isolation_forest(features, bundle)

        # ── Formule hybride ───────────────────────────────────
        # Le z-score override s'active uniquement si l'historique Welford
        # est suffisamment stable (nb_transactions >= Z_MIN_TRANSACTIONS)
        zscore_fiable = nb_tx >= Z_MIN_TRANSACTIONS
        score_final   = score_if_norm
        override      = False

        if zscore_fiable:
            if zscore > Z_OVERRIDE_HARD:
                score_final = max(score_if_norm, 90)
                override    = score_final > score_if_norm
            elif zscore > Z_OVERRIDE_SOFT:
                score_final = max(score_if_norm, 75)
                override    = score_final > score_if_norm

        score_final = int(round(min(max(score_final, 0), 100)))

        return AnomalyResult(
            score          = score_final,
            score_if       = score_if_norm,
            zscore_montant = zscore,
            is_anomaly     = score_final >= self.ANOMALY_THRESHOLD,
            override_active = override,
            model_version  = version,
            features       = features,
        )

    def _score_isolation_forest(
        self,
        features: dict[str, Any],
        bundle: dict,
    ) -> float:
        """
        Convertit le score_samples de l'IF en score 0-100.

        score_samples retourne des valeurs dans (-0.5, 0).
        Les valeurs proches de 0     → normales  → score bas
        Les valeurs proches de -0.5  → anomalies → score élevé

        Normalisation :
          score_norm = (score_samples - max_normal) / (min_normal - max_normal)
          clampé à [0, 1] puis multiplié par 100.

        On utilise des constantes empiriques calibrées sur nos données :
          max_normal ≈ -0.05  (légèrement négatif pour les très normaux)
          min_normal ≈ -0.50  (seuil d'anomalie de l'IF)
        """
        model  = bundle["model"]
        scaler = bundle["scaler"]

        vec = np.array(
            [[float(features.get(f, 0.0)) for f in FEATURE_NAMES]],
            dtype=np.float32,
        )
        vec_scaled     = scaler.transform(vec)
        raw_score      = float(model.score_samples(vec_scaled)[0])

        # Normalisation linéaire : -0.05 → 0, -0.50 → 100
        MAX_NORMAL = -0.05
        MIN_NORMAL = -0.50
        normalized = (raw_score - MAX_NORMAL) / (MIN_NORMAL - MAX_NORMAL)
        normalized = float(np.clip(normalized, 0.0, 1.0)) * 100.0

        return round(normalized, 2)

    # ── Introspection ─────────────────────────────────────────

    @property
    def is_ready(self) -> bool:
        with self._lock:
            return self._bundle is not None

    @property
    def version(self) -> str:
        with self._lock:
            return self._version

    def score_if_degraded(self) -> bool:
        """Vérifie que le mode dégradé retourne score=0. Utile pour les tests."""
        from datetime import datetime, timezone
        ev = {"id_compte": "test", "horodatage": datetime.now(timezone.utc).isoformat(),
              "montant": 1000.0, "device_id": "", "id_beneficiaire": "", "antenne": ""}
        result = self.predict(ev, {})
        return result.score == 0

    def status(self) -> dict[str, Any]:
        """Résumé de l'état du détecteur — pour /health."""
        with self._lock:
            return {
                "ready":        self._bundle is not None,
                "version":      self._version,
                "loaded_from":  self._loaded_from,
                "threshold":    self.ANOMALY_THRESHOLD,
                "z_soft":       Z_OVERRIDE_SOFT,
                "z_hard":       Z_OVERRIDE_HARD,
                "z_min_tx":     Z_MIN_TRANSACTIONS,
                "n_features":   len(FEATURE_NAMES),
            }
