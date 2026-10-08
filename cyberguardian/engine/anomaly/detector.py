"""
engine/anomaly/detector.py
───────────────────────────
AnomalyDetector — Couche 2 du moteur de scoring (IA-5).

Spécification IA-5 :
  « Isolation Forest entraîné sur le trafic légitime simulé, plus z-scores par
    abonné en garde-fou interprétable. Modèle sérialisé versionné dans S3
    models/. »

Responsabilités :
  - Charger le bundle (IsolationForest + RobustScaler + calibration) depuis
    MinIO/S3 (bucket models, pointeur anomaly/production/current.json)
  - Calculer les features via compute_features() (IA-4)
  - Score Isolation Forest calibré 0-100
  - Z-scores par abonné (montant, vitesse 24h, OTP 24h — voir zscores.py)
    en garde-fou interprétable
  - Rechargement à chaud (reload_model) et thread-safety (RLock)

Formule du score Couche 2 :
  Mode normal (IF disponible) :
    score = score_IF
    si z_max >= Z_OVERRIDE_HARD (5σ) et Z_GUARD_ENABLED :
        score = max(score_IF, Z_GUARD_FLOOR)     # plancher 50 → vérification
    Les dimensions à z >= Z_OVERRIDE_SOFT (3σ) sont toujours listées dans
    `raisons` (lisibles par un analyste), même sans effet sur le score.

  Mode dégradé (IF indisponible) :
    score = filet_de_secours(z_max) : 0σ→0, 3σ→75, 5σ→90, 8σ→100

  Compte sans historique (nb_transactions == 0) : score = 0.

Le plancher du garde-fou (50) place la transaction en zone CHALLENGE
(OTP supplémentaire) sans jamais la bloquer seul : le blocage reste une
décision du modèle.
"""

from __future__ import annotations

import logging
import os
import threading
from dataclasses import dataclass, field
from typing import Any

import numpy as np

from engine.anomaly.dataset import FEATURE_NAMES
from engine.anomaly.zscores import (
    compute_zscores, Z_OVERRIDE_SOFT, Z_OVERRIDE_HARD, Z_MIN_TRANSACTIONS,
)
from engine.rules.features import compute_features
from interfaces.store import get_object_store, BUCKET_MODELS

logger = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────
MODEL_BUCKET     = os.getenv("MODEL_BUCKET",     BUCKET_MODELS)
MODEL_S3_KEY     = os.getenv("MODEL_S3_KEY",     "anomaly/production/current.json")
MODEL_LOCAL_PATH = os.getenv("ANOMALY_MODEL_PATH", "")

# Garde-fou z-score en mode normal
Z_GUARD_ENABLED = os.getenv("Z_GUARD_ENABLED", "1") not in ("0", "false", "False")
Z_GUARD_FLOOR   = float(os.getenv("Z_GUARD_FLOOR", "50"))

# Filet de secours en mode dégradé
Z_DEGRADED_MAX  = float(os.getenv("Z_DEGRADED_MAX", "8.0"))

__all__ = [
    "AnomalyDetector", "AnomalyResult",
    "Z_OVERRIDE_SOFT", "Z_OVERRIDE_HARD", "Z_MIN_TRANSACTIONS",
    "Z_GUARD_ENABLED", "Z_GUARD_FLOOR", "Z_DEGRADED_MAX",
]


# ════════════════════════════════════════════════════════════
#  Résultat de prédiction
# ════════════════════════════════════════════════════════════

@dataclass
class AnomalyResult:
    """Résultat de la Couche 2 pour une transaction."""
    score:           int              # score Couche 2 final 0-100 (IF + garde-fou)
    score_if:        float            # score Isolation Forest seul 0-100
    zscore_montant:  float            # z-score du montant (feature IA-4)
    is_anomaly:      bool             # score >= ANOMALY_THRESHOLD
    override_active: bool             # True si le garde-fou / filet z-score a relevé le score
    model_version:   str
    features:        dict[str, Any]
    profile_age_ms:  float | None = None
    zscores:         dict[str, float] = field(default_factory=dict)   # z par dimension
    raisons:         list[str]        = field(default_factory=list)   # explications lisibles

    def to_dict(self) -> dict[str, Any]:
        return {
            "score":           self.score,
            "score_if":        round(self.score_if, 2),
            "zscore_montant":  round(self.zscore_montant, 4),
            "zscores":         self.zscores,
            "raisons":         self.raisons,
            "is_anomaly":      self.is_anomaly,
            "override_active": self.override_active,
            "model_version":   self.model_version,
            "profile_age_ms":  self.profile_age_ms,
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
    Couche 2 — Isolation Forest + z-scores par abonné.

    Usage :
        detector = AnomalyDetector()
        result   = detector.predict(event=tx_dict, profile=profile_dict)
        print(result.score, result.raisons)
    """

    ANOMALY_THRESHOLD = 50  # score >= 50 → is_anomaly = True

    def __init__(self, store=None) -> None:
        self._store   = store
        self._bundle  = None          # {"model", "scaler", "feature_names", "score_calibration", "version"}
        self._version = "unknown"
        self._lock    = threading.RLock()
        self._loaded_from = "none"
        self.reload_model()

    # ── Chargement / rechargement ────────────────────────────

    def reload_model(self) -> str:
        """
        Recharge le bundle depuis :
          1. Fichier local si ANOMALY_MODEL_PATH est défini
          2. MinIO/S3 : lit production/current.json → charge le .pkl pointé
          3. Mode dégradé sinon (filet de secours z-score)
        """
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

        with self._lock:
            self._bundle      = None
            self._version     = "degraded"
            self._loaded_from = "none"
        return "Mode dégradé — aucun modèle d'anomalie disponible (filet de secours z-score actif)"

    def _load_from_file(self, path: str):
        import pickle
        with open(path, "rb") as f:
            bundle = pickle.load(f)
        version = bundle.get("version", os.path.basename(path))
        return bundle, version

    def _load_from_store(self):
        obj_store = self._store or get_object_store()
        current   = obj_store.load_json(MODEL_BUCKET, MODEL_S3_KEY)
        model_key = current["model_key"]
        version   = current.get("version", "unknown")
        bundle    = obj_store.load_model(MODEL_BUCKET, model_key)
        return bundle, version, f"s3:{MODEL_BUCKET}/{model_key}"

    # ── Prédiction ───────────────────────────────────────────

    def predict(
        self,
        event:   dict[str, Any],
        profile: dict[str, Any],
    ) -> AnomalyResult:
        """
        Calcule le score d'anomalie d'une transaction.

        event   : transaction en cours (horodatage, montant, device_id…)
        profile : profil abonné AVANT cette transaction (Redis/DynamoDB)
        """
        features = compute_features(event, profile)
        zreport  = compute_zscores(features, profile)
        zscore   = float(features.get("zscore_montant", 0.0))
        nb_tx    = int(profile.get("nb_transactions", 0))

        # Fraîcheur du profil (monitoring)
        profile_age_ms: float | None = None
        mis_a_jour = profile.get("mis_a_jour_le")
        ts_event_str = event.get("horodatage", "")
        if mis_a_jour and ts_event_str:
            try:
                from engine.rules.features import _parse_ts as _pts
                age = (_pts(ts_event_str) - _pts(mis_a_jour)).total_seconds() * 1000
                profile_age_ms = round(age, 1)
            except Exception:
                pass

        with self._lock:
            bundle  = self._bundle
            version = self._version

        def _result(score: float, score_if: float, override: bool,
                    model_version: str, raisons: list[str]) -> AnomalyResult:
            s = int(round(min(max(score, 0.0), 100.0)))
            return AnomalyResult(
                score=s, score_if=score_if, zscore_montant=zscore,
                is_anomaly=s >= self.ANOMALY_THRESHOLD,
                override_active=override, model_version=model_version,
                features=features, profile_age_ms=profile_age_ms,
                zscores=zreport.as_dict(), raisons=raisons,
            )

        # ── Compte sans historique : rien de fiable à comparer ──
        if nb_tx == 0:
            return _result(0.0, 0.0, False, version, [])

        alertes = zreport.alertes(Z_OVERRIDE_SOFT)
        z_max   = zreport.z_max

        # ── Mode dégradé : le z-score sert de filet de secours ──
        if bundle is None:
            score_fallback = self._score_from_zscore(z_max)
            raisons = ["modèle IF indisponible — filet de secours z-score"] + alertes
            return _result(score_fallback, 0.0, score_fallback > 0, "degraded", raisons)

        # ── Mode normal : Isolation Forest + garde-fou z-score ──
        score_if = self._score_isolation_forest(features, bundle)
        score    = score_if
        override = False
        raisons  = list(alertes)
        if Z_GUARD_ENABLED and z_max >= Z_OVERRIDE_HARD and score_if < Z_GUARD_FLOOR:
            score    = Z_GUARD_FLOOR
            override = True
            raisons.insert(0, f"garde-fou z-score (≥ {Z_OVERRIDE_HARD:g}σ) : "
                              f"score relevé de {score_if:.0f} à {Z_GUARD_FLOOR:.0f}")
        return _result(score, score_if, override, version, raisons)

    def _score_isolation_forest(
        self,
        features: dict[str, Any],
        bundle: dict,
    ) -> float:
        """
        Convertit score_samples de l'IF en score 0-100 via la calibration
        quantile_piecewise du bundle (construite sur les scores TRAIN).
        Même calcul que evaluate._batch_score : batch et temps réel identiques.
        """
        model  = bundle["model"]
        scaler = bundle["scaler"]

        vec = np.array(
            [[float(features.get(f, 0.0)) for f in FEATURE_NAMES]],
            dtype=np.float32,
        )
        raw_score = float(model.score_samples(scaler.transform(vec))[0])

        calibration = bundle.get("score_calibration", {})
        if calibration.get("type") == "quantile_piecewise":
            points = calibration.get("points", [])
            if len(points) >= 2:
                x_cal = np.array([float(p[0]) for p in points])
                y_cal = np.array([float(p[1]) for p in points])
                normalized = float(np.interp(-raw_score, x_cal, y_cal))
                return round(float(np.clip(normalized, 0.0, 100.0)), 2)

        # Ancien bundle sans calibration
        logger.warning(
            "Bundle sans calibration quantile_piecewise — fallback formule linéaire. "
            "Réentraîner le modèle pour aligner batch et temps réel."
        )
        MAX_NORMAL = -0.05
        MIN_NORMAL = -0.50
        normalized = (raw_score - MAX_NORMAL) / (MIN_NORMAL - MAX_NORMAL)
        return round(float(np.clip(normalized, 0.0, 1.0)) * 100.0, 2)

    def _score_from_zscore(self, zscore: float) -> float:
        """
        Filet de secours (mode dégradé uniquement) :
          z <= 0 → 0 ; SOFT (3) → 75 ; HARD (5) → 90 ; >= Z_DEGRADED_MAX (8) → 100
        """
        breakpoints = [0.0, Z_OVERRIDE_SOFT, Z_OVERRIDE_HARD, Z_DEGRADED_MAX]
        scores      = [0.0, 75.0, 90.0, 100.0]
        return float(np.interp(zscore, breakpoints, scores))

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
        """Vérifie qu'un compte sans historique obtient bien score = 0."""
        from datetime import datetime, timezone
        ev = {
            "id_compte": "_test_degraded_",
            "horodatage": datetime.now(timezone.utc).isoformat(),
            "montant": 1000.0, "device_id": "", "id_beneficiaire": "", "antenne": "",
        }
        r1 = self.predict(ev, {})
        r2 = self.predict(ev, {"nb_transactions": 0, "montant_moyen": 50000.0})
        return r1.score == 0 and r2.score == 0

    def status(self) -> dict[str, Any]:
        """Résumé de l'état du détecteur — pour /health."""
        with self._lock:
            return {
                "ready":          self._bundle is not None,
                "version":        self._version,
                "loaded_from":    self._loaded_from,
                "threshold":      self.ANOMALY_THRESHOLD,
                "z_soft":         Z_OVERRIDE_SOFT,
                "z_hard":         Z_OVERRIDE_HARD,
                "z_guard_enabled": Z_GUARD_ENABLED,
                "z_guard_floor":  Z_GUARD_FLOOR,
                "z_degraded_max": Z_DEGRADED_MAX,
                "z_min_tx":       Z_MIN_TRANSACTIONS,
                "n_features":     len(FEATURE_NAMES),
            }
