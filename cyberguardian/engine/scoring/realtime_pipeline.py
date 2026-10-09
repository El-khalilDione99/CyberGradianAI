"""
engine/scoring/realtime_pipeline.py
─────────────────────────────────────
Cœur du moteur de scoring temps réel — CyberGuardian AI (IA-7).

Contrat d'entrée / sortie : docs/contrat_api_scoring.md (v1).

Pour chaque transaction :
  1. Lire le profil de l'abonné dans le feature store (Redis / DynamoDB)
  2. Couche 1 — RuleEngine.evaluate()         → S1 (score max des règles déclenchées)
  3. Couche 2 — AnomalyDetector.predict()     → S2 (Isolation Forest + garde-fou z-score)
  4. Couche 3 — XGBoostDetector.predict()     → S3 (probabilité × 100, 3 raisons SHAP)
  5. Agrégation : score_final = round(max(S1, w2·S2 + w3·S3))   } engine/scoring/aggregator.py
  6. Décision   : < 30 PASS · 30-69 CHALLENGE · ≥ 70 BLOCK        } (fonctions pures)

Ce module orchestre (profil, couches, résultat) ; la formule et les seuils
vivent dans aggregator.py, sans aucune entrée / sortie.

Une couche en échec ne bloque jamais la réponse : son score vaut 0 et son
statut passe à « erreur ». Le profil n'est PAS mis à jour par défaut : c'est
le rôle du Feature Updater (IA-3), qui consomme le même flux d'événements.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

from engine.anomaly.detector    import AnomalyDetector, Z_GUARD_FLOOR
from engine.rules.engine        import RuleEngine
from engine.supervised.detector import XGBoostDetector
from engine.features.updater    import apply_transaction
from engine.scoring.aggregator  import (          # formule et seuils : fonctions pures
    agreger, decider, FORMULE, SEUIL_CHALLENGE, SEUIL_BLOCK, SEUIL_ALERTE, WEIGHT_IF, WEIGHT_XGB,
)
from interfaces.store           import get_feature_store

logger = logging.getLogger(__name__)

UPDATE_PROFILE = os.getenv("SCORING_UPDATE_PROFILE", "false").lower() == "true"

FEATURES_EXPOSEES = [
    "hours_since_sim_swap", "amount_ratio", "zscore_montant", "new_device",
    "new_beneficiary", "is_roaming", "is_active_hour", "otp_count_1h",
    "nb_tx_1h", "nb_tx_24h", "nb_swaps_30j",
]


class RealtimePipeline:
    """
    Orchestrateur des trois couches. Thread-safe : chaque moteur protège son
    propre état (RLock), le pipeline peut être partagé entre requêtes.
    """

    def __init__(self, store=None, feature_store=None) -> None:
        self._feature_store = feature_store or get_feature_store()
        self._rules = RuleEngine(store=store)
        self._if    = AnomalyDetector(store=store)
        self._xgb   = XGBoostDetector(store=store)
        logger.info("RealtimePipeline prêt — %s", self.status())

    # ── Scoring ───────────────────────────────────────────────

    def score(self, event: dict[str, Any], profile_override: dict[str, Any] | None = None,
              update_profile: bool | None = None) -> dict[str, Any]:
        """Score une transaction et retourne la réponse au format du contrat v1."""
        t0 = time.perf_counter()
        id_compte = event.get("id_compte", "")

        if profile_override is not None:
            profile = profile_override
        else:
            try:
                profile = self._feature_store.get_profile(id_compte)
            except Exception as exc:
                logger.warning("Profil %s illisible (%s) — profil vide", id_compte[:12], exc)
                profile = {}

        c1 = self._couche1(event, profile)
        c2 = self._couche2(event, profile)
        c3 = self._couche3(event, profile)

        score_final, combine = agreger(c1["score"], c2["score"], c3["score"])
        decision, alerte = decider(score_final)

        if (UPDATE_PROFILE if update_profile is None else update_profile) and profile_override is None:
            try:
                self._feature_store.set_profile(id_compte, apply_transaction(dict(profile), event))
            except Exception as exc:
                logger.warning("Mise à jour du profil %s échouée : %s", id_compte[:12], exc)

        f2, f3 = c2.pop("_features", None), c3.pop("_features", None)
        features = f3 or f2 or {}
        return {
            "id_transaction":     event.get("id_transaction", ""),
            "id_compte":          id_compte,
            "scored_at":          datetime.now(timezone.utc).isoformat(),
            "decision":           decision,
            "alerte_prioritaire": alerte,
            "score_final":        score_final,
            "seuils":             {"challenge": SEUIL_CHALLENGE, "block": SEUIL_BLOCK, "alerte": SEUIL_ALERTE},
            "agregation":         {"formule": FORMULE, "w2_anomalie": WEIGHT_IF,
                                   "w3_supervise": WEIGHT_XGB, "score_combine": combine},
            "couche1":            c1,
            "couche2":            c2,
            "couche3":            c3,
            "features":           {k: features.get(k) for k in FEATURES_EXPOSEES if k in features},
            "latence_ms":         round((time.perf_counter() - t0) * 1000, 2),
        }

    def _couche1(self, event, profile) -> dict[str, Any]:
        try:
            r = self._rules.evaluate(event, profile)
            return {"score": r.score, "version": self._rules.version,
                    "regles": [{"rule_id": m.rule_id, "nom": m.rule_name, "score": m.score} for m in r.matches]}
        except Exception as exc:
            logger.error("Couche 1 en échec pour %s : %s", event.get("id_transaction"), exc)
            return {"score": 0, "version": "erreur", "regles": []}

    def _couche2(self, event, profile) -> dict[str, Any]:
        try:
            r = self._if.predict(event, profile)
            if r.model_version == "degraded":          # modèle absent : score de repli z-score
                score = float(r.score)
            else:                                        # IF, relevé au plancher si le garde-fou s'active
                score = float(Z_GUARD_FLOOR) if r.override_active else r.score_if
            return {"score": round(score, 2), "score_if": round(r.score_if, 2),
                    "garde_fou_actif": bool(r.override_active), "zscores": r.zscores, "raisons": r.raisons,
                    "version": r.model_version, "statut": "degrade" if r.model_version == "degraded" else "ok",
                    "_features": r.features}
        except Exception as exc:
            logger.error("Couche 2 en échec pour %s : %s", event.get("id_transaction"), exc)
            return {"score": 0.0, "score_if": 0.0, "garde_fou_actif": False, "zscores": {}, "raisons": [],
                    "version": "erreur", "statut": "erreur"}

    def _couche3(self, event, profile) -> dict[str, Any]:
        try:
            r = self._xgb.predict(event, profile)
            return {"score": round(r.probability * 100, 2), "probabilite": round(r.probability, 4),
                    "shap_top3": r.shap_top3, "version": r.model_version,
                    "statut": "degrade" if r.model_version == "degraded" else "ok", "_features": r.features}
        except Exception as exc:
            logger.error("Couche 3 en échec pour %s : %s", event.get("id_transaction"), exc)
            return {"score": 0.0, "probabilite": 0.0, "shap_top3": [], "version": "erreur", "statut": "erreur"}

    # ── Rechargement à chaud ──────────────────────────────────

    def reload(self) -> dict[str, str]:
        """Recharge règles et modèles depuis le stockage objet, sans redémarrer."""
        return {"couche1": self._rules.reload_rules(),
                "couche2": self._if.reload_model(),
                "couche3": self._xgb.reload_model()}

    # ── Introspection ─────────────────────────────────────────

    def versions(self) -> dict[str, str]:
        return {"couche1": self._rules.version, "couche2": self._if.version, "couche3": self._xgb.version}

    def status(self) -> dict[str, Any]:
        return {
            "couche1": {"pret": self._rules.rules_count > 0, "version": self._rules.version,
                        "nb_regles": self._rules.rules_count, "source": self._rules.loaded_from},
            "couche2": self._if.status(),
            "couche3": self._xgb.status(),
            "agregation": {"w2_anomalie": WEIGHT_IF, "w3_supervise": WEIGHT_XGB},
            "seuils": {"challenge": SEUIL_CHALLENGE, "block": SEUIL_BLOCK, "alerte": SEUIL_ALERTE},
        }

    @property
    def is_fully_ready(self) -> bool:
        return self._rules.rules_count > 0 and self._if.is_ready and self._xgb.is_ready
