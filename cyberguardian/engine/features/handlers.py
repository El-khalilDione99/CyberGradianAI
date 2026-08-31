"""
engine/features/handlers.py
────────────────────────────
Couche de dispatch entre les événements Kafka bruts et la logique
métier pure de updater.py.

Responsabilités :
  - Identifier le topic source d'un événement
  - Lire le profil depuis le feature store
  - Appeler la bonne fonction de mise à jour (updater.py)
  - Réécrire le profil mis à jour dans le feature store
  - Retourner des métriques de traitement pour les logs

Ce module est le seul à connaître à la fois le feature store et updater.py.
Il ne connaît pas Kafka — c'est main.py qui gère la boucle de consommation.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from interfaces.store import get_feature_store, FeatureStore
from engine.features.updater import (
    apply_transaction,
    apply_sim_event,
    apply_otp_event,
)

logger = logging.getLogger(__name__)

# Topics connus
TOPIC_TRANSACTIONS = "transactions"
TOPIC_SIM_EVENTS   = "sim-events"
TOPIC_OTP_EVENTS   = "otp-events"


# ════════════════════════════════════════════════════════════
#  Résultat d'un traitement
# ════════════════════════════════════════════════════════════

@dataclass
class HandleResult:
    """Résultat du traitement d'un événement unique."""
    topic:      str
    id_compte:  str
    success:    bool
    profile_created: bool = False   # True si le profil n'existait pas encore
    error:      str | None = None


# ════════════════════════════════════════════════════════════
#  Compteurs de session (métriques légères)
# ════════════════════════════════════════════════════════════

@dataclass
class HandlerStats:
    processed:   int = 0
    errors:      int = 0
    transactions: int = 0
    sim_events:   int = 0
    otp_events:   int = 0
    unknown:      int = 0
    new_profiles: int = 0

    def record(self, result: HandleResult) -> None:
        if result.success:
            self.processed += 1
            if result.topic == TOPIC_TRANSACTIONS:
                self.transactions += 1
            elif result.topic == TOPIC_SIM_EVENTS:
                self.sim_events += 1
            elif result.topic == TOPIC_OTP_EVENTS:
                self.otp_events += 1
            else:
                self.unknown += 1
            if result.profile_created:
                self.new_profiles += 1
        else:
            self.errors += 1

    def summary(self) -> str:
        return (
            f"processed={self.processed} "
            f"(tx={self.transactions}, sim={self.sim_events}, otp={self.otp_events}) "
            f"errors={self.errors} new_profiles={self.new_profiles}"
        )


# ════════════════════════════════════════════════════════════
#  Gestionnaire principal
# ════════════════════════════════════════════════════════════

class FeatureHandler:
    """
    Orchestre la mise à jour des profils abonnés.

    Usage :
        handler = FeatureHandler()
        result  = handler.handle(topic="transactions", event={...})
    """

    def __init__(self, store: FeatureStore | None = None) -> None:
        # Injection possible pour les tests ; sinon on utilise la factory
        self._store = store or get_feature_store()
        self.stats  = HandlerStats()

    # ── Point d'entrée principal ─────────────────────────────

    def handle(self, topic: str, event: dict[str, Any]) -> HandleResult:
        """
        Traite un événement provenant du topic donné.
        Lit le profil, applique la mise à jour, réécrit le profil.
        """
        id_compte = event.get("id_compte", "")

        if not id_compte:
            result = HandleResult(
                topic=topic, id_compte="", success=False,
                error="champ id_compte manquant"
            )
            self.stats.record(result)
            return result

        try:
            profile, created = self._load_or_create_profile(id_compte)
            profile = self._dispatch(topic, profile, event)
            self._store.set_profile(id_compte, profile)

            result = HandleResult(
                topic=topic,
                id_compte=id_compte,
                success=True,
                profile_created=created,
            )

        except Exception as exc:
            logger.error(
                "Erreur traitement [%s] compte=%s : %s",
                topic, id_compte[:16], exc, exc_info=True,
            )
            result = HandleResult(
                topic=topic, id_compte=id_compte,
                success=False, error=str(exc),
            )

        self.stats.record(result)
        return result

    def handle_batch(
        self,
        topic: str,
        events: list[dict[str, Any]],
    ) -> list[HandleResult]:
        """
        Traite un lot d'événements du même topic.
        Les événements d'un même compte sont traités séquentiellement
        pour garantir la cohérence des profils.
        """
        return [self.handle(topic, ev) for ev in events]

    # ── Dispatch par topic ───────────────────────────────────

    def _dispatch(
        self,
        topic: str,
        profile: dict[str, Any],
        event: dict[str, Any],
    ) -> dict[str, Any]:
        """Appelle la bonne fonction de mise à jour selon le topic."""
        if topic == TOPIC_TRANSACTIONS:
            return apply_transaction(profile, event)
        elif topic == TOPIC_SIM_EVENTS:
            return apply_sim_event(profile, event)
        elif topic == TOPIC_OTP_EVENTS:
            return apply_otp_event(profile, event)
        else:
            logger.warning("Topic inconnu ignoré : %s", topic)
            return profile

    # ── Lecture / création de profil ─────────────────────────

    def _load_or_create_profile(
        self, id_compte: str
    ) -> tuple[dict[str, Any], bool]:
        """
        Retourne (profil, created).
        Si le profil n'existe pas encore (compte jamais vu),
        crée un profil minimal de secours pour ne pas bloquer
        le traitement. Le Feature Updater ne dépend pas du simulateur.
        """
        profile = self._store.get_profile(id_compte)
        if profile:
            return profile, False

        # Profil minimal — sera enrichi au fil des événements
        logger.warning(
            "Profil introuvable pour %s — création d'un profil minimal",
            id_compte[:16],
        )
        profile = _build_minimal_profile(id_compte)
        return profile, True


# ════════════════════════════════════════════════════════════
#  Profil minimal de secours
# ════════════════════════════════════════════════════════════

def _build_minimal_profile(id_compte: str) -> dict[str, Any]:
    """
    Profil créé à la volée si un événement arrive pour un compte
    inconnu du feature store (edge case en production).
    Tous les compteurs démarrent à zéro.
    """
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()
    return {
        "id_compte":              id_compte,
        "region":                 "unknown",
        "segment":                "unknown",
        "antenne_domicile":       "",
        "antennes_connues":       [],
        "iccid_actuel":           "",
        "imsi_actuel":            "",
        "ts_dernier_swap":        None,
        "nb_swaps_30j":           0,
        "device_id_habituel":     "",
        "devices_connus":         [],
        "beneficiaires_connus":   [],
        "nb_transactions":        0,
        "montant_moyen":          0.0,
        "montant_m2_welford":     0.0,
        "ecart_type_montant":     0.0,
        "montant_moyen_habituel": 0.0,
        "nb_tx_1h":               0,
        "nb_tx_24h":              0,
        "nb_tx_7j":               0,
        "total_montant_24h":      0.0,
        "fenetre_1h_ts":          [],
        "fenetre_24h_ts":         [],
        "nb_otp_1h":              0,
        "nb_otp_24h":             0,
        "fenetre_otp_1h_ts":      [],
        "solde":                  0.0,
        "heures_actives":         [],
        "cree_le":                now,
        "mis_a_jour_le":          now,
    }
