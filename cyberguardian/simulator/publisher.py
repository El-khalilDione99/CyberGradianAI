"""
simulator/publisher.py
──────────────────────
Publication des événements dans les streams (Kafka local / Kinesis AWS).

Les événements sont publiés dans l'ordre chronologique — le feature
updater peut ainsi construire les profils Welford de façon cohérente.
"""

import time
import logging
from enum import Enum
from simulator.scenarios import Scenario
from interfaces.streams import get_publisher, StreamPublisher

logger = logging.getLogger(__name__)


class PublishMode(str, Enum):
    REPLAY = "replay"   # délais réels entre événements (démo live)
    BATCH  = "batch"    # publication immédiate (entraînement)


class EventPublisher:

    def __init__(self, mode: PublishMode = PublishMode.BATCH, speed_factor: float = 1.0):
        self._publisher: StreamPublisher = get_publisher()
        self._mode = mode
        self._speed_factor = speed_factor

    def publish_scenario(self, scenario: Scenario) -> dict:
        """Publie tous les événements d'un scénario. Retourne un mini rapport."""
        nb_tx = nb_sim = nb_otp = 0
        derniere_ts = None

        for ev in scenario.evenements:  # déjà triés par horodatage
            if self._mode == PublishMode.REPLAY and derniere_ts is not None:
                delai = (ev.horodatage - derniere_ts).total_seconds() / self._speed_factor
                if delai > 0:
                    time.sleep(min(delai, 60))  # plafonné à 60s pour la démo
            derniere_ts = ev.horodatage

            try:
                self._publisher.publish(
                    stream=ev.stream,
                    key=ev.cle_partition,
                    payload=ev.payload,
                )
                if ev.stream == "transactions":
                    nb_tx += 1
                elif ev.stream == "sim-events":
                    nb_sim += 1
                elif ev.stream == "otp-events":
                    nb_otp += 1
            except Exception as exc:
                logger.error("Erreur publication [%s] %s : %s",
                             ev.stream, ev.cle_partition[:12], exc)

        return {"nb_transactions": nb_tx, "nb_sim_events": nb_sim, "nb_otp_events": nb_otp}

    def publish_all(self, scenarios: list[Scenario], log_every: int = 500) -> dict:
        """Publie tous les scénarios. Retourne un rapport complet."""
        total_tx = total_sim = total_otp = 0
        nb_fraudes = nb_legit = erreurs = 0

        for i, scenario in enumerate(scenarios):
            try:
                r = self.publish_scenario(scenario)
                total_tx  += r["nb_transactions"]
                total_sim += r["nb_sim_events"]
                total_otp += r["nb_otp_events"]
                if scenario.est_fraude:
                    nb_fraudes += 1
                else:
                    nb_legit += 1
            except Exception as exc:
                erreurs += 1
                logger.error("Erreur scénario %s : %s", scenario.id_scenario, exc)

            if (i + 1) % log_every == 0:
                logger.info("Progression : %d/%d scénarios (%d tx, %d sim, %d otp)",
                            i + 1, len(scenarios), total_tx, total_sim, total_otp)

        return {
            "total_scenarios":  len(scenarios),
            "total_events":     total_tx + total_sim + total_otp,
            "nb_transactions":  total_tx,
            "nb_sim_events":    total_sim,
            "nb_otp_events":    total_otp,
            "fraud_scenarios":  nb_fraudes,
            "legit_scenarios":  nb_legit,
            "errors":           erreurs,
        }
