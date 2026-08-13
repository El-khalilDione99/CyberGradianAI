"""
simulator/publisher.py
──────────────────────
Publication des événements dans les streams (Kafka local / Kinesis AWS).
"""

import time
import logging
from enum import Enum
from simulator.scenarios import Scenario, Evenement
from interfaces.streams import get_publisher, StreamPublisher

logger = logging.getLogger(__name__)


class PublishMode(str, Enum):
    REPLAY = "replay"   # délais réels entre événements
    BATCH  = "batch"    # publication immédiate


class EventPublisher:
    def __init__(self, mode: PublishMode = PublishMode.BATCH, speed_factor: float = 1.0):
        self._publisher: StreamPublisher = get_publisher()
        self._mode = mode
        self._speed_factor = speed_factor

    def publish_scenario(self, scenario: Scenario) -> int:
        """Publie tous les événements d'un scénario. Retourne le nb publié."""
        evenements_tries = sorted(scenario.evenements, key=lambda e: e.delai_secondes)
        publie = 0
        dernier_delai = 0.0

        for ev in evenements_tries:
            if self._mode == PublishMode.REPLAY and ev.delai_secondes > 0:
                attente = (ev.delai_secondes - dernier_delai) / self._speed_factor
                if attente > 0:
                    time.sleep(attente)
            dernier_delai = ev.delai_secondes

            try:
                self._publisher.publish(
                    stream=ev.stream,
                    key=ev.cle_partition,
                    payload=ev.payload,
                )
                publie += 1
                logger.debug("Publié [%s] %s → %s",
                             ev.stream, ev.cle_partition[:8],
                             ev.payload.get("type_evenement", ev.payload.get("id_transaction", "?")))
            except Exception as exc:
                logger.error("Erreur publication stream=%s : %s", ev.stream, exc)

        return publie

    def publish_all(self, scenarios: list[Scenario], log_every: int = 100) -> dict:
        """Publie tous les scénarios. Retourne un rapport."""
        total_ev = 0
        nb_fraudes = 0
        nb_legit = 0
        erreurs = 0

        for i, scenario in enumerate(scenarios):
            try:
                n = self.publish_scenario(scenario)
                total_ev += n
                if scenario.est_fraude:
                    nb_fraudes += 1
                else:
                    nb_legit += 1
            except Exception as exc:
                erreurs += 1
                logger.error("Erreur scénario %s : %s", scenario.id_scenario, exc)

            if (i + 1) % log_every == 0:
                logger.info("Progression : %d/%d scénarios publiés (%d événements)",
                            i + 1, len(scenarios), total_ev)

        rapport = {
            "total_scenarios": len(scenarios),
            "total_events": total_ev,
            "fraud_scenarios": nb_fraudes,
            "legit_scenarios": nb_legit,
            "errors": erreurs,
        }
        logger.info("Publication terminée : %s", rapport)
        return rapport
