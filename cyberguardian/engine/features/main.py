"""
engine/features/main.py
────────────────────────
Point d'entrée du Feature Updater (IA-3).

Boucle de consommation sur les trois topics Kafka :
  - transactions
  - sim-events
  - otp-events

Pour chaque message reçu, appelle FeatureHandler.handle()
qui lit le profil dans Redis, le met à jour, et le réécrit.

Variables d'environnement :
  ENV                       : local | aws  (default: local)
  KAFKA_BOOTSTRAP_SERVERS   : default localhost:9092
  REDIS_HOST / REDIS_PORT   : default localhost / 6379
  FU_BATCH_SIZE             : taille des lots Kafka (default: 100)
  FU_POLL_INTERVAL_S        : pause entre deux polls si vide (default: 1.0)
  FU_LOG_EVERY              : log tous les N messages (default: 500)
"""

import logging
import os
import signal
import sys
import time

from dotenv import load_dotenv

load_dotenv(os.getenv("ENV_FILE", ".env.local"))

from engine.features.handlers import FeatureHandler
from interfaces.streams import get_consumer

# ── Logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("feature-updater")

# ── Configuration ────────────────────────────────────────────
TOPICS        = ["transactions", "sim-events", "otp-events"]
BATCH_SIZE    = int(os.getenv("FU_BATCH_SIZE",      "100"))
POLL_INTERVAL = float(os.getenv("FU_POLL_INTERVAL_S", "1.0"))
LOG_EVERY     = int(os.getenv("FU_LOG_EVERY",        "500"))

# ── Arrêt gracieux ───────────────────────────────────────────
_running = True

def _handle_signal(sig, frame):  # noqa: ANN001
    global _running
    logger.info("Signal %s reçu — arrêt en cours…", sig)
    _running = False

signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT,  _handle_signal)


# ════════════════════════════════════════════════════════════
#  Boucle principale
# ════════════════════════════════════════════════════════════

def run() -> None:
    """
    Démarre la boucle de consommation Kafka.
    Tourne indéfiniment jusqu'à SIGTERM / SIGINT ou erreur fatale.
    """
    logger.info("═══════════════════════════════════════════════")
    logger.info("  CyberGuardian AI — Feature Updater v1.0      ")
    logger.info("  ENV=%s | topics=%s",
                os.getenv("ENV", "local"), TOPICS)
    logger.info("  batch=%d | poll_interval=%.1fs | log_every=%d",
                BATCH_SIZE, POLL_INTERVAL, LOG_EVERY)
    logger.info("═══════════════════════════════════════════════")

    handler   = FeatureHandler()
    consumers = {
        topic: get_consumer(group_id=f"feature-updater-{topic}", topic=topic)
        for topic in TOPICS
    }

    total_messages = 0
    t_start        = time.time()

    while _running:
        messages_this_round = 0

        for topic, consumer in consumers.items():
            try:
                events = consumer.consume(topic, batch_size=BATCH_SIZE)
            except Exception as exc:
                logger.error("Erreur consommation [%s] : %s", topic, exc)
                continue

            if not events:
                continue

            results = handler.handle_batch(topic, events)
            messages_this_round += len(results)
            total_messages      += len(results)

            # Log des erreurs individuelles
            for r in results:
                if not r.success:
                    logger.warning(
                        "Échec [%s] compte=%s : %s",
                        r.topic, r.id_compte[:16] if r.id_compte else "?", r.error,
                    )

        # ── Log périodique ────────────────────────────────────
        if total_messages > 0 and total_messages % LOG_EVERY < messages_this_round:
            elapsed  = time.time() - t_start
            throughput = total_messages / elapsed if elapsed > 0 else 0
            logger.info(
                "Progression — %d messages traités (%.1f msg/s) | %s",
                total_messages,
                throughput,
                handler.stats.summary(),
            )

        # ── Pause si rien à consommer ─────────────────────────
        if messages_this_round == 0:
            time.sleep(POLL_INTERVAL)

    # ── Rapport final ─────────────────────────────────────────
    elapsed = time.time() - t_start
    logger.info("═══════════════════════════════════════════════")
    logger.info("  Feature Updater arrêté proprement")
    logger.info("  Total messages : %d en %.1fs", total_messages, elapsed)
    logger.info("  %s", handler.stats.summary())
    logger.info("═══════════════════════════════════════════════")


# ════════════════════════════════════════════════════════════
#  Entrée CLI
# ════════════════════════════════════════════════════════════

if __name__ == "__main__":
    try:
        run()
    except Exception as exc:
        logger.critical("Erreur fatale : %s", exc, exc_info=True)
        sys.exit(1)
