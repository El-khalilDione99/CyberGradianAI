"""
simulator/main.py
─────────────────
Point d'entrée du simulateur (IA-1).
"""

import argparse
import logging
import os
import sys
import time
from dotenv import load_dotenv

env_file = os.getenv("ENV_FILE", ".env.local")
load_dotenv(env_file)

from simulator.subscribers import generate_subscribers
from simulator.profiles import init_profiles, reset_profiles
from simulator.scenarios import generate_scenarios
from simulator.publisher import EventPublisher, PublishMode

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("simulator")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CyberGuardian AI — Simulateur de données")
    parser.add_argument(
        "--mode",
        choices=["batch", "replay"],
        default=os.getenv("SIM_MODE", "batch"),
        help="batch: publie tout immédiatement | replay: respecte les délais",
    )
    parser.add_argument(
        "--speed",
        type=float,
        default=float(os.getenv("SIM_SPEED", "1.0")),
        help="Facteur d'accélération du replay (ex: 10 = 10x plus vite)",
    )
    parser.add_argument(
        "--reset-profiles",
        action="store_true",
        default=False,
        help="Réinitialiser les profils avant la simulation (démo rejouable)",
    )
    parser.add_argument(
        "--nb-subscribers",
        type=int,
        default=int(os.getenv("NB_SUBSCRIBERS", "500")),
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=int(os.getenv("SEED", "42")),
    )
    parser.add_argument(
        "--attack-ratio",
        type=float,
        default=float(os.getenv("ATTACK_RATIO", "0.05")),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    logger.info("═══════════════════════════════════════")
    logger.info("  CyberGuardian AI — Simulateur v2.0  ")
    logger.info("  ENV=%s | mode=%s | seed=%d",
                os.getenv("ENV", "local"), args.mode, args.seed)
    logger.info("═══════════════════════════════════════")

    # ── Étape 1 : Générer les abonnés ────────────────────
    logger.info("Génération de %d abonnés (seed=%d)…", args.nb_subscribers, args.seed)
    t0 = time.time()
    subscribers = generate_subscribers(args.nb_subscribers, seed=args.seed)
    logger.info("  → %d abonnés générés en %.2fs", len(subscribers), time.time() - t0)

    # ── Étape 2 : Initialiser les profils ────────────────
    if args.reset_profiles:
        logger.info("Réinitialisation des profils…")
        reset_profiles(subscribers)
    else:
        logger.info("Initialisation des profils (skip si existants)…")
        init_profiles(subscribers)

    # ── Étape 3 : Générer les scénarios ──────────────────
    logger.info(
        "Génération des scénarios (attack_ratio=%.0f%%)…",
        args.attack_ratio * 100,
    )
    t0 = time.time()
    scenarios = generate_scenarios(
        subscribers,
        attack_ratio=args.attack_ratio,
        seed=args.seed,
    )
    logger.info("  → %d scénarios générés en %.2fs", len(scenarios), time.time() - t0)

    # ── Étape 4 : Publier les événements ─────────────────
    mode = PublishMode.REPLAY if args.mode == "replay" else PublishMode.BATCH
    publisher = EventPublisher(mode=mode, speed_factor=args.speed)

    logger.info("Publication des événements (mode=%s)…", args.mode)
    t0 = time.time()
    report = publisher.publish_all(scenarios, log_every=100)
    elapsed = time.time() - t0

    # ── Rapport final ─────────────────────────────────────
    logger.info("═══════════════════════════════════════")
    logger.info("  RAPPORT DE SIMULATION")
    logger.info("  Scénarios  : %d total", report["total_scenarios"])
    logger.info("  ├─ Fraudes : %d (%.1f%%)",
                report["fraud_scenarios"],
                report["fraud_scenarios"] / report["total_scenarios"] * 100)
    logger.info("  └─ Légitimes : %d", report["legit_scenarios"])
    logger.info("  Événements publiés : %d", report["total_events"])
    logger.info("  Erreurs            : %d", report["errors"])
    logger.info("  Durée              : %.2fs", elapsed)
    logger.info("═══════════════════════════════════════")

    if report["errors"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
