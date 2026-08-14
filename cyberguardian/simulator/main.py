"""
simulator/main.py
─────────────────
Point d'entrée du simulateur CyberGuardian AI (IA-1).

Modes :
  --mode batch   : publie tous les événements immédiatement (entraînement)
  --mode replay  : respecte les délais réels entre événements (démo live)

La simulation couvre 30 jours de trafic :
  - Transactions normales selon une loi de Poisson
  - Scénarios spéciaux injectés (fraudes + faux positifs légitimes)
  - Événements triés par horodatage pour garantir la cohérence des profils

Variables d'environnement :
  ENV, SIM_SEED, SIM_NB_ABONNES, SIM_DUREE_JOURS, SIM_DATE_DEBUT,
  SIM_TAUX_FRAUDE, SIM_TRANSACTIONS_JOUR, ...
"""

import argparse
import logging
import os
import sys
import time

from dotenv import load_dotenv

load_dotenv(os.getenv("ENV_FILE", ".env.local"))

from simulator.subscribers import generate_subscribers
from simulator.calendrier  import planifier_simulation
from simulator.profiles    import init_profiles, reset_profiles
from simulator.publisher   import EventPublisher, PublishMode
from simulator.config      import (
    SEED, NB_ABONNES, DUREE_SIMULATION_JOURS, DATE_DEBUT_SIMULATION,
    TAUX_SCENARIO_FRAUDE,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("simulator")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="CyberGuardian AI — Simulateur")
    p.add_argument("--mode", choices=["batch", "replay"],
                   default=os.getenv("SIM_MODE", "batch"))
    p.add_argument("--speed", type=float,
                   default=float(os.getenv("SIM_SPEED", "1.0")))
    p.add_argument("--reset-profiles", action="store_true", default=False)
    p.add_argument("--nb-abonnes", type=int,
                   default=int(os.getenv("SIM_NB_ABONNES", str(NB_ABONNES))))
    p.add_argument("--seed", type=int,
                   default=int(os.getenv("SIM_SEED", str(SEED))))
    p.add_argument("--duree-jours", type=int,
                   default=int(os.getenv("SIM_DUREE_JOURS", str(DUREE_SIMULATION_JOURS))))
    return p.parse_args()


def main() -> None:
    args = parse_args()

    logger.info("═══════════════════════════════════════════════")
    logger.info("  CyberGuardian AI — Simulateur v3.0           ")
    logger.info("  ENV=%s | mode=%s | seed=%d | %d abonnés | %d jours",
                os.getenv("ENV", "local"), args.mode, args.seed,
                args.nb_abonnes, args.duree_jours)
    logger.info("  Période : %s → +%d jours",
                DATE_DEBUT_SIMULATION, args.duree_jours)
    logger.info("═══════════════════════════════════════════════")

    # ── 1. Générer les comptes ────────────────────────────────
    logger.info("Génération de %d comptes…", args.nb_abonnes)
    t0 = time.time()
    comptes = generate_subscribers(args.nb_abonnes, seed=args.seed)
    logger.info("  → %d comptes en %.2fs", len(comptes), time.time() - t0)

    # ── 2. Initialiser les profils Redis ──────────────────────
    if args.reset_profiles:
        logger.info("Réinitialisation des profils…")
        reset_profiles(comptes)
    else:
        logger.info("Initialisation des profils (skip si existants)…")
        init_profiles(comptes)

    # ── 3. Planifier la simulation 30 jours ───────────────────
    logger.info("Planification de %d jours de simulation…", args.duree_jours)
    t0 = time.time()
    scenarios = planifier_simulation(comptes, seed=args.seed)
    logger.info("  → planifié en %.2fs", time.time() - t0)

    # ── 4. Publier dans Kafka ─────────────────────────────────
    mode = PublishMode.REPLAY if args.mode == "replay" else PublishMode.BATCH
    publisher = EventPublisher(mode=mode, speed_factor=args.speed)

    logger.info("Publication (mode=%s)…", args.mode)
    t0 = time.time()
    rapport = publisher.publish_all(scenarios, log_every=500)
    elapsed = time.time() - t0

    # ── 5. Rapport ────────────────────────────────────────────
    logger.info("═══════════════════════════════════════════════")
    logger.info("  RAPPORT DE SIMULATION")
    logger.info("  Scénarios  : %d", rapport["total_scenarios"])
    logger.info("  ├─ Fraudes : %d (%.1f%%)",
                rapport["fraud_scenarios"],
                rapport["fraud_scenarios"] / max(1, rapport["total_scenarios"]) * 100)
    logger.info("  └─ Légitimes: %d", rapport["legit_scenarios"])
    logger.info("  Événements : %d", rapport["total_events"])
    logger.info("  ├─ transactions : %d", rapport.get("nb_transactions", 0))
    logger.info("  ├─ sim-events   : %d", rapport.get("nb_sim_events", 0))
    logger.info("  └─ otp-events   : %d", rapport.get("nb_otp_events", 0))
    logger.info("  Erreurs    : %d", rapport["errors"])
    logger.info("  Durée      : %.2fs", elapsed)
    logger.info("═══════════════════════════════════════════════")

    if rapport["errors"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
