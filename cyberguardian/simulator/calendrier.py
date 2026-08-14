"""
simulator/calendrier.py
───────────────────────
Génère le calendrier complet de la simulation sur 30 jours.

Responsabilités :
  1. Planifier les transactions normales de chaque compte (loi de Poisson)
  2. Injecter les scénarios spéciaux (fraude + légitimes faux positifs)
     à des dates aléatoires dans la fenêtre
  3. Retourner tous les scénarios triés par horodatage

L'ordre chronologique est garanti — le feature updater pourra
construire les profils Welford de façon cohérente.
"""

import random
from datetime import datetime, timedelta

import numpy as np

from simulator.subscribers import Compte
from simulator.scenarios import (
    Scenario, TypeScenario,
    build_sim_swap_simple, build_sim_swap_cascade, build_pic_otp,
    build_swap_legitime, build_nouveau_device_legitime,
    build_gros_montant_legitime, build_voyage_legitime,
    build_transaction_normale,
)
from simulator.config import (
    SEED, DUREE_SIMULATION_JOURS, TRANSACTIONS_PAR_JOUR_LAMBDA,
    TAUX_SCENARIO_FRAUDE, TAUX_SCENARIO_SWAP_LEGITIME,
    TAUX_SCENARIO_NOUVEAU_DEVICE_LEGITIME, TAUX_SCENARIO_GROS_MONTANT_LEGITIME,
    TAUX_TRANSACTION_VOYAGE_LEGITIME,
    TYPES_ATTAQUE, POIDS_ATTAQUE,
    get_date_debut,
)


def _heure_aleatoire(rng: random.Random, compte: Compte) -> int:
    """Tire une heure dans les heures actives de l'abonné."""
    return rng.choice(compte.heures_actives) if compte.heures_actives else rng.randint(7, 21)


def _ts_jour(rng: random.Random, compte: Compte, date_debut: datetime, jour: int) -> datetime:
    """Génère un timestamp réaliste pour un jour donné."""
    heure   = _heure_aleatoire(rng, compte)
    minutes = rng.randint(0, 59)
    secondes = rng.randint(0, 59)
    return date_debut + timedelta(days=jour, hours=heure, minutes=minutes, seconds=secondes)


def planifier_simulation(
    comptes: list[Compte],
    seed: int = SEED,
) -> list[Scenario]:
    """
    Génère l'ensemble des scénarios sur DUREE_SIMULATION_JOURS jours.

    Retourne une liste de Scenario triée par horodatage du premier événement.
    """
    rng = random.Random(seed)
    np.random.seed(seed)

    date_debut = get_date_debut()
    nb_jours   = DUREE_SIMULATION_JOURS
    tous_scenarios: list[Scenario] = []

    # ── Sélectionner les comptes pour chaque type de scénario ──
    nb_fraude      = max(1, int(len(comptes) * TAUX_SCENARIO_FRAUDE))
    nb_swap_leg    = max(1, int(len(comptes) * TAUX_SCENARIO_SWAP_LEGITIME))
    nb_new_device  = max(1, int(len(comptes) * TAUX_SCENARIO_NOUVEAU_DEVICE_LEGITIME))
    nb_gros_mt     = max(1, int(len(comptes) * TAUX_SCENARIO_GROS_MONTANT_LEGITIME))

    comptes_fraude     = set(c.id_compte for c in rng.sample(comptes, nb_fraude))
    comptes_swap_leg   = set(c.id_compte for c in rng.sample(comptes, nb_swap_leg))
    comptes_new_device = set(c.id_compte for c in rng.sample(comptes, nb_new_device))
    comptes_gros_mt    = set(c.id_compte for c in rng.sample(comptes, nb_gros_mt))

    for compte in comptes:

        # ── 1. Transactions normales (loi de Poisson) ──────────
        for jour in range(nb_jours):
            nb_tx_jour = np.random.poisson(TRANSACTIONS_PAR_JOUR_LAMBDA)
            for _ in range(nb_tx_jour):
                ts = _ts_jour(rng, compte, date_debut, jour)
                # Voyage légitime (4% des transactions)
                if rng.random() < TAUX_TRANSACTION_VOYAGE_LEGITIME:
                    tous_scenarios.append(build_voyage_legitime(compte, rng, ts))
                else:
                    tous_scenarios.append(build_transaction_normale(compte, rng, ts))

        # ── 2. Scénario(s) fraude ──────────────────────────────
        if compte.id_compte in comptes_fraude:
            nb_attaques = rng.choices([1, 2, 3, 4, 5], weights=[0.30, 0.30, 0.20, 0.12, 0.08], k=1)[0]
            for _ in range(nb_attaques):
                poids_jours = [1.0 + 0.5 * (j / nb_jours) for j in range(nb_jours)]
                jour_fraude = rng.choices(range(nb_jours), weights=poids_jours, k=1)[0]
                ts_fraude   = _ts_jour(rng, compte, date_debut, jour_fraude)

                # Recharger le solde avant l'attaque — le fraudeur cible un compte avec de l'argent
                # On restaure le solde initial du segment pour garantir des transactions fraudes
                from simulator.config import SEGMENTS
                seg_data = SEGMENTS[compte.segment]
                if compte.solde < seg_data["solde_min"]:
                    compte.solde = rng.uniform(seg_data["solde_min"], seg_data["solde_min"] * 3)

                type_attaque = rng.choices(TYPES_ATTAQUE, weights=POIDS_ATTAQUE, k=1)[0]
                if type_attaque == "SIM_SWAP_SIMPLE":
                    tous_scenarios.append(build_sim_swap_simple(compte, rng, ts_fraude))
                elif type_attaque == "SIM_SWAP_CASCADE":
                    tous_scenarios.append(build_sim_swap_cascade(compte, rng, ts_fraude))
                else:
                    tous_scenarios.append(build_pic_otp(compte, rng, ts_fraude))

        # ── 3. Swap légitime ───────────────────────────────────
        if compte.id_compte in comptes_swap_leg and compte.id_compte not in comptes_fraude:
            jour  = rng.randint(0, nb_jours - 1)
            ts    = _ts_jour(rng, compte, date_debut, jour)
            tous_scenarios.append(build_swap_legitime(compte, rng, ts))

        # ── 4. Nouveau device légitime ─────────────────────────
        if compte.id_compte in comptes_new_device:
            jour  = rng.randint(0, nb_jours - 1)
            ts    = _ts_jour(rng, compte, date_debut, jour)
            tous_scenarios.append(build_nouveau_device_legitime(compte, rng, ts))

        # ── 5. Gros montant légitime ───────────────────────────
        if compte.id_compte in comptes_gros_mt:
            jour  = rng.randint(0, nb_jours - 1)
            ts    = _ts_jour(rng, compte, date_debut, jour)
            tous_scenarios.append(build_gros_montant_legitime(compte, rng, ts))

    # ── Trier tous les scénarios par horodatage du 1er événement ──
    tous_scenarios.sort(
        key=lambda s: s.evenements[0].horodatage if s.evenements else date_debut
    )

    # ── Rapport ────────────────────────────────────────────────
    fraudes  = sum(1 for s in tous_scenarios if s.est_fraude)
    legitimes = len(tous_scenarios) - fraudes
    total_ev = sum(len(s.evenements) for s in tous_scenarios)

    print(f"Simulation {nb_jours} jours planifiée :")
    print(f"  Scénarios : {len(tous_scenarios):>6}  (fraudes={fraudes}, légitimes={legitimes})")
    print(f"  Événements: {total_ev:>6}")

    return tous_scenarios
