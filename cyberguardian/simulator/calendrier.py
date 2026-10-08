"""
simulator/calendrier.py
───────────────────────
Génère le calendrier complet de la simulation sur 30 jours avec évolution d'état chronologique.

Responsabilités :
  1. Planifier les transactions et scénarios de chaque compte
  2. Exécuter les événements dans l'ordre chronologique strict (mise à jour continue de l'état : solde, device, SIM)
  3. Recharger périodiquement les soldes (salaire/dépôts) pour maintenir l'activité réaliste
  4. Retourner tous les scénarios triés par horodatage

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
    build_sim_swap_discret, build_swap_legitime, build_nouveau_device_legitime,
    build_gros_montant_legitime, build_voyage_legitime,
    build_transaction_normale, build_vol_telephone, build_rafale_otp_legitime,
    varier_otp_swap, ajouter_otp_paiement,
)
from simulator.config import (
    SEED, DUREE_SIMULATION_JOURS, TRANSACTIONS_PAR_JOUR_LAMBDA,
    TAUX_SCENARIO_FRAUDE, TAUX_SCENARIO_SWAP_LEGITIME,
    TAUX_SCENARIO_NOUVEAU_DEVICE_LEGITIME, TAUX_SCENARIO_GROS_MONTANT_LEGITIME,
    TAUX_TRANSACTION_VOYAGE_LEGITIME,
    TYPES_ATTAQUE, POIDS_ATTAQUE, SEGMENTS,
    NB_SWAPS_LEGITIMES, POIDS_SWAPS_LEGITIMES,
    NB_ATTAQUES_PAR_COMPTE, POIDS_ATTAQUES_PAR_COMPTE,
    PROBA_TX_LEGITIME_HORS_HEURES, PROBA_FRAUDE_HORS_HEURES, PROBA_RAFALE_OTP_LEGITIME_JOUR,
    NIVEAUX_ACTIVITE, POIDS_NIVEAUX_ACTIVITE, PROBA_RAFALE_TX_JOUR,
    PROBA_DEVICE_SECONDAIRE, PART_TX_DEVICE_SECONDAIRE,
    get_date_debut,
)


def _heure_aleatoire(rng: random.Random, compte: Compte, proba_hors: float = 0.0) -> int:
    """
    Heure de la transaction : dans les heures actives de l'abonné, sauf avec la
    probabilité proba_hors où elle tombe en dehors (6 h – 23 h hors heures actives).
    """
    actives = compte.heures_actives or list(range(7, 22))
    if rng.random() < proba_hors:
        hors = [h for h in range(6, 24) if h not in actives] or list(range(24))
        return rng.choice(hors)
    return rng.choice(actives)


def _ts_jour(rng: random.Random, compte: Compte, date_debut: datetime, jour: int,
             proba_hors: float = 0.0) -> datetime:
    """Génère un timestamp réaliste pour un jour donné."""
    heure   = _heure_aleatoire(rng, compte, proba_hors)
    minutes = rng.randint(0, 59)
    secondes = rng.randint(0, 59)
    return date_debut + timedelta(days=jour, hours=heure, minutes=minutes, seconds=secondes)


_BUILDERS_FRAUDE = {
    "SIM_SWAP_SIMPLE":  build_sim_swap_simple,
    "SIM_SWAP_CASCADE": build_sim_swap_cascade,
    "PIC_OTP":          build_pic_otp,
    "SIM_SWAP_DISCRET": build_sim_swap_discret,
    "VOL_TELEPHONE":    build_vol_telephone,
}


def planifier_simulation(
    comptes: list[Compte],
    seed: int = SEED,
) -> list[Scenario]:
    """
    Génère l'ensemble des scénarios sur DUREE_SIMULATION_JOURS jours avec évolution d'état chronologique.

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

    # ── Profils d'activité et appareil secondaire (lot 3) ──────
    # Tirés ici, APRÈS l'écriture des profils initiaux : un 2e appareil acquis
    # pendant le mois apparaît donc comme « nouveau » à sa première utilisation.
    for compte in comptes:
        compte.niveau_activite = rng.choices(NIVEAUX_ACTIVITE, weights=POIDS_NIVEAUX_ACTIVITE, k=1)[0]
        if rng.random() < PROBA_DEVICE_SECONDAIRE:
            compte.device_secondaire = "DEV-" + "".join(rng.choices("0123456789ABCDEF", k=8))

    for compte in comptes:
        actions = []  # (timestamp, action_type, sub_type)
        lam = TRANSACTIONS_PAR_JOUR_LAMBDA * compte.niveau_activite

        # 1. Transactions normales de routine (parfois hors heures actives)
        for jour in range(nb_jours):
            for _ in range(np.random.poisson(lam)):
                ts = _ts_jour(rng, compte, date_debut, jour, PROBA_TX_LEGITIME_HORS_HEURES)
                if rng.random() < TAUX_TRANSACTION_VOYAGE_LEGITIME:
                    actions.append((ts, "VOYAGE_LEGITIME", None))
                else:
                    actions.append((ts, "NORMAL", None))
            # Rafale légitime : 3 à 5 transactions en moins d'une heure
            if rng.random() < PROBA_RAFALE_TX_JOUR:
                ts = _ts_jour(rng, compte, date_debut, jour, PROBA_TX_LEGITIME_HORS_HEURES)
                for _ in range(rng.randint(3, 5)):
                    ts += timedelta(minutes=rng.uniform(2, 15))
                    actions.append((ts, "NORMAL", None))
            # Rafale d'OTP légitime (code mal saisi, reconnexions)
            if rng.random() < PROBA_RAFALE_OTP_LEGITIME_JOUR:
                actions.append((_ts_jour(rng, compte, date_debut, jour, PROBA_TX_LEGITIME_HORS_HEURES),
                                "RAFALE_OTP", None))

        # 2. Fraude(s) — jour tiré uniformément, heure parfois hors heures actives
        if compte.id_compte in comptes_fraude:
            nb_attaques = rng.choices(NB_ATTAQUES_PAR_COMPTE, weights=POIDS_ATTAQUES_PAR_COMPTE, k=1)[0]
            for _ in range(nb_attaques):
                jour_fraude  = rng.randint(0, nb_jours - 1)
                ts_fraude    = _ts_jour(rng, compte, date_debut, jour_fraude, PROBA_FRAUDE_HORS_HEURES)
                type_attaque = rng.choices(TYPES_ATTAQUE, weights=POIDS_ATTAQUE, k=1)[0]
                actions.append((ts_fraude, "FRAUDE", type_attaque))

        # 3. Swap(s) légitime(s) — 1 ou 2 dans le mois, y compris pour un compte
        #    qui sera aussi victime (une victime peut avoir changé de SIM elle-même).
        if compte.id_compte in comptes_swap_leg:
            nb_swaps = rng.choices(NB_SWAPS_LEGITIMES, weights=POIDS_SWAPS_LEGITIMES, k=1)[0]
            for _ in range(nb_swaps):
                jour  = rng.randint(0, nb_jours - 1)
                ts    = _ts_jour(rng, compte, date_debut, jour, PROBA_TX_LEGITIME_HORS_HEURES)
                actions.append((ts, "SWAP_LEGITIME", None))

        # 4. Nouveau device légitime
        if compte.id_compte in comptes_new_device:
            jour  = rng.randint(0, nb_jours - 1)
            ts    = _ts_jour(rng, compte, date_debut, jour, PROBA_TX_LEGITIME_HORS_HEURES)
            actions.append((ts, "NOUVEAU_DEVICE_LEGITIME", None))

        # 5. Gros montant légitime
        if compte.id_compte in comptes_gros_mt:
            jour  = rng.randint(0, nb_jours - 1)
            ts    = _ts_jour(rng, compte, date_debut, jour, PROBA_TX_LEGITIME_HORS_HEURES)
            actions.append((ts, "GROS_MONTANT_LEGITIME", None))

        # Trier toutes les actions de l'abonné dans l'ordre chronologique exact
        actions.sort(key=lambda x: x[0])

        seg_data = SEGMENTS[compte.segment]

        # Exécuter les actions séquentiellement avec mise à jour d'état en temps réel
        for ts, act_type, sub_type in actions:
            # Si le solde est épuisé ou trop bas, recharger (salaire / dépôt d'argent).
            # Plus de recharge spéciale avant une fraude (le solde ne doit rien révéler).
            if compte.solde < 1_000:
                compte.solde += rng.uniform(seg_data["solde_min"], seg_data["solde_max"] * 0.5)

            if act_type == "NORMAL":
                device = (compte.device_secondaire
                          if compte.device_secondaire and rng.random() < PART_TX_DEVICE_SECONDAIRE else None)
                sc = build_transaction_normale(compte, rng, ts, device=device)
            elif act_type == "VOYAGE_LEGITIME":
                sc = build_voyage_legitime(compte, rng, ts)
            elif act_type == "SWAP_LEGITIME":
                sc = build_swap_legitime(compte, rng, ts)
                varier_otp_swap(sc, rng)
            elif act_type == "NOUVEAU_DEVICE_LEGITIME":
                sc = build_nouveau_device_legitime(compte, rng, ts)
            elif act_type == "GROS_MONTANT_LEGITIME":
                sc = build_gros_montant_legitime(compte, rng, ts)
            elif act_type == "RAFALE_OTP":
                sc = build_rafale_otp_legitime(compte, rng, ts)
            else:  # FRAUDE
                sc = _BUILDERS_FRAUDE[sub_type](compte, rng, ts)
                varier_otp_swap(sc, rng)
            ajouter_otp_paiement(sc, rng)       # OTP de paiement de la vie courante
            if sc.evenements:
                tous_scenarios.append(sc)

    # ── Trier tous les scénarios par horodatage du 1er événement ──
    tous_scenarios.sort(
        key=lambda s: s.evenements[0].horodatage if s.evenements else date_debut
    )

    # ── Rapport ────────────────────────────────────────────────
    fraudes   = sum(1 for s in tous_scenarios if s.est_fraude)
    legitimes = len(tous_scenarios) - fraudes
    total_ev  = sum(len(s.evenements) for s in tous_scenarios)

    print(f"Simulation {nb_jours} jours planifiée (Évolution chronologique d'état) :")
    print(f"  Scénarios : {len(tous_scenarios):>6}  (fraudes={fraudes}, légitimes={legitimes})")
    print(f"  Événements: {total_ev:>6}")

    return tous_scenarios
