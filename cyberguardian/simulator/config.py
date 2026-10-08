"""
simulator/config.py
───────────────────
Tous les paramètres de simulation centralisés.
Chaque paramètre est surchargeable via variable d'environnement.
"""

import os
from datetime import date, datetime, timezone


# ── Reproductibilité ─────────────────────────────────────────
SEED = int(os.getenv("SIM_SEED", "42"))

# ── Population ───────────────────────────────────────────────
NB_ABONNES = int(os.getenv("SIM_NB_ABONNES", "500"))

# ── Fenêtre temporelle ───────────────────────────────────────
DUREE_SIMULATION_JOURS = int(os.getenv("SIM_DUREE_JOURS", "30"))
DATE_DEBUT_SIMULATION  = os.getenv("SIM_DATE_DEBUT", "2026-07-01")

def get_date_debut() -> datetime:
    return datetime.strptime(DATE_DEBUT_SIMULATION, "%Y-%m-%d").replace(tzinfo=timezone.utc)

# ── Taux de scénarios ────────────────────────────────────────
# Proportion de comptes qui reçoivent chaque type de scénario
TAUX_SCENARIO_FRAUDE                  = float(os.getenv("SIM_TAUX_FRAUDE",          "0.20"))   # 20%
# Les swaps légitimes (perte, vol, changement de téléphone, eSIM) sont en réalité
# bien plus fréquents que les swaps frauduleux : sans cela, « swap » ≈ « fraude ».
TAUX_SCENARIO_SWAP_LEGITIME           = float(os.getenv("SIM_TAUX_SWAP_LEGITIME",   "0.20"))   # 20%
TAUX_SCENARIO_NOUVEAU_DEVICE_LEGITIME = float(os.getenv("SIM_TAUX_DEVICE_LEGITIME", "0.05"))   # 5%
TAUX_SCENARIO_GROS_MONTANT_LEGITIME   = float(os.getenv("SIM_TAUX_GROS_MONTANT",    "0.02"))   # 2%
TAUX_TRANSACTION_VOYAGE_LEGITIME      = float(os.getenv("SIM_TAUX_VOYAGE",          "0.04"))   # 4%

# ── Comportement transactionnel ──────────────────────────────
# Loi de Poisson : nombre moyen de transactions par jour et par compte
TRANSACTIONS_PAR_JOUR_LAMBDA = float(os.getenv("SIM_TRANSACTIONS_JOUR", "1.5"))

# Distribution des types de transaction
TYPES_TRANSACTION = ["transfert", "paiement", "retrait", "depot"]
POIDS_TYPES       = [0.50,         0.25,       0.20,     0.05]

# Distribution des canaux de transaction
CANAUX_TRANSACTION = ["mobile_app", "ussd", "agent"]
POIDS_CANAUX       = [0.60,          0.30,   0.10]

# ── Segments de revenus (montant moyen log-normal) ───────────
# mu et sigma de la loi log-normale sur le montant en XOF
SEGMENTS = {
    "bas":   {"mu": 8.5,  "sigma": 0.8,  "weight": 0.45,
              "solde_min": 1_000,   "solde_max": 50_000},
    "moyen": {"mu": 10.5, "sigma": 0.7,  "weight": 0.40,
              "solde_min": 20_000,  "solde_max": 300_000},
    "haut":  {"mu": 12.5, "sigma": 0.6,  "weight": 0.15,
              "solde_min": 100_000, "solde_max": 2_000_000},
}

# ── Géographie ───────────────────────────────────────────────
REGIONS = [
    "dakar", "thies", "saint_louis", "kaolack", "ziguinchor",
    "tambacounda", "diourbel", "louga", "fatick", "kolda",
    "kedougou", "sedhiou", "kaffrine", "matam",
]

# Antennes par région (3 à 6 antennes par région)
ANTENNES_PAR_REGION: dict[str, list[str]] = {
    region: [f"{region.upper()[:3]}-ANT-{i:03d}" for i in range(1, n + 1)]
    for region, n in [
        ("dakar", 6), ("thies", 4), ("saint_louis", 4), ("kaolack", 4),
        ("ziguinchor", 3), ("tambacounda", 3), ("diourbel", 3), ("louga", 3),
        ("fatick", 3), ("kolda", 3), ("kedougou", 3), ("sedhiou", 3),
        ("kaffrine", 3), ("matam", 3),
    ]
}

# Préfixes Orange Sénégal
PREFIXES_ORANGE = ["77", "78", "76", "70"]

# ── Scénarios de fraude ──────────────────────────────────────
# Types d'attaque et leurs poids (tous basés sur SIM Swap)
# VOL_TELEPHONE : fraude SANS swap SIM (téléphone déverrouillé volé : appareil
# habituel, antenne locale, pas d'OTP de swap) — lot 4.
TYPES_ATTAQUE = ["SIM_SWAP_SIMPLE", "SIM_SWAP_CASCADE", "PIC_OTP", "SIM_SWAP_DISCRET", "VOL_TELEPHONE"]
POIDS_ATTAQUE = [0.25,              0.30,               0.10,      0.15,               0.20]

# Nombre d'attaques subies par un compte victime sur la période (1 ou 2).
# Auparavant 1 à 5 : au-delà de 2 swaps, nb_swaps_30j désignait la fraude à coup sûr,
# alors qu'un compte légitime fait au plus 2 swaps (NB_SWAPS_LEGITIMES).
NB_ATTAQUES_PAR_COMPTE    = [1, 2]
POIDS_ATTAQUES_PAR_COMPTE = [0.80, 0.20]

# Délai swap → 1re transaction frauduleuse (SIMPLE, CASCADE, PIC_OTP), loi LOG-UNIFORME :
# la plupart des attaquants agissent vite (médiane ≈ 11 min) mais la queue s'étale
# jusqu'à 2 h. Auparavant uniforme 1-10 min : signal réel mais trop net.
DELAI_TX_FRAUDE_MIN = 1.0      # minutes
DELAI_TX_FRAUDE_MAX = 120.0    # minutes

# Canal du swap selon type (fraude vs légitime)
CANAUX_SWAP_FRAUDE   = ["agence", "self_service", "centre_appel"]
POIDS_SWAP_FRAUDE    = [0.60,      0.30,           0.10]
CANAUX_SWAP_LEGITIME = ["agence", "self_service", "centre_appel"]
POIDS_SWAP_LEGITIME  = [0.40,      0.45,           0.15]

# Délai OTP → swap en minutes (chevauchement réaliste entre 2 min et 45 min)
DELAI_OTP_SWAP_FRAUDE_MIN   = 1.0    # précipitation ou attaques différées (1 à 45 min)
DELAI_OTP_SWAP_FRAUDE_MAX   = 45.0
DELAI_OTP_SWAP_LEGITIME_MIN = 2.0    # réactivation rapide en agence ou normale (2 à 120 min)
DELAI_OTP_SWAP_LEGITIME_MAX = 120.0

# Swap légitime : nombre de swaps dans le mois pour un compte concerné (1 ou 2)
NB_SWAPS_LEGITIMES    = [1, 2]
POIDS_SWAPS_LEGITIMES = [0.75, 0.25]

# Après un swap légitime, le vrai titulaire se sert de sa ligne : transactions
# normales peu après le swap (sinon « transaction juste après un swap » ≈ fraude).
PROBA_TX_APRES_SWAP_LEGITIME      = float(os.getenv("SIM_PROBA_TX_APRES_SWAP_LEG", "0.80"))
NB_TX_APRES_SWAP_LEGITIME_MAX     = 3
DELAI_TX_APRES_SWAP_LEGITIME_MIN  = 5.0     # minutes
DELAI_TX_APRES_SWAP_LEGITIME_MAX  = 360.0   # 6 h

# Swap légitime fait en voyage (autre région) ; les transactions qui suivent ont
# lieu dans cette même région. Crée des cas légitimes « swap récent + autre région ».
PROBA_SWAP_LEGITIME_EN_VOYAGE = float(os.getenv("SIM_PROBA_SWAP_LEG_VOYAGE", "0.15"))

# Part d'attaquants LOCAUX (antenne dans la région de la victime) par type de fraude.
# Moyenne pondérée par POIDS_ATTAQUE ≈ 45,5 %. Sans cela, « swap récent + autre
# région » est une signature parfaite de la fraude.
PROBA_ATTAQUANT_LOCAL = {
    "SIM_SWAP_SIMPLE":  0.50,   # réseaux locaux, complicité / ingénierie sociale de proximité
    "SIM_SWAP_CASCADE": 0.40,   # vidage organisé, souvent piloté à distance
    "PIC_OTP":          0.30,   # harcèlement OTP / hameçonnage mené à distance
    "SIM_SWAP_DISCRET": 0.60,   # l'attaquant imite la victime, y compris sa zone
    "VOL_TELEPHONE":    1.00,   # le voleur est là où se trouve le téléphone
}

# Attaque discrète : l'attaquant attend avant d'agir (de 15 min à 12 h)
DELAI_TX_FRAUDE_DISCRETE_MIN = 15.0    # minutes
DELAI_TX_FRAUDE_DISCRETE_MAX = 720.0   # 12 h

# ── Lot 2 : montants et solde ────────────────────────────────
# Montant fraude = facteur × montant_moyen_habituel (plafonné par le solde) :
# surtout proche de l'habitude (×0,8–3), parfois fort (×3–8).
FACTEUR_MONTANT_FRAUDE_COURANT = (0.8, 3.0)
FACTEUR_MONTANT_FRAUDE_FORT    = (3.0, 8.0)
PROBA_MONTANT_FRAUDE_FORT      = 0.30
FACTEUR_MONTANT_FRAUDE_MIN = FACTEUR_MONTANT_FRAUDE_COURANT[0]   # rétrocompatibilité
FACTEUR_MONTANT_FRAUDE_MAX = FACTEUR_MONTANT_FRAUDE_FORT[1]

# Transactions légitimes inhabituelles, dans la vie courante (pas un scénario à part)
PROBA_GROS_MONTANT_COURANT   = 0.05           # achat important : ×1,5–4 l'habitude
FACTEUR_GROS_MONTANT_COURANT = (1.5, 4.0)
PROBA_GROS_RETRAIT_COURANT   = 0.02           # retrait / transfert vidant 50–90 % du solde
PART_SOLDE_GROS_RETRAIT      = (0.5, 0.9)

# ── Lot 1 : heures et OTP ────────────────────────────────────
# Part des transactions hors des heures actives de l'abonné (sinon is_active_hour
# vaut toujours 1 pour les légitimes et 0 signale la fraude à coup sûr).
PROBA_TX_LEGITIME_HORS_HEURES = 0.12
PROBA_FRAUDE_HORS_HEURES      = 0.35

# OTP de la vie courante : paiement / connexion (auparavant, OTP = swap uniquement)
PROBA_OTP_PAIEMENT            = 0.30          # OTP 0,2–3 min avant une transaction (légitime ou fraude)
PROBA_RAFALE_OTP_LEGITIME_JOUR = 0.02         # par compte et par jour : 2–5 OTP rapprochés (code mal saisi)

# Nombre d'OTP de confirmation autour d'un swap (0 à 4), hors PIC_OTP qui reste une rafale
NB_OTP_SWAP_FRAUDE     = [0, 1, 2, 3, 4]
POIDS_OTP_SWAP_FRAUDE  = [0.15, 0.45, 0.20, 0.12, 0.08]
NB_OTP_SWAP_LEGITIME   = [0, 1, 2, 3]
POIDS_OTP_SWAP_LEGITIME = [0.10, 0.60, 0.20, 0.10]

# ── Lot 3 : appareils, cascade, profils ──────────────────────
# Part des fraudes depuis un NOUVEL appareil (sinon l'appareil habituel de la victime)
PROBA_NOUVEAU_DEVICE_FRAUDE = {
    "SIM_SWAP_SIMPLE": 0.50, "SIM_SWAP_CASCADE": 0.50, "PIC_OTP": 0.50,
    "SIM_SWAP_DISCRET": 0.30, "VOL_TELEPHONE": 0.0,
}
PROBA_BENEFICIAIRE_NOUVEAU_FRAUDE = 0.60

# Cascade : 1 à 8 virements, intervalles et montants variables
NB_VIREMENTS_CASCADE    = [1, 2, 3, 4, 5, 6, 7, 8]
POIDS_VIREMENTS_CASCADE = [0.10, 0.15, 0.20, 0.20, 0.15, 0.10, 0.05, 0.05]
INTERVALLE_CASCADE_MIN  = (1.0, 30.0)         # intervalle de base tiré par cascade, en minutes
PART_SOLDE_CASCADE      = {"petits": (0.05, 0.15), "moyens": (0.20, 0.50)}

# Profils d'activité (multiplicateur de TRANSACTIONS_PAR_JOUR_LAMBDA)
NIVEAUX_ACTIVITE       = [0.5, 1.0, 3.0]      # faible, moyen, gros consommateur
POIDS_NIVEAUX_ACTIVITE = [0.40, 0.45, 0.15]
PROBA_RAFALE_TX_JOUR   = 0.03                 # par jour : 3–5 transactions légitimes en moins d'1 h
PROBA_DEVICE_SECONDAIRE = 0.15                # comptes avec un 2e appareil (tablette, téléphone pro)
PART_TX_DEVICE_SECONDAIRE = 0.20

# ── Lot 4 : vol de téléphone déverrouillé (fraude sans swap) ─
NB_TX_VOL_TELEPHONE        = (1, 4)
DELAI_TX_VOL_TELEPHONE_MIN = (2.0, 45.0)      # minutes entre deux transactions du voleur

# ── Heures actives par segment ───────────────────────────────
HEURES_ACTIVES = {
    "bas":   list(range(8, 18)),
    "moyen": list(range(7, 21)),
    "haut":  list(range(6, 23)),
}
