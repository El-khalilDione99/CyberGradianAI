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
# Relevé de 3% à 10% (finding #4) : à 3%, trop peu d'abonnés produisaient une
# transaction légitime peu après un swap pour contrebalancer les fraudes qui,
# elles, suivent TOUJOURS un swap récent — le modèle apprenait "swap récent
# = fraude" à ~100%. Un premier essai à 6% (~22 transactions post-swap au
# total, 4 seulement dans le test) suffisait déjà à faire chuter la
# dépendance à `hours_since_sim_swap` (AUC-PR sans cette feature : 0.63→0.98)
# mais restait un échantillon fin ; 10% donne une population plus solide.
TAUX_SCENARIO_SWAP_LEGITIME           = float(os.getenv("SIM_TAUX_SWAP_LEGITIME",   "0.10"))   # 10%
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

# Poids de population approximatifs (ordre de grandeur indicatif, pas des
# chiffres officiels) : sans ça, un tirage uniforme sur les 14 régions
# sous-représentait fortement Dakar, qui concentre la plus grosse part de la
# population sénégalaise réelle (finding #6). Utilisé uniquement pour tirer
# la région d'un abonné — n'importe pas sur la logique de détection.
POIDS_REGIONS = [
    24,  10,  6,   6,   5,
    6,   8,   5,   5,   5,
    2,   4,   5,   4,
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
TYPES_ATTAQUE = ["SIM_SWAP_SIMPLE", "SIM_SWAP_CASCADE", "PIC_OTP", "SIM_SWAP_DISCRET"]
POIDS_ATTAQUE = [0.30,              0.35,               0.15,      0.20]

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

# Montant fraude = facteur × montant_moyen_habituel (inclut micro-fraudes et gros vidages)
FACTEUR_MONTANT_FRAUDE_MIN = 0.6
FACTEUR_MONTANT_FRAUDE_MAX = 10.0

# ── Heures actives par segment ───────────────────────────────
HEURES_ACTIVES = {
    "bas":   list(range(8, 18)),
    "moyen": list(range(7, 21)),
    "haut":  list(range(6, 23)),
}
