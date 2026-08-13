"""
simulator/subscribers.py
────────────────────────
Génération de 500 abonnés réalistes sénégalais.

Chaque abonné a :
  - un identifiant uniquement (jamais le numéro en clair)
  - un ou plusieurs appareils (IMEI)
  - une région géographique
  - une antenne habituelle (antenne_domicile)
  - un segment de revenus (détermine les montants habituels)
  - un seed fixe → résultats reproductibles
"""

import hashlib
import random
from dataclasses import dataclass, field

# ── Régions sénégalaises ─────────────────────────────────────

REGIONS = [
    "Dakar", "Thiès", "Diourbel", "Saint-Louis", "Louga",
    "Fatick", "Kaolack", "Kolda", "Ziguinchor", "Tambacounda",
    "Kédougou", "Sédhiou", "Kaffrine", "Matam",
]

# Antennes par région (format : REGION-ANT-XXX)
ANTENNAS_BY_REGION: dict[str, list[str]] = {
    region: [f"{region.upper()[:3]}-ANT-{i:03d}" for i in range(1, n + 1)]
    for region, n in [
        ("Dakar", 6), ("Thiès", 4), ("Diourbel", 3), ("Saint-Louis", 4),
        ("Louga", 3), ("Fatick", 3), ("Kaolack", 4), ("Kolda", 3),
        ("Ziguinchor", 3), ("Tambacounda", 3), ("Kédougou", 3),
        ("Sédhiou", 3), ("Kaffrine", 3), ("Matam", 3),
    ]
}

# Préfixes Orange Sénégal
PREFIXES_ORANGE = ["77", "78", "76", "70"]

# Segments de revenus → montants habituels en FCFA
SEGMENTS = {
    "bas":   {"min": 500,    "max": 10_000,  "weight": 0.45},
    "moyen": {"min": 5_000,  "max": 75_000,  "weight": 0.40},
    "haut":  {"min": 25_000, "max": 500_000, "weight": 0.15},
}

# Soldes initiaux par segment (FCFA)
SOLDES_PAR_SEGMENT = {
    "bas":   {"min": 1_000,   "max": 50_000},
    "moyen": {"min": 20_000,  "max": 300_000},
    "haut":  {"min": 100_000, "max": 2_000_000},
}


@dataclass
class Subscriber:
    identifiant: str          # SHA256(msisdn)[:16] — jamais le numéro en clair
    region: str
    antenne_domicile: str     # antenne habituelle de résidence
    segment: str              # "bas" | "moyen" | "haut"
    appareils: list[str]      # liste d'IMEIs (1 à 3)
    montant_min_habituel: int # montant minimum habituel (FCFA)
    montant_max_habituel: int # montant maximum habituel (FCFA)
    solde: int                # solde courant simulé (FCFA)
    heures_actives: list[int] = field(default_factory=list)
    beneficiaires_connus: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "identifiant": self.identifiant,
            "region": self.region,
            "antenne_domicile": self.antenne_domicile,
            "segment": self.segment,
            "appareils": self.appareils,
            "montant_min_habituel": self.montant_min_habituel,
            "montant_max_habituel": self.montant_max_habituel,
            "solde": self.solde,
            "heures_actives": self.heures_actives,
            "beneficiaires_connus": self.beneficiaires_connus,
        }


def _hacher_msisdn(msisdn: str) -> str:
    """On hache immédiatement — le numéro en clair n'est jamais stocké."""
    return hashlib.sha256(msisdn.encode()).hexdigest()[:16]


def _generer_imei(rng: random.Random) -> str:
    """Génère un IMEI fictif de 15 chiffres."""
    return "".join([str(rng.randint(0, 9)) for _ in range(15)])


def generate_subscribers(n: int = 500, seed: int = 42) -> list[Subscriber]:
    """
    Génère n abonnés réalistes avec seed fixe.
    Seeds fixées → démo rejouable à l'identique.
    Le msisdn est haché immédiatement et jamais conservé.
    """
    rng = random.Random(seed)
    abonnes = []

    noms_segments = list(SEGMENTS.keys())
    poids_segments = [SEGMENTS[s]["weight"] for s in noms_segments]

    for i in range(n):
        # Générer et hacher immédiatement le MSISDN
        prefix = rng.choice(PREFIXES_ORANGE)
        numero = "".join([str(rng.randint(0, 9)) for _ in range(7)])
        identifiant = _hacher_msisdn(f"221{prefix}{numero}")

        # Géographie
        region = rng.choice(REGIONS)
        antenne_domicile = rng.choice(ANTENNAS_BY_REGION[region])

        # Segment de revenus
        segment = rng.choices(noms_segments, weights=poids_segments, k=1)[0]
        donnees_seg = SEGMENTS[segment]
        donnees_solde = SOLDES_PAR_SEGMENT[segment]

        # Montants habituels
        montant_min = rng.randint(donnees_seg["min"], donnees_seg["min"] * 3)
        montant_max = rng.randint(donnees_seg["max"] // 2, donnees_seg["max"])

        # Solde initial réaliste
        solde = rng.randint(donnees_solde["min"], donnees_solde["max"])

        # Appareils (1 à 3, la plupart n'en ont qu'un)
        nb_appareils = rng.choices([1, 2, 3], weights=[0.75, 0.20, 0.05], k=1)[0]
        appareils = [_generer_imei(rng) for _ in range(nb_appareils)]

        # Horaires actifs
        heure_base = rng.randint(7, 20)
        etendue = rng.randint(3, 6)
        heures_actives = list(range(heure_base, min(heure_base + etendue, 23)))

        abonnes.append(Subscriber(
            identifiant=identifiant,
            region=region,
            antenne_domicile=antenne_domicile,
            segment=segment,
            appareils=appareils,
            montant_min_habituel=montant_min,
            montant_max_habituel=montant_max,
            solde=solde,
            heures_actives=heures_actives,
            beneficiaires_connus=[],
        ))

    # Liens bénéficiaires réalistes (2 à 8 personnes connues)
    tous_identifiants = [a.identifiant for a in abonnes]
    for ab in abonnes:
        nb = rng.randint(2, 8)
        candidats = [h for h in tous_identifiants if h != ab.identifiant]
        ab.beneficiaires_connus = rng.sample(candidats, min(nb, len(candidats)))

    return abonnes


if __name__ == "__main__":
    subs = generate_subscribers(500, seed=42)
    print(f"{len(subs)} abonnés générés")
    print(f"Exemple : {subs[0].to_dict()}")
