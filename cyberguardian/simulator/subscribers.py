"""
simulator/subscribers.py
────────────────────────
Génération des 500 comptes abonnés simulés.

Modèle Compte :
  - id_compte       : hash SHA256 du msisdn (jamais le numéro en clair)
  - iccid_actuel    : numéro de série SIM physique (19 chiffres)
  - imsi_actuel     : identifiant réseau SIM (15 chiffres)
  - montant_moyen   : tiré d'une loi log-normale (réaliste)
  - ecart_type      : dispersion autour du montant moyen
  - region          : région sénégalaise
  - antenne_domicile: antenne habituelle de résidence
  - device_id       : identifiant appareil principal
  - solde           : solde initial
  - date_creation   : date d'ouverture du compte
"""

import hashlib
import random
import string
from dataclasses import dataclass, field
from datetime import date, timedelta

import numpy as np

from simulator.config import (
    SEED, NB_ABONNES, PREFIXES_ORANGE, SEGMENTS,
    ANTENNES_PAR_REGION, REGIONS, HEURES_ACTIVES,
    get_date_debut, DUREE_SIMULATION_JOURS,
)


# ── Helpers ──────────────────────────────────────────────────

def _hacher_msisdn(msisdn: str) -> str:
    """Hash SHA256 du msisdn — jamais stocké en clair."""
    return "CPT-" + hashlib.sha256(msisdn.encode()).hexdigest()[:12]


def _generer_iccid(rng: random.Random) -> str:
    """ICCID : 19 chiffres (numéro de série SIM physique)."""
    return "".join([str(rng.randint(0, 9)) for _ in range(19)])


def _generer_imsi(rng: random.Random) -> str:
    """IMSI : 15 chiffres (identifiant réseau SIM)."""
    # Préfixe Sénégal : 608 (Orange) + 12 chiffres
    return "608" + "".join([str(rng.randint(0, 9)) for _ in range(12)])


def _generer_device_id(rng: random.Random) -> str:
    """Identifiant appareil : DEV- + 8 caractères hex."""
    return "DEV-" + "".join(rng.choices(string.hexdigits[:16], k=8)).upper()


def _montant_lognormal(rng: random.Random, mu: float, sigma: float) -> float:
    """Tire un montant depuis une loi log-normale."""
    return float(np.random.default_rng(rng.randint(0, 2**31)).lognormal(mu, sigma))


# ── Modèle Compte ────────────────────────────────────────────

@dataclass
class Compte:
    id_compte:             str           # CPT-<hash12> — jamais le msisdn
    iccid_actuel:          str           # numéro de série SIM (19 chiffres)
    imsi_actuel:           str           # identifiant réseau SIM (15 chiffres)
    region:                str           # région sénégalaise
    antenne_domicile:      str           # antenne habituelle de résidence
    segment:               str           # bas | moyen | haut
    device_id_habituel:    str           # DEV-<hex8>
    montant_moyen_habituel: float        # montant moyen en XOF (loi log-normale)
    ecart_type_montant:    float         # dispersion des montants
    solde:                 float         # solde courant (XOF)
    date_creation_compte:  date          # date d'ouverture du compte
    heures_actives:        list[int]     = field(default_factory=list)
    beneficiaires_habituels: list[str]   = field(default_factory=list)  # id_compte des bénéficiaires

    def to_dict(self) -> dict:
        return {
            "id_compte":              self.id_compte,
            "iccid_actuel":           self.iccid_actuel,
            "imsi_actuel":            self.imsi_actuel,
            "region":                 self.region,
            "antenne_domicile":       self.antenne_domicile,
            "segment":                self.segment,
            "device_id_habituel":     self.device_id_habituel,
            "montant_moyen_habituel": round(self.montant_moyen_habituel, 2),
            "ecart_type_montant":     round(self.ecart_type_montant, 2),
            "solde":                  round(self.solde, 2),
            "date_creation_compte":   self.date_creation_compte.isoformat(),
            "heures_actives":         self.heures_actives,
            "beneficiaires_habituels": self.beneficiaires_habituels,
        }


# ── Génération ───────────────────────────────────────────────

def generate_subscribers(n: int = NB_ABONNES, seed: int = SEED) -> list[Compte]:
    """
    Génère n comptes abonnés réalistes avec seed fixe.
    Le msisdn est haché immédiatement — jamais conservé en clair.
    Seeds fixées → démo rejouable à l'identique.
    """
    rng = random.Random(seed)
    np.random.seed(seed)
    comptes = []

    noms_segments = list(SEGMENTS.keys())
    poids_segments = [SEGMENTS[s]["weight"] for s in noms_segments]

    date_debut = get_date_debut().date()

    for i in range(n):
        # Msisdn → haché immédiatement
        prefix = rng.choice(PREFIXES_ORANGE)
        numero = "".join([str(rng.randint(0, 9)) for _ in range(7)])
        id_compte = _hacher_msisdn(f"221{prefix}{numero}")

        # Géographie
        region = rng.choice(REGIONS)
        antenne_domicile = rng.choice(ANTENNES_PAR_REGION[region])

        # Segment de revenus
        segment = rng.choices(noms_segments, weights=poids_segments, k=1)[0]
        seg = SEGMENTS[segment]

        # Montant moyen log-normal
        montant_moyen = max(500.0, _montant_lognormal(rng, seg["mu"], seg["sigma"]))
        ecart_type    = montant_moyen * rng.uniform(0.2, 0.6)

        # Solde initial réaliste
        solde = rng.uniform(seg["solde_min"], seg["solde_max"])

        # SIM
        iccid = _generer_iccid(rng)
        imsi  = _generer_imsi(rng)

        # Appareil
        device_id = _generer_device_id(rng)

        # Date de création du compte (entre 1 et 3 ans avant la simulation)
        jours_anciennete = rng.randint(30, 1095)
        date_creation = date_debut - timedelta(days=jours_anciennete)

        # Heures actives
        heures = HEURES_ACTIVES[segment]
        # Sous-ensemble aléatoire des heures actives du segment
        nb_heures = rng.randint(4, len(heures))
        debut_h = rng.randint(0, len(heures) - nb_heures)
        heures_actives = heures[debut_h: debut_h + nb_heures]

        comptes.append(Compte(
            id_compte=id_compte,
            iccid_actuel=iccid,
            imsi_actuel=imsi,
            region=region,
            antenne_domicile=antenne_domicile,
            segment=segment,
            device_id_habituel=device_id,
            montant_moyen_habituel=montant_moyen,
            ecart_type_montant=ecart_type,
            solde=solde,
            date_creation_compte=date_creation,
            heures_actives=heures_actives,
            beneficiaires_habituels=[],
        ))

    # Liens bénéficiaires réalistes (2 à 5 par compte)
    tous_ids = [c.id_compte for c in comptes]
    for compte in comptes:
        nb = rng.randint(2, 5)
        candidats = [cid for cid in tous_ids if cid != compte.id_compte]
        compte.beneficiaires_habituels = rng.sample(candidats, min(nb, len(candidats)))

    return comptes


if __name__ == "__main__":
    comptes = generate_subscribers()
    print(f"{len(comptes)} comptes générés")
    print(f"Exemple : {comptes[0].to_dict()}")
