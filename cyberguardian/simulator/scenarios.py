"""
simulator/scenarios.py
──────────────────────
Scénarios d'attaque SIM swap labellisés (IA-1).

Chaque scénario produit une séquence d'événements ordonnés dans le temps :
  1. sim_swap_event   → stream "sim-events"
  2. otp_event(s)     → stream "otp-events"
  3. transaction(s)   → stream "transactions"

Types de scénarios :
  - LEGITIME         : transaction normale (label=0)
  - SIM_SWAP_SIMPLE  : swap + 1 gros transfert (label=1)
  - SIM_SWAP_CASCADE : swap + plusieurs transferts rapides (label=1)
  - NOUVEAU_APPAREIL : nouveau device sans swap (label=0)
  - PIC_OTP          : pic d'OTP suspects (label=1)
"""

import random
import uuid
from datetime import datetime, timedelta, timezone
from enum import Enum
from dataclasses import dataclass
from simulator.subscribers import Subscriber, ANTENNAS_BY_REGION, REGIONS

# ── Canaux de transaction ────────────────────────────────────
CHANNELS = ["mobile_app", "ussd", "agent"]
POIDS_CHANNELS = [0.60, 0.30, 0.10]

# ── Identifiant opérateur générique ─────────────────────────
OPERATOR_ID = "OPERATOR_SIM"


class TypeScenario(str, Enum):
    LEGITIME         = "LEGITIME"
    SIM_SWAP_SIMPLE  = "SIM_SWAP_SIMPLE"
    SIM_SWAP_CASCADE = "SIM_SWAP_CASCADE"
    NOUVEAU_APPAREIL = "NOUVEAU_APPAREIL"
    PIC_OTP          = "PIC_OTP"


@dataclass
class Evenement:
    stream: str           # "sim-events" | "transactions" | "otp-events"
    cle_partition: str    # identifiant (msisdn_hash)
    payload: dict
    delai_secondes: float = 0.0


@dataclass
class Scenario:
    id_scenario: str
    type_scenario: TypeScenario
    abonne: Subscriber
    appareil_attaquant: str
    evenements: list[Evenement]
    est_fraude: bool


# ── Helpers ──────────────────────────────────────────────────

def _generer_id_transaction() -> str:
    return f"TX-{uuid.uuid4().hex[:12].upper()}"


def _antenne_aleatoire(rng: random.Random, region: str) -> str:
    """Retourne une antenne aléatoire de la région donnée."""
    return rng.choice(ANTENNAS_BY_REGION[region])


def _antenne_etrangere(rng: random.Random, region: str) -> str:
    """Retourne une antenne d'une région différente (attaquant distant)."""
    autres_regions = [r for r in REGIONS if r != region]
    region_etrangere = rng.choice(autres_regions)
    return rng.choice(ANTENNAS_BY_REGION[region_etrangere])


def _canal_aleatoire(rng: random.Random) -> str:
    return rng.choices(CHANNELS, weights=POIDS_CHANNELS, k=1)[0]


# ── Constructeurs d'événements ───────────────────────────────

def _creer_sim_event(
    abonne: Subscriber,
    appareil_attaquant: str,
    horodatage: datetime,
    rng: random.Random,
) -> dict:
    antenne_agence = _antenne_etrangere(rng, abonne.region)
    return {
        "type_evenement": "sim_swap",
        "id_evenement": f"SIM-{uuid.uuid4().hex[:10].upper()}",
        "identifiant": abonne.identifiant,
        "ancien_appareil": abonne.appareils[0],
        "nouvel_appareil": appareil_attaquant,
        "horodatage": horodatage.isoformat(),
        "agence": f"AGENCE-{rng.randint(1, 50):03d}",
        "antenne": antenne_agence,
        "operateur": OPERATOR_ID,
        "label": 1,
    }


def _creer_otp_event(
    abonne: Subscriber,
    appareil: str,
    horodatage: datetime,
    antenne: str,
) -> dict:
    return {
        "type_evenement": "otp_request",
        "id_evenement": f"OTP-{uuid.uuid4().hex[:10].upper()}",
        "identifiant": abonne.identifiant,
        "appareil": appareil,
        "antenne": antenne,
        "horodatage": horodatage.isoformat(),
    }


def _creer_transaction(
    abonne: Subscriber,
    appareil: str,
    montant: int,
    id_beneficiaire: str,
    horodatage: datetime,
    est_fraude: bool,
    antenne: str,
    rng: random.Random,
) -> dict:
    solde_avant = max(0, abonne.solde)
    solde_apres = max(0, solde_avant - montant)
    abonne.solde = solde_apres

    return {
        "type_evenement": "transaction",
        "id_transaction": _generer_id_transaction(),
        "identifiant": abonne.identifiant,
        "appareil": appareil,
        "montant_fcfa": montant,
        "identifiant_beneficiaire": id_beneficiaire,
        "horodatage": horodatage.isoformat(),
        "canal": _canal_aleatoire(rng),
        "antenne": antenne,
        "solde_avant": solde_avant,
        "solde_apres": solde_apres,
        "label": 1 if est_fraude else 0,
    }


# ── Scénario 1 : Transaction légitime ───────────────────────

def build_legitime(abonne: Subscriber, rng: random.Random, ts: datetime) -> Scenario:
    appareil = rng.choice(abonne.appareils)
    montant = rng.randint(abonne.montant_min_habituel,
                          min(abonne.montant_max_habituel, max(1, abonne.solde)))
    beneficiaire = rng.choice(abonne.beneficiaires_connus) if abonne.beneficiaires_connus else "inconnu"
    antenne = _antenne_aleatoire(rng, abonne.region)

    return Scenario(
        id_scenario=f"LEG-{uuid.uuid4().hex[:8]}",
        type_scenario=TypeScenario.LEGITIME,
        abonne=abonne,
        appareil_attaquant="",
        evenements=[Evenement(
            stream="transactions",
            cle_partition=abonne.identifiant,
            payload=_creer_transaction(abonne, appareil, montant, beneficiaire, ts, False, antenne, rng),
            delai_secondes=0,
        )],
        est_fraude=False,
    )


# ── Scénario 2 : SIM swap simple ────────────────────────────

def build_sim_swap_simple(
    abonne: Subscriber, rng: random.Random, ts: datetime, imei_attaquant: str
) -> Scenario:
    montant_max = max(1, min(abonne.montant_max_habituel * 15, abonne.solde))
    montant_min = max(1, min(abonne.montant_max_habituel * 5, montant_max))
    montant_fraude = rng.randint(montant_min, montant_max)
    id_beneficiaire_inconnu = f"INC-{uuid.uuid4().hex[:8]}"
    antenne_attaquant = _antenne_etrangere(rng, abonne.region)

    ts_otp = ts + timedelta(minutes=rng.uniform(1, 5))
    ts_tx  = ts + timedelta(minutes=rng.uniform(6, 15))

    return Scenario(
        id_scenario=f"SWP-{uuid.uuid4().hex[:8]}",
        type_scenario=TypeScenario.SIM_SWAP_SIMPLE,
        abonne=abonne,
        appareil_attaquant=imei_attaquant,
        evenements=[
            Evenement("sim-events", abonne.identifiant,
                      _creer_sim_event(abonne, imei_attaquant, ts, rng), 0),
            Evenement("otp-events", abonne.identifiant,
                      _creer_otp_event(abonne, imei_attaquant, ts_otp, antenne_attaquant),
                      (ts_otp - ts).total_seconds()),
            Evenement("transactions", abonne.identifiant,
                      _creer_transaction(abonne, imei_attaquant, montant_fraude,
                                         id_beneficiaire_inconnu, ts_tx, True, antenne_attaquant, rng),
                      (ts_tx - ts).total_seconds()),
        ],
        est_fraude=True,
    )


# ── Scénario 3 : SIM swap en cascade ────────────────────────

def build_sim_swap_cascade(
    abonne: Subscriber, rng: random.Random, ts: datetime, imei_attaquant: str
) -> Scenario:
    nb_transferts = rng.randint(3, 7)
    evenements = []
    antenne_attaquant = _antenne_etrangere(rng, abonne.region)

    evenements.append(Evenement("sim-events", abonne.identifiant,
                                _creer_sim_event(abonne, imei_attaquant, ts, rng), 0))

    for i in range(2):
        ts_otp = ts + timedelta(seconds=rng.uniform(30, 120) * (i + 1))
        evenements.append(Evenement("otp-events", abonne.identifiant,
                                    _creer_otp_event(abonne, imei_attaquant, ts_otp, antenne_attaquant),
                                    (ts_otp - ts).total_seconds()))

    ts_courant = ts + timedelta(minutes=5)
    for i in range(nb_transferts):
        if abonne.solde <= 0:
            break
        montant_min = max(1, int(abonne.solde * 0.10))
        montant_max = max(montant_min, int(abonne.solde * 0.40))
        montant = rng.randint(montant_min, montant_max)
        id_benef = f"INC-{uuid.uuid4().hex[:8]}"
        ts_tx = ts_courant + timedelta(seconds=rng.uniform(10, 60))
        evenements.append(Evenement("transactions", abonne.identifiant,
                                    _creer_transaction(abonne, imei_attaquant, montant,
                                                       id_benef, ts_tx, True, antenne_attaquant, rng),
                                    (ts_tx - ts).total_seconds()))
        ts_courant = ts_tx

    return Scenario(
        id_scenario=f"CAS-{uuid.uuid4().hex[:8]}",
        type_scenario=TypeScenario.SIM_SWAP_CASCADE,
        abonne=abonne,
        appareil_attaquant=imei_attaquant,
        evenements=evenements,
        est_fraude=True,
    )


# ── Scénario 4 : Nouveau appareil (voyage, nouveau tel) ──────

def build_nouveau_appareil(
    abonne: Subscriber, rng: random.Random, ts: datetime, nouvel_imei: str
) -> Scenario:
    montant = rng.randint(abonne.montant_min_habituel,
                          min(abonne.montant_max_habituel, max(1, abonne.solde)))
    beneficiaire = rng.choice(abonne.beneficiaires_connus) if abonne.beneficiaires_connus else "inconnu"
    antenne = _antenne_aleatoire(rng, abonne.region)

    return Scenario(
        id_scenario=f"NDV-{uuid.uuid4().hex[:8]}",
        type_scenario=TypeScenario.NOUVEAU_APPAREIL,
        abonne=abonne,
        appareil_attaquant=nouvel_imei,
        evenements=[Evenement(
            stream="transactions",
            cle_partition=abonne.identifiant,
            payload=_creer_transaction(abonne, nouvel_imei, montant, beneficiaire, ts, False, antenne, rng),
            delai_secondes=0,
        )],
        est_fraude=False,
    )


# ── Scénario 5 : Pic d'OTP suspects ─────────────────────────

def build_pic_otp(
    abonne: Subscriber, rng: random.Random, ts: datetime, imei_attaquant: str
) -> Scenario:
    nb_otp = rng.randint(5, 10)
    evenements = []
    antenne_attaquant = _antenne_etrangere(rng, abonne.region)

    for i in range(nb_otp):
        ts_otp = ts + timedelta(seconds=i * rng.uniform(5, 20))
        evenements.append(Evenement("otp-events", abonne.identifiant,
                                    _creer_otp_event(abonne, imei_attaquant, ts_otp, antenne_attaquant),
                                    (ts_otp - ts).total_seconds()))

    montant_max = max(1, min(abonne.montant_max_habituel * 10, abonne.solde))
    montant_min = max(1, min(abonne.montant_max_habituel * 3, montant_max))
    montant_fraude = rng.randint(montant_min, montant_max)
    ts_tx = ts + timedelta(minutes=rng.uniform(2, 8))
    evenements.append(Evenement("transactions", abonne.identifiant,
                                _creer_transaction(abonne, imei_attaquant, montant_fraude,
                                                   f"INC-{uuid.uuid4().hex[:8]}",
                                                   ts_tx, True, antenne_attaquant, rng),
                                (ts_tx - ts).total_seconds()))
    return Scenario(
        id_scenario=f"OTP-{uuid.uuid4().hex[:8]}",
        type_scenario=TypeScenario.PIC_OTP,
        abonne=abonne,
        appareil_attaquant=imei_attaquant,
        evenements=evenements,
        est_fraude=True,
    )


# ── Générateur principal ─────────────────────────────────────

def _generer_imei_attaquant(rng: random.Random) -> str:
    return "ATK" + "".join([str(rng.randint(0, 9)) for _ in range(12)])


def generate_scenarios(
    subscribers: list[Subscriber],
    attack_ratio: float = 0.05,
    seed: int = 42,
) -> list[Scenario]:
    """
    Génère un mix de scénarios légitimes et frauduleux.
    attack_ratio : fraction d'abonnés ciblés (ex: 0.05 = 5%)
    seed         : reproductibilité garantie
    """
    rng = random.Random(seed)
    scenarios = []
    ts_base = datetime.now(timezone.utc).replace(second=0, microsecond=0)

    nb_attaques = max(1, int(len(subscribers) * attack_ratio))
    cibles = rng.sample(subscribers, nb_attaques)
    ensemble_cibles = {a.identifiant for a in cibles}

    types_attaque = [
        TypeScenario.SIM_SWAP_SIMPLE,
        TypeScenario.SIM_SWAP_CASCADE,
        TypeScenario.PIC_OTP,
    ]
    poids_attaque = [0.50, 0.30, 0.20]

    for abonne in subscribers:
        ts = ts_base - timedelta(hours=rng.uniform(0, 24))

        if abonne.identifiant in ensemble_cibles:
            imei_attaquant = _generer_imei_attaquant(rng)
            type_attaque = rng.choices(types_attaque, weights=poids_attaque, k=1)[0]

            if type_attaque == TypeScenario.SIM_SWAP_SIMPLE:
                scenarios.append(build_sim_swap_simple(abonne, rng, ts, imei_attaquant))
            elif type_attaque == TypeScenario.SIM_SWAP_CASCADE:
                scenarios.append(build_sim_swap_cascade(abonne, rng, ts, imei_attaquant))
            else:
                scenarios.append(build_pic_otp(abonne, rng, ts, imei_attaquant))
        else:
            if rng.random() < 0.10:
                nouvel_imei = _generer_imei_attaquant(rng)
                scenarios.append(build_nouveau_appareil(abonne, rng, ts, nouvel_imei))
            else:
                scenarios.append(build_legitime(abonne, rng, ts))

    nb_fraudes = sum(1 for s in scenarios if s.est_fraude)
    print(f"Scénarios générés : {len(scenarios)} total, "
          f"{nb_fraudes} fraudes ({nb_fraudes/len(scenarios)*100:.1f}%)")
    return scenarios
