"""
simulator/scenarios.py
──────────────────────
Génération des scénarios sur 30 jours.

Scénarios frauduleux (Périmètre SIM Swap) :
  - SIM_SWAP_SIMPLE  : swap + 1 transfert (attaque classique : new device, antenne étrangère, montant élevé)
  - SIM_SWAP_CASCADE : swap + transferts multiples (vidage rapide ou progressif)
  - PIC_OTP          : rafale d'OTP + swap + transfert frauduleux
  - SIM_SWAP_DISCRET : swap discret + transfert modéré différé (attaque furtive)

Scénarios légitimes (faux positifs potentiels) :
  - SWAP_LEGITIME          : changement de SIM normal (50% nouveau device)
  - NOUVEAU_DEVICE_LEGITIME: nouveau téléphone
  - GROS_MONTANT_LEGITIME  : grosse transaction ponctuelle
  - VOYAGE_LEGITIME        : transaction depuis une autre région
  - NORMAL                 : transaction courante de routine (device habituel, antenne domicile)

Chaque événement est un dict prêt à être publié dans Kafka/Kinesis.
L'ordre temporel est garanti — les événements sont triés par horodatage.
"""

import random
import uuid
import string
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum

import numpy as np

from simulator.subscribers import Compte
from simulator.config import (
    ANTENNES_PAR_REGION, REGIONS, CANAUX_TRANSACTION, POIDS_CANAUX,
    TYPES_TRANSACTION, POIDS_TYPES, CANAUX_SWAP_FRAUDE, POIDS_SWAP_FRAUDE,
    CANAUX_SWAP_LEGITIME, POIDS_SWAP_LEGITIME,
    DELAI_OTP_SWAP_FRAUDE_MIN, DELAI_OTP_SWAP_FRAUDE_MAX,
    DELAI_OTP_SWAP_LEGITIME_MIN, DELAI_OTP_SWAP_LEGITIME_MAX,
    FACTEUR_MONTANT_FRAUDE_MIN, FACTEUR_MONTANT_FRAUDE_MAX,
)


class TypeScenario(str, Enum):
    # Fraudes
    SIM_SWAP_SIMPLE           = "SIM_SWAP_SIMPLE"
    SIM_SWAP_CASCADE          = "SIM_SWAP_CASCADE"
    PIC_OTP                   = "PIC_OTP"
    SIM_SWAP_DISCRET          = "SIM_SWAP_DISCRET"
    # Légitimes (faux positifs potentiels)
    SWAP_LEGITIME             = "SWAP_LEGITIME"
    NOUVEAU_DEVICE_LEGITIME   = "NOUVEAU_DEVICE_LEGITIME"
    GROS_MONTANT_LEGITIME     = "GROS_MONTANT_LEGITIME"
    VOYAGE_LEGITIME           = "VOYAGE_LEGITIME"
    NORMAL                    = "NORMAL"


@dataclass
class Evenement:
    stream:         str    # "transactions" | "sim-events" | "otp-events"
    cle_partition:  str    # id_compte
    payload:        dict
    horodatage:     datetime


@dataclass
class Scenario:
    id_scenario:    str
    type_scenario:  TypeScenario
    compte:         Compte
    evenements:     list[Evenement]
    est_fraude:     bool


# ── Helpers ──────────────────────────────────────────────────

def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8].upper()}"


def _new_iccid(rng: random.Random) -> str:
    return "".join([str(rng.randint(0, 9)) for _ in range(19)])


def _new_imsi(rng: random.Random) -> str:
    return "608" + "".join([str(rng.randint(0, 9)) for _ in range(12)])


def _new_device(rng: random.Random) -> str:
    return "DEV-" + "".join(rng.choices(string.hexdigits[:16], k=8)).upper()


def _antenne_region(rng: random.Random, region: str) -> str:
    return rng.choice(ANTENNES_PAR_REGION[region])


def _antenne_etrangere(rng: random.Random, region: str) -> str:
    autres = [r for r in REGIONS if r != region]
    return rng.choice(ANTENNES_PAR_REGION[rng.choice(autres)])


def _device_fraude(rng: random.Random, compte: Compte) -> str:
    """Appareil utilisé lors d'une fraude furtive (mixte)."""
    r = rng.random()
    if r < 0.60:
        return _new_device(rng)
    elif r < 0.90:
        return compte.device_id_habituel
    else:
        return "DEV-SEC-" + "".join(rng.choices(string.hexdigits[:16], k=6)).upper()


def _antenne_fraude(rng: random.Random, compte: Compte) -> str:
    """Antenne relais pour une fraude furtive (mixte)."""
    if rng.random() < 0.55:
        return _antenne_etrangere(rng, compte.region)
    return _antenne_region(rng, compte.region)


def _montant_normal(rng: random.Random, compte: Compte) -> float:
    """Tire un montant dans les habitudes de l'abonné."""
    m = abs(np.random.default_rng(rng.randint(0, 2**31)).normal(
        compte.montant_moyen_habituel, compte.ecart_type_montant
    ))
    return max(500.0, min(m, compte.solde if compte.solde > 500 else compte.montant_moyen_habituel))


def _montant_fraude(rng: random.Random, compte: Compte) -> float:
    """
    Montant frauduleux : cible un facteur élevé (2.5-10x l'habitude), mais ne
    doit jamais retomber sous 1.5x l'habitude quand le solde le permet — sinon
    le plafond (solde disponible) écrasait systématiquement le facteur et
    diluait `amount_ratio` en dessous de 1 (cf. finding #1 : la fraude n'avait
    pas l'air plus grosse que le trafic normal). Si le solde est trop faible
    pour tenir ce plancher, on vide ce qui reste (signal de vidage).
    """
    facteur       = rng.uniform(2.5, FACTEUR_MONTANT_FRAUDE_MAX)
    montant_cible = compte.montant_moyen_habituel * facteur
    plafond       = compte.solde if compte.solde > 1000 else compte.montant_moyen_habituel * 2
    plancher      = min(compte.montant_moyen_habituel * 1.5, plafond)
    return max(plancher, min(montant_cible, plafond))


def _beneficiaire_fraude(rng: random.Random, compte: Compte) -> str:
    """Bénéficiaire pour une fraude (50% connu, 50% nouveau)."""
    if rng.random() < 0.50 and compte.beneficiaires_habituels:
        return rng.choice(compte.beneficiaires_habituels)
    return "BEN-" + uuid.uuid4().hex[:8].upper()


def _beneficiaire_legitime(rng: random.Random, compte: Compte) -> str:
    """Bénéficiaire pour une transaction légitime (60% connu, 40% nouveau)."""
    if rng.random() < 0.40 or not compte.beneficiaires_habituels:
        return "BEN-" + uuid.uuid4().hex[:8].upper()
    return rng.choice(compte.beneficiaires_habituels)


# ── Constructeurs d'événements ───────────────────────────────

def _ev_transaction(
    compte: Compte,
    ts: datetime,
    montant: float,
    id_beneficiaire: str,
    device_id: str,
    antenne: str,
    id_scenario: str | None,
    est_fraude: bool,
    rng: random.Random,
    type_scenario: str = "NORMAL",
) -> dict:
    solde_avant = max(0.0, round(compte.solde, 2))
    solde_apres = max(0.0, round(solde_avant - montant, 2))
    compte.solde = solde_apres

    return {
        "id_transaction":   _new_id("TXN"),
        "id_compte":        compte.id_compte,
        "horodatage":       ts.isoformat(),
        "montant":          round(montant, 2),
        "devise":           "XOF",
        "type_transaction": rng.choices(TYPES_TRANSACTION, weights=POIDS_TYPES, k=1)[0],
        "id_beneficiaire":  id_beneficiaire,
        "device_id":        device_id,
        "antenne":          antenne,
        "solde_avant":      solde_avant,
        "solde_apres":      solde_apres,
        "id_scenario":      id_scenario,
        "type_scenario":    type_scenario,
        "label_fraude":     1 if est_fraude else 0,
    }


def _ev_sim(
    compte: Compte,
    ts: datetime,
    nouveau_iccid: str,
    nouveau_imsi: str,
    nouveau_device: str,
    antenne_otp: str,
    antenne_swap: str,
    id_scenario: str | None,
    est_fraude: bool,
    rng: random.Random,
    delai_otp_minutes: float,
    type_scenario: str = "NORMAL",
) -> dict:
    canal = rng.choices(
        CANAUX_SWAP_FRAUDE if est_fraude else CANAUX_SWAP_LEGITIME,
        weights=POIDS_SWAP_FRAUDE if est_fraude else POIDS_SWAP_LEGITIME,
        k=1
    )[0]
    return {
        "id_evenement":           _new_id("SIM"),
        "id_compte":              compte.id_compte,
        "horodatage":             ts.isoformat(),
        "ancien_iccid":           compte.iccid_actuel,
        "nouveau_iccid":          nouveau_iccid,
        "ancien_imsi":            compte.imsi_actuel,
        "nouveau_imsi":           nouveau_imsi,
        "type_sim":               rng.choices(["physique", "esim"], weights=[0.85, 0.15], k=1)[0],
        "canal_swap":             canal,
        "delai_otp_swap_minutes": round(delai_otp_minutes, 2),
        "antenne_otp":            antenne_otp,
        "antenne_swap":           antenne_swap,
        "device_id":              nouveau_device,
        "id_scenario":            id_scenario,
        "type_scenario":          type_scenario,
        "label_fraude":           1 if est_fraude else 0,
    }


def _ev_otp(
    compte: Compte,
    ts: datetime,
    antenne: str,
    id_scenario: str | None,
    type_scenario: str = "NORMAL",
) -> dict:
    return {
        "id_otp":        _new_id("OTP"),
        "id_compte":     compte.id_compte,
        "horodatage":    ts.isoformat(),
        "motif":         "confirmation_swap",
        "antenne":       antenne,
        "id_scenario":   id_scenario,
        "type_scenario": type_scenario,
    }


# ── Scénarios frauduleux (Périmètre SIM Swap) ────────────────

def build_sim_swap_simple(compte: Compte, rng: random.Random, ts: datetime) -> Scenario:
    """Attaque SIM Swap classique : nouveau device, antenne étrangère, délai OTP court."""
    sid = _new_id("SCN")
    nouveau_iccid  = _new_iccid(rng)
    nouveau_imsi   = _new_imsi(rng)
    nouveau_device = _new_device(rng)
    antenne_att    = _antenne_etrangere(rng, compte.region)
    delai_otp      = rng.uniform(DELAI_OTP_SWAP_FRAUDE_MIN, 15.0)

    ts_otp  = ts
    ts_swap = ts_otp + timedelta(minutes=delai_otp)
    ts_tx   = ts_swap + timedelta(minutes=rng.uniform(1, 10))

    ev = [
        Evenement("otp-events",   compte.id_compte,
                  _ev_otp(compte, ts_otp, antenne_att, sid,
                          TypeScenario.SIM_SWAP_SIMPLE), ts_otp),
        Evenement("sim-events",   compte.id_compte,
                  _ev_sim(compte, ts_swap, nouveau_iccid, nouveau_imsi,
                          nouveau_device, antenne_att, antenne_att,
                          sid, True, rng, delai_otp,
                          TypeScenario.SIM_SWAP_SIMPLE), ts_swap),
        Evenement("transactions", compte.id_compte,
                  _ev_transaction(compte, ts_tx, _montant_fraude(rng, compte),
                                  _beneficiaire_fraude(rng, compte),
                                  nouveau_device, antenne_att, sid, True, rng,
                                  TypeScenario.SIM_SWAP_SIMPLE), ts_tx),
    ]
    compte.iccid_actuel = nouveau_iccid
    compte.imsi_actuel  = nouveau_imsi
    return Scenario(sid, TypeScenario.SIM_SWAP_SIMPLE, compte, ev, True)


def build_sim_swap_cascade(compte: Compte, rng: random.Random, ts: datetime) -> Scenario:
    """Attaque SIM Swap avec transferts successifs (vidage)."""
    sid = _new_id("SCN")
    nouveau_iccid  = _new_iccid(rng)
    nouveau_imsi   = _new_imsi(rng)
    nouveau_device = _new_device(rng)
    antenne_att    = _antenne_etrangere(rng, compte.region)
    delai_otp      = rng.uniform(DELAI_OTP_SWAP_FRAUDE_MIN, 20.0)
    nb_transferts  = rng.randint(3, 6)

    ts_otp  = ts
    ts_swap = ts_otp + timedelta(minutes=delai_otp)

    ev = [
        Evenement("otp-events", compte.id_compte,
                  _ev_otp(compte, ts_otp, antenne_att, sid,
                          TypeScenario.SIM_SWAP_CASCADE), ts_otp),
        Evenement("sim-events", compte.id_compte,
                  _ev_sim(compte, ts_swap, nouveau_iccid, nouveau_imsi,
                          nouveau_device, antenne_att, antenne_att,
                          sid, True, rng, delai_otp,
                          TypeScenario.SIM_SWAP_CASCADE), ts_swap),
    ]

    compte.iccid_actuel = nouveau_iccid
    compte.imsi_actuel  = nouveau_imsi

    ts_courant = ts_swap + timedelta(minutes=rng.uniform(1, 10))
    nb_tx_crees = 0
    for _ in range(nb_transferts):
        if compte.solde < 100:
            break
        # Chaque transfert vise le plus élevé de : une part du solde restant,
        # ou un multiple de l'habitude de l'abonné — sinon un compte au solde
        # modeste (mais aux petites habitudes) produisait des cascades dont le
        # montant unitaire était proche, voire inférieur, à une transaction
        # normale (cf. finding #1).
        cible_solde    = compte.solde * rng.uniform(0.20, 0.50)
        cible_habitude = compte.montant_moyen_habituel * rng.uniform(1.5, 3.0)
        montant = max(100.0, min(max(cible_solde, cible_habitude), compte.solde))
        ts_courant += timedelta(minutes=rng.uniform(1, 20))
        ev.append(Evenement("transactions", compte.id_compte,
                            _ev_transaction(compte, ts_courant, montant,
                                            _beneficiaire_fraude(rng, compte),
                                            nouveau_device, antenne_att, sid, True, rng,
                                            TypeScenario.SIM_SWAP_CASCADE),
                            ts_courant))
        nb_tx_crees += 1

    if nb_tx_crees == 0 and compte.solde > 0:
        montant = max(100.0, compte.solde * 0.5)
        ts_courant += timedelta(minutes=2)
        ev.append(Evenement("transactions", compte.id_compte,
                            _ev_transaction(compte, ts_courant, montant,
                                            _beneficiaire_fraude(rng, compte),
                                            nouveau_device, antenne_att, sid, True, rng,
                                            TypeScenario.SIM_SWAP_CASCADE),
                            ts_courant))

    return Scenario(sid, TypeScenario.SIM_SWAP_CASCADE, compte, ev, True)


def build_pic_otp(compte: Compte, rng: random.Random, ts: datetime) -> Scenario:
    """Rafale d'OTP (interception/harcèlement) suivie d'un SIM Swap et transfert."""
    sid = _new_id("SCN")
    nouveau_iccid  = _new_iccid(rng)
    nouveau_imsi   = _new_imsi(rng)
    nouveau_device = _new_device(rng)
    antenne_att    = _antenne_etrangere(rng, compte.region)
    delai_otp      = rng.uniform(1.0, 15.0)
    nb_otp         = rng.randint(5, 9)

    ev = []
    ts_courant = ts
    for _ in range(nb_otp):
        ts_courant += timedelta(seconds=rng.uniform(5, 30))
        ev.append(Evenement("otp-events", compte.id_compte,
                            _ev_otp(compte, ts_courant, antenne_att, sid,
                                    TypeScenario.PIC_OTP), ts_courant))

    ts_swap = ts_courant + timedelta(minutes=delai_otp)
    ev.append(Evenement("sim-events", compte.id_compte,
                        _ev_sim(compte, ts_swap, nouveau_iccid, nouveau_imsi,
                                nouveau_device, antenne_att, antenne_att,
                                sid, True, rng, delai_otp,
                                TypeScenario.PIC_OTP), ts_swap))

    ts_tx = ts_swap + timedelta(minutes=rng.uniform(1, 10))
    montant = _montant_fraude(rng, compte)
    ev.append(Evenement("transactions", compte.id_compte,
                        _ev_transaction(compte, ts_tx, montant,
                                        _beneficiaire_fraude(rng, compte),
                                        nouveau_device, antenne_att, sid, True, rng,
                                        TypeScenario.PIC_OTP), ts_tx))

    compte.iccid_actuel = nouveau_iccid
    compte.imsi_actuel  = nouveau_imsi
    return Scenario(sid, TypeScenario.PIC_OTP, compte, ev, True)


def build_sim_swap_discret(compte: Compte, rng: random.Random, ts: datetime) -> Scenario:
    """
    Attaque SIM Swap furtive / modérée :
    - Swap effectué de façon discrète (délai OTP 15-45 min)
    - La transaction frauduleuse arrive 15 à 180 min après le swap
    - Montant modéré, bénéficiaire et antenne mixtes (device habituel ou secondaire)
    """
    sid = _new_id("SCN")
    nouveau_iccid  = _new_iccid(rng)
    nouveau_imsi   = _new_imsi(rng)
    device_att     = _device_fraude(rng, compte)
    antenne_att    = _antenne_fraude(rng, compte)
    delai_otp      = rng.uniform(15.0, 45.0)

    ts_otp  = ts
    ts_swap = ts_otp + timedelta(minutes=delai_otp)
    ts_tx   = ts_swap + timedelta(minutes=rng.uniform(15, 180))

    # "Modéré" mais toujours au-dessus de l'habitude (1.2-2.2x) : à 0.8x le
    # montant pouvait être *inférieur* à une transaction normale, ce qui
    # neutralisait tout signal de montant même pour ce scénario (finding #1).
    montant = min(
        compte.montant_moyen_habituel * rng.uniform(1.2, 2.2),
        compte.solde if compte.solde > 500 else compte.montant_moyen_habituel * 1.5,
    )

    ev = [
        Evenement("otp-events", compte.id_compte,
                  _ev_otp(compte, ts_otp, antenne_att, sid,
                          TypeScenario.SIM_SWAP_DISCRET), ts_otp),
        Evenement("sim-events", compte.id_compte,
                  _ev_sim(compte, ts_swap, nouveau_iccid, nouveau_imsi,
                          device_att, antenne_att, antenne_att,
                          sid, True, rng, delai_otp,
                          TypeScenario.SIM_SWAP_DISCRET), ts_swap),
        Evenement("transactions", compte.id_compte,
                  _ev_transaction(compte, ts_tx, montant,
                                  _beneficiaire_fraude(rng, compte),
                                  device_att, antenne_att, sid, True, rng,
                                  TypeScenario.SIM_SWAP_DISCRET), ts_tx),
    ]
    compte.iccid_actuel = nouveau_iccid
    compte.imsi_actuel  = nouveau_imsi
    return Scenario(sid, TypeScenario.SIM_SWAP_DISCRET, compte, ev, True)


# ── Scénarios légitimes (faux positifs) ──────────────────────

def build_swap_legitime(compte: Compte, rng: random.Random, ts: datetime) -> Scenario:
    """
    Swap SIM légitime (perte, casse, nouvelle carte en agence).

    Contrairement aux versions précédentes, ce scénario produit AUSSI une
    transaction normale peu après le swap : un abonné qui vient de changer
    de SIM/téléphone continue à s'en servir dans les heures qui suivent.
    Sans ce cas, aucune transaction légitime de tout le dataset n'a un
    `hours_since_sim_swap` bas — la Couche 3 apprend alors que « swap
    récent » prédit la fraude à ~100 %, ce qui ne serait plus vrai en
    production (cf. finding #4).
    """
    sid = _new_id("SCN")
    nouveau_iccid  = _new_iccid(rng)
    nouveau_imsi   = _new_imsi(rng)
    # 50% du temps, un swap SIM légitime s'accompagne d'un nouveau téléphone
    nouveau_device = _new_device(rng) if rng.random() < 0.50 else compte.device_id_habituel
    antenne        = _antenne_region(rng, compte.region)
    delai_otp      = rng.uniform(DELAI_OTP_SWAP_LEGITIME_MIN, DELAI_OTP_SWAP_LEGITIME_MAX)

    ts_otp  = ts
    ts_swap = ts_otp + timedelta(minutes=delai_otp)
    ts_tx   = ts_swap + timedelta(minutes=rng.uniform(10, 240))
    montant = _montant_normal(rng, compte)

    ev = [
        Evenement("otp-events", compte.id_compte,
                  _ev_otp(compte, ts_otp, antenne, sid,
                          TypeScenario.SWAP_LEGITIME), ts_otp),
        Evenement("sim-events", compte.id_compte,
                  _ev_sim(compte, ts_swap, nouveau_iccid, nouveau_imsi,
                          nouveau_device, antenne, antenne,
                          sid, False, rng, delai_otp,
                          TypeScenario.SWAP_LEGITIME), ts_swap),
        Evenement("transactions", compte.id_compte,
                  _ev_transaction(compte, ts_tx, montant,
                                  _beneficiaire_legitime(rng, compte),
                                  nouveau_device, antenne, sid, False, rng,
                                  TypeScenario.SWAP_LEGITIME), ts_tx),
    ]
    compte.iccid_actuel = nouveau_iccid
    compte.imsi_actuel  = nouveau_imsi
    if nouveau_device != compte.device_id_habituel:
        compte.device_id_habituel = nouveau_device
    return Scenario(sid, TypeScenario.SWAP_LEGITIME, compte, ev, False)


def build_nouveau_device_legitime(compte: Compte, rng: random.Random, ts: datetime) -> Scenario:
    sid = _new_id("SCN")
    nouveau_device = _new_device(rng)
    antenne        = _antenne_region(rng, compte.region)
    montant        = _montant_normal(rng, compte)

    ev = [Evenement("transactions", compte.id_compte,
                    _ev_transaction(compte, ts, montant,
                                    _beneficiaire_legitime(rng, compte),
                                    nouveau_device, antenne, sid, False, rng,
                                    TypeScenario.NOUVEAU_DEVICE_LEGITIME), ts)]
    compte.device_id_habituel = nouveau_device
    return Scenario(sid, TypeScenario.NOUVEAU_DEVICE_LEGITIME, compte, ev, False)


def build_gros_montant_legitime(compte: Compte, rng: random.Random, ts: datetime) -> Scenario:
    sid     = _new_id("SCN")
    antenne = _antenne_region(rng, compte.region)
    montant = min(
        compte.montant_moyen_habituel * rng.uniform(2.5, 6.0),
        compte.solde if compte.solde > 1000 else compte.montant_moyen_habituel * 3
    )
    ev = [Evenement("transactions", compte.id_compte,
                    _ev_transaction(compte, ts, montant,
                                    _beneficiaire_legitime(rng, compte),
                                    compte.device_id_habituel, antenne, sid, False, rng,
                                    TypeScenario.GROS_MONTANT_LEGITIME), ts)]
    return Scenario(sid, TypeScenario.GROS_MONTANT_LEGITIME, compte, ev, False)


def build_voyage_legitime(compte: Compte, rng: random.Random, ts: datetime) -> Scenario:
    sid             = _new_id("SCN")
    antenne_voyage  = _antenne_etrangere(rng, compte.region)
    montant         = _montant_normal(rng, compte)

    ev = [Evenement("transactions", compte.id_compte,
                    _ev_transaction(compte, ts, montant,
                                    _beneficiaire_legitime(rng, compte),
                                    compte.device_id_habituel, antenne_voyage, sid, False, rng,
                                    TypeScenario.VOYAGE_LEGITIME), ts)]
    return Scenario(sid, TypeScenario.VOYAGE_LEGITIME, compte, ev, False)


def build_transaction_normale(compte: Compte, rng: random.Random, ts: datetime) -> Scenario:
    # Transaction de routine standard (antenne locale et device habituel)
    antenne = _antenne_region(rng, compte.region)
    device  = compte.device_id_habituel
    montant = _montant_normal(rng, compte)
    ev = [Evenement("transactions", compte.id_compte,
                    _ev_transaction(compte, ts, montant,
                                    _beneficiaire_legitime(rng, compte),
                                    device, antenne, None, False, rng,
                                    TypeScenario.NORMAL), ts)]
    return Scenario("", TypeScenario.NORMAL, compte, ev, False)
