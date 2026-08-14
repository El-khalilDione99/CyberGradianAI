"""
simulator/scenarios.py
──────────────────────
Génération des scénarios sur 30 jours.

Scénarios frauduleux :
  - SIM_SWAP_SIMPLE  : swap + 1 gros transfert
  - SIM_SWAP_CASCADE : swap + vidage en 3-7 transferts rapides
  - PIC_OTP          : rafale d'OTP + transfert frauduleux

Scénarios légitimes (faux positifs potentiels) :
  - SWAP_LEGITIME          : changement de SIM normal
  - NOUVEAU_DEVICE_LEGITIME: nouveau téléphone
  - GROS_MONTANT_LEGITIME  : grosse transaction ponctuelle
  - VOYAGE_LEGITIME        : transaction depuis une autre région

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


def _montant_normal(rng: random.Random, compte: Compte) -> float:
    """Tire un montant dans les habitudes de l'abonné."""
    m = abs(np.random.default_rng(rng.randint(0, 2**31)).normal(
        compte.montant_moyen_habituel, compte.ecart_type_montant
    ))
    return max(500.0, min(m, compte.solde))


def _montant_fraude(rng: random.Random, compte: Compte) -> float:
    """Montant frauduleux : facteur × montant_moyen."""
    facteur = rng.uniform(FACTEUR_MONTANT_FRAUDE_MIN, FACTEUR_MONTANT_FRAUDE_MAX)
    return min(compte.montant_moyen_habituel * facteur, compte.solde)


def _beneficiaire(rng: random.Random, compte: Compte, connu: bool = True) -> str:
    if connu and compte.beneficiaires_habituels:
        return rng.choice(compte.beneficiaires_habituels)
    return "BEN-" + uuid.uuid4().hex[:8].upper()


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
        "label_fraude":           1 if est_fraude else 0,
    }


def _ev_otp(
    compte: Compte,
    ts: datetime,
    antenne: str,
    id_scenario: str | None,
) -> dict:
    return {
        "id_otp":       _new_id("OTP"),
        "id_compte":    compte.id_compte,
        "horodatage":   ts.isoformat(),
        "motif":        "confirmation_swap",
        "antenne":      antenne,
        "id_scenario":  id_scenario,
    }


# ── Scénarios frauduleux ─────────────────────────────────────

def build_sim_swap_simple(compte: Compte, rng: random.Random, ts: datetime) -> Scenario:
    sid = _new_id("SCN")
    nouveau_iccid  = _new_iccid(rng)
    nouveau_imsi   = _new_imsi(rng)
    nouveau_device = _new_device(rng)
    antenne_att    = _antenne_etrangere(rng, compte.region)
    delai_otp      = rng.uniform(DELAI_OTP_SWAP_FRAUDE_MIN, DELAI_OTP_SWAP_FRAUDE_MAX)

    ts_otp  = ts
    ts_swap = ts_otp + timedelta(minutes=delai_otp)
    ts_tx   = ts_swap + timedelta(minutes=rng.uniform(1, 8))

    ev = [
        Evenement("otp-events",   compte.id_compte,
                  _ev_otp(compte, ts_otp, antenne_att, sid), ts_otp),
        Evenement("sim-events",   compte.id_compte,
                  _ev_sim(compte, ts_swap, nouveau_iccid, nouveau_imsi,
                          nouveau_device, antenne_att, antenne_att,
                          sid, True, rng, delai_otp), ts_swap),
        Evenement("transactions", compte.id_compte,
                  _ev_transaction(compte, ts_tx, _montant_fraude(rng, compte),
                                  _beneficiaire(rng, compte, connu=False),
                                  nouveau_device, antenne_att, sid, True, rng), ts_tx),
    ]
    # Mettre à jour la SIM du compte
    compte.iccid_actuel = nouveau_iccid
    compte.imsi_actuel  = nouveau_imsi
    return Scenario(sid, TypeScenario.SIM_SWAP_SIMPLE, compte, ev, True)


def build_sim_swap_cascade(compte: Compte, rng: random.Random, ts: datetime) -> Scenario:
    sid = _new_id("SCN")
    nouveau_iccid  = _new_iccid(rng)
    nouveau_imsi   = _new_imsi(rng)
    nouveau_device = _new_device(rng)
    antenne_att    = _antenne_etrangere(rng, compte.region)
    delai_otp      = rng.uniform(DELAI_OTP_SWAP_FRAUDE_MIN, DELAI_OTP_SWAP_FRAUDE_MAX)
    nb_transferts  = rng.randint(3, 7)

    ts_otp  = ts
    ts_swap = ts_otp + timedelta(minutes=delai_otp)

    ev = [
        Evenement("otp-events", compte.id_compte,
                  _ev_otp(compte, ts_otp, antenne_att, sid), ts_otp),
        Evenement("sim-events", compte.id_compte,
                  _ev_sim(compte, ts_swap, nouveau_iccid, nouveau_imsi,
                          nouveau_device, antenne_att, antenne_att,
                          sid, True, rng, delai_otp), ts_swap),
    ]

    compte.iccid_actuel = nouveau_iccid
    compte.imsi_actuel  = nouveau_imsi

    ts_courant = ts_swap + timedelta(minutes=rng.uniform(1, 5))
    nb_tx_crees = 0
    for _ in range(nb_transferts):
        # Seuil bas : 100 XOF — on vide jusqu'au bout
        if compte.solde < 100:
            break
        # Montant : entre 20% et 50% du solde restant (vidage progressif)
        montant = max(100.0, min(
            compte.solde * rng.uniform(0.20, 0.50),
            compte.solde
        ))
        ts_courant += timedelta(seconds=rng.uniform(30, 120))
        ev.append(Evenement("transactions", compte.id_compte,
                            _ev_transaction(compte, ts_courant, montant,
                                            _beneficiaire(rng, compte, connu=False),
                                            nouveau_device, antenne_att, sid, True, rng),
                            ts_courant))
        nb_tx_crees += 1

    # Garantir au moins 1 transaction fraude même si solde très bas
    if nb_tx_crees == 0 and compte.solde > 0:
        montant = max(100.0, compte.solde * 0.5)
        ts_courant += timedelta(seconds=30)
        ev.append(Evenement("transactions", compte.id_compte,
                            _ev_transaction(compte, ts_courant, montant,
                                            _beneficiaire(rng, compte, connu=False),
                                            nouveau_device, antenne_att, sid, True, rng),
                            ts_courant))

    return Scenario(sid, TypeScenario.SIM_SWAP_CASCADE, compte, ev, True)


def build_pic_otp(compte: Compte, rng: random.Random, ts: datetime) -> Scenario:
    sid = _new_id("SCN")
    nouveau_device = _new_device(rng)
    antenne_att    = _antenne_etrangere(rng, compte.region)
    nb_otp         = rng.randint(5, 10)

    ev = []
    ts_courant = ts
    for _ in range(nb_otp):
        ts_courant += timedelta(seconds=rng.uniform(5, 30))
        ev.append(Evenement("otp-events", compte.id_compte,
                            _ev_otp(compte, ts_courant, antenne_att, sid), ts_courant))

    ts_tx = ts_courant + timedelta(minutes=rng.uniform(1, 5))
    montant = min(
        compte.montant_moyen_habituel * rng.uniform(FACTEUR_MONTANT_FRAUDE_MIN, FACTEUR_MONTANT_FRAUDE_MAX),
        compte.solde
    )
    ev.append(Evenement("transactions", compte.id_compte,
                        _ev_transaction(compte, ts_tx, montant,
                                        _beneficiaire(rng, compte, connu=False),
                                        nouveau_device, antenne_att, sid, True, rng),
                        ts_tx))

    return Scenario(sid, TypeScenario.PIC_OTP, compte, ev, True)


# ── Scénarios légitimes (faux positifs) ──────────────────────

def build_swap_legitime(compte: Compte, rng: random.Random, ts: datetime) -> Scenario:
    sid = _new_id("SCN")
    nouveau_iccid  = _new_iccid(rng)
    nouveau_imsi   = _new_imsi(rng)
    nouveau_device = compte.device_id_habituel  # même appareil
    antenne        = _antenne_region(rng, compte.region)
    delai_otp      = rng.uniform(DELAI_OTP_SWAP_LEGITIME_MIN, DELAI_OTP_SWAP_LEGITIME_MAX)

    ts_otp  = ts
    ts_swap = ts_otp + timedelta(minutes=delai_otp)

    ev = [
        Evenement("otp-events", compte.id_compte,
                  _ev_otp(compte, ts_otp, antenne, sid), ts_otp),
        Evenement("sim-events", compte.id_compte,
                  _ev_sim(compte, ts_swap, nouveau_iccid, nouveau_imsi,
                          nouveau_device, antenne, antenne,
                          sid, False, rng, delai_otp), ts_swap),
    ]
    compte.iccid_actuel = nouveau_iccid
    compte.imsi_actuel  = nouveau_imsi
    return Scenario(sid, TypeScenario.SWAP_LEGITIME, compte, ev, False)


def build_nouveau_device_legitime(compte: Compte, rng: random.Random, ts: datetime) -> Scenario:
    sid = _new_id("SCN")
    nouveau_device = _new_device(rng)
    antenne        = _antenne_region(rng, compte.region)
    montant        = _montant_normal(rng, compte)

    ev = [Evenement("transactions", compte.id_compte,
                    _ev_transaction(compte, ts, montant,
                                    _beneficiaire(rng, compte, connu=True),
                                    nouveau_device, antenne, sid, False, rng), ts)]
    # Ajouter le nouveau device aux habitudes
    compte.device_id_habituel = nouveau_device
    return Scenario(sid, TypeScenario.NOUVEAU_DEVICE_LEGITIME, compte, ev, False)


def build_gros_montant_legitime(compte: Compte, rng: random.Random, ts: datetime) -> Scenario:
    sid     = _new_id("SCN")
    antenne = _antenne_region(rng, compte.region)
    montant = min(
        compte.montant_moyen_habituel * rng.uniform(2.0, 4.0),
        compte.solde
    )
    ev = [Evenement("transactions", compte.id_compte,
                    _ev_transaction(compte, ts, montant,
                                    _beneficiaire(rng, compte, connu=True),
                                    compte.device_id_habituel, antenne, sid, False, rng), ts)]
    return Scenario(sid, TypeScenario.GROS_MONTANT_LEGITIME, compte, ev, False)


def build_voyage_legitime(compte: Compte, rng: random.Random, ts: datetime) -> Scenario:
    sid             = _new_id("SCN")
    antenne_voyage  = _antenne_etrangere(rng, compte.region)
    montant         = _montant_normal(rng, compte)

    ev = [Evenement("transactions", compte.id_compte,
                    _ev_transaction(compte, ts, montant,
                                    _beneficiaire(rng, compte, connu=True),
                                    compte.device_id_habituel, antenne_voyage, sid, False, rng), ts)]
    return Scenario(sid, TypeScenario.VOYAGE_LEGITIME, compte, ev, False)


def build_transaction_normale(compte: Compte, rng: random.Random, ts: datetime) -> Scenario:
    antenne = _antenne_region(rng, compte.region)
    montant = _montant_normal(rng, compte)
    ev = [Evenement("transactions", compte.id_compte,
                    _ev_transaction(compte, ts, montant,
                                    _beneficiaire(rng, compte, connu=True),
                                    compte.device_id_habituel, antenne, None, False, rng), ts)]
    return Scenario("", TypeScenario.NORMAL, compte, ev, False)
