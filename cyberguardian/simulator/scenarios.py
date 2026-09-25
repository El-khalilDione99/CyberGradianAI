"""
simulator/scenarios.py
──────────────────────
Génération des scénarios sur 30 jours.

Scénarios frauduleux (swap SIM, plus un type sans swap) :
  (appareil, antenne, montants et nombre d'OTP varient selon config.py : attaquants
   locaux ou distants, nouvel appareil ou appareil habituel, montants proches de
   l'habitude ou élevés — aucune signature fixe par type)
  - SIM_SWAP_SIMPLE  : swap + 1 transfert
  - SIM_SWAP_CASCADE : swap + 1 à 8 transferts, rythme et montants variables
  - PIC_OTP          : rafale d'OTP + swap + transfert frauduleux
  - SIM_SWAP_DISCRET : swap discret + transfert modéré différé de 15 min à 12 h (attaque furtive)
  - VOL_TELEPHONE    : téléphone déverrouillé volé, SANS swap : appareil habituel, antenne
                       locale, 1 à 4 transactions (fraude subtile)

Scénarios légitimes (faux positifs potentiels) :
  - SWAP_LEGITIME          : changement de SIM normal (50% nouveau device, 15% en voyage dans
                             une autre région), suivi dans 80% des cas de 1 à 3 transactions
                             du vrai titulaire (5 min à 6 h après, dans la région du swap)
  - NOUVEAU_DEVICE_LEGITIME: nouveau téléphone
  - GROS_MONTANT_LEGITIME  : grosse transaction ponctuelle
  - VOYAGE_LEGITIME        : transaction depuis une autre région
  - NORMAL                 : transaction courante de routine (device habituel, une antenne de sa région)

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
    PROBA_TX_APRES_SWAP_LEGITIME, NB_TX_APRES_SWAP_LEGITIME_MAX,
    DELAI_TX_APRES_SWAP_LEGITIME_MIN, DELAI_TX_APRES_SWAP_LEGITIME_MAX,
    DELAI_TX_FRAUDE_DISCRETE_MIN, DELAI_TX_FRAUDE_DISCRETE_MAX,
    PROBA_ATTAQUANT_LOCAL, PROBA_SWAP_LEGITIME_EN_VOYAGE,
    DELAI_TX_FRAUDE_MIN, DELAI_TX_FRAUDE_MAX,
    FACTEUR_MONTANT_FRAUDE_COURANT, FACTEUR_MONTANT_FRAUDE_FORT, PROBA_MONTANT_FRAUDE_FORT,
    PROBA_GROS_MONTANT_COURANT, FACTEUR_GROS_MONTANT_COURANT,
    PROBA_GROS_RETRAIT_COURANT, PART_SOLDE_GROS_RETRAIT,
    PROBA_OTP_PAIEMENT, NB_OTP_SWAP_FRAUDE, POIDS_OTP_SWAP_FRAUDE,
    NB_OTP_SWAP_LEGITIME, POIDS_OTP_SWAP_LEGITIME,
    PROBA_NOUVEAU_DEVICE_FRAUDE, PROBA_BENEFICIAIRE_NOUVEAU_FRAUDE,
    NB_VIREMENTS_CASCADE, POIDS_VIREMENTS_CASCADE, INTERVALLE_CASCADE_MIN, PART_SOLDE_CASCADE,
    NB_TX_VOL_TELEPHONE, DELAI_TX_VOL_TELEPHONE_MIN,
)


class TypeScenario(str, Enum):
    # Fraudes
    SIM_SWAP_SIMPLE           = "SIM_SWAP_SIMPLE"
    SIM_SWAP_CASCADE          = "SIM_SWAP_CASCADE"
    PIC_OTP                   = "PIC_OTP"
    SIM_SWAP_DISCRET          = "SIM_SWAP_DISCRET"
    VOL_TELEPHONE             = "VOL_TELEPHONE"      # fraude sans swap SIM
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
    # 16 caractères hexadécimaux : avec 8, des collisions apparaissaient dès
    # ~80 000 transactions (2 doublons à 1 500 abonnés), faussant les labels.
    return f"{prefix}-{uuid.uuid4().hex[:16].upper()}"


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


def _device_attaquant(rng: random.Random, compte: Compte, type_fraude: str) -> str:
    """Nouvel appareil avec la probabilité PROBA_NOUVEAU_DEVICE_FRAUDE[type], sinon l'appareil habituel."""
    if rng.random() < PROBA_NOUVEAU_DEVICE_FRAUDE[type_fraude]:
        return _new_device(rng)
    return compte.device_id_habituel


def _antenne_attaquant(rng: random.Random, compte: Compte, type_fraude: str) -> str:
    """
    Antenne de l'attaquant : locale (région de la victime) avec la probabilité
    PROBA_ATTAQUANT_LOCAL[type_fraude], sinon dans une autre région.
    """
    if rng.random() < PROBA_ATTAQUANT_LOCAL[type_fraude]:
        return _antenne_region(rng, compte.region)
    return _antenne_etrangere(rng, compte.region)


def _delai_tx_fraude(rng: random.Random) -> float:
    """
    Délai (minutes) entre le swap et la 1re transaction frauduleuse : loi
    log-uniforme entre DELAI_TX_FRAUDE_MIN et _MAX (médiane ≈ 11 min pour 1-120 min).
    La majorité des attaquants agit vite, une minorité attend jusqu'à 2 h.
    """
    return float(np.exp(rng.uniform(np.log(DELAI_TX_FRAUDE_MIN), np.log(DELAI_TX_FRAUDE_MAX))))


def _montant_normal(rng: random.Random, compte: Compte) -> float:
    """
    Montant d'une transaction légitime : le plus souvent dans les habitudes,
    parfois un achat important (×1,5–4) ou un gros retrait (50–90 % du solde).
    """
    r = rng.random()
    if r < PROBA_GROS_RETRAIT_COURANT and compte.solde > 1000:
        return max(500.0, compte.solde * rng.uniform(*PART_SOLDE_GROS_RETRAIT))
    if r < PROBA_GROS_RETRAIT_COURANT + PROBA_GROS_MONTANT_COURANT:
        m = compte.montant_moyen_habituel * rng.uniform(*FACTEUR_GROS_MONTANT_COURANT)
    else:
        m = abs(np.random.default_rng(rng.randint(0, 2**31)).normal(
            compte.montant_moyen_habituel, compte.ecart_type_montant
        ))
    return max(500.0, min(m, compte.solde if compte.solde > 500 else compte.montant_moyen_habituel))


def _montant_fraude(rng: random.Random, compte: Compte) -> float:
    """Montant frauduleux : surtout ×0,8–3 l'habitude, parfois ×3–8 (plafonné par le solde)."""
    bornes = FACTEUR_MONTANT_FRAUDE_FORT if rng.random() < PROBA_MONTANT_FRAUDE_FORT else FACTEUR_MONTANT_FRAUDE_COURANT
    facteur = rng.uniform(*bornes)
    montant_cible = compte.montant_moyen_habituel * facteur
    return max(1000.0, min(montant_cible, compte.solde if compte.solde > 1000 else compte.montant_moyen_habituel * 2))


def _beneficiaire_fraude(rng: random.Random, compte: Compte) -> str:
    """Bénéficiaire pour une fraude (60% nouveau, 40% connu)."""
    if rng.random() >= PROBA_BENEFICIAIRE_NOUVEAU_FRAUDE and compte.beneficiaires_habituels:
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
    motif: str = "confirmation_swap",
) -> dict:
    return {
        "id_otp":        _new_id("OTP"),
        "id_compte":     compte.id_compte,
        "horodatage":    ts.isoformat(),
        "motif":         motif,
        "antenne":       antenne,
        "id_scenario":   id_scenario,
        "type_scenario": type_scenario,
    }


# ── Scénarios frauduleux (Périmètre SIM Swap) ────────────────

def build_sim_swap_simple(compte: Compte, rng: random.Random, ts: datetime) -> Scenario:
    """Attaque SIM Swap classique : nouveau device, délai OTP court, attaquant local ou distant."""
    sid = _new_id("SCN")
    nouveau_iccid  = _new_iccid(rng)
    nouveau_imsi   = _new_imsi(rng)
    nouveau_device = _device_attaquant(rng, compte, "SIM_SWAP_SIMPLE")
    antenne_att    = _antenne_attaquant(rng, compte, "SIM_SWAP_SIMPLE")
    delai_otp      = rng.uniform(DELAI_OTP_SWAP_FRAUDE_MIN, 15.0)

    ts_otp  = ts
    ts_swap = ts_otp + timedelta(minutes=delai_otp)
    ts_tx   = ts_swap + timedelta(minutes=_delai_tx_fraude(rng))

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
    nouveau_device = _device_attaquant(rng, compte, "SIM_SWAP_CASCADE")
    antenne_att    = _antenne_attaquant(rng, compte, "SIM_SWAP_CASCADE")
    delai_otp      = rng.uniform(DELAI_OTP_SWAP_FRAUDE_MIN, 20.0)
    nb_transferts  = rng.choices(NB_VIREMENTS_CASCADE, weights=POIDS_VIREMENTS_CASCADE, k=1)[0]
    intervalle     = rng.uniform(*INTERVALLE_CASCADE_MIN)          # rythme propre à cette cascade
    part_solde     = PART_SOLDE_CASCADE[rng.choice(["petits", "moyens"])]

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

    ts_courant = ts_swap + timedelta(minutes=_delai_tx_fraude(rng))
    nb_tx_crees = 0
    for _ in range(nb_transferts):
        if compte.solde < 100:
            break
        montant = max(100.0, min(
            compte.solde * rng.uniform(*part_solde),
            compte.solde
        ))
        ts_courant += timedelta(minutes=intervalle * rng.uniform(0.5, 1.5))
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
    nouveau_device = _device_attaquant(rng, compte, "PIC_OTP")
    antenne_att    = _antenne_attaquant(rng, compte, "PIC_OTP")
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

    ts_tx = ts_swap + timedelta(minutes=_delai_tx_fraude(rng))
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
    - L'attaquant attend : la transaction frauduleuse arrive 15 min à 12 h après le swap
    - Montant modéré, bénéficiaire et antenne mixtes (device habituel ou secondaire)
    """
    sid = _new_id("SCN")
    nouveau_iccid  = _new_iccid(rng)
    nouveau_imsi   = _new_imsi(rng)
    device_att     = _device_attaquant(rng, compte, "SIM_SWAP_DISCRET")
    antenne_att    = _antenne_attaquant(rng, compte, "SIM_SWAP_DISCRET")
    delai_otp      = rng.uniform(15.0, 45.0)

    ts_otp  = ts
    ts_swap = ts_otp + timedelta(minutes=delai_otp)
    ts_tx   = ts_swap + timedelta(minutes=rng.uniform(DELAI_TX_FRAUDE_DISCRETE_MIN,
                                                      DELAI_TX_FRAUDE_DISCRETE_MAX))

    montant = min(compte.montant_moyen_habituel * rng.uniform(0.8, 1.8), compte.solde if compte.solde > 500 else compte.montant_moyen_habituel)

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
    sid = _new_id("SCN")
    nouveau_iccid  = _new_iccid(rng)
    nouveau_imsi   = _new_imsi(rng)
    # 50% du temps, un swap SIM légitime s'accompagne d'un nouveau téléphone
    nouveau_device = _new_device(rng) if rng.random() < 0.50 else compte.device_id_habituel
    # Swap fait en voyage (autre région) : le client y reste pour ses transactions suivantes
    if rng.random() < PROBA_SWAP_LEGITIME_EN_VOYAGE:
        region_swap = rng.choice([r for r in REGIONS if r != compte.region])
    else:
        region_swap = compte.region
    antenne        = _antenne_region(rng, region_swap)
    delai_otp      = rng.uniform(DELAI_OTP_SWAP_LEGITIME_MIN, DELAI_OTP_SWAP_LEGITIME_MAX)

    ts_otp  = ts
    ts_swap = ts_otp + timedelta(minutes=delai_otp)

    ev = [
        Evenement("otp-events", compte.id_compte,
                  _ev_otp(compte, ts_otp, antenne, sid,
                          TypeScenario.SWAP_LEGITIME), ts_otp),
        Evenement("sim-events", compte.id_compte,
                  _ev_sim(compte, ts_swap, nouveau_iccid, nouveau_imsi,
                          nouveau_device, antenne, antenne,
                          sid, False, rng, delai_otp,
                          TypeScenario.SWAP_LEGITIME), ts_swap),
    ]
    compte.iccid_actuel = nouveau_iccid
    compte.imsi_actuel  = nouveau_imsi
    if nouveau_device != compte.device_id_habituel:
        compte.device_id_habituel = nouveau_device

    # Le vrai titulaire réutilise aussitôt sa ligne (paiement, transfert…) :
    # des transactions légitimes tombent donc peu après un swap.
    if rng.random() < PROBA_TX_APRES_SWAP_LEGITIME:
        delais = sorted(rng.uniform(DELAI_TX_APRES_SWAP_LEGITIME_MIN, DELAI_TX_APRES_SWAP_LEGITIME_MAX)
                        for _ in range(rng.randint(1, NB_TX_APRES_SWAP_LEGITIME_MAX)))
        for d in delais:
            ts_tx = ts_swap + timedelta(minutes=d)
            ev.append(Evenement("transactions", compte.id_compte,
                                _ev_transaction(compte, ts_tx, _montant_normal(rng, compte),
                                                _beneficiaire_legitime(rng, compte),
                                                nouveau_device, _antenne_region(rng, region_swap),
                                                sid, False, rng, TypeScenario.SWAP_LEGITIME),
                                ts_tx))
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


def build_transaction_normale(compte: Compte, rng: random.Random, ts: datetime,
                              device: str | None = None) -> Scenario:
    # Transaction de routine standard (antenne de la région, appareil habituel
    # ou appareil secondaire si fourni)
    antenne = _antenne_region(rng, compte.region)
    device  = device or compte.device_id_habituel
    montant = _montant_normal(rng, compte)
    ev = [Evenement("transactions", compte.id_compte,
                    _ev_transaction(compte, ts, montant,
                                    _beneficiaire_legitime(rng, compte),
                                    device, antenne, None, False, rng,
                                    TypeScenario.NORMAL), ts)]
    return Scenario("", TypeScenario.NORMAL, compte, ev, False)


# ── Lot 4 : fraude sans swap SIM ─────────────────────────────

def build_vol_telephone(compte: Compte, rng: random.Random, ts: datetime) -> Scenario:
    """
    Vol d'un téléphone déverrouillé : le voleur utilise l'appareil HABITUEL de la
    victime, dans sa région, sans swap SIM ni OTP de confirmation. Il enchaîne 1 à
    4 transactions espacées de 2 à 45 min, vers des bénéficiaires surtout nouveaux.
    Signaux restants : montant, bénéficiaire, rythme, heure — fraude subtile.
    """
    sid      = _new_id("SCN")
    device   = compte.device_id_habituel
    ts_cour  = ts
    ev = []
    for _ in range(rng.randint(*NB_TX_VOL_TELEPHONE)):
        if compte.solde < 100:
            break
        ts_cour += timedelta(minutes=rng.uniform(*DELAI_TX_VOL_TELEPHONE_MIN))
        ev.append(Evenement("transactions", compte.id_compte,
                            _ev_transaction(compte, ts_cour, _montant_fraude(rng, compte),
                                            _beneficiaire_fraude(rng, compte), device,
                                            _antenne_region(rng, compte.region), sid, True, rng,
                                            TypeScenario.VOL_TELEPHONE), ts_cour))
    return Scenario(sid, TypeScenario.VOL_TELEPHONE, compte, ev, True)


# ── Lot 1 : OTP de la vie courante ───────────────────────────

def build_rafale_otp_legitime(compte: Compte, rng: random.Random, ts: datetime) -> Scenario:
    """Rafale légitime de 2 à 5 OTP rapprochés (code mal saisi, reconnexions)."""
    antenne, ev, t = _antenne_region(rng, compte.region), [], ts
    for _ in range(rng.randint(2, 5)):
        t += timedelta(seconds=rng.uniform(10, 90))
        ev.append(Evenement("otp-events", compte.id_compte,
                            _ev_otp(compte, t, antenne, None, TypeScenario.NORMAL, motif="connexion"), t))
    return Scenario("", TypeScenario.NORMAL, compte, ev, False)


def varier_otp_swap(sc: Scenario, rng: random.Random) -> None:
    """
    Fait varier le nombre d'OTP de confirmation autour d'un swap (0 à 4, ou 0 à 3
    pour un swap légitime). PIC_OTP garde sa rafale. Modifie le scénario en place.
    """
    if sc.type_scenario == TypeScenario.PIC_OTP:
        return
    otps = [e for e in sc.evenements if e.stream == "otp-events"]
    if not otps:
        return
    if sc.est_fraude:
        n = rng.choices(NB_OTP_SWAP_FRAUDE, weights=POIDS_OTP_SWAP_FRAUDE, k=1)[0]
    else:
        n = rng.choices(NB_OTP_SWAP_LEGITIME, weights=POIDS_OTP_SWAP_LEGITIME, k=1)[0]
    premier = min(otps, key=lambda e: e.horodatage)
    if n == 0:
        sc.evenements = [e for e in sc.evenements if e.stream != "otp-events"]
        return
    for _ in range(n - 1):
        t = premier.horodatage - timedelta(minutes=rng.uniform(0.5, 10))
        payload = dict(premier.payload, id_otp=_new_id("OTP"), horodatage=t.isoformat())
        sc.evenements.append(Evenement("otp-events", sc.compte.id_compte, payload, t))
    sc.evenements.sort(key=lambda e: e.horodatage)


def ajouter_otp_paiement(sc: Scenario, rng: random.Random) -> None:
    """Ajoute, avec la probabilité PROBA_OTP_PAIEMENT, un OTP de paiement 0,2–3 min avant chaque transaction."""
    ajouts = []
    for e in sc.evenements:
        if e.stream == "transactions" and rng.random() < PROBA_OTP_PAIEMENT:
            t = e.horodatage - timedelta(minutes=rng.uniform(0.2, 3))
            ajouts.append(Evenement("otp-events", sc.compte.id_compte,
                                    _ev_otp(sc.compte, t, e.payload["antenne"], e.payload["id_scenario"],
                                            e.payload["type_scenario"], motif="paiement"), t))
    if ajouts:
        sc.evenements.extend(ajouts)
        sc.evenements.sort(key=lambda e: e.horodatage)
