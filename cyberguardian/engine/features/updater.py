"""
engine/features/updater.py
──────────────────────────
Logique métier pure du Feature Updater (IA-3).

Ce module ne connaît ni Kafka ni Redis ni DynamoDB.
Il reçoit un profil (dict), un événement (dict), et retourne
le profil mis à jour. 100 % testable sans infrastructure.

Trois types de mise à jour :
  - apply_transaction  : Welford, fenêtres glissantes, solde, devices, bénéficiaires
  - apply_sim_event    : swap SIM (iccid, imsi, ts_dernier_swap, nb_swaps_30j)
  - apply_otp_event    : compteurs OTP (fenêtre glissante 1h et 24h)
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Any

# ── Constantes des fenêtres temporelles ──────────────────────
WINDOW_1H  = timedelta(hours=1)
WINDOW_24H = timedelta(hours=24)
WINDOW_7D  = timedelta(days=7)
WINDOW_30D = timedelta(days=30)

# Taille maximale des listes de timestamps stockées (garde-fou mémoire)
MAX_TS_LIST = 500


# ════════════════════════════════════════════════════════════
#  Helpers
# ════════════════════════════════════════════════════════════

def _parse_ts(ts_str: str) -> datetime:
    """Parse un timestamp ISO 8601 vers datetime UTC."""
    dt = datetime.fromisoformat(ts_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _now_str() -> str:
    return datetime.now(timezone.utc).isoformat()


def _purge_window(ts_list: list[str], cutoff: datetime) -> list[str]:
    """
    Supprime de la liste tous les timestamps antérieurs à cutoff.
    Retourne la liste nettoyée.

    Optimisation : comparaison lexicale de chaînes ISO 8601. Toutes les
    valeurs stockées passent par `datetime.isoformat()` avec le même fuseau
    (`+00:00`), donc l'ordre lexical == l'ordre chronologique. On évite ainsi
    de parser chaque timestamp en `datetime` à chaque événement (~50x plus
    rapide sur les fenêtres longues — cf. rejeu du dataset complet).
    """
    cutoff_iso = cutoff.isoformat()
    return [ts for ts in ts_list if ts >= cutoff_iso]


def _purge_tx_window(tx_list: list[dict[str, Any]], cutoff: datetime) -> list[dict[str, Any]]:
    """
    Supprime de la liste les transactions (dict avec clé 'ts') antérieures à cutoff.
    Même optimisation lexicale que `_purge_window`.
    """
    cutoff_iso = cutoff.isoformat()
    return [tx for tx in tx_list if tx["ts"] >= cutoff_iso]


def _count_in_window(ts_list: list[str], cutoff: datetime) -> int:
    cutoff_iso = cutoff.isoformat()
    return sum(1 for ts in ts_list if ts >= cutoff_iso)


# ════════════════════════════════════════════════════════════
#  Algorithme de Welford — moyenne et variance en ligne
#
#  Référence : B.P. Welford (1962), Technometrics.
#  Mise à jour en O(1) sans stocker l'historique complet.
#
#  Variables maintenues dans le profil :
#    nb_transactions     : nombre de transactions RÉELLES observées
#    montant_welford_n   : compte effectif de Welford (= nb_transactions +
#                          pseudo-observations du prior d'amorçage)
#    montant_moyen       : mean  (µ_n)
#    montant_m2_welford  : M2    (somme des carrés des écarts)
#    ecart_type_montant  : std   (√(M2 / n) si n > 1)
#
#  Amorçage : build_initial_profile fournit un prior (montant_moyen_habituel,
#  ecart_type_montant) valant WELFORD_PRIOR_N pseudo-observations. Sans un
#  compte effectif séparé, la 1ʳᵉ vraie transaction écraserait la moyenne
#  (mean += delta/1) et ferait chuter l'écart-type à 0 → z-scores aberrants.
#  `nb_transactions` reste le compte réel (utilisé par le garde-fou z-score
#  de la Couche 2 : Z_MIN_TRANSACTIONS).
# ════════════════════════════════════════════════════════════

def _welford_update(profile: dict, montant: float) -> None:
    """
    Met à jour les statistiques Welford du profil avec un nouveau montant.
    Modifie le profil en place.
    """
    # Compte effectif : le prior si présent, sinon le compte réel (rétrocompat
    # avec les profils sans amorçage — départ propre à 0).
    n_eff = profile.get("montant_welford_n")
    if n_eff is None:
        n_eff = profile.get("nb_transactions", 0)
    n = n_eff + 1

    mean = profile.get("montant_moyen", 0.0)
    m2   = profile.get("montant_m2_welford", 0.0)

    # Algorithme de Welford
    delta  = montant - mean
    mean  += delta / n
    delta2 = montant - mean
    m2    += delta * delta2

    profile["nb_transactions"]    = profile.get("nb_transactions", 0) + 1
    profile["montant_welford_n"]  = n
    profile["montant_moyen"]      = round(mean, 4)
    profile["montant_m2_welford"] = round(m2, 4)
    profile["ecart_type_montant"] = round(
        math.sqrt(m2 / n) if n > 1 else 0.0, 4
    )


# ════════════════════════════════════════════════════════════
#  Mise à jour — Transaction
# ════════════════════════════════════════════════════════════

def apply_transaction(profile: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
    """
    Met à jour le profil à partir d'un événement du topic `transactions`.

    Mises à jour effectuées :
      1. Statistiques Welford (montant moyen et écart-type)
      2. Fenêtres glissantes de vélocité (1h, 24h, 7j)
      3. Montant total sur 24h (calcul exact par déduction des transactions expirées)
      4. Solde courant
      5. Set des devices connus
      6. Set des bénéficiaires connus (CPT- et BEN-)
      7. Set des antennes connues
      8. Timestamp de dernière mise à jour
    """
    ts       = _parse_ts(event["horodatage"])
    montant  = float(event.get("montant", 0.0))
    device   = event.get("device_id", "")
    benef    = event.get("id_beneficiaire", "")
    antenne  = event.get("antenne", "")

    # ── 1. Welford ───────────────────────────────────────────
    _welford_update(profile, montant)

    # ── 2, 3 & 4. Fenêtres glissantes de vélocité (1h, 24h, 7j) ───
    cutoff_1h  = ts - WINDOW_1H
    cutoff_24h = ts - WINDOW_24H
    cutoff_7d  = ts - WINDOW_7D

    # Récupérer les listes existantes
    fen_1h     = profile.get("fenetre_1h_ts",  [])
    fen_24h_tx = profile.get("fenetre_24h_tx", [])
    fen_7d     = profile.get("fenetre_7j_ts",   [])

    # Fallback de rétro-compatibilité si fenetre_24h_tx n'existe pas encore
    if not fen_24h_tx and profile.get("fenetre_24h_ts"):
        fen_24h_tx = [{"ts": t, "m": 0.0} for t in profile["fenetre_24h_ts"]]

    # Purger les timestamps/transactions expirés
    fen_1h     = _purge_window(fen_1h, cutoff_1h)
    fen_24h_tx = _purge_tx_window(fen_24h_tx, cutoff_24h)
    fen_7d     = _purge_window(fen_7d, cutoff_7d)

    # Ajouter la nouvelle transaction
    ts_str = ts.isoformat()
    fen_1h.append(ts_str)
    fen_24h_tx.append({"ts": ts_str, "m": montant})
    fen_7d.append(ts_str)

    if len(fen_1h) > MAX_TS_LIST: fen_1h = fen_1h[-MAX_TS_LIST:]
    if len(fen_24h_tx) > MAX_TS_LIST: fen_24h_tx = fen_24h_tx[-MAX_TS_LIST:]
    if len(fen_7d) > MAX_TS_LIST: fen_7d = fen_7d[-MAX_TS_LIST:]

    fen_24h_ts = [item["ts"] for item in fen_24h_tx]

    profile["fenetre_1h_ts"]  = fen_1h
    profile["fenetre_24h_tx"] = fen_24h_tx
    profile["fenetre_24h_ts"] = fen_24h_ts
    profile["fenetre_7j_ts"]  = fen_7d

    profile["nb_tx_1h"]       = len(fen_1h)
    profile["nb_tx_24h"]      = len(fen_24h_tx)
    profile["nb_tx_7j"]       = len(fen_7d)

    # total_montant_24h : somme exacte des montants toujours dans la fenêtre 24h
    total_24h = sum(float(item["m"]) for item in fen_24h_tx)
    profile["total_montant_24h"] = round(total_24h, 2)

    # ── 5. Solde ──────────────────────────────────────────────
    solde_apres = event.get("solde_apres")
    if solde_apres is not None:
        profile["solde"] = round(float(solde_apres), 2)

    # ── 6. Devices connus ─────────────────────────────────────
    if device:
        devices = profile.get("devices_connus", [])
        if device not in devices:
            devices.append(device)
            profile["devices_connus"] = devices

    # ── 7. Bénéficiaires connus ───────────────────────────────
    if benef and (benef.startswith("CPT-") or benef.startswith("BEN-")):  # on apprend les vrais comptes/bénéficiaires
        beneficiaires = profile.get("beneficiaires_connus", [])
        if benef not in beneficiaires:
            beneficiaires.append(benef)
            profile["beneficiaires_connus"] = beneficiaires

    # ── 8. Antennes connues ───────────────────────────────────
    if antenne:
        antennes = profile.get("antennes_connues", [])
        if antenne not in antennes:
            antennes.append(antenne)
            profile["antennes_connues"] = antennes

    # ── Méta ──────────────────────────────────────────────────
    profile["mis_a_jour_le"] = ts_str

    return profile


# ════════════════════════════════════════════════════════════
#  Mise à jour — Événement SIM (swap)
# ════════════════════════════════════════════════════════════

def apply_sim_event(profile: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
    """
    Met à jour le profil à partir d'un événement du topic `sim-events`.

    Mises à jour effectuées :
      1. Mise à jour de l'ICCID et de l'IMSI actifs
      2. Enregistrement du timestamp du dernier swap (feature critique)
      3. Fenêtre glissante de 30 jours pour nb_swaps_30j

    Le device utilisé PENDANT le swap n'est volontairement PAS ajouté à
    `devices_connus` ici : la confiance dans un appareil se gagne par une
    transaction réussie (cf. apply_transaction), pas par le swap lui-même.
    Sinon, un swap frauduleux « blanchit » instantanément l'appareil de
    l'attaquant et neutralise le signal `new_device` sur la transaction qui
    suit — cf. dictionnaire_features.md, limite #2.
    """
    ts            = _parse_ts(event["horodatage"])
    nouveau_iccid  = event.get("nouveau_iccid", "")
    nouveau_imsi   = event.get("nouveau_imsi", "")
    ts_str         = ts.isoformat()
    cutoff_30d     = ts - WINDOW_30D

    # ── 1. Mise à jour SIM ───────────────────────────────────
    if nouveau_iccid:
        profile["iccid_actuel"] = nouveau_iccid
    if nouveau_imsi:
        profile["imsi_actuel"] = nouveau_imsi

    # ── 2. Timestamp dernier swap ────────────────────────────
    profile["ts_dernier_swap"] = ts_str

    # ── 3. Fenêtre glissante swaps 30j ───────────────────────
    fen_swaps_30j = profile.get("fenetre_swaps_30j_ts", [])
    fen_swaps_30j = _purge_window(fen_swaps_30j, cutoff_30d)
    fen_swaps_30j.append(ts_str)
    if len(fen_swaps_30j) > MAX_TS_LIST:
        fen_swaps_30j = fen_swaps_30j[-MAX_TS_LIST:]

    profile["fenetre_swaps_30j_ts"] = fen_swaps_30j
    profile["nb_swaps_30j"]         = len(fen_swaps_30j)

    # ── Méta ──────────────────────────────────────────────────
    profile["mis_a_jour_le"] = ts_str

    return profile


# ════════════════════════════════════════════════════════════
#  Mise à jour — Événement OTP
# ════════════════════════════════════════════════════════════

def apply_otp_event(profile: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
    """
    Met à jour le profil à partir d'un événement du topic `otp-events`.

    Mises à jour effectuées :
      1. Fenêtre glissante OTP 1h
      2. Fenêtre glissante OTP 24h
    """
    ts     = _parse_ts(event["horodatage"])
    ts_str = ts.isoformat()

    cutoff_1h  = ts - WINDOW_1H
    cutoff_24h = ts - WINDOW_24H

    # ── Fenêtre OTP 1h ───────────────────────────────────────
    fen_otp_1h = profile.get("fenetre_otp_1h_ts", [])
    fen_otp_1h = _purge_window(fen_otp_1h, cutoff_1h)
    fen_otp_1h.append(ts_str)
    if len(fen_otp_1h) > MAX_TS_LIST:
        fen_otp_1h = fen_otp_1h[-MAX_TS_LIST:]

    profile["fenetre_otp_1h_ts"] = fen_otp_1h
    profile["nb_otp_1h"]         = len(fen_otp_1h)

    # ── Fenêtre OTP 24h ──────────────────────────────────────
    fen_otp_24h = profile.get("fenetre_otp_24h_ts", [])
    fen_otp_24h = _purge_window(fen_otp_24h, cutoff_24h)
    fen_otp_24h.append(ts_str)
    if len(fen_otp_24h) > MAX_TS_LIST:
        fen_otp_24h = fen_otp_24h[-MAX_TS_LIST:]

    profile["fenetre_otp_24h_ts"] = fen_otp_24h
    profile["nb_otp_24h"]        = len(fen_otp_24h)

    # ── Méta ──────────────────────────────────────────────────
    profile["mis_a_jour_le"] = ts_str

    return profile
