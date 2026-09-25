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

CORRECTIF (voir diagnostic) :
  nb_tx_7j, nb_otp_24h et nb_swaps_30j étaient auparavant des compteurs
  incrémentaux "à vie" (jamais purgés), ce qui les rendait incohérents avec
  le calcul chronologique offline utilisé pour l'entraînement du modèle
  XGBoost (engine/training/training.py). Les trois sont maintenant de vraies
  fenêtres glissantes, sur le même modèle que fenetre_1h_ts/fenetre_24h_ts,
  avec purge systématique des timestamps expirés à chaque mise à jour.
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
    """
    return [ts for ts in ts_list if _parse_ts(ts) >= cutoff]


def _count_in_window(ts_list: list[str], cutoff: datetime) -> int:
    return sum(1 for ts in ts_list if _parse_ts(ts) >= cutoff)


def _count_in_window_before(ts_list: list[str], ts: datetime, window: timedelta) -> int:
    """Nombre d'horodatages dans [ts - window, ts[ (l'événement courant exclu)."""
    return sum(1 for t in ts_list if ts - window <= _parse_ts(t) < ts)


def _push_to_window(
    ts_list: list[str],
    cutoff: datetime,
    ts_str: str,
) -> list[str]:
    """
    Purge les timestamps expirés, ajoute le nouveau, et tronque la liste
    à MAX_TS_LIST si besoin. Pattern commun aux 4 fenêtres glissantes
    (1h transactions, 24h transactions, 7j transactions, 24h OTP, 30j swaps).
    """
    ts_list = _purge_window(ts_list, cutoff)
    ts_list.append(ts_str)
    if len(ts_list) > MAX_TS_LIST:
        ts_list = ts_list[-MAX_TS_LIST:]
    return ts_list


# ════════════════════════════════════════════════════════════
#  Algorithme de Welford — moyenne et variance en ligne
#
#  Référence : B.P. Welford (1962), Technometrics.
#  Mise à jour en O(1) sans stocker l'historique complet.
#
#  Variables maintenues dans le profil :
#    nb_transactions     : n
#    montant_moyen       : mean  (µ_n)
#    montant_m2_welford  : M2    (somme des carrés des écarts)
#    ecart_type_montant  : std   (√(M2 / n) si n > 1)
# ════════════════════════════════════════════════════════════

def _welford_update(profile: dict, montant: float) -> None:
    """
    Met à jour les statistiques Welford du profil avec un nouveau montant.
    Modifie le profil en place.
    """
    n    = profile.get("nb_transactions", 0) + 1
    mean = profile.get("montant_moyen", 0.0)
    m2   = profile.get("montant_m2_welford", 0.0)

    # Algorithme de Welford
    delta  = montant - mean
    mean  += delta / n
    delta2 = montant - mean
    m2    += delta * delta2

    profile["nb_transactions"]    = n
    profile["montant_moyen"]      = round(mean, 4)
    profile["montant_m2_welford"] = round(m2, 4)
    profile["ecart_type_montant"] = round(
        math.sqrt(m2 / n) if n > 1 else 0.0, 4
    )


# Statistiques Welford additionnelles, utilisées par les z-scores par abonné
# de la Couche 2 (engine/anomaly/zscores.py). Pour chaque préfixe, le profil
# contient <prefixe>_n, <prefixe>_moyen, <prefixe>_m2 et <prefixe>_std.
WELFORD_VELOCITE_24H = "w_tx24h"    # nb_tx_24h vu au moment de chaque transaction
WELFORD_OTP_24H      = "w_otp24h"   # nb_otp_24h vu au moment de chaque transaction


def _welford_update_generic(profile: dict, prefix: str, x: float) -> None:
    """Welford générique : met à jour moyenne/écart-type de `x` sous `prefix`."""
    n    = profile.get(f"{prefix}_n", 0) + 1
    mean = profile.get(f"{prefix}_moyen", 0.0)
    m2   = profile.get(f"{prefix}_m2", 0.0)

    delta  = x - mean
    mean  += delta / n
    m2    += delta * (x - mean)

    profile[f"{prefix}_n"]     = n
    profile[f"{prefix}_moyen"] = round(mean, 4)
    profile[f"{prefix}_m2"]    = round(m2, 4)
    profile[f"{prefix}_std"]   = round(math.sqrt(m2 / n) if n > 1 else 0.0, 4)


# ════════════════════════════════════════════════════════════
#  Mise à jour — Transaction
# ════════════════════════════════════════════════════════════

def apply_transaction(profile: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
    """
    Met à jour le profil à partir d'un événement du topic `transactions`.

    Mises à jour effectuées :
      1. Statistiques Welford (montant moyen et écart-type)
      2. Fenêtres glissantes de vélocité (1h, 24h, 7j)
      3. Montant total sur 24h
      4. Solde courant
      5. Set des devices connus
      6. Set des bénéficiaires connus
      7. Set des antennes connues
      8. Timestamp de dernière mise à jour
    """
    ts       = _parse_ts(event["horodatage"])
    montant  = float(event.get("montant", 0.0))
    device   = event.get("device_id", "")
    benef    = event.get("id_beneficiaire", "")
    antenne  = event.get("antenne", "")
    ts_str   = ts.isoformat()

    # ── 0. Welford vélocité / OTP (z-scores Couche 2) ────────
    # On enregistre les valeurs telles que compute_features() les voit pour
    # CETTE transaction : comptées sur [ts - 24 h, ts[, avant la mise à jour.
    _welford_update_generic(profile, WELFORD_VELOCITE_24H,
                            float(_count_in_window_before(profile.get("fenetre_24h_ts", []), ts, WINDOW_24H)))
    _welford_update_generic(profile, WELFORD_OTP_24H,
                            float(_count_in_window_before(profile.get("fenetre_otp_24h_ts", []), ts, WINDOW_24H)))

    # ── 1. Welford ───────────────────────────────────────────
    _welford_update(profile, montant)

    # ── Vélocité de vidage depuis le dernier swap ────────────
    # Incrémenté à chaque transaction, remis à 0 à chaque nouveau swap
    # (voir apply_sim_event). Le gating par récence se fait dans
    # compute_features (engine/rules/features.py).
    profile["nb_tx_depuis_swap"] = profile.get("nb_tx_depuis_swap", 0) + 1
    profile["montant_cumule_depuis_swap"] = round(
        profile.get("montant_cumule_depuis_swap", 0.0) + montant, 2
    )

    # ── 2 & 3. Fenêtres glissantes de vélocité (1h / 24h / 7j) ──
    cutoff_1h  = ts - WINDOW_1H
    cutoff_24h = ts - WINDOW_24H
    cutoff_7d  = ts - WINDOW_7D

    fen_1h  = _push_to_window(profile.get("fenetre_1h_ts",  []), cutoff_1h,  ts_str)
    fen_24h = _push_to_window(profile.get("fenetre_24h_ts", []), cutoff_24h, ts_str)
    fen_7j  = _push_to_window(profile.get("fenetre_7j_ts",  []), cutoff_7d,  ts_str)

    profile["fenetre_1h_ts"]  = fen_1h
    profile["fenetre_24h_ts"] = fen_24h
    profile["fenetre_7j_ts"]  = fen_7j
    profile["nb_tx_1h"]       = len(fen_1h)
    profile["nb_tx_24h"]      = len(fen_24h)
    profile["nb_tx_7j"]       = len(fen_7j)

    # ── 4. Montant total 24h ──────────────────────────────────
    # Recalculer depuis les timestamps encore valides
    total_24h = profile.get("total_montant_24h", 0.0)
    if profile["nb_tx_24h"] == 1:
        # Première transaction dans la fenêtre 24h après purge
        total_24h = montant
    else:
        total_24h += montant
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
    # NOTE : filtre "CPT-" à revalider — engine/training/training.py
    # apprend actuellement TOUS les bénéficiaires sans ce filtre, ce qui
    # peut créer une divergence entraînement/production sur new_beneficiary.
    # À trancher côté métier avant déploiement.
    if benef and benef.startswith("CPT-"):  # on n'apprend que les vrais comptes
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
      3. Fenêtre glissante de swaps sur 30j
      4. Mémorisation de l'appareil déclaré (device_dernier_swap), sans l'ajouter
         aux devices connus : il le deviendra à sa première transaction
    """
    ts             = _parse_ts(event["horodatage"])
    nouveau_iccid  = event.get("nouveau_iccid", "")
    nouveau_imsi   = event.get("nouveau_imsi", "")
    nouveau_device = event.get("device_id", "")
    ts_str         = ts.isoformat()

    # ── 1. Mise à jour SIM ───────────────────────────────────
    if nouveau_iccid:
        profile["iccid_actuel"] = nouveau_iccid
    if nouveau_imsi:
        profile["imsi_actuel"] = nouveau_imsi

    # ── 2. Timestamp dernier swap ────────────────────────────
    #    C'est la feature la plus critique : heures_depuis_swap
    #    est calculée à la volée par le moteur de scoring.
    profile["ts_dernier_swap"] = ts_str

    # ── 3. Fenêtre glissante de swaps (30j) ──────────────────
    cutoff_30j = ts - WINDOW_30D
    fen_swaps_30j = _push_to_window(
        profile.get("fenetre_swaps_30j_ts", []), cutoff_30j, ts_str
    )
    profile["fenetre_swaps_30j_ts"] = fen_swaps_30j
    profile["nb_swaps_30j"]         = len(fen_swaps_30j)

    profile["nb_tx_depuis_swap"]          = 0
    profile["montant_cumule_depuis_swap"] = 0.0

    # ── 4. Appareil déclaré au swap ──────────────────────────
    # Il n'est PAS ajouté aux devices connus : un appareil ne devient « connu »
    # qu'après une transaction faite depuis lui (apply_transaction). Sinon,
    # l'appareil de l'attaquant était déjà « connu » quand arrivait la transaction
    # frauduleuse, et new_device ne se déclenchait jamais après un swap.
    if nouveau_device:
        profile["device_dernier_swap"] = nouveau_device

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
      1. Fenêtre glissante OTP 1h (otp_spike = nb_otp_1h >= 3)
      2. Fenêtre glissante OTP 24h
    """
    ts     = _parse_ts(event["horodatage"])
    ts_str = ts.isoformat()

    cutoff_1h  = ts - WINDOW_1H
    cutoff_24h = ts - WINDOW_24H

    # ── Fenêtre OTP 1h ───────────────────────────────────────
    fen_otp_1h = _push_to_window(
        profile.get("fenetre_otp_1h_ts", []), cutoff_1h, ts_str
    )
    profile["fenetre_otp_1h_ts"] = fen_otp_1h
    profile["nb_otp_1h"]         = len(fen_otp_1h)

    # ── Fenêtre OTP 24h ──────────────────────────────────────
    fen_otp_24h = _push_to_window(
        profile.get("fenetre_otp_24h_ts", []), cutoff_24h, ts_str
    )
    profile["fenetre_otp_24h_ts"] = fen_otp_24h
    profile["nb_otp_24h"]         = len(fen_otp_24h)

    # ── Méta ──────────────────────────────────────────────────
    profile["mis_a_jour_le"] = ts_str

    return profile