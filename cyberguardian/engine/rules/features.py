"""
engine/rules/features.py
─────────────────────────
Calcul des features dérivées nécessaires au moteur de règles (IA-4).

Ces features ne sont PAS stockées dans Redis — elles sont calculées à la
volée au moment du scoring à partir de :
  - l'événement courant (transaction en cours d'évaluation)
  - le profil abonné lu dans Redis / DynamoDB (produit par IA-3)

Séparation claire des responsabilités :
  - IA-3 (Feature Updater) → maintient le profil dans Redis
  - IA-4 (ce module)       → dérive les features instantanées pour les règles
  - IA-5 / IA-6            → utiliseront aussi compute_features() en entrée

Features calculées :
  hours_since_sim_swap  : heures depuis le dernier swap SIM
  amount_ratio          : ratio montant courant / montant moyen habituel
  new_device            : appareil inconnu du profil
  new_beneficiary       : bénéficiaire inconnu du profil
  is_roaming            : antenne dans une autre région que le domicile
  otp_count_1h          : nb d'OTP dans l'heure précédant la transaction
  nb_tx_1h              : nb de transactions dans l'heure précédant la transaction
  (compteurs de fenêtre recalculés à l'heure de la transaction à partir des
   horodatages du profil, et non relus depuis le compteur stocké)
  nb_beneficiaires_1h   : nb de bénéficiaires distincts dans la dernière heure
  zscore_montant        : z-score du montant par rapport à l'historique Welford
  is_active_hour        : heure dans les heures habituelles d'activité
  hour_of_day           : heure locale de la transaction
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any


# ════════════════════════════════════════════════════════════
#  Helper interne
# ════════════════════════════════════════════════════════════

def _parse_ts(ts_str: str) -> datetime:
    dt = datetime.fromisoformat(ts_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


# ════════════════════════════════════════════════════════════
#  Point d'entrée principal
# ════════════════════════════════════════════════════════════

def compute_features(
    event: dict[str, Any],
    profile: dict[str, Any],
) -> dict[str, Any]:
    """
    Calcule toutes les features dérivées nécessaires au moteur de règles.

    Paramètres
    ----------
    event   : dict — l'événement transaction en cours d'évaluation.
              Champs attendus : id_compte, horodatage, montant,
              device_id, id_beneficiaire, antenne.
    profile : dict — profil abonné lu depuis Redis / DynamoDB.
              Peut être vide ({}) si le compte est inconnu.

    Retourne
    --------
    dict — toutes les features dérivées, avec des valeurs de repli
    sûres si le profil est vide ou incomplet.
    """

    # ── Données brutes de l'événement ────────────────────────
    montant     = float(event.get("montant", 0.0))
    device_id   = event.get("device_id", "")
    beneficiaire = event.get("id_beneficiaire", "")
    antenne     = event.get("antenne", "")
    ts_event_str = event.get("horodatage", "")

    ts_event = _parse_ts(ts_event_str) if ts_event_str else datetime.now(timezone.utc)
    hour_of_day = ts_event.hour

    # ── Données du profil (avec repli sûr si profil vide) ────
    montant_moyen_habituel = float(profile.get("montant_moyen_habituel", 0.0))
    montant_moyen          = float(profile.get("montant_moyen", 0.0))
    ecart_type             = float(profile.get("ecart_type_montant", 0.0))
    devices_connus         = profile.get("devices_connus", [])
    beneficiaires_connus   = profile.get("beneficiaires_connus", [])
    antennes_connues       = profile.get("antennes_connues", [])
    antenne_domicile       = profile.get("antenne_domicile", "")
    ts_dernier_swap        = profile.get("ts_dernier_swap")
    heures_actives         = profile.get("heures_actives", [])

    # ── Compteurs de fenêtres glissantes, recalculés À L'HEURE DE LA TRANSACTION ──
    # Les compteurs stockés (nb_tx_1h, nb_otp_1h…) datent de la dernière mise à
    # jour du profil : relus tels quels, ils sont périmés (ex. un pic d'OTP vieux
    # de 10 jours restait compté « dans l'heure »). On recompte donc les
    # horodatages du profil sur [ts - fenêtre, ts[ ; la transaction courante est
    # exclue. Repli sur le compteur stocké si le profil n'a pas la liste.
    fenetre_1h_ts = _dans_fenetre(profile.get("fenetre_1h_ts"), ts_event, 1)
    nb_tx_1h      = _compter(profile, "fenetre_1h_ts",        "nb_tx_1h",     ts_event, 1)
    nb_tx_24h     = _compter(profile, "fenetre_24h_ts",       "nb_tx_24h",    ts_event, 24)
    nb_tx_7j      = _compter(profile, "fenetre_7j_ts",        "nb_tx_7j",     ts_event, 24 * 7)
    nb_otp_1h     = _compter(profile, "fenetre_otp_1h_ts",    "nb_otp_1h",    ts_event, 1)
    nb_otp_24h    = _compter(profile, "fenetre_otp_24h_ts",   "nb_otp_24h",   ts_event, 24)
    nb_swaps_30j  = _compter(profile, "fenetre_swaps_30j_ts", "nb_swaps_30j", ts_event, 24 * 30)

    # ── 1. amount_ratio ───────────────────────────────────────
    # Utilise montant_moyen_habituel (référence stable du simulateur) en
    # priorité. Bascule sur montant_moyen (Welford courant) si absent.
    # Plancher à 1 FCFA pour éviter la division par zéro.
    ref_montant = montant_moyen_habituel if montant_moyen_habituel > 0 else montant_moyen
    ref_montant = max(ref_montant, 1.0)
    amount_ratio = montant / ref_montant

    # ── 2. zscore_montant ─────────────────────────────────────
    # z = (x - µ) / σ  — mesure à combien d'écarts-types se situe le montant.
    # σ plafonné à 1 pour éviter la division par zéro sur les nouveaux comptes.
    ref_mean = montant_moyen if montant_moyen > 0 else ref_montant
    ref_std  = max(ecart_type, 1.0)
    zscore_montant = (montant - ref_mean) / ref_std

    # ── 3. hours_since_sim_swap ───────────────────────────────
    # Plafonné à 90 jours (2160h) si aucun swap récent — évite les inf qui
    # cassent l'entraînement du modèle (RobustScaler, IsolationForest).
    HOURS_SINCE_SWAP_CAP = 2160.0  # 90 jours

    if ts_dernier_swap:
        try:
            ts_swap = _parse_ts(ts_dernier_swap)
            hours_since_sim_swap = (ts_event - ts_swap).total_seconds() / 3600.0
            hours_since_sim_swap = min(hours_since_sim_swap, HOURS_SINCE_SWAP_CAP)
        except (ValueError, TypeError):
            hours_since_sim_swap = HOURS_SINCE_SWAP_CAP
    else:
        hours_since_sim_swap = HOURS_SINCE_SWAP_CAP

    # Gating : nb_tx_depuis_swap / montant_cumule_depuis_swap ne sont
    # pertinents que si le swap est récent (sinon un compte normal actif
    # ressemble artificiellement à une cascade en cours).
    CASCADE_WINDOW_HOURS = 6.0
    nb_tx_depuis_swap_raw          = int(profile.get("nb_tx_depuis_swap", 0))
    montant_cumule_depuis_swap_raw = float(profile.get("montant_cumule_depuis_swap", 0.0))
    if hours_since_sim_swap <= CASCADE_WINDOW_HOURS:
        nb_tx_depuis_swap = nb_tx_depuis_swap_raw
        montant_cumule_depuis_swap = montant_cumule_depuis_swap_raw
    else:
        nb_tx_depuis_swap = 0
        montant_cumule_depuis_swap = 0.0

    # ── 4. new_device ─────────────────────────────────────────
    new_device = bool(device_id and device_id not in devices_connus)

    # ── 5. new_beneficiary ────────────────────────────────────
    # Un bénéficiaire non-CPT (ex: BEN-...) est toujours considéré inconnu.
    new_beneficiary = bool(
        beneficiaire and beneficiaire not in beneficiaires_connus
    )

    # ── 6. is_roaming ─────────────────────────────────────────
    # Changement de RÉGION par rapport au domicile. Un abonné utilise
    # normalement plusieurs antennes de sa région : comparer l'antenne exacte
    # à l'antenne domicile marquait ~72 % des transactions légitimes comme
    # « itinérance ». Si la région ne peut pas être déduite de l'identifiant,
    # repli sur « antenne jamais vue » (antennes_connues).
    region_courante = _region_antenne(antenne)
    region_domicile = _region_antenne(antenne_domicile)
    if antenne and region_courante and region_domicile:
        is_roaming = region_courante != region_domicile
    elif antennes_connues:
        is_roaming = bool(antenne and antenne not in antennes_connues)
    else:
        is_roaming = bool(antenne and antenne_domicile and antenne != antenne_domicile)

    # ── 7. otp_count_1h / 8. nb_tx_1h ─────────────────────────
    # Recalculés plus haut à l'heure de la transaction (fenêtre [ts - 1 h, ts[).
    otp_count_1h = nb_otp_1h

    # ── 9. nb_beneficiaires_1h ────────────────────────────────
    # Nombre de bénéficiaires DISTINCTS dans la dernière heure.
    # On n'a pas de liste de (timestamp, bénéficiaire) dans le profil —
    # on utilise une approximation : si nb_tx_1h >= 3 et new_beneficiary,
    # on considère que la cascade est possible.
    # En v2 : stocker la liste (ts, bénéficiaire) dans le profil pour un calcul exact.
    # Pour l'instant : approximation prudente mais raisonnable pour R08.
    nb_beneficiaires_1h = _estimate_beneficiaires_1h(
        fenetre_1h_ts, beneficiaires_connus, beneficiaire, ts_event
    )

    # ── 10. is_active_hour ────────────────────────────────────
    is_active_hour = hour_of_day in heures_actives if heures_actives else True

    return {
        # Features dérivées directes
        "amount_ratio":          round(amount_ratio, 4),
        "zscore_montant":        round(zscore_montant, 4),
        "hours_since_sim_swap":  round(hours_since_sim_swap, 4),
        "new_device":            new_device,
        "new_beneficiary":       new_beneficiary,
        "is_roaming":            is_roaming,
        # Features directes du profil (renommées pour le moteur de règles)
        "otp_count_1h":          otp_count_1h,
        "nb_tx_1h":              nb_tx_1h,
        "nb_beneficiaires_1h":   nb_beneficiaires_1h,
        # Features contextuelles
        "is_active_hour":        is_active_hour,
        "hour_of_day":           hour_of_day,
        # Features brutes du profil (utiles pour IA-5/IA-6)
        "montant_courant":       montant,
        "montant_moyen":         ref_mean,
        "ecart_type_montant":    ecart_type,
        "nb_swaps_30j":          nb_swaps_30j,
        "solde":                 float(profile.get("solde", 0.0)),
        "nb_otp_24h":            nb_otp_24h,
        "nb_tx_24h":             nb_tx_24h,
        "nb_tx_7j":              nb_tx_7j,
        "nb_tx_depuis_swap":          nb_tx_depuis_swap,
        "montant_cumule_depuis_swap": montant_cumule_depuis_swap,
    }


# ════════════════════════════════════════════════════════════
#  Fenêtres glissantes à l'heure de la transaction
# ════════════════════════════════════════════════════════════

def _dans_fenetre(ts_list: list[str] | None, ts_event: datetime, heures: float) -> list[str]:
    """Horodatages de ts_list compris dans [ts_event - heures, ts_event[."""
    debut = ts_event - timedelta(hours=heures)
    out = []
    for t in ts_list or []:
        try:
            if debut <= _parse_ts(t) < ts_event:
                out.append(t)
        except (ValueError, TypeError):
            continue
    return out


def compter_dans_fenetre(ts_list: list[str] | None, ts_event: datetime, heures: float) -> int:
    """Nombre d'horodatages dans [ts_event - heures, ts_event[."""
    return len(_dans_fenetre(ts_list, ts_event, heures))


def _compter(profile: dict[str, Any], cle_liste: str, cle_compteur: str,
             ts_event: datetime, heures: float) -> int:
    """Recompte la fenêtre si le profil a la liste d'horodatages, sinon repli sur le compteur."""
    if cle_liste in profile:
        return compter_dans_fenetre(profile.get(cle_liste), ts_event, heures)
    return int(profile.get(cle_compteur, 0))


def _region_antenne(antenne: str | None) -> str | None:
    """
    Région d'une antenne, déduite de son identifiant « <REG>-ANT-<n> »
    (ex. « DAK-ANT-003 » → « DAK »). None si le format n'est pas reconnu.
    En production, remplacer par la table de référence des cellules de l'opérateur.
    """
    if antenne and "-ANT-" in antenne:
        return antenne.split("-ANT-", 1)[0]
    return None


# ════════════════════════════════════════════════════════════
#  Estimation du nombre de bénéficiaires distincts sur 1h
# ════════════════════════════════════════════════════════════

def _estimate_beneficiaires_1h(
    fenetre_1h_ts: list[str],
    beneficiaires_connus: list[str],
    beneficiaire_courant: str,
    ts_event: datetime,
) -> int:
    """
    Approximation du nombre de bénéficiaires distincts dans la dernière heure.

    Limitation actuelle : le profil stocke les timestamps des transactions
    mais pas les bénéficiaires associés. On ne peut donc pas calculer
    exactement combien de bénéficiaires distincts ont été crédités dans l'heure.

    Approximation : on utilise nb_tx_1h comme proxy du nombre de bénéficiaires,
    plafonné à la longueur de la liste de bénéficiaires connus.
    Le cas R08 (cascade) nécessite nb_tx_1h >= 3 ET nb_beneficiaires_1h >= 3,
    ce qui est conservateur et évite les faux positifs sur un abonné actif
    qui enverrait plusieurs transactions au même bénéficiaire.

    En v2 : stocker (ts, id_beneficiaire) dans le profil pour un calcul exact.
    """
    nb_tx_recentes = len(fenetre_1h_ts)
    if nb_tx_recentes == 0:
        return 1 if beneficiaire_courant else 0

    # Hypothèse conservatrice : au plus nb_tx_recentes bénéficiaires distincts,
    # mais jamais plus que le nombre de bénéficiaires connus + 1 (le courant).
    max_possible = len(beneficiaires_connus) + (
        1 if beneficiaire_courant not in beneficiaires_connus else 0
    )
    return min(nb_tx_recentes, max_possible)
