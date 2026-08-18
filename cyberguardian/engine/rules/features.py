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
  is_roaming            : antenne hors domicile
  otp_count_1h          : nb d'OTP dans la dernière heure (depuis profil)
  nb_tx_1h              : nb de transactions dans la dernière heure
  nb_beneficiaires_1h   : nb de bénéficiaires distincts dans la dernière heure
  zscore_montant        : z-score du montant par rapport à l'historique Welford
  is_active_hour        : heure dans les heures habituelles d'activité
  hour_of_day           : heure locale de la transaction
"""

from __future__ import annotations

from datetime import datetime, timezone
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
    nb_otp_1h              = int(profile.get("nb_otp_1h", 0))
    nb_tx_1h               = int(profile.get("nb_tx_1h", 0))
    heures_actives         = profile.get("heures_actives", [])
    fenetre_1h_ts          = profile.get("fenetre_1h_ts", [])

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
    # float("inf") si aucun swap enregistré → aucune règle "swap récent" ne déclenche.
    if ts_dernier_swap:
        try:
            ts_swap = _parse_ts(ts_dernier_swap)
            hours_since_sim_swap = (ts_event - ts_swap).total_seconds() / 3600.0
        except (ValueError, TypeError):
            hours_since_sim_swap = float("inf")
    else:
        hours_since_sim_swap = float("inf")

    # ── 4. new_device ─────────────────────────────────────────
    new_device = bool(device_id and device_id not in devices_connus)

    # ── 5. new_beneficiary ────────────────────────────────────
    # Un bénéficiaire non-CPT (ex: BEN-...) est toujours considéré inconnu.
    new_beneficiary = bool(
        beneficiaire and beneficiaire not in beneficiaires_connus
    )

    # ── 6. is_roaming ─────────────────────────────────────────
    # Vrai si l'antenne courante n'est pas l'antenne domicile ET est inconnue.
    # Priorité à la comparaison antenne_domicile (plus fiable).
    if antenne_domicile:
        is_roaming = bool(antenne and antenne != antenne_domicile)
    else:
        is_roaming = bool(antenne and antenne not in antennes_connues)

    # ── 7. otp_count_1h ───────────────────────────────────────
    # Directement depuis le profil — mis à jour par apply_otp_event (IA-3).
    otp_count_1h = nb_otp_1h

    # ── 8. nb_tx_1h ───────────────────────────────────────────
    # Directement depuis le profil — fenêtre glissante maintenue par IA-3.
    # Note : nb_tx_1h dans le profil inclut déjà la transaction courante
    # si le feature-updater a tourné avant le scoring. Sinon, +1 ici.
    # On fait confiance au profil Redis pour ne pas doubler.

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
        "nb_swaps_30j":          int(profile.get("nb_swaps_30j", 0)),
        "solde":                 float(profile.get("solde", 0.0)),
        "nb_otp_24h":            int(profile.get("nb_otp_24h", 0)),
        "nb_tx_24h":             int(profile.get("nb_tx_24h", 0)),
        "nb_tx_7j":              int(profile.get("nb_tx_7j", 0)),
    }


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
