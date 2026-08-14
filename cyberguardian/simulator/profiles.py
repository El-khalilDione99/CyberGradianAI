"""
simulator/profiles.py
─────────────────────
Profils comportementaux initiaux par compte.
Stocké dans Redis (local) / DynamoDB (AWS).
Mis à jour en continu par le Feature Updater (IA-3).
"""

from datetime import datetime, timezone
from simulator.subscribers import Compte
from interfaces.store import get_feature_store


def build_initial_profile(compte: Compte) -> dict:
    """
    Construit le profil initial à partir du modèle Compte.
    Tous les compteurs démarrent à zéro — le Feature Updater
    les met à jour à chaque événement consommé.
    """
    return {
        # ── Identité ───────────────────────────────────────────
        "id_compte":          compte.id_compte,
        "region":             compte.region,
        "segment":            compte.segment,
        "date_creation":      compte.date_creation_compte.isoformat(),

        # ── Géographie ─────────────────────────────────────────
        "antenne_domicile":   compte.antenne_domicile,
        "antennes_connues":   [compte.antenne_domicile],  # enrichi par le feature updater

        # ── SIM ────────────────────────────────────────────────
        "iccid_actuel":       compte.iccid_actuel,
        "imsi_actuel":        compte.imsi_actuel,
        "ts_dernier_swap":    None,
        "nb_swaps_30j":       0,

        # ── Appareils ──────────────────────────────────────────
        "device_id_habituel": compte.device_id_habituel,
        "devices_connus":     [compte.device_id_habituel],

        # ── Bénéficiaires ──────────────────────────────────────
        "beneficiaires_connus": compte.beneficiaires_habituels,

        # ── Statistiques Welford (moyenne/variance en ligne) ───
        "nb_transactions":       0,
        "montant_moyen":         compte.montant_moyen_habituel,   # amorçage
        "montant_m2_welford":    compte.ecart_type_montant ** 2,  # amorçage variance
        "ecart_type_montant":    compte.ecart_type_montant,

        # ── Vélocité (fenêtres glissantes) ─────────────────────
        "nb_tx_1h":           0,
        "nb_tx_24h":          0,
        "nb_tx_7j":           0,
        "total_montant_24h":  0.0,
        "fenetre_1h_ts":      [],
        "fenetre_24h_ts":     [],

        # ── OTP ────────────────────────────────────────────────
        "nb_otp_1h":          0,
        "nb_otp_24h":         0,
        "fenetre_otp_1h_ts":  [],

        # ── Comportement typique (amorçage depuis le profil) ───
        "montant_moyen_habituel": compte.montant_moyen_habituel,
        "heures_actives":         compte.heures_actives,

        # ── Solde ──────────────────────────────────────────────
        "solde":              round(compte.solde, 2),

        # ── Méta ───────────────────────────────────────────────
        "cree_le":            datetime.now(timezone.utc).isoformat(),
        "mis_a_jour_le":      datetime.now(timezone.utc).isoformat(),
    }


def init_profiles(comptes: list[Compte], verbose: bool = True) -> None:
    """
    Écrit le profil initial de tous les comptes dans le feature store.
    Idempotent : ne réécrit pas un profil existant.
    """
    store = get_feature_store()
    crees, ignores = 0, 0
    for compte in comptes:
        if store.get_profile(compte.id_compte):
            ignores += 1
            continue
        store.set_profile(compte.id_compte, build_initial_profile(compte))
        crees += 1
    if verbose:
        print(f"Profils : {crees} créés, {ignores} déjà existants")


def reset_profiles(comptes: list[Compte]) -> None:
    """Réinitialise tous les profils (utile avant une démo rejouable)."""
    store = get_feature_store()
    for compte in comptes:
        store.set_profile(compte.id_compte, build_initial_profile(compte))
    print(f"{len(comptes)} profils réinitialisés")
