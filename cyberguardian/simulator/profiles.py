"""
simulator/profiles.py
─────────────────────
Profils comportementaux par abonné.
Stocké dans Redis (local) / DynamoDB (AWS).
"""

from datetime import datetime, timezone
from simulator.subscribers import Subscriber
from interfaces.store import get_feature_store


def build_initial_profile(sub: Subscriber) -> dict:
    """
    Construit le profil initial d'un abonné.
    Les compteurs démarrent à zéro — mis à jour par le feature-updater.
    """
    return {
        # ── Identité (anonymisée) ──────────────────────────
        "identifiant": sub.identifiant,
        "region": sub.region,
        "segment": sub.segment,

        # ── Géographie habituelle ──────────────────────────
        "antenne_domicile": sub.antenne_domicile,

        # ── Appareils ──────────────────────────────────────
        "appareils_connus": sub.appareils,
        "dernier_appareil": sub.appareils[0] if sub.appareils else None,

        # ── Bénéficiaires ──────────────────────────────────
        "beneficiaires_connus": sub.beneficiaires_connus,

        # ── SIM ────────────────────────────────────────────
        "ts_dernier_swap": None,
        "nb_swaps_30j": 0,

        # ── Statistiques Welford ───────────────────────────
        "nb_transactions": 0,
        "montant_moyen": 0.0,
        "montant_m2_welford": 0.0,

        # ── Vélocité ───────────────────────────────────────
        "nb_tx_1h": 0,
        "nb_tx_24h": 0,
        "nb_tx_7j": 0,
        "total_montant_24h": 0.0,
        "fenetre_1h_ts": [],
        "fenetre_24h_ts": [],

        # ── OTP ────────────────────────────────────────────
        "nb_otp_1h": 0,
        "nb_otp_24h": 0,

        # ── Comportement typique ───────────────────────────
        "montant_min_habituel": sub.montant_min_habituel,
        "montant_max_habituel": sub.montant_max_habituel,
        "heures_actives": sub.heures_actives,

        # ── Solde ──────────────────────────────────────────
        "solde": sub.solde,

        # ── Méta ───────────────────────────────────────────
        "cree_le": datetime.now(timezone.utc).isoformat(),
        "mis_a_jour_le": datetime.now(timezone.utc).isoformat(),
    }


def init_profiles(subscribers: list[Subscriber], verbose: bool = True) -> None:
    """
    Écrit le profil initial dans le feature store.
    Idempotent : n'écrase pas un profil existant.
    """
    store = get_feature_store()
    crees = 0
    ignores = 0

    for sub in subscribers:
        existant = store.get_profile(sub.identifiant)
        if existant:
            ignores += 1
            continue
        profil = build_initial_profile(sub)
        store.set_profile(sub.identifiant, profil)
        crees += 1

    if verbose:
        print(f"Profils initialisés : {crees} créés, {ignores} déjà existants")


def reset_profiles(subscribers: list[Subscriber]) -> None:
    """Remet à zéro tous les profils (démo rejouable)."""
    store = get_feature_store()
    for sub in subscribers:
        profil = build_initial_profile(sub)
        store.set_profile(sub.identifiant, profil)
    print(f"{len(subscribers)} profils réinitialisés")


if __name__ == "__main__":
    from simulator.subscribers import generate_subscribers
    subs = generate_subscribers(500, seed=42)
    init_profiles(subs)
