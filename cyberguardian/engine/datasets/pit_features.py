"""
engine/datasets/pit_features.py
───────────────────────────────
Calcul des features en COHÉRENCE TEMPORELLE (point-in-time).

Problème résolu
───────────────
Featuriser une transaction du jour 2 avec le profil dans son état du jour 2 —
et non son état final après 30 jours. Sinon `montant_moyen`, `nb_swaps_30j`,
`devices_connus`, etc. « voient le futur » → fuite de données, métriques
faussement optimistes.

Méthode
───────
Rejouer TOUS les événements (transactions + sim + otp) dans l'ordre
chronologique. Pour chaque transaction :
  1. `compute_features(tx, profil)`      ← profil AVANT la transaction
  2. `apply_transaction(profil, tx)`     ← puis on l'intègre au profil
Les sim-events / otp-events antérieurs sont déjà appliqués → `hours_since_sim_swap`,
`otp_count_1h`, etc. reflètent bien ce qui précède la transaction.

Sémantique figée (contrat IA-7)
───────────────────────────────
Le moteur de scoring lit le profil, score, PUIS le feature-updater intègre la
transaction. Une transaction n'est jamais featurisée contre sa propre
contribution (moyenne Welford, compteur de vélocité) — cf. tests/test_updater
::test_08_anti_leakage_order_and_compute_features.

On réutilise exactement `engine/rules/features.py` et
`engine/features/updater.py` → les features d'entraînement sont produites par
le même code que les features de production.
"""

from __future__ import annotations

import logging
from copy import deepcopy
from typing import Any

import pandas as pd

from engine.rules.features import compute_features
from engine.features.updater import (
    apply_transaction, apply_sim_event, apply_otp_event,
)

logger = logging.getLogger(__name__)

# Colonnes méta ajoutées à chaque ligne de features (préfixe _ = non-feature).
META_COLS = ["_id_compte", "_label", "_type_scenario", "_horodatage"]


def _stream_of(ev: dict) -> str:
    """Déduit le stream d'un événement à partir de ses champs identifiants."""
    if ev.get("_stream"):
        return ev["_stream"]
    if "id_transaction" in ev:
        return "transactions"
    if "id_otp" in ev:
        return "otp-events"
    return "sim-events"


def replay_features(
    events: list[dict[str, Any]],
    profiles_init: dict[str, dict],
) -> pd.DataFrame:
    """
    Rejeu chronologique → DataFrame (une ligne par transaction).

    Colonnes : toutes les features de `compute_features(...)` + `META_COLS`.
    `df.attrs["n_skipped"]` = nb d'événements ignorés (compte inconnu du dump).
    """
    profiles = {k: deepcopy(v) for k, v in profiles_init.items()}
    events_sorted = sorted(events, key=lambda e: e.get("horodatage", ""))

    rows: list[dict] = []
    skipped = 0

    for ev in events_sorted:
        id_compte = ev.get("id_compte", "")
        profil    = profiles.get(id_compte)
        if profil is None:
            skipped += 1
            continue

        stream = _stream_of(ev)

        if stream == "transactions":
            feats = compute_features(ev, profil)          # AVANT
            feats["_id_compte"]     = id_compte
            feats["_label"]         = int(ev.get("label_fraude", 0) or 0)
            feats["_type_scenario"] = ev.get("type_scenario", "NORMAL") or "NORMAL"
            feats["_horodatage"]    = ev.get("horodatage")
            rows.append(feats)
            apply_transaction(profil, ev)                 # PUIS
        elif stream == "sim-events":
            apply_sim_event(profil, ev)
        elif stream == "otp-events":
            apply_otp_event(profil, ev)

    df = pd.DataFrame(rows)
    df.attrs["n_skipped"] = skipped

    if skipped:
        logger.warning("replay_features — %d événements ignorés (compte absent du dump)", skipped)
    logger.info(
        "replay_features — %d transactions featurisées (%d fraudes)",
        len(df), int(df["_label"].sum()) if len(df) else 0,
    )
    return df
