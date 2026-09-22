"""
engine/datasets/raw.py
──────────────────────
Étape 0 : produire un DUMP BRUT figé et versionné du trafic simulé.

Le simulateur génère 30 jours de trafic sénégalais (transactions +
sim-events + otp-events). On l'écrit tel quel dans l'ObjectStore :

  cg-datasets/raw/<version>/
    transactions.parquet     ~23 000 lignes
    sim_events.parquet       ~270 lignes
    otp_events.parquet       ~460 lignes
    profiles_init.json       profil initial (état J0) des 500 abonnés
    manifest.json            seed, période, git sha, comptages
  cg-datasets/raw/latest.json   → {"version": "<version>"}

Tout le pipeline aval part de ce dump — plus aucune régénération à la volée.
Reproductible (seed figée) et auditable (git sha + comptages).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from interfaces.store import get_object_store, BUCKET_DATASETS
from engine.datasets._io import save_parquet, load_parquet, records_from_df, git_sha

from simulator.subscribers import generate_subscribers
from simulator.profiles import build_initial_profile
from simulator.calendrier import planifier_simulation
from simulator.config import (
    SEED, NB_ABONNES, DUREE_SIMULATION_JOURS, DATE_DEBUT_SIMULATION,
)

logger = logging.getLogger(__name__)

BUCKET      = BUCKET_DATASETS
RAW_PREFIX  = "raw"
LATEST_KEY  = f"{RAW_PREFIX}/latest.json"

# stream Kafka  →  nom de fichier parquet
_STREAM_FILE = {
    "transactions": "transactions.parquet",
    "sim-events":   "sim_events.parquet",
    "otp-events":   "otp_events.parquet",
}


@dataclass
class RawDump:
    version:       str
    events:        list[dict[str, Any]]      # tous streams, champ "_stream" ajouté
    profiles_init: dict[str, dict]           # {id_compte: profil J0}
    manifest:      dict[str, Any]

    def by_stream(self, stream: str) -> list[dict]:
        return [e for e in self.events if e.get("_stream") == stream]


# ════════════════════════════════════════════════════════════
#  Écriture
# ════════════════════════════════════════════════════════════

def build_raw_dump(
    seed: int = SEED,
    nb_abonnes: int = NB_ABONNES,
    store=None,
    version: str | None = None,
    promote: bool = True,
) -> str:
    """
    Simule le trafic et l'écrit dans cg-datasets/raw/<version>/.
    Retourne la version créée.
    """
    store   = store or get_object_store()
    version = version or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    # 1. Abonnés + profils initiaux — AVANT planification (qui mute les comptes :
    #    solde, iccid, device évoluent au fil des 30 jours).
    comptes       = generate_subscribers(nb_abonnes, seed=seed)
    profiles_init = {c.id_compte: build_initial_profile(c) for c in comptes}

    # 2. Planifier les 30 jours (mute `comptes` chronologiquement).
    scenarios = planifier_simulation(comptes, seed=seed)

    by_stream: dict[str, list[dict]] = {s: [] for s in _STREAM_FILE}
    for sc in scenarios:
        for ev in sc.evenements:
            by_stream.setdefault(ev.stream, []).append(ev.payload)

    # 3. Écrire un parquet par stream.
    for stream, fname in _STREAM_FILE.items():
        df = pd.DataFrame(by_stream.get(stream, []))
        save_parquet(store, BUCKET, f"{RAW_PREFIX}/{version}/{fname}", df)

    store.save_json(BUCKET, f"{RAW_PREFIX}/{version}/profiles_init.json", profiles_init)

    n_tx    = len(by_stream.get("transactions", []))
    n_fraud = sum(1 for r in by_stream.get("transactions", []) if r.get("label_fraude") == 1)
    manifest = {
        "version":        version,
        "seed":           seed,
        "nb_abonnes":     nb_abonnes,
        "date_debut":     DATE_DEBUT_SIMULATION,
        "duree_jours":    DUREE_SIMULATION_JOURS,
        "n_transactions": n_tx,
        "n_sim_events":   len(by_stream.get("sim-events", [])),
        "n_otp_events":   len(by_stream.get("otp-events", [])),
        "n_fraude_tx":    n_fraud,
        "fraud_rate_tx":  round(n_fraud / n_tx, 4) if n_tx else 0.0,
        "git_sha":        git_sha(),
        "created_at":     datetime.now(timezone.utc).isoformat(),
    }
    store.save_json(BUCKET, f"{RAW_PREFIX}/{version}/manifest.json", manifest)

    if promote:
        store.save_json(BUCKET, LATEST_KEY, {"version": version})

    logger.info(
        "Raw dump %s écrit — %d tx (%d fraudes), %d sim, %d otp",
        version, n_tx, n_fraud,
        manifest["n_sim_events"], manifest["n_otp_events"],
    )
    return version


# ════════════════════════════════════════════════════════════
#  Lecture
# ════════════════════════════════════════════════════════════

def load_raw(version: str = "latest", store=None) -> RawDump:
    """Charge un dump brut (par défaut le dernier promu)."""
    store = store or get_object_store()

    if version == "latest":
        version = store.load_json(BUCKET, LATEST_KEY)["version"]

    events: list[dict] = []
    for stream, fname in _STREAM_FILE.items():
        df = load_parquet(store, BUCKET, f"{RAW_PREFIX}/{version}/{fname}")
        for rec in records_from_df(df):
            rec["_stream"] = stream
            events.append(rec)

    profiles_init = store.load_json(BUCKET, f"{RAW_PREFIX}/{version}/profiles_init.json")
    manifest      = store.load_json(BUCKET, f"{RAW_PREFIX}/{version}/manifest.json")

    logger.info("Raw dump %s chargé — %d événements", version, len(events))
    return RawDump(version, events, profiles_init, manifest)
