"""
run_prepare_datasets.py
───────────────────────
Pipeline de données complet, en une commande :

  1. Simule 30 jours de trafic        → cg-datasets/raw/<v>/
  2. Rejeu chronologique des features (point-in-time)
  3. Dataset Couche 2 (anomalie)      → cg-datasets/anomaly/<v>/
  4. Dataset Couche 3 (supervisé)     → cg-datasets/supervised/<v>/

Ensuite : `python run_train_couche2.py` / `run_train_couche3.py` ne font plus
que charger le dataset prêt et entraîner.

Usage :
    python run_prepare_datasets.py
"""

import os
import sys
import time

sys.path.insert(0, ".")
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# MinIO local (aucun Redis ni Kafka nécessaire ici)
os.environ.setdefault("MINIO_ENDPOINT",   "localhost:19000")
os.environ.setdefault("MINIO_ACCESS_KEY", "minioadmin")
os.environ.setdefault("MINIO_SECRET_KEY", "minioadmin123")

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
                    datefmt="%H:%M:%S")

from engine.datasets.raw          import build_raw_dump, load_raw
from engine.datasets.pit_features import replay_features
from engine.datasets.prepare      import prepare_anomaly, prepare_supervised


def main() -> None:
    print("[1/4] Simulation → dump brut versionné (MinIO)…")
    raw_version = build_raw_dump()
    raw = load_raw(raw_version)
    m = raw.manifest
    print(f"      raw {raw_version} : {m['n_transactions']} tx "
          f"({m['n_fraude_tx']} fraudes, {m['fraud_rate_tx']:.1%}), "
          f"{m['n_sim_events']} sim, {m['n_otp_events']} otp | git {m['git_sha']}")

    print("[2/4] Rejeu chronologique des features (point-in-time)…")
    t0 = time.time()
    feat_df = replay_features(raw.events, raw.profiles_init)
    print(f"      {len(feat_df)} lignes de features en {time.time() - t0:.1f}s "
          f"({feat_df.attrs.get('n_skipped', 0)} événements ignorés)")

    print("[3/4] Dataset Couche 2 — Isolation Forest (légitimes purs + scaler)…")
    va = prepare_anomaly(raw=raw, feat_df=feat_df)

    print("[4/4] Dataset Couche 3 — XGBoost (toutes classes + scale_pos_weight)…")
    vs = prepare_supervised(raw=raw, feat_df=feat_df)

    print()
    print("═" * 60)
    print("  Datasets prêts sur MinIO (cg-datasets)")
    print("═" * 60)
    print(f"  raw/{raw_version}/")
    print(f"  anomaly/{va}/       (latest)")
    print(f"  supervised/{vs}/    (latest)")
    print()
    print("  → python run_train_couche2.py")
    print("  → python run_train_couche3.py")
    print("═" * 60)


if __name__ == "__main__":
    main()
