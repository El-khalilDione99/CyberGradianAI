"""
run_train_couche2.py
─────────────────────
Entraîne la Couche 2 (IA-5 — Isolation Forest) depuis Redis + MinIO local.

Usage :
    python run_train_couche2.py
"""
import sys, os
sys.path.insert(0, ".")

os.environ["REDIS_HOST"]       = "localhost"
os.environ["REDIS_PORT"]       = "16379"
os.environ["MINIO_ENDPOINT"]   = "localhost:19000"
os.environ["MINIO_ACCESS_KEY"] = "minioadmin"
os.environ["MINIO_SECRET_KEY"] = "minioadmin123"

from simulator.subscribers import generate_subscribers
from simulator.calendrier  import planifier_simulation
from simulator.config      import SEED, NB_ABONNES

print(f"[1/3] Génération de {NB_ABONNES} comptes (seed={SEED})...")
comptes   = generate_subscribers(NB_ABONNES, seed=SEED)
scenarios = planifier_simulation(comptes, seed=SEED)
evenements = [ev for sc in scenarios for ev in sc.evenements]
events    = [e.payload for e in evenements if e.stream == "transactions"]
n_fraude  = sum(1 for e in events if e.get("label_fraude") == 1)
print(f"      {len(events)} transactions | {n_fraude} fraudes")

from engine.anomaly.dataset import build_dataset
from engine.anomaly.train   import train

print("[2/3] Construction du dataset (filtre NORMAL + label=0)...")
ds = build_dataset(events=events)
print(f"      train={ds.n_train} | test={ds.n_test} | profils Redis={ds.meta['n_profiles']}")

print("[3/3] Entraînement Isolation Forest...")
res = train(ds, promote=True)
print()
print("=== Couche 2 — Résultat ===")
print(f"  Modèle         : {res.model_key}")
print(f"  Taux anomalie  : {res.metrics['train_anomaly_rate']:.3f}  (cible ~0.02)")
print(f"  Champion promu : {res.is_champion}")
print("===========================")
