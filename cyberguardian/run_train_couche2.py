"""
run_train_couche2.py
─────────────────────
Entraîne la Couche 2 (IA-5 — Isolation Forest) et la publie dans MinIO/S3
(bucket models : anomaly/isolation_forest_<version>.pkl + pointeur
anomaly/production/current.json).

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
from simulator.profiles    import build_initial_profile

print(f"[1/3] Génération de {NB_ABONNES} comptes (seed={SEED})...")
comptes   = generate_subscribers(NB_ABONNES, seed=SEED)
# Profils initiaux AVANT la simulation (comme simulator/main.py) : la simulation
# modifie les comptes (nouveau téléphone, solde…), ce serait une fuite du futur.
profils_initiaux = {c.id_compte: build_initial_profile(c) for c in comptes}
scenarios = planifier_simulation(comptes, seed=SEED)
evenements = [ev for sc in scenarios for ev in sc.evenements]
# Les 3 flux sont nécessaires : sans sim-events / otp-events, les features
# de swap SIM et d'OTP resteraient constantes et le modèle serait aveugle.
events    = [{**e.payload, "stream": e.stream} for e in evenements]
tx        = [e for e in events if e["stream"] == "transactions"]
n_fraude  = sum(1 for e in tx if e.get("label_fraude") == 1)
print(f"      {len(events)} événements | {len(tx)} transactions | {n_fraude} fraudes")

from engine.anomaly.dataset import build_dataset
from engine.anomaly.train   import train

print("[2/3] Construction du dataset (train = NORMAL + label=0, split par abonné)...")
ds = build_dataset(events=events, profiles_override=profils_initiaux)
print(f"      train={ds.n_train} | val={ds.n_val} (fraudes={int(ds.y_val.sum())}) "
      f"| test={ds.n_test} (fraudes={int(ds.y_test.sum())})")

print("[3/3] Entraînement Isolation Forest...")
res = train(ds, promote=True)
m = res.metrics
print()
print("=== Couche 2 — Résultat ===")
print(f"  Modèle             : {res.model_key}")
print(f"  Paramètres retenus : max_samples={m['if_max_samples']} max_features={m['if_max_features']}")
print(f"  Rappel@1%FPR (val) : {m['val_recall_at_1pct_fpr']}")
print(f"  Champion promu     : {res.is_champion}")
print("===========================")
