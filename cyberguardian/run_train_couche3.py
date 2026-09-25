"""
run_train_couche3.py
─────────────────────
Entraîne la Couche 3 (IA-6 — XGBoost supervisé).

  1. Événements du simulateur (3 flux : transactions, sim-events, otp-events)
  2. Labels : simulateur, remplacés par les labels analystes (table labels, écran W-4)
  3. Dataset : replay chronologique depuis les profils initiaux de Redis, features
     calculées par compute_features() — le même calcul qu'en production
  4. Export Parquet versionné dans S3 (bucket datasets)
  5. Entraînement : local, ou SageMaker Training Job si ENV=aws et USE_SAGEMAKER=true
  6. Champion/challenger + enregistrement dans la table models (PostgreSQL)

Usage :
    python run_train_couche3.py
"""
import sys, os
sys.path.insert(0, ".")

os.environ.setdefault("REDIS_HOST",       "localhost")
os.environ.setdefault("REDIS_PORT",       "16379")
os.environ.setdefault("MINIO_ENDPOINT",   "localhost:19000")
os.environ.setdefault("MINIO_ACCESS_KEY", "minioadmin")
os.environ.setdefault("MINIO_SECRET_KEY", "minioadmin123")
os.environ.setdefault("POSTGRES_DSN",
                      "postgresql://cyberguardian:cyberguardian_dev@localhost:15432/cyberguardian")

from simulator.subscribers import generate_subscribers
from simulator.calendrier  import planifier_simulation
from simulator.config      import SEED, NB_ABONNES
from simulator.profiles    import build_initial_profile

print(f"[1/5] Génération de {NB_ABONNES} comptes (seed={SEED})...")
comptes   = generate_subscribers(NB_ABONNES, seed=SEED)
# Profils initiaux AVANT la simulation (comme simulator/main.py) : la simulation
# modifie les comptes (nouveau téléphone, solde…), ce serait une fuite du futur.
profils_initiaux = {c.id_compte: build_initial_profile(c) for c in comptes}
scenarios = planifier_simulation(comptes, seed=SEED)
events    = [{**e.payload, "stream": e.stream} for sc in scenarios for e in sc.evenements]

from interfaces.registry        import get_registry
from interfaces.store           import get_object_store
from engine.supervised.dataset  import build_dataset, export_dataset_parquet
from engine.supervised.train    import train

registry = get_registry()
labels_analystes = registry.get_labels(source="analyste")
print(f"[2/5] Labels analystes (écran W-4) : {len(labels_analystes)}")

print("[3/5] Construction du dataset (replay depuis les profils initiaux)...")
ds = build_dataset(events=events, profiles_override=profils_initiaux,
                   labels_override=labels_analystes)
print(f"      train={ds.n_train} (fraudes={int(ds.y_train.sum())}) | "
      f"test={ds.n_test} (fraudes={int(ds.y_test.sum())}) | scale_pos_weight={ds.scale_pos_weight:.1f}")

store = get_object_store()
export = export_dataset_parquet(ds, store)
print(f"[4/5] Parquet exporté : {export['train_key']}")

print("[5/5] Entraînement XGBoost + champion/challenger...")
res = train(ds, store=store, registry=registry, dataset_export=export)
m = res.metrics
print()
print("═" * 60)
print(f"  Modèle            : {res.model_key}  ({res.training_mode})")
print(f"  AUC-PR test       : {m['auc_pr_test']:.4f}")
if "cv_auc_pr_mean" in m:
    print(f"  CV AUC-PR         : {m['cv_auc_pr_mean']:.4f} ± {m['cv_auc_pr_std']:.4f} ({m['cv_type']})")
print(f"  Décision          : {res.decision}")
if m.get("registry_error"):
    print(f"  ⚠️ Table models    : {m['registry_error']}")
print("═" * 60)
