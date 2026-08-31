"""
run_train_couche3.py
─────────────────────
Entraîne la Couche 3 (IA-6 — XGBoost supervisé) depuis Redis + MinIO local.

Usage :
    python run_train_couche3.py
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

print(f"[1/4] Génération de {NB_ABONNES} comptes (seed={SEED})...")
comptes    = generate_subscribers(NB_ABONNES, seed=SEED)
scenarios  = planifier_simulation(comptes, seed=SEED)
evenements = [ev for sc in scenarios for ev in sc.evenements]
events     = [e.payload for e in evenements if e.stream == "transactions"]
n_fraude   = sum(1 for e in events if e.get("label_fraude") == 1)
print(f"      {len(events)} transactions | {n_fraude} fraudes")

from engine.supervised.dataset import build_dataset
from engine.supervised.train   import train
from engine.supervised.evaluate import evaluate
from engine.supervised.detector import XGBoostDetector

print("[2/4] Construction du dataset supervisé (toutes classes)...")
ds = build_dataset(events=events)
print(f"      train={ds.n_train}  (fraudes={int(ds.y_train.sum())}, légitimes={ds.n_train - int(ds.y_train.sum())})")
print(f"      test={ds.n_test}   (fraudes={int(ds.y_test.sum())})")
print(f"      scale_pos_weight={ds.scale_pos_weight:.1f}")

print("[3/4] Entraînement XGBoost (CV 5 folds stratifiée)...")
res = train(ds, promote=True)

print()
print("═" * 50)
print("  Couche 3 — Résultats d'entraînement")
print("═" * 50)
print(f"  Modèle            : {res.model_key}")
print(f"  AUC-PR test       : {res.metrics['auc_pr_test']:.4f}")
print(f"  CV AUC-PR (mean)  : {res.metrics['cv_auc_pr_mean']:.4f} ± {res.metrics['cv_auc_pr_std']:.4f}")
print(f"  scale_pos_weight  : {res.metrics['scale_pos_weight']:.1f}")
print(f"  Champion promu    : {res.is_champion}")
print(f"  Mode entraînement : {res.training_mode}")
print()
print("  Top 5 features (gain XGBoost) :")
for feat, imp in list(res.metrics["feature_importance"].items())[:5]:
    bar = "█" * int(imp * 40)
    print(f"    {feat:<32} {imp:.4f}  {bar}")

# ── Évaluation complète ──────────────────────────────────
print()
print("[4/4] Évaluation sur le jeu de test...")
detector = XGBoostDetector()
report   = evaluate(detector, ds, save_report=True)

print()
print("═" * 50)
print("  Couche 3 — Indicateurs d'évaluation")
print("═" * 50)
print(f"  AUC-PR                  : {report['auc_pr']:.4f}")
print(f"  Rappel @ 1% FPR         : {report['recall_at_1pct_fpr']:.4f}")
print(f"  Rappel @ 5% FPR         : {report['recall_at_5pct_fpr']:.4f}")
print(f"  Précision @ rappel 50%  : {report['precision_at_recall_50']:.4f}")
print(f"  Précision @ rappel 80%  : {report['precision_at_recall_80']:.4f}")
print(f"  Score moyen fraudes     : {report['score_mean_fraud']:.1f} / 100")
print(f"  Score moyen légitimes   : {report['score_mean_legit']:.1f} / 100")
print(f"  Séparation              : {report['score_separation']:.1f} points")
print()
print("  Matrices de confusion :")
print(f"  {'Seuil':<8} {'Précision':<12} {'Rappel':<10} {'FPR':<10} {'TP':<6} {'FP':<6} {'FN'}")
print("  " + "─" * 56)
for seuil, cm in report["confusion_by_threshold"].items():
    print(f"  {seuil:<8} {cm['precision']:<12.3f} {cm['recall']:<10.3f} {cm['fpr']:<10.3f} {cm['tp']:<6} {cm['fp']:<6} {cm['fn']}")
if report.get("shap_importance"):
    print()
    print("  Top 5 features SHAP globales :")
    for s in report["shap_importance"][:5]:
        bar = "█" * int(s["importance"] * 40)
        print(f"    {s['feature']:<32} {s['importance']:.4f}  {bar}")
print()
print(f"  Rapport sauvegardé dans MinIO : cg-reports/xgboost/eval_<timestamp>.json")
print("═" * 50)
