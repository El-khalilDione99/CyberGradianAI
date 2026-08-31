"""
engine/supervised/evaluate.py
──────────────────────────────
Harnais d'évaluation de la Couche 3 — XGBoost supervisé (IA-6).

Métriques calculées (alignées avec IA-8) :
  - AUC-PR    : métrique principale (classes déséquilibrées)
  - Rappel @1% FPR et @5% FPR
  - Précision @rappel 50% et 80%
  - Score moyen fraudes vs légitimes
  - Top-10 features par importance SHAP globale
  - Matrices de confusion aux seuils 30/50/70/90
  - Distribution des probabilités par classe (histogramme)

Différences avec evaluate.py (IA-5) :
  - _batch_score utilise predict_proba() au lieu de score_samples()
  - Ajout de la section SHAP globale (importance moyenne |SHAP|)
  - Rapport contient les hyperparamètres du modèle

Usage :
    from engine.supervised.evaluate import evaluate
    from engine.supervised.dataset  import build_dataset
    from engine.supervised.detector import XGBoostDetector

    dataset  = build_dataset(events=events)
    detector = XGBoostDetector()
    report   = evaluate(detector, dataset)
    print(report["auc_pr"], report["recall_at_1pct_fpr"])
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import numpy as np
from sklearn.metrics import (
    average_precision_score,
    precision_recall_curve,
    roc_curve,
    confusion_matrix,
)

from engine.supervised.dataset  import SupervisedDatasetResult, XGB_FEATURE_NAMES
from engine.supervised.detector import XGBoostDetector
from interfaces.store import get_object_store, BUCKET_REPORTS

logger = logging.getLogger(__name__)

REPORT_KEY_PREFIX = "xgboost/eval"


# ════════════════════════════════════════════════════════════
#  Point d'entrée
# ════════════════════════════════════════════════════════════

def evaluate(
    detector:    XGBoostDetector,
    dataset:     SupervisedDatasetResult,
    store=None,
    save_report: bool = True,
) -> dict[str, Any]:
    """
    Évalue le détecteur XGBoost sur dataset.X_test / y_test.

    Paramètres
    ----------
    detector    : XGBoostDetector chargé (bundle en mémoire)
    dataset     : SupervisedDatasetResult avec X_test, y_test
    store       : ObjectStore injecté pour les tests
    save_report : si True, publie le rapport dans MinIO/S3

    Retourne
    --------
    dict avec toutes les métriques, sérialisable JSON.
    """
    if not detector.is_ready:
        raise RuntimeError(
            "XGBoostDetector non initialisé — appeler reload_model() d'abord"
        )
    if len(dataset.y_test) == 0:
        raise ValueError("y_test est vide")
    n_fraud = int(dataset.y_test.sum())
    if n_fraud == 0:
        raise ValueError("Aucune fraude dans y_test")

    logger.info(
        "Évaluation XGBoost — n_test=%d, n_fraud=%d (%.1f%%)",
        len(dataset.y_test), n_fraud, 100 * dataset.fraud_rate_test,
    )

    # ── Scores en batch ───────────────────────────────────────
    y_proba = _batch_score(detector, dataset)
    y_true  = dataset.y_test.astype(int)
    y_score = np.array(y_proba, dtype=np.float32)

    # ── Métriques principales ─────────────────────────────────
    auc_pr = float(average_precision_score(y_true, y_score))

    precision_arr, recall_arr, _ = precision_recall_curve(y_true, y_score)

    recall_at_1pct_fpr = _recall_at_fpr(y_true, y_score, 0.01)
    recall_at_5pct_fpr = _recall_at_fpr(y_true, y_score, 0.05)
    prec_at_rec50      = _precision_at_recall(precision_arr, recall_arr, 0.50)
    prec_at_rec80      = _precision_at_recall(precision_arr, recall_arr, 0.80)

    # ── Distribution ──────────────────────────────────────────
    fraud_mask = y_true == 1
    legit_mask = y_true == 0
    # Utiliser les probabilités × 100 pour être cohérent avec le score 0-100
    scores_100 = y_score * 100.0

    score_mean_fraud = float(scores_100[fraud_mask].mean()) if fraud_mask.sum() > 0 else 0.0
    score_mean_legit = float(scores_100[legit_mask].mean()) if legit_mask.sum() > 0 else 0.0

    hist_fraud, bin_edges = np.histogram(scores_100[fraud_mask], bins=10, range=(0, 100))
    hist_legit, _         = np.histogram(scores_100[legit_mask], bins=10, range=(0, 100))

    # ── Matrices de confusion ─────────────────────────────────
    confusion = {}
    for thresh in [30, 50, 70, 90]:
        y_pred = (scores_100 >= thresh).astype(int)
        tn, fp, fn, tp = confusion_matrix(y_true, y_pred, labels=[0, 1]).ravel()
        confusion[str(thresh)] = {
            "tp": int(tp), "fp": int(fp), "tn": int(tn), "fn": int(fn),
            "precision": round(tp / (tp + fp) if (tp + fp) > 0 else 0.0, 4),
            "recall":    round(tp / (tp + fn) if (tp + fn) > 0 else 0.0, 4),
            "fpr":       round(fp / (fp + tn) if (fp + tn) > 0 else 0.0, 4),
        }

    # ── Importance SHAP globale ───────────────────────────────
    shap_importance = _compute_global_shap(detector, dataset)

    # ── Rapport ───────────────────────────────────────────────
    report = {
        # Métriques clés (alignées IA-8)
        "auc_pr":                  round(auc_pr, 4),
        "recall_at_1pct_fpr":      round(recall_at_1pct_fpr, 4),
        "recall_at_5pct_fpr":      round(recall_at_5pct_fpr, 4),
        "precision_at_recall_50":  round(prec_at_rec50, 4),
        "precision_at_recall_80":  round(prec_at_rec80, 4),
        # Distribution
        "score_mean_fraud":        round(score_mean_fraud, 2),
        "score_mean_legit":        round(score_mean_legit, 2),
        "score_separation":        round(score_mean_fraud - score_mean_legit, 2),
        "hist_fraud": {
            "counts":    hist_fraud.tolist(),
            "bin_edges": [round(b, 1) for b in bin_edges.tolist()],
        },
        "hist_legit": {
            "counts":    hist_legit.tolist(),
            "bin_edges": [round(b, 1) for b in bin_edges.tolist()],
        },
        # Matrices de confusion
        "confusion_by_threshold": confusion,
        # Explicabilité
        "shap_importance": shap_importance,
        # Méta
        "n_test":          len(dataset.y_test),
        "n_fraud_test":    n_fraud,
        "fraud_rate_test": round(dataset.fraud_rate_test, 4),
        "model_version":   detector.version,
        "evaluated_at":    datetime.now(timezone.utc).isoformat(),
        "feature_names":   dataset.feature_names,
    }

    _log_summary(report)

    # ── Publication dans MinIO/S3 ─────────────────────────────
    if save_report:
        try:
            obj_store = store or get_object_store()
            ts  = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            key = f"{REPORT_KEY_PREFIX}_{ts}.json"
            obj_store.save_json(BUCKET_REPORTS, key, report)
            report["report_key"] = key
            logger.info("Rapport XGBoost publié → %s/%s", BUCKET_REPORTS, key)
        except Exception as exc:
            logger.warning("Impossible de publier le rapport : %s", exc)

    return report


# ════════════════════════════════════════════════════════════
#  Helpers
# ════════════════════════════════════════════════════════════

def _batch_score(
    detector: XGBoostDetector,
    dataset:  SupervisedDatasetResult,
) -> list[float]:
    """Score en batch sur X_test — sans recalculer les features."""
    with detector._lock:
        bundle = detector._bundle

    if bundle is None:
        return [0.0] * len(dataset.X_test)

    import xgboost as xgb  # type: ignore

    model = bundle["model"]

    if isinstance(model, xgb.XGBClassifier):
        return model.predict_proba(dataset.X_test)[:, 1].tolist()

    # xgb.Booster (SageMaker)
    feature_names = bundle.get("feature_names", XGB_FEATURE_NAMES)
    dmat = xgb.DMatrix(dataset.X_test, feature_names=feature_names)
    return model.predict(dmat).tolist()


def _compute_global_shap(
    detector: XGBoostDetector,
    dataset:  SupervisedDatasetResult,
    max_samples: int = 200,
) -> list[dict[str, Any]]:
    """
    Calcule l'importance SHAP globale : moyenne des |valeurs SHAP| sur
    un sous-ensemble aléatoire du jeu de test.
    Retourne les features triées par importance décroissante.
    """
    try:
        import shap  # type: ignore

        with detector._lock:
            bundle = detector._bundle

        if bundle is None:
            return []

        model = bundle["model"]

        # Sous-ensemble aléatoire pour la performance
        n = min(max_samples, len(dataset.X_test))
        idx = np.random.RandomState(42).choice(len(dataset.X_test), n, replace=False)
        X_sample = dataset.X_test[idx]

        explainer   = shap.TreeExplainer(model)
        shap_values = explainer.shap_values(X_sample)

        # Prendre la classe 1 (fraude) si liste
        if isinstance(shap_values, list):
            sv = shap_values[1] if len(shap_values) > 1 else shap_values[0]
        else:
            sv = shap_values

        mean_abs = np.abs(sv).mean(axis=0)
        total    = mean_abs.sum() or 1.0
        result   = []
        for i in np.argsort(mean_abs)[::-1][:10]:
            result.append({
                "feature":    XGB_FEATURE_NAMES[i],
                "mean_shap":  round(float(mean_abs[i]), 6),
                "importance": round(float(mean_abs[i] / total), 4),
            })
        return result

    except Exception as exc:
        logger.warning("Calcul SHAP global échoué : %s", exc)
        return []


def _recall_at_fpr(
    y_true: np.ndarray, y_score: np.ndarray, target_fpr: float
) -> float:
    fpr_arr, tpr_arr, _ = roc_curve(y_true, y_score)
    mask = fpr_arr <= target_fpr
    if mask.sum() == 0:
        return 0.0
    return float(tpr_arr[mask].max())


def _precision_at_recall(
    precision_arr: np.ndarray, recall_arr: np.ndarray, target_recall: float
) -> float:
    mask = recall_arr >= target_recall
    if mask.sum() == 0:
        return 0.0
    return float(precision_arr[mask].max())


def _log_summary(report: dict) -> None:
    logger.info(
        "Évaluation XGBoost — AUC-PR=%.3f | Rappel@1%%FPR=%.3f | "
        "Score fraudes=%.1f vs légitimes=%.1f (sep=%.1f)",
        report["auc_pr"], report["recall_at_1pct_fpr"],
        report["score_mean_fraud"], report["score_mean_legit"],
        report["score_separation"],
    )
    if report["auc_pr"] < 0.5:
        logger.warning(
            "AUC-PR=%.3f < 0.5 — modèle peu discriminant. "
            "Vérifier scale_pos_weight et les features.",
            report["auc_pr"],
        )
