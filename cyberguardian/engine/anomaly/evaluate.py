"""
engine/anomaly/evaluate.py
───────────────────────────
Harnais d'évaluation de la Couche 2 — Isolation Forest (IA-5).

Métriques calculées (alignées avec IA-8) :
  - AUC-PR    : aire sous la courbe précision/rappel (métrique principale
                pour les classes déséquilibrées)
  - Rappel    : à 1% FPR — combien de fraudes détecte-t-on si on accepte
                1% de faux positifs sur les légitimes
  - Précision : @rappel_50 — précision quand on rappelle 50% des fraudes
  - Score moyen fraudes vs légitimes (sanity check)
  - Distribution des scores par classe (pour visualisation)
  - Matrice de confusion à plusieurs seuils (30, 50, 70, 90)

Usage :
    from engine.anomaly.evaluate import evaluate
    from engine.anomaly.dataset  import build_dataset
    from engine.anomaly.detector import AnomalyDetector

    dataset  = build_dataset(events=events)
    detector = AnomalyDetector()
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

from engine.anomaly.dataset  import DatasetResult
from engine.anomaly.detector import AnomalyDetector
from interfaces.store import get_object_store, BUCKET_REPORTS

logger = logging.getLogger(__name__)

REPORT_KEY_PREFIX = "anomaly/eval"


# ════════════════════════════════════════════════════════════
#  Point d'entrée
# ════════════════════════════════════════════════════════════

def evaluate(
    detector: AnomalyDetector,
    dataset:  DatasetResult,
    store=None,
    save_report: bool = True,
) -> dict[str, Any]:
    """
    Évalue le détecteur d'anomalies sur dataset.X_test / y_test.

    Paramètres
    ----------
    detector    : AnomalyDetector chargé (modèle en mémoire)
    dataset     : DatasetResult avec X_test et y_test
    store       : ObjectStore injecté pour les tests
    save_report : si True, publie le rapport JSON dans MinIO/S3

    Retourne
    --------
    dict avec toutes les métriques (sérialisable JSON).
    """
    if not detector.is_ready:
        raise RuntimeError("AnomalyDetector non initialisé — appeler reload_model() d'abord")

    if len(dataset.y_test) == 0:
        raise ValueError("y_test est vide — impossible d'évaluer")

    n_fraud = int(dataset.y_test.sum())
    if n_fraud == 0:
        raise ValueError("Aucune fraude dans y_test — impossible de calculer AUC-PR")

    logger.info(
        "Évaluation — n_test=%d, n_fraud=%d (%.1f%%)",
        len(dataset.y_test), n_fraud,
        100 * dataset.fraud_rate_test,
    )

    # ── Scores sur le jeu de test ─────────────────────────────
    # On utilise le bundle directement pour obtenir les scores bruts IF
    # sans passer par predict() (plus rapide en batch).
    scores = _batch_score(detector, dataset)

    y_true = dataset.y_test.astype(int)
    y_score = np.array(scores, dtype=np.float32)

    # ── Métriques principales ─────────────────────────────────
    auc_pr = float(average_precision_score(y_true, y_score))

    precision_arr, recall_arr, thresholds_pr = precision_recall_curve(y_true, y_score)

    recall_at_1pct_fpr = _recall_at_fpr(y_true, y_score, target_fpr=0.01)
    recall_at_5pct_fpr = _recall_at_fpr(y_true, y_score, target_fpr=0.05)

    precision_at_recall_50 = _precision_at_recall(precision_arr, recall_arr, target_recall=0.50)
    precision_at_recall_80 = _precision_at_recall(precision_arr, recall_arr, target_recall=0.80)

    # ── Distribution des scores ───────────────────────────────
    fraud_mask  = y_true == 1
    legit_mask  = y_true == 0
    score_mean_fraud = float(y_score[fraud_mask].mean())  if fraud_mask.sum() > 0 else 0.0
    score_mean_legit = float(y_score[legit_mask].mean())  if legit_mask.sum() > 0 else 0.0
    score_sep        = score_mean_fraud - score_mean_legit  # > 0 si le modèle discrimine

    # Histogramme 10 bins pour visualisation
    hist_fraud, bin_edges = np.histogram(y_score[fraud_mask], bins=10, range=(0, 100))
    hist_legit, _         = np.histogram(y_score[legit_mask], bins=10, range=(0, 100))

    # ── Matrices de confusion à plusieurs seuils ─────────────
    confusion = {}
    for thresh in [30, 50, 70, 90]:
        y_pred = (y_score >= thresh).astype(int)
        tn, fp, fn, tp = confusion_matrix(y_true, y_pred, labels=[0, 1]).ravel()
        precision_t = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall_t    = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        fpr_t       = fp / (fp + tn) if (fp + tn) > 0 else 0.0
        confusion[str(thresh)] = {
            "tp": int(tp), "fp": int(fp), "tn": int(tn), "fn": int(fn),
            "precision": round(precision_t, 4),
            "recall":    round(recall_t, 4),
            "fpr":       round(fpr_t, 4),
        }

    # ── Rapport final ─────────────────────────────────────────
    report = {
        # Métriques clés (alignées IA-8)
        "auc_pr":                  round(auc_pr, 4),
        "recall_at_1pct_fpr":      round(recall_at_1pct_fpr, 4),
        "recall_at_5pct_fpr":      round(recall_at_5pct_fpr, 4),
        "precision_at_recall_50":  round(precision_at_recall_50, 4),
        "precision_at_recall_80":  round(precision_at_recall_80, 4),
        # Distribution des scores
        "score_mean_fraud":        round(score_mean_fraud, 2),
        "score_mean_legit":        round(score_mean_legit, 2),
        "score_separation":        round(score_sep, 2),
        "hist_fraud": {
            "counts": hist_fraud.tolist(),
            "bin_edges": [round(b, 1) for b in bin_edges.tolist()],
        },
        "hist_legit": {
            "counts": hist_legit.tolist(),
            "bin_edges": [round(b, 1) for b in bin_edges.tolist()],
        },
        # Matrices de confusion par seuil
        "confusion_by_threshold": confusion,
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
            ts    = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            key   = f"{REPORT_KEY_PREFIX}_{ts}.json"
            obj_store.save_json(BUCKET_REPORTS, key, report)
            report["report_key"] = key
            logger.info("Rapport d'évaluation publié → %s/%s", BUCKET_REPORTS, key)
        except Exception as exc:
            logger.warning("Impossible de publier le rapport : %s", exc)

    return report


# ════════════════════════════════════════════════════════════
#  Helpers
# ════════════════════════════════════════════════════════════

def _batch_score(
    detector: AnomalyDetector,
    dataset:  DatasetResult,
) -> list[float]:
    """
    Calcule les scores IF en batch directement depuis le bundle
    (sans recalculer les features — on utilise X_test directement).
    """
    with detector._lock:
        bundle = detector._bundle

    if bundle is None:
        return [0.0] * len(dataset.X_test)

    model  = bundle["model"]
    scaler = bundle["scaler"]

    X_scaled = scaler.transform(dataset.X_test)
    raw_scores = model.score_samples(X_scaled)

    # Même normalisation que dans detector._score_isolation_forest
    MAX_NORMAL = -0.05
    MIN_NORMAL = -0.50
    normalized = (raw_scores - MAX_NORMAL) / (MIN_NORMAL - MAX_NORMAL)
    normalized = np.clip(normalized, 0.0, 1.0) * 100.0

    return normalized.tolist()


def _recall_at_fpr(
    y_true:     np.ndarray,
    y_score:    np.ndarray,
    target_fpr: float,
) -> float:
    """
    Retourne le rappel maximal atteignable pour un FPR <= target_fpr.
    Utilise la courbe ROC.
    """
    fpr_arr, tpr_arr, _ = roc_curve(y_true, y_score)
    # Trouver le TPR (=rappel) maximal pour FPR <= target_fpr
    mask = fpr_arr <= target_fpr
    if mask.sum() == 0:
        return 0.0
    return float(tpr_arr[mask].max())


def _precision_at_recall(
    precision_arr: np.ndarray,
    recall_arr:    np.ndarray,
    target_recall: float,
) -> float:
    """
    Retourne la précision maximale pour un rappel >= target_recall.
    """
    mask = recall_arr >= target_recall
    if mask.sum() == 0:
        return 0.0
    return float(precision_arr[mask].max())


def _log_summary(report: dict) -> None:
    logger.info(
        "Évaluation terminée — AUC-PR=%.3f | Rappel@1%%FPR=%.3f | "
        "Score moyen fraudes=%.1f vs légitimes=%.1f (séparation=%.1f)",
        report["auc_pr"],
        report["recall_at_1pct_fpr"],
        report["score_mean_fraud"],
        report["score_mean_legit"],
        report["score_separation"],
    )
    if report["auc_pr"] < 0.5:
        logger.warning(
            "AUC-PR=%.3f < 0.5 — modèle peu discriminant. "
            "Vérifier les features et la contamination IF.",
            report["auc_pr"],
        )
