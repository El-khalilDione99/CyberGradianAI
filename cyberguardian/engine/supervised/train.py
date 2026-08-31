"""
engine/supervised/train.py
───────────────────────────
Entraînement XGBoost (IA-6) — local ou via SageMaker Training Job.

RÈGLE ABSOLUE : aucun endpoint d'inférence SageMaker créé.
  L'entraînement utilise SageMaker Training Jobs (ml.m5.large, CPU, ~5 min).
  L'inférence tourne dans le conteneur FastAPI — le modèle est chargé depuis S3.

Deux modes :
  1. LOCAL (ENV=local ou USE_SAGEMAKER=false) :
     XGBoost Python directement — rapide, pour le développement.

  2. SAGEMAKER (ENV=aws et USE_SAGEMAKER=true) :
     Soumet un Training Job SageMaker, attend la fin,
     récupère le modèle depuis S3 output.

Champion/challenger :
  - Comparaison sur AUC-PR sur le jeu de test de référence
  - Promotion si challenger_auc_pr > champion_auc_pr + MIN_IMPROVEMENT
  - Historique des entraînements dans production/history.json

Versionnage S3 :
  cg-models/
    xgboost/
      xgboost_<YYYYMMDD_HHmmss>.pkl       ← bundle {model, feature_names}
      xgboost_<YYYYMMDD_HHmmss>_metrics.json
      production/
        current.json                       ← champion actif
        history.json                       ← historique de tous les entraînements
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import numpy as np

from engine.supervised.dataset import SupervisedDatasetResult, XGB_FEATURE_NAMES
from interfaces.store import get_object_store, BUCKET_MODELS

logger = logging.getLogger(__name__)

# ── Paramètres XGBoost ────────────────────────────────────────
XGB_N_ESTIMATORS   = int(os.getenv("XGB_N_ESTIMATORS",    "300"))
XGB_MAX_DEPTH      = int(os.getenv("XGB_MAX_DEPTH",        "6"))
XGB_LEARNING_RATE  = float(os.getenv("XGB_LEARNING_RATE", "0.05"))
XGB_SUBSAMPLE      = float(os.getenv("XGB_SUBSAMPLE",     "0.8"))
XGB_COLSAMPLE      = float(os.getenv("XGB_COLSAMPLE",     "0.8"))
XGB_CV_FOLDS       = int(os.getenv("XGB_CV_FOLDS",        "5"))
SEED               = int(os.getenv("SEED",                 "42"))
USE_SAGEMAKER      = os.getenv("USE_SAGEMAKER", "false").lower() == "true"

# Seuil minimal d'amélioration AUC-PR pour promouvoir un challenger
MIN_IMPROVEMENT = float(os.getenv("XGB_MIN_IMPROVEMENT", "0.005"))

MODEL_PREFIX    = "xgboost/xgboost"
PRODUCTION_KEY  = "xgboost/production/current.json"
HISTORY_KEY     = "xgboost/production/history.json"

# SageMaker
SAGEMAKER_ROLE          = os.getenv("SAGEMAKER_ROLE_ARN", "")
SAGEMAKER_INSTANCE_TYPE = os.getenv("SAGEMAKER_INSTANCE_TYPE", "ml.m5.large")
SAGEMAKER_REGION        = os.getenv("AWS_DEFAULT_REGION", "eu-west-3")


# ════════════════════════════════════════════════════════════
#  Résultat d'entraînement
# ════════════════════════════════════════════════════════════

@dataclass
class TrainResult:
    model_key:    str
    metrics_key:  str
    version:      str
    metrics:      dict[str, Any] = field(default_factory=dict)
    is_champion:  bool = False
    training_mode: str = "local"   # "local" | "sagemaker"


# ════════════════════════════════════════════════════════════
#  Point d'entrée
# ════════════════════════════════════════════════════════════

def train(
    dataset: SupervisedDatasetResult,
    store=None,
    promote: bool = True,
) -> TrainResult:
    """
    Entraîne XGBoost sur le dataset supervisé.

    En mode local  (ENV=local)  : entraînement direct avec la lib XGBoost.
    En mode AWS    (ENV=aws + USE_SAGEMAKER=true) : SageMaker Training Job.

    Paramètres
    ----------
    dataset : SupervisedDatasetResult produit par build_dataset()
    store   : ObjectStore injecté pour les tests (None = auto)
    promote : si True, tente la promotion en production après évaluation

    Retourne
    --------
    TrainResult avec clés S3, métriques et statut champion.
    """
    if USE_SAGEMAKER and os.getenv("ENV", "local") == "aws":
        return _train_sagemaker(dataset, store, promote)
    return _train_local(dataset, store, promote)


# ════════════════════════════════════════════════════════════
#  Mode local — XGBoost direct
# ════════════════════════════════════════════════════════════

def _train_local(
    dataset: SupervisedDatasetResult,
    store,
    promote: bool,
) -> TrainResult:
    """Entraînement local XGBoost avec validation croisée stratifiée."""
    import xgboost as xgb  # type: ignore
    from sklearn.model_selection import StratifiedKFold, cross_validate
    from sklearn.metrics import average_precision_score

    obj_store = store or get_object_store()
    version   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    logger.info(
        "XGBoost local — n_train=%d (fraudes=%d, spw=%.1f) | "
        "n_estimators=%d, max_depth=%d, lr=%.3f",
        dataset.n_train,
        int(dataset.y_train.sum()),
        dataset.scale_pos_weight,
        XGB_N_ESTIMATORS, XGB_MAX_DEPTH, XGB_LEARNING_RATE,
    )

    # ── 1. Paramètres du modèle ───────────────────────────────
    params = {
        "n_estimators":       XGB_N_ESTIMATORS,
        "max_depth":          XGB_MAX_DEPTH,
        "learning_rate":      XGB_LEARNING_RATE,
        "subsample":          XGB_SUBSAMPLE,
        "colsample_bytree":   XGB_COLSAMPLE,
        "scale_pos_weight":   dataset.scale_pos_weight,
        "objective":          "binary:logistic",
        "eval_metric":        "aucpr",
        "random_state":       SEED,
        "n_jobs":             -1,
        "tree_method":        "hist",  # CPU, efficace en mémoire
    }

    # ── 2. Validation croisée stratifiée ─────────────────────
    # StratifiedKFold garantit la même proportion de fraudes dans chaque fold.
    cv = StratifiedKFold(n_splits=XGB_CV_FOLDS, shuffle=True, random_state=SEED)
    cv_model = xgb.XGBClassifier(**params)

    cv_results = cross_validate(
        cv_model,
        dataset.X_train, dataset.y_train,
        cv=cv,
        scoring="average_precision",
        return_train_score=True,
        n_jobs=1,  # pas de parallélisme imbriqué avec n_jobs=-1 dans XGB
    )

    cv_auc_pr_mean = float(cv_results["test_score"].mean())
    cv_auc_pr_std  = float(cv_results["test_score"].std())
    logger.info(
        "CV (%d folds) — AUC-PR : %.4f ± %.4f",
        XGB_CV_FOLDS, cv_auc_pr_mean, cv_auc_pr_std,
    )

    # ── 3. Entraînement final sur tout le train ───────────────
    model = xgb.XGBClassifier(**params)
    model.fit(
        dataset.X_train, dataset.y_train,
        eval_set=[(dataset.X_test, dataset.y_test)],
        verbose=False,
    )

    # ── 4. Métriques sur le test ──────────────────────────────
    y_proba = model.predict_proba(dataset.X_test)[:, 1]
    auc_pr_test = float(average_precision_score(dataset.y_test, y_proba))
    logger.info("AUC-PR test final : %.4f", auc_pr_test)

    # ── 5. Importance des features (gain) ─────────────────────
    importances = model.get_booster().get_score(importance_type="gain")
    # Normaliser par rapport au total
    total = sum(importances.values()) or 1.0
    feature_importance = {
        k: round(v / total, 6) for k, v in sorted(
            importances.items(), key=lambda x: x[1], reverse=True
        )
    }

    # ── 6. Métriques complètes ────────────────────────────────
    metrics = {
        "version":              version,
        "training_mode":        "local",
        "n_train":              dataset.n_train,
        "n_test":               dataset.n_test,
        "n_fraud_train":        int(dataset.y_train.sum()),
        "n_fraud_test":         int(dataset.y_test.sum()),
        "fraud_rate_train":     round(dataset.fraud_rate_train, 4),
        "fraud_rate_test":      round(dataset.fraud_rate_test, 4),
        "scale_pos_weight":     round(dataset.scale_pos_weight, 2),
        "n_features":           len(XGB_FEATURE_NAMES),
        "feature_names":        XGB_FEATURE_NAMES,
        "xgb_params":           params,
        "cv_folds":             XGB_CV_FOLDS,
        "cv_auc_pr_mean":       round(cv_auc_pr_mean, 4),
        "cv_auc_pr_std":        round(cv_auc_pr_std, 4),
        "auc_pr_test":          round(auc_pr_test, 4),
        "feature_importance":   feature_importance,
        "seed":                 SEED,
        "trained_at":           datetime.now(timezone.utc).isoformat(),
        "dataset_meta":         dataset.meta,
    }

    # ── 7. Sérialisation dans MinIO/S3 ────────────────────────
    # Bundle minimal : modèle + noms des features
    # (pas de scaler : XGBoost n'en a pas besoin avec tree_method=hist)
    bundle = {
        "model":         model,
        "feature_names": XGB_FEATURE_NAMES,
        "version":       version,
    }

    model_key   = f"{MODEL_PREFIX}_{version}.pkl"
    metrics_key = f"{MODEL_PREFIX}_{version}_metrics.json"

    obj_store.save_model(BUCKET_MODELS, model_key, bundle)
    obj_store.save_json(BUCKET_MODELS, metrics_key, metrics)
    logger.info("Modèle XGBoost sauvegardé → %s/%s", BUCKET_MODELS, model_key)

    # ── 8. Champion/challenger ────────────────────────────────
    is_champion = False
    if promote:
        is_champion = _promote(obj_store, model_key, metrics_key, metrics)

    return TrainResult(
        model_key     = model_key,
        metrics_key   = metrics_key,
        version       = version,
        metrics       = metrics,
        is_champion   = is_champion,
        training_mode = "local",
    )


# ════════════════════════════════════════════════════════════
#  Mode SageMaker — Training Job
# ════════════════════════════════════════════════════════════

def _train_sagemaker(
    dataset: SupervisedDatasetResult,
    store,
    promote: bool,
) -> TrainResult:
    """
    Soumet un SageMaker Training Job.

    Prérequis :
      - SAGEMAKER_ROLE_ARN défini
      - Dataset Parquet déjà exporté dans S3 (build_dataset(..., export_parquet=True))
      - Pas d'endpoint d'inférence créé — règle absolue du guide

    Le job :
      1. Lit le Parquet depuis S3 input
      2. Entraîne XGBoost
      3. Sauvegarde model.tar.gz dans S3 output
      Le présent module télécharge ensuite le .tar.gz, l'extrait,
      resérialise en .pkl et suit le même flow de versionnage que le mode local.
    """
    import boto3  # type: ignore

    obj_store = store or get_object_store()
    version   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    job_name  = f"cg-xgboost-{version}"

    from engine.supervised.dataset import PARQUET_TRAIN_KEY, PARQUET_TEST_KEY

    s3_input_train  = f"s3://{BUCKET_MODELS.replace('cg-', 'cg-')}/{PARQUET_TRAIN_KEY}"
    # En pratique le bucket est cg-datasets pour les données
    from interfaces.store import BUCKET_DATASETS
    s3_input_train = f"s3://{BUCKET_DATASETS}/{PARQUET_TRAIN_KEY}"
    s3_output      = f"s3://{BUCKET_MODELS}/xgboost/sagemaker_output/"

    logger.info(
        "Lancement SageMaker Training Job %s — instance=%s",
        job_name, SAGEMAKER_INSTANCE_TYPE,
    )

    sm_client = boto3.client("sagemaker", region_name=SAGEMAKER_REGION)

    hyperparameters = {
        "n_estimators":     str(XGB_N_ESTIMATORS),
        "max_depth":        str(XGB_MAX_DEPTH),
        "learning_rate":    str(XGB_LEARNING_RATE),
        "subsample":        str(XGB_SUBSAMPLE),
        "colsample_bytree": str(XGB_COLSAMPLE),
        "scale_pos_weight": str(round(dataset.scale_pos_weight, 4)),
        "seed":             str(SEED),
    }

    # Image XGBoost managée par AWS (pas besoin de Dockerfile custom)
    xgb_image = (
        f"683313688378.dkr.ecr.{SAGEMAKER_REGION}.amazonaws.com/"
        "sagemaker-xgboost:1.7-1"
    )

    sm_client.create_training_job(
        TrainingJobName     = job_name,
        AlgorithmSpecification = {
            "TrainingImage":     xgb_image,
            "TrainingInputMode": "File",
        },
        RoleArn             = SAGEMAKER_ROLE,
        InputDataConfig     = [{
            "ChannelName":     "train",
            "DataSource":      {
                "S3DataSource": {
                    "S3DataType":             "S3Prefix",
                    "S3Uri":                  s3_input_train,
                    "S3DataDistributionType": "FullyReplicated",
                }
            },
            "ContentType": "application/x-parquet",
        }],
        OutputDataConfig    = {"S3OutputPath": s3_output},
        ResourceConfig      = {
            "InstanceType":  SAGEMAKER_INSTANCE_TYPE,
            "InstanceCount": 1,
            "VolumeSizeInGB": 10,
        },
        StoppingCondition   = {"MaxRuntimeInSeconds": 1800},
        HyperParameters     = hyperparameters,
    )

    # Attendre la fin du job
    logger.info("Job SageMaker soumis — attente de la fin…")
    waiter = sm_client.get_waiter("training_job_completed_or_stopped")
    waiter.wait(TrainingJobName=job_name)

    # Récupérer le statut
    response = sm_client.describe_training_job(TrainingJobName=job_name)
    status   = response["TrainingJobStatus"]
    if status != "Completed":
        raise RuntimeError(
            f"SageMaker Training Job {job_name} terminé avec statut : {status}"
        )

    model_artifact = response["ModelArtifacts"]["S3ModelArtifacts"]
    logger.info("SageMaker Job terminé — artefact : %s", model_artifact)

    # Extraire le modèle depuis model.tar.gz → re-sérialiser en .pkl
    model_key, metrics, bundle = _extract_sagemaker_model(
        obj_store, model_artifact, version, dataset, hyperparameters
    )
    metrics_key = f"{MODEL_PREFIX}_{version}_metrics.json"
    obj_store.save_json(BUCKET_MODELS, metrics_key, metrics)

    is_champion = False
    if promote:
        is_champion = _promote(obj_store, model_key, metrics_key, metrics)

    return TrainResult(
        model_key     = model_key,
        metrics_key   = metrics_key,
        version       = version,
        metrics       = metrics,
        is_champion   = is_champion,
        training_mode = "sagemaker",
    )


def _extract_sagemaker_model(
    store,
    s3_uri:   str,
    version:  str,
    dataset:  SupervisedDatasetResult,
    hyperparameters: dict,
) -> tuple[str, dict, dict]:
    """
    Télécharge model.tar.gz depuis S3, extrait le modèle XGBoost,
    le ré-emballe dans notre format bundle et le sauvegarde.
    """
    import tarfile
    import io
    import pickle
    import boto3  # type: ignore
    from sklearn.metrics import average_precision_score

    # Télécharger model.tar.gz
    bucket, key = s3_uri.replace("s3://", "").split("/", 1)
    s3 = boto3.client("s3", region_name=SAGEMAKER_REGION)
    response = s3.get_object(Bucket=bucket, Key=key)
    tar_bytes = response["Body"].read()

    # Extraire xgboost-model (format natif SageMaker XGBoost)
    with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:gz") as tar:
        member = next(m for m in tar.getmembers() if "xgboost" in m.name)
        model_bytes = tar.extractfile(member).read()

    import xgboost as xgb  # type: ignore
    booster = xgb.Booster()
    booster.load_model(bytearray(model_bytes))

    # Recalculer AUC-PR sur le test
    dtest   = xgb.DMatrix(dataset.X_test, feature_names=XGB_FEATURE_NAMES)
    y_proba = booster.predict(dtest)
    auc_pr  = float(average_precision_score(dataset.y_test, y_proba))

    bundle = {
        "model":         booster,
        "feature_names": XGB_FEATURE_NAMES,
        "version":       version,
        "model_type":    "xgb.Booster",  # indique le type pour le detector
    }

    model_key = f"{MODEL_PREFIX}_{version}.pkl"
    store.save_model(BUCKET_MODELS, model_key, bundle)

    metrics = {
        "version":          version,
        "training_mode":    "sagemaker",
        "auc_pr_test":      round(auc_pr, 4),
        "xgb_params":       hyperparameters,
        "feature_names":    XGB_FEATURE_NAMES,
        "n_train":          dataset.n_train,
        "scale_pos_weight": round(dataset.scale_pos_weight, 2),
        "trained_at":       datetime.now(timezone.utc).isoformat(),
    }

    return model_key, metrics, bundle


# ════════════════════════════════════════════════════════════
#  Champion / challenger
# ════════════════════════════════════════════════════════════

def _promote(
    store,
    model_key:   str,
    metrics_key: str,
    metrics:     dict,
) -> bool:
    """
    Promotion en production basée sur AUC-PR.

    - Aucun champion → promotion automatique
    - Challenger AUC-PR > champion AUC-PR + MIN_IMPROVEMENT → promotion
    - Sinon → archivé dans history.json sans promotion
    """
    current: dict[str, Any] = {}
    try:
        current = store.load_json(BUCKET_MODELS, PRODUCTION_KEY)
    except Exception:
        pass  # pas encore de champion

    challenger_auc = metrics.get("auc_pr_test", 0.0)
    champion_auc   = current.get("auc_pr_test", 0.0)

    # Promotion automatique si pas de champion
    if not current:
        logger.info(
            "Aucun champion XGBoost — promotion automatique (AUC-PR=%.4f)",
            challenger_auc,
        )
        _write_current(store, model_key, metrics_key, metrics)
        _append_history(store, model_key, metrics, promoted=True)
        return True

    # Comparer les AUC-PR
    if challenger_auc > champion_auc + MIN_IMPROVEMENT:
        logger.info(
            "Challenger AUC-PR=%.4f bat champion AUC-PR=%.4f (+%.4f) → promotion",
            challenger_auc, champion_auc, challenger_auc - champion_auc,
        )
        _write_current(store, model_key, metrics_key, metrics)
        _append_history(store, model_key, metrics, promoted=True)
        return True

    logger.info(
        "Challenger AUC-PR=%.4f ne bat pas champion AUC-PR=%.4f "
        "(seuil +%.4f requis) → pas de promotion",
        challenger_auc, champion_auc, MIN_IMPROVEMENT,
    )
    _append_history(store, model_key, metrics, promoted=False)
    return False


def _write_current(
    store, model_key: str, metrics_key: str, metrics: dict
) -> None:
    """Écrit xgboost/production/current.json."""
    current_info = {
        "model_key":      model_key,
        "metrics_key":    metrics_key,
        "version":        metrics.get("version", "unknown"),
        "promoted_at":    datetime.now(timezone.utc).isoformat(),
        "auc_pr_test":    metrics.get("auc_pr_test", 0.0),
        "cv_auc_pr_mean": metrics.get("cv_auc_pr_mean", 0.0),
        "n_train":        metrics.get("n_train", 0),
        "training_mode":  metrics.get("training_mode", "local"),
    }
    store.save_json(BUCKET_MODELS, PRODUCTION_KEY, current_info)
    logger.info("xgboost/production/current.json → %s", model_key)


def _append_history(
    store, model_key: str, metrics: dict, promoted: bool
) -> None:
    """
    Ajoute une entrée dans xgboost/production/history.json.
    Conserve les 20 derniers entraînements.
    """
    try:
        history: list = []
        try:
            history = store.load_json(BUCKET_MODELS, HISTORY_KEY)
        except Exception:
            pass

        entry = {
            "model_key":     model_key,
            "version":       metrics.get("version", "unknown"),
            "auc_pr_test":   metrics.get("auc_pr_test", 0.0),
            "n_train":       metrics.get("n_train", 0),
            "promoted":      promoted,
            "training_mode": metrics.get("training_mode", "local"),
            "recorded_at":   datetime.now(timezone.utc).isoformat(),
        }
        history.append(entry)
        history = history[-20:]  # garder les 20 derniers
        store.save_json(BUCKET_MODELS, HISTORY_KEY, history)
    except Exception as exc:
        logger.warning("Impossible de mettre à jour history.json : %s", exc)
