"""
engine/supervised/train.py
───────────────────────────
Entraînement XGBoost (IA-6) — local ou via SageMaker Training Job.

Spécification IA-6 :
  XGBoost entraîné via SageMaker Training Jobs à la demande (ml.m5.large,
  jobs de quelques minutes) sur les datasets exportés vers S3 au format
  Parquet. Déséquilibre géré par scale_pos_weight, validation croisée
  stratifiée, explicabilité SHAP.
  INTERDIT : endpoint d'inférence SageMaker permanent — l'inférence vit dans
  le conteneur de l'API (XGBoostDetector charge le .pkl depuis S3).
  Champion/challenger : un candidat n'est promu vers production/ que s'il bat
  le champion ; version, métriques et dataset d'origine sont enregistrés dans
  la table models (interfaces/registry.py).

Deux modes :
  1. LOCAL (défaut) : XGBoost Python directement — développement, notebook.
  2. SAGEMAKER (ENV=aws et USE_SAGEMAKER=true) : Training Job à la demande,
     puis récupération du modèle depuis S3 et même flux de versionnage.

Parité entraînement / inférence : le dataset vient de
engine.supervised.dataset.build_dataset(), qui calcule les features avec
compute_features() — la même fonction que XGBoostDetector.predict().

Versionnage S3 (bucket models) :
  xgboost/xgboost_<version>.pkl            ← bundle {model, feature_names, version}
  xgboost/xgboost_<version>_metrics.json
  xgboost/production/current.json           ← pointeur vers le champion
  xgboost/production/history.json           ← 20 derniers entraînements
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import numpy as np

from engine.supervised.dataset import (
    SupervisedDatasetResult, XGB_FEATURE_NAMES, export_dataset_parquet,
)
from interfaces.store import get_object_store, BUCKET_MODELS, BUCKET_DATASETS

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

# SageMaker — Training Job à la demande uniquement (jamais d'endpoint)
SAGEMAKER_ROLE          = os.getenv("SAGEMAKER_ROLE_ARN", "")
SAGEMAKER_INSTANCE_TYPE = os.getenv("SAGEMAKER_INSTANCE_TYPE", "ml.m5.large")
SAGEMAKER_MAX_RUNTIME_S = int(os.getenv("SAGEMAKER_MAX_RUNTIME_S", "1800"))
SAGEMAKER_REGION        = os.getenv("AWS_DEFAULT_REGION", "eu-west-3")

# Image XGBoost managée par AWS (algorithme intégré, version 1.7-1).
# Le compte ECR dépend de la région ; SAGEMAKER_XGB_IMAGE permet de forcer l'URI.
_XGB_IMAGE_ACCOUNTS = {
    "eu-west-3":    "659782779980",
    "eu-west-1":    "141502667606",
    "eu-central-1": "492215442770",
    "us-east-1":    "683313688378",
    "us-west-2":    "246618743249",
}


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
    decision:     str = ""         # explication champion / challenger
    champion_auc_pr: float | None = None


# ════════════════════════════════════════════════════════════
#  Point d'entrée
# ════════════════════════════════════════════════════════════

def train(
    dataset: SupervisedDatasetResult,
    store=None,
    promote: bool = True,
    registry=None,
    dataset_export: dict[str, str] | None = None,
    params_override: dict[str, Any] | None = None,
) -> TrainResult:
    """
    Entraîne XGBoost sur le dataset supervisé.

    Paramètres
    ----------
    dataset         : SupervisedDatasetResult (engine.supervised.dataset.build_dataset)
    store           : ObjectStore injecté (None = MinIO/S3)
    promote         : si True, applique la règle champion/challenger
    registry        : ModelRegistry (table models). None = get_registry()
    dataset_export  : résultat de export_dataset_parquet() ; exporté ici si absent
    params_override : hyperparamètres à remplacer (ex. pour comparer des candidats)
    """
    obj_store = store or get_object_store()
    if dataset_export is None:
        dataset_export = export_dataset_parquet(dataset, obj_store)
    if USE_SAGEMAKER and os.getenv("ENV", "local") == "aws":
        result = _train_sagemaker(dataset, obj_store, dataset_export, params_override)
    else:
        result = _train_local(dataset, obj_store, params_override)
    result.metrics["dataset_origine"] = dataset_export

    if promote:
        promoted, champion_auc, decision = _promote(obj_store, result, dataset)
    else:
        promoted, champion_auc, decision = False, None, "promotion non demandée"
    result.is_champion, result.champion_auc_pr, result.decision = promoted, champion_auc, decision
    _append_history(obj_store, result.model_key, result.metrics, promoted)
    _register(registry, result, dataset_export)
    return result


def xgb_params(scale_pos_weight: float, override: dict[str, Any] | None = None) -> dict[str, Any]:
    params = {
        "n_estimators":     XGB_N_ESTIMATORS,
        "max_depth":        XGB_MAX_DEPTH,
        "learning_rate":    XGB_LEARNING_RATE,
        "subsample":        XGB_SUBSAMPLE,
        "colsample_bytree": XGB_COLSAMPLE,
        "scale_pos_weight": scale_pos_weight,   # compense le déséquilibre des classes
        "objective":        "binary:logistic",
        "eval_metric":      "aucpr",
        "random_state":     SEED,
        "n_jobs":           -1,
        "tree_method":      "hist",
    }
    params.update(override or {})
    return params


# ════════════════════════════════════════════════════════════
#  Mode local — XGBoost direct
# ════════════════════════════════════════════════════════════

def _cv_splitter(dataset: SupervisedDatasetResult):
    """
    Validation croisée STRATIFIÉE (même taux de fraude dans chaque fold) et
    GROUPÉE par abonné (un abonné n'est jamais à la fois dans l'entraînement et
    la validation d'un fold — sinon ses fenêtres glissantes fuiraient).
    """
    from sklearn.model_selection import StratifiedGroupKFold, StratifiedKFold
    groups = dataset.groups_train
    if len(groups) == dataset.n_train and len(set(groups)) >= XGB_CV_FOLDS:
        return StratifiedGroupKFold(n_splits=XGB_CV_FOLDS, shuffle=True, random_state=SEED), groups, "StratifiedGroupKFold"
    return StratifiedKFold(n_splits=XGB_CV_FOLDS, shuffle=True, random_state=SEED), None, "StratifiedKFold"


def _train_local(
    dataset: SupervisedDatasetResult,
    obj_store,
    params_override: dict[str, Any] | None,
) -> TrainResult:
    import xgboost as xgb  # type: ignore
    from sklearn.metrics import average_precision_score

    version = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")[:-3]
    feature_names = dataset.feature_names or XGB_FEATURE_NAMES
    params = xgb_params(dataset.scale_pos_weight, params_override)

    logger.info("XGBoost local — n_train=%d (fraudes=%d, spw=%.1f)",
                dataset.n_train, int(dataset.y_train.sum()), dataset.scale_pos_weight)

    # ── 1. Validation croisée stratifiée (sur le train uniquement) ──
    cv, groups, cv_type = _cv_splitter(dataset)
    folds = []
    for k, (tr, va) in enumerate(cv.split(dataset.X_train, dataset.y_train, groups)):
        m = xgb.XGBClassifier(**params)
        m.fit(dataset.X_train[tr], dataset.y_train[tr])
        p = m.predict_proba(dataset.X_train[va])[:, 1]
        folds.append({"fold": k + 1, "n_val": int(len(va)), "fraudes_val": int(dataset.y_train[va].sum()),
                      "auc_pr": round(float(average_precision_score(dataset.y_train[va], p)), 4)})
    cv_scores = np.array([f["auc_pr"] for f in folds])
    logger.info("CV %s (%d folds) — AUC-PR %.4f ± %.4f", cv_type, len(folds), cv_scores.mean(), cv_scores.std())

    # ── 2. Entraînement final sur tout le train (le test n'est pas utilisé) ──
    model = xgb.XGBClassifier(**params)
    model.fit(dataset.X_train, dataset.y_train)

    # ── 3. Métriques sur le test ──────────────────────────────
    y_proba = model.predict_proba(dataset.X_test)[:, 1]
    auc_pr_test = float(average_precision_score(dataset.y_test, y_proba))

    # ── 4. Importance des features (gain) ─────────────────────
    gains = model.get_booster().get_score(importance_type="gain")
    gains = {feature_names[int(k[1:])] if k.startswith("f") and k[1:].isdigit() else k: v
             for k, v in gains.items()}
    total = sum(gains.values()) or 1.0
    feature_importance = {k: round(v / total, 6)
                          for k, v in sorted(gains.items(), key=lambda x: x[1], reverse=True)}

    metrics = {
        "version":            version,
        "training_mode":      "local",
        "n_train":            dataset.n_train,
        "n_test":             dataset.n_test,
        "n_fraud_train":      int(dataset.y_train.sum()),
        "n_fraud_test":       int(dataset.y_test.sum()),
        "fraud_rate_train":   round(dataset.fraud_rate_train, 4),
        "fraud_rate_test":    round(dataset.fraud_rate_test, 4),
        "scale_pos_weight":   round(dataset.scale_pos_weight, 2),
        "n_features":         len(feature_names),
        "feature_names":      feature_names,
        "xgb_params":         params,
        "cv_type":            cv_type,
        "cv_folds":           len(folds),
        "cv_detail":          folds,
        "cv_auc_pr_mean":     round(float(cv_scores.mean()), 4),
        "cv_auc_pr_std":      round(float(cv_scores.std()), 4),
        "auc_pr_test":        round(auc_pr_test, 4),
        "feature_importance": feature_importance,
        "seed":               SEED,
        "trained_at":         datetime.now(timezone.utc).isoformat(),
        "dataset_meta":       dataset.meta,
    }
    return _save(obj_store, model, feature_names, version, metrics, "local")


def _save(obj_store, model, feature_names, version, metrics, mode) -> TrainResult:
    import sklearn
    import xgboost as xgb  # type: ignore
    bundle = {"model": model, "feature_names": feature_names, "version": version,
              "xgboost_version": xgb.__version__, "sklearn_version": sklearn.__version__}
    model_key   = f"{MODEL_PREFIX}_{version}.pkl"
    metrics_key = f"{MODEL_PREFIX}_{version}_metrics.json"
    obj_store.save_model(BUCKET_MODELS, model_key, bundle)
    obj_store.save_json(BUCKET_MODELS, metrics_key, metrics)
    logger.info("Modèle XGBoost sauvegardé → %s/%s", BUCKET_MODELS, model_key)
    return TrainResult(model_key=model_key, metrics_key=metrics_key, version=version,
                       metrics=metrics, training_mode=mode)


# ════════════════════════════════════════════════════════════
#  Mode SageMaker — Training Job à la demande
# ════════════════════════════════════════════════════════════

def sagemaker_image_uri(region: str = SAGEMAKER_REGION) -> str:
    if os.getenv("SAGEMAKER_XGB_IMAGE"):
        return os.environ["SAGEMAKER_XGB_IMAGE"]
    account = _XGB_IMAGE_ACCOUNTS.get(region)
    if account is None:
        raise ValueError(f"Région {region} sans image XGBoost connue — définir SAGEMAKER_XGB_IMAGE")
    return f"{account}.dkr.ecr.{region}.amazonaws.com/sagemaker-xgboost:1.7-1"


def build_training_job_request(
    job_name: str,
    train_key: str,
    scale_pos_weight: float,
    params_override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Construit la requête sagemaker.create_training_job().
    Fonction pure : testable et inspectable sans compte AWS.

    - Algorithme XGBoost intégré AWS (pas de conteneur à maintenir)
    - Données : Parquet dans S3, label en 1re colonne
    - Instance ml.m5.large, 1 seule, durée max 30 min → job à la demande,
      les ressources sont libérées à la fin. AUCUN endpoint n'est créé.
    """
    p = xgb_params(scale_pos_weight, params_override)
    hyperparameters = {                        # noms attendus par l'algorithme intégré
        "num_round":        str(p["n_estimators"]),
        "max_depth":        str(p["max_depth"]),
        "eta":              str(p["learning_rate"]),
        "subsample":        str(p["subsample"]),
        "colsample_bytree": str(p["colsample_bytree"]),
        "scale_pos_weight": str(round(p["scale_pos_weight"], 4)),
        "objective":        p["objective"],
        "eval_metric":      p["eval_metric"],
        "tree_method":      p["tree_method"],
        "seed":             str(SEED),
    }
    return {
        "TrainingJobName": job_name,
        "AlgorithmSpecification": {"TrainingImage": sagemaker_image_uri(),
                                   "TrainingInputMode": "File"},
        "RoleArn": SAGEMAKER_ROLE,
        "InputDataConfig": [{
            "ChannelName": "train",
            "DataSource": {"S3DataSource": {
                "S3DataType": "S3Prefix",
                "S3Uri": f"s3://{BUCKET_DATASETS}/{train_key}",
                "S3DataDistributionType": "FullyReplicated",
            }},
            "ContentType": "application/x-parquet",
        }],
        "OutputDataConfig": {"S3OutputPath": f"s3://{BUCKET_MODELS}/xgboost/sagemaker_output/"},
        "ResourceConfig": {"InstanceType": SAGEMAKER_INSTANCE_TYPE, "InstanceCount": 1,
                           "VolumeSizeInGB": 10},
        "StoppingCondition": {"MaxRuntimeInSeconds": SAGEMAKER_MAX_RUNTIME_S},
        "HyperParameters": hyperparameters,
    }


def _train_sagemaker(
    dataset: SupervisedDatasetResult,
    obj_store,
    dataset_export: dict[str, str],
    params_override: dict[str, Any] | None,
) -> TrainResult:
    """Soumet un Training Job, attend sa fin, récupère le modèle. Aucun endpoint."""
    import boto3  # type: ignore

    version  = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    job_name = f"cg-xgboost-{version}".replace("_", "-")
    request  = build_training_job_request(job_name, dataset_export["train_key"],
                                          dataset.scale_pos_weight, params_override)

    sm = boto3.client("sagemaker", region_name=SAGEMAKER_REGION)
    logger.info("SageMaker Training Job %s — %s", job_name, SAGEMAKER_INSTANCE_TYPE)
    sm.create_training_job(**request)
    sm.get_waiter("training_job_completed_or_stopped").wait(TrainingJobName=job_name)
    desc = sm.describe_training_job(TrainingJobName=job_name)
    if desc["TrainingJobStatus"] != "Completed":
        raise RuntimeError(f"Training Job {job_name} : {desc['TrainingJobStatus']} "
                           f"({desc.get('FailureReason', '')})")

    booster = _download_sagemaker_booster(desc["ModelArtifacts"]["S3ModelArtifacts"])
    from sklearn.metrics import average_precision_score
    bundle_tmp = {"model": booster, "feature_names": dataset.feature_names}
    auc_pr = float(average_precision_score(dataset.y_test, predict_proba(bundle_tmp, dataset.X_test)))
    metrics = {
        "version":          version,
        "training_mode":    "sagemaker",
        "sagemaker_job":    job_name,
        "instance_type":    SAGEMAKER_INSTANCE_TYPE,
        "billable_seconds": desc.get("BillableTimeInSeconds"),
        "auc_pr_test":      round(auc_pr, 4),
        "xgb_params":       request["HyperParameters"],
        "feature_names":    dataset.feature_names,
        "n_train":          dataset.n_train,
        "scale_pos_weight": round(dataset.scale_pos_weight, 2),
        "trained_at":       datetime.now(timezone.utc).isoformat(),
        "dataset_meta":     dataset.meta,
    }
    return _save(obj_store, booster, dataset.feature_names, version, metrics, "sagemaker")


def _download_sagemaker_booster(s3_uri: str):
    import io
    import tarfile
    import boto3  # type: ignore
    import xgboost as xgb  # type: ignore
    bucket, key = s3_uri.replace("s3://", "").split("/", 1)
    tar_bytes = boto3.client("s3", region_name=SAGEMAKER_REGION).get_object(
        Bucket=bucket, Key=key)["Body"].read()
    with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:gz") as tar:
        member = next(m for m in tar.getmembers() if "xgboost-model" in m.name)
        model_bytes = tar.extractfile(member).read()
    booster = xgb.Booster()
    booster.load_model(bytearray(model_bytes))
    return booster


# ════════════════════════════════════════════════════════════
#  Champion / challenger
# ════════════════════════════════════════════════════════════

def predict_proba(bundle: dict, X: np.ndarray) -> np.ndarray:
    """Probabilités de fraude pour un bundle XGBClassifier ou Booster (SageMaker)."""
    import xgboost as xgb  # type: ignore
    model = bundle["model"]
    if isinstance(model, xgb.XGBClassifier):
        return model.predict_proba(X)[:, 1]
    return model.predict(xgb.DMatrix(X, feature_names=bundle.get("feature_names")))


def _promote(store, result: TrainResult, dataset: SupervisedDatasetResult
             ) -> tuple[bool, float | None, str]:
    """
    Le challenger est promu SEULEMENT s'il bat le champion.

    Comparaison équitable : le champion est rechargé et ré-évalué sur le
    MÊME jeu de test que le challenger (et non sur son ancien score).
    Promotion si AUC-PR challenger > AUC-PR champion + MIN_IMPROVEMENT.
    """
    from sklearn.metrics import average_precision_score
    challenger_auc = float(result.metrics.get("auc_pr_test", 0.0))

    try:
        current = store.load_json(BUCKET_MODELS, PRODUCTION_KEY)
    except Exception:
        current = {}
    if not current:
        decision = f"aucun champion en production → promotion automatique (AUC-PR {challenger_auc:.4f})"
        _write_current(store, result)
        return True, None, decision

    try:
        champion_bundle = store.load_model(BUCKET_MODELS, current["model_key"])
        champion_auc = float(average_precision_score(
            dataset.y_test, predict_proba(champion_bundle, dataset.X_test)))
    except Exception as exc:
        decision = f"champion illisible ({exc}) → promotion du challenger"
        _write_current(store, result)
        return True, None, decision

    if challenger_auc > champion_auc + MIN_IMPROVEMENT:
        decision = (f"challenger {challenger_auc:.4f} > champion {champion_auc:.4f} "
                    f"+ {MIN_IMPROVEMENT} → PROMU")
        _write_current(store, result)
        return True, champion_auc, decision

    decision = (f"challenger {challenger_auc:.4f} ≤ champion {champion_auc:.4f} "
                f"+ {MIN_IMPROVEMENT} → REJETÉ (le champion {current.get('version')} reste en production)")
    logger.info(decision)
    return False, champion_auc, decision


def _write_current(store, result: TrainResult) -> None:
    m = result.metrics
    store.save_json(BUCKET_MODELS, PRODUCTION_KEY, {
        "model_key":      result.model_key,
        "metrics_key":    result.metrics_key,
        "version":        result.version,
        "promoted_at":    datetime.now(timezone.utc).isoformat(),
        "auc_pr_test":    m.get("auc_pr_test", 0.0),
        "cv_auc_pr_mean": m.get("cv_auc_pr_mean", 0.0),
        "n_train":        m.get("n_train", 0),
        "training_mode":  result.training_mode,
        "dataset_origine": m.get("dataset_origine"),
    })
    logger.info("xgboost/production/current.json → %s", result.model_key)


def _append_history(store, model_key: str, metrics: dict, promoted: bool) -> None:
    """Ajoute une entrée dans xgboost/production/history.json (20 dernières)."""
    try:
        try:
            history: list = store.load_json(BUCKET_MODELS, HISTORY_KEY)
        except Exception:
            history = []
        history.append({
            "model_key":     model_key,
            "version":       metrics.get("version", "unknown"),
            "auc_pr_test":   metrics.get("auc_pr_test", 0.0),
            "n_train":       metrics.get("n_train", 0),
            "promoted":      promoted,
            "training_mode": metrics.get("training_mode", "local"),
            "recorded_at":   datetime.now(timezone.utc).isoformat(),
        })
        store.save_json(BUCKET_MODELS, HISTORY_KEY, history[-20:])
    except Exception as exc:
        logger.warning("Impossible de mettre à jour history.json : %s", exc)


def _register(registry, result: TrainResult, dataset_export: dict[str, str]) -> None:
    """Enregistre le candidat (promu ou non) dans la table models."""
    try:
        if registry is None:
            from interfaces.registry import get_registry
            registry = get_registry()
        registry.register_model(
            couche="xgboost", version=result.version, model_key=result.model_key,
            metrics_key=result.metrics_key, metrics=result.metrics,
            promoted=result.is_champion, decision=result.decision,
            dataset_key=dataset_export.get("train_key"),
            dataset_sha256=dataset_export.get("train_sha256"),
            champion_auc_pr=result.champion_auc_pr,
        )
    except Exception as exc:
        result.metrics["registry_error"] = str(exc)
        logger.error("Enregistrement dans la table models échoué : %s", exc)
