"""
interfaces/store.py
───────────────────
Abstraction du stockage.

  LOCAL  → Redis  (feature store)  +  MinIO  (objets S3-compatible)
  AWS    → DynamoDB  (feature store)  +  S3  (objets)

Aucun code applicatif ne doit importer redis, boto3.dynamodb
ou boto3.s3 directement. Tout passe par ce module.
"""

import io
import json
import os
import pickle
from abc import ABC, abstractmethod
from typing import Any

ENV = os.getenv("ENV", "local")


# ════════════════════════════════════════════════════════════
#  FEATURE STORE  —  profils abonnés (lecture/écriture rapide)
# ════════════════════════════════════════════════════════════

class FeatureStore(ABC):
    """Stockage clé-valeur pour les profils abonnés."""

    @abstractmethod
    def get_profile(self, msisdn_hash: str) -> dict[str, Any]:
        """Retourne le profil d'un abonné, dict vide si inconnu."""

    @abstractmethod
    def set_profile(self, msisdn_hash: str, profile: dict[str, Any]) -> None:
        """Écrit ou met à jour le profil d'un abonné."""

    @abstractmethod
    def delete_profile(self, msisdn_hash: str) -> None:
        """Supprime le profil (utile pour les tests)."""


# ── Local : Redis ────────────────────────────────────────────

class RedisFeatureStore(FeatureStore):
    def __init__(self) -> None:
        import redis as _redis  # type: ignore

        self._client = _redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            decode_responses=True,
        )

    def get_profile(self, msisdn_hash: str) -> dict[str, Any]:
        raw = self._client.get(f"profile:{msisdn_hash}")
        if raw is None:
            return {}
        return json.loads(raw)

    def set_profile(self, msisdn_hash: str, profile: dict[str, Any]) -> None:
        self._client.set(f"profile:{msisdn_hash}", json.dumps(profile))

    def delete_profile(self, msisdn_hash: str) -> None:
        self._client.delete(f"profile:{msisdn_hash}")


# ── AWS : DynamoDB ───────────────────────────────────────────

class DynamoDBFeatureStore(FeatureStore):
    def __init__(self) -> None:
        import boto3  # type: ignore

        dynamodb = boto3.resource(
            "dynamodb",
            region_name=os.getenv("AWS_DEFAULT_REGION", "eu-west-3"),
        )
        table_name = os.getenv("DYNAMODB_TABLE_USER_PROFILES", "user_profiles")
        self._table = dynamodb.Table(table_name)

    def get_profile(self, msisdn_hash: str) -> dict[str, Any]:
        response = self._table.get_item(Key={"msisdn_hash": msisdn_hash})
        item = response.get("Item", {})
        # Supprimer la clé primaire du profil retourné
        item.pop("msisdn_hash", None)
        return item

    def set_profile(self, msisdn_hash: str, profile: dict[str, Any]) -> None:
        self._table.put_item(Item={"msisdn_hash": msisdn_hash, **profile})

    def delete_profile(self, msisdn_hash: str) -> None:
        self._table.delete_item(Key={"msisdn_hash": msisdn_hash})


# ════════════════════════════════════════════════════════════
#  OBJECT STORE  —  modèles, datasets, rapports
# ════════════════════════════════════════════════════════════

class ObjectStore(ABC):
    """Stockage d'objets (modèles .pkl, datasets .parquet, rapports)."""

    @abstractmethod
    def upload(self, bucket: str, key: str, data: bytes) -> None:
        """Envoie des bytes dans bucket/key."""

    @abstractmethod
    def download(self, bucket: str, key: str) -> bytes:
        """Télécharge bucket/key et retourne des bytes."""

    @abstractmethod
    def exists(self, bucket: str, key: str) -> bool:
        """Vérifie si bucket/key existe."""

    def save_model(self, bucket: str, key: str, model: Any) -> None:
        """Sérialise un modèle Python avec pickle et l'envoie."""
        self.upload(bucket, key, pickle.dumps(model))

    def load_model(self, bucket: str, key: str) -> Any:
        """Télécharge et désérialise un modèle Python."""
        return pickle.loads(self.download(bucket, key))

    def save_json(self, bucket: str, key: str, obj: Any) -> None:
        self.upload(bucket, key, json.dumps(obj, indent=2).encode("utf-8"))

    def load_json(self, bucket: str, key: str) -> Any:
        return json.loads(self.download(bucket, key).decode("utf-8"))


# ── Local : MinIO (API S3-compatible) ───────────────────────

class MinIOObjectStore(ObjectStore):
    def __init__(self) -> None:
        import boto3  # type: ignore

        endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
        self._client = boto3.client(
            "s3",
            endpoint_url=f"http://{endpoint}",
            aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
            region_name="us-east-1",  # MinIO ignore la région
        )

    def upload(self, bucket: str, key: str, data: bytes) -> None:
        self._client.put_object(Bucket=bucket, Key=key, Body=data)

    def download(self, bucket: str, key: str) -> bytes:
        response = self._client.get_object(Bucket=bucket, Key=key)
        return response["Body"].read()

    def exists(self, bucket: str, key: str) -> bool:
        try:
            self._client.head_object(Bucket=bucket, Key=key)
            return True
        except self._client.exceptions.ClientError:
            return False


# ── AWS : S3 ────────────────────────────────────────────────

class S3ObjectStore(ObjectStore):
    def __init__(self) -> None:
        import boto3  # type: ignore

        self._client = boto3.client(
            "s3",
            region_name=os.getenv("AWS_DEFAULT_REGION", "eu-west-3"),
        )

    def upload(self, bucket: str, key: str, data: bytes) -> None:
        self._client.put_object(Bucket=bucket, Key=key, Body=data)

    def download(self, bucket: str, key: str) -> bytes:
        response = self._client.get_object(Bucket=bucket, Key=key)
        return response["Body"].read()

    def exists(self, bucket: str, key: str) -> bool:
        try:
            self._client.head_object(Bucket=bucket, Key=key)
            return True
        except self._client.exceptions.ClientError:
            return False


# ════════════════════════════════════════════════════════════
#  Factories
# ════════════════════════════════════════════════════════════

def get_feature_store() -> FeatureStore:
    if ENV == "aws":
        return DynamoDBFeatureStore()
    return RedisFeatureStore()


def get_object_store() -> ObjectStore:
    if ENV == "aws":
        return S3ObjectStore()
    return MinIOObjectStore()


# Noms des buckets (identiques local et AWS)
BUCKET_DATASETS = os.getenv("MINIO_BUCKET_DATASETS", os.getenv("S3_BUCKET_DATASETS", "cg-datasets"))
BUCKET_MODELS   = os.getenv("MINIO_BUCKET_MODELS",   os.getenv("S3_BUCKET_MODELS",   "cg-models"))
BUCKET_REPORTS  = os.getenv("MINIO_BUCKET_REPORTS",  os.getenv("S3_BUCKET_REPORTS",  "cg-reports"))
