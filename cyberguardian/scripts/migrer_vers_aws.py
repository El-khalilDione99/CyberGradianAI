"""
scripts/migrer_vers_aws.py
──────────────────────────
Copie l'état local du moteur vers AWS, sans réentraîner :

  MinIO  cg-models/*         → S3        (règles, modèles anomaly/ et xgboost/)
  Redis  profile:<id_compte> → S3 profiles/profiles.json  (profils abonnés, par défaut)
                                ou DynamoDB avec --table

À lancer depuis le PC où tournent MinIO et Redis (docker compose), avec des
identifiants AWS configurés (aws configure). Les noms du bucket et de la table
sont donnés par `terraform output`.

    python scripts/migrer_vers_aws.py --bucket cyberguardian-models-123456789012 --region eu-west-3
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import boto3
import redis
from dotenv import load_dotenv

RACINE = Path(__file__).resolve().parents[1]
load_dotenv(RACINE / ".env.local")


def copier_modeles(bucket_source: str, bucket_cible: str, region: str) -> int:
    minio = boto3.client(
        "s3",
        endpoint_url=f"http://{os.getenv('MINIO_ENDPOINT', 'localhost:19000')}",
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
        region_name="us-east-1",
    )
    s3 = boto3.client("s3", region_name=region)
    n = 0
    for page in minio.get_paginator("list_objects_v2").paginate(Bucket=bucket_source):
        for obj in page.get("Contents", []):
            data = minio.get_object(Bucket=bucket_source, Key=obj["Key"])["Body"].read()
            s3.put_object(Bucket=bucket_cible, Key=obj["Key"], Body=data)
            print(f"  s3://{bucket_cible}/{obj['Key']}  ({len(data):,} octets)")
            n += 1
    return n


def _redis() -> redis.Redis:
    return redis.Redis(host=os.getenv("REDIS_HOST", "localhost"),
                       port=int(os.getenv("REDIS_PORT", "16379")), decode_responses=True)


def copier_profils_s3(bucket: str, cle: str, region: str) -> int:
    r = _redis()
    # Même format que S3SnapshotFeatureStore : {id_compte: profil}
    profils = {k.removeprefix("profile:"): json.loads(v)
               for k in r.scan_iter(match="profile:*", count=500)
               if (v := r.get(k)) is not None}
    boto3.client("s3", region_name=region).put_object(
        Bucket=bucket, Key=cle, Body=json.dumps(profils).encode("utf-8"))
    return len(profils)


def copier_profils(table: str, region: str) -> int:
    r = _redis()
    t = boto3.resource("dynamodb", region_name=region).Table(table)
    n = 0
    # Même format que DynamoDBFeatureStore : le profil JSON dans l'attribut `profile`
    with t.batch_writer() as lot:
        for cle in r.scan_iter(match="profile:*", count=500):
            brut = r.get(cle)
            if brut is None:
                continue
            lot.put_item(Item={"msisdn_hash": cle.removeprefix("profile:"), "profile": brut})
            n += 1
    return n


def main() -> None:
    p = argparse.ArgumentParser(description="Copie modèles (MinIO → S3) et profils (Redis → DynamoDB)")
    p.add_argument("--bucket", required=True, help="bucket S3 cible (terraform output models_bucket)")
    p.add_argument("--table", help="table DynamoDB cible ; sans elle, les profils vont dans S3")
    p.add_argument("--cle-profils", default="profiles/profiles.json", help="clé S3 du fichier de profils")
    p.add_argument("--region", default=os.getenv("AWS_DEFAULT_REGION", "eu-west-3"))
    p.add_argument("--bucket-source", default=os.getenv("MINIO_BUCKET_MODELS", "cg-models"))
    p.add_argument("--sans-profils", action="store_true", help="ne copier que les modèles et règles")
    a = p.parse_args()

    print(f"Modèles et règles : MinIO {a.bucket_source} → S3 {a.bucket}")
    nb = copier_modeles(a.bucket_source, a.bucket, a.region)
    print(f"→ {nb} objets copiés")
    if a.sans_profils:
        return
    if a.table:
        print(f"Profils abonnés : Redis → DynamoDB {a.table}")
        print(f"→ {copier_profils(a.table, a.region)} profils copiés")
    else:
        print(f"Profils abonnés : Redis → s3://{a.bucket}/{a.cle_profils}")
        print(f"→ {copier_profils_s3(a.bucket, a.cle_profils, a.region)} profils copiés")


if __name__ == "__main__":
    main()
