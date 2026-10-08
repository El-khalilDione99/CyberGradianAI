"""
interfaces/registry.py
───────────────────────
Registre relationnel (PostgreSQL local / RDS sur AWS) — IA-6.

Deux tables :

  models : un enregistrement par modèle entraîné (promu ou non)
           version, couche, clé S3 du modèle, métriques, dataset d'origine
           (clé S3 du Parquet + empreinte SHA-256), statut de promotion.

  labels : labels de fraude par transaction.
           source = "simulateur" (labels du simulateur, d'abord)
                  | "analyste"   (corrections saisies sur l'écran W-4, ensuite)
           Un label analyste l'emporte toujours sur un label simulateur.

Connexion : POSTGRES_DSN (docker-compose / RDS). Pour les tests et le notebook,
un DSN SQLite (ex. "sqlite://" en mémoire) fonctionne à l'identique.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import (
    Boolean, Column, DateTime, Float, Integer, MetaData, String, Table, Text,
    create_engine, select,
)
from sqlalchemy.pool import StaticPool

_metadata = MetaData()

models_table = Table(
    "models", _metadata,
    Column("id",              Integer, primary_key=True, autoincrement=True),
    Column("couche",          String(32),  nullable=False),   # "xgboost", "isolation_forest"…
    Column("version",         String(32),  nullable=False),
    Column("model_key",       String(255), nullable=False),   # clé S3 du .pkl
    Column("metrics_key",     String(255)),
    Column("training_mode",   String(16)),                    # "local" | "sagemaker"
    Column("dataset_key",     String(255)),                   # Parquet d'origine (S3)
    Column("dataset_sha256",  String(64)),
    Column("auc_pr_test",     Float),
    Column("champion_auc_pr", Float),                         # champion ré-évalué sur le même test
    Column("promoted",        Boolean, nullable=False, default=False),
    Column("decision",        Text),                          # explication de la décision
    Column("metrics_json",    Text),
    Column("created_at",      DateTime(timezone=True), nullable=False),
)

labels_table = Table(
    "labels", _metadata,
    Column("id_transaction", String(64), primary_key=True),
    Column("label",          Integer, nullable=False),        # 0 = légitime, 1 = fraude
    Column("source",         String(16), nullable=False),     # "simulateur" | "analyste"
    Column("auteur",         String(64)),
    Column("updated_at",     DateTime(timezone=True), nullable=False),
)


class ModelRegistry:
    """Accès aux tables models et labels."""

    def __init__(self, dsn: str | None = None) -> None:
        dsn = dsn or os.getenv("POSTGRES_DSN", "sqlite:///cg_registry.db")
        kwargs: dict[str, Any] = {}
        if dsn in ("sqlite://", "sqlite:///:memory:"):
            # une seule connexion partagée, sinon chaque connexion a sa propre base vide
            kwargs = {"connect_args": {"check_same_thread": False}, "poolclass": StaticPool}
        self.dsn = dsn
        self._engine = create_engine(dsn, future=True, **kwargs)
        _metadata.create_all(self._engine)

    # ── Table models ─────────────────────────────────────────

    def register_model(self, *, couche: str, version: str, model_key: str,
                       metrics: dict[str, Any], promoted: bool, decision: str,
                       metrics_key: str | None = None, dataset_key: str | None = None,
                       dataset_sha256: str | None = None,
                       champion_auc_pr: float | None = None) -> None:
        with self._engine.begin() as conn:
            conn.execute(models_table.insert().values(
                couche=couche, version=version, model_key=model_key,
                metrics_key=metrics_key,
                training_mode=metrics.get("training_mode"),
                dataset_key=dataset_key, dataset_sha256=dataset_sha256,
                auc_pr_test=metrics.get("auc_pr_test"),
                champion_auc_pr=champion_auc_pr,
                promoted=promoted, decision=decision,
                metrics_json=json.dumps(metrics, default=str),
                created_at=datetime.now(timezone.utc),
            ))

    def list_models(self, couche: str | None = None) -> list[dict[str, Any]]:
        q = select(models_table).order_by(models_table.c.id)
        if couche:
            q = q.where(models_table.c.couche == couche)
        with self._engine.connect() as conn:
            return [dict(r._mapping) for r in conn.execute(q)]

    # ── Table labels ─────────────────────────────────────────

    def set_label(self, id_transaction: str, label: int, source: str,
                  auteur: str | None = None) -> None:
        """Insère ou remplace un label. Un label analyste n'est jamais écrasé par le simulateur."""
        with self._engine.begin() as conn:
            existing = conn.execute(
                select(labels_table.c.source).where(labels_table.c.id_transaction == id_transaction)
            ).scalar()
            if existing == "analyste" and source != "analyste":
                return
            conn.execute(labels_table.delete().where(labels_table.c.id_transaction == id_transaction))
            conn.execute(labels_table.insert().values(
                id_transaction=id_transaction, label=int(label), source=source,
                auteur=auteur, updated_at=datetime.now(timezone.utc),
            ))

    def import_simulator_labels(self, labels: dict[str, int]) -> int:
        """Import en masse des labels du simulateur, sans écraser les labels analystes."""
        now = datetime.now(timezone.utc)
        with self._engine.begin() as conn:
            analystes = set(conn.execute(
                select(labels_table.c.id_transaction).where(labels_table.c.source == "analyste")
            ).scalars())
            conn.execute(labels_table.delete().where(labels_table.c.source == "simulateur"))
            rows = [{"id_transaction": k, "label": int(v), "source": "simulateur",
                     "auteur": None, "updated_at": now}
                    for k, v in labels.items() if k not in analystes]
            if rows:
                conn.execute(labels_table.insert(), rows)
        return len(rows)

    def get_labels(self, source: str | None = None) -> dict[str, int]:
        """{id_transaction: label}, filtré par source si demandé."""
        q = select(labels_table.c.id_transaction, labels_table.c.label)
        if source:
            q = q.where(labels_table.c.source == source)
        with self._engine.connect() as conn:
            return {r.id_transaction: int(r.label) for r in conn.execute(q)}


def get_registry(dsn: str | None = None) -> ModelRegistry:
    return ModelRegistry(dsn)
