"""
engine/scoring_api/decisions.py
────────────────────────────────
Enregistrement des décisions de scoring (table `decisions`, contrat v1 §6).

PostgreSQL local ou RDS via POSTGRES_DSN (même code, seul le DSN change).
Une base indisponible n'empêche jamais le scoring : l'erreur est journalisée
et l'état est exposé par /health.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any

from sqlalchemy import (JSON, Boolean, Column, DateTime, Float, Integer, MetaData, String, Table,
                        create_engine, select, text)
from sqlalchemy.pool import StaticPool

logger = logging.getLogger(__name__)
_metadata = MetaData()

decisions_table = Table(
    "decisions", _metadata,
    Column("id",                 Integer, primary_key=True, autoincrement=True),
    Column("id_transaction",     String(64), nullable=False, index=True),
    Column("id_compte",          String(64), nullable=False, index=True),
    Column("scored_at",          DateTime(timezone=True), nullable=False, index=True),
    Column("decision",           String(16), nullable=False),
    Column("alerte_prioritaire", Boolean, nullable=False),
    Column("score_final",        Integer, nullable=False),
    Column("score_c1",           Float),
    Column("score_c2",           Float),
    Column("score_c3",           Float),
    Column("score_combine",      Float),
    Column("regles",             JSON),
    Column("explications",       JSON),
    Column("versions",           JSON),
    Column("latence_ms",         Float),
)


class DecisionStore:
    """Écrit chaque décision dans la table `decisions`."""

    def __init__(self, dsn: str | None = None) -> None:
        self.dsn = dsn if dsn is not None else os.getenv("POSTGRES_DSN", "")
        self._engine = None
        if not self.dsn:
            logger.warning("POSTGRES_DSN absent — les décisions ne sont pas enregistrées")
            return
        kwargs: dict[str, Any] = {"pool_pre_ping": True}
        if self.dsn in ("sqlite://", "sqlite:///:memory:"):
            kwargs = {"connect_args": {"check_same_thread": False}, "poolclass": StaticPool}
        try:
            self._engine = create_engine(self.dsn, future=True, **kwargs)
            _metadata.create_all(self._engine)
        except Exception as exc:
            logger.error("Base des décisions indisponible : %s", exc)
            self._engine = None

    def record(self, r: dict[str, Any]) -> None:
        """Enregistre une réponse de scoring (format du contrat v1)."""
        if self._engine is None:
            return
        try:
            with self._engine.begin() as conn:
                conn.execute(decisions_table.insert().values(
                    id_transaction=r["id_transaction"], id_compte=r["id_compte"],
                    scored_at=datetime.fromisoformat(r["scored_at"]),
                    decision=r["decision"], alerte_prioritaire=r["alerte_prioritaire"],
                    score_final=r["score_final"],
                    score_c1=r["couche1"]["score"], score_c2=r["couche2"]["score"],
                    score_c3=r["couche3"]["score"], score_combine=r["agregation"]["score_combine"],
                    regles=r["couche1"]["regles"],
                    explications={"couche2": r["couche2"]["raisons"], "couche3": r["couche3"]["shap_top3"]},
                    versions={"couche1": r["couche1"]["version"], "couche2": r["couche2"]["version"],
                              "couche3": r["couche3"]["version"]},
                    latence_ms=r["latence_ms"],
                ))
        except Exception as exc:
            logger.error("Écriture de la décision %s échouée : %s", r.get("id_transaction"), exc)

    def healthy(self) -> bool:
        if self._engine is None:
            return False
        try:
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False

    def last(self, n: int = 10) -> list[dict[str, Any]]:
        """Dernières décisions (tests et diagnostic)."""
        if self._engine is None:
            return []
        with self._engine.connect() as conn:
            q = select(decisions_table).order_by(decisions_table.c.id.desc()).limit(n)
            return [dict(row._mapping) for row in conn.execute(q)]
