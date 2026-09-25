"""
engine/scoring_api/app.py
──────────────────────────
Service FastAPI de scoring temps réel (IA-7). Contrat : docs/contrat_api_scoring.md.

  POST /v1/score       scorer une transaction (3 couches + agrégation + décision)
  POST /reload-model   recharger règles et modèles (en-tête X-API-Key)
  GET  /health         état des couches, de la base des décisions et des métriques
  GET  /metrics        métriques courantes (latence p50/p99, décisions, scores)

Lancement local :
    uvicorn engine.scoring_api.app:app --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import logging
import os
import secrets
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

from fastapi import BackgroundTasks, FastAPI, Header, HTTPException
from pydantic import BaseModel, ConfigDict, Field, field_validator

from engine.scoring.realtime_pipeline import RealtimePipeline
from engine.scoring_api.decisions import DecisionStore
from engine.scoring_api.metrics import Metrics

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s %(name)s — %(message)s")
logger = logging.getLogger("scoring_api")


class Transaction(BaseModel):
    """Transaction à scorer (contrat v1 §1). Les champs inconnus sont ignorés."""
    model_config = ConfigDict(extra="ignore")

    id_transaction:   str = Field(min_length=1)
    id_compte:        str = Field(min_length=1)
    horodatage:       str
    montant:          float = Field(ge=0)
    devise:           str = "XOF"
    type_transaction: str | None = None
    id_beneficiaire:  str = ""
    device_id:        str = ""
    antenne:          str = ""
    solde_avant:      float | None = None
    solde_apres:      float | None = None

    @field_validator("horodatage")
    @classmethod
    def _iso(cls, v: str) -> str:
        try:
            datetime.fromisoformat(v)
        except ValueError as exc:
            raise ValueError("horodatage doit être au format ISO 8601, ex. 2026-07-14T09:19:02+00:00") from exc
        return v


def create_app(pipeline: RealtimePipeline | None = None, decisions: DecisionStore | None = None,
               metrics: Metrics | None = None, reload_api_key: str | None = None) -> FastAPI:
    """Fabrique de l'application (injection des dépendances pour les tests)."""
    etat: dict[str, Any] = {}
    cle = reload_api_key if reload_api_key is not None else os.getenv("RELOAD_API_KEY", "")

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        # Chargement des règles et des modèles au démarrage (MinIO en local, S3 sur AWS)
        etat["pipeline"]  = pipeline or RealtimePipeline()
        etat["decisions"] = decisions or DecisionStore()
        etat["metrics"]   = metrics or Metrics()
        etat["metrics"].demarrer()
        if not cle:
            logger.warning("RELOAD_API_KEY absente — /reload-model désactivé")
        yield
        etat["metrics"].arreter()

    app = FastAPI(title="CyberGuardian — moteur de scoring", version="1.0.0", lifespan=lifespan)

    @app.post("/v1/score")
    def score(tx: Transaction, taches: BackgroundTasks) -> dict[str, Any]:
        resultat = etat["pipeline"].score(tx.model_dump())
        etat["metrics"].enregistrer(resultat["latence_ms"], resultat["score_final"], resultat["decision"])
        taches.add_task(etat["decisions"].record, resultat)        # écrit après la réponse
        return resultat

    @app.post("/reload-model")
    def reload_model(x_api_key: str | None = Header(default=None)) -> dict[str, Any]:
        if not cle:
            raise HTTPException(503, "Rechargement désactivé : RELOAD_API_KEY n'est pas configurée.")
        if not x_api_key or not secrets.compare_digest(x_api_key, cle):
            raise HTTPException(401, "Clé d'API absente ou incorrecte (en-tête X-API-Key).")
        messages = etat["pipeline"].reload()
        logger.info("Rechargement effectué : %s", messages)
        return {"rechargement": messages, "versions": etat["pipeline"].versions()}

    @app.get("/health")
    def health() -> dict[str, Any]:
        pipe, base = etat["pipeline"], etat["decisions"].healthy()
        return {"status": "ok" if pipe.is_fully_ready and base else "degrade",
                "couches": pipe.status(),
                "base_decisions": "ok" if base else "indisponible",
                "metriques": etat["metrics"].backend}

    @app.get("/metrics")
    def metrics_endpoint() -> dict[str, Any]:
        return etat["metrics"].snapshot()

    return app


app = create_app()
