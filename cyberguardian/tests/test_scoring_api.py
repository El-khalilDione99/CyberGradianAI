"""
tests/test_scoring_api.py
──────────────────────────
Service de scoring IA-7 (contrat docs/contrat_api_scoring.md), sans infrastructure :
modèles entraînés en mémoire, base des décisions SQLite en mémoire, feature store factice.
"""

import sys

import pytest

sys.path.insert(0, ".")

from fastapi.testclient import TestClient

from simulator.subscribers import generate_subscribers
from simulator.calendrier import planifier_simulation
from simulator.profiles import build_initial_profile
from interfaces.store import ObjectStore, FeatureStore
from interfaces.registry import ModelRegistry
from engine.anomaly.dataset import build_dataset as build_ds2
from engine.anomaly.train import train as train2
from engine.supervised.dataset import build_dataset as build_ds3
from engine.supervised.train import train as train3
from engine.scoring.aggregator import agreger, decider
from engine.scoring.realtime_pipeline import RealtimePipeline
from engine.scoring_api.app import create_app
from engine.scoring_api.decisions import DecisionStore
from engine.scoring_api.metrics import Metrics

CLE = "cle-de-test"


class MemStore(ObjectStore):
    def __init__(self): self._d = {}
    def upload(self, b, k, d): self._d[f"{b}/{k}"] = d
    def download(self, b, k): return self._d[f"{b}/{k}"]
    def exists(self, b, k): return f"{b}/{k}" in self._d


class DictFeatureStore(FeatureStore):
    def __init__(self, profils): self._p = profils
    def get_profile(self, k): return self._p.get(k, {})
    def set_profile(self, k, p): self._p[k] = p
    def delete_profile(self, k): self._p.pop(k, None)


@pytest.fixture(scope="module")
def monde():
    comptes = generate_subscribers(120, seed=7)
    init = {c.id_compte: build_initial_profile(c) for c in comptes}
    events = [{**e.payload, "stream": e.stream} for sc in planifier_simulation(comptes, seed=7) for e in sc.evenements]
    store = MemStore()
    train2(build_ds2(events=events, profiles_override=init), store=store)
    train3(build_ds3(events=events, profiles_override=init), store=store, registry=ModelRegistry("sqlite://"))
    tx = [e for e in events if e["stream"] == "transactions"]
    return store, init, tx


@pytest.fixture()
def client(monde):
    store, init, _ = monde
    pipeline = RealtimePipeline(store=store, feature_store=DictFeatureStore(dict(init)))
    decisions = DecisionStore("sqlite://")
    app = create_app(pipeline=pipeline, decisions=decisions, metrics=Metrics("local"), reload_api_key=CLE)
    with TestClient(app) as c:
        c.decisions, c.pipeline = decisions, pipeline
        yield c


def _payload(ev):
    return {k: ev[k] for k in ["id_transaction", "id_compte", "horodatage", "montant", "devise",
                               "type_transaction", "id_beneficiaire", "device_id", "antenne",
                               "solde_avant", "solde_apres"]}


# ── Agrégation et politique de seuils ─────────────────────────

def test_agregation_ponderee_trois_couches():
    assert agreger(92, 10, 20, 0.05, 0.40, 0.55) == (20, 19.6)  # 0,05·92 + 0,40·10 + 0,55·20
    assert agreger(0, 50, 100) == (75, 75.0)                    # 0,40·50 + 0,55·100
    assert agreger(100, 100, 100) == (100, 100.0)               # plafond


def test_agregateur_pur_sans_fastapi_ni_entree_sortie():
    """aggregator.py ne dépend ni de FastAPI, ni du stockage, ni des modèles."""
    import ast, pathlib
    src = pathlib.Path("engine/scoring/aggregator.py").read_text(encoding="utf-8")
    modules = {n.module or "" for n in ast.walk(ast.parse(src)) if isinstance(n, ast.ImportFrom)} | \
              {a.name for n in ast.walk(ast.parse(src)) if isinstance(n, ast.Import) for a in n.names}
    assert modules <= {"__future__", "os"}, modules
    import engine.scoring.realtime_pipeline as rp, engine.scoring.aggregator as ag
    assert rp.agreger is ag.agreger and rp.decider is ag.decider     # une seule implémentation


@pytest.mark.parametrize("score,decision,alerte", [
    (0, "PASS", False), (29, "PASS", False), (30, "CHALLENGE", False), (69, "CHALLENGE", False),
    (70, "BLOCK", False), (89, "BLOCK", False), (90, "BLOCK", True), (100, "BLOCK", True)])
def test_politique_de_seuils(score, decision, alerte):
    assert decider(score) == (decision, alerte)


# ── POST /v1/score ────────────────────────────────────────────

def test_score_respecte_le_contrat_et_enregistre_la_decision(client, monde):
    ev = monde[2][-1]
    r = client.post("/v1/score", json=_payload(ev))
    assert r.status_code == 200
    body = r.json()
    for champ in ["id_transaction", "id_compte", "scored_at", "decision", "alerte_prioritaire", "score_final",
                  "seuils", "agregation", "couche1", "couche2", "couche3", "features", "latence_ms"]:
        assert champ in body
    assert body["seuils"] == {"challenge": 30, "block": 70, "alerte": 90}
    assert body["decision"] == decider(body["score_final"])[0]
    s1, s2, s3 = body["couche1"]["score"], body["couche2"]["score"], body["couche3"]["score"]
    assert body["score_final"] == round(max(s1, 0.1 * s2 + 0.9 * s3))
    assert {"statut", "version"} <= set(body["couche2"]) and body["couche3"]["statut"] == "ok"
    lignes = client.decisions.last(1)
    assert lignes and lignes[0]["id_transaction"] == ev["id_transaction"]
    assert lignes[0]["score_final"] == body["score_final"]


def test_fraude_swap_detectee(client, monde):
    fraudes = [e for e in monde[2] if e["label_fraude"] == 1
               and getattr(e["type_scenario"], "value", e["type_scenario"]) == "SIM_SWAP_SIMPLE"]
    ev = fraudes[0]
    # profil avec un swap il y a 10 min, comme le verrait le service juste après le swap
    profil = dict(monde[1][ev["id_compte"]], ts_dernier_swap=ev["horodatage"], nb_transactions=20)
    client.pipeline._feature_store.set_profile(ev["id_compte"], profil)
    body = client.post("/v1/score", json=_payload(ev)).json()
    assert body["decision"] in ("CHALLENGE", "BLOCK")
    assert any(r["rule_id"] == "R11" for r in body["couche1"]["regles"])


@pytest.mark.parametrize("modif", [{"horodatage": "hier"}, {"montant": -5}, {"id_compte": ""}])
def test_requete_invalide_422(client, monde, modif):
    assert client.post("/v1/score", json={**_payload(monde[2][0]), **modif}).status_code == 422


def test_champ_obligatoire_manquant_422(client, monde):
    p = _payload(monde[2][0]); p.pop("montant")
    assert client.post("/v1/score", json=p).status_code == 422


def test_couche_en_echec_ne_bloque_pas(client, monde, monkeypatch):
    def boom(*a, **k): raise RuntimeError("panne simulée")
    monkeypatch.setattr(client.pipeline._xgb, "predict", boom)
    r = client.post("/v1/score", json=_payload(monde[2][5]))
    assert r.status_code == 200 and r.json()["couche3"]["statut"] == "erreur"


# ── /reload-model, /health, /metrics ──────────────────────────

def test_reload_model_securise(client):
    assert client.post("/reload-model").status_code == 401
    assert client.post("/reload-model", headers={"X-API-Key": "mauvaise"}).status_code == 401
    r = client.post("/reload-model", headers={"X-API-Key": CLE})
    assert r.status_code == 200 and set(r.json()["versions"]) == {"couche1", "couche2", "couche3"}


def test_reload_model_desactive_sans_cle(monde):
    store, init, _ = monde
    app = create_app(pipeline=RealtimePipeline(store=store, feature_store=DictFeatureStore(dict(init))),
                     decisions=DecisionStore("sqlite://"), metrics=Metrics("local"), reload_api_key="")
    with TestClient(app) as c:
        assert c.post("/reload-model", headers={"X-API-Key": "x"}).status_code == 503


def test_health_et_metrics(client, monde):
    for ev in monde[2][:20]:
        client.post("/v1/score", json=_payload(ev))
    h = client.get("/health").json()
    assert h["status"] == "ok" and h["base_decisions"] == "ok"
    assert h["couches"]["couche1"]["nb_regles"] == 12
    m = client.get("/metrics").json()
    assert m["requetes"] == 20 and m["latence_ms"]["p99"] >= m["latence_ms"]["p50"] > 0
    assert sum(v["nombre"] for v in m["decisions"].values()) == 20
    assert sum(m["distribution_scores"].values()) == 20
