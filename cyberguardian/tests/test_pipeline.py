"""
tests/test_pipeline.py
───────────────────────
Test du pipeline des 3 couches ensemble sur le même événement.

Vérifie :
  - Les 3 couches chargent correctement
  - SIM_SWAP_SIMPLE → score max ≥ 70 (BLOCK) sur au moins une couche
  - Transaction NORMAL → Couche 3 score < 30
  - Le score final (max des 3) est cohérent avec la décision
  - SHAP top-3 de la Couche 3 pointe bien vers "fraud" sur une fraude

Pas besoin de Docker ni de Redis — tout tourne en mémoire.

Lancer :
    python -m pytest tests/test_pipeline.py -v
"""

import sys
import random
from datetime import datetime, timedelta, timezone

import pytest

sys.path.insert(0, ".")

from simulator.subscribers import generate_subscribers
from simulator.scenarios import (
    build_transaction_normale, build_sim_swap_simple,
    build_sim_swap_cascade, build_pic_otp,
    build_gros_montant_legitime, build_voyage_legitime,
)
from interfaces.store import ObjectStore
from engine.rules.engine        import RuleEngine
from engine.anomaly.dataset     import build_dataset as build_if_ds
from engine.anomaly.train       import train as train_if
from engine.anomaly.detector    import AnomalyDetector
from engine.supervised.dataset  import build_dataset as build_xgb_ds
from engine.supervised.train    import train as train_xgb
from engine.supervised.detector import XGBoostDetector


# ════════════════════════════════════════════════════════════
#  Store en mémoire
# ════════════════════════════════════════════════════════════

class InMemoryStore(ObjectStore):
    def __init__(self):
        self._data = {}

    def upload(self, bucket, key, data):
        self._data[f"{bucket}/{key}"] = data

    def download(self, bucket, key):
        k = f"{bucket}/{key}"
        if k not in self._data:
            raise KeyError(k)
        return self._data[k]

    def exists(self, bucket, key):
        return f"{bucket}/{key}" in self._data


# ════════════════════════════════════════════════════════════
#  Fixtures
# ════════════════════════════════════════════════════════════

@pytest.fixture(scope="module")
def pipeline():
    """
    Construit le pipeline complet une fois pour tout le module :
      - Génère 100 comptes
      - Entraîne IA-5 (Isolation Forest)
      - Entraîne IA-6 (XGBoost)
      - Charge les 3 couches
    Retourne (couche1, couche2, couche3)
    """
    comptes = generate_subscribers(100, seed=42)
    rng     = random.Random(42)
    t0      = datetime(2026, 7, 1, 10, 0, tzinfo=timezone.utc)
    events, profiles = [], {}

    for i, c in enumerate(comptes):
        ts = t0 + timedelta(hours=i * 2)
        profiles[c.id_compte] = {
            "montant_moyen_habituel": c.montant_moyen_habituel,
            "montant_moyen":          c.montant_moyen_habituel,
            "ecart_type_montant":     c.ecart_type_montant,
            "devices_connus":         [c.device_id_habituel],
            "beneficiaires_connus":   list(c.beneficiaires_habituels),
            "antennes_connues":       [c.antenne_domicile],
            "antenne_domicile":       c.antenne_domicile,
            "ts_dernier_swap":        None,
            "nb_otp_1h": 0, "nb_tx_1h": 1, "nb_tx_24h": 3, "nb_tx_7j": 15,
            "nb_otp_24h": 0, "nb_swaps_30j": 0, "solde": c.solde,
            "heures_actives": c.heures_actives,
            "fenetre_1h_ts": [], "nb_transactions": 20,
        }
        for _ in range(8):
            sc = build_transaction_normale(c, rng, ts)
            events += [e.payload for e in sc.evenements if e.stream == "transactions"]
            ts += timedelta(hours=3)
        builders = [build_sim_swap_simple, build_sim_swap_cascade, build_pic_otp,
                    build_gros_montant_legitime, build_voyage_legitime,
                    build_transaction_normale]
        sc = builders[i % 6](c, rng, ts)
        events += [e.payload for e in sc.evenements if e.stream == "transactions"]

    store = InMemoryStore()
    train_if(build_if_ds(events=events, profiles_override=profiles),
             store=store, promote=True)
    train_xgb(build_xgb_ds(events=events, profiles_override=profiles),
              store=store, promote=True)

    couche1 = RuleEngine()
    couche2 = AnomalyDetector(store=store)
    couche3 = XGBoostDetector(store=store)

    return couche1, couche2, couche3


@pytest.fixture(scope="module")
def fraude_event():
    """Événement SIM_SWAP_SIMPLE + profil associé."""
    c   = generate_subscribers(1, seed=77)[0]
    ts  = datetime(2026, 7, 14, 9, 0, tzinfo=timezone.utc)
    sc  = build_sim_swap_simple(c, random.Random(77), ts)
    ev  = next(e.payload for e in sc.evenements if e.stream == "transactions")
    pf  = {
        "montant_moyen_habituel": c.montant_moyen_habituel,
        "montant_moyen":          c.montant_moyen_habituel,
        "ecart_type_montant":     c.ecart_type_montant,
        "devices_connus":         [c.device_id_habituel],
        "beneficiaires_connus":   [],
        "antennes_connues":       [c.antenne_domicile],
        "antenne_domicile":       c.antenne_domicile,
        "ts_dernier_swap":        ts.isoformat(),
        "nb_otp_1h": 6, "nb_tx_1h": 3, "nb_tx_24h": 5, "nb_tx_7j": 20,
        "nb_otp_24h": 6, "nb_swaps_30j": 1, "solde": c.solde,
        "heures_actives": c.heures_actives, "fenetre_1h_ts": [], "nb_transactions": 20,
    }
    return ev, pf


@pytest.fixture(scope="module")
def normal_event():
    """Événement NORMAL + profil associé."""
    c   = generate_subscribers(1, seed=88)[0]
    ts  = datetime(2026, 7, 14, 10, 0, tzinfo=timezone.utc)
    sc  = build_transaction_normale(c, random.Random(88), ts)
    ev  = next(e.payload for e in sc.evenements if e.stream == "transactions")
    pn  = {
        "montant_moyen_habituel": c.montant_moyen_habituel,
        "montant_moyen":          c.montant_moyen_habituel,
        "ecart_type_montant":     c.ecart_type_montant,
        "devices_connus":         [c.device_id_habituel],
        "beneficiaires_connus":   list(c.beneficiaires_habituels),
        "antennes_connues":       [c.antenne_domicile],
        "antenne_domicile":       c.antenne_domicile,
        "ts_dernier_swap":        None,
        "nb_otp_1h": 0, "nb_tx_1h": 1, "nb_tx_24h": 2, "nb_tx_7j": 12,
        "nb_otp_24h": 0, "nb_swaps_30j": 0, "solde": c.solde,
        "heures_actives": c.heures_actives, "fenetre_1h_ts": [], "nb_transactions": 25,
    }
    return ev, pn


# ════════════════════════════════════════════════════════════
#  Tests chargement
# ════════════════════════════════════════════════════════════

class TestChargement:

    def test_couche1_chargee(self, pipeline):
        c1, _, _ = pipeline
        assert c1.rules_count == 10, f"Attendu 10 règles, obtenu {c1.rules_count}"

    def test_couche2_prete(self, pipeline):
        _, c2, _ = pipeline
        assert c2.is_ready, "AnomalyDetector non prêt"

    def test_couche3_prete(self, pipeline):
        _, _, c3 = pipeline
        assert c3.is_ready, "XGBoostDetector non prêt"


# ════════════════════════════════════════════════════════════
#  Tests scénario fraude
# ════════════════════════════════════════════════════════════

class TestFraude:

    def test_couche1_declenche_regles(self, pipeline, fraude_event):
        c1, _, _ = pipeline
        ev, pf   = fraude_event
        r1 = c1.evaluate(ev, pf)
        assert len(r1.matches) > 0, "Couche 1 : aucune règle déclenchée sur une fraude"

    def test_couche1_score_eleve(self, pipeline, fraude_event):
        c1, _, _ = pipeline
        ev, pf   = fraude_event
        r1 = c1.evaluate(ev, pf)
        assert r1.score >= 70, f"Couche 1 score trop bas sur fraude : {r1.score}"

    def test_couche3_score_eleve(self, pipeline, fraude_event):
        _, _, c3 = pipeline
        ev, pf   = fraude_event
        r3 = c3.predict(ev, pf)
        assert r3.score >= 70, f"Couche 3 score trop bas sur fraude : {r3.score}"

    def test_score_final_bloque(self, pipeline, fraude_event):
        c1, c2, c3 = pipeline
        ev, pf     = fraude_event
        score_final = max(c1.evaluate(ev, pf).score,
                          c2.predict(ev, pf).score,
                          c3.predict(ev, pf).score)
        assert score_final >= 70, f"Score final fraude trop bas pour BLOCK : {score_final}"

    def test_shap_direction_fraude(self, pipeline, fraude_event):
        _, _, c3 = pipeline
        ev, pf   = fraude_event
        r3 = c3.predict(ev, pf)
        assert len(r3.shap_top3) > 0
        # La feature la plus influente doit pousser vers "fraud"
        top = r3.shap_top3[0]
        assert top["direction"] == "fraud", (
            f"Top feature '{top['feature']}' pointe vers '{top['direction']}' "
            f"au lieu de 'fraud'"
        )


# ════════════════════════════════════════════════════════════
#  Tests scénario normal
# ════════════════════════════════════════════════════════════

class TestNormal:

    def test_couche1_aucune_regle(self, pipeline, normal_event):
        c1, _, _ = pipeline
        ev, pn   = normal_event
        r1 = c1.evaluate(ev, pn)
        assert r1.score == 0, f"Couche 1 ne doit pas déclencher sur NORMAL : score={r1.score}"

    def test_couche3_score_bas(self, pipeline, normal_event):
        _, _, c3 = pipeline
        ev, pn   = normal_event
        r3 = c3.predict(ev, pn)
        assert r3.score < 30, f"Couche 3 score trop élevé sur NORMAL : {r3.score}"

    def test_couche3_passe(self, pipeline, normal_event):
        _, _, c3 = pipeline
        ev, pn   = normal_event
        r3 = c3.predict(ev, pn)
        decision = "BLOCK" if r3.score >= 70 else ("CHALLENGE" if r3.score >= 30 else "PASS")
        assert decision == "PASS", f"Couche 3 : décision '{decision}' au lieu de 'PASS'"


# ════════════════════════════════════════════════════════════
#  Tests discrimination fraude vs normal
# ════════════════════════════════════════════════════════════

class TestDiscrimination:

    def test_couche3_fraude_superieur_a_normal(self, pipeline, fraude_event, normal_event):
        _, _, c3   = pipeline
        ev_f, pf   = fraude_event
        ev_n, pn   = normal_event
        score_f = c3.predict(ev_f, pf).score
        score_n = c3.predict(ev_n, pn).score
        assert score_f > score_n, (
            f"Couche 3 : score fraude ({score_f}) doit être > score normal ({score_n})"
        )

    def test_couche1_fraude_superieur_a_normal(self, pipeline, fraude_event, normal_event):
        c1, _, _   = pipeline
        ev_f, pf   = fraude_event
        ev_n, pn   = normal_event
        score_f = c1.evaluate(ev_f, pf).score
        score_n = c1.evaluate(ev_n, pn).score
        assert score_f > score_n, (
            f"Couche 1 : score fraude ({score_f}) doit être > score normal ({score_n})"
        )
