"""
tests/test_ia6.py
──────────────────
Test de la Couche 3 (IA-6) — XGBoost supervisé en isolation.

Vérifie :
  - build_dataset() produit un dataset cohérent (fraudes + légitimes, scale_pos_weight)
  - train() entraîne le modèle, le sauvegarde, promeut le champion
  - predict() retourne un score plus élevé pour une fraude que pour un légitime
  - evaluate() produit AUC-PR, rappel@1%FPR, SHAP globale

Pas besoin de Docker ni de Redis — tout tourne en mémoire.

Lancer :
    python -m pytest tests/test_ia6.py -v
"""

import sys
import random
import pickle
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
from engine.supervised.dataset  import build_dataset, XGB_FEATURE_NAMES, INF_HOURS_SUBSTITUTE
from engine.supervised.train    import train
from engine.supervised.detector import XGBoostDetector
from engine.supervised.evaluate import evaluate


# ════════════════════════════════════════════════════════════
#  Store en mémoire (remplace MinIO)
# ════════════════════════════════════════════════════════════

class InMemoryStore(ObjectStore):
    def __init__(self):
        self._data = {}

    def upload(self, bucket, key, data):
        self._data[f"{bucket}/{key}"] = data

    def download(self, bucket, key):
        k = f"{bucket}/{key}"
        if k not in self._data:
            raise KeyError(f"Clé absente du store en mémoire : {k}")
        return self._data[k]

    def exists(self, bucket, key):
        return f"{bucket}/{key}" in self._data


# ════════════════════════════════════════════════════════════
#  Fixture : données simulées (100 comptes, seed=42)
# ════════════════════════════════════════════════════════════

@pytest.fixture(scope="module")
def simulated_data():
    """Génère 100 comptes avec scénarios variés, retourne (events, profiles)."""
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

    return events, profiles


@pytest.fixture(scope="module")
def trained_bundle(simulated_data):
    """Entraîne le modèle une fois pour tout le module."""
    events, profiles = simulated_data
    store   = InMemoryStore()
    dataset = build_dataset(events=events, profiles_override=profiles)
    result  = train(dataset, store=store, promote=True)
    detector = XGBoostDetector(store=store)
    return dataset, result, detector, store


# ════════════════════════════════════════════════════════════
#  Tests dataset
# ════════════════════════════════════════════════════════════

class TestDataset:

    def test_features_count(self, simulated_data):
        events, profiles = simulated_data
        ds = build_dataset(events=events, profiles_override=profiles)
        assert len(ds.feature_names) == 17, f"Attendu 17 features, obtenu {len(ds.feature_names)}"

    def test_hours_since_swap_no_inf(self, simulated_data):
        import numpy as np
        events, profiles = simulated_data
        ds = build_dataset(events=events, profiles_override=profiles)
        col = ds.feature_names.index("hours_since_sim_swap")
        assert not np.any(np.isinf(ds.X_train[:, col])), "inf trouvé dans X_train"
        assert not np.any(np.isnan(ds.X_train[:, col])), "NaN trouvé dans X_train"

    def test_fraudes_dans_train_et_test(self, simulated_data):
        events, profiles = simulated_data
        ds = build_dataset(events=events, profiles_override=profiles)
        assert ds.y_train.sum() > 0, "Aucune fraude dans y_train"
        assert ds.y_test.sum() > 0,  "Aucune fraude dans y_test"

    def test_scale_pos_weight_superieur_a_1(self, simulated_data):
        events, profiles = simulated_data
        ds = build_dataset(events=events, profiles_override=profiles)
        assert ds.scale_pos_weight > 1, "scale_pos_weight doit être > 1"

    def test_pas_de_nan(self, simulated_data):
        import numpy as np
        events, profiles = simulated_data
        ds = build_dataset(events=events, profiles_override=profiles)
        assert not np.any(np.isnan(ds.X_train)), "NaN dans X_train"
        assert not np.any(np.isnan(ds.X_test)),  "NaN dans X_test"


# ════════════════════════════════════════════════════════════
#  Tests entraînement
# ════════════════════════════════════════════════════════════

class TestTrain:

    def test_model_key_non_vide(self, trained_bundle):
        _, result, _, _ = trained_bundle
        assert result.model_key, "model_key vide"

    def test_champion_promu(self, trained_bundle):
        _, result, _, _ = trained_bundle
        assert result.is_champion, "Le premier modèle doit être promu champion"

    def test_auc_pr_dans_intervalle(self, trained_bundle):
        _, result, _, _ = trained_bundle
        auc = result.metrics["auc_pr_test"]
        assert 0.0 <= auc <= 1.0, f"AUC-PR hors [0,1] : {auc}"

    def test_current_json_cree(self, trained_bundle):
        _, _, _, store = trained_bundle
        assert store.exists("cg-models", "xgboost/production/current.json")

    def test_history_json_cree(self, trained_bundle):
        _, _, _, store = trained_bundle
        assert store.exists("cg-models", "xgboost/production/history.json")

    def test_feature_importance_presente(self, trained_bundle):
        _, result, _, _ = trained_bundle
        assert result.metrics.get("feature_importance"), "feature_importance absente"

    def test_cv_auc_pr_present(self, trained_bundle):
        _, result, _, _ = trained_bundle
        assert "cv_auc_pr_mean" in result.metrics

    def test_scale_pos_weight_dans_metrics(self, trained_bundle):
        _, result, _, _ = trained_bundle
        assert "scale_pos_weight" in result.metrics


# ════════════════════════════════════════════════════════════
#  Tests prédiction
# ════════════════════════════════════════════════════════════

class TestPredict:

    def _profil_fraude(self):
        c = generate_subscribers(1, seed=77)[0]
        ts_swap = datetime(2026, 7, 14, 9, 0, tzinfo=timezone.utc)
        sc = build_sim_swap_simple(c, random.Random(77), ts_swap)
        ev = next(e.payload for e in sc.evenements if e.stream == "transactions")
        pf = {
            "montant_moyen_habituel": c.montant_moyen_habituel,
            "montant_moyen":          c.montant_moyen_habituel,
            "ecart_type_montant":     c.ecart_type_montant,
            "devices_connus":         [c.device_id_habituel],
            "beneficiaires_connus":   [],
            "antennes_connues":       [c.antenne_domicile],
            "antenne_domicile":       c.antenne_domicile,
            "ts_dernier_swap":        ts_swap.isoformat(),
            "nb_otp_1h": 6, "nb_tx_1h": 3, "nb_tx_24h": 5, "nb_tx_7j": 20,
            "nb_otp_24h": 6, "nb_swaps_30j": 1, "solde": c.solde,
            "heures_actives": c.heures_actives, "fenetre_1h_ts": [], "nb_transactions": 20,
        }
        return ev, pf

    def _profil_normal(self):
        c  = generate_subscribers(1, seed=88)[0]
        ts = datetime(2026, 7, 14, 10, 0, tzinfo=timezone.utc)
        sc = build_transaction_normale(c, random.Random(88), ts)
        ev = next(e.payload for e in sc.evenements if e.stream == "transactions")
        pn = {
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

    def test_is_ready(self, trained_bundle):
        _, _, detector, _ = trained_bundle
        assert detector.is_ready

    def test_score_fraude_superieur_a_normal(self, trained_bundle):
        _, _, detector, _ = trained_bundle
        ev_f, pf = self._profil_fraude()
        ev_n, pn = self._profil_normal()
        res_f = detector.predict(ev_f, pf)
        res_n = detector.predict(ev_n, pn)
        assert res_f.score > res_n.score, (
            f"Score fraude ({res_f.score}) doit être > score normal ({res_n.score})"
        )

    def test_score_fraude_bloque(self, trained_bundle):
        _, _, detector, _ = trained_bundle
        ev_f, pf = self._profil_fraude()
        res = detector.predict(ev_f, pf)
        assert res.score >= 70, f"Score fraude trop bas pour BLOCK : {res.score}"

    def test_score_normal_passe(self, trained_bundle):
        _, _, detector, _ = trained_bundle
        ev_n, pn = self._profil_normal()
        res = detector.predict(ev_n, pn)
        assert res.score < 30, f"Score normal trop élevé pour PASS : {res.score}"

    def test_probability_dans_intervalle(self, trained_bundle):
        _, _, detector, _ = trained_bundle
        ev_f, pf = self._profil_fraude()
        res = detector.predict(ev_f, pf)
        assert 0.0 <= res.probability <= 1.0

    def test_shap_top3_non_vide(self, trained_bundle):
        _, _, detector, _ = trained_bundle
        ev_f, pf = self._profil_fraude()
        res = detector.predict(ev_f, pf)
        assert len(res.shap_top3) > 0, "shap_top3 vide"

    def test_shap_contient_bons_champs(self, trained_bundle):
        _, _, detector, _ = trained_bundle
        ev_f, pf = self._profil_fraude()
        res = detector.predict(ev_f, pf)
        for s in res.shap_top3:
            assert "feature"   in s
            assert "value"     in s
            assert "shap"      in s
            assert "direction" in s
            assert s["direction"] in ("fraud", "legitimate")

    def test_mode_degrade_score_zero(self):
        det = XGBoostDetector()   # pas de store → mode dégradé
        c   = generate_subscribers(1, seed=1)[0]
        ts  = datetime(2026, 7, 1, 10, 0, tzinfo=timezone.utc)
        sc  = build_transaction_normale(c, random.Random(1), ts)
        ev  = next(e.payload for e in sc.evenements if e.stream == "transactions")
        res = det.predict(ev, {})
        assert res.score == 0, "Mode dégradé doit retourner score=0"


# ════════════════════════════════════════════════════════════
#  Tests évaluation
# ════════════════════════════════════════════════════════════

class TestEvaluate:

    def test_auc_pr_dans_intervalle(self, trained_bundle):
        dataset, _, detector, store = trained_bundle
        report = evaluate(detector, dataset, store=store, save_report=False)
        assert 0.0 <= report["auc_pr"] <= 1.0

    def test_score_fraudes_superieur_a_legitimess(self, trained_bundle):
        dataset, _, detector, store = trained_bundle
        report = evaluate(detector, dataset, store=store, save_report=False)
        assert report["score_mean_fraud"] > report["score_mean_legit"], (
            f"Score fraudes ({report['score_mean_fraud']:.1f}) doit être > "
            f"légitimes ({report['score_mean_legit']:.1f})"
        )

    def test_confusion_4_seuils(self, trained_bundle):
        dataset, _, detector, store = trained_bundle
        report = evaluate(detector, dataset, store=store, save_report=False)
        assert len(report["confusion_by_threshold"]) == 4

    def test_shap_importance_presente(self, trained_bundle):
        dataset, _, detector, store = trained_bundle
        report = evaluate(detector, dataset, store=store, save_report=False)
        assert "shap_importance" in report

    def test_rapport_publie_dans_store(self, trained_bundle):
        dataset, _, detector, store = trained_bundle
        evaluate(detector, dataset, store=store, save_report=True)
        assert any("xgboost/eval" in k for k in store._data)
