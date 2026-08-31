"""
tests/test_anomaly.py
──────────────────────
Tests unitaires complets pour la Couche 2 — Détection d'anomalies (IA-5).

Valide :
  1. Construction de matrice de features (dataset.py).
  2. Entraînement de l'IsolationForest et évaluation (evaluate.py).
  3. Scoring par AnomalyDetector et surcharge Z-Score (detector.py).
"""

import sys
from pathlib import Path

CYBERGUARDIAN_DIR = Path(__file__).resolve().parent.parent
if str(CYBERGUARDIAN_DIR) not in sys.path:
    sys.path.insert(0, str(CYBERGUARDIAN_DIR))

import unittest
from datetime import datetime, timezone
import numpy as np

from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import RobustScaler

from engine.anomaly.dataset import build_dataset, FEATURE_NAMES
from engine.anomaly.evaluate import evaluate
from engine.anomaly.detector import AnomalyDetector


class TestAnomalyLayer(unittest.TestCase):

    def setUp(self):
        self.now = datetime.now(timezone.utc)

        # Génération d'événements et profils factices pour tester l'IA-5
        self.events = []
        self.profiles = {}

        # 100 événements légitimes (comportement habituel sur 10 abonnés)
        for i in range(100):
            c_id = f"CPT-LEGIT-{i % 10}"
            self.profiles[c_id] = {
                "id_compte": c_id,
                "montant_moyen_habituel": 10000.0,
                "montant_moyen": 10000.0,
                "ecart_type_montant": 2000.0,
                "nb_transactions": 20,
                "devices_connus": ["DEV-HABITUEL"],
                "beneficiaires_connus": ["CPT-BENEF1"],
                "antenne_domicile": "DAK-ANT-001",
                "ts_dernier_swap": None,
                "nb_otp_1h": 0,
                "nb_tx_1h": 1,
            }
            self.events.append({
                "id_transaction": f"TXN-LEGIT-{i}",
                "id_compte": c_id,
                "horodatage": self.now.isoformat(),
                "montant": 8000.0 + (i % 5) * 500.0,
                "device_id": "DEV-HABITUEL",
                "id_beneficiaire": "CPT-BENEF1",
                "antenne": "DAK-ANT-001",
                "label_fraude": 0,
                "type_scenario": "NORMAL",
            })

        # 20 événements frauduleux répartis sur 5 abonnés
        for i in range(20):
            c_id = f"CPT-FRAUD-{i % 5}"
            self.profiles[c_id] = {
                "id_compte": c_id,
                "montant_moyen_habituel": 5000.0,
                "montant_moyen": 5000.0,
                "ecart_type_montant": 1000.0,
                "nb_transactions": 15,
                "devices_connus": ["DEV-HABITUEL"],
                "beneficiaires_connus": ["CPT-BENEF1"],
                "antenne_domicile": "DAK-ANT-001",
                "ts_dernier_swap": self.now.isoformat(),
                "nb_otp_1h": 6,
                "nb_tx_1h": 5,
            }
            self.events.append({
                "id_transaction": f"TXN-FRAUD-{i}",
                "id_compte": c_id,
                "horodatage": self.now.isoformat(),
                "montant": 80000.0,
                "device_id": "DEV-ATK",
                "id_beneficiaire": "CPT-COMPLICE",
                "antenne": "MAT-ANT-002",
                "label_fraude": 1,
                "type_scenario": "SIM_SWAP_SIMPLE",
            })

    def _create_fitted_detector(self, ds):
        scaler = RobustScaler()
        X_train_scaled = scaler.fit_transform(ds.X_train)

        model = IsolationForest(n_estimators=50, contamination=0.1, random_state=42)
        model.fit(X_train_scaled)

        class DummyStore:
            def load_json(self, key, default=None):
                return {"model_key": "dummy.pkl", "version": "test_version"}
            def load_pickle(self, key):
                return {
                    "model": model,
                    "scaler": scaler,
                    "feature_names": FEATURE_NAMES,
                }
            def save_json(self, key, data):
                pass
            def save_pickle(self, key, obj):
                pass

        detector = AnomalyDetector(store=DummyStore())
        detector._bundle = {
            "model": model,
            "scaler": scaler,
            "feature_names": FEATURE_NAMES,
        }
        detector._version = "test_version"
        detector._loaded_from = "memory"
        return detector

    def test_01_build_dataset(self):
        """Vérifie la construction du dataset de features."""
        ds = build_dataset(self.events, profiles_override=self.profiles)
        self.assertEqual(ds.n_train + ds.n_test, 120)
        self.assertEqual(ds.X_train.shape[1], len(FEATURE_NAMES))
        self.assertEqual(len(ds.y_test), ds.n_test)

    def test_02_train_and_evaluate(self):
        """Vérifie l'entraînement et l'évaluation du modèle."""
        ds = build_dataset(self.events, profiles_override=self.profiles)
        detector = self._create_fitted_detector(ds)

        # Évaluation
        report = evaluate(detector, ds, save_report=False)
        self.assertIn("auc_pr", report)
        self.assertIn("recall_at_1pct_fpr", report)

    def test_03_detector_evaluation(self):
        """Vérifie la prédiction par AnomalyDetector."""
        ds = build_dataset(self.events, profiles_override=self.profiles)
        detector = self._create_fitted_detector(ds)

        # 1. Évaluation d'une transaction normale
        res_norm = detector.predict(self.events[0], self.profiles[self.events[0]["id_compte"]])
        self.assertIsInstance(res_norm.score, int)

        # 2. Évaluation d'une transaction frauduleuse
        res_fraud = detector.predict(self.events[100], self.profiles[self.events[100]["id_compte"]])
        self.assertGreaterEqual(res_fraud.score, 70, "Transaction frauduleuse -> score élevé (>= 70)")


if __name__ == "__main__":
    unittest.main(verbosity=2)
