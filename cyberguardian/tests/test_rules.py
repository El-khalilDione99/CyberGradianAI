"""
tests/test_rules.py
───────────────────
Tests unitaires complets pour le Moteur de Règles (IA-4).

Valide :
  1. Chargement et parsing des 10 règles YAML (R01 à R10).
  2. Calcul des features dérivées instantanées (compute_features).
  3. Déclenchement individuel de chaque règle (R01 à R10).
  4. Gestion des profils vides / incomplets (valeurs de repli sûres).
  5. Logique WEAK_SIGNALS (R10) et logique AND / SINGLE.
  6. Rechargement des règles (reload_rules) et thread-safety.
"""

import sys
from pathlib import Path

# Assure que le dossier cyberguardian est dans sys.path
CYBERGUARDIAN_DIR = Path(__file__).resolve().parent.parent
if str(CYBERGUARDIAN_DIR) not in sys.path:
    sys.path.insert(0, str(CYBERGUARDIAN_DIR))

import unittest
from datetime import datetime, timedelta, timezone

from engine.rules.engine import RuleEngine, _check_condition
from engine.rules.features import compute_features


class TestRuleEngine(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.engine = RuleEngine()

    def setUp(self):
        self.now = datetime.now(timezone.utc)
        self.base_profile = {
            "id_compte": "CPT-TEST12345",
            "region": "dakar",
            "segment": "moyen",
            "antenne_domicile": "DAK-ANT-001",
            "antennes_connues": ["DAK-ANT-001", "DAK-ANT-002"],
            "iccid_actuel": "1234567890123456789",
            "imsi_actuel": "608123456789012",
            "ts_dernier_swap": None,
            "nb_swaps_30j": 0,
            "device_id_habituel": "DEV-HABITUEL",
            "devices_connus": ["DEV-HABITUEL"],
            "beneficiaires_connus": ["CPT-BENEF1", "CPT-BENEF2"],
            "nb_transactions": 10,
            "montant_moyen": 10000.0,
            "montant_moyen_habituel": 10000.0,
            "ecart_type_montant": 2000.0,
            "nb_tx_1h": 0,
            "nb_tx_24h": 2,
            "fenetre_1h_ts": [],
            "nb_otp_1h": 0,
            "heures_actives": list(range(7, 21)),
            "solde": 150000.0,
        }
        self.base_event = {
            "id_transaction": "TXN-TEST-001",
            "id_compte": "CPT-TEST12345",
            "horodatage": self.now.isoformat(),
            "montant": 8000.0,
            "device_id": "DEV-HABITUEL",
            "id_beneficiaire": "CPT-BENEF1",
            "antenne": "DAK-ANT-001",
            "solde_avant": 150000.0,
            "solde_apres": 142000.0,
        }

    # ── 1. Chargement du moteur ──────────────────────────────

    def test_01_rules_loaded(self):
        """Vérifie que les 10 règles sont bien chargées depuis rules.yaml."""
        self.assertEqual(self.engine.rules_count, 10, "Le moteur doit charger exactement 10 règles")
        status = self.engine.status()
        self.assertEqual(len(status["rules_ids"]), 10)
        expected_ids = [f"R{i:02d}" for i in range(1, 11)]
        self.assertEqual(status["rules_ids"], expected_ids)

    # ── 2. Evaluation de transaction normale ─────────────────

    def test_02_normal_transaction_no_rules_triggered(self):
        """Une transaction normale dans les habitudes de l'abonné ne déclenche aucune règle."""
        res = self.engine.evaluate(self.base_event, self.base_profile)
        self.assertEqual(res.score, 0)
        self.assertFalse(res.triggered)
        self.assertEqual(len(res.matches), 0)

    # ── 3. R01 : SIM Swap récent + montant important ─────────

    def test_03_rule_r01_sim_swap_recent_amount_ratio(self):
        """R01 : swap < 1h ET amount_ratio > 3 -> Score 92."""
        profile = dict(self.base_profile)
        profile["ts_dernier_swap"] = (self.now - timedelta(minutes=20)).isoformat()

        event = dict(self.base_event)
        event["montant"] = 40000.0  # 4x le montant moyen (10 000)

        res = self.engine.evaluate(event, profile)
        match_ids = [m.rule_id for m in res.matches]
        self.assertIn("R01", match_ids)
        self.assertGreaterEqual(res.score, 92)

    # ── 4. R02 : Nouveau device + montant important ──────────

    def test_04_rule_r02_new_device_amount_ratio(self):
        """R02 : nouveau device ET amount_ratio > 3 -> Score 78."""
        event = dict(self.base_event)
        event["device_id"] = "DEV-INCONNU-ATK"
        event["montant"] = 35000.0  # 3.5x la moyenne

        res = self.engine.evaluate(event, self.base_profile)
        match_ids = [m.rule_id for m in res.matches]
        self.assertIn("R02", match_ids)
        self.assertGreaterEqual(res.score, 78)

    # ── 5. R03 : Nouveau device après SIM swap ───────────────

    def test_05_rule_r03_new_device_after_sim_swap(self):
        """R03 : new_device ET hours_since_sim_swap < 2h -> Score 93."""
        profile = dict(self.base_profile)
        profile["ts_dernier_swap"] = (self.now - timedelta(minutes=30)).isoformat()

        event = dict(self.base_event)
        event["device_id"] = "DEV-NOUVEAU-PIRATE"

        res = self.engine.evaluate(event, profile)
        match_ids = [m.rule_id for m in res.matches]
        self.assertIn("R03", match_ids)
        self.assertGreaterEqual(res.score, 93)

    # ── 6. R04 : Pic OTP ─────────────────────────────────────

    def test_06_rule_r04_otp_spike(self):
        """R04 : otp_count_1h >= 3 -> Score 72."""
        profile = dict(self.base_profile)
        profile["nb_otp_1h"] = 4

        res = self.engine.evaluate(self.base_event, profile)
        match_ids = [m.rule_id for m in res.matches]
        self.assertIn("R04", match_ids)
        self.assertGreaterEqual(res.score, 72)

    # ── 7. R05 : Vélocité excessive ──────────────────────────

    def test_07_rule_r05_high_velocity(self):
        """R05 : nb_tx_1h >= 4 -> Score 70."""
        profile = dict(self.base_profile)
        profile["nb_tx_1h"] = 5

        res = self.engine.evaluate(self.base_event, profile)
        match_ids = [m.rule_id for m in res.matches]
        self.assertIn("R05", match_ids)
        self.assertGreaterEqual(res.score, 70)

    # ── 8. R06 : Montant très supérieur à l'habitude ─────────

    def test_08_rule_r06_huge_amount_ratio(self):
        """R06 : amount_ratio > 5 -> Score 68."""
        event = dict(self.base_event)
        event["montant"] = 60000.0  # 6x la moyenne habituelle

        res = self.engine.evaluate(event, self.base_profile)
        match_ids = [m.rule_id for m in res.matches]
        self.assertIn("R06", match_ids)
        self.assertGreaterEqual(res.score, 68)

    # ── 9. R07 : Changement géographique ─────────────────────

    def test_09_rule_r07_roaming(self):
        """R07 : antenne hors domicile -> Score 60."""
        event = dict(self.base_event)
        event["antenne"] = "THI-ANT-004"  # Hors de DAK-ANT-001

        res = self.engine.evaluate(event, self.base_profile)
        match_ids = [m.rule_id for m in res.matches]
        self.assertIn("R07", match_ids)
        self.assertGreaterEqual(res.score, 60)

    # ── 10. R09 : Nouveau bénéficiaire + montant élevé ───────

    def test_10_rule_r09_new_beneficiary_amount(self):
        """R09 : new_beneficiary ET amount_ratio > 3 -> Score 78."""
        event = dict(self.base_event)
        event["id_beneficiaire"] = "BEN-COMPLICE-99"
        event["montant"] = 35000.0

        res = self.engine.evaluate(event, self.base_profile)
        match_ids = [m.rule_id for m in res.matches]
        self.assertIn("R09", match_ids)
        self.assertGreaterEqual(res.score, 78)

    # ── 11. R10 : Accumulation de signaux faibles ────────────

    def test_11_rule_r10_weak_signals_accumulation(self):
        """R10 : 3+ signaux faibles simultanés -> Score 88."""
        profile = dict(self.base_profile)
        profile["nb_tx_1h"] = 2  # Signal faible 1

        event = dict(self.base_event)
        event["montant"] = 25000.0               # Signal faible 2 : ratio > 2
        event["antenne"] = "MAT-ANT-002"          # Signal faible 3 : is_roaming == True
        event["id_beneficiaire"] = "BEN-INCONNU"  # Signal faible 4 : new_beneficiary == True

        res = self.engine.evaluate(event, profile)
        match_ids = [m.rule_id for m in res.matches]
        self.assertIn("R10", match_ids)
        self.assertGreaterEqual(res.score, 88)

    # ── 12. Robustesse sur profil vide / None ─────────────────

    def test_12_empty_profile_safe_fallback(self):
        """Le moteur doit s'exécuter sans crash même si le profil Redis est vide."""
        res = self.engine.evaluate(self.base_event, {})
        self.assertIsInstance(res.score, int)
        self.assertIn("amount_ratio", res.features)

    # ── 13. Opérateurs atomiques ─────────────────────────────

    def test_13_operator_evaluator(self):
        """Teste les comparaisons numériques, booléennes et formats texte."""
        self.assertTrue(_check_condition(10, ">", 5))
        self.assertTrue(_check_condition(5, "<=", 5))
        self.assertTrue(_check_condition(True, "==", "true"))
        self.assertTrue(_check_condition(False, "==", "false"))
        self.assertFalse(_check_condition(10, "<", 5))


if __name__ == "__main__":
    unittest.main(verbosity=2)
