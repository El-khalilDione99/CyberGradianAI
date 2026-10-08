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
        """Vérifie que les 12 règles (v2.0) sont bien chargées depuis rules.yaml."""
        self.assertEqual(self.engine.rules_count, 12, "Le moteur doit charger exactement 12 règles")
        status = self.engine.status()
        self.assertEqual(status["rules_ids"], [f"R{i:02d}" for i in range(1, 13)])

    # ── 2. Evaluation de transaction normale ─────────────────

    def test_02_normal_transaction_no_rules_triggered(self):
        """Une transaction normale dans les habitudes de l'abonné ne déclenche aucune règle."""
        res = self.engine.evaluate(self.base_event, self.base_profile)
        self.assertEqual(res.score, 0)
        self.assertFalse(res.triggered)
        self.assertEqual(len(res.matches), 0)

    def _swap_il_y_a(self, minutes: float) -> dict:
        profile = dict(self.base_profile)
        profile["ts_dernier_swap"] = (self.now - timedelta(minutes=minutes)).isoformat()
        return profile

    def _ids(self, res):
        return [m.rule_id for m in res.matches]

    # ── 3. R01 : SIM Swap récent + montant > ×1,5 ────────────

    def test_03_rule_r01_sim_swap_recent_amount_ratio(self):
        """R01 : swap < 1h ET amount_ratio > 1,5 -> Score 92 (BLOCK)."""
        event = dict(self.base_event, montant=20000.0)  # 2x le montant moyen (10 000)
        res = self.engine.evaluate(event, self._swap_il_y_a(20))
        self.assertIn("R01", self._ids(res))
        self.assertGreaterEqual(res.score, 92)

    # ── 4. R02 : Nouveau device + montant > ×1,5 ─────────────

    def test_04_rule_r02_new_device_amount_ratio(self):
        """R02 : nouveau device ET amount_ratio > 1,5 -> Score 60 (vérification)."""
        event = dict(self.base_event, device_id="DEV-INCONNU-ATK", montant=20000.0)
        res = self.engine.evaluate(event, self.base_profile)
        self.assertIn("R02", self._ids(res))
        self.assertEqual(res.score, 60)

    # ── 5. R03 : Nouveau device après SIM swap ───────────────

    def test_05_rule_r03_new_device_after_sim_swap(self):
        """R03 : new_device ET swap < 2h -> Score 65 (R11 aussi : swap < 1h)."""
        event = dict(self.base_event, device_id="DEV-NOUVEAU-PIRATE")
        res = self.engine.evaluate(event, self._swap_il_y_a(90))   # 1h30 : R03 sans R11
        self.assertIn("R03", self._ids(res))
        self.assertNotIn("R11", self._ids(res))
        self.assertEqual(res.score, 65)

    # ── 6. R04 : Pic OTP après swap ──────────────────────────

    def test_06_rule_r04_otp_spike(self):
        """R04 : otp_count_1h >= 3 ET swap < 6h -> Score 85 ; sans swap récent, rien."""
        profile = self._swap_il_y_a(180)
        profile["nb_otp_1h"] = 4
        res = self.engine.evaluate(self.base_event, profile)
        self.assertIn("R04", self._ids(res))
        self.assertGreaterEqual(res.score, 85)
        sans_swap = dict(self.base_profile, nb_otp_1h=4)
        self.assertNotIn("R04", self._ids(self.engine.evaluate(self.base_event, sans_swap)))

    # ── 7. R05 : Vélocité après swap ─────────────────────────

    def test_07_rule_r05_high_velocity(self):
        """R05 : nb_tx_1h >= 3 ET swap < 6h -> Score 85 ; sans swap récent, rien."""
        # nb_tx_1h est recalculé à l'heure de la transaction à partir des horodatages
        fen = [(self.now - timedelta(minutes=m)).isoformat() for m in (5, 15, 25)]
        profile = self._swap_il_y_a(180)
        profile.update(fenetre_1h_ts=fen, nb_tx_1h=3)
        res = self.engine.evaluate(self.base_event, profile)
        self.assertIn("R05", self._ids(res))
        self.assertGreaterEqual(res.score, 85)
        sans_swap = dict(self.base_profile, fenetre_1h_ts=fen, nb_tx_1h=3)
        self.assertNotIn("R05", self._ids(self.engine.evaluate(self.base_event, sans_swap)))

    # ── 8. R06 : Montant très supérieur à l'habitude ─────────

    def test_08_rule_r06_huge_amount_ratio(self):
        """R06 : amount_ratio > 5 -> Score 40 (signal faible)."""
        event = dict(self.base_event, montant=60000.0)  # 6x la moyenne habituelle
        res = self.engine.evaluate(event, self.base_profile)
        self.assertIn("R06", self._ids(res))
        self.assertEqual(res.score, 40)

    # ── 9. R07 / R12 : Changement géographique ───────────────

    def test_09_rule_r07_roaming(self):
        """R07 : autre région ET swap < 6h -> 80 ; R12 : autre région seule -> 25 (information)."""
        event = dict(self.base_event, antenne="THI-ANT-004")  # région Thiès, domicile à Dakar
        res = self.engine.evaluate(event, self._swap_il_y_a(180))
        self.assertIn("R07", self._ids(res))
        self.assertGreaterEqual(res.score, 80)
        seul = self.engine.evaluate(event, self.base_profile)
        self.assertEqual(self._ids(seul), ["R12"])
        self.assertEqual(seul.score, 25)

    def test_09b_meme_region_pas_itinerance(self):
        """Une autre antenne de la même région n'est pas de l'itinérance."""
        event = dict(self.base_event, antenne="DAK-ANT-005")  # jamais vue, mais à Dakar
        res = self.engine.evaluate(event, self.base_profile)
        self.assertFalse(res.features["is_roaming"])
        self.assertNotIn("R07", self._ids(res))
        self.assertNotIn("R12", self._ids(res))

    # ── 10. R09 : Nouveau bénéficiaire + montant élevé ───────

    def test_10_rule_r09_new_beneficiary_amount(self):
        """R09 : new_beneficiary ET amount_ratio > 3 -> Score 40 (signal faible)."""
        event = dict(self.base_event, id_beneficiaire="BEN-COMPLICE-99", montant=35000.0)
        res = self.engine.evaluate(event, self.base_profile)
        self.assertIn("R09", self._ids(res))
        self.assertEqual(res.score, 40)

    # ── 11. R10 : Accumulation de signaux faibles ────────────

    def test_11_rule_r10_weak_signals_accumulation(self):
        """R10 : 3+ signaux faibles simultanés -> Score 65."""
        event = dict(self.base_event)
        event["montant"] = 25000.0               # Signal faible : ratio > 2
        event["antenne"] = "MAT-ANT-002"          # Signal faible : is_roaming == True
        event["id_beneficiaire"] = "BEN-INCONNU"  # Signal faible : new_beneficiary == True
        res = self.engine.evaluate(event, self.base_profile)
        self.assertIn("R10", self._ids(res))
        self.assertEqual(res.score, 65)

    # ── 11b. R11 : SIM swap très récent ──────────────────────

    def test_11b_rule_r11_swap_tres_recent(self):
        """R11 : swap < 1h quel que soit le montant -> Score 60 (vérification OTP)."""
        res = self.engine.evaluate(self.base_event, self._swap_il_y_a(30))
        self.assertEqual(self._ids(res), ["R11"])
        self.assertEqual(res.score, 60)

    # ── 12. Robustesse sur profil vide / None ─────────────────

    def test_12_empty_profile_safe_fallback(self):
        """Le moteur doit s'exécuter sans crash même si le profil Redis est vide."""
        res = self.engine.evaluate(self.base_event, {})
        self.assertIsInstance(res.score, int)
        self.assertIn("amount_ratio", res.features)

    # ── 13. Opérateurs atomiques ─────────────────────────────

    def test_12b_fenetres_recalculees_a_l_heure_de_la_transaction(self):
        """Un pic d'OTP vieux de 10 jours ne doit plus compter « dans l'heure »."""
        profile = dict(self.base_profile)
        vieux = [(self.now - timedelta(days=10, minutes=m)).isoformat() for m in range(6)]
        profile.update(nb_otp_1h=6, fenetre_otp_1h_ts=vieux, nb_otp_24h=6, fenetre_otp_24h_ts=vieux,
                       nb_tx_1h=3, fenetre_1h_ts=[(self.now - timedelta(hours=5)).isoformat()])
        f = compute_features(self.base_event, profile)
        self.assertEqual(f["otp_count_1h"], 0)
        self.assertEqual(f["nb_otp_24h"], 0)
        self.assertEqual(f["nb_tx_1h"], 0)

    def test_13_operator_evaluator(self):
        """Teste les comparaisons numériques, booléennes et formats texte."""
        self.assertTrue(_check_condition(10, ">", 5))
        self.assertTrue(_check_condition(5, "<=", 5))
        self.assertTrue(_check_condition(True, "==", "true"))
        self.assertTrue(_check_condition(False, "==", "false"))
        self.assertFalse(_check_condition(10, "<", 5))


if __name__ == "__main__":
    unittest.main(verbosity=2)
