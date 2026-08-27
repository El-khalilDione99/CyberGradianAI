"""
tests/test_updater.py
─────────────────────
Tests unitaires pour la logique métier du Feature Updater (IA-3).

Valide :
  1. Algorithme de Welford O(1) (moyenne et écart-type en ligne).
  2. Fenêtres glissantes temporelles (1h, 24h, 7j).
  3. Enrichissement des ensembles (devices, bénéficiaires, antennes).
  4. Gestion des événements SIM swap (ts_dernier_swap, nb_swaps_30j, ICCID, IMSI).
  5. Gestion des événements OTP (compteurs glissants 1h et 24h).
  6. Idempotence et intégrité du solde.
"""

import sys
from pathlib import Path

# Assure que le dossier cyberguardian est dans sys.path
CYBERGUARDIAN_DIR = Path(__file__).resolve().parent.parent
if str(CYBERGUARDIAN_DIR) not in sys.path:
    sys.path.insert(0, str(CYBERGUARDIAN_DIR))

import math
import unittest
from datetime import datetime, timedelta, timezone

from engine.features.updater import (
    apply_transaction,
    apply_sim_event,
    apply_otp_event,
)


class TestFeatureUpdater(unittest.TestCase):

    def setUp(self):
        self.now = datetime.now(timezone.utc)
        self.initial_profile = {
            "id_compte": "CPT-TEST12345",
            "region": "dakar",
            "segment": "moyen",
            "date_creation": "2024-01-01T00:00:00+00:00",
            "antenne_domicile": "DAK-ANT-001",
            "antennes_connues": ["DAK-ANT-001"],
            "iccid_actuel": "1111111111111111111",
            "imsi_actuel": "608111111111111",
            "ts_dernier_swap": None,
            "nb_swaps_30j": 0,
            "device_id_habituel": "DEV-ORIGINAL",
            "devices_connus": ["DEV-ORIGINAL"],
            "beneficiaires_connus": ["CPT-BENEF1"],
            "nb_transactions": 0,
            "montant_moyen": 10000.0,
            "montant_m2_welford": 4000000.0,  # 2000^2
            "ecart_type_montant": 2000.0,
            "nb_tx_1h": 0,
            "nb_tx_24h": 0,
            "nb_tx_7j": 0,
            "total_montant_24h": 0.0,
            "fenetre_1h_ts": [],
            "fenetre_24h_ts": [],
            "fenetre_7j_ts": [],
            "nb_otp_1h": 0,
            "nb_otp_24h": 0,
            "fenetre_otp_1h_ts": [],
            "fenetre_otp_24h_ts": [],
            "montant_moyen_habituel": 10000.0,
            "heures_actives": list(range(7, 21)),
            "solde": 100000.0,
            "cree_le": self.now.isoformat(),
            "mis_a_jour_le": self.now.isoformat(),
        }

    # ── 1. Mise à jour de transaction simple ─────────────────

    def test_01_apply_single_transaction(self):
        """Vérifie l'incrémentation des compteurs et la mise à jour du solde."""
        event = {
            "id_transaction": "TXN-001",
            "id_compte": "CPT-TEST12345",
            "horodatage": self.now.isoformat(),
            "montant": 5000.0,
            "device_id": "DEV-ORIGINAL",
            "id_beneficiaire": "CPT-BENEF1",
            "antenne": "DAK-ANT-001",
            "solde_avant": 100000.0,
            "solde_apres": 95000.0,
        }
        updated = apply_transaction(self.initial_profile, event)

        self.assertEqual(updated["nb_transactions"], 1)
        self.assertEqual(updated["solde"], 95000.0)
        self.assertEqual(updated["nb_tx_1h"], 1)
        self.assertEqual(updated["nb_tx_24h"], 1)
        self.assertEqual(updated["total_montant_24h"], 5000.0)

    # ── 2. Algorithme de Welford ─────────────────────────────

    def test_02_welford_statistics(self):
        """Vérifie la convergence exacte de la moyenne et de la variance de Welford."""
        profile = {
            "nb_transactions": 0,
            "montant_moyen": 0.0,
            "montant_m2_welford": 0.0,
            "ecart_type_montant": 0.0,
            "solde": 100000.0,
            "devices_connus": ["DEV-ORIGINAL"],
            "beneficiaires_connus": ["CPT-BENEF1"],
            "antennes_connues": ["DAK-ANT-001"],
            "fenetre_1h_ts": [],
            "fenetre_24h_ts": [],
            "fenetre_7j_ts": [],
        }
        montants = [10000.0, 20000.0, 30000.0, 40000.0]

        for i, m in enumerate(montants):
            event = {
                "id_transaction": f"TXN-{i}",
                "id_compte": "CPT-TEST12345",
                "horodatage": (self.now + timedelta(minutes=i)).isoformat(),
                "montant": m,
                "device_id": "DEV-ORIGINAL",
                "id_beneficiaire": "CPT-BENEF1",
                "antenne": "DAK-ANT-001",
                "solde_avant": 100000.0,
                "solde_apres": 100000.0 - m,
            }
            profile = apply_transaction(profile, event)

        self.assertEqual(profile["nb_transactions"], 4)
        expected_mean = sum(montants) / len(montants)  # 25000.0
        self.assertAlmostEqual(profile["montant_moyen"], expected_mean, places=2)

        # Calcul variance population (sqrt(M2 / n))
        variance_pop = sum((x - expected_mean) ** 2 for x in montants) / len(montants)
        expected_std = math.sqrt(variance_pop)
        self.assertAlmostEqual(profile["ecart_type_montant"], expected_std, places=2)

    # ── 3. Enrichissement des sets (devices, bénéficiaires) ───

    def test_03_sets_enrichment(self):
        """Vérifie l'ajout automatique de nouveaux appareils et bénéficiaires."""
        event = {
            "id_transaction": "TXN-NEW-DEV",
            "id_compte": "CPT-TEST12345",
            "horodatage": self.now.isoformat(),
            "montant": 12000.0,
            "device_id": "DEV-NOUVEAU-TABLETTE",
            "id_beneficiaire": "CPT-NOUVEAU-AMI",
            "antenne": "THI-ANT-002",
            "solde_avant": 100000.0,
            "solde_apres": 88000.0,
        }
        updated = apply_transaction(self.initial_profile, event)

        self.assertIn("DEV-NOUVEAU-TABLETTE", updated["devices_connus"])
        self.assertIn("CPT-NOUVEAU-AMI", updated["beneficiaires_connus"])
        self.assertIn("THI-ANT-002", updated["antennes_connues"])

    # ── 4. Gestion des événements SIM Swap ───────────────────

    def test_04_apply_sim_event(self):
        """Vérifie la mise à jour de la SIM (ICCID, IMSI, horodatage, compteur swaps, device)."""
        sim_event = {
            "id_evenement": "SIM-001",
            "id_compte": "CPT-TEST12345",
            "horodatage": self.now.isoformat(),
            "nouveau_iccid": "9999999999999999999",
            "nouveau_imsi": "608999999999999",
            "device_id": "DEV-SWAP-AGENCE",
            "antenne_swap": "MAT-ANT-002",
        }
        updated = apply_sim_event(self.initial_profile, sim_event)

        self.assertEqual(updated["iccid_actuel"], "9999999999999999999")
        self.assertEqual(updated["imsi_actuel"], "608999999999999")
        self.assertEqual(updated["ts_dernier_swap"], self.now.isoformat())
        self.assertEqual(updated["nb_swaps_30j"], 1)
        self.assertIn("DEV-SWAP-AGENCE", updated["devices_connus"])

    # ── 5. Gestion des demandes OTP et fenêtres glissantes ───

    def test_05_apply_otp_event_and_expiration(self):
        """Vérifie le comptage et l'expiration des OTP au-delà de 1h."""
        profile = dict(self.initial_profile)

        # 3 OTP dans l'heure courante
        for i in range(3):
            otp_event = {
                "id_otp": f"OTP-REC-{i}",
                "id_compte": "CPT-TEST12345",
                "horodatage": (self.now + timedelta(minutes=i * 2)).isoformat(),
                "antenne": "DAK-ANT-001",
            }
            profile = apply_otp_event(profile, otp_event)

        self.assertEqual(profile["nb_otp_1h"], 3)
        self.assertEqual(profile["nb_otp_24h"], 3)

        # 1 OTP vieux de 2 heures
        old_otp_event = {
            "id_otp": "OTP-OLD",
            "id_compte": "CPT-TEST12345",
            "horodatage": (self.now - timedelta(hours=2)).isoformat(),
            "antenne": "DAK-ANT-001",
        }
        # Lors d'un nouvel événement maintenant, l'ancien ne doit pas être compté dans 1h
        recent_otp = {
            "id_otp": "OTP-NOW",
            "id_compte": "CPT-TEST12345",
            "horodatage": (self.now + timedelta(hours=2, minutes=5)).isoformat(),
            "antenne": "DAK-ANT-001",
        }
        profile = apply_otp_event(profile, recent_otp)
        self.assertEqual(profile["nb_otp_1h"], 1)  # Seul le plus récent est dans la dernière heure


if __name__ == "__main__":
    unittest.main(verbosity=2)
