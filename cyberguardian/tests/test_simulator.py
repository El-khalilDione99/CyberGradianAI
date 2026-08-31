"""
tests/test_simulator.py
───────────────────────
Validation complète du simulateur v3 (modèle Compte, 30 jours).
"""

import sys
from pathlib import Path

# Assure que le dossier cyberguardian est dans sys.path quel que soit l'endroit où le test est lancé
CYBERGUARDIAN_DIR = Path(__file__).resolve().parent.parent
if str(CYBERGUARDIAN_DIR) not in sys.path:
    sys.path.insert(0, str(CYBERGUARDIAN_DIR))

import copy
import random
from datetime import datetime, timezone

print("=" * 60)
print("TEST SIMULATEUR v3 — CyberGuardian AI (30 jours)")
print("=" * 60)

# ════════════════════════════════════════════════════════════
# 1. CONFIG
# ════════════════════════════════════════════════════════════
from simulator.config import (
    SEED, NB_ABONNES, DUREE_SIMULATION_JOURS, SEGMENTS,
    ANTENNES_PAR_REGION, REGIONS, TYPES_TRANSACTION,
    CANAUX_SWAP_FRAUDE, CANAUX_SWAP_LEGITIME,
)

assert DUREE_SIMULATION_JOURS == 30,     "Durée simulation doit être 30 jours"
assert NB_ABONNES == 500,               "Nombre d'abonnés doit être 500"
assert SEED == 42,                      "Seed doit être 42"
assert set(SEGMENTS.keys()) == {"bas", "moyen", "haut"}
assert len(REGIONS) == 14
print("\n[OK] config.py — paramètres valides")

# ════════════════════════════════════════════════════════════
# 2. COMPTES (subscribers)
# ════════════════════════════════════════════════════════════
from simulator.subscribers import generate_subscribers, Compte

comptes = generate_subscribers(500, seed=42)
assert len(comptes) == 500
c0 = comptes[0]

# Champs obligatoires présents
champs_compte = [
    "id_compte", "iccid_actuel", "imsi_actuel", "region",
    "antenne_domicile", "segment", "device_id_habituel",
    "montant_moyen_habituel", "ecart_type_montant", "solde",
    "date_creation_compte", "heures_actives", "beneficiaires_habituels",
]
d = c0.to_dict()
for champ in champs_compte:
    assert champ in d, f"Champ manquant dans Compte : {champ}"

# Format id_compte
assert c0.id_compte.startswith("CPT-"), f"id_compte doit commencer par CPT- : {c0.id_compte}"
assert len(c0.iccid_actuel) == 19,      f"ICCID doit avoir 19 chiffres"
assert len(c0.imsi_actuel) == 15,       f"IMSI doit avoir 15 chiffres"
assert c0.imsi_actuel.startswith("608"), "IMSI sénégalais doit commencer par 608"
assert c0.device_id_habituel.startswith("DEV-"), "device_id doit commencer par DEV-"

# Pas de msisdn en clair
assert "msisdn" not in d, "msisdn ne doit pas apparaître dans le Compte"

# Montant log-normal > 500
assert c0.montant_moyen_habituel >= 500, "Montant moyen doit être >= 500 XOF"
assert c0.ecart_type_montant > 0,        "Ecart-type doit être > 0"

# Solde cohérent
seg_data = SEGMENTS[c0.segment]
assert c0.solde >= seg_data["solde_min"], f"Solde trop bas pour segment {c0.segment}"
assert c0.solde <= seg_data["solde_max"], f"Solde trop haut pour segment {c0.segment}"

# Antenne cohérente
assert c0.antenne_domicile in ANTENNES_PAR_REGION[c0.region], \
    f"Antenne {c0.antenne_domicile} ne correspond pas à région {c0.region}"

# Bénéficiaires
assert 2 <= len(c0.beneficiaires_habituels) <= 5
assert all(b.startswith("CPT-") for b in c0.beneficiaires_habituels)

# Reproductibilité
comptes2 = generate_subscribers(500, seed=42)
assert comptes[0].id_compte == comptes2[0].id_compte, "Seed non reproductible"

print(f"[OK] 500 comptes générés")
print(f"     id_compte   : {c0.id_compte}")
print(f"     iccid       : {c0.iccid_actuel}")
print(f"     imsi        : {c0.imsi_actuel}")
print(f"     region      : {c0.region}")
print(f"     antenne     : {c0.antenne_domicile}")
print(f"     segment     : {c0.segment}")
print(f"     montant_moy : {c0.montant_moyen_habituel:,.0f} XOF")
print(f"     solde       : {c0.solde:,.0f} XOF")
print(f"     device_id   : {c0.device_id_habituel}")
print(f"[OK] Seed reproductible")

# ════════════════════════════════════════════════════════════
# 3. PROFIL INITIAL
# ════════════════════════════════════════════════════════════
from simulator.profiles import build_initial_profile

profil = build_initial_profile(c0)
champs_profil = [
    "id_compte", "region", "segment", "date_creation",
    "antenne_domicile", "antennes_connues",
    "iccid_actuel", "imsi_actuel", "ts_dernier_swap", "nb_swaps_30j",
    "device_id_habituel", "devices_connus", "beneficiaires_connus",
    "nb_transactions", "montant_moyen", "montant_m2_welford", "ecart_type_montant",
    "nb_tx_1h", "nb_tx_24h", "nb_tx_7j", "total_montant_24h",
    "fenetre_1h_ts", "fenetre_24h_ts",
    "nb_otp_1h", "nb_otp_24h", "fenetre_otp_1h_ts",
    "montant_moyen_habituel", "heures_actives", "solde",
    "cree_le", "mis_a_jour_le",
]
for champ in champs_profil:
    assert champ in profil, f"Champ manquant dans profil : {champ}"

# Amorçage Welford
assert profil["montant_moyen"] == c0.montant_moyen_habituel
assert profil["nb_transactions"] == 0
assert profil["ts_dernier_swap"] is None

print(f"\n[OK] Profil initial — {len(champs_profil)} champs")
print(f"     montant_moyen amorcé : {profil['montant_moyen']:,.0f} XOF")

# ════════════════════════════════════════════════════════════
# 4. SCÉNARIOS — structure et champs
# ════════════════════════════════════════════════════════════
from simulator.scenarios import (
    build_sim_swap_simple, build_sim_swap_cascade, build_pic_otp,
    build_swap_legitime, build_nouveau_device_legitime,
    build_gros_montant_legitime, build_voyage_legitime,
    build_transaction_normale, TypeScenario,
)

# On travaille sur une copie pour ne pas modifier le compte original
c_test = copy.deepcopy(comptes[5])  # compte avec solde suffisant
ts = datetime.now(timezone.utc)

# Champs obligatoires sim-events
def check_sim_event(ev: dict, est_fraude: bool):
    champs = ["id_evenement", "id_compte", "horodatage",
              "ancien_iccid", "nouveau_iccid", "ancien_imsi", "nouveau_imsi",
              "type_sim", "canal_swap", "delai_otp_swap_minutes",
              "antenne_otp", "antenne_swap", "device_id",
              "id_scenario", "label_fraude"]
    for c in champs:
        assert c in ev, f"Champ manquant dans sim-event : {c}"
    assert ev["label_fraude"] == (1 if est_fraude else 0)
    if est_fraude:
        assert ev["canal_swap"] in CANAUX_SWAP_FRAUDE
        assert 0 < ev["delai_otp_swap_minutes"] <= 10
    else:
        assert ev["canal_swap"] in CANAUX_SWAP_LEGITIME

# Champs obligatoires otp-events
def check_otp_event(ev: dict):
    champs = ["id_otp", "id_compte", "horodatage", "motif", "antenne", "id_scenario"]
    for c in champs:
        assert c in ev, f"Champ manquant dans otp-event : {c}"
    assert ev["motif"] == "confirmation_swap"

# Champs obligatoires transactions
def check_transaction(ev: dict, est_fraude: bool):
    champs = ["id_transaction", "id_compte", "horodatage",
              "montant", "devise", "type_transaction",
              "id_beneficiaire", "device_id", "antenne",
              "solde_avant", "solde_apres", "id_scenario", "label_fraude"]
    for c in champs:
        assert c in ev, f"Champ manquant dans transaction : {c}"
    assert ev["devise"] == "XOF"
    assert ev["montant"] >= 0
    assert ev["solde_apres"] <= ev["solde_avant"]
    assert ev["solde_apres"] >= 0
    assert ev["type_transaction"] in TYPES_TRANSACTION
    assert ev["label_fraude"] == (1 if est_fraude else 0)

# Test SIM_SWAP_SIMPLE
s = build_sim_swap_simple(copy.deepcopy(c_test), random.Random(42), ts)
assert s.type_scenario == TypeScenario.SIM_SWAP_SIMPLE
assert s.est_fraude
assert len(s.evenements) == 3  # otp + sim + tx
streams = [ev.stream for ev in s.evenements]
assert "otp-events"   in streams
assert "sim-events"   in streams
assert "transactions" in streams
for ev in s.evenements:
    if ev.stream == "sim-events":
        check_sim_event(ev.payload, True)
    elif ev.stream == "otp-events":
        check_otp_event(ev.payload)
    elif ev.stream == "transactions":
        check_transaction(ev.payload, True)
print(f"\n[OK] SIM_SWAP_SIMPLE : {len(s.evenements)} événements, id={s.id_scenario}")

# Test SIM_SWAP_CASCADE
s = build_sim_swap_cascade(copy.deepcopy(c_test), random.Random(42), ts)
assert s.type_scenario == TypeScenario.SIM_SWAP_CASCADE
assert s.est_fraude
assert len(s.evenements) >= 3  # otp + sim + au moins 1 tx
nb_tx = sum(1 for ev in s.evenements if ev.stream == "transactions")
assert nb_tx >= 1
print(f"[OK] SIM_SWAP_CASCADE : {len(s.evenements)} événements, {nb_tx} transferts")

# Test PIC_OTP
s = build_pic_otp(copy.deepcopy(c_test), random.Random(42), ts)
assert s.type_scenario == TypeScenario.PIC_OTP
assert s.est_fraude
nb_otp = sum(1 for ev in s.evenements if ev.stream == "otp-events")
assert 5 <= nb_otp <= 10
print(f"[OK] PIC_OTP : {len(s.evenements)} événements, {nb_otp} OTP")

# Test SWAP_LEGITIME
s = build_swap_legitime(copy.deepcopy(c_test), random.Random(42), ts)
assert s.type_scenario == TypeScenario.SWAP_LEGITIME
assert not s.est_fraude
for ev in s.evenements:
    if ev.stream == "sim-events":
        check_sim_event(ev.payload, False)
        assert ev.payload["delai_otp_swap_minutes"] >= 5
print(f"[OK] SWAP_LEGITIME : délai OTP >= 5 min (légitime)")

# Test TRANSACTION_NORMALE
s = build_transaction_normale(copy.deepcopy(c_test), random.Random(42), ts)
assert not s.est_fraude
assert len(s.evenements) == 1
tx = s.evenements[0].payload
assert tx["id_scenario"] is None  # pas de scénario scripté
check_transaction(tx, False)
print(f"[OK] TRANSACTION_NORMALE : id_scenario=null, label=0")

# ════════════════════════════════════════════════════════════
# 5. CALENDRIER — volume sur 30 jours
# ════════════════════════════════════════════════════════════
from simulator.calendrier import planifier_simulation

print(f"\nPlanification 30 jours (500 abonnés)...")
scenarios = planifier_simulation(comptes, seed=42)

total_ev = sum(len(s.evenements) for s in scenarios)
nb_fraudes = sum(1 for s in scenarios if s.est_fraude)
nb_legit   = sum(1 for s in scenarios if not s.est_fraude)

# Événements par stream
nb_tx  = sum(1 for s in scenarios for ev in s.evenements if ev.stream == "transactions")
nb_sim = sum(1 for s in scenarios for ev in s.evenements if ev.stream == "sim-events")
nb_otp = sum(1 for s in scenarios for ev in s.evenements if ev.stream == "otp-events")

# Vérifications volume
assert len(scenarios) > 1000,  f"Trop peu de scénarios : {len(scenarios)}"
assert nb_fraudes >= 5,        f"Trop peu de fraudes : {nb_fraudes}"
assert nb_tx > 5000,           f"Trop peu de transactions : {nb_tx}"

# Ordre chronologique garanti
horodatages = [
    s.evenements[0].horodatage
    for s in scenarios
    if s.evenements
]
assert horodatages == sorted(horodatages), "Scénarios non triés par horodatage !"

# Labels cohérents
for s in scenarios:
    for ev in s.evenements:
        if ev.stream == "transactions":
            assert "label_fraude" in ev.payload
            assert ev.payload["label_fraude"] in [0, 1]

print(f"\n[OK] VOLUME 30 JOURS :")
print(f"     Scénarios total  : {len(scenarios):>7}")
print(f"     - Fraudes        : {nb_fraudes:>7}  ({nb_fraudes/len(scenarios)*100:.1f}%)")
print(f"     - Légitimes      : {nb_legit:>7}")
print(f"     Événements total : {total_ev:>7}")
print(f"     - transactions   : {nb_tx:>7}")
print(f"     - sim-events     : {nb_sim:>7}")
print(f"     - otp-events     : {nb_otp:>7}")
print(f"[OK] Ordre chronologique garanti")
print(f"[OK] Labels cohérents (0/1)")

# ════════════════════════════════════════════════════════════
# 6. QUALITÉ DES DONNÉES
# ════════════════════════════════════════════════════════════
print(f"\nQualité des données...")

# Pas de msisdn en clair dans les payloads
for s in scenarios[:100]:
    for ev in s.evenements:
        assert "msisdn" not in ev.payload, "msisdn en clair dans un payload !"

# Solde jamais négatif
soldes_negatifs = [
    ev.payload["solde_apres"]
    for s in scenarios
    for ev in s.evenements
    if ev.stream == "transactions" and ev.payload["solde_apres"] < 0
]
assert len(soldes_negatifs) == 0, f"{len(soldes_negatifs)} soldes négatifs !"

# Devise toujours XOF
assert all(
    ev.payload["devise"] == "XOF"
    for s in scenarios
    for ev in s.evenements
    if ev.stream == "transactions"
)

# id_scenario cohérent (fraudes ont toujours un id_scenario)
for s in scenarios:
    if s.est_fraude:
        for ev in s.evenements:
            assert ev.payload.get("id_scenario") is not None, \
                f"Fraude {s.id_scenario} sans id_scenario dans {ev.stream}"

print(f"[OK] Aucun msisdn en clair dans les payloads")
print(f"[OK] Aucun solde négatif")
print(f"[OK] Devise toujours XOF")
print(f"[OK] id_scenario présent dans tous les scénarios fraude")

print("\n" + "=" * 60)
print("TOUS LES TESTS PASSÉS ✓")
print("=" * 60)
