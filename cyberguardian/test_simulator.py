"""Script de test du simulateur — valide les champs francisés."""
import sys
sys.path.insert(0, ".")

print("=" * 55)
print("TEST SIMULATEUR CyberGuardian AI — champs en français")
print("=" * 55)

# ── Test 1 : Abonnés ─────────────────────────────────────────
from simulator.subscribers import generate_subscribers, ANTENNAS_BY_REGION

subs = generate_subscribers(500, seed=42)
assert len(subs) == 500
sub0 = subs[0]

# Champs francisés présents
assert hasattr(sub0, "identifiant"),           "identifiant manquant"
assert hasattr(sub0, "antenne_domicile"),      "antenne_domicile manquant"
assert hasattr(sub0, "appareils"),             "appareils manquant"
assert hasattr(sub0, "montant_min_habituel"),  "montant_min_habituel manquant"
assert hasattr(sub0, "montant_max_habituel"),  "montant_max_habituel manquant"
assert hasattr(sub0, "solde"),                 "solde manquant"
assert hasattr(sub0, "heures_actives"),        "heures_actives manquant"
assert hasattr(sub0, "beneficiaires_connus"),  "beneficiaires_connus manquant"

# Anciens champs absents
assert not hasattr(sub0, "full_name"),   "full_name ne doit plus exister"
assert not hasattr(sub0, "msisdn"),      "msisdn en clair ne doit plus exister"
assert not hasattr(sub0, "devices"),     "devices (anglais) ne doit plus exister"
assert not hasattr(sub0, "home_antenna"),"home_antenna (anglais) ne doit plus exister"

print(f"[OK] 500 abonnés — champs francisés corrects")
print(f"     identifiant     : {sub0.identifiant}")
print(f"     region          : {sub0.region}")
print(f"     antenne_domicile: {sub0.antenne_domicile}")
print(f"     solde           : {sub0.solde:,} FCFA")
print(f"     segment         : {sub0.segment}")

# Reproductibilité
subs2 = generate_subscribers(500, seed=42)
assert subs[0].identifiant == subs2[0].identifiant
print(f"[OK] Seed reproductible")

# ── Test 2 : Profils ─────────────────────────────────────────
from simulator.profiles import build_initial_profile

profil = build_initial_profile(sub0)

champs_requis = [
    "identifiant", "region", "segment", "antenne_domicile",
    "appareils_connus", "dernier_appareil", "beneficiaires_connus",
    "ts_dernier_swap", "nb_swaps_30j",
    "nb_transactions", "montant_moyen", "montant_m2_welford",
    "nb_tx_1h", "nb_tx_24h", "nb_tx_7j",
    "total_montant_24h", "fenetre_1h_ts", "fenetre_24h_ts",
    "nb_otp_1h", "nb_otp_24h",
    "montant_min_habituel", "montant_max_habituel",
    "heures_actives", "solde", "cree_le", "mis_a_jour_le",
]
for champ in champs_requis:
    assert champ in profil, f"Champ manquant dans profil : {champ}"

# Anciens champs absents
assert "full_name"     not in profil
assert "msisdn"        not in profil
assert "known_devices" not in profil
assert "last_device"   not in profil
assert "home_antenna"  not in profil

print(f"\n[OK] Profil — {len(champs_requis)} champs francisés présents")
print(f"     antenne_domicile: {profil['antenne_domicile']}")
print(f"     solde           : {profil['solde']:,} FCFA")

# ── Test 3 : Scénarios ───────────────────────────────────────
from simulator.scenarios import generate_scenarios, TypeScenario, OPERATOR_ID, CHANNELS

scenarios = generate_scenarios(subs, attack_ratio=0.05, seed=42)
fraudes = [s for s in scenarios if s.est_fraude]
legitimes = [s for s in scenarios if not s.est_fraude]

assert len(scenarios) == 500
assert len(fraudes) > 0
print(f"\n[OK] {len(scenarios)} scénarios — {len(fraudes)} fraudes ({len(fraudes)/500*100:.1f}%)")

# ── Test 4 : sim-events ──────────────────────────────────────
sim_events = [ev.payload for s in scenarios for ev in s.evenements if ev.stream == "sim-events"]
assert len(sim_events) > 0
ev = sim_events[0]

assert ev["type_evenement"] == "sim_swap"
assert ev["operateur"] == OPERATOR_ID
assert "identifiant"     in ev
assert "ancien_appareil" in ev
assert "nouvel_appareil" in ev
assert "antenne"         in ev
assert "agence"          in ev
assert "horodatage"      in ev
assert "operator"        not in ev
assert "msisdn_hash"     not in ev
assert "old_device"      not in ev

print(f"\n[OK] sim-events — champs francisés corrects")
print(f"     operateur : {ev['operateur']}")
print(f"     antenne   : {ev['antenne']}")

# ── Test 5 : otp-events ──────────────────────────────────────
otp_events = [ev.payload for s in scenarios for ev in s.evenements if ev.stream == "otp-events"]
assert len(otp_events) > 0
ev = otp_events[0]

assert ev["type_evenement"] == "otp_request"
assert "identifiant" in ev
assert "appareil"    in ev
assert "antenne"     in ev
assert "horodatage"  in ev
assert "device"      not in ev

print(f"\n[OK] otp-events — champs francisés corrects")

# ── Test 6 : transactions ────────────────────────────────────
tx_events = [ev.payload for s in scenarios for ev in s.evenements if ev.stream == "transactions"]
assert len(tx_events) > 0
ev = tx_events[0]

assert ev["type_evenement"] == "transaction"
assert "id_transaction"          in ev
assert "identifiant"             in ev
assert "appareil"                in ev
assert "montant_fcfa"            in ev
assert "identifiant_beneficiaire" in ev
assert "horodatage"              in ev
assert "canal"                   in ev
assert "antenne"                 in ev
assert "solde_avant"             in ev
assert "solde_apres"             in ev
assert "label"                   in ev

# Anciens champs absents
assert "tx_id"          not in ev
assert "msisdn_hash"    not in ev
assert "device"         not in ev
assert "amount_fcfa"    not in ev
assert "channel"        not in ev
assert "balance_before" not in ev

# canal variable
canaux_utilises = set(e["canal"] for e in tx_events)
assert len(canaux_utilises) > 1
assert canaux_utilises.issubset(set(CHANNELS))
print(f"\n[OK] transactions — champs francisés corrects")
print(f"     canaux utilisés : {canaux_utilises}")

# solde cohérent
for e in tx_events:
    assert e["solde_apres"] <= e["solde_avant"], "solde_apres > solde_avant"
print(f"[OK] solde_avant / solde_apres cohérents")

# Exemple fraude
tx_fraude = next(e for e in tx_events if e["label"] == 1)
print(f"\n    Exemple transaction frauduleuse :")
print(f"    montant_fcfa : {tx_fraude['montant_fcfa']:,} FCFA")
print(f"    solde_avant  : {tx_fraude['solde_avant']:,} FCFA")
print(f"    solde_apres  : {tx_fraude['solde_apres']:,} FCFA")
print(f"    canal        : {tx_fraude['canal']}")
print(f"    antenne      : {tx_fraude['antenne']}")

print("\n" + "=" * 55)
print("TOUS LES TESTS PASSÉS")
print("=" * 55)
