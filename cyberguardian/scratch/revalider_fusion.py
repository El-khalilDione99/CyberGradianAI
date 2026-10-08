"""
scratch/revalider_fusion.py
────────────────────────────
Revalide la fusion ponderee (0,05 / 0,40 / 0,55) sur un jeu INDEPENDANT
(seed different de l'entrainement) genere par le simulateur de la branche
ia7-simulateur, avec les modeles champions entraines par run_train_couche2.py
et run_train_couche3.py.

Rejoue chaque abonne chronologiquement (sim-events / otp-events / transactions),
score chaque transaction via RealtimePipeline (couche1 + couche2 + couche3 +
agregation ponderee), puis mesure rappel et friction sur les trois couches
combinees. Termine par un petit balayage des poids pour verifier que
0,05 / 0,40 / 0,55 est un choix raisonnable sur ces nouvelles donnees.
"""
import os
import sys
sys.path.insert(0, ".")

os.environ.setdefault("REDIS_HOST", "localhost")
os.environ.setdefault("REDIS_PORT", "16379")
os.environ.setdefault("MINIO_ENDPOINT", "localhost:19000")
os.environ.setdefault("MINIO_ACCESS_KEY", "minioadmin")
os.environ.setdefault("MINIO_SECRET_KEY", "minioadmin123")

from simulator.subscribers import generate_subscribers
from simulator.calendrier import planifier_simulation
from simulator.config import NB_ABONNES
from simulator.profiles import build_initial_profile

from engine.scoring.realtime_pipeline import RealtimePipeline
from engine.scoring.aggregator import agreger, decider
from engine.features.updater import apply_transaction, apply_sim_event, apply_otp_event

SEED_INDEPENDANT = 777   # different du seed=42 utilise pour entrainer les modeles

print(f"[1/3] Generation d'un jeu INDEPENDANT (seed={SEED_INDEPENDANT}, {NB_ABONNES} abonnes)...")
comptes = generate_subscribers(NB_ABONNES, seed=SEED_INDEPENDANT)
profiles = {c.id_compte: build_initial_profile(c) for c in comptes}
scenarios = planifier_simulation(comptes, seed=SEED_INDEPENDANT)

# Regrouper les evenements par abonne, tries chronologiquement
par_compte: dict[str, list] = {c.id_compte: [] for c in comptes}
for sc in scenarios:
    for ev in sc.evenements:
        par_compte[ev.cle_partition].append(ev)
for id_compte in par_compte:
    par_compte[id_compte].sort(key=lambda e: e.horodatage)

n_tx = sum(1 for evs in par_compte.values() for e in evs if e.stream == "transactions")
n_fraud = sum(1 for evs in par_compte.values() for e in evs
              if e.stream == "transactions" and e.payload.get("label_fraude") == 1)
print(f"      {n_tx} transactions ({n_fraud} fraudes)")

print("[2/3] Rejeu chronologique + scoring (regles + IF + XGBoost, modeles champions)...")
pipeline = RealtimePipeline()
print(f"      {pipeline.status()['couche1']['nb_regles']} regles | "
      f"couche2 {pipeline.status()['couche2']['version']} | couche3 {pipeline.status()['couche3']['version']}")

resultats = []   # (label_fraude, s1, s2, s3)
for id_compte, evs in par_compte.items():
    profile = profiles[id_compte]
    for ev in evs:
        if ev.stream == "transactions":
            r = pipeline.score(ev.payload, profile_override=profile, update_profile=False)
            resultats.append((
                int(ev.payload.get("label_fraude", 0)),
                r["couche1"]["score"], r["couche2"]["score"], r["couche3"]["score"],
            ))
            profile = apply_transaction(profile, ev.payload)
        elif ev.stream == "sim-events":
            profile = apply_sim_event(profile, ev.payload)
        elif ev.stream == "otp-events":
            profile = apply_otp_event(profile, ev.payload)
    profiles[id_compte] = profile

print(f"      {len(resultats)} transactions scorees")


def mesurer(w1: float, w2: float, w3: float) -> dict:
    n_fraude = n_legit = 0
    fraude_bloquee = fraude_challenge = legit_friction = 0
    for label, s1, s2, s3 in resultats:
        score, _ = agreger(s1, s2, s3, w1, w2, w3)
        decision, _ = decider(score)
        if label == 1:
            n_fraude += 1
            if decision == "BLOCK":
                fraude_bloquee += 1
            elif decision == "CHALLENGE":
                fraude_challenge += 1
        else:
            n_legit += 1
            if decision != "PASS":
                legit_friction += 1
    detectees = fraude_bloquee + fraude_challenge
    return {
        "n_fraude": n_fraude, "n_legit": n_legit,
        "bloquees": fraude_bloquee, "challenge": fraude_challenge,
        "rappel_block": round(fraude_bloquee / n_fraude, 4) if n_fraude else 0.0,
        "rappel_block_ou_challenge": round(detectees / n_fraude, 4) if n_fraude else 0.0,
        "friction_legit": round(legit_friction / n_legit, 4) if n_legit else 0.0,
    }


print()
print("=== Resultat avec nos poids actuels (0,05 / 0,40 / 0,55) ===")
m = mesurer(0.05, 0.40, 0.55)
print(f"  Fraudes          : {m['n_fraude']}")
print(f"  Bloquees (BLOCK) : {m['bloquees']}  (rappel={m['rappel_block']:.1%})")
print(f"  Challenge        : {m['challenge']}")
print(f"  Detectees (BLOCK+CHALLENGE) : rappel={m['rappel_block_ou_challenge']:.1%}")
print(f"  Friction sur le legitime    : {m['friction_legit']:.2%}")

print()
print("=== Petit balayage des poids (w1=0,05 fixe) ===")
print(f"{'w2 (IF)':>8} {'w3 (XGB)':>9} {'rappel_block':>13} {'rappel_block+challenge':>24} {'friction_legit':>15}")
for w2 in (0.10, 0.20, 0.30, 0.40, 0.50, 0.60):
    w3 = round(1.0 - 0.05 - w2, 2)
    if w3 <= 0:
        continue
    m = mesurer(0.05, w2, w3)
    marque = "  <-- actuel" if abs(w2 - 0.40) < 1e-9 else ""
    print(f"{w2:>8.2f} {w3:>9.2f} {m['rappel_block']:>13.1%} "
          f"{m['rappel_block_ou_challenge']:>24.1%} {m['friction_legit']:>15.2%}{marque}")

print()
print("[3/3] Termine.")
