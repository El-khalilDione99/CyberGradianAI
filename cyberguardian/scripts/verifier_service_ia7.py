"""
scripts/verifier_service_ia7.py
────────────────────────────────
Vérification du service de scoring IA-7 par de VRAIES requêtes HTTP.

Rejoue le mois simulé (1 500 abonnés) pour les abonnés du jeu de test de la
Couche 3 — ceux que le modèle XGBoost publié n'a jamais vus — et envoie chaque
transaction à POST /v1/score. Le script joue aussi le rôle du Feature Updater
(IA-3) : swaps SIM, OTP et transactions déjà scorées sont appliqués au profil
Redis, dans l'ordre chronologique, comme en production.

⚠️ Réinitialise dans Redis les profils des abonnés testés (environnement local).

Prérequis : docker compose up -d redis postgres minio scoring-api
            (modèles publiés dans MinIO, voir README §5)

Usage :
    python scripts/verifier_service_ia7.py                  # jeu de test complet (~15 600 tx, ~7 min)
    python scripts/verifier_service_ia7.py --abonnes 30     # contrôle rapide sur 30 abonnés
    python scripts/verifier_service_ia7.py --url http://127.0.0.1:8000   (127.0.0.1 : sous Windows, « localhost » ajoute ~40 ms par appel)
"""

import argparse
import json
import os
import random
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

RACINE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RACINE))
os.environ.setdefault("SIM_NB_ABONNES", "1500")          # volume de référence
os.environ.setdefault("REDIS_HOST", "localhost")
os.environ.setdefault("REDIS_PORT", "16379")
import logging; logging.disable(logging.WARNING)

import httpx
import numpy as np

from simulator.subscribers import generate_subscribers
from simulator.calendrier import planifier_simulation
from simulator.config import SEED, NB_ABONNES
from simulator.profiles import build_initial_profile
from engine.supervised.dataset import TRAIN_RATIO, SEED as SEED_SPLIT, _stratified_split_by_subscriber
from engine.features.updater import apply_transaction, apply_sim_event, apply_otp_event
from interfaces.store import RedisFeatureStore

# Valeurs de référence : jeu de test complet mesuré par ce script le 2026-09-25
# (règles 2.0, IF 20260925_105341, XGBoost 20260925_105400_342, w3 = 0,9).
# À mettre à jour après tout réentraînement volontaire. Seuils d'alerte du contrôle ci-dessous.
REFERENCE = {"detection": 0.833, "fraudes_bloquees": 0.758, "fausses_alertes": 0.035, "bloques_a_tort": 0.004}
TOLERANCE = {"detection_min": 0.75, "fausses_alertes_max": 0.05, "bloques_a_tort_max": 0.01, "p99_max_ms": 250}
CHAMPS = ["id_transaction", "id_compte", "horodatage", "montant", "devise", "type_transaction",
          "id_beneficiaire", "device_id", "antenne", "solde_avant", "solde_apres"]


def pct(x): return f"{100 * x:5.1f} %"
def ok(cond): return "✅" if cond else "⚠️ "


def main():
    ap = argparse.ArgumentParser(description="Vérification du service de scoring IA-7 par requêtes HTTP")
    ap.add_argument("--url", default=os.getenv("SCORING_URL", "http://127.0.0.1:8000"))
    ap.add_argument("--abonnes", type=int, default=0, help="nombre d'abonnés de test (0 = tous)")
    args = ap.parse_args()
    cli = httpx.Client(base_url=args.url, timeout=30)

    print("=" * 72)
    print(f"  VÉRIFICATION DU SERVICE IA-7 — {args.url} — {datetime.now():%Y-%m-%d %H:%M}")
    print("=" * 72)
    try:
        sante_debut = cli.get("/health").json()
    except Exception as exc:
        sys.exit(f"❌ Service injoignable sur {args.url} ({exc}). Lancer : docker compose up -d scoring-api")
    for c in ("couche1", "couche2", "couche3"):
        s = sante_debut["couches"][c]
        print(f"  {c} : version {s.get('version')}  ({s.get('source') or s.get('loaded_from')})")
    print(f"  /health : {sante_debut['status']}  | base des décisions : {sante_debut['base_decisions']}")

    # ── Jeu de test : abonnés de test de la Couche 3 ─────────────
    print(f"\nPréparation : simulation de {NB_ABONNES} abonnés (seed {SEED})…")
    comptes = generate_subscribers(NB_ABONNES, seed=SEED)
    initiaux = {c.id_compte: build_initial_profile(c) for c in comptes}       # avant la simulation
    events = sorted(({**e.payload, "stream": e.stream} for sc in planifier_simulation(comptes, seed=SEED)
                     for e in sc.evenements), key=lambda e: e["horodatage"])
    lignes = [{"_id_compte": e["id_compte"], "_label": int(e["label_fraude"])} for e in events if e["stream"] == "transactions"]
    _, test_rows, _ = _stratified_split_by_subscriber(lignes, TRAIN_RATIO, SEED_SPLIT)   # même découpage que la Couche 3
    testes = sorted({r["_id_compte"] for r in test_rows})
    if args.abonnes:
        fraudeurs = [a for a in testes if any(r["_label"] for r in test_rows if r["_id_compte"] == a)]
        autres = [a for a in testes if a not in fraudeurs]
        rng = random.Random(SEED)
        n_f = min(len(fraudeurs), max(1, args.abonnes // 4))
        testes = sorted(rng.sample(fraudeurs, n_f) + rng.sample(autres, min(len(autres), args.abonnes - n_f)))
    testes = set(testes)

    fs = RedisFeatureStore()
    for a in testes:                                   # profils Redis = état au début du mois
        fs.set_profile(a, initiaux[a])
    cli.post("/v1/score", json={"id_transaction": "TXN-PRECHAUFFAGE", "id_compte": "CPT-PRECHAUFFAGE",
                                "horodatage": "2026-07-01T00:00:00+00:00", "montant": 1000})   # initialise SHAP

    n_tx = sum(1 for e in events if e["stream"] == "transactions" and e["id_compte"] in testes)
    print(f"Envoi de {n_tx} transactions ({len(testes)} abonnés de test) par POST /v1/score…")

    resultats, lat_http, lat_srv, erreurs = [], [], [], 0
    t0 = time.perf_counter()
    for ev in events:
        a = ev["id_compte"]
        if a not in testes:
            continue
        if ev["stream"] == "sim-events":
            fs.set_profile(a, apply_sim_event(fs.get_profile(a), ev))
        elif ev["stream"] == "otp-events":
            fs.set_profile(a, apply_otp_event(fs.get_profile(a), ev))
        else:
            t = time.perf_counter()
            r = cli.post("/v1/score", json={k: ev.get(k) for k in CHAMPS})
            lat_http.append((time.perf_counter() - t) * 1000)
            if r.status_code != 200:
                erreurs += 1
            else:
                b = r.json(); lat_srv.append(b["latence_ms"])
                resultats.append((int(ev["label_fraude"]), getattr(ev["type_scenario"], "value", ev["type_scenario"]), b["decision"]))
            fs.set_profile(a, apply_transaction(fs.get_profile(a), ev))       # rôle du Feature Updater
            if len(lat_http) % 2000 == 0:
                print(f"  … {len(lat_http)}/{n_tx}")
    duree = time.perf_counter() - t0
    sante_fin = cli.get("/health").json()

    # ── Résumé ──────────────────────────────────────────────────
    dec = Counter(d for _, _, d in resultats)
    fraudes = [(s, d) for f, s, d in resultats if f == 1]
    legit = [d for f, _, d in resultats if f == 0]
    nf, nl, n = len(fraudes), len(legit), len(resultats)
    det = sum(d != "PASS" for _, d in fraudes) / max(nf, 1)
    bloq = sum(d == "BLOCK" for _, d in fraudes) / max(nf, 1)
    fa = sum(d != "PASS" for d in legit) / max(nl, 1)
    fab = sum(d == "BLOCK" for d in legit) / max(nl, 1)
    p50, p99 = np.percentile(lat_http, 50), np.percentile(lat_http, 99)

    print("\n" + "=" * 72)
    print("  RÉSUMÉ")
    print("=" * 72)
    print(f"  Transactions envoyées : {len(lat_http)}   (réponses OK : {n}, erreurs HTTP : {erreurs})")
    print(f"  Temps total           : {duree:.0f} s   ({len(lat_http) / duree:.0f} requêtes/s)")
    print(f"\n  Répartition des décisions")
    for d in ("PASS", "CHALLENGE", "BLOCK"):
        print(f"    {d:<10} {dec[d]:>7}   {pct(dec[d] / max(n, 1))}")
    print(f"\n  Fraudes connues : {nf}")
    print(f"    détectées (CHALLENGE ou BLOCK) : {sum(d != 'PASS' for _, d in fraudes):>5}   {pct(det)}   (référence {pct(REFERENCE['detection'])})")
    print(f"      dont bloquées (BLOCK)        : {sum(d == 'BLOCK' for _, d in fraudes):>5}   {pct(bloq)}   (référence {pct(REFERENCE['fraudes_bloquees'])})")
    print(f"    passées en PASS (manquées)     : {sum(d == 'PASS' for _, d in fraudes):>5}   {pct(1 - det)}")
    print(f"\n  Légitimes connues : {nl}")
    print(f"    fausses alertes (CHALLENGE ou BLOCK) : {sum(d != 'PASS' for d in legit):>5}   {pct(fa)}   (référence {pct(REFERENCE['fausses_alertes'])})")
    print(f"      dont bloquées à tort (BLOCK)       : {sum(d == 'BLOCK' for d in legit):>5}   {pct(fab)}   (référence {pct(REFERENCE['bloques_a_tort'])})")
    print(f"\n  Détection par type de fraude")
    for s, c in sorted(Counter(s for s, _ in fraudes).items()):
        d_s = sum(d != "PASS" for ss, d in fraudes if ss == s)
        print(f"    {s:<18} {d_s:>4}/{c:<4} {pct(d_s / c)}")
    print(f"\n  Latence mesurée sur les appels HTTP : moyenne {np.mean(lat_http):.1f} ms | p50 {p50:.1f} ms | p99 {p99:.1f} ms")
    print(f"  (dont calcul côté serveur : moyenne {np.mean(lat_srv):.1f} ms | p99 {np.percentile(lat_srv, 99):.1f} ms)")
    print(f"  /health : début {sante_debut['status']} → fin {sante_fin['status']}")

    verdicts = [
        (det >= TOLERANCE["detection_min"], f"Fraudes détectées ≥ {pct(TOLERANCE['detection_min']).strip()}"),
        (fa <= TOLERANCE["fausses_alertes_max"], f"Fausses alertes ≤ {pct(TOLERANCE['fausses_alertes_max']).strip()}"),
        (fab <= TOLERANCE["bloques_a_tort_max"], f"Légitimes bloqués à tort ≤ {pct(TOLERANCE['bloques_a_tort_max']).strip()}"),
        (p99 <= TOLERANCE["p99_max_ms"], f"Latence p99 ≤ {TOLERANCE['p99_max_ms']} ms"),
        (erreurs == 0, "Aucune erreur HTTP"),
        (sante_fin["status"] == "ok", "/health = ok"),
    ]
    print("\n" + "-" * 72)
    for cond, lib in verdicts:
        print(f"  {ok(cond)} {lib}")
    tout_ok = all(c for c, _ in verdicts)
    print(f"\n  VERDICT : {'✅ SERVICE CONFORME' if tout_ok else '⚠️  À EXAMINER (voir lignes ⚠️ ci-dessus)'}")
    if args.abonnes:
        print("  (contrôle rapide sur un échantillon : chiffres moins stables que sur le jeu complet)")
    print("=" * 72)

    dossier = RACINE / "rapports_verification"; dossier.mkdir(exist_ok=True)
    fichier = dossier / f"verification_ia7_{datetime.now():%Y%m%d_%H%M%S}.json"
    fichier.write_text(json.dumps({
        "date": datetime.now().isoformat(), "url": args.url, "abonnes_testes": len(testes),
        "transactions": len(lat_http), "erreurs_http": erreurs, "duree_s": round(duree, 1),
        "decisions": dict(dec), "fraudes": nf, "detection": round(det, 4), "fraudes_bloquees": round(bloq, 4),
        "legitimes": nl, "fausses_alertes": round(fa, 4), "bloques_a_tort": round(fab, 4),
        "latence_http_ms": {"moyenne": round(float(np.mean(lat_http)), 1), "p50": round(float(p50), 1), "p99": round(float(p99), 1)},
        "health": {"debut": sante_debut["status"], "fin": sante_fin["status"]},
        "versions": {c: sante_fin["couches"][c].get("version") for c in ("couche1", "couche2", "couche3")},
        "verdict": "conforme" if tout_ok else "a_examiner",
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"  Rapport enregistré : {fichier.relative_to(RACINE)}")
    sys.exit(0 if tout_ok else 1)


if __name__ == "__main__":
    main()
