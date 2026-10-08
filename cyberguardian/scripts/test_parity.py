"""
scripts/test_parity.py
────────────────────────
Test de parité entre :
  - les features utilisées à l'ENTRAÎNEMENT (engine.training.training,
    calcul chronologique offline sur l'historique complet)
  - les features calculées en PRODUCTION (engine.rules.features.compute_features,
    à partir du profil Redis maintenu en temps réel par le Feature Updater / IA-3)

Contrairement à engine/supervised/evaluate.py, ce script appelle réellement
compute_features() avec les VRAIS profils lus dans Redis — c'est le seul test
qui peut révéler un écart entre "ce que le modèle a appris" et "ce qu'il reçoit
réellement en production" (train/serve skew).

Prérequis avant de lancer ce script :
  1. Redis et Redpanda/Kafka doivent tourner (docker compose up ...)
  2. Le Feature Updater doit tourner avec la version corrigée de
     engine/features/updater.py (docker compose build feature-updater)
  3. Les événements de simulation doivent avoir été publiés sur Kafka et
     consommés par le Feature Updater, pour que Redis contienne de vrais
     profils à jour (docker compose --profile simulator run --rm simulator ...)

Usage :
    python scripts/test_parity.py
"""

import os
import sys
sys.path.insert(0, ".")

os.environ.setdefault("REDIS_HOST", "localhost")
os.environ.setdefault("REDIS_PORT", "16379")

import random
import pandas as pd

from simulator.subscribers import generate_subscribers
from simulator.calendrier  import planifier_simulation
from simulator.config      import SEED, NB_ABONNES

from engine.training.training import build_training_dataset
from engine.rules.features    import compute_features
from interfaces.store         import get_feature_store

N_ECHANTILLON = int(os.getenv("PARITY_SAMPLE_SIZE", "30"))
TOLERANCE_NUMERIQUE = 1e-6  # tolérance pour les comparaisons float


def main() -> None:
    print("[1/3] Régénération des événements de simulation (mêmes seed/paramètres)...")
    comptes    = generate_subscribers(NB_ABONNES, seed=SEED)
    scenarios  = planifier_simulation(comptes, seed=SEED)
    evenements = [ev for sc in scenarios for ev in sc.evenements]

    df_tx  = pd.DataFrame([e.payload for e in evenements if e.stream == "transactions"])
    df_sim = pd.DataFrame([e.payload for e in evenements if e.stream == "sim-events"])
    df_otp = pd.DataFrame([e.payload for e in evenements if e.stream == "otp-events"])

    for _df in (df_tx, df_sim, df_otp):
        _df["horodatage"] = pd.to_datetime(
            _df["horodatage"], format="ISO8601", utc=True
        ).dt.tz_localize(None)

    print(f"      {len(df_tx)} transactions, {len(df_sim)} sim-events, {len(df_otp)} otp-events")

    print("[2/3] Calcul des features de référence (training.py, offline)...")
    devices_initiaux       = {c.id_compte: c.device_id_habituel for c in comptes}
    beneficiaires_initiaux = {c.id_compte: list(c.beneficiaires_habituels) for c in comptes}

    df_features = build_training_dataset(
        df_tx, df_sim, df_otp,
        devices_initiaux=devices_initiaux,
        beneficiaires_initiaux=beneficiaires_initiaux,
    )
    feature_names = [c for c in df_features.columns
                      if c not in ("id_transaction", "id_compte", "horodatage", "label_fraude")]

    print(f"      {len(df_features)} lignes de référence, {len(feature_names)} features")

    print(f"[3/3] Comparaison avec compute_features() + profils Redis réels "
          f"(échantillon de {N_ECHANTILLON})...")
    print("      NOTE : store.get_profile() retourne l'état FINAL du profil (après")
    print("      ingestion de tous les événements du compte). On ne peut donc comparer")
    print("      correctement qu'avec la DERNIÈRE transaction de chaque compte — pour")
    print("      toute transaction antérieure, les deux instants ne coïncident pas et")
    print("      un écart est ATTENDU, pas un bug (solde/compteurs évoluent après coup).")

    store = get_feature_store()
    # Un seul point de comparaison valide par compte : sa DERNIÈRE transaction,
    # seul moment où "état final Redis" == "état attendu par training.py"
    derniere_tx_par_compte = (
        df_features.sort_values("horodatage").groupby("id_compte").tail(1)
    )
    echantillon = derniere_tx_par_compte.sample(
        n=min(N_ECHANTILLON, len(derniere_tx_par_compte)), random_state=SEED
    )

    df_tx_by_id = df_tx.set_index("id_transaction")

    ecarts = []
    profils_absents = 0

    for _, row in echantillon.iterrows():
        id_compte = row["id_compte"]
        id_tx     = row["id_transaction"]

        if id_tx not in df_tx_by_id.index:
            continue
        event = df_tx_by_id.loc[id_tx].to_dict()
        event["horodatage"] = event["horodatage"].isoformat()

        profile = store.get_profile(id_compte)
        if not profile:
            profils_absents += 1
            continue

        features_prod = compute_features(event, profile)

        for f in feature_names:
            val_ref  = row[f]
            val_prod = features_prod.get(f)

            if isinstance(val_ref, bool) or isinstance(val_prod, bool):
                match = bool(val_ref) == bool(val_prod)
            elif isinstance(val_ref, (int, float)) and isinstance(val_prod, (int, float)):
                match = abs(float(val_ref) - float(val_prod)) <= TOLERANCE_NUMERIQUE
            else:
                match = val_ref == val_prod

            if not match:
                ecarts.append({
                    "id_compte": id_compte,
                    "id_transaction": id_tx,
                    "feature": f,
                    "valeur_training": val_ref,
                    "valeur_production": val_prod,
                })

    print()
    print("═" * 60)
    print("  Résultat du test de parité")
    print("═" * 60)
    print(f"  Échantillon testé     : {len(echantillon)} transactions")
    print(f"  Profils Redis absents : {profils_absents}")
    print(f"  Écarts détectés       : {len(ecarts)}")

    if profils_absents == len(echantillon):
        print()
        print("  ⚠️  Aucun profil trouvé dans Redis pour l'échantillon testé.")
        print("      Vérifier que le Feature Updater a bien consommé les événements")
        print("      de CETTE simulation (mêmes SEED/NB_ABONNES) et que Redis n'a")
        print("      pas été vidé entre temps.")
    elif ecarts:
        print()
        print("  ❌ DES ÉCARTS ONT ÉTÉ DÉTECTÉS — ne pas déployer avant correction.")
        print()
        ecarts_df = pd.DataFrame(ecarts)
        print(f"  Features concernées : {sorted(ecarts_df['feature'].unique().tolist())}")
        print()
        print(ecarts_df.head(20).to_string(index=False))
        if len(ecarts) > 20:
            print(f"  ... et {len(ecarts) - 20} écarts supplémentaires")
    else:
        print()
        print("  ✅ Aucun écart — les features training et production sont identiques")
        print("     sur cet échantillon. Le pipeline peut être considéré comme cohérent.")
    print("═" * 60)


if __name__ == "__main__":
    main()