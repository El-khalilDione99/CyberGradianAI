"""
engine/training/training.py
─────────────────────────────
⚠️ OBSOLÈTE pour l'entraînement de la Couche 3 : ce module calcule les features
avec son propre code (pandas), différent de compute_features() utilisé en
production. Le modèle entraîné ainsi obtenait AUC-PR 0,98 hors ligne mais 0,05
en temps réel. L'entraînement utilise désormais engine.supervised.dataset
(même calcul que XGBoostDetector.predict()). Conservé pour scripts/test_parity.py.

Reconstruit les features d'entraînement XGBoost à partir de l'historique complet
des événements simulés (transactions, sim-events, otp-events), en calculant les
vraies fenêtres glissantes (nb_tx_1h/24h/7j, nb_otp_24h, nb_swaps_30j,
hours_since_sim_swap) par un parcours chronologique compte par compte.

Contrairement à engine/rules/features.py (qui lit un profil Redis censé être
mis à jour en temps réel par IA-3), ce script travaille offline sur l'historique
complet des événements. Il n'a donc pas le problème diagnostiqué en simulation
actuellement (profil jamais alimenté -> compteurs bloqués à 0 / hours_since_sim_swap
bloqué au plafond de 2160h pour tout le monde).

Règle d'or anti-fuite (data leakage) :
Pour chaque transaction, on ne regarde QUE les événements dont l'horodatage est
STRICTEMENT ANTÉRIEUR à celui de la transaction courante. Aucune information du
futur n'entre dans le calcul d'une feature.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

HOURS_SINCE_SWAP_CAP = 2160.0  # 90 jours, cohérent avec engine/rules/features.py


# ════════════════════════════════════════════════════════════
#  Normalisation
# ════════════════════════════════════════════════════════════
def _to_dt(df: pd.DataFrame, col: str = "horodatage") -> pd.DataFrame:
    df = df.copy()
    # format='ISO8601' : gère les formats mixtes (avec/sans microsecondes)
    # tz_localize(None) : passe en naive pour permettre la comparaison avec
    # np.datetime64 côté calcul des fenêtres glissantes
    df[col] = pd.to_datetime(df[col], format="ISO8601", utc=True).dt.tz_localize(None)
    return df


# ════════════════════════════════════════════════════════════
#  Point d'entrée principal
# ════════════════════════════════════════════════════════════

def build_training_dataset(
    df_tx: pd.DataFrame,
    df_sim: pd.DataFrame,
    df_otp: pd.DataFrame,
    devices_initiaux: dict[str, str] | None = None,
    beneficiaires_initiaux: dict[str, list[str]] | None = None,
) -> pd.DataFrame:
    """
    Paramètres
    ----------
    df_tx  : événements "transactions" (id_compte, horodatage, montant,
             device_id, id_beneficiaire, antenne, solde_avant, label_fraude, ...)
    df_sim : événements "sim-events" (id_compte, horodatage, device_id, ...)
    df_otp : événements "otp-events" (id_compte, horodatage, antenne, ...)
    devices_initiaux       : {id_compte: device_id_habituel} — état connu à J0,
                              issu de l'objet Compte. Sans ça, la toute première
                              transaction de chaque compte sera artificiellement
                              marquée new_device=True.
    beneficiaires_initiaux : {id_compte: [beneficiaires_habituels]} — idem.

    Retourne
    --------
    DataFrame avec une ligne par transaction et les 17 features + label_fraude.
    """
    devices_initiaux = devices_initiaux or {}
    beneficiaires_initiaux = beneficiaires_initiaux or {}

    df_tx = _to_dt(df_tx).sort_values(["id_compte", "horodatage"]).reset_index(drop=True)
    df_sim = _to_dt(df_sim).sort_values(["id_compte", "horodatage"]).reset_index(drop=True)
    df_otp = _to_dt(df_otp).sort_values(["id_compte", "horodatage"]).reset_index(drop=True)

    rows = []
    for id_compte, tx_grp in df_tx.groupby("id_compte", sort=False):
        sim_ts = df_sim.loc[df_sim["id_compte"] == id_compte, "horodatage"].to_numpy()
        otp_ts = df_otp.loc[df_otp["id_compte"] == id_compte, "horodatage"].to_numpy()
        tx_ts = tx_grp["horodatage"].to_numpy()

        # État "connu du profil", initialisé avec l'historique pré-simulation
        # (sinon la 1ère transaction de chaque compte fausse new_device/new_beneficiary)
        devices_connus: set[str] = set()
        if id_compte in devices_initiaux:
            devices_connus.add(devices_initiaux[id_compte])
        beneficiaires_connus: set[str] = set(beneficiaires_initiaux.get(id_compte, []))
        antennes_connues: set[str] = set()

        montants_hist: list[float] = []

        for i, (_, ev) in enumerate(tx_grp.iterrows()):
            ts = ev["horodatage"]
            ts64 = np.datetime64(ts)
            montant = float(ev["montant"])
            device_id = ev.get("device_id", "") or ""
            beneficiaire = ev.get("id_beneficiaire", "") or ""
            antenne = ev.get("antenne", "") or ""

            # ── Compteurs glissants (strictement < ts) ───────────────
            nb_tx_1h = int(((tx_ts < ts64) & (tx_ts >= ts64 - np.timedelta64(1, "h"))).sum())
            nb_tx_24h = int(((tx_ts < ts64) & (tx_ts >= ts64 - np.timedelta64(24, "h"))).sum())
            nb_tx_7j = int(((tx_ts < ts64) & (tx_ts >= ts64 - np.timedelta64(7, "D"))).sum())

            otp_count_1h = int(((otp_ts < ts64) & (otp_ts >= ts64 - np.timedelta64(1, "h"))).sum())
            nb_otp_24h = int(((otp_ts < ts64) & (otp_ts >= ts64 - np.timedelta64(24, "h"))).sum())
            nb_swaps_30j = int(((sim_ts < ts64) & (sim_ts >= ts64 - np.timedelta64(30, "D"))).sum())

            # ── hours_since_sim_swap (dernier swap strictement antérieur) ──
            swaps_avant = sim_ts[sim_ts < ts64]
            if len(swaps_avant) > 0:
                dernier_swap = swaps_avant.max()
                delta_h = (ts64 - dernier_swap) / np.timedelta64(1, "h")
                hours_since_sim_swap = min(float(delta_h), HOURS_SINCE_SWAP_CAP)
            else:
                hours_since_sim_swap = HOURS_SINCE_SWAP_CAP

            # ── new_device / new_beneficiary / is_roaming ────────────
            new_device = bool(device_id and device_id not in devices_connus)
            new_beneficiary = bool(beneficiaire and beneficiaire not in beneficiaires_connus)
            is_roaming = bool(antenne and antennes_connues and antenne not in antennes_connues)

            # ── montant_moyen / ecart_type_montant (calcul glissant, historique < ts) ──
            if montants_hist:
                montant_moyen = float(np.mean(montants_hist))
                ecart_type_montant = float(np.std(montants_hist, ddof=0)) if len(montants_hist) > 1 else 0.0
            else:
                montant_moyen = montant  # pas d'historique -> repli sur la tx courante
                ecart_type_montant = 0.0

            ref_montant = max(montant_moyen, 1.0)
            amount_ratio = montant / ref_montant

            ref_std = max(ecart_type_montant, 1.0)
            zscore_montant = (montant - montant_moyen) / ref_std

            hour_of_day = ts.hour
            # NOTE: is_active_hour est une approximation grossière (toujours True).
            # En production ceci vient de profile["heures_actives"] (IA-3). Si vous avez
            # accès au segment du compte (bas/moyen/haut) et à HEURES_ACTIVES de
            # simulator/config.py, remplacez cette ligne par un vrai lookup.
            is_active_hour = True

            rows.append({
                "id_transaction": ev.get("id_transaction"),
                "id_compte": id_compte,
                "horodatage": ts,
                "amount_ratio": round(amount_ratio, 4),
                "zscore_montant": round(zscore_montant, 4),
                "hours_since_sim_swap": round(hours_since_sim_swap, 4),
                "new_device": new_device,
                "new_beneficiary": new_beneficiary,
                "is_roaming": is_roaming,
                "otp_count_1h": otp_count_1h,
                "nb_tx_1h": nb_tx_1h,
                "nb_tx_24h": nb_tx_24h,
                "nb_tx_7j": nb_tx_7j,
                "nb_otp_24h": nb_otp_24h,
                "nb_swaps_30j": nb_swaps_30j,
                "is_active_hour": is_active_hour,
                "hour_of_day": hour_of_day,
                "montant_moyen": round(montant_moyen, 4),
                "ecart_type_montant": round(ecart_type_montant, 4),
                "solde": float(ev.get("solde_avant", 0.0)),
                "label_fraude": int(ev.get("label_fraude", 0)),
            })

            # ── Mise à jour de l'état "connu" APRÈS calcul (jamais avant !) ──
            if device_id:
                devices_connus.add(device_id)
            if beneficiaire:
                beneficiaires_connus.add(beneficiaire)
            if antenne:
                antennes_connues.add(antenne)
            montants_hist.append(montant)

    return pd.DataFrame(rows)


# ════════════════════════════════════════════════════════════
#  Split stratifié + résultat supervisé (remplace build_dataset())
# ════════════════════════════════════════════════════════════

# Même ordre que engine/supervised/dataset.py::XGB_FEATURE_NAMES
# — gardé identique pour que detector.py reste compatible.
FEATURE_NAMES: list[str] = [
    "amount_ratio",
    "zscore_montant",
    "hours_since_sim_swap",
    "new_device",
    "new_beneficiary",
    "is_roaming",
    "otp_count_1h",
    "nb_tx_1h",
    "nb_tx_24h",
    "nb_tx_7j",
    "nb_otp_24h",
    "nb_swaps_30j",
    "is_active_hour",
    "hour_of_day",
    "montant_moyen",
    "ecart_type_montant",
    "solde",
]


@dataclass
class SupervisedDatasetResult:
    X_train:          np.ndarray
    y_train:          np.ndarray
    X_test:           np.ndarray
    y_test:           np.ndarray
    scale_pos_weight: float
    feature_names:    list[str]
    meta:             dict = field(default_factory=dict)

    @property
    def n_train(self) -> int:
        return len(self.X_train)

    @property
    def n_test(self) -> int:
        return len(self.X_test)
    @property
    def fraud_rate_train(self) -> float:
        if len(self.y_train) == 0:
            return 0.0
        return float(self.y_train.sum() / len(self.y_train))

    @property
    def fraud_rate_test(self) -> float:
        if len(self.y_test) == 0:
            return 0.0
        return float(self.y_test.sum() / len(self.y_test))

def split_supervised_dataset(
    df_features: pd.DataFrame,
    feature_names: list[str] | None = None,
    train_ratio: float = 0.80,
    seed: int = 42,
) -> SupervisedDatasetResult:
    """
    Split stratifié 80/20 PAR ABONNÉ (même logique que
    engine/supervised/dataset.py::_stratified_split_by_subscriber),
    appliqué au DataFrame produit par build_training_dataset().

    Garantit que train ET test contiennent des fraudes et des légitimes,
    et qu'un même compte n'apparaît jamais à la fois en train et en test
    (évite une fuite via les fenêtres glissantes d'un même abonné).
    """
    feature_names = feature_names or FEATURE_NAMES

    by_compte: dict[str, list[int]] = {}
    for idx, id_compte in df_features["id_compte"].items():
        by_compte.setdefault(id_compte, []).append(idx)

    comptes_fraude = [c for c, idxs in by_compte.items()
                       if df_features.loc[idxs, "label_fraude"].any()]
    comptes_legit  = [c for c, idxs in by_compte.items()
                       if not df_features.loc[idxs, "label_fraude"].any()]

    rng = random.Random(seed)
    comptes_fraude = sorted(comptes_fraude)
    comptes_legit  = sorted(comptes_legit)
    rng.shuffle(comptes_fraude)
    rng.shuffle(comptes_legit)

    cut_fraude = max(1, int(len(comptes_fraude) * train_ratio))
    cut_legit  = max(1, int(len(comptes_legit) * train_ratio))
    train_ids = set(comptes_fraude[:cut_fraude]) | set(comptes_legit[:cut_legit])

    mask_train = df_features["id_compte"].isin(train_ids)
    df_train = df_features[mask_train]
    df_test  = df_features[~mask_train]

    X_train = df_train[feature_names].astype(np.float32).to_numpy()
    y_train = df_train["label_fraude"].astype(np.int8).to_numpy()
    X_test  = df_test[feature_names].astype(np.float32).to_numpy()
    y_test  = df_test["label_fraude"].astype(np.int8).to_numpy()

    n_fraud_train = int(y_train.sum())
    n_legit_train = int((y_train == 0).sum())
    scale_pos_weight = (n_legit_train / n_fraud_train) if n_fraud_train > 0 else 1.0

    meta = {
        "n_total":          len(df_features),
        "n_train":          len(X_train),
        "n_test":           len(X_test),
        "n_fraud_train":    n_fraud_train,
        "n_fraud_test":     int(y_test.sum()),
        "fraud_rate_train": round(float(y_train.mean()), 4) if len(y_train) else 0.0,
        "fraud_rate_test":  round(float(y_test.mean()), 4) if len(y_test) else 0.0,
        "scale_pos_weight": round(scale_pos_weight, 2),
        "feature_names":    feature_names,
        "seed":             seed,
    }

    return SupervisedDatasetResult(
        X_train=X_train, y_train=y_train,
        X_test=X_test, y_test=y_test,
        scale_pos_weight=scale_pos_weight,
        feature_names=feature_names,
        meta=meta,
    )


if __name__ == "__main__":
    # Exemple d'utilisation — adaptez les chemins/format à votre pipeline
    df_tx = pd.read_csv("data/transactions.csv")
    df_sim = pd.read_csv("data/sim_events.csv")
    df_otp = pd.read_csv("data/otp_events.csv")

    df_features = build_training_dataset(df_tx, df_sim, df_otp)
    ds = split_supervised_dataset(df_features)

    print(f"Train : {ds.n_train} (fraudes={int(ds.y_train.sum())})")
    print(f"Test  : {ds.n_test} (fraudes={int(ds.y_test.sum())})")
    print(f"scale_pos_weight = {ds.scale_pos_weight:.1f}")