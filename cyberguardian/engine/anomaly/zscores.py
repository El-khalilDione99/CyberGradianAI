"""
engine/anomaly/zscores.py
──────────────────────────
Z-scores par abonné — garde-fou interprétable de la Couche 2 (IA-5).

L'Isolation Forest donne un score global mais peu lisible pour un analyste.
Les z-scores répondent à une question simple, par abonné :
    « à combien d'écarts-types de SES habitudes se situe cette transaction ? »

Trois dimensions, chacune suivie par Welford dans le profil (IA-3) :
  montant    : montant de la transaction    (montant_moyen / ecart_type_montant)
  vitesse    : nb de transactions sur 24h   (w_tx24h_moyen / w_tx24h_std)
  otp        : nb d'OTP sur 24h             (w_otp24h_moyen / w_otp24h_std)

Rôle dans le score (voir detector.py) :
  - z >= Z_OVERRIDE_SOFT (3) : l'alerte est remontée en clair (raison lisible),
                               sans modifier le score.
  - z >= Z_OVERRIDE_HARD (5) : le garde-fou impose un score plancher
                               Z_GUARD_FLOOR (50 → CHALLENGE / vérification),
                               même si l'Isolation Forest n'a rien vu.
  - Seules les dimensions avec un historique stable (>= Z_MIN_TRANSACTIONS
    observations) sont prises en compte.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

from engine.features.updater import WELFORD_VELOCITE_24H, WELFORD_OTP_24H

Z_OVERRIDE_SOFT    = float(os.getenv("Z_OVERRIDE_SOFT", "3.0"))
Z_OVERRIDE_HARD    = float(os.getenv("Z_OVERRIDE_HARD", "5.0"))
Z_MIN_TRANSACTIONS = int(os.getenv("Z_MIN_TRANSACTIONS", "10"))

# Plancher d'écart-type : évite des z-scores démesurés sur un abonné très
# régulier (écart-type quasi nul). Relatif au montant moyen pour le montant,
# absolu (1 unité) pour les compteurs.
MONTANT_STD_FLOOR_RATIO = float(os.getenv("Z_MONTANT_STD_FLOOR_RATIO", "0.10"))
COUNT_STD_FLOOR         = 1.0


@dataclass
class ZScoreDimension:
    nom:        str
    z:          float
    observe:    float
    moyenne:    float
    ecart_type: float
    fiable:     bool       # historique suffisant pour faire confiance au z

    def raison(self) -> str:
        return (f"{self.nom} : {self.z:+.1f}σ (observé {self.observe:g}, "
                f"habituel {self.moyenne:.1f} ± {self.ecart_type:.1f})")


@dataclass
class ZScoreReport:
    dimensions: dict[str, ZScoreDimension] = field(default_factory=dict)

    @property
    def z_max(self) -> float:
        """Plus grand z-score parmi les dimensions fiables (0 si aucune)."""
        vals = [d.z for d in self.dimensions.values() if d.fiable]
        return max(vals) if vals else 0.0

    def alertes(self, seuil: float = Z_OVERRIDE_SOFT) -> list[str]:
        """Raisons lisibles des dimensions fiables dont z >= seuil."""
        return [d.raison() for d in self.dimensions.values()
                if d.fiable and d.z >= seuil]

    def as_dict(self) -> dict[str, float]:
        """z-scores des dimensions fiables uniquement (historique suffisant)."""
        return {k: round(d.z, 2) for k, d in self.dimensions.items() if d.fiable}


def _z(x: float, mean: float, std: float) -> float:
    return (x - mean) / std if std > 0 else 0.0


def compute_zscores(features: dict[str, Any], profile: dict[str, Any]) -> ZScoreReport:
    """
    Calcule les z-scores par abonné à partir des features de la transaction
    (compute_features) et du profil AVANT la transaction.
    """
    nb_tx = int(profile.get("nb_transactions", 0))
    dims: dict[str, ZScoreDimension] = {}

    # ── Montant ─────────────────────────────────────────────
    montant = float(features.get("montant_courant", 0.0))
    mean_m  = float(profile.get("montant_moyen", 0.0))
    std_m   = max(float(profile.get("ecart_type_montant", 0.0)),
                  MONTANT_STD_FLOOR_RATIO * mean_m, 1.0)
    dims["montant"] = ZScoreDimension(
        "montant", _z(montant, mean_m, std_m), montant, mean_m, std_m,
        fiable=nb_tx >= Z_MIN_TRANSACTIONS,
    )

    # ── Vitesse (nb transactions 24h) et OTP (nb OTP 24h) ───
    for nom, prefix, feat in [
        ("vitesse_24h", WELFORD_VELOCITE_24H, "nb_tx_24h"),
        ("otp_24h",     WELFORD_OTP_24H,      "nb_otp_24h"),
    ]:
        n    = int(profile.get(f"{prefix}_n", 0))
        mean = float(profile.get(f"{prefix}_moyen", 0.0))
        std  = max(float(profile.get(f"{prefix}_std", 0.0)), COUNT_STD_FLOOR)
        obs  = float(features.get(feat, 0.0))
        dims[nom] = ZScoreDimension(
            nom, _z(obs, mean, std), obs, mean, std,
            fiable=n >= Z_MIN_TRANSACTIONS,
        )

    return ZScoreReport(dimensions=dims)
