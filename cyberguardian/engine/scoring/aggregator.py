"""
engine/scoring/aggregator.py
─────────────────────────────
Agrégation des trois couches et politique de décision — IA-7.

Fonctions PURES : aucune entrée / sortie (ni profil, ni modèle, ni base, ni HTTP).
Elles ne dépendent que des scores reçus et de la configuration ci-dessous.

  score_final = round( max( S1 , w2·S2 + w3·S3 ) ), borné à [0, 100]

    S1 : Couche 1, score maximal des règles déclenchées (0 si aucune)
    S2 : Couche 2, Isolation Forest + garde-fou z-score (0-100)
    S3 : Couche 3, probabilité XGBoost × 100

  Décision sur le score final : < 30 PASS · 30-69 CHALLENGE · ≥ 70 BLOCK ·
  ≥ 90 BLOCK + alerte prioritaire.

Poids par défaut w2 = 0,1 / w3 = 0,9 : choisis par balayage (w3 de 0,5 à 1,0) —
au-delà de w2 = 0,1, la Couche 2 dégrade AUC-PR et rappel du score combiné.
"""

from __future__ import annotations

import os

# ── Politique de seuils (score final) ─────────────────────────
SEUIL_CHALLENGE = int(os.getenv("SCORE_THRESHOLD_LOW",  "30"))
SEUIL_BLOCK     = int(os.getenv("SCORE_THRESHOLD_MED",  "70"))
SEUIL_ALERTE    = int(os.getenv("SCORE_THRESHOLD_HIGH", "90"))

# ── Poids de la combinaison anomalie / supervisé ──────────────
WEIGHT_IF  = float(os.getenv("SCORE_WEIGHT_IF",  "0.1"))
WEIGHT_XGB = float(os.getenv("SCORE_WEIGHT_XGB", "0.9"))

FORMULE = "max(S1, w2*S2 + w3*S3)"


def agreger(s1: float, s2: float, s3: float,
            w2: float = WEIGHT_IF, w3: float = WEIGHT_XGB) -> tuple[int, float]:
    """score_final = round(max(S1, w2·S2 + w3·S3)), borné à [0, 100]. Retourne (final, combiné)."""
    combine = w2 * s2 + w3 * s3
    return int(round(min(100.0, max(0.0, s1, combine)))), round(combine, 2)


def decider(score: int) -> tuple[str, bool]:
    """Retourne (décision, alerte_prioritaire) selon la politique 30 / 70 / 90."""
    if score >= SEUIL_BLOCK:
        return "BLOCK", score >= SEUIL_ALERTE
    if score >= SEUIL_CHALLENGE:
        return "CHALLENGE", False
    return "PASS", False
