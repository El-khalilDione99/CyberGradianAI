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

Poids retenus : w2 = 0,1 / w3 = 0,9.

Choisis après comparaison mesurée (2026-10-09, scratch/comparer_fusion.py) de
plusieurs formules sur le même jeu de test (1500 abonnés, 300 jamais vus à
l'entraînement, 15 602 tx / 120 fraudes), à modèles identiques :

  - La fusion pondérée des trois couches (0,05/0,40/0,55, utilisée un temps)
    détecte autant de fraudes au total (83,3 %) mais n'en BLOQUE que 62,5 %
    (le reste part en CHALLENGE). Or pour un SIM swap, le CHALLENGE (OTP) est
    une protection illusoire : l'attaquant a la carte SIM, c'est lui qui reçoit
    le code — il peut l'approuver lui-même. Le taux de BLOCK réel est donc le
    chiffre qui compte, pas le total détecté.
  - Le `max` ci-dessus détecte le même total (83,3 %) mais en BLOQUE 76,7 % —
    parce qu'une règle forte peut déclencher un blocage à elle seule, sans
    attendre l'accord des deux autres couches.
  - `max(S1, S2, S3)` sans poids détecte plus (86,7 %) mais au prix d'une
    friction x4 sur les légitimes (10,7 % contre 3,4 %) : inacceptable.
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
