"""
engine/rules/__init__.py
─────────────────────────
Couche 1 du moteur de scoring — Moteur de règles expertes (IA-4).

Exports publics :
  RuleEngine   : moteur d'évaluation + rechargement à chaud
  compute_features : calcul des features dérivées à la volée
"""

from engine.rules.engine import RuleEngine
from engine.rules.features import compute_features

__all__ = ["RuleEngine", "compute_features"]
