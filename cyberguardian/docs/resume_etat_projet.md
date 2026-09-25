# CyberGuardian AI — État du projet (25/09/2026, branche `ia7-simulateur`)

> ⚠️ **À lire avant toute fusion dans `main`.** Cette branche contient des corrections du
> simulateur et des features qui **se recoupent partiellement** avec celles des 3 commits
> du 22/09 sur `main` (a5f9216, b4b637c, e0613b0) : `updater.py`, `rules/features.py`,
> `calendrier.py`, `config.py`, `scenarios.py`, `test_updater.py` sont en conflit.
> Les deux équipes ont traité les mêmes problèmes différemment (ex. `is_roaming` :
> comparaison de régions ici, `antennes_connues` sur `main`). **La fusion doit être décidée
> ensemble** : elle modifiera le simulateur, **invalidera tous les chiffres ci-dessous** et
> imposera de réentraîner les Couches 2 et 3, de republier les modèles et de relancer
> `scripts/verifier_service_ia7.py`.

## 1. Simulateur (1 500 abonnés, 30 jours)

- **Plus de labels cachés** : les scénarios de fraude n'ont plus de signature que seul le
  simulateur connaît (antenne et appareil de l'attaquant réalistes, délai swap → fraude
  tiré entre 1 min et 2 h, fraudes sans swap via le vol de téléphone).
- **Plus de fuites temporelles** : profils initiaux construits *avant* la simulation,
  compteurs de fenêtres recalculés à l'heure de la transaction, rejeu chronologique strict,
  même calcul de features à l'entraînement et en production.
- Faux positifs réalistes : swaps et nouveaux appareils légitimes, rafales d'OTP
  légitimes, voyages, gros montants ponctuels.

## 2. Performances (jeu de test : 300 abonnés jamais vus, 15 602 transactions, 120 fraudes)

| Couche | Mesure clé | Résultat |
|---|---|---|
| 1 — Règles (v2.0, 12 règles, **recalibrées**) | précision / rappel / faux positifs | 50 % / 71 % / 0,6 % (avant : 10 % / 47 % / 3,6 %) |
| 2 — Isolation Forest + garde-fou z-score | AUC-PR / ROC-AUC / séparation | 0,374 / 0,912 / 60,7 % |
| 3 — XGBoost + SHAP | AUC-PR / ROC-AUC / séparation | 0,741 / 0,934 / 73,8 % |
| **Score final** `max(S1, 0,1·S2 + 0,9·S3)` | AUC-PR / fraudes bloquées | 0,723 / 75,8 % |

## 3. IA-7 — service de scoring : terminé et vérifié en local

- FastAPI : `POST /v1/score`, `POST /reload-model` (clé d'API), `GET /health`, `GET /metrics` ;
  contrat figé dans `docs/contrat_api_scoring.md`. Décisions enregistrées dans PostgreSQL.
- Agrégation isolée dans `engine/scoring/aggregator.py` (fonctions pures). 88 tests passent.
- **Vérification par vraies requêtes HTTP** (`python scripts/verifier_service_ia7.py`) :
  **83,3 % des fraudes détectées** (75,8 % bloquées), **3,5 % de fausses alertes**
  (0,4 % bloquées à tort), latence HTTP **moyenne 21,7 ms, p99 37,2 ms**, 0 erreur,
  `/health` ok → verdict « SERVICE CONFORME ».

## 4. Limites connues

- **Concurrence avec le Feature Updater** : non coordonnés ; une transaction scorée avant le
  traitement du swap qui la précède voit un profil incomplet. À traiter avant production.
- **Vol de téléphone** (sans swap SIM) : 3 fraudes sur 22 détectées — limite acceptée.
- **AWS** (S3, RDS, CloudWatch, Fargate) : code prêt, jamais exécuté.
- **Données simulées** : chiffres valables pour ce simulateur, pas pour du trafic réel.
