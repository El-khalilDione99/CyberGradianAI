# CyberGuardian AI — État du projet (25/09/2026, branche `ia7-simulateur`)

> ⚠️ **Mise à jour du 2026-10-08 : la fusion a eu lieu.** Pour les 6 fichiers en conflit
> (`updater.py`, `rules/features.py`, `calendrier.py`, `config.py`, `scenarios.py`,
> `test_updater.py`), la version de cette branche a été retenue. Seule la formule
> d'agrégation a été changée : la fusion pondérée des trois couches (`w1 = 0,05`,
> `w2 = 0,40`, `w3 = 0,55`) remplace le `max(S1, 0,1·S2 + 0,9·S3)` d'origine — même
> politique que l'API de l'app mobile. Les Couches 2 et 3 ont été réentraînées et
> `scripts/verifier_service_ia7.py` relancé : les chiffres du §2 sont à jour pour cette
> formule (ligne « Score final » modifiée, le reste est inchangé).

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
| **Score final** `round(0,05·S1 + 0,40·S2 + 0,55·S3)` | détectées / bloquées | 83,3 % / 62,5 % |

## 3. IA-7 — service de scoring : terminé et vérifié en local

- FastAPI : `POST /v1/score`, `POST /reload-model` (clé d'API), `GET /health`, `GET /metrics` ;
  contrat figé dans `docs/contrat_api_scoring.md`. Décisions enregistrées dans PostgreSQL.
- Agrégation isolée dans `engine/scoring/aggregator.py` (fonctions pures). 88 tests passent.
- **Vérification par vraies requêtes HTTP** (`python scripts/verifier_service_ia7.py`,
  relancée le 2026-10-08 avec la fusion pondérée) : **83,3 % des fraudes détectées**
  (62,5 % bloquées), **2,8 % de fausses alertes** (0,2 % bloquées à tort), latence HTTP
  **moyenne 27,3 ms, p99 36,8 ms**, 0 erreur, `/health` ok → verdict « SERVICE CONFORME ».

## 4. Limites connues

- **Concurrence avec le Feature Updater** : non coordonnés ; une transaction scorée avant le
  traitement du swap qui la précède voit un profil incomplet. À traiter avant production.
- **Vol de téléphone** (sans swap SIM) : 3 fraudes sur 22 détectées — limite acceptée.
- **AWS** (S3, RDS, CloudWatch, Fargate) : code prêt, jamais exécuté.
- **Données simulées** : chiffres valables pour ce simulateur, pas pour du trafic réel.
