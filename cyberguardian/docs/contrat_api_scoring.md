# Contrat d'API — Moteur de scoring (IA-7)

**Version du contrat : v1** · figé le 2026-09-25

Aucun contrat IA-9 n'existait dans le projet : ce document fige le format de fait du
pipeline de scoring, complété par la Couche 1 (règles). Toute évolution ultérieure doit
rester **additive** (nouveaux champs optionnels uniquement) ; supprimer ou renommer un
champ impose une version v2.

> **Mise à jour du 2026-10-08** : après fusion avec le travail de l'équipe IA sur la
> fusion des scores, la formule d'agrégation est passée d'un `max(S1, w2·S2 + w3·S3)` à
> une **moyenne pondérée des trois couches**, `round(w1·S1 + w2·S2 + w3·S3)`, avec
> `w1 = 0,05`, `w2 = 0,40`, `w3 = 0,55`. Les noms de champs du contrat ne changent pas
> (`agregation`, `score_combine`, etc.) ; seules la formule et les clés de poids évoluent
> (`w1_regles` s'ajoute à `w2_anomalie` / `w3_supervise`). Revalidée par
> `scripts/verifier_service_ia7.py` sur le jeu de test complet (1 500 abonnés,
> 15 602 transactions, 120 fraudes) : 83,3 % des fraudes détectées (62,5 % bloquées),
> 2,8 % de fausses alertes (0,2 % bloquées à tort) — voir §2 et §8.
>
> **Ce service est le point de contact officiel pour l'équipe web/mobile.** C'est
> `POST /v1/score` qu'elle doit appeler pour obtenir une décision, pas reconstruire son
> propre calcul de score côté application.

---

## 1. `POST /v1/score` — scorer une transaction

### Requête (JSON)

Même format que les événements du topic `transactions`.

| Champ | Type | Obligatoire | Description |
|---|---|---|---|
| `id_transaction` | string | oui | Identifiant unique de la transaction |
| `id_compte` | string | oui | Identifiant haché du compte (jamais le MSISDN) |
| `horodatage` | string (ISO 8601 avec fuseau) | oui | Date et heure de la transaction |
| `montant` | number ≥ 0 | oui | Montant en XOF |
| `devise` | string | non (défaut `XOF`) | Devise |
| `type_transaction` | string | non | `transfert`, `paiement`, `retrait`, `depot` |
| `id_beneficiaire` | string | non | Bénéficiaire |
| `device_id` | string | non | Appareil utilisé |
| `antenne` | string | non | Antenne relais (`<REG>-ANT-<n>`) |
| `solde_avant` | number | non | Solde avant la transaction |
| `solde_apres` | number | non | Solde après la transaction |

Les champs inconnus sont ignorés. Exemple :

```json
{
  "id_transaction": "TXN-4F1A2B3C4D5E6F70",
  "id_compte": "CPT-25c30f12e100",
  "horodatage": "2026-07-14T09:19:02+00:00",
  "montant": 45000,
  "devise": "XOF",
  "type_transaction": "transfert",
  "id_beneficiaire": "BEN-9A8B7C6D",
  "device_id": "DEV-71C817C8",
  "antenne": "DAK-ANT-003",
  "solde_avant": 120000,
  "solde_apres": 75000
}
```

### Réponse `200` (JSON)

| Champ | Type | Description |
|---|---|---|
| `id_transaction`, `id_compte` | string | Repris de la requête |
| `scored_at` | string ISO 8601 | Heure du scoring (UTC) |
| `decision` | `PASS` \| `CHALLENGE` \| `BLOCK` | Décision (voir §2) |
| `alerte_prioritaire` | bool | `true` si `score_final ≥ 90` |
| `score_final` | int 0-100 | Score agrégé |
| `seuils` | objet | `{"challenge": 30, "block": 70, "alerte": 90}` |
| `agregation` | objet | `{"formule": "round(w1*S1 + w2*S2 + w3*S3)", "w1_regles": 0.05, "w2_anomalie": 0.40, "w3_supervise": 0.55, "score_combine": float}` |
| `couche1` | objet | `score` (int), `regles` : liste de `{rule_id, nom, score}` déclenchées, `version` |
| `couche2` | objet | `score` (float 0-100, garde-fou inclus), `score_if`, `garde_fou_actif` (bool), `zscores` (objet), `raisons` (liste de textes), `version`, `statut` |
| `couche3` | objet | `score` (float 0-100), `probabilite` (0-1), `shap_top3` : liste de `{feature, value, shap, direction}`, `version`, `statut` |
| `features` | objet | Instantané des variables clés utilisées |
| `latence_ms` | float | Temps de scoring côté serveur |

`statut` d'une couche : `ok`, `degrade` (modèle indisponible : score de repli) ou `erreur`
(exception : score 0). Une couche en échec n'empêche jamais la réponse.

Exemple :

```json
{
  "id_transaction": "TXN-4F1A2B3C4D5E6F70",
  "id_compte": "CPT-25c30f12e100",
  "scored_at": "2026-09-25T10:12:44.120Z",
  "decision": "BLOCK",
  "alerte_prioritaire": false,
  "score_final": 83,
  "seuils": {"challenge": 30, "block": 70, "alerte": 90},
  "agregation": {"formule": "round(w1*S1 + w2*S2 + w3*S3)", "w1_regles": 0.05, "w2_anomalie": 0.40, "w3_supervise": 0.55, "score_combine": 82.73},
  "couche1": {"score": 92, "regles": [{"rule_id": "R01", "nom": "SIM swap récent + montant supérieur à l'habitude", "score": 92},
                                      {"rule_id": "R11", "nom": "SIM swap très récent", "score": 60}], "version": "2.0"},
  "couche2": {"score": 71.3, "score_if": 71.3, "garde_fou_actif": false, "zscores": {"montant": 3.4},
              "raisons": ["montant : +3.4σ (observé 45000, habituel 12000.0 ± 9700.0)"], "version": "20260925_101500", "statut": "ok"},
  "couche3": {"score": 90.2, "probabilite": 0.902,
              "shap_top3": [{"feature": "hours_since_sim_swap", "value": 0.3, "shap": 4.8, "direction": "fraud"}],
              "version": "20260925_102030_114", "statut": "ok"},
  "features": {"hours_since_sim_swap": 0.3, "amount_ratio": 3.75, "is_roaming": false, "new_device": true},
  "latence_ms": 6.8
}
```

### Erreurs

| Code | Cas |
|---|---|
| `422` | Requête invalide (champ obligatoire manquant, horodatage non ISO, montant négatif) |
| `500` | Erreur interne inattendue |

---

## 2. Agrégation et décision

- `S1` : score de la Couche 1 = score maximal des règles déclenchées (0 si aucune).
- `S2` : score de la Couche 2 (Isolation Forest + garde-fou z-score).
- `S3` : score de la Couche 3 = probabilité XGBoost × 100.

**`score_final = round( w1·S1 + w2·S2 + w3·S3 )`**, avec `w1 = 0,05`, `w2 = 0,40`, `w3 = 0,55`
(fusion pondérée des trois couches — même méthode que l'API de l'app mobile).

Revalidé sur le jeu de test complet (1 500 abonnés simulés, 15 602 transactions, 120
fraudes, via `scripts/verifier_service_ia7.py`) : **83,3 % des fraudes détectées**
(CHALLENGE ou BLOCK), dont 62,5 % bloquées directement ; **2,8 % de fausses alertes**
sur les légitimes, dont 0,2 % bloquées à tort. Un petit balayage des poids sur un jeu
indépendant montre qu'au-delà de w2 = 0,20-0,30, chaque point ajouté à la Couche 2 coûte
plus de friction qu'il n'apporte de rappel ; 0,40 reste un choix défendable mais pas le
point optimal du compromis rappel / friction (voir `scratch/revalider_fusion.py`).

| `score_final` | `decision` | `alerte_prioritaire` |
|---|---|---|
| < 30 | `PASS` | false |
| 30 – 69 | `CHALLENGE` (OTP renforcé) | false |
| 70 – 89 | `BLOCK` | false |
| ≥ 90 | `BLOCK` | **true** |

Les seuils s'appliquent au **score final** uniquement ; aucune couche ne décide seule.

---

## 3. `POST /reload-model` — recharger règles et modèles

Recharge `rules.yaml` et les deux modèles depuis le stockage objet (pointeurs
`production/current.json`), sans redémarrer le service.

- En-tête obligatoire : `X-API-Key: <clé>` (variable d'environnement `RELOAD_API_KEY`).
- `200` : `{"rechargement": {"couche1": "...", "couche2": "...", "couche3": "..."}, "versions": {...}}`
- `401` : clé absente ou incorrecte.
- `503` : `RELOAD_API_KEY` non configurée côté serveur (endpoint désactivé).

## 4. `GET /health` — état du service

`200` avec `{"status": "ok" | "degrade", "couches": {...}, "base_decisions": "ok" | "indisponible", "metriques": "<backend>"}`.
`status = degrade` dès qu'un modèle ou la base des décisions est indisponible ; le service
continue de scorer.

## 5. `GET /metrics` — métriques courantes (local)

Instantané en mémoire, depuis le démarrage : nombre de requêtes, latence p50 / p99,
distribution des scores par tranche de 10, taux par décision, taux d'alerte (BLOCK).
En production, les mêmes métriques sont envoyées à CloudWatch (§7).

---

## 6. Enregistrement des décisions

Chaque réponse est enregistrée dans la table `decisions` (PostgreSQL local / RDS),
après la réponse (sans ajouter de latence). Une indisponibilité de la base n'empêche pas
le scoring (erreur journalisée, `/health` en `degrade`).

| Colonne | Contenu |
|---|---|
| `id` | clé technique |
| `id_transaction`, `id_compte` | identifiants |
| `scored_at` | horodatage du scoring |
| `decision`, `alerte_prioritaire`, `score_final` | décision |
| `score_c1`, `score_c2`, `score_c3`, `score_combine` | scores par couche |
| `regles` | règles déclenchées (JSON) |
| `explications` | raisons z-score et SHAP (JSON) |
| `versions` | versions règles / modèles (JSON) |
| `latence_ms` | latence serveur |

## 7. Métriques CloudWatch

Espace de noms `CyberGuardian/Scoring`, publication toutes les 60 s
(`METRICS_BACKEND=cloudwatch`) :

| Métrique | Unité |
|---|---|
| `LatenceP50`, `LatenceP99` | millisecondes |
| `Requetes` | nombre |
| `TauxAlerte` (part des BLOCK), `TauxChallenge` | pourcentage |
| `ScoreFinal` (distribution : valeurs + effectifs) | aucune |

En local (`METRICS_BACKEND=local`), les métriques restent en mémoire et sont lisibles sur
`GET /metrics`.

## 8. Configuration (variables d'environnement)

| Variable | Défaut | Rôle |
|---|---|---|
| `ENV` | `local` | `local` (MinIO, Redis) ou `aws` (S3, DynamoDB) |
| `SCORE_THRESHOLD_LOW` / `_MED` / `_HIGH` | 30 / 70 / 90 | Seuils CHALLENGE / BLOCK / alerte |
| `FUSION_WEIGHT_RULES` / `FUSION_WEIGHT_ANOMALY` / `FUSION_WEIGHT_SUPERVISED` | 0.05 / 0.40 / 0.55 | Poids w1 / w2 / w3 |
| `POSTGRES_DSN` | — | Base des décisions (PostgreSQL local ou RDS) |
| `METRICS_BACKEND` | `local` | `local` ou `cloudwatch` |
| `RELOAD_API_KEY` | — | Clé de `/reload-model` (endpoint désactivé si absente) |
| `SCORING_UPDATE_PROFILE` | `false` | Mise à jour du profil par l'API. Laisser à `false` : le profil est tenu à jour par le Feature Updater (IA-3) |
| `MINIO_*`, `RULES_*`, `MODEL_*` | voir `.env.example` | Emplacements des règles et modèles |

## 9. Hypothèses et limites

- Le profil de l'abonné est **lu** dans le feature store (Redis / DynamoDB) ; il est mis à
  jour par le Feature Updater à partir du flux d'événements, pas par l'API.
- **Limite connue, à traiter avant la mise en production réelle — concurrence avec le
  Feature Updater** : les deux services tournent en parallèle sans coordination. Une
  transaction scorée avant que le Feature Updater ait traité le swap SIM ou les OTP qui la
  précèdent est évaluée sur un profil incomplet. Non testé avec les deux services actifs
  simultanément.
- Un compte inconnu ou sans historique reçoit un profil vide : Couche 2 à 0, règles et
  Couche 3 calculées sur des variables par défaut.
- Le vol de téléphone déverrouillé (fraude sans swap SIM) est peu détecté par les trois
  couches : limite connue et acceptée.
