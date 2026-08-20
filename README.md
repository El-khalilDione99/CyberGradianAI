# CyberGuardian AI

> Moteur IA de détection de fraude **SIM swap** en temps réel pour le **Mobile Money au Sénégal**

---

## Le problème

Chaque jour au Sénégal, des fraudeurs convainquent un agent télécom de transférer le numéro d'une victime sur une nouvelle carte SIM. Pendant les minutes qui suivent, ils interceptent tous les SMS — y compris les codes de sécurité —, réinitialisent le PIN et vident le compte mobile money de la victime.

**3 794 infractions de cybercriminalité** ont été recensées en 2025 au Sénégal, avec des pertes se comptant en milliards de FCFA à l'échelle du continent.

---

## La solution

CyberGuardian AI détecte cette fraude en temps réel en croisant deux signaux que seul l'opérateur peut voir ensemble : les **événements réseau** (changement de SIM, appareil utilisé, antenne relais) et les **transactions mobile money**.

Quand la combinaison est suspecte, le système agit **avant que l'argent ne sorte**.

---

## Architecture

### Moteur IA — 3 couches

```
Transaction entrante
        │
        ▼
┌───────────────────────────────────────┐
│  Couche 1 — Règles (IA-4)             │  10 règles YAML expertes (R01–R10)
│  (ex: swap < 1h + ratio montant > 3)  │  Rechargement à chaud S3 / fichier
└──────────────────┬────────────────────┘
                   │
                   ▼
┌───────────────────────────────────────┐
│  Couche 2 — Anomalie (IA-5)           │  Isolation Forest + z-scores Welford
│  (détection comportement inhabituel)  │  Comportement hors profil
└──────────────────┬────────────────────┘
                   │
                   ▼
┌───────────────────────────────────────┐
│  Couche 3 — Supervisé (IA-6)          │  XGBoost entraîné sur données labellisées
│  (probabilité de fraude + SHAP)       │  Explicabilité temps réel
└──────────────────┬────────────────────┘
                   │
                   ▼
       Score 0-100 → PASS / CHALLENGE / BLOCK
```

### Infrastructure — Local → AWS

| Composant | Local | AWS |
|---|---|---|
| Bus d'événements | Redpanda (Kafka) | Kinesis Data Streams |
| Feature store | Redis | DynamoDB |
| Base relationnelle | PostgreSQL | RDS PostgreSQL |
| Stockage objets | MinIO | S3 |
| API de scoring | Docker Compose | ECS Fargate |
| Entraînement | Local | SageMaker Training Jobs |

> Le code est **agnostique du cloud** — les services AWS sont accédés via `interfaces/streams.py` et `interfaces/store.py`. Un redéploiement on-premise chez l'opérateur reste possible sans réécriture.

---

## Structure du projet

```
CyberGradianAI/
│
├── cyberguardian/
│   │
│   ├── simulator/                    # IA-1 — Simulateur de données ✅
│   │   ├── subscribers.py            # 500 abonnés sénégalais (seed=42)
│   │   ├── profiles.py               # Profils comportementaux → Redis / DynamoDB
│   │   ├── scenarios.py              # 7 scénarios (3 fraudes, 4 légitimes)
│   │   ├── publisher.py              # Publication dans Kafka / Kinesis
│   │   ├── main.py                   # Point d'entrée (batch | replay)
│   │   └── Dockerfile
│   │
│   ├── engine/
│   │   ├── __init__.py
│   │   ├── features/                 # IA-3 — Feature Updater ✅
│   │   │   ├── updater.py            # Logique métier pure (Welford, fenêtres, sets)
│   │   │   ├── handlers.py           # Dispatch topic → store (FeatureHandler)
│   │   │   ├── main.py               # Boucle de consommation Kafka persistante
│   │   │   └── Dockerfile            # Image légère (requirements-updater.txt)
│   │   ├── rules/                    # IA-4 — Moteur de règles expertes ✅
│   │   │   ├── rules.yaml            # 10 règles YAML documentées (R01–R10)
│   │   │   ├── features.py           # Calcul des features dérivées à la volée
│   │   │   ├── engine.py             # Évaluation + rechargement à chaud S3/fichier
│   │   │   └── __init__.py
│   │   ├── anomaly/                  # IA-5 — Isolation Forest (à faire)
│   │   ├── supervised/               # IA-6 — XGBoost + SHAP (à faire)
│   │   └── scoring_api/              # IA-7 — API FastAPI + /reload-rules (à faire)
│   │
│   ├── interfaces/
│   │   ├── streams.py                # Abstraction Kafka ↔ Kinesis
│   │   └── store.py                  # Abstraction Redis/MinIO ↔ DynamoDB/S3
│   │
│   ├── infra/                        # IA-9 — Infrastructure Terraform AWS (à faire)
│   │
│   ├── docs/
│   │   └── dictionnaire_features.md  # IA-2 — 12 features documentées ✅
│   │
│   ├── notebooks/
│   │   └── 01_EDA_simulateur.ipynb   # EDA Phase 1
│   │
│   ├── docker-compose.yml            # Stack locale complète
│   ├── requirements.txt              # Dépendances Python complètes (API + ML)
│   ├── requirements-updater.txt      # Dépendances minimales du feature-updater
│   └── .gitignore
│
└── README.md
```

---

## Avancement

| Tâche | Description | Statut |
|---|---|---|
| IA-1 | Simulateur (500 abonnés, 7 scénarios, 22 793 événements) | ✅ Fait |
| IA-2 | Dictionnaire de features (12 features documentées) | ✅ Fait |
| IA-3 | Feature Updater (Welford, fenêtres glissantes, sets) | ✅ Fait + testé Docker |
| IA-4 | Moteur de règles YAML (10 règles R01–R10, rechargeable S3) | ✅ Fait + validé |
| IA-5 | Détection d'anomalies (Isolation Forest + z-scores) | ⏳ À faire |
| IA-6 | XGBoost via SageMaker + SHAP + champion/challenger | ⏳ À faire |
| IA-7 | API FastAPI scoring + `/reload-rules` sur ECS Fargate | ⏳ À faire |
| IA-8 | Harnais d'évaluation + CI GitHub Actions | ⏳ À faire |
| IA-9 | Socle Terraform AWS (Kinesis, DynamoDB, S3, ECS…) | ⏳ À faire |
| IA-10 | Fiche technique 2 pages pour le jury | ⏳ À faire |

---

## Données simulées

### Topics Kafka / Kinesis

| Topic | Champs clés | Label |
|---|---|---|
| `sim-events` | `id_compte`, `ancien_iccid`, `nouveau_iccid`, `device_id`, `antenne`, `horodatage` | `1` (fraude) |
| `otp-events` | `id_compte`, `motif`, `antenne`, `horodatage` | — |
| `transactions` | `id_compte`, `montant`, `solde_avant`, `solde_apres`, `device_id`, `antenne`, `horodatage` | `0` ou `1` |

### Scénarios simulés

| Scénario | Description | Label |
|---|---|---|
| `NORMAL` | Transaction normale dans les habitudes de l'abonné | 0 |
| `SIM_SWAP_SIMPLE` | Swap SIM + 1 gros transfert vers bénéficiaire inconnu | 1 |
| `SIM_SWAP_CASCADE` | Swap SIM + vidage en cascade (3 à 7 transferts rapides) | 1 |
| `PIC_OTP` | Pic de demandes OTP (5 à 10) suivi d'un transfert frauduleux | 1 |
| `SWAP_LEGITIME` | Changement de SIM normal (même device, antenne locale) | 0 |
| `NOUVEAU_DEVICE_LEGITIME` | Nouveau téléphone, comportement habituel sinon | 0 |
| `GROS_MONTANT_LEGITIME` | Grosse transaction ponctuelle, device et bénéficiaire connus | 0 |
| `VOYAGE_LEGITIME` | Transaction depuis une autre région, profil normal sinon | 0 |

### Politique de scoring

| Score | Décision | Action |
|---|---|---|
| 0 – 29 | `PASS` | Transaction laissée passer |
| 30 – 69 | `CHALLENGE` | Vérification supplémentaire (SMS / biométrie) |
| 70 – 89 | `BLOCK` | Transaction bloquée + alerte analyste |
| 90 – 100 | `BLOCK` | Blocage immédiat + priorité haute |

---

## Démarrage rapide — tester le pipeline complet

### Prérequis

- Docker Desktop avec WSL2 activé (Windows)
- Python 3.11+
- Git

### 1. Cloner le repo

```bash
git clone https://github.com/El-khalilDione99/CyberGradianAI.git
cd CyberGradianAI/cyberguardian
```

### 2. Démarrer l'infrastructure

Lance Redpanda, Redis, PostgreSQL et MinIO. Les services `redpanda-init` et `minio-init` créent automatiquement les topics Kafka, les buckets MinIO **et uploadent `rules.yaml` dans MinIO** au démarrage.

```bash
docker compose up redpanda redis postgres minio redpanda-init minio-init -d
```

Vérifier que tout est healthy :

```bash
docker compose ps
# NAME              STATUS
# cg_redpanda       Up X minutes (healthy)
# cg_redis          Up X minutes (healthy)
# cg_postgres       Up X minutes (healthy)
# cg_minio          Up X minutes (healthy)
```

### 3. Construire et lancer le Feature Updater

```bash
docker compose build feature-updater

docker run -d --name cg_feature_updater \
  --network cyberguardian_default \
  -e ENV=local \
  -e KAFKA_BOOTSTRAP_SERVERS=redpanda:9092 \
  -e REDIS_HOST=redis \
  -e REDIS_PORT=6379 \
  cyberguardian-feature-updater
```

### 4. Lancer le simulateur

```bash
docker compose --profile simulator run --rm simulator --mode batch --reset-profiles
```

Le simulateur publie ~23 000 événements dans les 3 topics Kafka. Le Feature Updater les consomme et met à jour les 500 profils dans Redis.

### 5. Vérifier le pipeline

```bash
# 500 profils créés dans Redis
docker exec cg_redis redis-cli DBSIZE

# Inspecter un profil mis à jour
docker exec cg_redis redis-cli GET "profile:CPT-<hash>"

# Logs du Feature Updater (doit afficher ~28 msg/s, 0 erreur)
docker logs cg_feature_updater --tail 20
```

Vérifier que `rules.yaml` est bien stocké dans MinIO :

```bash
docker run --rm --network cyberguardian_default \
  --entrypoint sh minio/mc:latest -c \
  "mc alias set local http://minio:9000 minioadmin minioadmin123 --api S3v4 > /dev/null \
   && mc ls local/cg-models/ --recursive"
```

### 6. Tester le moteur de règles en Python

Sans infrastructure, la logique des règles est testable directement :

```bash
cd cyberguardian
python -c "
from engine.rules import RuleEngine, compute_features
from datetime import datetime, timezone

engine = RuleEngine()
print(f'Moteur chargé : {engine.rules_count} règles')

# Scénario SIM_SWAP_SIMPLE
event = {
    'id_compte': 'CPT-test',
    'horodatage': '2026-07-14T09:15:00+00:00',
    'montant': 185000.0,
    'device_id': 'DEV-ATTAQUANT',
    'id_beneficiaire': 'BEN-INCONNU',
    'antenne': 'MAT-ANT-002',
}
profile = {
    'montant_moyen_habituel': 20000.0,
    'montant_moyen': 21000.0,
    'ecart_type_montant': 4000.0,
    'devices_connus': ['DEV-HABITUEL'],
    'beneficiaires_connus': ['CPT-connu'],
    'antenne_domicile': 'DAK-ANT-001',
    'ts_dernier_swap': '2026-07-14T09:00:00+00:00',
    'nb_otp_1h': 6,
    'nb_tx_1h': 4,
}
result = engine.evaluate(event, profile)
print(f'Score : {result.score}')
print(f'Règles : {[m.rule_id for m in result.matches]}')
print(f'Décision : BLOCK' if result.score >= 70 else 'PASS/CHALLENGE')
"
```

Résultat attendu : **score 93, règles R01 R02 R03 R04 R05 R06 R07 R09 R10 déclenchées**.

### 7. Vérifier les données Kafka

```bash
docker exec cg_redpanda rpk topic consume transactions \
  --brokers localhost:9092 --num 5 --offset start --format \"%v\n\"
```

---

## Feature Updater — fonctionnement (IA-3)

Le Feature Updater écoute en permanence les 3 topics Kafka et met à jour les profils abonnés dans Redis. C'est la mémoire vivante du système — sans lui, le moteur de scoring n'a pas de contexte comportemental.

```
Kafka topics                Redis (profil abonné)
──────────────              ─────────────────────
transactions  ──┐           nb_transactions, montant_moyen (Welford)
sim-events    ──┼──► FU ──► ts_dernier_swap, nb_swaps_30j, iccid_actuel
otp-events    ──┘           nb_otp_1h (fenêtre glissante), nb_otp_24h
                            nb_tx_1h / nb_tx_24h / nb_tx_7j
                            devices_connus, beneficiaires_connus
                            solde, antennes_connues
```

**Algorithme de Welford** : moyenne et variance calculées en O(1) sans stocker l'historique des montants. Implémenté dans `engine/features/updater.py` — logique pure, testable sans infrastructure.

**Performance mesurée** : ~28 msg/s, 0 erreur sur 22 793 événements simulés.

---

## Moteur de règles — fonctionnement (IA-4)

Le moteur de règles est la Couche 1 du scoring. Il évalue 10 règles YAML contre les features calculées à la volée et retourne le score le plus élevé parmi les règles déclenchées.

### Les 10 règles (R01–R10)

| Règle | Nom | Logique | Score | Statut seuil |
|---|---|---|---|---|
| R01 | SIM swap récent + montant important | `hours_since_swap < 1h` ET `amount_ratio > 3` | 92 | EDA v3 |
| R02 | Nouveau device + montant important | `new_device` ET `amount_ratio > 3` | 78 | EDA v3 |
| R03 | Nouveau device après swap SIM | `new_device` ET `hours_since_swap < 2h` | 93 | EDA v3 |
| R04 | Pic d'OTP | `otp_count_1h >= 3` | 72 | Baseline v1.0 |
| R05 | Vélocité excessive | `nb_tx_1h >= 4` | 70 | Baseline v1.0 |
| R06 | Montant très supérieur à l'habitude | `amount_ratio > 5` | 68 | Baseline v1.0 |
| R07 | Changement géographique | `is_roaming == True` | 60 | Baseline v1.0 |
| R08 | Cascade de transferts | `nb_beneficiaires_1h >= 3` ET `nb_tx_1h >= 3` | 90 | Baseline v1.0 |
| R09 | Nouveau bénéficiaire + montant élevé | `new_beneficiary` ET `amount_ratio > 3` | 78 | EDA v3 |
| R10 | Accumulation de signaux faibles | 3+ signaux faibles simultanés | 88 | Baseline v1.0 |

> R01, R02, R03, R09 ont leurs seuils validés par l'EDA v3. Les autres (R04–R08, R10) seront recalibrés via le harnais IA-8 dès que du volume de scoring réel est disponible.

### Features calculées à la volée

Ces features ne sont pas stockées dans Redis — elles sont dérivées au moment du scoring depuis l'événement courant + le profil abonné :

| Feature | Formule |
|---|---|
| `amount_ratio` | `montant / montant_moyen_habituel` |
| `hours_since_sim_swap` | `(now - ts_dernier_swap).total_seconds() / 3600` |
| `new_device` | `device_id not in devices_connus` |
| `new_beneficiary` | `id_beneficiaire not in beneficiaires_connus` |
| `is_roaming` | `antenne != antenne_domicile` |
| `otp_count_1h` | `nb_otp_1h` (depuis le profil Redis) |
| `zscore_montant` | `(montant - montant_moyen) / ecart_type` |

### Rechargement à chaud

En local : le moteur lit `engine/rules/rules.yaml` (monté comme volume).
En AWS : si le fichier local est absent, le moteur télécharge `cg-models/rules/rules.yaml` depuis S3. L'endpoint `/reload-rules` (IA-7, à faire) déclenchera ce rechargement sans redémarrage du conteneur.

---

## Services locaux

| Service | Port hôte | Interface |
|---|---|---|
| Redpanda (Kafka) | `19092` | — |
| Redis | `16379` | — |
| PostgreSQL | `15432` | — |
| MinIO (API) | `19000` | — |
| MinIO (Console) | `19002` | [http://localhost:19002](http://localhost:19002) |

> Identifiants MinIO : `minioadmin` / `minioadmin123`

---

## Variables d'environnement clés

| Variable | Valeur locale | Description |
|---|---|---|
| `ENV` | `local` | `local` ou `aws` — bascule les interfaces |
| `KAFKA_BOOTSTRAP_SERVERS` | `redpanda:9092` | Broker Kafka / Kinesis endpoint |
| `REDIS_HOST` | `redis` | Host Redis / DynamoDB endpoint |
| `REDIS_PORT` | `6379` | Port Redis |
| `RULES_PATH` | `/app/engine/rules/rules.yaml` | Chemin du fichier de règles (volume local) |
| `RULES_BUCKET` | `cg-models` | Bucket S3/MinIO contenant les règles |
| `RULES_S3_KEY` | `rules/rules.yaml` | Clé S3 du fichier de règles |
| `SCORE_THRESHOLD_LOW` | `30` | Seuil PASS → CHALLENGE |
| `SCORE_THRESHOLD_MED` | `70` | Seuil CHALLENGE → BLOCK |
| `SCORE_THRESHOLD_HIGH` | `90` | Seuil BLOCK prioritaire |
| `FU_BATCH_SIZE` | `100` | Taille des lots Kafka pour le feature updater |
| `SEED` | `42` | Seed globale — démo rejouable à l'identique |

> Aucune clé AWS statique dans le code. En production : rôles IAM attachés aux tâches ECS et OIDC pour la CI.

---

## Jalons du projet

| Jalon | Date | Objectif | Statut |
|---|---|---|---|
| J1 | 26 juillet 2026 | Flux simulé dans Kinesis, profil mis à jour dans DynamoDB | ✅ Validé en local |
| J2 | 23 août 2026 | Démo interne bout-en-bout sur AWS, fraude détectée et visible | ⏳ En cours |
| J3 | 20 septembre 2026 | Modèles et infrastructure demo gelés | ⏳ |
| J4 | 11 octobre 2026 | Reproductibilité vérifiée (démo rejouée 3 fois) | ⏳ |
| Démo finale | Mi-octobre 2026 | Démo live devant le jury | ⏳ |

> **Règle projet** : si on prend du retard, on réduit le périmètre — on ne décale jamais la date.

---

## Principes non négociables

- **Score mesurable** — on vend un score 0-100, pas une prédiction magique
- **Explicabilité** — chaque décision est justifiable (règles nommées, SHAP)
- **Agnosticisme cloud** — aucun appel boto3 dans la logique métier, tout passe par `interfaces/`
- **Seeds fixes** — `SEED=42` partout, une démo doit être rejouable à l'identique
- **Pas de MSK ni endpoint SageMaker permanent** — les deux pièges qui brûlent les crédits AWS
- **Traçabilité des modèles** — tout modèle sérialisé est versionné dans S3 avec ses métriques

---

## Honnêteté sur les données

Le système tourne sur des **données 100% simulées**, réalistes mais fictives.
C'est assumé et documenté. L'étape suivante avec Sonatel sera un pilote sur données réelles anonymisées, en mode observation.

---

## Stack technique

- **Langage** : Python 3.11+
- **ML** : scikit-learn, XGBoost, SHAP
- **API** : FastAPI, uvicorn
- **Streaming** : kafka-python (local), boto3 Kinesis (AWS)
- **Feature store** : redis-py (local), boto3 DynamoDB (AWS)
- **Infra** : Docker Compose (local), Terraform + AWS ECS Fargate (cloud)
- **Région AWS** : eu-west-3 (Paris)

---

*CyberGuardian AI — Août 2026*
