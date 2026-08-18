# CyberGuardian AI

> Moteur IA de détection de fraude **SIM swap** en temps réel pour le **Mobile Money au Sénégal**

---

## Le problème

Chaque jour au Sénégal, des fraudeurs convainquent un agent télécom de transférer le numéro d'une victime sur une nouvelle carte SIM. Pendant les minutes qui suivent, ils interceptent tous les SMS — y compris les codes de sécurité — réinitialisent le PIN et vident le compte mobile money de la victime.

**3 794 infractions de cybercriminalité** ont été recensées en 2025 au Sénégal, avec des pertes se comptant en milliards de FCFA à l'échelle du continent.

---

## La solution

CyberGuardian AI détecte cette fraude en temps réel en croisant deux signaux que seul l'opérateur peut voir ensemble : les **événements réseau** (changement de SIM, appareil utilisé, antenne) et les **transactions mobile money**.

Quand la combinaison est suspecte, le système agit **avant que l'argent ne sorte**.

---

## Architecture

### Moteur IA — 3 couches

```
Transaction entrante
        │
        ▼
┌───────────────────────┐
│  Couche 1 — Règles    │  8 à 12 règles YAML expertes
│  (IA-4)               │  ex: swap_recent + nouveau_appareil
└──────────┬────────────┘
           │
           ▼
┌───────────────────────┐
│  Couche 2 — Anomalie  │  Isolation Forest + z-scores Welford
│  (IA-5)               │  détection comportement inhabituel
└──────────┬────────────┘
           │
           ▼
┌───────────────────────┐
│  Couche 3 — Supervisé │  XGBoost entraîné sur données labellisées
│  (IA-6)               │  explicabilité SHAP
└──────────┬────────────┘
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
│   ├── simulator/               # IA-1 — Simulateur de données ✅
│   │   ├── subscribers.py       # Génération de 500 abonnés sénégalais (seed=42)
│   │   ├── profiles.py          # Profils comportementaux → Redis / DynamoDB
│   │   ├── scenarios.py         # 5 types de scénarios labellisés
│   │   ├── publisher.py         # Publication dans Kafka / Kinesis
│   │   ├── main.py              # Point d'entrée (batch | replay)
│   │   └── Dockerfile
│   │
│   ├── engine/
│   │   ├── features/            # IA-3 — Feature Updater ✅
│   │   │   ├── updater.py       # Logique métier pure (Welford, fenêtres, sets)
│   │   │   ├── handlers.py      # Dispatch topic → store (FeatureHandler)
│   │   │   ├── main.py          # Boucle de consommation Kafka persistante
│   │   │   └── Dockerfile       # Image légère (requirements-updater.txt)
│   │   ├── rules/               # IA-4 — Règles YAML expertes (à faire)
│   │   ├── anomaly/             # IA-5 — Isolation Forest (à faire)
│   │   ├── supervised/          # IA-6 — XGBoost + SHAP (à faire)
│   │   └── scoring_api/         # IA-7 — API FastAPI (à faire)
│   │
│   ├── interfaces/
│   │   ├── streams.py           # Abstraction Kafka ↔ Kinesis (consumer persistant poll())
│   │   └── store.py             # Abstraction Redis/MinIO ↔ DynamoDB/S3
│   │
│   ├── infra/                   # IA-9 — Infrastructure Terraform AWS (à faire)
│   │
│   ├── docs/
│   │   └── dictionnaire_features.md   # IA-2 — 12 features documentées ✅
│   │
│   ├── notebooks/
│   │   └── 01_EDA_simulateur.ipynb    # EDA Phase 1
│   │
│   ├── docker-compose.yml             # Stack locale complète
│   ├── requirements.txt               # Dépendances Python complètes (API + ML)
│   ├── requirements-updater.txt       # Dépendances minimales du feature-updater
│   └── .gitignore
│
└── README.md
```

---

## Avancement

| Tâche | Description | Statut |
|---|---|---|
| IA-1 | Simulateur de données (500 abonnés, scénarios labellisés) | ✅ Fait |
| IA-2 | Dictionnaire de features (12 features documentées) | ✅ Fait |
| IA-3 | Feature Updater (Welford, fenêtres glissantes, sets) | ✅ Fait + testé Docker |
| IA-4 | Moteur de règles YAML (8-12 règles, rechargeable S3) | ⏳ À faire |
| IA-5 | Détection d'anomalies (Isolation Forest + z-scores) | ⏳ À faire |
| IA-6 | XGBoost via SageMaker + SHAP + champion/challenger | ⏳ À faire |
| IA-7 | API de scoring FastAPI sur ECS Fargate | ⏳ À faire |
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

### Scénarios d'attaque simulés

| Scénario | Description | Label |
|---|---|---|
| `LEGITIME` | Transaction normale dans les habitudes de l'abonné | 0 |
| `SIM_SWAP_SIMPLE` | Swap SIM + 1 gros transfert vers bénéficiaire inconnu | 1 |
| `SIM_SWAP_CASCADE` | Swap SIM + vidage en cascade (3 à 7 transferts rapides) | 1 |
| `NOUVEAU_APPAREIL` | Transaction depuis un nouvel appareil (voyage, nouveau téléphone) | 0 |
| `PIC_OTP` | Pic de demandes OTP (5 à 10) suivi d'un transfert frauduleux | 1 |

### Politique de scoring

| Score | Décision | Action |
|---|---|---|
| 0 – 29 | `PASS` | Transaction laissée passer |
| 30 – 69 | `CHALLENGE` | Vérification supplémentaire |
| 70 – 89 | `BLOCK` | Transaction bloquée + alerte analyste |
| 90 – 100 | `BLOCK` | Blocage immédiat + priorité haute |

---

## Démarrage rapide

### Prérequis

- Docker Desktop (avec WSL2 activé sur Windows)
- Python 3.11+
- Git

### 1. Cloner le repo

```bash
git clone https://github.com/El-khalilDione99/CyberGradianAI.git
cd CyberGradianAI/cyberguardian
```

### 2. Démarrer l'infrastructure

```bash
docker compose up redpanda redis postgres minio redpanda-init minio-init -d
```

Vérifier que tout est healthy :

```bash
docker compose ps
# Tous les services doivent afficher (healthy)
```

### 3. Lancer le Feature Updater

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

### 5. Vérifier les profils dans Redis

```bash
# Nombre de profils mis à jour
docker exec cg_redis redis-cli DBSIZE

# Inspecter un profil (remplacer la clé par une vraie)
docker exec cg_redis redis-cli GET "profile:CPT-<hash>"
```

### 6. Vérifier les données dans Kafka

```bash
docker exec cg_redpanda rpk topic consume transactions \
  --brokers localhost:9092 --num 5 --offset start --format "%v\n"
```

### 7. Lancer l'EDA

```bash
pip install -r requirements.txt
jupyter notebook notebooks/01_EDA_simulateur.ipynb
```

---

## Feature Updater — fonctionnement

Le Feature Updater (IA-3) écoute en permanence les 3 topics Kafka et met à jour les profils abonnés dans Redis. C'est la mémoire vivante du système — sans lui, le moteur de scoring n'a pas de contexte comportemental.

```
Kafka topics                Redis (profil abonné)
──────────────              ─────────────────────
transactions  ──┐           nb_transactions, montant_moyen (Welford)
sim-events    ──┼──►  FU ──► ts_dernier_swap, nb_swaps_30j, iccid_actuel
otp-events    ──┘           nb_otp_1h (fenêtre glissante), nb_otp_24h
                            nb_tx_1h / nb_tx_24h / nb_tx_7j
                            devices_connus, beneficiaires_connus
                            solde, antennes_connues
```

**Algorithme de Welford** : moyenne et variance calculées en O(1) sans stocker l'historique des montants. Implémenté dans `engine/features/updater.py` — logique pure, testable sans infrastructure.

**Performance mesurée** : ~28 msg/s, 0 erreur sur 22 793 événements simulés.

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
| `FU_BATCH_SIZE` | `100` | Taille des lots Kafka pour le feature updater |
| `FU_POLL_INTERVAL_S` | `1.0` | Pause (s) si aucun message à consommer |
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
- **Explicabilité** — chaque décision doit être justifiable (SHAP, règles nommées)
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
