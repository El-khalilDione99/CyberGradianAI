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
│   ├── simulator/               # IA-1 — Simulateur de données
│   │   ├── subscribers.py       # Génération de 500 abonnés sénégalais (seed=42)
│   │   ├── profiles.py          # Profils comportementaux → Redis / DynamoDB
│   │   ├── scenarios.py         # 5 types de scénarios labellisés
│   │   ├── publisher.py         # Publication dans Kafka / Kinesis
│   │   ├── main.py              # Point d'entrée (batch | replay)
│   │   └── Dockerfile
│   │
│   ├── engine/
│   │   ├── rules/               # IA-4 — Règles YAML expertes
│   │   ├── anomaly/             # IA-5 — Isolation Forest
│   │   ├── supervised/          # IA-6 — XGBoost + SHAP
│   │   ├── scoring_api/         # IA-7 — API FastAPI (ECS Fargate)
│   │   └── features/            # IA-3 — Feature updater (Lambda / consumer)
│   │
│   ├── interfaces/
│   │   ├── streams.py           # Abstraction Kafka ↔ Kinesis
│   │   └── store.py             # Abstraction Redis/MinIO ↔ DynamoDB/S3
│   │
│   ├── infra/                   # IA-9 — Infrastructure Terraform AWS
│   │   └── (Kinesis, DynamoDB, S3, ECS, RDS, IAM, CloudWatch)
│   │
│   ├── notebooks/
│   │   └── 01_EDA_simulateur.ipynb   # EDA Phase 1 — données brutes Kafka
│   │
│   ├── docker-compose.yml       # Stack locale complète
│   ├── requirements.txt         # Dépendances Python
│   └── .gitignore
│
└── README.md
```

---

## Données simulées

### Topics Kafka / Kinesis

| Topic | Champs clés | Label |
|---|---|---|
| `sim-events` | `identifiant`, `ancien_appareil`, `nouvel_appareil`, `antenne`, `horodatage` | `1` (fraude) |
| `otp-events` | `identifiant`, `appareil`, `antenne`, `horodatage` | — |
| `transactions` | `identifiant`, `montant_fcfa`, `solde_avant`, `solde_apres`, `canal`, `antenne`, `horodatage` | `0` ou `1` |

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
| 30 – 69 | `CHALLENGE` | Vérification supplémentaire (10 secondes pour un client honnête) |
| 70 – 89 | `BLOCK` | Transaction bloquée + alerte analyste |
| 90 – 100 | `BLOCK` | Blocage immédiat + priorité haute |

---

## Démarrage rapide

### Prérequis

- Docker Desktop
- Python 3.11+
- Git

### 1. Cloner le repo

```bash
git clone https://github.com/El-khalilDione99/CyberGradianAI.git
cd CyberGradianAI
```

### 2. Démarrer la stack locale

```bash
cd cyberguardian
docker compose up redpanda redis postgres minio -d
```

### 3. Lancer le simulateur

```bash
docker compose --profile simulator run --rm simulator
```

### 4. Vérifier les données dans Kafka

```bash
docker exec cg_redpanda rpk topic consume transactions \
  --brokers localhost:9092 --num 5 --offset start --format "%v\n"
```

### 5. Lancer l'EDA

```bash
pip install -r requirements.txt
jupyter notebook notebooks/01_EDA_simulateur.ipynb
```

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

## Variables d'environnement

Copier `.env.local` pour le développement local :

```bash
cp cyberguardian/.env.local .env
```

Pour le déploiement AWS, utiliser `.env.aws` comme référence (les secrets sont dans AWS Secrets Manager).

---

## Jalons du projet

| Jalon | Date | Objectif |
|---|---|---|
| J1 | 26 juillet 2026 | Flux simulé dans Kinesis, profil mis à jour dans DynamoDB |
| J2 | 23 août 2026 | Démo interne bout-en-bout sur AWS, fraude détectée et visible |
| J3 | 20 septembre 2026 | Modèles et infrastructure demo gelés |
| J4 | 11 octobre 2026 | Reproductibilité vérifiée (démo rejouée 3 fois) |
| Démo finale | Mi-octobre 2026 | Démo live 90 secondes devant le jury |

> **Règle projet** : si on prend du retard, on réduit le périmètre — on ne décale jamais la date.

---

## Honnêteté sur les données

Le système tourne sur des **données 100% simulées**, réalistes mais fictives.
C'est assumé et documenté. L'étape suivante avec Sonatel sera un pilote sur données réelles anonymisées, en mode observation.

---

## Équipe IA

- **Stack** : Python 3.11, scikit-learn, XGBoost, SHAP, FastAPI, boto3
- **Cloud** : AWS eu-west-3 (Paris) — Kinesis, DynamoDB, S3, ECS Fargate, SageMaker, RDS
- **Principes** : score mesurable, décision explicable, code agnostique du cloud

---

*CyberGuardian AI — Juillet 2026*
