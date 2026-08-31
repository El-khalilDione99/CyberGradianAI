# 🛡️ CyberGuardian AI

> Moteur d'IA multicouche de détection de fraude **SIM Swap** en temps réel pour le **Mobile Money au Sénégal**.

---

## 📌 Présentation

Chaque jour, des attaques par **SIM Swap** permettent à des fraudeurs d'intercepter les SMS de sécurité et de vider les comptes Mobile Money d'abonnés en quelques minutes.

**CyberGuardian AI** combine la puissance de 3 couches complémentaires d'IA et de règles pour détecter et bloquer ces attaques **en temps réel avant que l'argent ne sorte**, en croisant les événements réseau (Swap SIM, géolocalisation, changement de mobile) et les transactions financières.

---

## 🏛️ Architecture Moteur IA (3 Couches)

```
Transaction Entrante
        │
        ▼
┌───────────────────────────────────────┐
│  Couche 1 — Règles Expertes (IA-4)    │  10 règles YAML (R01–R10)
│  (Détection immédiate basée sur l'EDA)│  Rechargement à chaud S3 / MinIO
└──────────────────┬────────────────────┘
                   │
                   ▼
┌───────────────────────────────────────┐
│  Couche 2 — Isolation Forest (IA-5)   │  Anomalies comportementales (outliers)
│  (Modèle non-supervisé + Z-Scores)    │  Formule hybride Welford
└──────────────────┬────────────────────┘
                   │
                   ▼
┌───────────────────────────────────────┐
│  Couche 3 — XGBoost + SHAP (IA-6)     │  Modèle supervisé sur données labellisées
│  (Probabilité fine & Top-3 SHAP)      │  Explicabilité temps réel
└──────────────────┬────────────────────┘
                   │
                   ▼
        Score Final 0 – 100 → PASS / CHALLENGE / BLOCK
```

### ⚡ Matrice d'Architecture (Local ↔ AWS Cloud)

| Composant | Stack Locale | AWS Cloud (Production) |
|---|---|---|
| **Bus d'Événements** | Redpanda (Kafka) | Kinesis Data Streams |
| **Feature Store** | Redis | DynamoDB |
| **Stockage Objets / Modèles** | MinIO | S3 (`cg-models`) |
| **Base Relationnelle** | PostgreSQL | RDS PostgreSQL |
| **API & Service Scoring** | Docker Compose | ECS Fargate |

> **Agnosticisme Cloud** : Le code métier utilise une couche d'abstraction (`interfaces/streams.py`, `interfaces/store.py`). Aucune dépendance propriétaire n'est codée en dur.

---

## 📂 Structure du Projet

```
CyberGradianAI/
├── README.md
└── cyberguardian/
    ├── simulator/                    # IA-1 — Simulateur de données & attaques ✅
    ├── engine/
    │   ├── features/                 # IA-3 — Worker Feature Updater (Welford O(1)) ✅
    │   ├── rules/                    # IA-4 — Couche 1 : Moteur de règles YAML R01-R10 ✅
    │   ├── anomaly/                  # IA-5 — Couche 2 : Isolation Forest + Z-score ✅
    │   ├── supervised/               # IA-6 — Couche 3 : XGBoost + SHAP ✅
    │   └── scoring_api/              # IA-7 — API FastAPI de scoring temps réel (en cours)
    ├── interfaces/                   # Abstractions Cloud (Kafka/Redis ↔ Kinesis/DynamoDB)
    ├── tests/                        # 🧪 Suites de 60 tests unitaires & d'intégration (100% OK) ✅
    ├── run_train_couche2.py          # Script d'entraînement Couche 2 (Isolation Forest)
    ├── run_train_couche3.py          # Script d'entraînement Couche 3 (XGBoost + SHAP)
    ├── docker-compose.yml            # Stack locale (Redpanda, Redis, Postgres, MinIO)
    └── requirements.txt              # Dépendances Python
```

---

## 📊 Avancement des Tâches (IA-1 à IA-10)

| Jalon | Périmètre | Statut |
|---|---|:---:|
| **IA-1** | Simulateur de trafic & attaques (500 abonnés, 23 934 événements, 7 scénarios) | ✅ **Validé** |
| **IA-2** | Dictionnaire de features (12 features temps réel) | ✅ **Validé** |
| **IA-3** | Feature Updater (Welford $O(1)$, fenêtres 1h/24h/7j, sets Redis) | ✅ **Validé** |
| **IA-4** | Couche 1 — Moteur de Règles Expertes (10 règles R01-R10, hot-reload) | ✅ **Validé** |
| **IA-5** | Couche 2 — Détection d'anomalies (Isolation Forest + RobustScaler + Z-Score) | ✅ **Validé** |
| **IA-6** | Couche 3 — XGBoost supervisé + Explicabilité SHAP (scale_pos_weight) | ✅ **Validé** |
| **IA-7** | API FastAPI de Scoring temps réel + `/reload-rules` | ⏳ **En cours** |
| **IA-8** | Harnais d'évaluation automatisé & CI/CD GitHub Actions | ⏳ **Prochainement** |
| **IA-9** | Infra Terraform AWS (Kinesis, DynamoDB, S3, ECS Fargate) | ⏳ **Prochainement** |
| **IA-10**| Fiche technique & dossier jury | ⏳ **Prochainement** |

---

## 🚀 Démarrage Rapide

### 1. Prérequis & Clonnage

- **Docker Desktop** avec WSL2 (Windows) ou Linux/macOS
- **Python 3.11+**

```bash
git clone https://github.com/El-khalilDione99/CyberGradianAI.git
cd CyberGradianAI/cyberguardian
pip install -r requirements.txt
```

### 2. Démarrer l'infrastructure locale (Redpanda, Redis, Postgres, MinIO)

```bash
docker compose up redpanda redis postgres minio redpanda-init minio-init -d
```

### 3. Lancer le Feature Updater (Ingestion temps réel)

```bash
docker compose build feature-updater
docker run -d --name cg_feature_updater \
  --network cyberguardian_default \
  -e ENV=local -e KAFKA_BOOTSTRAP_SERVERS=redpanda:9092 \
  -e REDIS_HOST=redis -e REDIS_PORT=6379 \
  cyberguardian-feature-updater
```

### 4. Injecter les données de simulation (23 934 événements)

```bash
docker compose --profile simulator run --rm simulator --mode batch --reset-profiles
```

### 5. Entraîner les Couches d'IA (Couches 2 & 3)

```bash
# Entraîner la Couche 2 (Isolation Forest)
python run_train_couche2.py

# Entraîner la Couche 3 (XGBoost + SHAP)
python run_train_couche3.py
```

### 6. Lancer la suite de tests unitaires & d'intégration (60/60 tests)

```bash
python -m pytest tests/ -v
```

---

## 🛡️ Politique de Scoring & Décision

| Score | Décision | Action Métier |
|---|---|---|
| **0 – 29** | `PASS` | Transaction autorisée immédiatement |
| **30 – 69** | `CHALLENGE` | Authentification renforcée (OTP / SMS / Biométrie) |
| **70 – 89** | `BLOCK` | Transaction bloquée + alerte système |
| **90 – 100** | `BLOCK` | Blocage immédiat + alerte prioritaire fraudeur |

---

## ⚙️ Variables d'Environnement Clés

| Variable | Valeur Locale | Description |
|---|---|---|
| `ENV` | `local` | Bascule l'infrastructure (`local` ou `aws`) |
| `KAFKA_BOOTSTRAP_SERVERS` | `redpanda:9092` | Broker Kafka / Redpanda |
| `REDIS_HOST` | `redis` | Serveur Redis Feature Store |
| `REDIS_PORT` | `6379` | Port Redis |
| `MINIO_ENDPOINT` | `localhost:19000` | Stockage S3/MinIO local |
| `SEED` | `42` | Seed globale garantissant la reproductibilité |

---

## ⚖️ Principes Fondamentaux

1. **Explicabilité totale** : Chaque blocage produit le nom des règles déclenchées (Couche 1) et les 3 facteurs d'impact SHAP (Couche 3).
2. **Reproductibilité stricte** : `SEED=42` utilisé partout pour garantir des démonstrations identiques.
3. **Zéro verrou propriétaire** : Logique métier 100% agnostique du cloud.

---

*CyberGuardian AI — Système de protection de la finance numérique.*
