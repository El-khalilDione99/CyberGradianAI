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
│  Couche 1 — Règles Expertes (IA-4)    │  12 règles YAML (R01–R12, v2.0)
│  (Recalibrées sur simulateur réaliste)│  Rechargement à chaud S3 / MinIO
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
   Score final = round(max(S1, 0,1·S2 + 0,9·S3))  →  PASS / CHALLENGE / BLOCK (+ alerte ≥ 90)
```

Le score final prend le maximum entre les règles (S1) et une combinaison pondérée de
l'Isolation Forest (S2, poids 0,1) et du XGBoost (S3, poids 0,9) : une règle forte déclenche
un blocage à elle seule, sans attendre l'accord des deux autres couches — important pour un
SIM swap, où le CHALLENGE (OTP) ne protège pas vraiment (l'attaquant a la carte SIM).
Comparée à une fusion pondérée des trois couches sur le même jeu de test (1 500 abonnés,
15 602 transactions, 120 fraudes), cette formule détecte autant de fraudes au total (83,3 %)
mais en *bloque* 76,7 % contre 62,5 % — voir `docs/contrat_api_scoring.md` §2.

**`POST /v1/score` (service `scoring_api`) est le point d'entrée officiel pour l'équipe
web/mobile** : c'est lui qu'elle doit appeler pour obtenir une décision, pas reconstruire
son propre calcul de score côté application.

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
    │   ├── rules/                    # IA-4 — Couche 1 : Moteur de règles YAML R01-R12 (v2.0) ✅
    │   ├── anomaly/                  # IA-5 — Couche 2 : Isolation Forest + Z-score ✅
    │   ├── supervised/               # IA-6 — Couche 3 : XGBoost + SHAP ✅
    │   ├── scoring/                  # IA-7 — Cœur du moteur : 3 couches + agrégation + décision ✅
    │   └── scoring_api/              # IA-7 — Service FastAPI (/v1/score, /reload-model, /health, /metrics) ✅
    ├── interfaces/                   # Abstractions Cloud (Kafka/Redis ↔ Kinesis/DynamoDB, registre RDS)
    ├── docs/                         # Dictionnaire des features, contrat d'API du scoring
    ├── notebooks/                    # Validation des Couches 2 et 3 (1 500 abonnés, temps réel)
    ├── tests/                        # 🧪 87 tests unitaires & d'intégration (100% OK) ✅
    ├── run_train_couche2.py          # Script d'entraînement Couche 2 (Isolation Forest)
    ├── run_train_couche3.py          # Script d'entraînement Couche 3 (XGBoost + SHAP)
    ├── docker-compose.yml            # Stack locale (Redpanda, Redis, Postgres, MinIO)
    └── requirements.txt              # Dépendances Python
```

---

## 📊 Avancement des Tâches (IA-1 à IA-10)

| Jalon | Périmètre | Statut |
|---|---|:---:|
| **IA-1** | Simulateur de trafic & attaques (5 types de fraude dont 1 sans swap, légitimes inhabituels ; 1 500 abonnés de référence) | ✅ **Validé** |
| **IA-2** | Dictionnaire de features (12 features temps réel) | ✅ **Validé** |
| **IA-3** | Feature Updater (Welford $O(1)$, fenêtres 1h/24h/7j, sets Redis) | ✅ **Validé** |
| **IA-4** | Couche 1 — Moteur de Règles Expertes (12 règles R01-R12 v2.0, hot-reload) | ✅ **Validé** |
| **IA-5** | Couche 2 — Détection d'anomalies (Isolation Forest + RobustScaler + Z-Score) | ✅ **Validé** |
| **IA-6** | Couche 3 — XGBoost supervisé + SHAP, champion/challenger, table `models` | ✅ **Validé** |
| **IA-7** | Moteur de scoring intégré : service FastAPI, 3 couches, agrégation, seuils 30/70/90, décisions en base, métriques, `/reload-model` sécurisé | ✅ **Terminé en local** (AWS : à faire) |
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

### 5. Entraîner et publier les Couches d'IA dans MinIO (Couches 2 & 3)

```bash
# 1 500 abonnés = volume de référence (500 donne des résultats trop optimistes)
SIM_NB_ABONNES=1500 python run_train_couche2.py   # Isolation Forest → cg-models/anomaly/
SIM_NB_ABONNES=1500 python run_train_couche3.py   # XGBoost + SHAP  → cg-models/xgboost/ (+ table models)
```

### 6. Lancer le service de scoring (IA-7)

```bash
docker compose up -d scoring-api          # charge règles et modèles depuis MinIO au démarrage
curl http://localhost:8000/health
curl -X POST http://localhost:8000/v1/score -H "Content-Type: application/json" \
     -d '{"id_transaction":"TXN-1","id_compte":"CPT-...","horodatage":"2026-07-14T09:19:02+00:00","montant":45000}'
curl -X POST http://localhost:8000/reload-model -H "X-API-Key: change-me-local"
```

Contrat d'API complet (entrées, sorties, agrégation, erreurs) : [`docs/contrat_api_scoring.md`](docs/contrat_api_scoring.md).

### 7. Lancer la suite de tests unitaires & d'intégration (87 tests)

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

Les seuils s'appliquent au **score final agrégé** ; aucune couche ne décide seule.
Chaque décision est enregistrée dans la table `decisions` (PostgreSQL local / RDS).

---

## ⚙️ Variables d'Environnement Clés

| Variable | Valeur Locale | Description |
|---|---|---|
| `ENV` | `local` | Bascule l'infrastructure (`local` ou `aws`) |
| `KAFKA_BOOTSTRAP_SERVERS` | `redpanda:9092` | Broker Kafka / Redpanda |
| `REDIS_HOST` | `redis` | Serveur Redis Feature Store |
| `REDIS_PORT` | `6379` | Port Redis |
| `MINIO_ENDPOINT` | `localhost:19000` | Stockage S3/MinIO local |
| `POSTGRES_DSN` | `postgresql://…:15432/cyberguardian` | Base relationnelle (décisions, modèles, labels) |
| `SCORE_WEIGHT_IF` / `SCORE_WEIGHT_XGB` | `0.1` / `0.9` | Poids de la combinaison Couche 2 / Couche 3 |
| `RULES_SOURCE` | `s3` (service) / `file` | Source des règles au démarrage et au rechargement |
| `RELOAD_API_KEY` | `change-me-local` | Clé de l'endpoint `/reload-model` |
| `METRICS_BACKEND` | `local` | `local` (GET /metrics) ou `cloudwatch` |
| `SIM_NB_ABONNES` | `500` (référence : `1500`) | Volume de la simulation |
| `SEED` | `42` | Seed globale garantissant la reproductibilité |

---

## ⚠️ Limites connues (à traiter avant la mise en production réelle)

1. **Concurrence entre le service de scoring et le Feature Updater.** Le service *lit* le
   profil de l'abonné dans Redis / DynamoDB ; c'est le Feature Updater (IA-3) qui le met à
   jour à partir du flux d'événements. Les deux tournent en parallèle, sans coordination :
   si une transaction est scorée avant que le Feature Updater ait traité le swap SIM ou les
   OTP qui la précèdent, elle est évaluée sur un profil incomplet (swap récent invisible,
   donc signal le plus discriminant absent). Non testé avec les deux services actifs
   simultanément. Pistes : garantir l'ordre de traitement par abonné (même partition),
   exposer l'âge du profil (`profile_age_ms`) et le surveiller, ou appliquer les événements
   SIM/OTP en attente avant le scoring.
2. **Mode AWS non exécuté.** Le passage vers S3, RDS, DynamoDB et CloudWatch est codé et
   piloté par variables d'environnement, mais n'a été testé qu'en local (MinIO,
   PostgreSQL, Redis). Infrastructure ECS Fargate / Terraform non réalisée.
3. **Vol de téléphone déverrouillé** (fraude sans swap SIM) : peu détecté par les trois
   couches — limite acceptée (même appareil, même lieu, pas de swap).
4. **Données simulées** : toutes les performances sont mesurées sur le simulateur ; elles
   devront être revalidées sur du trafic réel.

---

## ⚖️ Principes Fondamentaux

1. **Explicabilité totale** : Chaque blocage produit le nom des règles déclenchées (Couche 1) et les 3 facteurs d'impact SHAP (Couche 3).
2. **Reproductibilité stricte** : `SEED=42` utilisé partout pour garantir des démonstrations identiques.
3. **Zéro verrou propriétaire** : Logique métier 100% agnostique du cloud.

---

*CyberGuardian AI — Système de protection de la finance numérique.*
