# CyberGuardian AI

> Moteur IA de dÃ©tection de fraude **SIM swap** en temps rÃ©el pour le **Mobile Money au SÃ©nÃ©gal**

---

## Le problÃ¨me

Chaque jour au SÃ©nÃ©gal, des fraudeurs convainquent un agent tÃ©lÃ©com de transfÃ©rer le numÃ©ro d'une victime sur une nouvelle carte SIM. Pendant les minutes qui suivent, ils interceptent tous les SMS â€” y compris les codes de sÃ©curitÃ© â€” rÃ©initialisent le PIN et vident le compte mobile money de la victime.

**3 794 infractions de cybercriminalitÃ©** ont Ã©tÃ© recensÃ©es en 2025 au SÃ©nÃ©gal, avec des pertes se comptant en milliards de FCFA Ã  l'Ã©chelle du continent.

---

## La solution

CyberGuardian AI dÃ©tecte cette fraude en temps rÃ©el en croisant deux signaux que seul l'opÃ©rateur peut voir ensemble : les **Ã©vÃ©nements rÃ©seau** (changement de SIM, appareil utilisÃ©, antenne) et les **transactions mobile money**.

Quand la combinaison est suspecte, le systÃ¨me agit **avant que l'argent ne sorte**.

---

## Architecture

### Moteur IA â€” 3 couches

```
Transaction entrante
        â”‚
        â–¼
â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
â”‚  Couche 1 â€” RÃ¨gles expertes (IA-4)  âœ…   â”‚
â”‚  10 rÃ¨gles YAML (R01â€“R10)                 â”‚
â”‚  Rechargement Ã  chaud depuis S3/MinIO     â”‚
â”‚  Score max parmi les rÃ¨gles dÃ©clenchÃ©es   â”‚
â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
                   â”‚
                   â–¼
â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
â”‚  Couche 2 â€” DÃ©tection d'anomalies (IA-5) âœ…â”‚
â”‚  Isolation Forest + z-score Welford       â”‚
â”‚  Formule hybride : IF principal,          â”‚
â”‚  z-score override aux seuils 3Ïƒ et 5Ïƒ    â”‚
â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
                   â”‚
                   â–¼
â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
â”‚  Couche 3 â€” ModÃ¨le supervisÃ© (IA-6)  âœ…  â”‚
â”‚  XGBoost (local) ou SageMaker Training    â”‚
â”‚  scale_pos_weight, CV stratifiÃ©e 5 folds  â”‚
â”‚  SHAP top-3 par transaction               â”‚
â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
                   â”‚
                   â–¼
         Score 0â€“100 â†’ PASS / CHALLENGE / BLOCK
```

### Infrastructure â€” Local â†’ AWS

| Composant | Local | AWS |
|---|---|---|
| Bus d'Ã©vÃ©nements | Redpanda (Kafka) | Kinesis Data Streams |
| Feature store | Redis | DynamoDB |
| Base relationnelle | PostgreSQL | RDS PostgreSQL |
| Stockage objets | MinIO | S3 |
| API de scoring | Docker Compose | ECS Fargate |
| EntraÃ®nement | Local | SageMaker Training Jobs |

> Le code est **agnostique du cloud** â€” les services AWS sont accÃ©dÃ©s via `interfaces/streams.py` et `interfaces/store.py`. Un redÃ©ploiement on-premise chez l'opÃ©rateur reste possible sans rÃ©Ã©criture.

---

## Structure du projet

```
CyberGradianAI/
â”‚
â”œâ”€â”€ cyberguardian/
â”‚   â”‚
â”‚   â”œâ”€â”€ simulator/                    # IA-1 â€” Simulateur de donnÃ©es âœ…
â”‚   â”‚   â”œâ”€â”€ subscribers.py            # 500 abonnÃ©s sÃ©nÃ©galais (seed=42)
â”‚   â”‚   â”œâ”€â”€ profiles.py               # Profils comportementaux â†’ Redis / DynamoDB
â”‚   â”‚   â”œâ”€â”€ scenarios.py              # 7 scÃ©narios (3 fraudes, 4 lÃ©gitimes)
â”‚   â”‚   â”œâ”€â”€ publisher.py              # Publication dans Kafka / Kinesis
â”‚   â”‚   â”œâ”€â”€ main.py                   # Point d'entrÃ©e (batch | replay)
â”‚   â”‚   â””â”€â”€ Dockerfile
â”‚   â”‚
â”‚   â”œâ”€â”€ engine/
â”‚   â”‚   â”œâ”€â”€ __init__.py
â”‚   â”‚   â”œâ”€â”€ features/                 # IA-3 â€” Feature Updater âœ…
â”‚   â”‚   â”‚   â”œâ”€â”€ updater.py            # Logique mÃ©tier pure (Welford, fenÃªtres, sets)
â”‚   â”‚   â”‚   â”œâ”€â”€ handlers.py           # Dispatch topic â†’ store (FeatureHandler)
â”‚   â”‚   â”‚   â”œâ”€â”€ main.py               # Boucle de consommation Kafka persistante
â”‚   â”‚   â”‚   â””â”€â”€ Dockerfile            # Image lÃ©gÃ¨re (requirements-updater.txt)
â”‚   â”‚   â”œâ”€â”€ rules/                    # IA-4 â€” Moteur de rÃ¨gles expertes âœ…
â”‚   â”‚   â”‚   â”œâ”€â”€ rules.yaml            # 10 rÃ¨gles YAML documentÃ©es (R01â€“R10)
â”‚   â”‚   â”‚   â”œâ”€â”€ features.py           # Calcul des features dÃ©rivÃ©es Ã  la volÃ©e
â”‚   â”‚   â”‚   â”œâ”€â”€ engine.py             # Ã‰valuation + rechargement Ã  chaud S3/fichier
â”‚   â”‚   â”‚   â””â”€â”€ __init__.py
â”‚   â”‚   â”œâ”€â”€ anomaly/                  # IA-5 â€” Isolation Forest âœ…
â”‚   â”‚   â”‚   â”œâ”€â”€ dataset.py            # Double filtre NORMAL+label=0, split stratifiÃ©
â”‚   â”‚   â”‚   â”œâ”€â”€ train.py              # IsolationForest + RobustScaler, champion/challenger
â”‚   â”‚   â”‚   â”œâ”€â”€ detector.py           # AnomalyDetector, formule hybride IF + z-score
â”‚   â”‚   â”‚   â”œâ”€â”€ evaluate.py           # AUC-PR, rappel@1%FPR, rapport S3
â”‚   â”‚   â”‚   â””â”€â”€ __init__.py
â”‚   â”‚   â”œâ”€â”€ supervised/               # IA-6 â€” XGBoost supervisÃ© âœ…
â”‚   â”‚   â”‚   â”œâ”€â”€ dataset.py            # Toutes classes, scale_pos_weight, export Parquet
â”‚   â”‚   â”‚   â”œâ”€â”€ train.py              # XGBoost local + SageMaker stub, champion/challenger
â”‚   â”‚   â”‚   â”œâ”€â”€ detector.py           # XGBoostDetector, SHAP top-3, rechargement Ã  chaud
â”‚   â”‚   â”‚   â”œâ”€â”€ evaluate.py           # AUC-PR, SHAP globale, rapport S3
â”‚   â”‚   â”‚   â””â”€â”€ __init__.py
â”‚   â”‚   â””â”€â”€ scoring_api/              # IA-7 â€” API FastAPI + /reload-rules (Ã  faire)
â”‚   â”‚
â”‚   â”œâ”€â”€ interfaces/
â”‚   â”‚   â”œâ”€â”€ streams.py                # Abstraction Kafka â†” Kinesis
â”‚   â”‚   â””â”€â”€ store.py                  # Abstraction Redis/MinIO â†” DynamoDB/S3
â”‚   â”‚
â”‚   â”œâ”€â”€ infra/                        # IA-9 â€” Infrastructure Terraform AWS (Ã  faire)
â”‚   â”‚
â”‚   â”œâ”€â”€ docs/
â”‚   â”‚   â””â”€â”€ dictionnaire_features.md  # IA-2 â€” 12 features documentÃ©es âœ…
â”‚   â”‚
â”‚   â”œâ”€â”€ notebooks/
â”‚   â”‚   â””â”€â”€ 01_EDA_simulateur.ipynb   # EDA Phase 1
â”‚   â”‚
â”‚   â”œâ”€â”€ docker-compose.yml            # Stack locale complÃ¨te
â”‚   â”œâ”€â”€ requirements.txt              # DÃ©pendances Python complÃ¨tes (API + ML)
â”‚   â”œâ”€â”€ requirements-updater.txt      # DÃ©pendances minimales du feature-updater
â”‚   â””â”€â”€ .gitignore
â”‚
â””â”€â”€ README.md
```

---

## Avancement

| TÃ¢che | Description | Statut |
|---|---|---|
| IA-1 | Simulateur (500 abonnÃ©s, 7 scÃ©narios, 22 793 Ã©vÃ©nements) | âœ… Fait |
| IA-2 | Dictionnaire de features (12 features documentÃ©es) | âœ… Fait |
| IA-3 | Feature Updater (Welford, fenÃªtres glissantes, sets) | âœ… Fait + testÃ© Docker |
| IA-4 | Moteur de rÃ¨gles YAML (10 rÃ¨gles R01â€“R10, rechargeable S3) | âœ… Fait + 46/46 tests |
| IA-5 | DÃ©tection d'anomalies (Isolation Forest + z-scores) | âœ… Fait + 34/34 tests |
| IA-6 | XGBoost (local + SageMaker stub) + SHAP + champion/challenger | âœ… Fait + 47/47 tests |
| IA-7 | API FastAPI scoring + `/reload-rules` sur ECS Fargate | â³ Ã€ faire |
| IA-8 | Harnais d'Ã©valuation + CI GitHub Actions | â³ Ã€ faire |
| IA-9 | Socle Terraform AWS (Kinesis, DynamoDB, S3, ECSâ€¦) | â³ Ã€ faire |
| IA-10 | Fiche technique 2 pages pour le jury | â³ Ã€ faire |

---

## DonnÃ©es simulÃ©es

### Topics Kafka / Kinesis

| Topic | Champs clÃ©s | Label |
|---|---|---|
| `sim-events` | `id_compte`, `ancien_iccid`, `nouveau_iccid`, `device_id`, `antenne`, `horodatage` | `1` (fraude) |
| `otp-events` | `id_compte`, `motif`, `antenne`, `horodatage` | â€” |
| `transactions` | `id_compte`, `montant`, `solde_avant`, `solde_apres`, `device_id`, `antenne`, `horodatage` | `0` ou `1` |

### ScÃ©narios simulÃ©s

| ScÃ©nario | Description | Label |
|---|---|---|
| `NORMAL` | Transaction normale dans les habitudes de l'abonnÃ© | 0 |
| `SIM_SWAP_SIMPLE` | Swap SIM + 1 gros transfert vers bÃ©nÃ©ficiaire inconnu | 1 |
| `SIM_SWAP_CASCADE` | Swap SIM + vidage en cascade (3 Ã  7 transferts rapides) | 1 |
| `PIC_OTP` | Pic de demandes OTP (5 Ã  10) suivi d'un transfert frauduleux | 1 |
| `SWAP_LEGITIME` | Changement de SIM normal (mÃªme device, antenne locale) | 0 |
| `NOUVEAU_DEVICE_LEGITIME` | Nouveau tÃ©lÃ©phone, comportement habituel sinon | 0 |
| `GROS_MONTANT_LEGITIME` | Grosse transaction ponctuelle, device et bÃ©nÃ©ficiaire connus | 0 |
| `VOYAGE_LEGITIME` | Transaction depuis une autre rÃ©gion, profil normal sinon | 0 |

### Politique de scoring

| Score | DÃ©cision | Action |
|---|---|---|
| 0 â€“ 29 | `PASS` | Transaction laissÃ©e passer |
| 30 â€“ 69 | `CHALLENGE` | VÃ©rification supplÃ©mentaire |
| 70 â€“ 89 | `BLOCK` | Transaction bloquÃ©e + alerte analyste |
| 90 â€“ 100 | `BLOCK` | Blocage immÃ©diat + prioritÃ© haute |

---

## DÃ©marrage rapide â€” tester le pipeline complet

### PrÃ©requis

- Docker Desktop avec WSL2 activÃ© (Windows)
- Python 3.11+
- Git

### 1. Cloner le repo

```bash
git clone https://github.com/El-khalilDione99/CyberGradianAI.git
cd CyberGradianAI/cyberguardian
```

### 2. DÃ©marrer l'infrastructure

Lance Redpanda, Redis, PostgreSQL et MinIO. Les services `redpanda-init` et `minio-init` crÃ©ent automatiquement les topics Kafka, les buckets MinIO **et uploadent `rules.yaml` dans MinIO** au dÃ©marrage.

```bash
docker compose up redpanda redis postgres minio redpanda-init minio-init -d
```

VÃ©rifier que tout est healthy :

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

Le simulateur publie ~23 000 Ã©vÃ©nements dans les 3 topics Kafka. Le Feature Updater les consomme et met Ã  jour les 500 profils dans Redis.

### 5. VÃ©rifier le pipeline

```bash
# 500 profils crÃ©Ã©s dans Redis
docker exec cg_redis redis-cli DBSIZE

# Inspecter un profil mis Ã  jour
docker exec cg_redis redis-cli GET "profile:CPT-<hash>"

# Logs du Feature Updater (doit afficher ~28 msg/s, 0 erreur)
docker logs cg_feature_updater --tail 20
```

VÃ©rifier que `rules.yaml` est bien stockÃ© dans MinIO :

```bash
docker run --rm --network cyberguardian_default \
  --entrypoint sh minio/mc:latest -c \
  "mc alias set local http://minio:9000 minioadmin minioadmin123 --api S3v4 > /dev/null \
   && mc ls local/cg-models/ --recursive"
```

RÃ©sultat attendu :
```
[2026-08-19 13:06:20 UTC] 7.4KiB STANDARD rules/rules.yaml
```

Si le fichier est absent (premier dÃ©marrage avant la crÃ©ation des rÃ¨gles), l'uploader manuellement :

```bash
docker run --rm --network cyberguardian_default \
  -v "$(pwd)/engine/rules:/rules:ro" \
  --entrypoint sh minio/mc:latest -c \
  "mc alias set local http://minio:9000 minioadmin minioadmin123 --api S3v4 > /dev/null \
   && mc cp /rules/rules.yaml local/cg-models/rules/rules.yaml \
   && echo 'Upload OK' \
   && mc ls local/cg-models/ --recursive"
```

> **Note Windows** : remplacer `$(pwd)` par le chemin absolu, ex :
> `-v "c:\Users\adjie\...\cyberguardian\engine\rules:/rules:ro"`

VÃ©rifier le contenu du fichier uploadÃ© :

```bash
docker run --rm --network cyberguardian_default \
  --entrypoint sh minio/mc:latest -c \
  "mc alias set local http://minio:9000 minioadmin minioadmin123 --api S3v4 > /dev/null \
   && mc cat local/cg-models/rules/rules.yaml | head -5"
```

RÃ©sultat attendu :
```
# CyberGuardian AI â€” Moteur de rÃ¨gles (Couche 1) â€” IA-4
# Version: 1.0 (baseline complÃ¨te â€” 10/10 rÃ¨gles finalisÃ©es)
```

### 6. Tester le moteur de rÃ¨gles en Python

Sans infrastructure, la logique des rÃ¨gles est testable directement :

```bash
cd cyberguardian
python -c "
from engine.rules import RuleEngine, compute_features
from datetime import datetime, timezone

engine = RuleEngine()
print(f'Moteur chargÃ© : {engine.rules_count} rÃ¨gles')

# ScÃ©nario SIM_SWAP_SIMPLE
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
print(f'RÃ¨gles : {[m.rule_id for m in result.matches]}')
print(f'DÃ©cision : BLOCK' if result.score >= 70 else 'PASS/CHALLENGE')
"
```

RÃ©sultat attendu : **score 93, rÃ¨gles R01 R02 R03 R04 R05 R06 R07 R09 R10 dÃ©clenchÃ©es**.

### 7. VÃ©rifier les donnÃ©es Kafka

```bash
docker exec cg_redpanda rpk topic consume transactions \
  --brokers localhost:9092 --num 5 --offset start --format "%v\n"
```

---

## Feature Updater â€” fonctionnement (IA-3)

Le Feature Updater Ã©coute en permanence les 3 topics Kafka et met Ã  jour les profils abonnÃ©s dans Redis. C'est la mÃ©moire vivante du systÃ¨me â€” sans lui, le moteur de rÃ¨gles n'a pas de contexte comportemental.

```
Kafka topics                Redis (profil abonnÃ©)
â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€              â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
transactions  â”€â”€â”           nb_transactions, montant_moyen (Welford)
sim-events    â”€â”€â”¼â”€â”€â–º FU â”€â”€â–º ts_dernier_swap, nb_swaps_30j, iccid_actuel
otp-events    â”€â”€â”˜           nb_otp_1h (fenÃªtre glissante), nb_otp_24h
                            nb_tx_1h / nb_tx_24h / nb_tx_7j
                            devices_connus, beneficiaires_connus
                            solde, antennes_connues
```

**Algorithme de Welford** : moyenne et variance en O(1) sans stocker l'historique.
**Performance mesurÃ©e** : ~28 msg/s, 0 erreur sur 22 793 Ã©vÃ©nements simulÃ©s.

---

## Moteur de rÃ¨gles â€” fonctionnement (IA-4)

Le moteur de rÃ¨gles est la Couche 1 du scoring. Il Ã©value 10 rÃ¨gles YAML contre les features calculÃ©es Ã  la volÃ©e et retourne le score le plus Ã©levÃ© parmi les rÃ¨gles dÃ©clenchÃ©es.

### Les 10 rÃ¨gles (R01â€“R10)

| RÃ¨gle | Nom | Logique | Score | Statut seuil |
|---|---|---|---|---|
| R01 | SIM swap rÃ©cent + montant important | `hours_since_swap < 1h` ET `amount_ratio > 3` | 92 | EDA v3 |
| R02 | Nouveau device + montant important | `new_device` ET `amount_ratio > 3` | 78 | EDA v3 |
| R03 | Nouveau device aprÃ¨s swap SIM | `new_device` ET `hours_since_swap < 2h` | 93 | EDA v3 |
| R04 | Pic d'OTP | `otp_count_1h >= 3` | 72 | Baseline v1.0 |
| R05 | VÃ©locitÃ© excessive | `nb_tx_1h >= 4` | 70 | Baseline v1.0 |
| R06 | Montant trÃ¨s supÃ©rieur Ã  l'habitude | `amount_ratio > 5` | 68 | Baseline v1.0 |
| R07 | Changement gÃ©ographique | `is_roaming == True` | 60 | Baseline v1.0 |
| R08 | Cascade de transferts | `nb_beneficiaires_1h >= 3` ET `nb_tx_1h >= 3` | 90 | Baseline v1.0 |
| R09 | Nouveau bÃ©nÃ©ficiaire + montant Ã©levÃ© | `new_beneficiary` ET `amount_ratio > 3` | 78 | EDA v3 |
| R10 | Accumulation de signaux faibles | 3+ signaux faibles simultanÃ©s | 88 | Baseline v1.0 |

> R01, R02, R03, R09 ont leurs seuils validÃ©s par l'EDA v3. Les autres (R04â€“R08, R10) seront recalibrÃ©s via le harnais IA-8 dÃ¨s que du volume de scoring rÃ©el est disponible.

### Features calculÃ©es Ã  la volÃ©e

Ces features ne sont pas stockÃ©es dans Redis â€” elles sont dÃ©rivÃ©es au moment du scoring depuis l'Ã©vÃ©nement courant + le profil abonnÃ© :

| Feature | Formule |
|---|---|
| `amount_ratio` | `montant / montant_moyen_habituel` |
| `hours_since_sim_swap` | `(now - ts_dernier_swap).total_seconds() / 3600` |
| `new_device` | `device_id not in devices_connus` |
| `new_beneficiary` | `id_beneficiaire not in beneficiaires_connus` |
| `is_roaming` | `antenne != antenne_domicile` |
| `otp_count_1h` | `nb_otp_1h` (depuis le profil Redis) |
| `zscore_montant` | `(montant - montant_moyen) / ecart_type` |

### Rechargement Ã  chaud

En local : le moteur lit `engine/rules/rules.yaml` (montÃ© comme volume).
En AWS : si le fichier local est absent, le moteur tÃ©lÃ©charge `cg-models/rules/rules.yaml` depuis S3. L'endpoint `/reload-rules` (IA-7, Ã  faire) dÃ©clenchera ce rechargement sans redÃ©marrage du conteneur.

---

## DÃ©tection d'anomalies â€” fonctionnement (IA-5)

La Couche 2 dÃ©tecte les comportements inhabituels sans labels â€” elle apprend le trafic normal et signale tout ce qui s'en Ã©carte.

**Isolation Forest** entraÃ®nÃ© uniquement sur les transactions `type_scenario=NORMAL` (double filtre). Score normalisÃ© 0-100 : 0 = comportement typique, 100 = forte anomalie.

**Formule hybride** (dÃ©cidÃ©e en amont, pas improvisÃ©e au code) :
```
score_couche2 = score_IF
  si zscore_montant > 3.0  â†’  score = max(score_IF, 75)   # override soft
  si zscore_montant > 5.0  â†’  score = max(score_IF, 90)   # override hard
  (ignorÃ© si nb_transactions < 10 â€” std Welford instable)
```

**15 features** (hours_since_sim_swap exclu â€” `inf` pollue RobustScaler).
**Split stratifiÃ© par abonnÃ©** : garantit qu'un abonnÃ© ne soit pas Ã  la fois dans train ET test.
**Versionnage** : `cg-models/anomaly/production/current.json` pointe vers le champion.

---

## XGBoost supervisÃ© â€” fonctionnement (IA-6)

La Couche 3 apprend directement les patterns de fraude Ã  partir des labels.

**17 features** (15 de IA-5 + `hours_since_sim_swap` gÃ©rÃ© par XGBoost + `solde`).
`hours_since_sim_swap = inf` â†’ remplacÃ© par `9999.0` (les arbres tolÃ¨rent les valeurs extrÃªmes).

**scale_pos_weight = n_lÃ©gitimes / n_fraudes** : compense le dÃ©sÃ©quilibre sans SMOTE.
Avec ~2% de fraudes â†’ `scale_pos_weight â‰ˆ 50`, chaque fraude pÃ¨se 50Ã— plus dans la perte.

**Validation croisÃ©e stratifiÃ©e 5 folds** : chaque fold contient la mÃªme proportion de fraudes.
MÃ©trique d'optimisation : **AUC-PR** (pas accuracy, pas AUC-ROC).

**Deux modes d'entraÃ®nement** :
- Local (`ENV=local`) : XGBoost Python direct, quelques secondes
- SageMaker (`ENV=aws + USE_SAGEMAKER=true`) : Training Job ml.m5.large, ~5 min, sans endpoint d'infÃ©rence permanent

**SHAP top-3** : chaque prÃ©diction retourne les 3 features qui ont le plus contribuÃ©, avec direction (`fraud` ou `legitimate`). Exemple :
```json
[
  {"feature": "new_device",   "value": 1.0,  "shap": 0.42, "direction": "fraud"},
  {"feature": "amount_ratio", "value": 9.25, "shap": 0.38, "direction": "fraud"},
  {"feature": "is_roaming",   "value": 1.0,  "shap": 0.21, "direction": "fraud"}
]
```

**Champion/challenger** basÃ© sur AUC-PR :
- Pas de champion â†’ promotion automatique
- Challenger AUC-PR > champion + 0.005 â†’ promotion
- Historique des 20 derniers entraÃ®nements dans `xgboost/production/history.json`

**Versionnage S3** :
```
cg-models/xgboost/
  xgboost_<YYYYMMDD_HHmmss>.pkl
  xgboost_<YYYYMMDD_HHmmss>_metrics.json
  production/
    current.json    â† champion actif
    history.json    â† 20 derniers entraÃ®nements
```

---

## Tester la Couche 3 (IA-6) en local

Les tests sont dans le dossier `tests/`. Pas besoin de Docker ni de Redis â€” tout tourne en mÃ©moire.

### PrÃ©requis

```bash
pip install pytest
pip install -r requirements.txt
```

### Tester la Couche 3 seule

VÃ©rifie le dataset supervisÃ©, l'entraÃ®nement XGBoost, les prÃ©dictions et l'Ã©valuation AUC-PR.

```bash
cd cyberguardian
python -m pytest tests/test_ia6.py -v
```

RÃ©sultat attendu :
```
tests/test_ia6.py::TestDataset::test_features_count                     PASSED
tests/test_ia6.py::TestDataset::test_hours_since_swap_no_inf            PASSED
tests/test_ia6.py::TestDataset::test_fraudes_dans_train_et_test         PASSED
tests/test_ia6.py::TestDataset::test_scale_pos_weight_superieur_a_1     PASSED
tests/test_ia6.py::TestDataset::test_pas_de_nan                         PASSED
tests/test_ia6.py::TestTrain::test_model_key_non_vide                   PASSED
tests/test_ia6.py::TestTrain::test_champion_promu                       PASSED
tests/test_ia6.py::TestTrain::test_auc_pr_dans_intervalle               PASSED
tests/test_ia6.py::TestTrain::test_current_json_cree                    PASSED
tests/test_ia6.py::TestTrain::test_history_json_cree                    PASSED
tests/test_ia6.py::TestTrain::test_feature_importance_presente          PASSED
tests/test_ia6.py::TestTrain::test_cv_auc_pr_present                    PASSED
tests/test_ia6.py::TestTrain::test_scale_pos_weight_dans_metrics        PASSED
tests/test_ia6.py::TestPredict::test_is_ready                           PASSED
tests/test_ia6.py::TestPredict::test_score_fraude_superieur_a_normal    PASSED
tests/test_ia6.py::TestPredict::test_score_fraude_bloque                PASSED
tests/test_ia6.py::TestPredict::test_score_normal_passe                 PASSED
tests/test_ia6.py::TestPredict::test_probability_dans_intervalle        PASSED
tests/test_ia6.py::TestPredict::test_shap_top3_non_vide                 PASSED
tests/test_ia6.py::TestPredict::test_shap_contient_bons_champs          PASSED
tests/test_ia6.py::TestPredict::test_mode_degrade_score_zero            PASSED
tests/test_ia6.py::TestEvaluate::test_auc_pr_dans_intervalle            PASSED
tests/test_ia6.py::TestEvaluate::test_score_fraudes_superieur_a_legitimess PASSED
tests/test_ia6.py::TestEvaluate::test_confusion_4_seuils                PASSED
tests/test_ia6.py::TestEvaluate::test_shap_importance_presente          PASSED
tests/test_ia6.py::TestEvaluate::test_rapport_publie_dans_store         PASSED
26 passed in ~20s
```

### Ce que ces tests vÃ©rifient

| Classe | Ce qui est testÃ© |
|---|---|
| `TestDataset` | 17 features, pas d'inf/NaN, fraudes dans train ET test, scale_pos_weight > 1 |
| `TestTrain` | champion promu, current.json + history.json crÃ©Ã©s, AUC-PR dans [0,1], feature_importance prÃ©sente |
| `TestPredict` | score fraude > score normal, fraude â†’ BLOCK, normal â†’ PASS, SHAP top-3 avec direction, mode dÃ©gradÃ© = 0 |
| `TestEvaluate` | AUC-PR valide, sÃ©paration fraudes/lÃ©gitimes, 4 seuils de confusion, SHAP globale, rapport S3 |

---

## Tester le pipeline des 3 couches ensemble

Charge les 3 couches simultanÃ©ment et les fait scorer le mÃªme Ã©vÃ©nement.

```bash
cd cyberguardian
python -m pytest tests/test_pipeline.py -v
```

RÃ©sultat attendu :
```
tests/test_pipeline.py::TestChargement::test_couche1_chargee             PASSED
tests/test_pipeline.py::TestChargement::test_couche2_prete               PASSED
tests/test_pipeline.py::TestChargement::test_couche3_prete               PASSED
tests/test_pipeline.py::TestFraude::test_couche1_declenche_regles        PASSED
tests/test_pipeline.py::TestFraude::test_couche1_score_eleve             PASSED
tests/test_pipeline.py::TestFraude::test_couche3_score_eleve             PASSED
tests/test_pipeline.py::TestFraude::test_score_final_bloque              PASSED
tests/test_pipeline.py::TestFraude::test_shap_direction_fraude           PASSED
tests/test_pipeline.py::TestNormal::test_couche1_aucune_regle            PASSED
tests/test_pipeline.py::TestNormal::test_couche3_score_bas               PASSED
tests/test_pipeline.py::TestNormal::test_couche3_passe                   PASSED
tests/test_pipeline.py::TestDiscrimination::test_couche3_fraude_superieur_a_normal PASSED
tests/test_pipeline.py::TestDiscrimination::test_couche1_fraude_superieur_a_normal PASSED
13 passed in ~20s
```

### Ce que ces tests vÃ©rifient

| Classe | Ce qui est testÃ© |
|---|---|
| `TestChargement` | Couche 1 a 10 rÃ¨gles, Couche 2 is_ready, Couche 3 is_ready |
| `TestFraude` | RÃ¨gles dÃ©clenchÃ©es, score C1 â‰¥ 70, score C3 â‰¥ 70, score final â‰¥ 70 â†’ BLOCK, SHAP pointe vers "fraud" |
| `TestNormal` | Couche 1 score = 0 (aucune rÃ¨gle), Couche 3 score < 30 â†’ PASS |
| `TestDiscrimination` | Score fraude > score normal pour C1 ET C3 |

### Tout lancer en une commande

```bash
cd cyberguardian
python -m pytest tests/ -v
```

RÃ©sultat attendu : **39 passed** en ~30 secondes.

> Les warnings `DeprecationWarning: datetime.datetime.utcnow()` viennent de la librairie `botocore` (AWS SDK), pas de notre code. Ils peuvent Ãªtre ignorÃ©s.
---

| Service | Port hÃ´te | Interface |
|---|---|---|
| Redpanda (Kafka) | `19092` | â€” |
| Redis | `16379` | â€” |
| PostgreSQL | `15432` | â€” |
| MinIO (API) | `19000` | â€” |
| MinIO (Console) | `19002` | [http://localhost:19002](http://localhost:19002) |

> Identifiants MinIO : `minioadmin` / `minioadmin123`

---

## Variables d'environnement clÃ©s

| Variable | Valeur locale | Description |
|---|---|---|
| `ENV` | `local` | `local` ou `aws` â€” bascule les interfaces |
| `KAFKA_BOOTSTRAP_SERVERS` | `redpanda:9092` | Broker Kafka / Kinesis endpoint |
| `REDIS_HOST` | `redis` | Host Redis / DynamoDB endpoint |
| `REDIS_PORT` | `6379` | Port Redis |
| `RULES_PATH` | `/app/engine/rules/rules.yaml` | Chemin du fichier de rÃ¨gles (volume local) |
| `RULES_BUCKET` | `cg-models` | Bucket S3/MinIO contenant les rÃ¨gles |
| `RULES_S3_KEY` | `rules/rules.yaml` | ClÃ© S3 du fichier de rÃ¨gles |
| `SCORE_THRESHOLD_LOW` | `30` | Seuil PASS â†’ CHALLENGE |
| `SCORE_THRESHOLD_MED` | `70` | Seuil CHALLENGE â†’ BLOCK |
| `SCORE_THRESHOLD_HIGH` | `90` | Seuil BLOCK prioritaire |
| `FU_BATCH_SIZE` | `100` | Taille des lots Kafka pour le feature updater |
| `SEED` | `42` | Seed globale â€” dÃ©mo rejouable Ã  l'identique |
| `IF_N_ESTIMATORS` | `200` | Nombre d'arbres Isolation Forest |
| `IF_CONTAMINATION` | `0.02` | Proportion de fraudes estimÃ©e (2%) |
| `Z_OVERRIDE_SOFT` | `3.0` | z-score â†’ plancher score couche 2 Ã  75 |
| `Z_OVERRIDE_HARD` | `5.0` | z-score â†’ plancher score couche 2 Ã  90 |
| `XGB_N_ESTIMATORS` | `300` | Nombre d'arbres XGBoost |
| `XGB_MAX_DEPTH` | `6` | Profondeur maximale des arbres |
| `XGB_MIN_IMPROVEMENT` | `0.005` | Seuil AUC-PR pour promouvoir un challenger |
| `USE_SAGEMAKER` | `false` | `true` pour entraÃ®ner via SageMaker Training Job |
| `SAGEMAKER_ROLE_ARN` | â€” | ARN du rÃ´le IAM SageMaker (AWS uniquement) |

> Aucune clÃ© AWS statique dans le code. En production : rÃ´les IAM attachÃ©s aux tÃ¢ches ECS et OIDC pour la CI.

---

## Jalons du projet

| Jalon | Date | Objectif | Statut |
|---|---|---|---|
| J1 | 26 juillet 2026 | Flux simulÃ© dans Kinesis, profil mis Ã  jour dans DynamoDB | âœ… ValidÃ© en local |
| J2 | 23 aoÃ»t 2026 | DÃ©mo interne bout-en-bout sur AWS, fraude dÃ©tectÃ©e et visible | â³ En cours |
| J3 | 20 septembre 2026 | ModÃ¨les et infrastructure demo gelÃ©s | â³ |
| J4 | 11 octobre 2026 | ReproductibilitÃ© vÃ©rifiÃ©e (dÃ©mo rejouÃ©e 3 fois) | â³ |
| DÃ©mo finale | Mi-octobre 2026 | DÃ©mo live devant le jury | â³ |

> **RÃ¨gle projet** : si on prend du retard, on rÃ©duit le pÃ©rimÃ¨tre â€” on ne dÃ©cale jamais la date.

---

## Principes non nÃ©gociables

- **Score mesurable** â€” on vend un score 0-100, pas une prÃ©diction magique
- **ExplicabilitÃ©** â€” chaque dÃ©cision est justifiable (rÃ¨gles nommÃ©es, SHAP)
- **Agnosticisme cloud** â€” aucun appel boto3 dans la logique mÃ©tier, tout passe par `interfaces/`
- **Seeds fixes** â€” `SEED=42` partout, une dÃ©mo doit Ãªtre rejouable Ã  l'identique
- **Pas de MSK ni endpoint SageMaker permanent** â€” les deux piÃ¨ges qui brÃ»lent les crÃ©dits AWS
- **TraÃ§abilitÃ© des modÃ¨les** â€” tout modÃ¨le sÃ©rialisÃ© est versionnÃ© dans S3 avec ses mÃ©triques

---

## HonnÃªtetÃ© sur les donnÃ©es

Le systÃ¨me tourne sur des **donnÃ©es 100% simulÃ©es**, rÃ©alistes mais fictives.
C'est assumÃ© et documentÃ©. L'Ã©tape suivante avec Sonatel sera un pilote sur donnÃ©es rÃ©elles anonymisÃ©es, en mode observation.

---

## Stack technique

- **Langage** : Python 3.11+
- **ML** : scikit-learn, XGBoost, SHAP
- **API** : FastAPI, uvicorn
- **Streaming** : kafka-python (local), boto3 Kinesis (AWS)
- **Feature store** : redis-py (local), boto3 DynamoDB (AWS)
- **Infra** : Docker Compose (local), Terraform + AWS ECS Fargate (cloud)
- **RÃ©gion AWS** : eu-west-3 (Paris)

---

*CyberGuardian AI â€” AoÃ»t 2026*
