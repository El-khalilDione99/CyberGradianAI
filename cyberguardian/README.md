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
│   │   │
│   │   ├── rules/                    # IA-4 — Couche 1 : Moteur de règles expertes ✅
│   │   │   ├── rules.yaml            # 10 règles YAML documentées (R01–R10)
│   │   │   ├── features.py           # Calcul des features dérivées à la volée
│   │   │   └── engine.py             # Évaluation + rechargement à chaud S3/fichier
│   │   │
│   │   ├── anomaly/                  # IA-5 — Couche 2 : Isolation Forest + Z-score ✅
│   │   │   ├── dataset.py            # Construction dataset d'entraînement
│   │   │   ├── train.py              # Entraînement et versionnage MinIO/S3
│   │   │   ├── evaluate.py           # Harnais d'évaluation (AUC-PR, Recall@1%)
│   │   │   └── detector.py           # Predictor hybride IF + Z-scores
│   │   │
│   │   ├── supervised/               # IA-6 — Couche 3 : XGBoost + SHAP ✅
│   │   │   ├── dataset.py            # Dataset supervisé + scale_pos_weight
│   │   │   ├── train.py              # XGBoost CV 5-folds + promotion champion
│   │   │   ├── evaluate.py           # Explicabilité globale SHAP + métriques
│   │   │   └── detector.py           # Scoring supervisé + Top-3 SHAP
│   │   │
│   │   └── scoring_api/              # IA-7 — API FastAPI de scoring temps réel (à faire)
│   │
│   ├── interfaces/
│   │   ├── streams.py                # Abstraction Kafka ↔ Kinesis
│   │   └── store.py                  # Abstraction Redis/MinIO ↔ DynamoDB/S3
│   │
│   ├── tests/                        # 🧪 Suites de tests unitaires (60/60 OK) ✅
│   │   ├── test_simulator.py         # Validation du simulateur (IA-1)
│   │   ├── test_updater.py           # Validation du Feature Updater (IA-3)
│   │   ├── test_rules.py             # Validation Couche 1 (IA-4)
│   │   ├── test_anomaly.py           # Validation Couche 2 (IA-5)
│   │   ├── test_ia6.py               # Validation Couche 3 (IA-6)
│   │   └── test_pipeline.py          # Validation Pipeline d'intégration (IA-7)
│   │
│   ├── run_train_couche2.py          # Script d'entraînement Couche 2 (IA-5)
│   ├── run_train_couche3.py          # Script d'entraînement Couche 3 (IA-6)
│   ├── docker-compose.yml            # Stack locale complète (6 services)
│   ├── requirements.txt              # Dépendances Python complètes (API + ML)
│   ├── requirements-updater.txt      # Dépendances minimales du feature-updater
│   ├── .env.example                  # Modèle des variables d'environnement
│   └── .gitignore
│
└── README.md
```

---

## Avancement

| Tâche | Description | Statut |
|---|---|---|
| IA-1 | Simulateur (500 abonnés, 7 scénarios, 23 934 événements) | ✅ Fait |
| IA-2 | Dictionnaire de features (12 features documentées) | ✅ Fait |
| IA-3 | Feature Updater (Welford, fenêtres glissantes, sets) | ✅ Fait + testé Docker |
| IA-4 | Moteur de règles YAML (10 règles R01–R10, rechargeable S3) | ✅ Fait + validé |
| IA-5 | Détection d'anomalies (Isolation Forest + z-scores) | ✅ Fait + validé |
| IA-6 | XGBoost supervisé + Explicabilité SHAP | ✅ Fait + validé |
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

Ce guide démarre toute la stack locale, lance le Feature Updater, fait tourner le simulateur, entraîne les 3 couches IA et vérifie que tout fonctionne.

> **Prérequis** : Docker Desktop avec WSL2 activé (Windows), Python 3.11+, Git.

---

### Étape 1 — Cloner le repo

```bash
git clone https://github.com/El-khalilDione99/CyberGradianAI.git
cd CyberGradianAI/cyberguardian
```

---

### Étape 2 — Démarrer l'infrastructure

Lance Redpanda (Kafka), Redis, PostgreSQL et MinIO. Les services `redpanda-init` et `minio-init` créent automatiquement les topics Kafka, les buckets MinIO **et uploadent `rules.yaml` dans MinIO** au démarrage.

```bash
docker compose up redpanda redis postgres minio redpanda-init minio-init -d
```

Vérifier que les services sont prêts :

```bash
docker compose ps
```

Résultat attendu :
```
NAME               STATUS
cg_redpanda        Up X minutes (healthy)
cg_redis           Up X minutes (healthy)
cg_postgres        Up X minutes (healthy)
cg_minio           Up X minutes (healthy)
cg_minio_init      Exited (0)   ← normal
cg_redpanda_init   Exited (0)   ← normal
```

---

### Étape 3 — Construire et lancer le Feature Updater (IA-3)

Le Feature Updater écoute Kafka et met à jour les profils abonnés dans Redis en temps réel.

```bash
# Construire l'image (première fois ~2-3 min)
docker compose build feature-updater

# Lancer en arrière-plan
docker run -d --name cg_feature_updater \
  --network cyberguardian_default \
  -e ENV=local \
  -e KAFKA_BOOTSTRAP_SERVERS=redpanda:9092 \
  -e REDIS_HOST=redis \
  -e REDIS_PORT=6379 \
  cyberguardian-feature-updater
```

Vérifier qu'il est démarré :

```bash
docker logs cg_feature_updater --tail 5
```

---

### Étape 4 — Lancer le simulateur

Génère 500 abonnés et publie ~23 000 événements dans les 3 topics Kafka.
Le Feature Updater les consomme au fur et à mesure et met à jour Redis.

```bash
docker compose --profile simulator run --rm simulator --mode batch --reset-profiles
```

Durée : ~20 secondes. Résultat attendu :
```
Simulation 30 jours planifiée :
  Scénarios :  22793  (fraudes=220, légitimes=22573)
  Événements:  23934
Publication terminée.
```

---

### Étape 5 — Vérifier Redis et Kafka

```bash
# 500 profils doivent être dans Redis
docker exec cg_redis redis-cli DBSIZE

# Inspecter un profil réel
docker exec cg_redis redis-cli GET "profile:CPT-7e16cdf07c7a"

# Logs du Feature Updater (doit afficher ~300 à 530+ msg/s, 0 erreur)
docker logs cg_feature_updater --tail 10
```

---

### Étape 6 — Entraîner la Couche 1 (IA-4 — Règles)

La Couche 1 ne nécessite pas d'entraînement — les règles sont dans `engine/rules/rules.yaml`.
Vérifier qu'elles sont bien dans MinIO :

```bash
docker run --rm --network cyberguardian_default \
  --entrypoint sh minio/mc:latest -c \
  "mc alias set local http://minio:9000 minioadmin minioadmin123 --api S3v4 > /dev/null \
   && mc cat local/cg-models/rules/rules.yaml | head -5"
```

Résultat attendu :
```
# CyberGuardian AI — Moteur de règles (Couche 1) — IA-4
# Version: 1.0 (baseline complète — 10/10 règles finalisées)
```

Tester la Couche 1 directement :

```bash
python -c "
import sys; sys.path.insert(0, '.')
from engine.rules.engine import RuleEngine
e = RuleEngine()
print(f'Couche 1 OK — {e.rules_count} regles chargees depuis {e.loaded_from}')
"
```

---

### Étape 7 — Entraîner la Couche 2 (IA-5 — Isolation Forest)

Lit les profils Redis, construit le dataset, entraîne l'Isolation Forest et sauvegarde le modèle dans MinIO.

```bash
python run_train_couche2.py
```

---

### Étape 8 — Entraîner la Couche 3 (IA-6 — XGBoost)

Lit les profils Redis, entraîne XGBoost avec `scale_pos_weight`, génère les métriques SHAP et sauvegarde dans MinIO.

```bash
python run_train_couche3.py
```

---

### Étape 9 — Vérifier les modèles dans MinIO

```bash
docker run --rm --network cyberguardian_default \
  --entrypoint sh minio/mc:latest -c \
  "mc alias set local http://minio:9000 minioadmin minioadmin123 --api S3v4 > /dev/null \
   && mc ls local/cg-models/ --recursive"
```

Résultat attendu :
```
rules/rules.yaml
anomaly/isolation_forest_<timestamp>.pkl
anomaly/isolation_forest_<timestamp>_metrics.json
anomaly/production/current.json
xgboost/xgboost_<timestamp>.pkl
xgboost/xgboost_<timestamp>_metrics.json
xgboost/production/current.json
xgboost/production/history.json
```

---

### Étape 10 — Tester le scoring avec les 3 couches

Charge les modèles depuis MinIO + les profils depuis Redis, score un événement réel.

```bash
python -c "
import sys, os, json
sys.path.insert(0, '.')
os.environ['REDIS_HOST']       = 'localhost'
os.environ['REDIS_PORT']       = '16379'
os.environ['MINIO_ENDPOINT']   = 'localhost:19000'
os.environ['MINIO_ACCESS_KEY'] = 'minioadmin'
os.environ['MINIO_SECRET_KEY'] = 'minioadmin123'

import redis as _redis
from datetime import datetime, timezone, timedelta
from engine.rules.engine        import RuleEngine
from engine.anomaly.detector    import AnomalyDetector
from engine.supervised.detector import XGBoostDetector

# Charger les 3 couches
c1 = RuleEngine()
c2 = AnomalyDetector()
c3 = XGBoostDetector()
print(f'Couche 1 — {c1.rules_count} regles | Couche 2 — {c2.is_ready} v{c2.version} | Couche 3 — {c3.is_ready} v{c3.version}')

# Lire un vrai profil depuis Redis
r     = _redis.Redis(host='localhost', port=16379, decode_responses=True)
key   = r.keys('profile:*')[0]
profil = json.loads(r.get(key))
print(f'Profil reel : {profil[\"id_compte\"]} | segment={profil.get(\"segment\")} | nb_tx={profil.get(\"nb_transactions\")}')

# Simuler un SIM_SWAP sur ce compte
ts_swap = datetime.now(timezone.utc) - timedelta(minutes=15)
event = {
    'id_compte':       profil['id_compte'],
    'horodatage':      datetime.now(timezone.utc).isoformat(),
    'montant':         profil.get('montant_moyen_habituel', 10000) * 5,
    'device_id':       'DEV-ATTAQUANT',
    'id_beneficiaire': 'BEN-INCONNU',
    'antenne':         'MAT-ANT-999',
}
profil_fraude = dict(profil)
profil_fraude['ts_dernier_swap'] = ts_swap.isoformat()
profil_fraude['nb_otp_1h']       = 6

# Scoring 3 couches
r1 = c1.evaluate(event, profil_fraude)
r2 = c2.predict(event,  profil_fraude)
r3 = c3.predict(event,  profil_fraude)
score_final = max(r1.score, r2.score, r3.score)
decision    = 'BLOCK' if score_final >= 70 else ('CHALLENGE' if score_final >= 30 else 'PASS')

print()
print('=== Résultat du scoring ===')
print(f'  Couche 1 (regles) : {r1.score}/100  regles={[m.rule_id for m in r1.matches]}')
print(f'  Couche 2 (IF)     : {r2.score}/100  zscore={r2.zscore_montant:.1f}')
print(f'  Couche 3 (XGBoost): {r3.score}/100  proba={r3.probability:.4f}')
print(f'  SCORE FINAL (max) : {score_final}/100  -> {decision}')
print(f'  SHAP top-3 : {[(s[\"feature\"],s[\"direction\"]) for s in r3.shap_top3]}')
"
```

---

### Récapitulatif des commandes

```bash
# 1. Infrastructure
docker compose up redpanda redis postgres minio redpanda-init minio-init -d

# 2. Feature Updater (IA-3)
docker compose build feature-updater
docker run -d --name cg_feature_updater --network cyberguardian_default \
  -e ENV=local -e KAFKA_BOOTSTRAP_SERVERS=redpanda:9092 \
  -e REDIS_HOST=redis -e REDIS_PORT=6379 \
  cyberguardian-feature-updater

# 3. Simulateur
docker compose --profile simulator run --rm simulator --mode batch --reset-profiles

# 4. Vérifier Redis
docker exec cg_redis redis-cli DBSIZE   # → 500

# 5. Couche 1 — vérification (pas d'entraînement)
python -c "import sys; sys.path.insert(0,'.'); from engine.rules.engine import RuleEngine; e=RuleEngine(); print(e.rules_count,'regles')"

# 6. Couche 2 — entraînement IA-5
# (voir Étape 7 ci-dessus)

# 7. Couche 3 — entraînement IA-6
# (voir Étape 8 ci-dessus)

# 8. Vérifier MinIO
docker run --rm --network cyberguardian_default --entrypoint sh minio/mc:latest \
  -c "mc alias set local http://minio:9000 minioadmin minioadmin123 --api S3v4 >/dev/null && mc ls local/cg-models/ --recursive"

# 9. Test scoring complet
# (voir Étape 10 ci-dessus)

# 10. Arrêt
docker stop cg_feature_updater && docker rm cg_feature_updater
docker compose down
=======
Le simulateur publie ~23 000 événements dans les 3 topics Kafka. Le Feature Updater les consomme et met à jour les 500 profils dans Redis.

### 5. Vérifier le pipeline

```bash
# 500 profils créés dans Redis
docker exec cg_redis redis-cli DBSIZE

# Inspecter un profil mis à jour
docker exec cg_redis redis-cli GET "profile:CPT-<hash>"

# Logs du Feature Updater (doit afficher ~300 à 530+ msg/s, 0 erreur)
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
>>>>>>> c511adf10871166e2ba3ea9839d73c9a9eff9a4d
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

**Performance mesurée** : ~300 à 530+ msg/s, 0 erreur sur 23 934 événements simulés.

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
