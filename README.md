# CyberGuardian AI

Moteur IA de détection de fraude SIM swap en temps réel — Mobile Money Sénégal

## Structure

```
cyberguardian/
├── simulator/       # IA-1 : Simulateur de données (500 abonnés, scénarios d'attaque)
├── engine/          # Moteur IA 3 couches (règles, anomalie, XGBoost)
├── interfaces/      # Abstraction Kafka/Kinesis et Redis/DynamoDB
├── infra/           # Infrastructure Terraform AWS
├── notebooks/       # EDA et analyse des données
└── docker-compose.yml
```

## Stack locale

| Service | Port | Rôle |
|---|---|---|
| Redpanda | 19092 | Bus d'événements (Kafka) |
| Redis | 16379 | Feature store |
| PostgreSQL | 15432 | Base relationnelle |
| MinIO | 19000 | Stockage objets (S3) |

## Démarrage

```bash
docker compose up redpanda redis postgres minio
docker compose --profile simulator run --rm simulator
```
