# Déploiement du service de scoring sur AWS

Ce guide met en ligne `scoring_api` (le moteur IA à 3 couches) sur AWS, pour que les équipes
mobile et dashboard l'appellent par une URL publique (HTTP, ou HTTPS avec CloudFront). L'infrastructure est décrite en
Terraform dans `infra/terraform/`.

```
App mobile / Dashboard ──▶ [CloudFront, optionnel] ──▶ ALB ──▶ ECS Fargate (scoring_api)
                                                            ├─ S3        règles, modèles, profils abonnés
                                                            └─ CloudWatch logs + métriques
```

| Local (docker compose) | AWS |
|---|---|
| MinIO `cg-models` | S3 `cyberguardian-models-<id du compte>` |
| Redis `profile:<id_compte>` | S3 `profiles/profiles.json`, chargé en mémoire au démarrage |
| PostgreSQL `decisions` | non déployé : chaque décision est renvoyée dans la réponse de l'API |
| `GET /metrics` | CloudWatch, espace de noms `CyberGuardian/Scoring` |

Services utilisés : ECS Fargate, ECR, IAM, S3, VPC / Security Groups, ALB, CloudWatch (la liste
du coach), plus CloudFront en option pour l'URL HTTPS.

CloudFront est désactivé par défaut (`enable_cloudfront = false`) : l'API est alors servie en
**HTTP** par l'ALB. Quand le coach a activé CloudFront, il suffit de relancer
`terraform apply -var="enable_cloudfront=true"` : l'URL devient HTTPS et l'ALB se ferme au reste
d'internet. Rien d'autre ne change.

Coût indicatif, en fonctionnement 24 h/24, à Paris :

| Service | Coût par mois |
|---|---|
| Fargate, 1 vCPU et 2 Go | environ 42 $ |
| ALB | environ 22 $ |
| Adresses IPv4 publiques (2 pour l'ALB, 1 pour la tâche) | environ 11 $ |
| CloudWatch logs, S3, ECR | environ 2 $ |
| CloudFront, si activé | moins de 1 $ à ce volume |
| **Total** | **environ 75 à 80 $** |

Avec un budget de 230 $, cela fait environ trois mois en continu. Pour économiser, on peut arrêter
le conteneur quand personne ne s'en sert (`terraform apply -var="desired_count=0"`). L'ALB et les IP
restent alors facturés (environ 33 $ par mois). `terraform destroy` arrête tous les coûts. Pour tout supprimer :
`terraform destroy`.

## 0. Prérequis sur le PC

- Un compte AWS avec un utilisateur IAM qui a les droits d'administration, configuré avec `aws configure`
  (région `eu-west-3`).
- AWS CLI v2, Terraform ≥ 1.5 et Docker Desktop.
- La stack locale démarrée avec les modèles déjà entraînés, c'est-à-dire MinIO et Redis remplis comme
  dans le README, étapes 2 à 5.

Toutes les commandes ci-dessous sont en PowerShell et se lancent depuis `cyberguardian/`.

## 1. Créer le stockage (ECR, S3)

On crée d'abord ce qui doit exister avant le service : le dépôt d'image et le bucket.

```powershell
cd infra\terraform
terraform init
terraform apply -target="aws_ecr_repository.api" -target="aws_s3_bucket.models"
$ECR    = terraform output -raw ecr_repository_url
$BUCKET = terraform output -raw models_bucket
cd ..\..
```

## 2. Copier les modèles et les profils (sans réentraîner)

Le script lit MinIO et Redis en local (paramètres de `.env.local`) et écrit tout dans S3 :

```powershell
.venv\Scripts\python.exe scripts\migrer_vers_aws.py --bucket $BUCKET --region eu-west-3
```

Le script doit lister `rules/rules.yaml`, `anomaly/production/current.json`,
`xgboost/production/current.json` et les fichiers `.pkl` des modèles, puis environ 1 500 profils
écrits dans `profiles/profiles.json`.

## 3. Construire et pousser l'image

**Variante sans Docker sur le PC (recommandée si le disque est plein) :** le workflow GitHub Actions
`.github/workflows/scoring-api-image.yml` construit l'image sur les serveurs de GitHub, la pousse dans
ECR et relance le service. Il se déclenche à chaque push qui touche le moteur, ou à la main depuis
l'onglet Actions. Son rôle AWS (`cyberguardian-github-deploy`, sans clé stockée) est créé par
`terraform apply` à l'étape 4 : faire l'étape 4 d'abord, puis lancer le workflow.

Sinon, en local :

```powershell
aws ecr get-login-password --region eu-west-3 | docker login --username AWS --password-stdin $ECR.Split("/")[0]
docker build --platform linux/amd64 -f engine/scoring_api/Dockerfile -t "${ECR}:latest" .
docker push "${ECR}:latest"
```

## 4. Créer le reste (VPC, ECS, ALB, CloudFront)

```powershell
cd infra\terraform
terraform apply                                   # HTTP (sans CloudFront)
# terraform apply -var="enable_cloudfront=true"   # HTTPS, une fois CloudFront autorisé
terraform output api_url
terraform output -raw scoring_api_key
```

Comptez quelques minutes, ou environ 10 minutes de plus avec CloudFront. Au démarrage, le conteneur charge les
règles, les modèles et les profils depuis S3.

## 5. Vérifier

```powershell
$URL = terraform output -raw api_url
$KEY = terraform output -raw scoring_api_key
curl.exe "$URL/health"
curl.exe -X POST "$URL/v1/score" -H "Content-Type: application/json" -H "X-API-Key: $KEY" `
  -d '{\"id_transaction\":\"TXN-1\",\"id_compte\":\"CPT-...\",\"horodatage\":\"2026-07-14T09:19:02+00:00\",\"montant\":45000}'
```

`/health` doit renvoyer `"status": "ok"`, avec les trois couches chargées depuis S3 et
`"base_decisions": "desactivee"`. Pour un `id_compte`, prends un compte présent dans
`profiles/profiles.json`.

En cas de problème, les logs du conteneur se lisent avec :

```powershell
aws logs tail /ecs/cyberguardian/scoring-api --follow
```

## 6. Ce qu'on donne aux équipes mobile et dashboard

- **L'URL** : la sortie `api_url`, par exemple `https://dxxxxxxxx.cloudfront.net`.
- **La clé** : la sortie `scoring_api_key`, à envoyer dans l'en-tête `X-API-Key` de chaque appel `POST /v1/score`.
  Elle se transmet par un canal privé, jamais dans un dépôt Git. Côté mobile, il vaut mieux que l'appel
  passe par leur backend plutôt que d'embarquer la clé dans l'app.
- **Le contrat** : `docs/contrat_api_scoring.md` et `docs/guide_integration_scoring_api.md`. Le
  format des requêtes et des réponses est le même qu'en local.
- `/health` et `/metrics` restent ouverts sans clé, pour la supervision.

## 7. Mettre à jour

- **Nouveaux profils** : relancer `scripts/migrer_vers_aws.py`, puis redémarrer le service
  (`aws ecs update-service ... --force-new-deployment`, voir ci-dessous) : les profils ne sont lus qu'au démarrage.
- **Nouveaux modèles** : réentraîner en local, relancer `scripts/migrer_vers_aws.py --sans-profils`, puis
  appeler `POST $URL/reload-model` avec `X-API-Key: <reload_api_key>`. Le rechargement se fait à chaud,
  sans redéploiement.
- **Nouveau code** : reconstruire et pousser l'image (étape 3), puis
  `aws ecs update-service --cluster cyberguardian --service scoring-api --force-new-deployment`.

## Limites de ce déploiement

- **Les profils sont figés.** S3 contient la copie des profils simulés au moment de la migration.
  Le Feature Updater (IA-3) n'est pas déployé sur AWS, faute de flux Kinesis branché sur les événements
  réseau (swap SIM, OTP). Un swap SIM réel ne sera donc pas vu par le moteur tant que ce flux
  n'existe pas. Le scoring ne mettra pas non plus à jour les profils (`SCORING_UPDATE_PROFILE=false`).
- **Seuls les comptes simulés sont connus.** Un `id_compte` absent du fichier de profils est scoré sur un profil vide.
- **Pas d'historique des décisions côté IA.** Chaque réponse de `/v1/score` contient la décision complète
  (score, couches, règles, SHAP) : c'est au backend des devs de l'enregistrer s'il en a besoin pour le
  dashboard. Pour réactiver l'enregistrement, il suffit de définir `POSTGRES_DSN` vers une base PostgreSQL.
- **Une seule instance.** Passer `desired_count` à 2 apporte la haute disponibilité, mais double le coût de Fargate.
