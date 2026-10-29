# ══════════════════════════════════════════════════════════
#  CyberGuardian AI — Déploiement AWS du service de scoring (IA-9)
#
#  Internet → CloudFront (HTTPS) → ALB → ECS Fargate (scoring_api)
#                                          ├─ S3        règles, modèles, profils abonnés
#                                          └─ CloudWatch logs + métriques
#
#  Procédure complète : docs/deploiement_aws.md
# ══════════════════════════════════════════════════════════

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws    = { source = "hashicorp/aws", version = "~> 5.60" }
    random = { source = "hashicorp/random", version = "~> 3.6" }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = { Project = var.project, ManagedBy = "terraform" }
  }
}

data "aws_caller_identity" "current" {}
data "aws_availability_zones" "available" { state = "available" }

locals {
  name          = var.project
  models_bucket = "${var.project}-models-${data.aws_caller_identity.current.account_id}"
}
