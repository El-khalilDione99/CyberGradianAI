# ── Image Docker du service ────────────────────────────────
resource "aws_ecr_repository" "api" {
  name                 = "${local.name}/scoring-api"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
  image_scanning_configuration { scan_on_push = true }
}

# ── Règles et modèles (remplace MinIO cg-models) ───────────
resource "aws_s3_bucket" "models" {
  bucket = local.models_bucket
}

resource "aws_s3_bucket_versioning" "models" {
  bucket = aws_s3_bucket.models.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_public_access_block" "models" {
  bucket                  = aws_s3_bucket.models.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Les profils abonnés sont un fichier JSON dans ce même bucket
# (profiles/profiles.json), chargé en mémoire au démarrage du conteneur.

# ── Clés d'API (injectées dans le conteneur) ───────────────
resource "random_password" "reload_key" {
  length  = 40
  special = false
}

resource "random_password" "scoring_key" {
  length  = 40
  special = false
}
