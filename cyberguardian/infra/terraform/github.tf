# ── Construction de l'image par GitHub Actions (OIDC, sans clé stockée) ──
# Le workflow .github/workflows/scoring-api-image.yml assume ce rôle pour
# pousser l'image dans ECR puis relancer le service ECS.

variable "github_repositories" {
  # GitHub envoie désormais le sub sous la forme repo:owner@id_owner/nom@id_depot:...
  # On garde aussi l'ancienne forme (owner/nom) pour les comptes qui l'utilisent encore.
  description = "Dépôts GitHub autorisés à pousser l'image, tels qu'ils apparaissent dans le sub OIDC"
  type        = list(string)
  default = [
    "El-khalilDione99/CyberGradianAI",
    "El-khalilDione99@150396064/CyberGradianAI@1333300719",
    "khalilldion99/CyberGradianAI",
    "khalilldion99@350281002/CyberGradianAI@1439720053",
  ]
}

resource "aws_iam_openid_connect_provider" "github" {
  url             = "https://token.actions.githubusercontent.com"
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]
}

data "aws_iam_policy_document" "github_assume" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = [for r in var.github_repositories : "repo:${r}:*"]
    }
  }
}

resource "aws_iam_role" "github_deploy" {
  name               = "${local.name}-github-deploy"
  assume_role_policy = data.aws_iam_policy_document.github_assume.json
}

resource "aws_iam_role_policy" "github_deploy" {
  name = "push-image-et-redeploiement"
  role = aws_iam_role.github_deploy.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["ecr:GetAuthorizationToken"]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "ecr:BatchCheckLayerAvailability", "ecr:BatchGetImage", "ecr:CompleteLayerUpload",
          "ecr:GetDownloadUrlForLayer", "ecr:InitiateLayerUpload", "ecr:PutImage", "ecr:UploadLayerPart",
        ]
        Resource = aws_ecr_repository.api.arn
      },
      {
        Effect   = "Allow"
        Action   = ["ecs:UpdateService", "ecs:DescribeServices"]
        Resource = aws_ecs_service.api.id
      },
    ]
  })
}

output "github_deploy_role_arn" {
  value = aws_iam_role.github_deploy.arn
}
