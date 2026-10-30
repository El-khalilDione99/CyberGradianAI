output "api_url" {
  description = "URL publique à donner aux équipes mobile et dashboard"
  value = (var.enable_cloudfront
    ? "https://${aws_cloudfront_distribution.api[0].domain_name}"
  : "http://${aws_lb.api.dns_name}")
}

output "ecr_repository_url" {
  value = aws_ecr_repository.api.repository_url
}

output "models_bucket" {
  value = aws_s3_bucket.models.bucket
}

output "ecs_cluster" {
  value = aws_ecs_cluster.main.name
}

output "ecs_service" {
  value = aws_ecs_service.api.name
}

output "scoring_api_key" {
  description = "Clé X-API-Key des consommateurs de /v1/score (terraform output -raw scoring_api_key)"
  value       = random_password.scoring_key.result
  sensitive   = true
}

output "reload_api_key" {
  description = "Clé X-API-Key de /reload-model (terraform output -raw reload_api_key)"
  value       = random_password.reload_key.result
  sensitive   = true
}
