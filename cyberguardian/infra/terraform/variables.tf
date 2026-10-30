variable "region" {
  description = "Région AWS"
  type        = string
  default     = "eu-west-3" # Paris
}

variable "project" {
  description = "Préfixe des ressources"
  type        = string
  default     = "cyberguardian"
}

variable "image_tag" {
  description = "Tag de l'image scoring_api poussée dans ECR"
  type        = string
  default     = "latest"
}

variable "task_cpu" {
  description = "CPU de la tâche Fargate (1024 = 1 vCPU)"
  type        = number
  default     = 1024
}

variable "task_memory" {
  description = "Mémoire de la tâche Fargate en Mo (XGBoost + SHAP)"
  type        = number
  default     = 2048
}

variable "desired_count" {
  description = "Nombre de conteneurs scoring_api"
  type        = number
  default     = 1
}


variable "score_weight_if" {
  type    = string
  default = "0.1"
}

variable "score_weight_xgb" {
  type    = string
  default = "0.9"
}

variable "enable_cloudfront" {
  description = "true : URL HTTPS via CloudFront (ALB fermé au reste d'internet). false : URL HTTP directe de l'ALB"
  type        = bool
  default     = false
}
