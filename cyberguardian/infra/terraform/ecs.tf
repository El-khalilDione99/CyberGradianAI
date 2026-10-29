resource "aws_cloudwatch_log_group" "api" {
  name              = "/ecs/${local.name}/scoring-api"
  retention_in_days = 30
}

resource "aws_ecs_cluster" "main" {
  name = local.name
  setting {
    name  = "containerInsights"
    value = "disabled" # Container Insights est payant ; les logs CloudWatch suffisent
  }
}

resource "aws_ecs_task_definition" "api" {
  family                   = "${local.name}-scoring-api"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.task_cpu
  memory                   = var.task_memory
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  container_definitions = jsonencode([{
    name         = "scoring-api"
    image        = "${aws_ecr_repository.api.repository_url}:${var.image_tag}"
    essential    = true
    portMappings = [{ containerPort = 8000, protocol = "tcp" }]

    environment = [
      { name = "ENV", value = "aws" },
      { name = "AWS_DEFAULT_REGION", value = var.region },
      { name = "S3_BUCKET_MODELS", value = aws_s3_bucket.models.bucket },
      { name = "FEATURE_STORE", value = "s3" },
      { name = "PROFILES_S3_KEY", value = "profiles/profiles.json" },
      { name = "RULES_SOURCE", value = "s3" },
      { name = "RULES_BUCKET", value = aws_s3_bucket.models.bucket },
      { name = "RULES_S3_KEY", value = "rules/rules.yaml" },
      { name = "RULES_PATH", value = "/app/engine/rules/rules.yaml" },
      { name = "SCORE_THRESHOLD_LOW", value = "30" },
      { name = "SCORE_THRESHOLD_MED", value = "70" },
      { name = "SCORE_THRESHOLD_HIGH", value = "90" },
      { name = "SCORE_WEIGHT_IF", value = var.score_weight_if },
      { name = "SCORE_WEIGHT_XGB", value = var.score_weight_xgb },
      { name = "METRICS_BACKEND", value = "cloudwatch" },
      { name = "SCORING_UPDATE_PROFILE", value = "false" },
      { name = "RELOAD_API_KEY", value = random_password.reload_key.result },
      { name = "SCORING_API_KEY", value = random_password.scoring_key.result },
      # POSTGRES_DSN absent : décisions non enregistrées (renvoyées dans chaque réponse)
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.api.name
        "awslogs-region"        = var.region
        "awslogs-stream-prefix" = "api"
      }
    }
  }])
}

resource "aws_ecs_service" "api" {
  name                              = "scoring-api"
  cluster                           = aws_ecs_cluster.main.id
  task_definition                   = aws_ecs_task_definition.api.arn
  desired_count                     = var.desired_count
  launch_type                       = "FARGATE"
  health_check_grace_period_seconds = 180 # chargement des modèles + SHAP au démarrage

  network_configuration {
    subnets          = aws_subnet.public[*].id
    security_groups  = [aws_security_group.ecs.id]
    assign_public_ip = true
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.api.arn
    container_name   = "scoring-api"
    container_port   = 8000
  }

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  depends_on = [aws_lb_listener.http]
}

# ── Load balancer ──────────────────────────────────────────
resource "aws_lb" "api" {
  name               = "${local.name}-alb"
  load_balancer_type = "application"
  security_groups    = [aws_security_group.alb.id]
  subnets            = aws_subnet.public[*].id
}

resource "aws_lb_target_group" "api" {
  name        = "${local.name}-api"
  port        = 8000
  protocol    = "HTTP"
  target_type = "ip"
  vpc_id      = aws_vpc.main.id

  health_check {
    path                = "/health"
    matcher             = "200"
    interval            = 30
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }
}

resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.api.arn
  port              = 80
  protocol          = "HTTP"
  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.api.arn
  }
}

# ── CloudFront : URL HTTPS publique sans nom de domaine ────
resource "aws_cloudfront_distribution" "api" {
  count       = var.enable_cloudfront ? 1 : 0
  enabled     = true
  comment     = "${local.name} scoring API"
  price_class = "PriceClass_100"

  origin {
    domain_name = aws_lb.api.dns_name
    origin_id   = "alb"
    custom_origin_config {
      http_port              = 80
      https_port             = 443
      origin_protocol_policy = "http-only"
      origin_ssl_protocols   = ["TLSv1.2"]
    }
  }

  default_cache_behavior {
    target_origin_id       = "alb"
    viewer_protocol_policy = "https-only"
    allowed_methods        = ["GET", "HEAD", "OPTIONS", "PUT", "POST", "PATCH", "DELETE"]
    cached_methods         = ["GET", "HEAD"]
    # Politiques gérées AWS : CachingDisabled + AllViewerExceptHostHeader (transmet X-API-Key)
    cache_policy_id          = "4135ea2d-6df8-44a3-9df3-4b5a84be39ad"
    origin_request_policy_id = "b689b0a8-53d0-40ab-baf2-68738e2966ac"
  }

  restrictions {
    geo_restriction { restriction_type = "none" }
  }

  viewer_certificate { cloudfront_default_certificate = true }
}
