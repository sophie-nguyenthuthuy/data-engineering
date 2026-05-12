data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

locals {
  account_id     = data.aws_caller_identity.current.account_id
  region         = data.aws_region.current.region
  producer_image = coalesce(var.producer_image, "${aws_ecr_repository.producer.repository_url}:latest")
  api_image      = coalesce(var.api_image, "${aws_ecr_repository.api.repository_url}:latest")
}

# ------------------------------------------------------------------------------
# ECR — one repo per image we build in CI. `latest` is a mutable convenience tag.
# ------------------------------------------------------------------------------
resource "aws_ecr_repository" "producer" {
  name                 = "${var.name_prefix}/producer"
  image_tag_mutability = "MUTABLE"
  image_scanning_configuration {
    scan_on_push = true
  }
  tags = var.tags
}

resource "aws_ecr_repository" "api" {
  name                 = "${var.name_prefix}/api"
  image_tag_mutability = "MUTABLE"
  image_scanning_configuration {
    scan_on_push = true
  }
  tags = var.tags
}

resource "aws_ecr_lifecycle_policy" "producer" {
  repository = aws_ecr_repository.producer.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep last 10 tagged images, expire untagged after 7 days"
      selection = {
        tagStatus   = "untagged"
        countType   = "sinceImagePushed"
        countUnit   = "days"
        countNumber = 7
      }
      action = { type = "expire" }
    }]
  })
}

resource "aws_ecr_lifecycle_policy" "api" {
  repository = aws_ecr_repository.api.name
  policy     = aws_ecr_lifecycle_policy.producer.policy
}

# ------------------------------------------------------------------------------
# CloudWatch log groups for task stdout. Loki on ECS would need a sidecar; we
# keep it simple with awslogs + a Grafana CloudWatch datasource.
# ------------------------------------------------------------------------------
resource "aws_cloudwatch_log_group" "producer" {
  name              = "/ecs/${var.name_prefix}/producer"
  retention_in_days = 14
  tags              = var.tags
}

resource "aws_cloudwatch_log_group" "api" {
  name              = "/ecs/${var.name_prefix}/api"
  retention_in_days = 14
  tags              = var.tags
}

# ------------------------------------------------------------------------------
# IAM — execution role (pull images, write logs) + task roles (app permissions)
# ------------------------------------------------------------------------------
data "aws_iam_policy_document" "ecs_task_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "execution" {
  name               = "${var.name_prefix}-ecs-exec"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_assume.json
  tags               = var.tags
}

resource "aws_iam_role_policy_attachment" "execution_managed" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

data "aws_iam_policy_document" "execution_secrets" {
  statement {
    sid       = "ReadDbSecret"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [var.db_secret_arn]
  }
}

resource "aws_iam_policy" "execution_secrets" {
  name   = "${var.name_prefix}-ecs-exec-secrets"
  policy = data.aws_iam_policy_document.execution_secrets.json
}

resource "aws_iam_role_policy_attachment" "execution_secrets" {
  role       = aws_iam_role.execution.name
  policy_arn = aws_iam_policy.execution_secrets.arn
}

# Producer task role: write to MSK + observability. No S3 reads.
data "aws_iam_policy_document" "producer_task" {
  statement {
    sid = "KafkaIAMWrite"
    actions = [
      "kafka-cluster:Connect",
      "kafka-cluster:DescribeCluster",
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:WriteData",
      "kafka-cluster:WriteDataIdempotently",
      "kafka-cluster:CreateTopic",
    ]
    resources = [
      var.msk_cluster_arn,
      "${replace(var.msk_cluster_arn, ":cluster/", ":topic/")}/*",
    ]
  }
}

resource "aws_iam_role" "producer_task" {
  name               = "${var.name_prefix}-producer-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_assume.json
  tags               = var.tags
}

resource "aws_iam_role_policy" "producer_task" {
  role   = aws_iam_role.producer_task.id
  policy = data.aws_iam_policy_document.producer_task.json
}

# API task role: S3 RW on analysis, read raw, DB secret (for read-only reports).
data "aws_iam_policy_document" "api_task" {
  statement {
    sid       = "S3ReadRaw"
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = [var.raw_bucket_arn, "${var.raw_bucket_arn}/*"]
  }
  statement {
    sid = "S3ReadWriteAnalysis"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
    ]
    resources = [var.analysis_bucket_arn, "${var.analysis_bucket_arn}/*"]
  }
}

resource "aws_iam_role" "api_task" {
  name               = "${var.name_prefix}-api-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_assume.json
  tags               = var.tags
}

resource "aws_iam_role_policy" "api_task" {
  role   = aws_iam_role.api_task.id
  policy = data.aws_iam_policy_document.api_task.json
}

# ------------------------------------------------------------------------------
# ECS cluster + Fargate capacity provider
# ------------------------------------------------------------------------------
resource "aws_ecs_cluster" "this" {
  name = "${var.name_prefix}-cluster"
  setting {
    name  = "containerInsights"
    value = "enabled"
  }
  tags = var.tags
}

resource "aws_ecs_cluster_capacity_providers" "this" {
  cluster_name       = aws_ecs_cluster.this.name
  capacity_providers = ["FARGATE", "FARGATE_SPOT"]

  default_capacity_provider_strategy {
    capacity_provider = "FARGATE"
    weight            = 1
    base              = 1
  }
}

# ------------------------------------------------------------------------------
# Security groups: tasks egress to MSK + all; ALB ingress :80 → API SG :8000
# ------------------------------------------------------------------------------
resource "aws_security_group" "tasks" {
  name        = "${var.name_prefix}-tasks"
  description = "ECS tasks — egress to MSK, ALB ingress on API port."
  vpc_id      = var.vpc_id
  tags        = merge(var.tags, { Name = "${var.name_prefix}-tasks" })
}

resource "aws_vpc_security_group_egress_rule" "tasks_all" {
  security_group_id = aws_security_group.tasks.id
  description       = "All egress; NAT covers Internet, VPC endpoints cover S3."
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

resource "aws_security_group" "alb" {
  name        = "${var.name_prefix}-alb"
  description = "ALB — HTTPS from Internet."
  vpc_id      = var.vpc_id
  tags        = merge(var.tags, { Name = "${var.name_prefix}-alb" })
}

resource "aws_vpc_security_group_ingress_rule" "alb_http" {
  security_group_id = aws_security_group.alb.id
  description       = "HTTP from anywhere (terminate at ALB; TLS via ACM in prod)."
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = 80
  to_port           = 80
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "alb_all" {
  security_group_id = aws_security_group.alb.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

# Allow ALB → API tasks on :8000 without a CIDR detour.
resource "aws_vpc_security_group_ingress_rule" "tasks_from_alb" {
  security_group_id            = aws_security_group.tasks.id
  description                  = "ALB → API"
  referenced_security_group_id = aws_security_group.alb.id
  from_port                    = 8000
  to_port                      = 8000
  ip_protocol                  = "tcp"
}

# ------------------------------------------------------------------------------
# ALB for the API service
# ------------------------------------------------------------------------------
resource "aws_lb" "api" {
  name               = "${var.name_prefix}-api"
  internal           = false
  load_balancer_type = "application"
  subnets            = var.public_subnet_ids
  security_groups    = [aws_security_group.alb.id]
  idle_timeout       = 60
  tags               = var.tags
}

resource "aws_lb_target_group" "api" {
  name        = "${var.name_prefix}-api"
  port        = 8000
  protocol    = "HTTP"
  target_type = "ip"
  vpc_id      = var.vpc_id
  health_check {
    path                = "/healthz"
    matcher             = "200"
    healthy_threshold   = 2
    unhealthy_threshold = 3
    interval            = 15
    timeout             = 5
  }
  tags = var.tags
}

resource "aws_lb_listener" "api_http" {
  load_balancer_arn = aws_lb.api.arn
  port              = 80
  protocol          = "HTTP"
  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.api.arn
  }
}

# ------------------------------------------------------------------------------
# Task definitions
# ------------------------------------------------------------------------------
resource "aws_ecs_task_definition" "producer" {
  family                   = "${var.name_prefix}-producer"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.producer_cpu
  memory                   = var.producer_memory
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.producer_task.arn

  container_definitions = jsonencode([{
    name      = "producer"
    image     = local.producer_image
    essential = true
    environment = [
      { name = "BROKERS", value = var.msk_bootstrap_brokers },
      { name = "TOPIC", value = var.topic },
      { name = "SASL_ENABLED", value = "true" },
      { name = "SASL_MECHANISM", value = "OAUTHBEARER" }, # AWS_MSK_IAM via client plugin
      { name = "OTEL_SERVICE_NAME", value = "producer" },
      # OTEL endpoint set by observability module when ADOT sidecar is wired.
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.producer.name
        awslogs-region        = local.region
        awslogs-stream-prefix = "producer"
      }
    }
  }])

  tags = var.tags
}

resource "aws_ecs_task_definition" "api" {
  family                   = "${var.name_prefix}-api"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.api_cpu
  memory                   = var.api_memory
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.api_task.arn

  container_definitions = jsonencode([{
    name      = "api"
    image     = local.api_image
    essential = true
    portMappings = [{
      containerPort = 8000
      protocol      = "tcp"
    }]
    environment = [
      { name = "OTEL_SERVICE_NAME", value = "api" },
    ]
    healthCheck = {
      command     = ["CMD-SHELL", "curl -fsS http://localhost:8000/healthz || exit 1"]
      interval    = 15
      timeout     = 5
      retries     = 3
      startPeriod = 10
    }
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.api.name
        awslogs-region        = local.region
        awslogs-stream-prefix = "api"
      }
    }
  }])

  tags = var.tags
}

# ------------------------------------------------------------------------------
# Services
# ------------------------------------------------------------------------------
resource "aws_ecs_service" "producer" {
  name            = "${var.name_prefix}-producer"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.producer.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [aws_security_group.tasks.id]
    assign_public_ip = false
  }

  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100
  propagate_tags                     = "SERVICE"
  tags                               = var.tags
}

resource "aws_ecs_service" "api" {
  name            = "${var.name_prefix}-api"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.api.arn
  desired_count   = var.api_desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [aws_security_group.tasks.id]
    assign_public_ip = false
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.api.arn
    container_name   = "api"
    container_port   = 8000
  }

  deployment_minimum_healthy_percent = 50
  deployment_maximum_percent         = 200
  propagate_tags                     = "SERVICE"
  depends_on                         = [aws_lb_listener.api_http]
  tags                               = var.tags
}
