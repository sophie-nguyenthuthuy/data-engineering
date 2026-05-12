provider "aws" {
  region = var.region
  default_tags {
    tags = local.common_tags
  }
}

data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  common_tags = {
    Project     = "end2end-pipeline"
    Environment = "dev"
    ManagedBy   = "terraform"
  }
  azs = slice(data.aws_availability_zones.available.names, 0, 2)
}

module "network" {
  source      = "../../modules/network"
  name_prefix = var.name_prefix
  azs         = local.azs
  tags        = local.common_tags
}

module "kafka" {
  source             = "../../modules/kafka"
  name_prefix        = var.name_prefix
  vpc_id             = module.network.vpc_id
  vpc_cidr_block     = module.network.vpc_cidr_block
  private_subnet_ids = module.network.private_subnet_ids
  tags               = local.common_tags
}

module "storage" {
  source        = "../../modules/storage"
  name_prefix   = var.name_prefix
  force_destroy = true # dev only — don't do this in prod
  tags          = local.common_tags
}

module "analytics" {
  source              = "../../modules/analytics"
  name_prefix         = var.name_prefix
  vpc_id              = module.network.vpc_id
  private_subnet_ids  = module.network.private_subnet_ids
  vpc_cidr_block      = module.network.vpc_cidr_block
  skip_final_snapshot = true # dev only
  tags                = local.common_tags
}

module "compute" {
  source                = "../../modules/compute"
  name_prefix           = var.name_prefix
  vpc_id                = module.network.vpc_id
  public_subnet_ids     = module.network.public_subnet_ids
  private_subnet_ids    = module.network.private_subnet_ids
  msk_cluster_arn       = module.kafka.cluster_arn
  msk_security_group_id = module.kafka.security_group_id
  msk_bootstrap_brokers = module.kafka.bootstrap_brokers_sasl_iam
  raw_bucket_arn        = module.storage.raw_bucket_arn
  analysis_bucket_arn   = module.storage.analysis_bucket_arn
  db_secret_arn         = module.analytics.db_secret_arn
  tags                  = local.common_tags
}

module "observability" {
  source                  = "../../modules/observability"
  name_prefix             = var.name_prefix
  grafana_admin_role_arns = var.grafana_admin_sso_ids
  tags                    = local.common_tags
}

module "iam" {
  source       = "../../modules/iam"
  name_prefix  = var.name_prefix
  github_owner = var.github_owner
  github_repo  = var.github_repo
  allowed_refs = var.gha_allowed_refs
  tags         = local.common_tags
}
