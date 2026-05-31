# ══════════════════════════════════════════════════════════════════════
# Pipeline de Dados de Saúde — Infraestrutura AWS (Terraform)
# ══════════════════════════════════════════════════════════════════════
#
# Provisiona: VPC, S3 (Data Lake), EMR (Spark), RDS (PostgreSQL),
#             MSK (Kafka), IAM roles e security groups.
#
# Uso:
#   terraform init
#   terraform plan -var-file="environments/dev.tfvars"
#   terraform apply -var-file="environments/dev.tfvars"
# ══════════════════════════════════════════════════════════════════════

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    bucket         = "healthcare-pipeline-tfstate"
    key            = "terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "terraform-locks"
    encrypt        = true
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "healthcare-data-pipeline"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

# ── VPC ──────────────────────────────────────────────────────────────
module "vpc" {
  source = "./modules/vpc"

  environment   = var.environment
  vpc_cidr      = var.vpc_cidr
  azs           = var.availability_zones
  public_cidrs  = var.public_subnet_cidrs
  private_cidrs = var.private_subnet_cidrs
}

# ── S3 Data Lake ─────────────────────────────────────────────────────
module "data_lake" {
  source = "./modules/s3"

  environment       = var.environment
  bucket_prefix     = "healthcare-datalake"
  enable_versioning = true
  lifecycle_rules = {
    bronze_to_ia_days   = 30
    silver_to_ia_days   = 90
    gold_retention_days = 365
  }
}

# ── EMR (Spark Cluster) ─────────────────────────────────────────────
module "emr" {
  source = "./modules/emr"

  environment      = var.environment
  vpc_id           = module.vpc.vpc_id
  subnet_id        = module.vpc.private_subnet_ids[0]
  master_instance  = var.emr_master_instance
  core_instance    = var.emr_core_instance
  core_count       = var.emr_core_count
  data_lake_bucket = module.data_lake.bucket_arn
  release_label    = "emr-7.1.0"
  applications     = ["Spark", "Hive", "JupyterEnterpriseGateway"]
}

# ── RDS (PostgreSQL) ─────────────────────────────────────────────────
module "rds" {
  source = "./modules/rds"

  environment    = var.environment
  vpc_id         = module.vpc.vpc_id
  subnet_ids     = module.vpc.private_subnet_ids
  instance_class = var.rds_instance_class
  db_name        = "healthcare"
  db_username    = var.db_username
  db_password    = var.db_password
  multi_az       = var.environment == "prod"
}

# ── MSK (Kafka) ──────────────────────────────────────────────────────
module "msk" {
  source = "./modules/msk"

  environment     = var.environment
  vpc_id          = module.vpc.vpc_id
  subnet_ids      = module.vpc.private_subnet_ids
  broker_instance = var.msk_broker_instance
  broker_count    = var.msk_broker_count
  kafka_version   = "3.6.0"
}
