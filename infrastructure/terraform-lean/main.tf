# ══════════════════════════════════════════════════════════════════════
# Pipeline de Dados de Saúde — Stack AWS enxuto (S3 + RDS)
# ══════════════════════════════════════════════════════════════════════
#
# Versão de baixo custo da infraestrutura: provisiona apenas o Data Lake
# (S3) e o banco (RDS PostgreSQL db.t3.micro, free tier), usando a VPC
# default da conta. Não cria EMR, MSK nem NAT Gateway.
#
# Uso:
#   export TF_VAR_db_password="<senha forte>"
#   terraform init
#   terraform plan -var-file="lean.tfvars"
#   terraform apply -var-file="lean.tfvars"
# ══════════════════════════════════════════════════════════════════════

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "healthcare-data-pipeline"
      Environment = var.environment
      ManagedBy   = "terraform"
      Stack       = "lean"
    }
  }
}

data "aws_caller_identity" "current" {}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# ── S3 Data Lake (reutiliza o módulo da stack completa) ──────────────
module "data_lake" {
  source = "../terraform/modules/s3"

  environment       = var.environment
  bucket_prefix     = "healthcare-datalake-${data.aws_caller_identity.current.account_id}"
  enable_versioning = true
  lifecycle_rules = {
    bronze_to_ia_days   = 30
    silver_to_ia_days   = 90
    gold_retention_days = 365
  }
}

# ── RDS PostgreSQL (db.t3.micro, free tier) ──────────────────────────
resource "aws_db_subnet_group" "main" {
  name       = "healthcare-lean-${var.environment}"
  subnet_ids = data.aws_subnets.default.ids
  tags       = { Name = "healthcare-lean-${var.environment}-db-subnet" }
}

resource "aws_security_group" "rds" {
  name_prefix = "healthcare-lean-rds-"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
    description = "PostgreSQL access"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "healthcare-lean-${var.environment}-rds-sg" }
}

resource "aws_db_instance" "postgresql" {
  identifier     = "healthcare-lean-${var.environment}"
  engine         = "postgres"
  engine_version = var.rds_engine_version
  instance_class = "db.t3.micro"

  db_name  = "healthcare"
  username = var.db_username
  password = var.db_password

  allocated_storage     = 20
  max_allocated_storage = 100
  storage_encrypted     = true

  db_subnet_group_name    = aws_db_subnet_group.main.name
  vpc_security_group_ids  = [aws_security_group.rds.id]
  publicly_accessible     = true
  multi_az                = false
  backup_retention_period = 0
  skip_final_snapshot     = true
  apply_immediately       = true

  tags = { Name = "healthcare-lean-${var.environment}-postgres" }
}
