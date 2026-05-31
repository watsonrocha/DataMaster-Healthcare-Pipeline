variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
}

variable "rds_engine_version" {
  description = "PostgreSQL engine version for RDS"
  type        = string
  default     = "16.3"
}

variable "db_username" {
  description = "RDS master username"
  type        = string
  default     = "pipeline"
  sensitive   = true
}

variable "db_password" {
  description = "RDS master password (supply via TF_VAR_db_password)"
  type        = string
  sensitive   = true
}

variable "allowed_cidr" {
  description = "CIDR allowed to reach the RDS instance on 5432"
  type        = string
  default     = "0.0.0.0/0"
}
