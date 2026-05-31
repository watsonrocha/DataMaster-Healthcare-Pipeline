variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "availability_zones" {
  description = "List of availability zones"
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b", "us-east-1c"]
}

variable "public_subnet_cidrs" {
  description = "CIDR blocks for public subnets"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
}

variable "private_subnet_cidrs" {
  description = "CIDR blocks for private subnets"
  type        = list(string)
  default     = ["10.0.11.0/24", "10.0.12.0/24", "10.0.13.0/24"]
}

# ── EMR ──────────────────────────────────────────────────────────────
variable "emr_master_instance" {
  description = "EC2 instance type for EMR master"
  type        = string
  default     = "m5.xlarge"
}

variable "emr_core_instance" {
  description = "EC2 instance type for EMR core nodes"
  type        = string
  default     = "m5.xlarge"
}

variable "emr_core_count" {
  description = "Number of EMR core instances"
  type        = number
  default     = 2
}

# ── RDS ──────────────────────────────────────────────────────────────
variable "rds_instance_class" {
  description = "RDS instance class"
  type        = string
  default     = "db.t3.medium"
}

variable "db_username" {
  description = "RDS master username"
  type        = string
  default     = "pipeline"
  sensitive   = true
}

variable "db_password" {
  description = "RDS master password"
  type        = string
  sensitive   = true
}

# ── MSK (Kafka) ──────────────────────────────────────────────────────
variable "msk_broker_instance" {
  description = "MSK broker instance type"
  type        = string
  default     = "kafka.m5.large"
}

variable "msk_broker_count" {
  description = "Number of MSK broker nodes"
  type        = number
  default     = 3
}
