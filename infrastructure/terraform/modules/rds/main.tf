resource "aws_db_subnet_group" "main" {
  name       = "healthcare-${var.environment}"
  subnet_ids = var.subnet_ids
  tags       = { Name = "healthcare-${var.environment}-db-subnet" }
}

resource "aws_security_group" "rds" {
  name_prefix = "healthcare-rds-"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
    description = "PostgreSQL from VPC"
  }

  tags = { Name = "healthcare-${var.environment}-rds-sg" }
}

resource "aws_db_instance" "postgresql" {
  identifier     = "healthcare-${var.environment}"
  engine         = "postgres"
  engine_version = "16.3"
  instance_class = var.instance_class

  db_name  = var.db_name
  username = var.db_username
  password = var.db_password

  allocated_storage     = 20
  max_allocated_storage = 100
  storage_encrypted     = true

  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  multi_az               = var.multi_az

  backup_retention_period = 7
  skip_final_snapshot     = var.environment != "prod"

  tags = { Name = "healthcare-${var.environment}-postgres" }
}

variable "environment" { type = string }
variable "vpc_id" { type = string }
variable "subnet_ids" { type = list(string) }
variable "instance_class" { type = string }
variable "db_name" { type = string }
variable "db_username" { type = string }
variable "db_password" { type = string }
variable "multi_az" { type = bool }

output "endpoint" { value = aws_db_instance.postgresql.endpoint }
output "address" { value = aws_db_instance.postgresql.address }
output "port" { value = aws_db_instance.postgresql.port }
