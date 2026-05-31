output "data_lake_bucket" {
  description = "S3 Data Lake bucket name"
  value       = module.data_lake.bucket_name
}

output "checkpoints_bucket" {
  description = "S3 checkpoints bucket name"
  value       = module.data_lake.checkpoints_bucket
}

output "rds_endpoint" {
  description = "RDS PostgreSQL endpoint (host:port)"
  value       = aws_db_instance.postgresql.endpoint
}

output "rds_address" {
  description = "RDS PostgreSQL host"
  value       = aws_db_instance.postgresql.address
}

output "rds_port" {
  description = "RDS PostgreSQL port"
  value       = aws_db_instance.postgresql.port
}

output "rds_database" {
  description = "RDS database name"
  value       = aws_db_instance.postgresql.db_name
}
