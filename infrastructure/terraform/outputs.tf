output "vpc_id" {
  description = "VPC ID"
  value       = module.vpc.vpc_id
}

output "data_lake_bucket" {
  description = "S3 Data Lake bucket name"
  value       = module.data_lake.bucket_name
}

output "data_lake_bucket_arn" {
  description = "S3 Data Lake bucket ARN"
  value       = module.data_lake.bucket_arn
}

output "emr_cluster_id" {
  description = "EMR cluster ID"
  value       = module.emr.cluster_id
}

output "emr_master_dns" {
  description = "EMR master node public DNS"
  value       = module.emr.master_public_dns
}

output "rds_endpoint" {
  description = "RDS PostgreSQL endpoint"
  value       = module.rds.endpoint
}

output "msk_bootstrap_brokers" {
  description = "MSK Kafka bootstrap brokers"
  value       = module.msk.bootstrap_brokers
}

output "msk_zookeeper_connect" {
  description = "MSK Zookeeper connection string"
  value       = module.msk.zookeeper_connect
}
