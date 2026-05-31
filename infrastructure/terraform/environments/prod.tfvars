aws_region  = "us-east-1"
environment = "prod"

vpc_cidr = "10.0.0.0/16"

# EMR (cluster robusto para produção)
emr_master_instance = "m5.2xlarge"
emr_core_instance   = "m5.2xlarge"
emr_core_count      = 4

# RDS (Multi-AZ habilitado automaticamente para prod)
rds_instance_class = "db.r6g.large"
db_username        = "pipeline"

# MSK (Kafka)
msk_broker_instance = "kafka.m5.large"
msk_broker_count    = 3
