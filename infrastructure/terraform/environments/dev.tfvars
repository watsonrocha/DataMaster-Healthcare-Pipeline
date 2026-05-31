aws_region  = "us-east-1"
environment = "dev"

# EMR (cluster menor para dev)
emr_master_instance = "m5.xlarge"
emr_core_instance   = "m5.large"
emr_core_count      = 1

# RDS
rds_instance_class = "db.t3.micro"
db_username        = "pipeline"

# MSK (Kafka)
msk_broker_instance = "kafka.t3.small"
msk_broker_count    = 2
