resource "aws_security_group" "msk" {
  name_prefix = "healthcare-msk-"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 9092
    to_port     = 9098
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
    description = "Kafka brokers from VPC"
  }

  ingress {
    from_port   = 2181
    to_port     = 2181
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
    description = "Zookeeper from VPC"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "healthcare-${var.environment}-msk-sg" }
}

resource "aws_msk_cluster" "kafka" {
  cluster_name           = "healthcare-${var.environment}"
  kafka_version          = var.kafka_version
  number_of_broker_nodes = var.broker_count

  broker_node_group_info {
    instance_type   = var.broker_instance
    client_subnets  = var.subnet_ids
    security_groups = [aws_security_group.msk.id]

    storage_info {
      ebs_storage_info {
        volume_size = 100
      }
    }
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS_PLAINTEXT"
      in_cluster    = true
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.kafka.arn
    revision = aws_msk_configuration.kafka.latest_revision
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk.name
      }
    }
  }

  tags = { Name = "healthcare-${var.environment}-msk" }
}

resource "aws_msk_configuration" "kafka" {
  name              = "healthcare-${var.environment}"
  kafka_versions    = [var.kafka_version]
  server_properties = <<-PROPERTIES
    auto.create.topics.enable=true
    default.replication.factor=2
    min.insync.replicas=1
    num.partitions=6
    log.retention.hours=168
  PROPERTIES
}

resource "aws_cloudwatch_log_group" "msk" {
  name              = "/aws/msk/healthcare-${var.environment}"
  retention_in_days = 14
}

variable "environment" { type = string }
variable "vpc_id" { type = string }
variable "subnet_ids" { type = list(string) }
variable "broker_instance" { type = string }
variable "broker_count" { type = number }
variable "kafka_version" { type = string }

output "bootstrap_brokers" {
  value = aws_msk_cluster.kafka.bootstrap_brokers
}

output "bootstrap_brokers_tls" {
  value = aws_msk_cluster.kafka.bootstrap_brokers_tls
}

output "zookeeper_connect" {
  value = aws_msk_cluster.kafka.zookeeper_connect_string
}
