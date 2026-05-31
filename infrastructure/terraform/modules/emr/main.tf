resource "aws_security_group" "emr_master" {
  name_prefix = "healthcare-emr-master-"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 8443
    to_port     = 8443
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
    description = "EMR managed scaling"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "healthcare-${var.environment}-emr-master-sg" }
}

resource "aws_security_group" "emr_core" {
  name_prefix = "healthcare-emr-core-"
  vpc_id      = var.vpc_id

  ingress {
    from_port       = 0
    to_port         = 65535
    protocol        = "tcp"
    security_groups = [aws_security_group.emr_master.id]
    description     = "All traffic from master"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "healthcare-${var.environment}-emr-core-sg" }
}

resource "aws_iam_role" "emr_service" {
  name = "healthcare-${var.environment}-emr-service"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "elasticmapreduce.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "emr_service" {
  role       = aws_iam_role.emr_service.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
}

resource "aws_iam_role" "emr_ec2" {
  name = "healthcare-${var.environment}-emr-ec2"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "emr_ec2_s3" {
  name = "s3-data-lake-access"
  role = aws_iam_role.emr_ec2.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:DeleteObject"]
      Resource = [var.data_lake_bucket, "${var.data_lake_bucket}/*"]
    }]
  })
}

resource "aws_iam_instance_profile" "emr_ec2" {
  name = "healthcare-${var.environment}-emr-ec2-profile"
  role = aws_iam_role.emr_ec2.name
}

resource "aws_emr_cluster" "spark" {
  name          = "healthcare-pipeline-${var.environment}"
  release_label = var.release_label
  applications  = var.applications
  service_role  = aws_iam_role.emr_service.arn

  ec2_attributes {
    instance_profile                  = aws_iam_instance_profile.emr_ec2.arn
    subnet_id                         = var.subnet_id
    emr_managed_master_security_group = aws_security_group.emr_master.id
    emr_managed_slave_security_group  = aws_security_group.emr_core.id
  }

  master_instance_group {
    instance_type  = var.master_instance
    instance_count = 1
  }

  core_instance_group {
    instance_type  = var.core_instance
    instance_count = var.core_count
  }

  configurations_json = jsonencode([
    {
      Classification = "spark-defaults"
      Properties = {
        "spark.sql.extensions"                          = "io.delta.sql.DeltaSparkSessionExtension"
        "spark.sql.catalog.spark_catalog"               = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        "spark.serializer"                              = "org.apache.spark.serializer.KryoSerializer"
        "spark.sql.adaptive.enabled"                    = "true"
        "spark.sql.adaptive.coalescePartitions.enabled" = "true"
      }
    },
    {
      Classification = "spark-hive-site"
      Properties = {
        "hive.metastore.client.factory.class" = "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
      }
    }
  ])

  tags = { Name = "healthcare-pipeline-${var.environment}" }
}

variable "environment" { type = string }
variable "vpc_id" { type = string }
variable "subnet_id" { type = string }
variable "master_instance" { type = string }
variable "core_instance" { type = string }
variable "core_count" { type = number }
variable "data_lake_bucket" { type = string }
variable "release_label" { type = string }
variable "applications" { type = list(string) }

output "cluster_id" { value = aws_emr_cluster.spark.id }
output "master_public_dns" { value = aws_emr_cluster.spark.master_public_dns }
