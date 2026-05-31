resource "aws_s3_bucket" "data_lake" {
  bucket = "${var.bucket_prefix}-${var.environment}"

  tags = { Name = "${var.bucket_prefix}-${var.environment}" }
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  versioning_configuration {
    status = var.enable_versioning ? "Enabled" : "Suspended"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    id     = "bronze-lifecycle"
    status = "Enabled"
    filter { prefix = "bronze/" }

    transition {
      days          = var.lifecycle_rules.bronze_to_ia_days
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 180
      storage_class = "GLACIER"
    }
  }

  rule {
    id     = "silver-lifecycle"
    status = "Enabled"
    filter { prefix = "silver/" }

    transition {
      days          = var.lifecycle_rules.silver_to_ia_days
      storage_class = "STANDARD_IA"
    }
  }

  rule {
    id     = "gold-retention"
    status = "Enabled"
    filter { prefix = "gold/" }

    expiration {
      days = var.lifecycle_rules.gold_retention_days
    }
  }
}

# Bucket para checkpoints do Structured Streaming
resource "aws_s3_bucket" "checkpoints" {
  bucket = "${var.bucket_prefix}-checkpoints-${var.environment}"
  tags   = { Name = "${var.bucket_prefix}-checkpoints-${var.environment}" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "checkpoints" {
  bucket = aws_s3_bucket.checkpoints.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
  }
}

variable "environment" { type = string }
variable "bucket_prefix" { type = string }
variable "enable_versioning" { type = bool }
variable "lifecycle_rules" {
  type = object({
    bronze_to_ia_days   = number
    silver_to_ia_days   = number
    gold_retention_days = number
  })
}

output "bucket_name" { value = aws_s3_bucket.data_lake.id }
output "bucket_arn" { value = aws_s3_bucket.data_lake.arn }
output "checkpoints_bucket" { value = aws_s3_bucket.checkpoints.id }
