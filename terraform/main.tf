provider "aws" {
    profile = "default"
    region  = "us-east-1"
}

# ─────────────────────────────────────────────
# S3 BUCKET
# ─────────────────────────────────────────────

resource "aws_s3_bucket" "data_bucket" {
    bucket = "nyc-taxi-project-112025"

    lifecycle {
      prevent_destroy = true
    }
}

resource "aws_s3_bucket_versioning" "data_bucket" {
    bucket = aws_s3_bucket.data_bucket.id
    versioning_configuration {
        status = "Enabled"
    }
}

resource "aws_s3_object" "reference" {
    bucket = aws_s3_bucket.data_bucket.id
    key    = "reference/"

   lifecycle {
        prevent_destroy = true
    }
}

resource "aws_s3_object" "raw_folder" {
    bucket = aws_s3_bucket.data_bucket.id
    key    = "raw/"

   lifecycle {
        prevent_destroy = true
    }
}

resource "aws_s3_object" "staging_folder" {
    bucket = aws_s3_bucket.data_bucket.id
    key    = "staging/"

   lifecycle {
        prevent_destroy = true
    }
}

resource "aws_s3_object" "curated_folder" {
    bucket = aws_s3_bucket.data_bucket.id
    key    = "curated/"
    
   lifecycle {
        prevent_destroy = true
    }
}

# folder for Athena query results
resource "aws_s3_object" "athena_results_folder" {
    bucket = aws_s3_bucket.data_bucket.id
    key    = "athena-results/"
}

# ─────────────────────────────────────────────
# Athena
# ─────────────────────────────────────────────

# workgroup to configure where Athena stores query results
resource "aws_athena_workgroup" "nyc_taxi" {
    name = "nyc-taxi"

    configuration {
        result_configuration {
            output_location = "s3://${aws_s3_bucket.data_bucket.bucket}/athena-results/"
        }
    }
}

# ─────────────────────────────────────────────
# GLUE DATA CATALOG
# ─────────────────────────────────────────────

resource "aws_glue_catalog_database" "nyc_taxi_staging" {
    name = "nyc_taxi_staging"
}


# ─────────────────────────────────────────────
# IAM ROLE FOR GLUE CRAWLER
# ─────────────────────────────────────────────

# role that allows Glue to access S3 and write to the data catalog
resource "aws_iam_role" "glue_crawler_role" {
    name = "nyc-taxi-staging-glue-crawler-role"

  # Trust policy that specifies who (glue.amazonaws.com) can assume this role 
  # and what actions they can perform when they do
    assume_role_policy = jsonencode({
        Version = "2012-10-17"
        Statement = [{
            Effect    = "Allow"
            Principal = { Service = "glue.amazonaws.com" }
            Action    = "sts:AssumeRole"
        }]
    })
}

# attaches predefined AWS managed policy that grants Glue permissions to do its core job — 
# writing to the Glue Data Catalog, logging to CloudWatch, etc
resource "aws_iam_role_policy_attachment" "glue_service_policy" {
    role       = aws_iam_role.glue_crawler_role.name
    policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# attach policy to role to allow Glue crawler to read from S3 bucket 
resource "aws_iam_role_policy" "glue_s3_policy" {
    name = "nyc-taxi-staging-glue-s3-policy"
    role = aws_iam_role.glue_crawler_role.id

    policy = jsonencode({
        Version = "2012-10-17"
        Statement = [{
            Effect = "Allow"
            Action = [
                "s3:GetObject",
                "s3:ListBucket"
            ]
            Resource = [
                aws_s3_bucket.data_bucket.arn,
                "${aws_s3_bucket.data_bucket.arn}/*"
            ]
        }]
    })
}

# ─────────────────────────────────────────────
# GLUE CRAWLERS
# ─────────────────────────────────────────────

resource "aws_glue_crawler" "staging_crawler" {
    name          = "nyc-taxi-staging-crawler"
    role          = aws_iam_role.glue_crawler_role.arn
    database_name = aws_glue_catalog_database.nyc_taxi_staging.name

    s3_target {
        path = "s3://${aws_s3_bucket.data_bucket.bucket}/staging/stg_yellow_tripdata/"
    }
}
