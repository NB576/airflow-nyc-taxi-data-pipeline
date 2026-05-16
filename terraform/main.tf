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

# database to hold all nyc taxi tables
resource "aws_glue_catalog_database" "nyc_taxi" {
    name = "nyc_taxi"
}

resource aws_glue_catalog_table "fact_yellow_tripdata" {
    name          = "fact_yellow_tripdata"
    database_name = aws_glue_catalog_database.nyc_taxi.name
    table_type    = "EXTERNAL_TABLE" # data is stored outside of Glue (s3) metadata stored in Glue Data Catalog
    
    # required - defines how the data is stored in S3 and how to read it
    storage_descriptor {
        location      = "s3://${aws_s3_bucket.data_bucket.bucket}/curated/fact_yellow_tripdata/"
        input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
        output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

        ser_de_info {
            serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
        }
    }

    # manage partition keys to ensure type is set to int (after terraform apply) as crawler defaults to string
    partition_keys {
      name = "year"
      type = "int"
    }
    partition_keys {
        name = "month"
        type = "int"
    }


    lifecycle {
      # This prevents Terraform from overwriting crawler-driven changes to columns
      ignore_changes = [ 
        storage_descriptor[0].columns,
        # ignore changes to the number_of_buckets, compressed flags which is sometimes changed when crawler runs 
        # which leads to phantom drift issue which terraform attempts to fix on every apply
        storage_descriptor[0].number_of_buckets,
        storage_descriptor[0].compressed
       ]
    }
}

# ─────────────────────────────────────────────
# IAM ROLE FOR GLUE CRAWLER
# ─────────────────────────────────────────────

# role that allows Glue to access S3 and write to the data catalog
resource "aws_iam_role" "glue_crawler_role" {
    name = "nyc-taxi-glue-crawler-role"

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
    name = "nyc-taxi-glue-s3-policy"
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

# crawler for facts table
resource "aws_glue_crawler" "fact_yellow_tripdata" {
    name          = "nyc-taxi-fact-yellow-tripdata"
    role          = aws_iam_role.glue_crawler_role.arn
    database_name = aws_glue_catalog_database.nyc_taxi.name

    s3_target {
        path = "s3://${aws_s3_bucket.data_bucket.bucket}/curated/fact_yellow_tripdata/"
    }

    schema_change_policy {
        # is default but specified explicitly to avoid accidental changes to table schema from crawler runs
        update_behavior = "UPDATE_IN_DATABASE"  
        delete_behavior = "LOG"
    }

    # tells crawler to infer partitions from year=/month= folder structure
    configuration = jsonencode({
        Version = 1.0
        CrawlerOutput = {
            Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
        }
        Grouping = {
            TableGroupingPolicy     = "CombineCompatibleSchemas"
            TableLevelConfiguration = 3  # treat everything under this path as one table
            # s3://bucket/curated/fact_yellow_tripdata/year=2024/month=1/file.parquet
            #    level 1  level 2     level 3             level 4
        }
    })

    
}

# crawler for dim_date
resource "aws_glue_crawler" "dim_date" {
    name          = "nyc-taxi-dim-date"
    role          = aws_iam_role.glue_crawler_role.arn
    database_name = aws_glue_catalog_database.nyc_taxi.name

    s3_target {
        path = "s3://${aws_s3_bucket.data_bucket.bucket}/curated/dim_date/"
    }
}

# crawler for dim_time
resource "aws_glue_crawler" "dim_time" {
    name          = "nyc-taxi-dim-time"
    role          = aws_iam_role.glue_crawler_role.arn
    database_name = aws_glue_catalog_database.nyc_taxi.name

    s3_target {
        path = "s3://${aws_s3_bucket.data_bucket.bucket}/curated/dim_time/"
    }
}

# crawler for dim_location
resource "aws_glue_crawler" "dim_location" {
    name          = "nyc-taxi-dim-location"
    role          = aws_iam_role.glue_crawler_role.arn
    database_name = aws_glue_catalog_database.nyc_taxi.name

    s3_target {
        path = "s3://${aws_s3_bucket.data_bucket.bucket}/curated/dim_location/"
    }
}

# crawler for dim_payment
resource "aws_glue_crawler" "dim_payment" {
    name          = "nyc-taxi-dim-payment"
    role          = aws_iam_role.glue_crawler_role.arn
    database_name = aws_glue_catalog_database.nyc_taxi.name

    s3_target {
        path = "s3://${aws_s3_bucket.data_bucket.bucket}/curated/dim_payment/"
    }
}
