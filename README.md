# 🚕 NYC Yellow Taxi Data Pipeline

An end to end data pipeline that ingests NYC yellow taxi trip data from the TLC public dataset for a given year, transforms it through a medallion architecture, and produces a star schema ready for BI analysis.

---

## Table of Contents

- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Pipeline Walkthrough](#pipeline-walkthrough)
- [Design Decisions](#design-decisions)
- [Setup Instructions](#setup-instructions)
- [Known Limitations](#known-limitations)
- [Future Enhancements](#future-enhancements)

---

## Architecture

The pipeline follows a **medallion architecture** with three layers of data refinement:

```
NYC TLC Public Dataset
        │
        ▼
┌───────────────────┐
│   Raw / Bronze    │  Parquet files ingested from TLC API
│                   │  Partitioned by year / month
│  S3: /raw/        │  Data quality checks applied
└────────┬──────────┘
         │
         ▼
┌───────────────────┐
│ Staging / Silver  │  Cleaned, filtered, and enriched
│                   │  Derived columns added
│  S3: /staging/    │  Deduplication applied
└────────┬──────────┘
         │
         ▼
┌───────────────────┐
│  Curated / Gold   │  Star schema — facts + dimensions
│                   │  Surrogate keys generated
│  S3: /curated/    │  Referential integrity validated
└────────┬──────────┘
         │
         ▼
┌───────────────────┐
│   BI Layer        │  (Planned — see Future Enhancements)
│  AWS Athena +     │
│  Superset         │
└───────────────────┘
```

### Star Schema

```
                    dim_date
                       │
         dim_time ─────┤
                       │
dim_location ──── fact_yellow_tripdata ──── dim_payment
                       │
              dim_location (dropoff)
```

| Table | Description |
|---|---|
| `fact_yellow_tripdata` | Trip level facts — fares, distances, durations |
| `dim_date` | Calendar attributes — year, quarter, month, day, weekend flag |
| `dim_time` | Hour and day of week combinations with part of day label |
| `dim_location` | NYC taxi zone lookup — borough and zone name |
| `dim_payment` | Payment type mapping with cash flag |

---

## Tech Stack

| Tool | Purpose |
|---|---|
| **Apache Airflow** | Pipeline orchestration and scheduling |
| **Astronomer CLI** | Local Airflow development environment |
| **Apache Spark (PySpark)** | Distributed data transformation |
| **AWS S3** | Data lake storage across all medallion layers |
| **Terraform** | Infrastructure as code — AWS environment provisioning |
| **Docker** | Containerised local development |
| **Python** | Pipeline logic, data quality checks, helper utilities |

---

## Pipeline Walkthrough

The pipeline is orchestrated by Airflow and is triggered manually for a given year, configured via the `YEAR` constant in `constants.py`. Each run processes the full year of NYC yellow taxi data across all four stages.

### 1. Raw Layer — `raw_to_staging` task group

For each month of the configured year:

- Generates the TLC dataset download URL for that month
- Downloads the parquet file and uploads it to `s3://bucket/raw/year=YYYY/month=MM/`
- Runs data quality checks to validate the raw file meets acceptability thresholds before proceeding

### 2. Staging Layer — `staging_transform` Spark job

Reads raw parquet files month by month and applies:

- **Type casting** — pickup and dropoff datetimes converted to timestamps
- **Filtering** — removes invalid rows (negative fares, impossible trip durations, out of range location IDs, incorrect year)
- **Derived columns** — trip duration, pickup hour, day of week, weekend flag, tip rate, fare per mile, average speed
- **Categorical enrichment** — payment type and rate code names mapped from lookup dictionaries
- **Deduplication** — duplicate trip records present in raw TLC data removed using surrogate key
- Output written to `s3://bucket/staging/fact_yellow_tripdata/` partitioned by year and month

### 3. Curated Layer — `curated_transform` Spark job

Reads staging data month by month and builds the star schema:

- **`fact_yellow_tripdata`** — surrogate key generated via SHA2-256 hash of natural keys, foreign keys to all dimension tables
- **`dim_date`** — one row per calendar day for the configured year
- **`dim_time`** — 168 rows covering all hour × day of week combinations
- **`dim_location`** — 265 NYC taxi zones sourced from TLC reference CSV
- **`dim_payment`** — 7 payment types with cash flag

### 4. Curated Quality Checks — `curated_quality_checks` Spark job

Validates the curated layer before it is consumed downstream:

- Surrogate key uniqueness — no duplicate `trip_id` values
- No null surrogate keys
- Referential integrity — all foreign keys in facts exist in dimension tables
- Row count threshold — catches silent pipeline failures
- Cross month duplicate detection

---

## Design Decisions

### SHA2-256 Surrogate Keys

Surrogate keys are generated as SHA2-256 hashes of the minimum set of natural key columns that uniquely identify a trip (`VendorID`, pickup datetime, dropoff datetime, pickup location, dropoff location, rate code, payment type).

This approach ensures:
- **Stability** — the same trip always produces the same key regardless of when the pipeline runs
- **Reproducibility** — keys can be regenerated from source data without a sequence generator
- **Consistency** — safe to use across pipelines and systems without coordination

### Partitioning by Year and Month

All parquet files across the raw, staging, and curated layers are partitioned by `year` and `month`. This enables Spark's partition pruning to skip irrelevant data at read time — a downstream job filtering for a single month only reads that month's files, never touching the rest.

### Month by Month Processing

Data is processed one month at a time rather than loading a full year into memory at once. This is a deliberate constraint for local development — a full year of NYC taxi data (~41M rows) exceeds the memory available in a local Docker environment. In a production environment on a properly sized cluster, the full dataset would be processed in a single pass.

### Dynamic Partition Overwrite

Spark is configured with `spark.sql.sources.partitionOverwriteMode=dynamic`. This means each write only overwrites the specific partition being written, leaving all other partitions untouched. This makes the pipeline safe to rerun without duplicating or losing data — a core requirement for idempotent pipelines.

### Infrastructure as Code with Terraform

AWS infrastructure is provisioned and managed using Terraform rather than manually through the AWS console. This ensures the environment is reproducible, version controlled, and can be torn down and recreated consistently:

```hcl
# S3 bucket with versioning enabled
resource "aws_s3_bucket" "data_bucket" {
    bucket = "nyc-taxi-project-112025"
}
```

Versioning is enabled on the S3 bucket so that overwritten parquet files can be recovered if needed — an important safety net when running a pipeline with `overwrite` write mode.

### Deduplication at Staging

The NYC TLC raw dataset contains occasional duplicate trip records within the same monthly file. Deduplication is applied at the staging layer using the surrogate key hash before writing to S3. This ensures clean data flows through the entire pipeline and the surrogate key uniqueness constraint is never violated at the curated layer.

---

## Setup Instructions

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (minimum 10GB memory allocated)
- [Astronomer CLI](https://docs.astronomer.io/astro/cli/install-cli)
- [Terraform](https://developer.hashicorp.com/terraform/install) (v1.0+)
- [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html)
- AWS account with S3 access
- AWS credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)

### 1. Clone the repository

```bash
git clone https://github.com/your-username/airflow-nyc-taxi-data-pipeline.git
cd airflow-nyc-taxi-data-pipeline
```

### 2. Configure AWS CLI credentials

Terraform uses the AWS CLI `default` profile to authenticate. Configure it with your AWS credentials:

```bash
aws configure
```

You will be prompted for:

```
AWS Access Key ID:     your_access_key
AWS Secret Access Key: your_secret_key
Default region name:   us-east-1
Default output format: json
```

This creates a `~/.aws/credentials` file that Terraform reads automatically via the `default` profile configured in `main.tf`.

### 3. Provision AWS infrastructure with Terraform

```bash
cd terraform
terraform init
terraform plan
terraform apply
```

This creates:
- S3 bucket `nyc-taxi-project-112025` with versioning enabled
- `raw/` and `curated/` folder structure

After applying, manually upload the TLC reference file:
```
s3://nyc-taxi-project-112025/reference/taxi_zone_lookup.csv
```

### 4. Configure environment variables

Create a `.env` file in the project root:

```bash
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
S3_BUCKET=nyc-taxi-project-112025
YEAR=2024
```

### 5. Start Airflow

```bash
astro dev start
```

### 6. Configure Airflow connections

In the Airflow UI (`http://localhost:8080`):

- Add connection `aws_default` with your AWS credentials
- Add connection `spark_default` with master set to `local[4]`

### 7. Trigger the DAG

In the Airflow UI, enable the `nyc_taxi` DAG and trigger it manually. The pipeline is designed to be triggered on demand for a given year rather than run on a schedule — update the `YEAR` constant in `constants.py` before triggering.

---

## Known Limitations

**Single year scope** — the pipeline is designed to process one year of data at a time, triggered manually with the year configured via the `YEAR` constant in `constants.py`. The DAG schedule is set to `None` to prevent automatic triggering. Extending to multi-year would require parameterising the year argument and handling potential schema drift between years — both straightforward extensions.

**Local development memory constraints** — processing is done month by month due to the memory available in a local Docker environment. On a production cluster with adequate memory, the full dataset would be processed in a single pass with significantly better performance.

**No BI layer** — the curated star schema is ready for BI consumption but a dashboard has not yet been implemented. See Future Enhancements below.

---

## Future Enhancements

- **BI Dashboard** — connect AWS Athena to query the curated parquet files and build dashboards in Apache Superset covering trip volume trends, fare analysis, and pickup/dropoff location patterns
- **AWS Certification** — migrate from local Spark to AWS Glue or EMR for scalable cloud-native processing
- **dbt integration** — replace curated Spark transforms with dbt models for better SQL-based lineage and testing
- **Multi-year support** — extend the pipeline to process multiple years with a date range argument
- **Schema validation** — add explicit schema validation checks at each medallion layer boundary
- **Unit tests** — add pytest coverage for helper functions and transform logic
- **CI/CD** — add GitHub Actions workflow to run tests on every push