from pendulum import datetime

S3_BUCKET = "nyc-taxi-project-112025"

YEAR = 2024

MIN_ROWS_PER_MONTH = 100_000

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Sec-Fetch-Mode": "no-cors",
    "Sec-Fetch-Site": "none"
}

PAYMENT_MAP = { 0: "Flex Fare", 1: "Credit", 2: "Cash", 3: "No Charge", 4: "Dispute", 5: "Unknown", 6: "Voided" }
RATECODE_MAP = { 1: "Standard", 2: "JFK", 3: "Newark", 4: "Nassau/Westchester", 5: "Negotiated", 6: "Group ride", 99: "Null/Unknown" }

SPARK_CONF = {
    "spark.master": "local[4]",
    # let Spark's memory manager handle the internal memory division between driver and executor
    "spark.driver.memory": "5g", 
    # caps result collection — kept low as pipeline has no collect/count operations
    "spark.driver.maxResultSize": "2g",
    # overrides the default S3A auth order with AWS SDK's official sequence of credential sources so that env vars checked first
    "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    # stream data as its generated instead of buffering it all first
    "spark.hadoop.fs.s3a.fast.upload": "true",
    # use disk as upload buffer instead of RAM (required due to local machine resource constraints)
    "spark.hadoop.fs.s3a.fast.upload.buffer": "disk",
    # uploads in 100MB chunks — more reliable than one giant upload
    "spark.hadoop.fs.s3a.multipart.size": "104857600",
    # allows more concurrent S3 connections for faster parallel writes
    "spark.hadoop.fs.s3a.connection.maximum": "100",
    # optimise query performance using spark adaptive sql
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true"
}