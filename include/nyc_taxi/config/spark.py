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
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    # only overwrite partitions being written to
    "spark.sql.sources.partitionOverwriteMode": "dynamic"

}