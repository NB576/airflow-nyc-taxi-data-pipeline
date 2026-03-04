from include.nyc_taxi.constants import S3_BUCKET, PAYMENT_MAP, RATECODE_MAP
from include.nyc_taxi.config import s3_fs, default_conn
from include.nyc_taxi.helpers import write_table_parquet, write_dataset_parquet
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType
from datetime import datetime

def main(start_date, end_date):
    spark = SparkSession.builder \
        .appName("nyc-taxi-curated-transform") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

    year = datetime.strptime(start_date, "%Y-%m-%d").year

    df_staging = spark.read.parquet(f"s3a://{S3_BUCKET}/staging/{year}/*/*.parquet")

    # TODO: your real logic
    build_facts_table(df_staging)
    # build_dim_date(start_date, end_date)
    # ...

    spark.stop()


def build_facts_table(df_staging):
    df_facts = df_staging \
        .withColumn("trip_id", F.sha2( # use min required columns to uniquely identify a trip
            F.concat_ws(
                "||",
                F.coalesce(F.col("VendorID").cast("string"), F.lit("NULL")),
                F.coalesce(F.col("tpep_pickup_datetime").cast("string"), F.lit("NULL")),
                F.coalesce(F.col("tpep_dropoff_datetime").cast("string"), F.lit("NULL")),
                F.coalesce(F.col("PULocationID").cast("string"), F.lit("NULL")),
                F.coalesce(F.col("DOLocationID").cast("string"), F.lit("NULL")),
                F.coalesce(F.col("RatecodeID").cast("string"), F.lit("NULL")),
                F.coalesce(F.col("payment_type").cast("string"), F.lit("NULL"))),
              256)) \
        .withColumn("pickup_date_id", F.dayofyear("tpep_pickup_datetime").cast(IntegerType())) \
        .withColumn("pickup_time_id", (F.col("pickup_hour") * 100 + F.col("pickup_dayofweek")* 10).cast(IntegerType())) \
        .withColumn("pickup_location_id", F.col("PULocationID").cast(IntegerType())) \
        .withColumn("dropoff_location_id", F.col("PULocationID").cast(IntegerType())) \
        .withColumn("payment_type_id", F.col("payment_type").cast(IntegerType())) \
        .withColumn("vendor_id", F.col("VendorID").cast(IntegerType()))
        
    final_cols = [
        "trip_id",
        "pickup_date_id",
        "pickup_time_id",
        "pickup_location_id",
        "dropoff_location_id",
        "payment_type_id",
        "vendor_id",
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "revenue",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "total_amount",
        "tip_rate",
        "fare_per_mile",
        "fare_per_minute",
        "trip_duration",
        "avg_speed_mph",
        "year",
        "month",
    ]

    df_facts.select(final_cols) \
        .write.mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(f"s3a://{S3_BUCKET}/curated/fact_yellow_tripdata/")
    
    print("")
    print(f"fact_yellow_tripdata written — {df_facts.count()} rows")
    print("")

def build_dim_date(start_date: str, end_date: str, spark: SparkSession):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    # generate a row per calendar day between start and end
    days = (end - start).days + 1
    date_df = spark.range(days) \
        .withColumn("full_date", F.date_add(F.lit(start_date), F.col("id").cast(IntegerType()))) \
        .withColumn("date_id", F.col("id").cast(IntegerType())) \
        .withColumn("year", F.year("full_date").cast(IntegerType())) \
        .withColumn("quarter", F.quarter("full_date").cast(IntegerType())) \
        .withColumn("month", F.month("full_date").cast(IntegerType())) \
        .withColumn("month_name", F.date_format("full_date", "MMMM")) \
        .withColumn("day", F.dayofmonth("full_date").cast(IntegerType())) \
        .withColumn("day_name", F.date_format("full_date", "EEEE")) \
        .withColumn("day_of_week", F.dayofweek("full_date").cast(IntegerType())) \
        .withColumn("weekend", F.when(F.dayofweek("full_date").isin([1,7]), 1).otherwise(0).cast(IntegerType())) \
        .drop("id")
    
    date_df.write.mode("overwrite") \
        .parquet(f"s3a://{S3_BUCKET}/curated/dim_date/")

    print("")
    print(f"dim_date written — {date_df.count()} rows")
    print("")

def build_dim_location(spark: SparkSession):
    df = spark.read.option("header", True).csv(f"s3a://{S3_BUCKET}/reference/taxi_zone_lookup.csv")

    dim_location = df.select(
        F.col("LocationID").cast(IntegerType()).alias("location_key"), 
        F.col("Borough").alias("borough"), 
        F.col("Zone").alias("zone")
    )

    dim_location.write.mode("overwrite") \
        .parquet(f"s3a://{S3_BUCKET}/curated/dim_location/")

def build_dim_payment(spark: SparkSession):
    rows = [
        (k, v, 1 if v == "Cash" else 0)
        for k,v in PAYMENT_MAP.items()
    ]

    dim_payment = spark.createDateframe(rows, ["payment_key", "payment_type_id", "payment_type", "is_cash"])

    dim_payment.write.mode("overwrite") \
        .parquet(f"s3a://{S3_BUCKET}/curated/dim_payment/")

    print("")
    print(f"dim_payment written — {dim_payment.count()} rows")
    print("")

def build_dim_time(spark: SparkSession):
    rows = []
    for h in range(24):
        for d in range(7):
            time_key = h * 100 + d * 10 # puts h in hundreds pos and d in tens pos ie 940 is 9am thursday
            part_of_day = (
                "Morning" if h < 12 else \
                "Afternoon" if h < 17 else \
                "Evening" if h < 22 else \
                "Night"
            )
            rows.append((time_key, part_of_day))

    dim_time = spark.createDataframe(rows, ["time_key", "hour", "day_of_week", "part_of_day"])

    spark.write.mode("overwrite").parquet(f"s3a://{S3_BUCKET}/curated/dim_time/")
    
    print("")
    print(f"dim_time written — {dim_time.count()} rows")
    print("")

def main(start_date: str, end_date: str):
    spark = SparkSession.builder \
        .appName("nyc-taxi-curated-transform") \
        .getOrCreate()

    year = datetime.strptime(start_date, "%Y-%m-%d").year
    print("")
    print(f"Reading staging data for year {year}...")
    print("")

    df_staging = spark.read.parquet(f"s3a://{S3_BUCKET}/staging/{year}/*/")

    print("")
    print(f"Loaded {df_staging.count()} rows from staging...")
    print("")

    build_facts_table(df_staging, spark)
    build_dim_date(start_date, end_date, spark)
    build_dim_location(spark)
    build_dim_payment(spark)
    build_dim_time(spark)

    spark.stop()

    print("")
    print("Curated transform complete.")            
    print("")

    

if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("Usage: curated_job.py <start_date> <end_date>")
        sys.exit(1)

    start_date = sys.argv[1]
    end_date   = sys.argv[2]

    main(start_date, end_date)
