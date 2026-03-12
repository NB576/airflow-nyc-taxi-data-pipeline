from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ShortType, FloatType
from datetime import datetime
from include.nyc_taxi.jobs import helpers
from include.nyc_taxi.constants import PAYMENT_MAP, RATECODE_MAP, S3_BUCKET, YEAR

def staging_transform(df_raw, year: int):

     # convert pickup, dropoff date columns to timestamp and then drop null rows
    df_staging = df_raw \
        .withColumn("tpep_pickup_datetime", F.to_timestamp("tpep_pickup_datetime")) \
        .withColumn("tpep_dropoff_datetime", F.to_timestamp("tpep_dropoff_datetime")) \
        .dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime"])
    
    # add additional columns required for filtering
    df_staging = df_staging \
        .withColumn("trip_duration_minutes", (F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long")) / 60) \
        .withColumn("year", F.year("tpep_pickup_datetime")) \
        .withColumn("month", F.month("tpep_pickup_datetime"))
    
    # filter rows based on required column constraints
    df_staging = df_staging.filter(
        (F.col("tpep_pickup_datetime") < F.col("tpep_dropoff_datetime")) &
        (F.col("passenger_count").between(1, 6)) &
        (F.col("trip_distance") > 0) &
        (F.col("fare_amount") > 0) &
        (F.col("total_amount") > 0) &
        (F.col("trip_duration_minutes").between(1, 120)) &
        (F.col("PULocationID").between(1, 265)) &
        (F.col("DOLocationID").between(1, 265)) &
        (F.col("year") == year)
    )

    # add additional time based columns
    df_staging = df_staging \
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime")) \
        .withColumn("pickup_dayofweek", F.dayofweek("tpep_pickup_datetime")) \
        .withColumn("pickup_weekend", F.when(F.col("pickup_dayofweek").isin([1,7]), 1).otherwise(0))
    
    # add additional monetary ratio and revenue columns
    df_staging = df_staging \
        .withColumn("tip_rate", F.col("tip_amount") / F.col("fare_amount")) \
        .withColumn("fare_per_mile", F.col("fare_amount") / F.col("trip_distance")) \
        .withColumn("fare_per_minute", F.col("fare_amount") / F.col("trip_duration_minutes")) \
        .withColumn("revenue", F.col("fare_amount") + F.col("extra") + F.col("tip_amount"))
    
    # categorical enrichment
    df_staging = df_staging \
        .withColumn("payment_type_name", helpers.map_col(mapping=PAYMENT_MAP, input_col="payment_type")) \
        .withColumn("rate_code_name", helpers.map_col(mapping=RATECODE_MAP, input_col="RatecodeID"))
    
    # enrich with trip efficiency measure column
    df_staging = df_staging.withColumn("avg_speed_mph", F.col("trip_distance") / (F.col("trip_duration_minutes") / 60))

    # final staging df column selection, casting to respective types
    df_staging = df_staging.select(
        F.col("VendorID").cast(ShortType()),
        F.col("tpep_pickup_datetime"),
        F.col("tpep_dropoff_datetime"),
        F.col("pickup_hour").cast(ShortType()),
        F.col("pickup_dayofweek").cast(ShortType()),
        F.col("pickup_weekend").cast(ShortType()),
        F.col("passenger_count").cast(ShortType()),
        F.col("trip_distance").cast(FloatType()),
        F.col("PULocationID").cast(ShortType()),
        F.col("DOLocationID").cast(ShortType()),
        F.col("RatecodeID").cast(ShortType()),
        F.col("rate_code_name"),
        F.col("payment_type").cast(ShortType()),
        F.col("payment_type_name"),
        F.col("fare_amount").cast(FloatType()),
        F.col("extra").cast(FloatType()),
        F.col("mta_tax").cast(FloatType()),
        F.col("tip_amount").cast(FloatType()),
        F.col("tolls_amount").cast(FloatType()),
        F.col("total_amount").cast(FloatType()),
        F.col("tip_rate").cast(FloatType()),
        F.col("fare_per_mile").cast(FloatType()),
        F.col("fare_per_minute").cast(FloatType()),
        F.col("trip_duration_minutes").cast(FloatType()),
        F.col("avg_speed_mph").cast(FloatType()),
        F.col("revenue").cast(FloatType()),
        F.col("year").cast(ShortType()),
        F.col("month").cast(ShortType()),
    )

    # partitionBy required (even though processing one month's data at a time) to facilitate read efficiency
    df_staging.write.mode("append") \
        .partitionBy("year", "month") \
        .parquet(f"s3a://{S3_BUCKET}/staging/"
    )


def main(year: str):
    spark = SparkSession.builder \
        .appName("nyc-taxi-staging-transform") \
        .getOrCreate()
    
    for month in range(1, 13):
        print("")
        print(f"Processing {year}/{month}...")
        df_raw = spark.read.parquet(f"s3a://{S3_BUCKET}/raw/{year}/{month:02d}/")
        staging_transform(df_raw, int(year))
        print(f"Completed staging transform for {year}/{month}")
        print("")

    spark.stop()

    print("")
    print("Staging transform complete")            
    print("")

if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: staging_spark_job.py <year>")
        sys.exit(1)
    
    year = sys.argv[1]

    main(year)
    
