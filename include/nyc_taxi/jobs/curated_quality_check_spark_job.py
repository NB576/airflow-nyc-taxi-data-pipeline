from include.nyc_taxi.constants import S3_BUCKET, PAYMENT_MAP, RATECODE_MAP
from include.nyc_taxi.config import s3_fs, default_conn
from include.nyc_taxi.helpers import write_table_parquet, write_dataset_parquet
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType
from datetime import datetime


def main(year: str):
    spark = SparkSession.builder \
        .appName("curated_quality_checks") \
        .getOrCreate()
    
    df_dim_payment = spark.read.parquet(f"s3a://{S3_BUCKET}/curated/dim_payment/")
    df_dim_date = spark.read.parquet(f"s3a://{S3_BUCKET}/curated/dim_date/")
    df_dim_location = spark.read.parquet(f"s3a://{S3_BUCKET}/curated/dim_location/")
    df_dim_time = spark.read.parquet(f"s3a://{S3_BUCKET}/curated/dim_time/")

    # perform curated quality check on each monthly file (due to performance constraints)
    for month in range(1, 13):
        fact_df = spark.read.parquet(f"s3a://{S3_BUCKET}/curated/fact_yellow_tripdata/") \
            .filter(F.col("year") == int(year)) \
            .filter(F.col("month") == month)
        
        curated_quality_checks(df_fact=fact_df,
                               df_dim_payment=df_dim_payment,
                               df_dim_date=df_dim_date,
                               df_dim_location=df_dim_location,
                               df_dim_time=df_dim_time
                               )

    # duplicate_count = spark.read \
    #     .parquet(f"s3a://{S3_BUCKET}/curated/fact_yellow_tripdata/") \
    #     .filter(F.col("year") == int(year)) \
    #     .groupBy("trip_id") \
    #     .count() \
    #     .filter(F.col("count") > 1) \
    #     .count()

    # if duplicate_count > 0:
    #     raise ValueError(f"Cross month duplicate trip_ids: {duplicate_count} duplicates found")

    # print("")
    # print(f"Cross month uniqueness check passed")
    # print("")


def curated_quality_checks(df_fact, df_dim_payment, df_dim_date, df_dim_location, df_dim_time):
    failed_checks = []

    # payments foreign key integrity check
    missing_payments = df_fact.select("payment_type_id").distinct() \
                        .subtract(df_dim_payment.select("payment_key").distinct())
    
    if missing_payments.count() != 0:
        failed_checks.append(f"Facts table references payment_type_id not in dim_payments table (count={missing_payments.count()})")

    # pickup location foreign key integrity check
    missing_locations_pickup = df_fact.select(["pickup_location_id"]).distinct() \
                        .subtract(df_dim_location.select("location_key").distinct())
    
    if missing_locations_pickup.count() != 0:
        failed_checks.append(f"Facts table has pickup_location_id not in dim_locations table (count={missing_locations_pickup.count()})")

    # dropoff location foreign key integrity check
    missing_locations_dropoff = df_fact.select(["dropoff_location_id"]).distinct() \
                        .subtract(df_dim_location.select("location_key").distinct())

    if missing_locations_dropoff.count() != 0:
        failed_checks.append(f"Facts table has pickup_location_id or dropoff_location_id not in dim_locations table (count={missing_locations_dropoff.count()})")
    
    # time foreign key integrity check
    missing_time = df_fact.select("pickup_time_id").distinct() \
                        .subtract(df_dim_time.select("time_key").distinct())
    
    if missing_time.count() != 0:
        failed_checks.append(f"Facts table references pickup_time_id not in dim_time table (count={missing_time.count()})")
    
    # date foreign key integrity check
    missing_date = df_fact.select("pickup_date_id").distinct() \
                        .subtract(df_dim_date.select("date_key").distinct())
    
    if missing_date.count() != 0:
        print("")
        print("")
        missing_date.show()
        print("")
        print("")

        failed_checks.append(f"Facts table references pickup_date_id not in dim_date table (count={missing_date.count()})")

    if failed_checks:
        raise ValueError(f"Curated quality checks failed: {failed_checks}") 
    
if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: curated_quality_check_spark_job.py <year>")
        sys.exit(1)
    
    year = sys.argv[1]
    main(year)