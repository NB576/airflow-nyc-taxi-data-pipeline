from pendulum import datetime
from include.nyc_taxi.constants import S3_BUCKET, PAYMENT_MAP, RATECODE_MAP
from include.nyc_taxi.utils import get_storage_options
from include.nyc_taxi.config import s3_fs, default_conn
from include.nyc_taxi.helpers import write_table_parquet, write_dataset_parquet
from airflow.models import Connection
import include.nyc_taxi.errors as errors
import pyarrow as pa
import pandas as pd
import numpy as np


def generate_monthly_dates(start_date, end_date):
    months = []
    current = start_date
    while current <= end_date:
        months.append(current.strftime('%Y-%m'))
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    
    return months

def run_data_quality_checks(year_month, 
                            min_rows=10000, 
                            max_col_null_pct=0.01, 
                            max_total_null_pct = 0.01,
                            max_neg_duration_pct=0.01):
    """
    Simple quality checks on raw Parquet files in S3:
    - required columns are present and in expected data types.
    - row count exceeds minimum row threshold.
    - no null pickup/dropoff timestamps.
    - non-negative trip durations below specified threshold
    - file schema validation
    - column and total null percentages are within specified threshold.
    """
    
    # check each available month has a file present in s3
    split = year_month.split("-")

    s3_prefix = f'raw/{split[0]}/{split[1]}/'
    # best practice to ensure only one ending "/"
    s3_key = f"{S3_BUCKET}/{s3_prefix}".rstrip("/") + "/" 

    default_conn = Connection.get_connection_from_secrets('aws_default')

    key = s3_fs.ls(f"s3://{s3_key}")
    if not key:
        raise errors.DataSourceMissingError(f"No Parquet files found under s3://{s3_key}")

    # Build full S3 URL as fs.open requires path to be "s3://bucket/key"
    storage_options = {
        "key": default_conn.login,
        "secret": default_conn.password,
        "token": default_conn.extra_dejson.get("session_token")
        }     
    df = pd.read_parquet(path=f"s3://{s3_key}",
                        storage_options=storage_options)

    # schema validation check on required columns
    required_columns = {
        "tpep_pickup_datetime", "tpep_dropoff_datetime", 
        "trip_distance", "fare_amount", "total_amount",
        "PULocationID", "DOLocationID"
    }

    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise errors.ColumnNotFoundError(col=missing_columns, source=s3_key)
    
    # Ensure correct dtypes for parquet efficiency
    dtype_mapping = {
        'tpep_pickup_datetime': 'datetime64[ns]',
        'tpep_dropoff_datetime': 'datetime64[ns]',
        'passenger_count': 'float64',
        'trip_distance': 'float32',
        'fare_amount': 'float32',
        'total_amount': 'float32',
        'PULocationID': 'int32',
        'DOLocationID': 'int32'
    }
    
    dtype_errors = []
    for col, dtype in dtype_mapping.items():
        if col in df.columns:
            try:
                df[col] = df[col].astype(dtype)
            except:
                dtype_errors.append(f"Schema error: column '{col}' has dtype '{df[col].dtype}', expected '{dtype_mapping[col]}'")
    
    if dtype_errors:     
        msg = '\n'.join(dtype_errors)
        raise errors.SchemaValidationError(msg)
    
    # minimum rows per file check
    file_row_count = len(df)
    if file_row_count < min_rows:
        raise errors.MiniumumRowsError(file_row_count, min_rows)


    pickup_col = "tpep_pickup_datetime"
    dropoff_col = "tpep_dropoff_datetime"

    # convert columns to datetime format, non conforming entries set to na to give correct null count
    df[pickup_col] = pd.to_datetime(df[pickup_col], errors="coerce")
    df[dropoff_col] = pd.to_datetime(df[dropoff_col], errors="coerce")

    pickup_null_count = df[pickup_col].isna().sum()
    dropoff_null_count = df[dropoff_col].isna().sum()
    
    if pickup_null_count > 0:
        raise errors.NonNullColumnError(col=pickup_col, null_count=pickup_null_count)
    if dropoff_null_count > 0:
        raise errors.NonNullColumnError(col=dropoff_col, null_count=dropoff_null_count)

    # create new col "trip duration" for trip duration in minutes
    df["trip_duration_seconds"] = (df[dropoff_col] - df[pickup_col]).dt.total_seconds() / 60

    # check each column's null percentage is below specified limit
    # check total null percentage is below specified limit
    req_cols_total_null_count = 0
    null_error_cols = []
    for col in required_columns:
        null_count = df[col].isna().sum() 
        null_pct = null_count / file_row_count if file_row_count > 0 else 0
        req_cols_total_null_count += null_count
        if null_pct > max_col_null_pct:
            null_error_cols.append(null_error_cols)
    
    if null_error_cols:
        raise errors.NullThresholdError(cols=null_error_cols, threshold_pct=max_col_null_pct)
    if file_row_count > 0:
        total_null_pct = req_cols_total_null_count / file_row_count
        if total_null_pct > max_total_null_pct:
            raise errors.TotalNullsThresholdError(total_null_threshold_pct=max_total_null_pct, total_null_pct=total_null_pct)
    
    #check for negative duration values
    neg_duration_count = len(df[df["trip_duration_seconds"] < 0])   
    neg_duration_pct = neg_duration_count / file_row_count if file_row_count else 0

    # raise error if neg duration threshold exceeded
    if neg_duration_pct > max_neg_duration_pct:
        raise errors.NegativeDurationThresholdError(neg_duration_pct, max_neg_duration_pct)

    return s3_key

def run_transform_raw_to_staging(year_month, s3_key):
    year_month_split = year_month.split("-")
    year = year_month_split[0]
    month = year_month_split[1]

    df = pd.read_parquet(path=f"s3://{s3_key}",
                            storage_options=get_storage_options()
                            )

    # drop invalid datetime rows
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"], errors="coerce")
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'], errors="coerce")
    
    df = df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime"])
    
    # create cols used in invalid row filter)
    df["trip_duration"] = (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).dt.total_seconds() / 60
    df["year"] = df['tpep_pickup_datetime'].dt.year
    df["month"] = df['tpep_pickup_datetime'].dt.month

    # Filter out invalid rows
    df = df[(df["tpep_pickup_datetime"] < df["tpep_dropoff_datetime"]) &            
            (df["passenger_count"].between(1,6)) &
            (df["trip_distance"] > 0) &
            (df["fare_amount"] > 0) &
            (df["total_amount"] > 0) &
            (df["year"] == int(year)) &
            (df["month"] == int(month)) &
            (df["trip_duration"].between(1, 120)) &
            (df["PULocationID"].between(1, 265)) &
            (df["DOLocationID"].between(1, 265))]

    # enrich with additional time based columns
    df["pickup_hour"] = df['tpep_pickup_datetime'].dt.hour
    df["pickup_dayofweek"] = df['tpep_pickup_datetime'].dt.dayofweek  # 0=Mon, 6=Sun
    df["pickup_weekend"] = df["pickup_dayofweek"].isin([5,6]).astype(int)

    # enrich with monetary ratio columns
    df["tip_rate"] = df["tip_amount"] / df["fare_amount"]
    df["fare_per_mile"] = df["fare_amount"] / df["trip_distance"]
    df["fare_per_minute"] = df["fare_amount"] / df["trip_duration"]
    
    # add revenue column
    df["revenue"] = df["fare_amount"] + df["extra"] + df["tip_amount"]

    #  enrich with categorical standardization (converts payment_type, ratecode column encodings)
    df["payment_type_name"] = df["payment_type"].map(PAYMENT_MAP)
    df["rate_code_name"] = df["RatecodeID"].map(RATECODE_MAP)
    
    # enrich with trip efficiency measure column
    df["avg_speed_mph"] = df["trip_distance"] / (df["trip_duration"] / 60)

    # final curated column selection and type casting
    curated_columns = [
        "VendorID", 
        "tpep_pickup_datetime", 
        "tpep_dropoff_datetime",
        "pickup_hour",
        "pickup_dayofweek", 
        "pickup_weekend",
        "passenger_count", 
        "trip_distance", 
        "PULocationID", 
        "DOLocationID",
        "RatecodeID", 
        "rate_code_name", 
        "payment_type", 
        "payment_type_name",
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
        "month"
    ]

    # create copy to avoid pandas SettingsWithCopy warning in logs (modifing a view of a df)
    df_staging = df[curated_columns].copy()

    # cast columns to appropriate data types for consistent typing across staging, curated layers
    int_small_cols = [ 
        "VendorID", "pickup_hour", "pickup_dayofweek", "pickup_weekend",
        "passenger_count", "PULocationID", "DOLocationID",  
        "RatecodeID", "year", "month", "payment_type", 
    ]
    
    cat_cols = [
        "payment_type_name", "rate_code_name"
    ]

    float_cols = [
        "trip_distance", "trip_duration", "fare_amount", "extra", 
        "mta_tax", "tip_amount", "tolls_amount", "total_amount",
        "tip_rate", "fare_per_mile", "fare_per_minute", "avg_speed_mph"
    ]

    for c in int_small_cols:
        df_staging[c] = df_staging[c].astype("int16")

    for c in cat_cols:
        df_staging[c] = df_staging[c].astype("category")

    for c in float_cols:
        df_staging[c] = df_staging[c].astype("float32")

    staging_path_dir = f"staging/{year}/{month}"
    filename = f"yellow_tripdata_{year}-{month}"
    # staging_path = staging_path_dir + f"yellow_tripdata_{year}-{month}.parquet"
    
    return write_table_parquet(staging_path_dir, filename, df_staging)

    # if not s3_fs.glob(f"{staging_path_prefix}/*.parquet"):
    #     table = pa.Table.from_pandas(df_staging)
    #     with s3_fs.open(staging_path, "wb") as f:
    #         pa.parquet.write_table(table, f)


def run_staging_to_curated(start_date, end_date, file_paths):

    dataset = pa.ParquetDataset(file_paths, s3_fs)
    table = dataset.read()
    df_staging = table.to_pandas()

    build_facts_table(df_staging)
    build_dim_date(start_date, end_date)
    build_dim_location()
    build_dim_time()
    build_dim_payment()

    # build_data_marts(df_staging_complete)


def build_facts_table(df_staging):

    df_facts = df_staging.copy()

    # add surrogate key
    df_facts["trip_id"] = range(1, len(df_facts) + 1)

    # add FKs to dimensions
    df_facts["pickup_date_id"] = df_facts["tpep_pickup_datetime"].dt.date
    df_facts["pickup_date_id"] = df_facts["tpep_pickup_datetime"].dt.dayofyear # dim_date FK
    df_facts["pickup_time_id"] = df_facts["pickup_hour"] * 100 + df_facts["pickup_dayofweek"] * 10
    df_facts["pickup_location_id"] = df_facts["PULocationID"]
    df_facts["pickup_dropoff_id"] = df_facts["DOLocationID"]
    df_facts["payment_type_id"] = df_facts["payment_type"]
    df_facts["vendor_id"] = df_facts["VendorID"]

    # add additional business metric dimensions

    # final table column list
    final_column_list = [
        # key columns
        "trip_id",
        "pickup_date_id",
        "pickup_time_id",
        "pickup_location_id",
        "pickup_dropoff_id",
        "payment_type_id",      
        # identifier columns
        "vendor_id",     
       # numerical columns
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
    ]

    df_facts_final = df_facts[final_column_list].copy()

    # cast types for consistency
    df_facts_final["trip_id"] = df_facts_final["trip_id"].astype("int64")
    int_cols = ["pickup_date_id", "pickup_time_id", "pickup_location_id", 
                "dropoff_location_id", "payment_type_id", "vendor_id"]
    
    for col in int_cols:
        df_facts_final[col] = df_facts_final[col].astype('int32')

    write_dataset_parquet("curated/fact_yellow_tripdata", df_facts_final, ["year", "month"])
   
    # table = pa.Table.from_pandas(df_facts_final)
    # pa.parquet.write_to_dataset(
    #     table,
    #     root_path=f"{S3_BUCKET}/curated/fact_yellow_tripdata/",
    #     filesystem=s3_fs,
    #     partition_cols=["year", "month"]
    

def build_dim_date(start_date, end_date):
    date_range = pd.date_range(start_date, end_date, freq='D')
    
    dim_date = pd.DataFrame({
        'date_id': range(1, len(date_range) + 1),  # Surrogate PK
        'full_date': date_range,
        'year': date_range.year,
        'quarter': date_range.quarter,
        'month': date_range.month,
        'month_name': date_range.strftime('%B'),
        'day': date_range.day,
        'day_name': date_range.day_name(),
        'day_of_week': date_range.dayofweek,  # 0=Mon, 6=Sun
        'weekend': date_range.dayofweek.isin([5, 6]).astype(int),
    })
    
    dim_date['date_id'] = dim_date['date_id'].astype('int32')
    int_cols = ['year', 'quarter', 'month', 'day', 'day_of_week', 'weekend']
    for col in int_cols:
        dim_date[col] = dim_date[col].astype('int8')
    

    write_table_parquet("curated/2024", "dim_date", dim_date)
    #tbc: write parquet file to s3


def build_dim_location():
    zone_lookup = pd.read_csv(f"s3://{S3_BUCKET}/reference/taxi_zone_lookup.csv")

    dim_location = zone_lookup["LocationID", "Borough", "Zone"].copy()
    dim_location.columns = ["location_key,", "borough", "zone"]

    dim_location['location_key'] = dim_location['location_key'].astype('int32')
    dim_location['borough'] = dim_location['borough'].astype('category')
    dim_location['zone_name'] = dim_location['zone_name'].astype('category')

    #tbc: write parquet file to s3

def build_dim_payment():
    
    dim_payment = pd.DataFrame([{
            "payment_key": i,
            "payment_type_id": k, 
            "payment_type": v, 
            "is_cash": 1 if v == "Cash" else 0
            } 
        for i, (k, v) in enumerate(PAYMENT_MAP.items())])

    dim_payment['payment_key'] = dim_payment['payment_key'].astype('int32')
    dim_payment['payment_type_id'] = dim_payment['payment_type_id'].astype('int8')
    dim_payment['payment_name'] = dim_payment['payment_name'].astype('category')

    #tbc: write parquet file to s3

def build_dim_time():
    hours_in_day = range(24)
    days_in_week = range(7)

    dim_time = pd.Dataframe({
        "time_key": [ h * 100 + d * 10 for h in hours_in_day for d in days_in_week],
        "hour": [h for h in hours_in_day for d in days_in_week],
        "day_of_week": [d for h in hours_in_day for d in days_in_week],
        "part_of_day": ["Morning" if h < 12 
                        else "Afternoon" if h < 17 
                        else "Evening" if h < 22
                        else "Night"
                        for h in hours_in_day for d in days_in_week]
    })

    dim_time['time_key'] = dim_time['time_key'].astype('int32')
    dim_time['hour'] = dim_time['hour'].astype('int8')
    dim_time['day_of_week'] = dim_time['day_of_week'].astype('int8')
    dim_time['part_of_day'] = dim_time['part_of_day'].astype('category')
    
    #tbc: write parquet file to s3



    






    
    




