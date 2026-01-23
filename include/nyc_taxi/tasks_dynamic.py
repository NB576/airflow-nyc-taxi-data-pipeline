from pendulum import datetime
from include.nyc_taxi.constants import S3_BUCKET, BROWSER_HEADERS
from include.nyc_taxi.utils import get_storage_options
from include.nyc_taxi.config import s3_fs, default_conn
import include.nyc_taxi.errors as errors
import pyarrow.parquet as pq
import pandas as pd

from airflow.models import Connection
from s3fs import S3FileSystem

def generate_monthly_dates_2024():
    start_date = datetime(2024, 7, 1)
    months = []
    current = start_date
    while current <= datetime(2024, 12, 1):
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

def run_transform_to_staging(s3_key):

    df = pd.read_parquet(path=f"s3://{s3_key}",
                            storage_options=get_storage_options()
                            )
    
    df["pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df['dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    df["trip_duration_date_seconds"] = (df["dropoff_datetime"] - df["pickup_datetime"]).dt.total_seconds()

    # filter anomaly rows (based on business rules)
    df = df[(df["trip_duration_seconds"] > 30) & 
            (df["trip_duration_seconds"] < 24*3600) &
            
            (df["passenger_counts"].between(1,6))
            ]
    
    #add partitioning columns
    df["year"] = df['pickup_datetime'].dt.year
    df["month"] = df['pickup_datetime'].dt.month
    df["day"] = df['pickup_datetime'].dt.day

    # staging_path = f"curated/year={year}/month={month:02d}/"
    # table = pq.Table.from_pandas(df)
    # pq.write_to_dataset(
    #     table,
    #     root_path=f"{S3_BUCKET}/{staging_path}",
    #     filesystem=s3_fs,
    #     partition_cols=["year", "month", "day"]
    #                     )





            







    
    




