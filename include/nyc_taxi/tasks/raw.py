from pendulum import datetime
from include.nyc_taxi.constants import S3_BUCKET, PAYMENT_MAP, RATECODE_MAP
from include.nyc_taxi.utils import get_storage_options
from include.nyc_taxi.config import s3_fs, default_conn
from include.nyc_taxi.helpers import write_table_parquet, write_dataset_parquet
from airflow.models import Connection
import include.nyc_taxi.errors as errors
import pyarrow.dataset as ds
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

def run_data_quality_checks(year_month: str, 
                            min_rows: int = 10000, 
                            max_col_null_pct: float = 0.05, 
                            max_total_null_pct: float = 0.05,
                            max_neg_duration_pct: float =0.01):
    """
    Simple quality checks on raw Parquet files in S3:
    - File exists in S3
    - Required columns are present
    - Column dtypes match expected schema
    - Row count exceeds minimum threshold
    - No null pickup/dropoff timestamps
    - Negative trip duration percentage within threshold
    - Per-column and total null percentages within threshold
    """
    
    # check each available month has a file present in s3
    split = year_month.split("-")
    year = split[0]
    month = split[1]
    raw_file_path = f"s3://{S3_BUCKET}/raw/{year}/{month}/yellow_tripdata_{year_month}.parquet"
    s3_key = f"{S3_BUCKET}/raw/{year}/{month}/"

    # check file existence
    if not s3_fs.exists(raw_file_path):
        raise errors.DataSourceMissingError(f"No Parquet files found under s3://{s3_key}")

    default_conn = Connection.get_connection_from_secrets("aws_default")
    storage_options = {
        "key": default_conn.login,
        "secret": default_conn.password,
        "token": default_conn.extra_dejson.get("session_token")
        }     
    
    df = pd.read_parquet(path=raw_file_path,
                        storage_options=storage_options)

    # required columns check
    required_columns = {
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime", 
        "trip_distance",
        "fare_amount", 
        "total_amount",
        "PULocationID", 
        "DOLocationID"
    }

    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise errors.ColumnNotFoundError(col=missing_columns, source=raw_file_path)
    
    # data type check on required columns
    expected_dtypes = {
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
    for col, expected_dtype in expected_dtypes.items():
        if col in df.columns:
            actual_dtype = str(df[col].dtype)
            if not pd.api.types.is_dtype_equal(df[col].dtype, expected_dtype):
                dtype_errors.append(f"Column '{col}': expected '{expected_dtype}', got '{actual_dtype}'")
    if dtype_errors:     
        msg = '\n'.join(dtype_errors)
        raise errors.SchemaValidationError(msg)
    
    # minimum rows per file check
    file_row_count = len(df)
    if file_row_count < min_rows:
        raise errors.MiniumumRowsError(file_row_count, min_rows)

    # datetime columns null count check
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

    # negative trip duration check
    df["trip_duration_minutes"] = (df[dropoff_col] - df[pickup_col]).dt.total_seconds() / 60
    neg_duration_count = len(df[df["trip_duration_seconds"] < 0])   
    neg_duration_pct = neg_duration_count / file_row_count if file_row_count else 0

    # raise error if neg duration threshold exceeded
    if neg_duration_pct > max_neg_duration_pct:
        raise errors.NegativeDurationThresholdError(neg_duration_pct, max_neg_duration_pct)
    
    # check each column's null percentage is below specified limit
    # check total null percentage is below specified limit
    req_cols_total_null_count = 0
    null_error_cols = []
    for col in required_columns:
        null_count = df[col].isna().sum() 
        null_pct = null_count / file_row_count if file_row_count > 0 else 0
        req_cols_total_null_count += null_count
        
        if null_pct > max_col_null_pct:
            null_error_cols.append(col)
    
    if null_error_cols:
        raise errors.NullThresholdError(cols=null_error_cols, threshold_pct=max_col_null_pct)
    
    if file_row_count > 0:
        total_null_pct = req_cols_total_null_count / (file_row_count * len(required_columns))
        if total_null_pct > max_total_null_pct:
            raise errors.TotalNullsThresholdError(total_null_threshold_pct=max_total_null_pct, total_null_pct=total_null_pct)

    return s3_key


    






    
    




