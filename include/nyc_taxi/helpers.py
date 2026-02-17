from include.nyc_taxi.config import s3_fs, default_conn
from include.nyc_taxi.constants import S3_BUCKET
import pyarrow as pa
import pandas as pd

def write_table_parquet(bucket_dir, filename, df):
    file_path = f"s3://{S3_BUCKET}/{bucket_dir}/{filename}.parquet"

    if s3_fs.exists(file_path):
        print(f"File already present at {file_path}")
    else:
        table = pa.Table.from_pandas(df)
        with s3_fs.open(file_path, "wb") as f:
            pa.parquet.write_table(table, f)
    
    return file_path

def write_dataset_parquet(bucket_dir, df, partition_cols):
    path = f"s3://{S3_BUCKET}/{bucket_dir}"

    s3_fs.invalidate_cache()  # refresh stored s3 listing cache
    if not s3_fs.glob(f"{path}/*.parquet"):
        table = pa.Table.from_pandas(df)
        pa.parquet.write_to_dataset(
            table,
            root_path=f"{path}/",
            filesystem=s3_fs,
            partition_sols=partition_cols
        )
    
    return path
    
