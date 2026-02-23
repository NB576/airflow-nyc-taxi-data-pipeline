from include.nyc_taxi.constants import S3_BUCKET, PAYMENT_MAP, RATECODE_MAP
from include.nyc_taxi.config import s3_fs, default_conn
from include.nyc_taxi.helpers import write_table_parquet, write_dataset_parquet
import pyarrow.dataset as ds
import pandas as pd

def curated_transform(monthly_dates, file_paths):

    dataset = ds.dataset(file_paths, format="parquet", filesystem=s3_fs)
    
    table = dataset.to_table()

    # print(df_staging.head())

    # build_facts_table(df_staging)
    # build_dim_date(start_date, end_date)
    # build_dim_location()
    # build_dim_time()
    # build_dim_payment()

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