from include.nyc_taxi.constants import PAYMENT_MAP, RATECODE_MAP
from include.nyc_taxi.utils import get_storage_options
from include.nyc_taxi.helpers import write_table_parquet
import include.nyc_taxi.errors as errors
import pandas as pd

def staging_transform(year_month, s3_key):
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
    
    path = write_table_parquet(staging_path_dir, filename, df_staging)

    return path

    # if not s3_fs.glob(f"{staging_path_prefix}/*.parquet"):
    #     table = pa.Table.from_pandas(df_staging)
    #     with s3_fs.open(staging_path, "wb") as f:
    #         pa.parquet.write_table(table, f)



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

