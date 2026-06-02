{{ config(
    partitioned_by=['year', 'month']
) }}

select
    -- surrogate key
    to_hex(md5(to_utf8(concat_ws('||',
        coalesce(cast(VendorID as varchar), 'NULL'),
        coalesce(cast(tpep_pickup_datetime as varchar), 'NULL'),
        coalesce(cast(tpep_dropoff_datetime as varchar), 'NULL'),
        coalesce(cast(PULocationID as varchar), 'NULL'),
        coalesce(cast(DOLocationID as varchar), 'NULL'),
        coalesce(cast(RatecodeID as varchar), 'NULL'),
        coalesce(cast(payment_type as varchar), 'NULL')
    )))) as trip_id,

    -- foreign keys
    day_of_year(tpep_pickup_datetime) as pickup_date_id,
    (hour(tpep_pickup_datetime) * 100
        + day_of_week(tpep_pickup_datetime) * 10) as pickup_time_id,
    cast(PULocationID as integer) as pickup_location_id,
    cast(DOLocationID as integer) as dropoff_location_id,
    cast(payment_type as integer) as payment_type_id,
    cast(VendorID as integer) as vendor_id,

    -- measures
    passenger_count,
    trip_distance,
    RatecodeID,
    revenue,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    total_amount,
    tip_rate,
    fare_per_mile,
    fare_per_minute,
    trip_duration_minutes,
    avg_speed_mph,
    year,
    month

from {{ source('silver', 'stg_yellow_tripdata') }}
where tpep_pickup_datetime >= cast('{{ var("start_date") }}' as timestamp)
  and tpep_pickup_datetime <  cast('{{ var("end_date") }}' as timestamp)