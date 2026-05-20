select
    cast(LocationID as integer) as location_key,
    Borough as borough,
    Zone as zone
from {{ ref('taxi_zone_lookup') }}