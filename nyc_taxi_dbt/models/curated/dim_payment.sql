-- model not required but kept for consistency with other dimensions.
select
    payment_key,
    payment_type,
    is_cash
from {{ ref('payment') }}