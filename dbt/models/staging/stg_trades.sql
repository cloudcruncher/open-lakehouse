select
    product,
    cast(ts as timestamp) as ts,
    cast(price as decimal(38, 8)) as price,
    cast(size as decimal(38, 8)) as size
from {{ ref('trades') }}
