-- One row per product and minute. VWAP is clamped into [low, high]: a single-price minute can
-- otherwise land a rounding step outside the range (the bug markets-data 0.10.0 fixed).
with bars as (
    select
        product,
        date_trunc('minute', ts) as minute,
        {{ first_by('price', 'ts') }} as open,
        max(price) as high,
        min(price) as low,
        {{ last_by('price', 'ts') }} as close,
        sum(size) as volume,
        sum(price * size) / nullif(sum(size), 0) as raw_vwap,
        count(*) as trades
    from {{ ref('stg_trades') }}
    group by 1, 2
)

select
    product || '|' || cast(minute as varchar) as bar_key,
    product,
    minute,
    open,
    high,
    low,
    close,
    volume,
    least(high, greatest(low, raw_vwap)) as vwap,
    trades
from bars
