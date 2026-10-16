select * from {{ ref('crypto_ohlcv_1m') }}
where low > high or open not between low and high or close not between low and high
