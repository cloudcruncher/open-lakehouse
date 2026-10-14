-- Returns offending rows; the test passes when none come back.
select * from {{ ref('crypto_ohlcv_1m') }} where vwap < low or vwap > high
