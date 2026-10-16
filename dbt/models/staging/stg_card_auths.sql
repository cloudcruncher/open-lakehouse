select
    merchant,
    cast(ts as timestamp) as ts,
    cast(amount_eur as decimal(18, 2)) as amount_eur,
    decision
from {{ ref('card_auths') }}
