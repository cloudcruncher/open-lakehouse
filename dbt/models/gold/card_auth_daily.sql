select
    merchant || '|' || cast(cast(ts as date) as varchar) as day_key,
    merchant,
    cast(ts as date) as day,
    count(*) as auths,
    count(*) filter (where decision = 'approved') as approved,
    count(*) filter (where decision = 'declined') as declined,
    sum(amount_eur) as amount_eur,
    round(count(*) filter (where decision = 'approved') * 1.0 / count(*), 4) as approval_rate
from {{ ref('stg_card_auths') }}
group by 1, 2, 3
