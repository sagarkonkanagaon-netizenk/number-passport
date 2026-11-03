{{ config(materialized='table', schema='gold') }}

-- Risk-weighted assets by Basel asset class (standardised approach, simplified).
with net as (
    select
        e.loan_id,
        e.asset_class,
        e.ead_usd,
        greatest(e.ead_usd - coalesce(c.collateral_usd, 0), 0) as net_ead_usd
    from {{ source('silver', 'exposures') }} as e
    left join {{ source('silver', 'collateral_adj') }} as c
        on c.loan_id = e.loan_id
)

select
    n.asset_class,
    sum(n.net_ead_usd * rw.risk_weight) as rwa_usd,
    sum(n.ead_usd) as ead_usd,
    count(*) as exposure_count
from net as n
join {{ source('ref', 'risk_weights') }} as rw
    on rw.asset_class = n.asset_class
group by n.asset_class
