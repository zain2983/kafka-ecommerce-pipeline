-- design.md section 18: one row per calendar day of PURCHASE activity.
--
-- Each PURCHASE event already represents one line-item order (it
-- carries its own quantity/unit_price - see the example event in
-- design.md section 5), so "orders" here is a count of PURCHASE events,
-- not a count of distinct users or checkout sessions.

with purchases as (

    select *
    from {{ ref('stg_events') }}
    where event_type = 'PURCHASE'

)

select
    event_timestamp::date as sale_date,
    count(*) as orders,
    sum(quantity) as units_sold,
    sum(quantity * unit_price) as revenue
from purchases
group by 1
order by 1
