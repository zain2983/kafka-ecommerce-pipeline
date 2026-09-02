-- design.md section 18, revised: one row per 15-minute bucket of PURCHASE
-- activity, refreshed by a cron-scheduled `dbt run` every 15 minutes
-- (scripts/run_dbt.sh) instead of the old once-a-day granularity, so the
-- dashboard reflects the live event stream instead of a stale daily total.
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
    -- Floor event_timestamp to the start of its 15-minute bucket (900s).
    to_timestamp(floor(extract(epoch from event_timestamp) / 900) * 900) as interval_start,
    count(*) as orders,
    sum(quantity) as units_sold,
    sum(quantity * unit_price) as revenue
from purchases
group by 1
order by 1
