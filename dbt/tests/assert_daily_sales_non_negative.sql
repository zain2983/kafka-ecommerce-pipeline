-- Singular test (design.md section 26 - "Data quality tests"): a dbt
-- test passes when the query returns ZERO rows. orders/units_sold
-- being negative, or revenue being negative, would mean either bad
-- source data slipped past validator.py or a bug in daily_sales.sql's
-- aggregation - either way, something worth failing loudly on rather
-- than a generic not_null test would catch.

select *
from {{ ref('daily_sales') }}
where orders < 0
   or units_sold < 0
   or revenue < 0
