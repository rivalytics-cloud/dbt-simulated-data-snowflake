-- Columns and types
desc table FINANCING_PRODUCTS.PUBLIC.raw_transactions;

-- Distinct Assets
select distinct asset_symbol from raw_transactions;

-- Basic counts & distinctness
select
  count(*)                                    as total_rows,
  count(distinct transaction_id)                      as distinct_txn_id,
  sum(case when transaction_id is null then 1 else 0 end) as null_txn_id
from FINANCING_PRODUCTS.PUBLIC.raw_transactions;

-- Duplicates
select transaction_id, count(*) as cnt
from FINANCING_PRODUCTS.PUBLIC.raw_transactions
group by 1
having count(*) > 1
order by cnt desc;

-- Nulls
select
  sum(case when user_id is null then 1 else 0 end) as null_user_id,
  sum(case when product_id is null then 1 else 0 end) as null_product_id,
  sum(case when event_time is null then 1 else 0 end) as null_event_time,
  sum(case when asset_amount is null then 1 else 0 end) as null_asset_amount,
  sum(case when usd_fx_rate is null then 1 else 0 end) as null_fx_rate
from FINANCING_PRODUCTS.PUBLIC.raw_transactions;

-- Value ranges
select
  min(asset_amount) as min_asset_amount,
  max(asset_amount) as max_asset_amount,
  min(usd_fx_rate) as min_fx_rate,
  max(usd_fx_rate) as max_fx_rate
from FINANCING_PRODUCTS.PUBLIC.raw_transactions;

-- Domain checks
select distinct transaction_action 
from FINANCING_PRODUCTS.PUBLIC.raw_transactions;

-- Timestamp
select
  min(event_time) as min_event_time,
  max(event_time) as max_event_time,
  sum(case when event_time > current_timestamp() then 1 else 0 end) as future_rows
from FINANCING_PRODUCTS.PUBLIC.raw_transactions;

