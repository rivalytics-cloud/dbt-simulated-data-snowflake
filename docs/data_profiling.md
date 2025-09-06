Data Profiling Log — RAW_TRANSACTIONS

Location: FINANCING_PRODUCTS.PUBLIC.RAW_TRANSACTIONS

1) Schema snapshot (from DESCRIBE)

All columns landed as VARCHAR(16777216) in Snowflake.

No PK/FK constraints defined at the warehouse level; all columns nullable.

Key business fields in raw:

IDs: TRANSACTION_ID (candidate key, duplicates exist), USER_ID, PRODUCT_ID

Categorical: TRANSACTION_ACTION, ASSET_SYMBOL, RISK_FLAG

Measures: ASSET_AMOUNT, USD_FX_RATE

Timestamps: EVENT_TIME, INGESTION_TS (stored as text)

2) Findings from profiling

Duplicates: ~50 duplicate TRANSACTION_IDs (case drift e.g., t0001118 vs T0001118).

Nulls:

USER_ID: 0

PRODUCT_ID: 0

EVENT_TIME: 0 (but needs casting)

USD_FX_RATE: 2,541 nulls

TRANSACTION_ACTION: 186 blanks/nulls found during testing

Domains / drift:

ASSET_SYMBOL: Bitcoin, BTC, btc, ETH, ETHEREUM, Eth → should collapse to BTC/ETH

TRANSACTION_ACTION: repay, repayment, borrow, loan_borrowed, stake, unstake, margin_open, margin_close, fee (casing / synonym drift)

RISK_FLAG: fraud, Fraudulent, FR, NULL

Ranges / types:

ASSET_AMOUNT: string; potential non-numeric (“NaN”) and negatives

USD_FX_RATE: string; observed ≈ 0.8001–1.4998, but many nulls

Timestamps stored as strings → must cast before any min/max checks

3) Staging decisions implemented (stg_transactions)

Typing / normalization

try_to_number(ASSET_AMOUNT) → asset_amount

try_to_number(USD_FX_RATE) → usd_fx_rate

try_to_timestamp_ntz(EVENT_TIME) → transaction_ts

try_to_timestamp_ntz(INGESTION_TS) → ingestion_ts

lower(trim(TRANSACTION_ACTION)) retained as raw for mapping fallback

upper(ASSET_SYMBOL) helper + seed mapping

Seed-driven cleanup

asset_symbol_map.csv → normalize to BTC/ETH

transaction_action_map.csv → canonicalize verbs (e.g., repayment → repay)

risk_flag_map.csv → map fraud, Fraudulent, FR → fraud (TEXT label)

Business rules / filters

Drop rows where any of transaction_id, user_id, product_id, transaction_ts is NULL

Ensure transaction_action is not null via coalesce(mapped, nullif(raw,''))

Keep rows with asset_amount IS NULL OR asset_amount > 0

Keep rows with usd_fx_rate IS NULL OR usd_fx_rate > 0

Deduplication

Keep latest per transaction_id using row_number() ordered by transaction_ts DESC, then ingestion_ts DESC

4) dbt tests in place (models/staging/staging.yml)

Keys:

transaction_id → unique, not_null

Required fields:

user_id, product_id, transaction_ts → not_null

transaction_action → not_null

Controlled categories:

asset_symbol → accepted_values: ['BTC','ETH'] (where not null)

risk_flag (TEXT label) → accepted_values: ['fraud'] (where not null)

Note: No singular tests currently (you removed the tests/ folder). If needed later, add checks like no_negative_asset_amounts.sql or no_future_transaction_ts.sql.

5) Open items / next steps

Stage users and products with similar profiling & mapping (case, nulls, IDs)

Add relationship tests once dims exist:

stg_transactions.user_id → dim_users.user_id

stg_transactions.product_id → dim_products.product_id

Optional singular tests (future):

no_negative_asset_amounts

no_null_or_zero_fx_rate

no_future_transaction_ts