with src as (
  select * from {{ source('raw', 'transactions') }}
),

typed as (
  select
    trim(TRANSACTION_ID)                     as transaction_id_raw,
    trim(USER_ID)                            as user_id_raw,
    trim(PRODUCT_ID)                         as product_id_raw,
    trim(ASSET_SYMBOL)                       as asset_symbol_raw,
    lower(trim(TRANSACTION_ACTION))          as transaction_action_raw,  
    try_to_number(trim(ASSET_AMOUNT))        as asset_amount_raw,
    try_to_number(trim(USD_FX_RATE))         as usd_fx_rate_raw,
    try_to_timestamp_ntz(trim(EVENT_TIME))   as event_time_raw,
    try_to_timestamp_ntz(trim(INGESTION_TS)) as ingestion_ts_raw,
    trim(RISK_FLAG)                          as risk_flag_raw
  from src
),

normalized as (
  select
    transaction_id_raw               as transaction_id,
    user_id_raw                      as user_id,
    product_id_raw                   as product_id,
    asset_symbol_raw,
    upper(asset_symbol_raw)          as asset_symbol_norm,
    asset_amount_raw                 as asset_amount,
    usd_fx_rate_raw                  as usd_fx_rate,
    event_time_raw                   as transaction_ts,
    ingestion_ts_raw                 as ingestion_ts,  
    risk_flag_raw,
    case
      when upper(risk_flag_raw) in ('Y','TRUE','1') then true
      when upper(risk_flag_raw) in ('N','FALSE','0') then false
      else null
    end                              as risk_flag_bool,
    transaction_action_raw
  from typed
),


mapped as (
  select
    n.*,
    a.clean_value as asset_symbol_mapped,                        
    r.clean_value as risk_flag_mapped,                            
    m.clean_value as transaction_action_mapped,                   
    coalesce(m.clean_value, nullif(n.transaction_action_raw,''))  as transaction_action_clean
  from normalized n
  left join {{ ref('asset_symbol_map') }}       a
    on lower(trim(n.asset_symbol_raw))     = lower(trim(a.raw_value))
  left join {{ ref('risk_flag_map') }}          r
    on lower(trim(n.risk_flag_raw))        = lower(trim(r.raw_value))
  left join {{ ref('transaction_action_map') }} m
    on lower(trim(n.transaction_action_raw)) = lower(trim(m.raw_value))
),

filtered as (
  select *
  from mapped
  where
    transaction_id   is not null
    and user_id      is not null
    and product_id   is not null
    and transaction_ts is not null
    and transaction_action_clean is not null  
    and (asset_amount is null or asset_amount > 0)
    and (usd_fx_rate  is null or usd_fx_rate  > 0)
),

deduped as (
  select *
  from (
    select
      *,
      row_number() over (
        partition by transaction_id
        order by transaction_ts desc nulls last, ingestion_ts desc nulls last
      ) as rn
    from filtered
  )
  where rn = 1
)

select
  transaction_id,
  user_id,
  product_id,
  transaction_action_clean as transaction_action,
  coalesce(asset_symbol_mapped, asset_symbol_norm)      as asset_symbol,
  asset_amount,
  usd_fx_rate,
  transaction_ts,
  ingestion_ts,
  risk_flag_bool,


  coalesce(risk_flag_mapped, case when risk_flag_bool then 'fraud' end) as risk_flag
from deduped
