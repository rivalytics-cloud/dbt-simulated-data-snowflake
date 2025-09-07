# 🧹 dbt Simulated Data Project — Snowflake  

[![dbt](https://img.shields.io/badge/dbt-analytics--engineering-orange)]()  
[![Snowflake](https://img.shields.io/badge/Snowflake-Data--Warehouse-blue)]()  

---

## 📌 Overview  
This project demonstrates how to take **messy, untrustworthy raw data** and transform it into a **reliable dataset for analytics** using dbt and Snowflake.  

It was built as a final analytics engineering project to showcase:  
-  Data profiling  
-  Cleaning & normalization (seed maps)  
-  Staging models  
-  Testing and documentation  

---

## 🚨 Business Problem  
Messy data isn’t just inconvenient — it’s costly.  
When raw data is inconsistent, duplicated, or stored in the wrong format, it:  

-  Breaks reports  
-  Creates conflicting metrics  
-  Destroys trust across the business  

---

## 🔎 Data Profiling Findings  

In this simulated crypto transactions dataset, I discovered:  

### Flags  
- `fraud`, `Fraudulent`, `FR`  

### Assets  
- `btc`, `BTC`, `Bitcoin`, `ETH`, `ETHEREUM`, `Eth`  

### Transaction Actions  
- `repay`, `repayment`, `loan_borrowed`, `STAKE`, `stake`, `fee`  

### Other Issues  
- All columns ingested as **VARCHAR(16777216)** → required casting  
- `USD_FX_RATE`: 2,500+ nulls  
- `ASSET_AMOUNT`: some invalid (NaN) values  
- Duplicate `TRANSACTION_ID`s (~50 duplicates)  

---

## 🛠️ dbt Solutions  

To solve these issues, I:  

- **Staged transactions** into a clean model (`stg_transactions`)  
- **Normalized categorical values** using **seed maps**:  
  - `asset_symbol_map`  
  - `risk_flag_map`  
  - `transaction_action_map`  
- **Cast proper data types** (e.g., timestamps, numeric fields)  
- **Filtered invalid rows** (e.g., negative asset amounts, zero FX rates)  
- **Deduplicated** on `TRANSACTION_ID` keeping the latest by timestamp  
- **Enforced quality with tests** (`unique`, `not_null`, `accepted_values`)  

---

## 📊 Data Lineage  

Here’s how raw data and seeds flow into the staging model:  

<img width="847" height="497" alt="image" src="https://github.com/user-attachments/assets/3b65be42-6978-4f88-8e51-42425d222032" />


---

## 🧪 dbt Tests Implemented  

- **Generic tests**  
  - `transaction_id`: `unique`, `not_null`  
  - `user_id`, `product_id`, `transaction_ts`: `not_null`  
  - `asset_symbol`: `accepted_values` (`BTC`, `ETH`)  
  - `risk_flag`: `accepted_values` (`fraud`)  

- **Custom checks**  
  - No NaN or negative asset amounts  
  - No zero/null FX rates  

---

## 💡 What I Learned  

- How to profile messy raw data and document issues  
- How to use **dbt seeds** to normalize categorical values  
- How to build a **staging layer** that enforces trust in downstream models  
- How to use **dbt tests + lineage** to create reliable pipelines  
- Why analytics engineering matters: messy data → broken insights  

---

## 🚀 Tech Stack  

- **dbt Cloud** — modeling, testing, documentation  
- **Snowflake** — data warehouse  
- **GitHub** — version control and project sharing  

---

## 📂 Project Structure  

models/

  staging/
  
    stg_transactions.sql
    
    staging.yml
    
seeds/

    asset_symbol_map.csv
  
    risk_flag_map.csv
  
    transaction_action_map.csv
  
analyses/

    tx_profiling.sql
  
logs/

    profile_log.md
  
profile.example.yml

sources.yml

---

✨ This project is just the start — staging transactions now lays the groundwork for building **dimensional models (dim_users, dim_products)** and fact tables for reliable analytics.  

--- 
<img width="732" height="108" alt="image" src="https://github.com/user-attachments/assets/88653994-4502-43d5-b6f7-950043252f95" />
