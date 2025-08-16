# ETRM Developer Case Study — Phase 1

This repository contains a prototype system to **ingest**, **normalize**, and **store** open positions from two brokers, plus a **Power BI** dashboard setup.

## What’s included
- `data/` — sample broker files (`Broker A Open Positions.csv`, `Broker B Open Positions.csv`)
- `src/normalize_trades.py` — ETL script to normalize and persist data
- `schema.sql` — SQLite schema for the `positions` table
- `output/` — generated artifacts after running the ETL
  - `trades_normalized.db` (SQLite) — table: `positions`
  - `positions_normalized_export.csv` — Power BI–ready CSV

## How to run (local)
1. **Python env**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Run ETL**
   ```bash
   python src/normalize_trades.py \
     --broker-a "data/Broker A Open Positions.csv" \
     --broker-b "data/Broker B Open Positions.csv" \
     --sqlite output/trades_normalized.db \
     --csv output/positions_normalized_export.csv
   ```

3. **Artifacts produced**
   - SQLite DB at `output/trades_normalized.db` (table: `positions`)
   - CSV at `output/positions_normalized_export.csv`
   - PBIX Dashboard at `etrm_case_pbix_file.pbix`



## Database schema
See `schema.sql`. The ETL writes *exactly* these columns.
- `broker, account_id, product_code, product_name, instrument_type, put_call, strike_price, delivery_month, side, quantity, trade_date, trade_price, market_price, variation_margin, currency, lot_size, fx_spot_rate`.

---

