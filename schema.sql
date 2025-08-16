-- SQLite schema for normalized positions
CREATE TABLE IF NOT EXISTS positions (
  broker TEXT,
  account_id TEXT,
  product_code TEXT,
  product_name TEXT,
  instrument_type TEXT, -- FUTURE or OPTION
  put_call TEXT,        -- PUT / CALL if option
  strike_price REAL,
  delivery_month TEXT,  -- YYYY-MM
  side TEXT,            -- BUY / SELL (if available)
  quantity REAL,
  trade_date TEXT,      -- ISO date
  trade_price REAL,
  market_price REAL,
  variation_margin REAL,
  currency TEXT,
  lot_size REAL,
  fx_spot_rate REAL
);
