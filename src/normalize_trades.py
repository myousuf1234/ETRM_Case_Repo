import argparse
import os
import sqlite3
import pandas as pd
import re

# --- helpers ---------------------------------------------------------------

def normalize_delivery_month(s: pd.Series) -> pd.Series:
    """
    Return YYYY-MM for common inputs: YYMM, YYYYMM, YYYY-MM, MM-YYYY, etc.
    """
    s = s.astype(str).str.strip()
    s = s.replace({"": None, "nan": None, "NaT": None})
    s = s.str.replace(r"[\/\.\s]", "-", regex=True)

    def norm_one(x):
        if x is None or x == "":
            return None
        # YYYY-MM
        if re.fullmatch(r"\d{4}-\d{2}", x):
            y = int(x[:4]); m = int(x[5:7]);  return f"{y:04d}-{m:02d}"
        # YYYYMM
        if re.fullmatch(r"\d{6}", x):
            y = int(x[:4]); m = int(x[4:6]);  return f"{y:04d}-{m:02d}"
        # YYMM  (assume 20YY; change threshold if you expect 19YY)
        if re.fullmatch(r"\d{4}", x):
            yy = int(x[:2]); mm = int(x[2:4])
            yyyy = 2000 + yy if yy < 80 else 1900 + yy
            return f"{yyyy:04d}-{mm:02d}"
        # MM-YYYY
        m = re.fullmatch(r"(\d{2})-(\d{4})", x)
        if m:
            return f"{int(m.group(2)):04d}-{int(m.group(1)):02d}"
        # Fallback: try to parse
        try:
            dt = pd.to_datetime(x, errors="raise")
            return f"{dt.year:04d}-{dt.month:02d}"
        except Exception:
            return None

    return s.map(norm_one)

def normalize_trade_date(s: pd.Series) -> pd.Series:
    d = pd.to_datetime(s, errors="coerce")
    return d.dt.strftime("%Y-%m-%d")

def map_side(s: pd.Series) -> pd.Series:
    if s is None:
        return None
    s = s.astype(str).str.strip().str.upper()
    return s.replace({"B": "BUY", "BUY": "BUY", "BOUGHT": "BUY",
                      "S": "SELL", "SELL": "SELL", "SOLD": "SELL"})

def map_put_call(s: pd.Series) -> pd.Series:
    if s is None:
        return None
    s = s.astype(str).str.strip().str.upper()
    return s.replace({"P": "PUT", "PUT": "PUT", "C": "CALL", "CALL": "CALL"})

# --- normalizers -----------------------------------------------------------

def normalize_broker_a(df: pd.DataFrame) -> pd.DataFrame:
    # Safely get columns with defaults
    def col(name, default=None):
        return df[name] if name in df.columns else default

    # Build output
    out = pd.DataFrame(index=df.index)
    out["broker"] = "Broker A"

    # Account with prefix
    acct = col("Client Code")
    out["account_id"] = ("A:" + acct.astype(str).str.strip()) if acct is not None else "A:UNKNOWN"

    out["product_code"] = col("Exchange Instrument Code")

    # Align long name: use Commodity Name (fallbacks only if needed)
    out["product_name"] = (
        col("Commodity Name")
        if "Commodity Name" in df.columns else
        col("Instrument Long Name") if "Instrument Long Name" in df.columns else
        col("Commodity Code")
    )

    out["instrument_type"] = col("Future/Option")
    out["put_call"] = map_put_call(col("Put/Call"))
    out["strike_price"] = pd.to_numeric(col("Strike") if "Strike" in df.columns else col("Exercise Price"), errors="coerce")

    # Uniform delivery month
    out["delivery_month"] = normalize_delivery_month(col("Delivery Month/Year"))

    # Side + quantity
    out["side"] = map_side(col("Bought or Sold"))
    out["quantity"] = pd.to_numeric(col("Quantity"), errors="coerce")

    # Dates + prices + VM + currency
    out["trade_date"] = normalize_trade_date(col("Trade Date"))
    out["trade_price"] = pd.to_numeric(col("Price") if "Price" in df.columns else col("Trade Price"), errors="coerce")
    out["market_price"] = pd.to_numeric(col("Market Rate") if "Market Rate" in df.columns else col("Current Price"), errors="coerce")
    out["variation_margin"] = pd.to_numeric(col("Variation Margin") if "Variation Margin" in df.columns else col("Variation Margin Amount"), errors="coerce")
    out["currency"] = col("Transaction Currency")
    out["lot_size"] = pd.to_numeric(col("Lot Size") if "Lot Size" in df.columns else col("Contract Size"), errors="coerce")
    out["fx_spot_rate"] = pd.to_numeric(col("FX Spot Rate"), errors="coerce") if "FX Spot Rate" in df.columns else None

    return out

def normalize_broker_b(df: pd.DataFrame) -> pd.DataFrame:
    def col(name, default=None):
        return df[name] if name in df.columns else default

    out = pd.DataFrame(index=df.index)
    out["broker"] = "Broker B"

    # Account with prefix + fallback to Ledger Code
    if "Client Account" in df.columns and df["Client Account"].notna().any():
        out["account_id"] = "B:" + df["Client Account"].astype(str).str.strip()
    elif "Ledger Code" in df.columns:
        out["account_id"] = "B:" + df["Ledger Code"].astype(str).str.strip()
    else:
        out["account_id"] = "B:UNKNOWN"

    out["product_code"] = col("Instrument Code")
    out["product_name"] = col("Instrument Long Name") if "Instrument Long Name" in df.columns else col("Commodity Name")

    # Instrument type per row: OPTION if Strike or Option Type present
    is_option = pd.Series([False]*len(df))
    if "Strike" in df.columns:      is_option |= df["Strike"].notna()
    if "Option Type" in df.columns:  is_option |= df["Option Type"].notna()
    out["instrument_type"] = is_option.map(lambda x: "OPTION" if x else "FUTURE")

    out["put_call"] = map_put_call(col("Option Type"))
    out["strike_price"] = pd.to_numeric(col("Strike"), errors="coerce")

    # Uniform delivery month
    out["delivery_month"] = normalize_delivery_month(col("Delivery/Prompt date"))

    # Side (if present) + quantity
    out["side"] = map_side(col("Side"))
    out["quantity"] = pd.to_numeric(col("Volume"), errors="coerce")

    # Dates + prices + VM + currency
    trade_date_col = col("Trade Date") if "Trade Date" in df.columns else col("Last Traded date")
    out["trade_date"] = normalize_trade_date(trade_date_col)
    out["trade_price"] = pd.to_numeric(col("Price"), errors="coerce")
    out["market_price"] = pd.to_numeric(col("Market Rate"), errors="coerce")
    out["variation_margin"] = pd.to_numeric(col("Variation Margin"), errors="coerce")
    out["currency"] = col("Currency Code") if "Currency Code" in df.columns else col("Transaction Currency")
    out["lot_size"] = pd.to_numeric(col("Lot Size"), errors="coerce")
    out["fx_spot_rate"] = pd.to_numeric(col("FX Spot Rate"), errors="coerce")

    return out

# --- main ------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Normalize broker positions and load to SQLite + CSV")
    ap.add_argument("--broker-a", default="data/Broker A Open Positions.csv")
    ap.add_argument("--broker-b", default="data/Broker B Open Positions.csv")
    ap.add_argument("--sqlite",   default="output/trades_normalized.db")
    ap.add_argument("--csv",      default="output/positions_normalized_export.csv")
    args = ap.parse_args()

    # Read inputs
    df_a = pd.read_csv(args.broker_a)
    df_b = pd.read_csv(args.broker_b)

    # Normalize
    norm_a = normalize_broker_a(df_a)
    norm_b = normalize_broker_b(df_b)

    # Combine
    normalized = pd.concat([norm_a, norm_b], ignore_index=True)

    # Save CSV
    os.makedirs(os.path.dirname(args.csv), exist_ok=True)
    normalized.to_csv(args.csv, index=False)

    # Save SQLite
    os.makedirs(os.path.dirname(args.sqlite), exist_ok=True)
    con = sqlite3.connect(args.sqlite)
    normalized.to_sql("positions", con, if_exists="replace", index=False)
    con.close()

    print(f"Wrote {len(normalized)} rows to:")
    print(f"  CSV   → {args.csv}")
    print(f"  SQLite→ {args.sqlite} (table: positions)")

if __name__ == "__main__":
    main()
