# src/database.py
from typing import Iterable, List, Dict, Any
import pandas as pd
from sqlalchemy import create_engine, text, inspect

def get_engine(db_path: str):
    return create_engine(f"sqlite:///{db_path}", future=True)

def init_schema(engine):
    """Create tables if they don't exist."""
    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS tickers(
          id INTEGER PRIMARY KEY,
          symbol TEXT UNIQUE
        );
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS daily_metrics(
          symbol TEXT,
          date TEXT,
          close REAL,
          sma50 REAL,
          sma200 REAL,
          high_52w REAL,
          bvps REAL,
          pb REAL,
          ev REAL,
          pct_from_52w_high REAL,
          is_52w_high INTEGER,
          UNIQUE(symbol,date)
        );
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS signal_events(
          symbol TEXT,
          date TEXT,
          type TEXT,
          UNIQUE(symbol,date,type)
        );
        """))

def add_missing_columns(engine, table: str, columns: Dict[str, str]):
    """Automatically add missing columns to a table."""
    inspector = inspect(engine)
    existing = [col["name"] for col in inspector.get_columns(table)]
    with engine.begin() as conn:
        for col, col_type in columns.items():
            if col not in existing:
                conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}"))

def upsert_ticker(engine, symbol: str):
    with engine.begin() as conn:
        conn.execute(text("INSERT OR IGNORE INTO tickers(symbol) VALUES(:s)"), {"s": symbol})

def _to_scalar(v):
    if isinstance(v, (list, tuple)):
        return v[0] if v else None
    if hasattr(v, "item"):
        try:
            return v.item()
        except Exception:
            pass
    return None if pd.isna(v) else v

def upsert_daily(engine, symbol: str, df: pd.DataFrame):
    if df.empty:
        return

    # Ensure columns exist in DB
    add_missing_columns(engine, "daily_metrics", {
        "pct_from_52w_high": "REAL",
        "is_52w_high": "INTEGER"
    })

    rows = []
    for _, r in df.iterrows():
        rows.append({
            "symbol": symbol,
            "date": str(pd.to_datetime(r["date"]).date()),
            "close": _to_scalar(r.get("close")),
            "sma50": _to_scalar(r.get("sma50")),
            "sma200": _to_scalar(r.get("sma200")),
            "high_52w": _to_scalar(r.get("high_52w")),
            "bvps": _to_scalar(r.get("bvps")),
            "pb": _to_scalar(r.get("pb")),
            "ev": _to_scalar(r.get("ev")),
            "pct_from_52w_high": _to_scalar(r.get("pct_from_52w_high")),
            "is_52w_high": int(bool(_to_scalar(r.get("is_52w_high")))) if r.get("is_52w_high") is not None else None,
        })

    with engine.begin() as conn:
        for row in rows:
            conn.execute(text("""
INSERT INTO daily_metrics(symbol,date,close,sma50,sma200,high_52w,bvps,pb,ev,pct_from_52w_high,is_52w_high)
VALUES(:symbol,:date,:close,:sma50,:sma200,:high_52w,:bvps,:pb,:ev,:pct_from_52w_high,:is_52w_high)
ON CONFLICT(symbol,date) DO UPDATE SET
  close=excluded.close,
  sma50=excluded.sma50,
  sma200=excluded.sma200,
  high_52w=excluded.high_52w,
  bvps=excluded.bvps,
  pb=excluded.pb,
  ev=excluded.ev,
  pct_from_52w_high=excluded.pct_from_52w_high,
  is_52w_high=excluded.is_52w_high
"""), row)

def upsert_signals(engine, symbol: str, events: List[Dict[str, Any]]):
    if not events:
        return
    with engine.begin() as conn:
        for e in events:
            conn.execute(text("""
INSERT INTO signal_events(symbol,date,type)
VALUES(:symbol,:date,:type)
ON CONFLICT(symbol,date,type) DO NOTHING
"""), {"symbol": symbol, "date": str(e["date"]), "type": e["type"]})
