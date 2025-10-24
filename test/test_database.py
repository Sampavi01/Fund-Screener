# tests/test_database.py
import pandas as pd
from sqlalchemy import text

from src.database import get_engine, init_schema, upsert_daily, upsert_signals, upsert_ticker

def test_daily_upsert_idempotent(tmp_path):
    db_path = tmp_path / "test.db"
    engine = get_engine(str(db_path))
    init_schema(engine)
    upsert_ticker(engine, "TEST")

    df = pd.DataFrame([{
        "date": pd.Timestamp("2024-01-02"),
        "close": 100.0, "sma50": None, "sma200": None,
        "high_52w": None, "bvps": None, "pb": None, "ev": None,
        "pct_from_52w_high": None, "is_52w_high": None
    }])

    upsert_daily(engine, "TEST", df)
    # Update same date with new close to verify UPDATE path
    df.loc[0, "close"] = 101.0
    upsert_daily(engine, "TEST", df)

    with engine.begin() as conn:
        cnt = conn.execute(text("SELECT COUNT(*) FROM daily_metrics WHERE symbol='TEST'")).scalar_one()
        val = conn.execute(text("SELECT close FROM daily_metrics WHERE symbol='TEST' AND date='2024-01-02'")).scalar_one()
    assert cnt == 1
    assert abs(val - 101.0) < 1e-9

def test_signal_unique_constraint(tmp_path):
    db_path = tmp_path / "test2.db"
    engine = get_engine(str(db_path))
    init_schema(engine)

    events = [{"date": pd.Timestamp("2024-02-01").date(), "type": "golden_cross"}]
    upsert_signals(engine, "TEST", events)
    upsert_signals(engine, "TEST", events)  # insert same again

    with engine.begin() as conn:
        cnt = conn.execute(text("SELECT COUNT(*) FROM signal_events WHERE symbol='TEST'")).scalar_one()
    assert cnt == 1
