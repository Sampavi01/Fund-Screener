
import pandas as pd
import numpy as np

from src.models import RawBundle, PriceRow, FundamentalsQuarter
from src.processor import process_bundle

def _bundle_from_prices(prices):
    rows = [PriceRow(date=pd.to_datetime(d).date(), close=float(c)) for d, c in prices]
    return RawBundle(ticker="TEST", prices=rows, fundamentals_q=[])

def test_sma_windows_and_52w_metrics():
    # Build 260 business days with linearly increasing close
    dates = pd.bdate_range("2024-01-01", periods=260)
    closes = np.arange(100.0, 360.0)  # strictly rising
    bun = _bundle_from_prices(list(zip(dates, closes)))
    df, rows = process_bundle(bun, min_sma_days=200)

    # Columns exist
    for col in ["date","close","sma50","sma200","high_52w","pct_from_52w_high","is_52w_high"]:
        assert col in df.columns

    # SMA50 should be NaN before 50th observation (strict window)
    assert df["sma50"].iloc[48] != df["sma50"].iloc[48]  # NaN
    assert not np.isnan(df["sma50"].iloc[49])

    # SMA200 should be NaN before 200th observation
    assert df["sma200"].iloc[198] != df["sma200"].iloc[198]  # NaN
    assert not np.isnan(df["sma200"].iloc[199])

    # 52-week high (252) is NaN until 252 bars
    assert df["high_52w"].iloc[250] != df["high_52w"].iloc[250]  # NaN
    assert not np.isnan(df["high_52w"].iloc[251])

    # On a strictly rising series, last row is new 52w high
    assert bool(df["is_52w_high"].iloc[-1]) is True
    assert df["pct_from_52w_high"].iloc[-1] == 0.0
