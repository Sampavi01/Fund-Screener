# src/processor.py
from __future__ import annotations
import numpy as np
import pandas as pd
from typing import List, Tuple
from src.models import RawBundle, ProcessedRow, FundamentalsQuarter
def _funds_to_df(fqs: List[FundamentalsQuarter]) -> pd.DataFrame:
    if not fqs:
        return pd.DataFrame(columns=["date","book_value","shares_out","total_debt","cash"])
    rows = []
    for q in fqs:
        rows.append({
            "date": pd.to_datetime(q.period_end),
            "book_value": float(q.book_value) if q.book_value is not None else np.nan,
            "shares_out": float(q.shares_out) if q.shares_out is not None else np.nan,
            "total_debt": float(q.total_debt) if q.total_debt is not None else np.nan,
            "cash": float(q.cash) if q.cash is not None else np.nan,
        })
    df = pd.DataFrame(rows).sort_values("date")
    return df

def process_bundle(raw: RawBundle, min_sma_days: int = 200) -> Tuple[pd.DataFrame, List[ProcessedRow]]:
    p = pd.DataFrame([{"date": r.date, "close": r.close} for r in raw.prices])
    if p.empty:
        return p, []
    p["date"] = pd.to_datetime(p["date"], errors="coerce")
    p = p.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

    # Strict windows to avoid early signals
    p["sma50"] = p["close"].rolling(50, min_periods=50).mean()
    p["sma200"] = p["close"].rolling(min_sma_days, min_periods=min_sma_days).mean()
    p["high_52w"] = p["close"].rolling(252, min_periods=252).max()

    # Fundamentals merge (quarterly -> daily ffill)
    fdf = _funds_to_df(raw.fundamentals_q)
    if not fdf.empty:
        # Reindex to daily over price range for ffill
        rng = pd.date_range(start=p["date"].min(), end=p["date"].max(), freq="D")
        fdf = fdf.set_index("date").reindex(rng).ffill().rename_axis("date").reset_index()
        p = p.merge(fdf, on="date", how="left")
    else:
        for col in ["book_value","shares_out","total_debt","cash"]:
            p[col] = np.nan

    # Ratios
    with np.errstate(invalid="ignore", divide="ignore"):
        bvps = np.where((p["book_value"] > 0) & (p["shares_out"] > 0),
                        p["book_value"] / p["shares_out"], np.nan)
        p["bvps"] = bvps
        p["pb"] = np.where(bvps > 0, p["close"] / bvps, np.nan)
        market_cap = np.where(p["shares_out"].notna(), p["close"] * p["shares_out"], np.nan)
        p["ev"] = market_cap + p["total_debt"].fillna(0.0) - p["cash"].fillna(0.0)
        p["pct_from_52w_high"] = np.where(p["high_52w"].notna(),
                                          (p["close"] - p["high_52w"]) / p["high_52w"], np.nan)
        p["is_52w_high"] = np.where(p["high_52w"].notna(),
                                    (p["close"] - p["high_52w"]).abs() <= 1e-8, False)

    # Validate rows
    validated: List[ProcessedRow] = []
    for _, r in p.iterrows():
        validated.append(
            ProcessedRow(
                date=pd.to_datetime(r["date"]).date(),
                close=float(r["close"]),
                sma50=None if pd.isna(r["sma50"]) else float(r["sma50"]),
                sma200=None if pd.isna(r["sma200"]) else float(r["sma200"]),
                high_52w=None if pd.isna(r["high_52w"]) else float(r["high_52w"]),
                pct_from_52w_high=None if pd.isna(r["pct_from_52w_high"]) else float(r["pct_from_52w_high"]),
                is_52w_high=bool(r["is_52w_high"]) if not pd.isna(r["is_52w_high"]) else None,
                bvps=None if pd.isna(r["bvps"]) else float(r["bvps"]),
                pb=None if pd.isna(r["pb"]) else float(r["pb"]),
                ev=None if pd.isna(r["ev"]) else float(r["ev"]),
            )
        )
    # Keep only columns needed for DB upsert
    cols = ["date","close","sma50","sma200","high_52w","bvps","pb","ev","pct_from_52w_high","is_52w_high"]
    p = p[cols].copy()
    return p, validated
