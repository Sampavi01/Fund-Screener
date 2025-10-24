from __future__ import annotations
import pandas as pd
import yfinance as yf
from typing import List
from src.models import RawBundle, PriceRow, FundamentalsQuarter

def _retry(fn, tries: int = 3):
    last = None
    for _ in range(tries):
        try:
            return fn()
        except Exception as e:
            last = e
    if last:
        raise last

def fetch_prices(ticker: str, period: str = "5y") -> pd.DataFrame:
    def _call():
        df = yf.download(ticker, period=period, auto_adjust=True, progress=False)
        if df is None or df.empty:
            return pd.DataFrame(columns=["date", "close"])

        df = df.copy()

        # Flatten MultiIndex columns
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [
                "_".join([str(c) for c in col if c is not None]).strip() or "unnamed"
                for col in df.columns
            ]

        # Reset index if it's a DatetimeIndex
        if isinstance(df.index, pd.DatetimeIndex):
            df.index.name = "date"
            df = df.reset_index()

        # Normalize column names
        df.columns = [str(c).lower() for c in df.columns]

        # Find the 'close' column
        close_col = next((c for c in df.columns if "close" in c), None)
        if "date" not in df.columns or close_col is None:
            raise KeyError(f"Expected columns 'date' and 'close', found: {df.columns.tolist()}")

        # Keep only date and close
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date", close_col]).sort_values("date").reset_index(drop=True)
        df.rename(columns={close_col: "close"}, inplace=True)
        return df[["date", "close"]]

    return _retry(_call, tries=3)


def fetch_fundamentals_q(ticker: str) -> List[FundamentalsQuarter]:
    try:
        t = yf.Ticker(ticker)
        qb = t.quarterly_balance_sheet
        ab = t.balance_sheet
        info = getattr(t, "info", {}) or {}
        shares = info.get("sharesOutstanding") or info.get("floatShares")

        src = qb if isinstance(qb, pd.DataFrame) and not qb.empty else ab
        rows: List[FundamentalsQuarter] = []

        if isinstance(src, pd.DataFrame) and not src.empty:
            df = src.copy()
            df.index = df.index.astype(str).str.lower()
            for c in df.columns:
                try:
                    pe = pd.to_datetime(c).date()
                except Exception:
                    continue

                def pick(keys):
                    for k in keys:
                        if k in df.index:
                            v = df.loc[k, c]
                            return float(v) if pd.notna(v) else None
                    return None

                total_debt = pick(["totaldebt", "total debt", "total debt net"])
                cash = pick(["cash", "cashandcashequivalents", "cash and cash equivalents"])
                book_value = pick(["totalstockholdersequity", "total stockholder equity", "total equity"])
                rows.append(FundamentalsQuarter(
                    period_end=pe,
                    total_debt=total_debt,
                    cash=cash,
                    shares_out=float(shares) if shares else None,
                    book_value=book_value,
                ))
        return rows
    except Exception:
        return []

def fetch_raw_bundle(ticker: str, period: str = "5y") -> RawBundle:
    prices_df = fetch_prices(ticker, period=period)
    if prices_df.empty:
        raise RuntimeError(f"No price data for {ticker}. Check symbol/network.")

    # Ensure 'date' exists
    if "date" not in prices_df.columns:
        raise KeyError(f"'date' column missing from prices DataFrame for {ticker}")

    prices: List[PriceRow] = [
        PriceRow(date=pd.to_datetime(r["date"]).date(), close=float(r["close"]))
        for _, r in prices_df.iterrows()
    ]

    fundamentals_q = fetch_fundamentals_q(ticker)
    return RawBundle(ticker=ticker, prices=prices, fundamentals_q=fundamentals_q)
