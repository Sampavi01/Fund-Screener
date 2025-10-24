# src/signals.py
from __future__ import annotations
import pandas as pd
from typing import List, Tuple, Dict, Any
from src.models import SignalEvent
def detect_crossovers(ticker: str, df: pd.DataFrame) -> Tuple[List[SignalEvent], List[Dict[str, Any]]]:
    d = df.copy()
    d["date"] = pd.to_datetime(d["date"], errors="coerce")
    d = d.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

    events: List[SignalEvent] = []
    contexts: List[Dict[str, Any]] = []
    prev_state = None
    prev_sma50 = None
    prev_sma200 = None

    for _, row in d.iterrows():
        sma50 = row.get("sma50")
        sma200 = row.get("sma200")
        if pd.isna(sma50) or pd.isna(sma200):
            prev_state = None
            prev_sma50 = None
            prev_sma200 = None
            continue

        cur = 1 if float(sma50) >= float(sma200) else 0
        dt = pd.to_datetime(row["date"]).date()

        if prev_state is not None:
            if prev_state == 0 and cur == 1:
                events.append(SignalEvent(ticker=ticker, date=dt, type="golden_cross"))
                contexts.append({
                    "date": str(dt),
                    "sma50_prev": float(prev_sma50),
                    "sma200_prev": float(prev_sma200),
                    "sma50_cur": float(sma50),
                    "sma200_cur": float(sma200),
                    "type": "golden_cross",
                })
            elif prev_state == 1 and cur == 0:
                events.append(SignalEvent(ticker=ticker, date=dt, type="death_cross"))
                contexts.append({
                    "date": str(dt),
                    "sma50_prev": float(prev_sma50),
                    "sma200_prev": float(prev_sma200),
                    "sma50_cur": float(sma50),
                    "sma200_cur": float(sma200),
                    "type": "death_cross",
                })
        prev_state = cur
        prev_sma50 = float(sma50)
        prev_sma200 = float(sma200)

    return events, contexts
