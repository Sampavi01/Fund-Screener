# src/models.py
from __future__ import annotations
from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, field_validator

class PriceRow(BaseModel):
    date: date
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: float
    volume: Optional[int] = None

    @field_validator("high")
    def high_ge_low(cls, v, info):
        values = info.data
        low = values.get("low")
        if v is not None and low is not None and v < low:
            raise ValueError("high must be >= low")
        return v

class FundamentalsQuarter(BaseModel):
    period_end: date
    total_debt: Optional[Decimal] = None
    cash: Optional[Decimal] = None
    shares_out: Optional[Decimal] = None
    book_value: Optional[Decimal] = None
    revenue: Optional[Decimal] = None
    ebitda: Optional[Decimal] = None

class RawBundle(BaseModel):
    ticker: str
    prices: List[PriceRow]
    fundamentals_q: List[FundamentalsQuarter] = Field(default_factory=list)

class ProcessedRow(BaseModel):
    date: date
    close: float
    sma50: Optional[float] = None
    sma200: Optional[float] = None
    high_52w: Optional[float] = None
    pct_from_52w_high: Optional[float] = None
    is_52w_high: Optional[bool] = None
    bvps: Optional[float] = None
    pb: Optional[float] = None
    ev: Optional[float] = None

class SignalEvent(BaseModel):
    ticker: str
    date: date
    type: str  # 'golden_cross' | 'death_cross'

class ExportPayload(BaseModel):
    ticker: str
    generated_at: str
    metrics: List[Dict[str, Any]]
    signals: List[SignalEvent]
    notes: Dict[str, Any]
