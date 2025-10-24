<<<<<<< HEAD
# Fund-Screener
=======
# Fund Screener — Production-Grade Financial Analysis Pipeline

A command-line pipeline that ingests daily prices and quarterly fundamentals from yfinance, validates with Pydantic models, computes technical/fundamental metrics, detects golden/death cross signals, persists to SQLite, and exports per‑ticker JSON.

## Project structure
src/
init.py
data_fetcher.py
processor.py
signals.py
database.py
models.py
main.py

test/
test_processor.py
test_signals.py
test_database.py
config.yaml

## Features
- Price ingest with retry and normalization to a clean time series.  
- Fundamentals ingest with quarterly→annual fallback and normalization (book value, cash, debt, shares).  
- Pydantic models for raw and processed rows, signal events, and the export payload.  
- Strict rolling windows: SMA‑50, SMA‑200, 52‑week high with min_periods to suppress early false signals.  
- Derived metrics: pct_from_52w_high and is_52w_high, plus BVPS, P/B, and simplified EV.  
- Golden/death cross detection with early‑period suppression and optional event context.  
- SQLite persistence with idempotent upserts: tickers, daily_metrics, signal_events.  
- CLI JSON export per ticker with provenance notes and generation timestamp.  

## Requirements
- Python 3.10+  
- Recommended: virtual environment (venv/conda)  
- Install dependencies:
## Configuration
Create config.yaml at the repository root:
db_path: data/app.db
output_dir: out
historical_period: "5y"
min_sma_days: 200
log_level: INFO
tickers: [NVDA, AAPL, RELIANCE.NS]
Notes:
- NSE symbols require the .NS suffix (e.g., RELIANCE.NS).  
- Recent IPOs (<10 months) won’t have SMA‑200 or 52‑week metrics until enough history accumulates.  

## Usage
Run commands from the repository root (the folder that contains src/).

Single ticker (prints JSON and writes a file):
python -m src.main --ticker NVDA --output out/NVDA.json --config config.yaml

Multiple tickers (writes one file per symbol to output_dir):
python -m src.main --tickers NVDA,AAPL,RELIANCE.NS --config config.yaml


What the CLI does:
1) Fetch prices (and attempt fundamentals).  
2) Process: SMA50/200, 52w high, pct_from_52w_high, is_52w_high; derive BVPS/PB/EV if fundamentals present.  
3) Detect golden/death crosses (suppressed until SMAs are valid).  
4) Upsert daily metrics and signal events into SQLite (idempotent).  
5) Export a JSON payload with metrics, signals, and notes.  

## Outputs
- SQLite at db_path:
  - tickers(symbol)
  - daily_metrics(symbol, date, close, sma50, sma200, high_52w, bvps, pb, ev, pct_from_52w_high, is_52w_high)
  - signal_events(symbol, date, type)

- JSON at output_dir/TICKER.json:
{
"ticker": "NVDA",
"generated_at": "2025-03-11T12:00:00.000Z",
"metrics": [
{
"date": "2025-03-07",
"close": 238.49,
"sma50": 238.02,
"sma200": 226.09,
"high_52w": 258.10,
"pct_from_52w_high": -0.0760,
"is_52w_high": false,
"bvps": null,
"pb": null,
"ev": null
}
],
"signals": [],
"notes": {
"rows": 1256,
"min_sma_days": 200,
"data_source": "yfinance",
"short_history": false
}
}
## Tests
Run from the repository root (the parent of src/ and test/).

- All tests:
pytest -q test
>>>>>>> 6cd5c81 (Initial commit)
