# main.py
import os
import json
import logging
import typer
import pandas as pd
import yaml
from datetime import datetime, date

# Package-local imports (adjust 'BRC' if your folder is named differently)
from src.data_fetcher import fetch_raw_bundle
from src.processor import process_bundle
from src.signals import detect_crossovers
from src.database import get_engine, init_schema, upsert_daily, upsert_signals, upsert_ticker
from src.models import ExportPayload, SignalEvent

app = typer.Typer(add_completion=False)

def load_cfg(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, (date, datetime)):
            return o.isoformat()
        if isinstance(o, SignalEvent):
            try:
                return o.model_dump()
            except Exception:
                return {"date": getattr(o, "date", None), "type": getattr(o, "type", None)}
        return super().default(o)

def _ensure_parent(path: str):
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)

@app.command()
def run(
    ticker: str = typer.Option(..., "--ticker", "-t", help="Stock symbol"),
    output: str = typer.Option(None, "--output", "-o", help="Output JSON file"),
    config: str = typer.Option("config.yaml", "--config"),
):
    # Load config
    cfg = load_cfg(config)
    logging.basicConfig(level=getattr(logging, cfg.get("log_level", "INFO")))
    logging.info(f"Starting run for {ticker}")

    # Ensure DB and output directories
    db_path = cfg.get("db_path", "data/app.db")
    _ensure_parent(db_path)
    out_dir = cfg.get("output_dir", "out")
    os.makedirs(out_dir, exist_ok=True)

    if output is None:
        output = os.path.join(out_dir, f"{ticker.upper()}.json")
    _ensure_parent(output)

    # DB setup
    engine = get_engine(db_path)
    init_schema(engine)

    # Fetch and process
    period = cfg.get("historical_period", "5y")
    min_sma_days = int(cfg.get("min_sma_days", 200))

    raw = fetch_raw_bundle(ticker, period=period)
    logging.info(f"Fetched {len(raw.prices)} price rows for {ticker}")

    df, rows_validated = process_bundle(raw, min_sma_days=min_sma_days)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    short_history = len(df) < min_sma_days
    logging.info(f"Processed {len(df)} rows; short_history={short_history}")

    # Prepare rows for DB
    tmp = df.copy()
    tmp["date_str"] = tmp["date"].dt.strftime("%Y-%m-%d")
    wanted = ["date_str","close","sma50","sma200","high_52w","bvps","pb","ev","pct_from_52w_high","is_52w_high"]
    present = [c for c in wanted if c in tmp.columns]
    subset = tmp.loc[:, present].rename(columns={"date_str": "date"})
    subset = subset.where(pd.notna(subset), None)

    # Persist metrics and ticker
    upsert_ticker(engine, ticker)
    upsert_daily(engine, ticker, subset)

    # Detect and persist signals
    events, contexts = detect_crossovers(ticker, df)
    upsert_signals(engine, ticker, [{"date": e.date, "type": e.type} for e in events])

    # Build payload
    payload = ExportPayload(
        ticker=ticker,
        generated_at=datetime.utcnow().isoformat(),
        metrics=[r.model_dump() for r in rows_validated],
        signals=events,
        notes={
            "rows": int(len(df)),
            "min_sma_days": min_sma_days,
            "data_source": "yfinance",
            "short_history": short_history,
            "event_contexts": contexts,
        },
    )

    # Save JSON
    logging.info(f"Writing JSON to {output}")
    with open(output, "w", encoding="utf-8") as f:
        json.dump(payload.model_dump(), f, ensure_ascii=False, indent=2, cls=EnhancedJSONEncoder)

    # Console summary
    typer.echo(f" Saved: {output}")
    typer.echo(f" Database updated at {db_path}")
    if events:
        typer.echo(f"⚡ Signals found: {len(events)}")
        for e in events[:5]:
            typer.echo(f"  - {e.type} on {e.date}")
    else:
        typer.echo(" No crossovers in the selected period.")

if __name__ == "__main__":
    import sys
    if len(sys.argv) == 1:
        sys.argv.append("run")
    app()
