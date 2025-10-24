import pandas as pd

from src.signals import detect_crossovers

def test_crossovers_detected_and_early_period_suppressed():
    # Construct series where 50 crosses above 200 and then below
    dates = pd.bdate_range("2024-01-01", periods=6)
    df = pd.DataFrame({
        "date": dates,
        "sma50": [None, None, 9, 11, 10, 8],   # becomes >= then <
        "sma200": [None, None, 10, 10, 10, 9],
    })
    events, contexts = detect_crossovers("TEST", df)

    # Expect golden on index 3 (prev 9<10, cur 11>=10), death on index 5 (prev 10>=10, cur 8<9)
    types = [e.type for e in events]
    assert types == ["golden_cross", "death_cross"]
    # Contexts correspond to events and include prev/cur values
    assert all(k in contexts[0] for k in ["sma50_prev","sma200_prev","sma50_cur","sma200_cur"])
