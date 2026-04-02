import numpy as np
import pandas as pd

from risk_inference.realtime_trap_engine import (
    build_component_scores,
    classify_trap_type,
    extract_trap_reasons,
    buyer_seller_control,
)
from risk_inference.engine import MarketTrapEngine


def _sample_df(rows=80, drift=0.0008, vol_scale=0.015):
    rng = np.random.default_rng(123)
    returns = rng.normal(drift, vol_scale / 100.0, rows)
    price = 100 * np.cumprod(1 + returns)
    volume = rng.integers(1000, 5000, rows).astype(float)
    ts = pd.date_range("2026-01-01", periods=rows, freq="1min")
    return pd.DataFrame({
        "timestamp": (ts.astype("int64") // 10**9).astype(int),
        "price": price,
        "volume": volume,
    })


def test_component_scores_are_bounded():
    df = _sample_df()
    components, diagnostics = build_component_scores(df)
    assert diagnostics is not None
    for key in ("structure_failure", "volume_behavior", "momentum_exhaustion"):
        assert 0.0 <= components[key] <= 1.0


def test_reason_engine_returns_top_2_to_3():
    components = {
        "structure_failure": 0.82,
        "volume_behavior": 0.57,
        "momentum_exhaustion": 0.43,
    }
    diagnostics = {
        "breakout_failure_strength": 0.80,
        "price_up_volume_down_strength": 0.56,
        "rsi_fall_strength": 0.42,
    }
    reasons = extract_trap_reasons(components, diagnostics, anomaly_component=0.35, max_reasons=3)

    assert 2 <= len(reasons) <= 3
    confidences = [r["confidence"] for r in reasons]
    assert confidences == sorted(confidences, reverse=True)


def test_trap_classification_mapping():
    assert classify_trap_type({"structure_failure": 0.9, "volume_behavior": 0.1, "momentum_exhaustion": 0.1}, 0.1) == "Breakout Failure Trap"
    assert classify_trap_type({"structure_failure": 0.1, "volume_behavior": 0.8, "momentum_exhaustion": 0.1}, 0.1) == "Distribution Trap"
    assert classify_trap_type({"structure_failure": 0.1, "volume_behavior": 0.1, "momentum_exhaustion": 0.1}, 0.9) == "Liquidity Sweep Trap"
    assert classify_trap_type({"structure_failure": 0.1, "volume_behavior": 0.1, "momentum_exhaustion": 0.7}, 0.2) == "Fake Momentum Trap"


def test_control_indicator_outputs_expected_labels():
    df = _sample_df(drift=0.0015)
    control = buyer_seller_control(df)
    assert control in {"Buyers in Control", "Sellers in Control", "Neutral"}


def test_engine_snapshot_has_required_contract():
    engine = MarketTrapEngine(model_path="models/isolation_forest.pkl")
    df = _sample_df(rows=120)
    snap = engine.get_risk_snapshot("btcusdt", df)

    for key in ["risk_score", "risk_level", "trap_type", "reasons", "control", "components"]:
        assert key in snap
    assert 0.0 <= snap["risk_score"] <= 100.0
