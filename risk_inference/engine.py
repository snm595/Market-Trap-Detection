"""
MarketTrap Unified Risk Engine - Institutional Logic
Combines ML anomaly detection with heuristic market structure and volume signals.
"""

from __future__ import annotations
import logging
from typing import Dict, List, Tuple, Optional
import numpy as np
import pandas as pd
from ml_pipeline.anomaly_model import IsolationForestModel
from risk_inference.realtime_trap_engine import build_component_scores, classify_trap_type, extract_trap_reasons, buyer_seller_control

logger = logging.getLogger(__name__)

class MarketTrapEngine:
    """Unified engine for real-time market trap detection."""
    
    def __init__(self, model_path: str = "models/isolation_forest.pkl"):
        try:
            self.model = IsolationForestModel.load(model_path)
            logger.info("MarketTrapEngine: Anomaly model loaded successfully.")
        except Exception as e:
            logger.warning(f"MarketTrapEngine: Could not load model from {model_path}. Anomaly scoring will be disabled. Error: {e}")
            self.model = None

    def compute_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardized feature engineering for the engine."""
        frame = df.copy()
        
        # Ensure we have numeric types
        frame['price'] = pd.to_numeric(frame['price'])
        frame['volume'] = pd.to_numeric(frame['volume'])
        
        # 1. Price returns
        frame['price_return'] = frame['price'].pct_change().fillna(0.0)
        
        # 2. Volume changes
        frame['volume_change'] = frame['volume'].pct_change().replace([np.inf, -np.inf], 0.0).fillna(0.0)
        
        # 3. Volatility (10-period rolling std of returns)
        frame['volatility'] = frame['price_return'].rolling(window=10, min_periods=1).std().fillna(0.0)
        
        # 4. Breakout strength (price relative to 20-period high)
        window_size = min(20, len(frame))
        rolling_max = frame['price'].rolling(window=window_size, min_periods=1).max().shift(1).fillna(frame['price'].iloc[0])
        frame['breakout_strength'] = (frame['price'] - rolling_max) / rolling_max.replace(0, np.nan)
        frame['is_breakout'] = (frame['price'] > rolling_max).astype(int)
        
        # 5. Price-Volume divergence
        frame['pv_divergence'] = ((frame['price_return'] > 0) & (frame['volume_change'] < 0)).astype(int)
        
        frame = frame.fillna(0)
        return frame

    def get_risk_snapshot(self, symbol: str, df_1m: pd.DataFrame) -> Dict:
        """Produce a comprehensive risk snapshot for a symbol based on recent 1m data."""
        if df_1m is None or len(df_1m) < 10:
            return {
                "risk_score": 0.0,
                "risk_level": "LOW",
                "trap_type": "Neutral",
                "reasons": [],
                "control": "Neutral",
                "components": {}
            }
            
        # 1. Feature Engineering
        features = self.compute_features(df_1m)
        
        # 2. Heuristic Components
        components, diagnostics = build_component_scores(df_1m)
        
        # 3. ML Anomaly Score
        ml_risk_pct = 0.0
        if self.model:
            try:
                ml_res = self.model.predict_latest_risk(features, symbol=symbol)
                ml_risk_pct = ml_res['risk_percentage']
            except Exception as e:
                logger.error(f"Error in ML prediction: {e}")
                
        anomaly_component = ml_risk_pct / 100.0
        
        # 4. Weighted Aggregation
        # Weights optimized for institutional distribution detection
        weighted_risk = (
            0.35 * components["structure_failure"] +
            0.25 * components["volume_behavior"] +
            0.20 * components["momentum_exhaustion"] +
            0.20 * anomaly_component
        ) * 100.0
        
        risk_score = round(max(0.0, min(weighted_risk, 100.0)), 1)
        
        # 5. Contextual metadata
        trap_type = classify_trap_type(components, anomaly_component)
        control = buyer_seller_control(df_1m)
        reasons = extract_trap_reasons(components, diagnostics, anomaly_component, max_reasons=3)
        
        risk_level = "LOW"
        if risk_score >= 70: risk_level = "CRITICAL"
        elif risk_score >= 40: risk_level = "ELEVATED"
        
        return {
            "risk_score": risk_score,
            "risk_level": risk_level,
            "trap_type": trap_type,
            "reasons": reasons,
            "control": control,
            "components": {**components, "anomaly": anomaly_component},
            "diagnostics": diagnostics
        }
