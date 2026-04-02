"""
Simple feature engineering using pandas (fallback when Spark is not available)
"""

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

def compute_features_pandas(df: pd.DataFrame) -> pd.DataFrame:
    """Compute features using pandas when Spark is not available."""
    logger.info("Computing features using pandas")
    
    try:
        # Ensure we have required columns
        required_cols = ['timestamp', 'price', 'volume']
        for col in required_cols:
            if col not in df.columns:
                logger.error(f"Missing required column: {col}")
                return pd.DataFrame()
        
        # Sort by timestamp
        df = df.sort_values('timestamp').copy()
        
        # Ensure numeric types
        df['price'] = pd.to_numeric(df['price'])
        df['volume'] = pd.to_numeric(df['volume'])
        
        # 1. Price returns
        df['price_return'] = df['price'].pct_change()
        
        # 2. Volume changes
        df['volume_change'] = df['volume'].pct_change()
        
        # 3. Volatility (10-period rolling std of returns)
        df['volatility'] = df['price_return'].rolling(window=10, min_periods=1).std()
        
        # 4. Breakout strength (price relative to 20-period high)
        window_size = min(20, len(df))
        df['rolling_max'] = df['price'].rolling(window=window_size, min_periods=1).max()
        df['breakout_strength'] = (df['price'] - df['rolling_max']) / df['rolling_max']
        df['is_breakout'] = (df['price'] > df['rolling_max'].shift(1)).astype(int)
        
        # 5. Price-Volume divergence
        pv_corr = df['price_return'].rolling(window=10, min_periods=1).corr(df['volume_change'])
        df['pv_divergence'] = ((df['price_return'] > 0) & (df['volume_change'] < 0)) | \
                                  ((df['price_return'] < 0) & (df['volume_change'] > 0))
        df['pv_divergence'] = df['pv_divergence'].astype(int)
        
        # Fill any remaining NA values
        df = df.fillna(0)
        
        # Ensure we have all required columns
        feature_columns = [
            'price_return', 'volume_change', 'volatility',
            'breakout_strength', 'is_breakout', 'pv_divergence'
        ]
        
        # Add missing columns with default values if needed
        for col in feature_columns:
            if col not in df.columns:
                df[col] = 0
        
        # Return only feature columns plus metadata
        result_cols = ['timestamp', 'symbol'] + feature_columns
        available_cols = [col for col in result_cols if col in df.columns]
        
        logger.info(f"Computed {len(df)} feature records")
        return df[available_cols]
        
    except Exception as e:
        logger.error(f"Error computing features: {e}")
        return pd.DataFrame()
