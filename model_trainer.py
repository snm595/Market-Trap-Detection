"""
Model training pipeline for MarketTrap
"""

import pandas as pd
import numpy as np
import joblib
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import yfinance as yf
from pathlib import Path

from ml_pipeline.anomaly_model import IsolationForestModel
from feature_engineering.spark_features import compute_features_spark
from simple_features import compute_features_pandas

logger = logging.getLogger(__name__)

class ModelTrainer:
    """Handles model training and validation for MarketTrap."""
    
    def __init__(self):
        self.model = None
        self.training_data = None
        self.validation_data = None
        self.feature_columns = [
            'price_return', 'volume_change', 'volatility',
            'breakout_strength', 'is_breakout', 'pv_divergence'
        ]
        self.model_path = "models/isolation_forest.pkl"
        self.scaler_path = "models/scaler.pkl"
        
    def collect_historical_data(self, symbols: List[str], period: str = "2y") -> pd.DataFrame:
        """Collect historical data for training."""
        logger.info(f"Collecting {period} of historical data for {symbols}")
        
        all_data = []
        
        for symbol in symbols:
            try:
                # Convert symbol format for yfinance
                yf_symbol = symbol.replace('USDT', '-USD')
                
                # Download historical data
                ticker = yf.Ticker(yf_symbol)
                hist = ticker.history(period=period, interval='1d')
                
                if hist.empty:
                    logger.warning(f"No data found for {symbol}")
                    continue
                
                # Process data
                df = hist.reset_index()
                df['symbol'] = symbol
                df['timestamp'] = df['Date']
                df['price'] = df['Close']
                df['volume'] = df['Volume']
                
                # Keep only needed columns
                df = df[['timestamp', 'symbol', 'price', 'volume', 'Open', 'High', 'Low', 'Close']]
                all_data.append(df)
                
                logger.info(f"Collected {len(df)} days of data for {symbol}")
                
            except Exception as e:
                logger.error(f"Error collecting data for {symbol}: {e}")
        
        if all_data:
            combined_data = pd.concat(all_data, ignore_index=True)
            logger.info(f"Total historical data: {len(combined_data)} records")
            return combined_data
        else:
            logger.warning("No historical data collected, using sample data")
            # Create sample data for demonstration
            return self._create_sample_data(symbols)
    
    def _create_sample_data(self, symbols: List[str]) -> pd.DataFrame:
        """Create sample training data when historical data is not available."""
        logger.info("Creating sample training data for demonstration")
        
        import numpy as np
        
        all_data = []
        
        for symbol in symbols:
            # Generate sample price data
            np.random.seed(42)
            days = 365
            
            # Base price for each symbol
            base_prices = {
                'BTCUSDT': 43000,
                'ETHUSDT': 2600,
                'SOLUSDT': 105,
                'BNBUSDT': 310
            }
            
            base_price = base_prices.get(symbol, 100)
            
            # Generate realistic price movements
            returns = np.random.normal(0, 0.02, days)  # 2% daily volatility
            prices = [base_price]
            
            for i in range(days):
                price_change = returns[i]
                new_price = prices[-1] * (1 + price_change)
                prices.append(new_price)
            
            # Generate volume data
            volumes = np.random.lognormal(15, 1, days)  # Log-normal volume distribution
            
            # Create DataFrame
            dates = pd.date_range(end=datetime.now(), periods=days, freq='D')
            
            df = pd.DataFrame({
                'timestamp': dates,
                'symbol': symbol,
                'price': prices[1:],  # Exclude initial price
                'volume': volumes,
                'Open': prices[:-1],
                'High': [max(prices[i], prices[i+1]) for i in range(len(prices)-1)],
                'Low': [min(prices[i], prices[i+1]) for i in range(len(prices)-1)],
                'Close': prices[1:]
            })
            
            all_data.append(df)
        
        combined_data = pd.concat(all_data, ignore_index=True)
        logger.info(f"Created {len(combined_data)} sample data points")
        return combined_data
    
    def prepare_training_data(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        logger.info("Preparing training data with features")
        
        # Group by symbol and compute features for each
        feature_data = []
        
        for symbol in raw_data['symbol'].unique():
            symbol_data = raw_data[raw_data['symbol'] == symbol].copy()
            symbol_data = symbol_data.sort_values('timestamp')
            
            # Compute features using existing function
            try:
                features_df = compute_features_pandas(symbol_data)
                if not features_df.empty:
                    feature_data.append(features_df)
            except Exception as e:
                logger.error(f"Error computing features for {symbol}: {e}")
        
        if feature_data:
            combined_features = pd.concat(feature_data, ignore_index=True)
            logger.info(f"Prepared {len(combined_features)} feature records")
            return combined_features
        else:
            return pd.DataFrame()
    
    def train_model(self, training_data: pd.DataFrame) -> IsolationForestModel:
        """Train isolation forest model."""
        logger.info("Training Isolation Forest model")
        
        try:
            # Ensure we have required columns
            missing_cols = set(self.feature_columns) - set(training_data.columns)
            if missing_cols:
                logger.error(f"Missing feature columns: {missing_cols}")
                return None
            
            # Extract features
            features = training_data[self.feature_columns].fillna(0)
            
            # Create and train model
            model = IsolationForestModel(
                contamination=0.1,  # Assume 10% anomalies
                n_estimators=100,
                random_state=42
            )
            
            model.fit(features)
            
            logger.info(f"Model trained on {len(features)} samples")
            logger.info(f"Feature columns used: {self.feature_columns}")
            
            return model
            
        except Exception as e:
            logger.error(f"Error training model: {e}")
            return None
    
    def validate_model(self, model: IsolationForestModel, validation_data: pd.DataFrame) -> Dict:
        """Validate model performance."""
        logger.info("Validating model performance")
        
        try:
            # Extract features
            features = validation_data[self.feature_columns].fillna(0)
            
            # Get predictions (using anomaly_score method)
            scores = model.anomaly_score(features)
            
            # Calculate metrics
            anomaly_rate = np.mean(scores < -1)  # -1 indicates anomaly in IsolationForest
            
            results = {
                'validation_samples': len(features),
                'anomaly_rate': anomaly_rate,
                'mean_score': np.mean(scores),
                'std_score': np.std(scores),
                'high_risk_count': np.sum(scores > 80),
                'validation_timestamp': datetime.now()
            }
            
            logger.info(f"Validation results: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Error validating model: {e}")
            return {}
    
    def save_model(self, model: IsolationForestModel, metadata: Dict = None):
        """Save trained model and metadata."""
        try:
            # Create models directory if it doesn't exist
            Path("models").mkdir(exist_ok=True)
            
            # Save model
            joblib.dump(model, self.model_path)
            logger.info(f"Model saved to {self.model_path}")
            
            # Save metadata
            if metadata:
                metadata_path = "models/model_metadata.pkl"
                joblib.dump(metadata, metadata_path)
                logger.info(f"Model metadata saved to {metadata_path}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error saving model: {e}")
            return False
    
    def load_model(self) -> Optional[IsolationForestModel]:
        """Load trained model."""
        try:
            if Path(self.model_path).exists():
                model = joblib.load(self.model_path)
                logger.info(f"Model loaded from {self.model_path}")
                return model
            else:
                logger.warning("No trained model found")
                return None
        except Exception as e:
            logger.error(f"Error loading model: {e}")
            return None
    
    def retrain_model(self, symbols: List[str] = None, validation_split: float = 0.2) -> bool:
        """Complete model retraining pipeline."""
        logger.info("Starting model retraining pipeline")
        
        try:
            # Default symbols if none provided
            if not symbols:
                symbols = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT']
            
            # Collect historical data
            raw_data = self.collect_historical_data(symbols)
            if raw_data.empty:
                logger.error("No training data available")
                return False
            
            # Prepare features
            training_data = self.prepare_training_data(raw_data)
            if training_data.empty:
                logger.error("No feature data prepared")
                return False
            
            # Split data
            split_idx = int(len(training_data) * (1 - validation_split))
            train_data = training_data.iloc[:split_idx]
            val_data = training_data.iloc[split_idx:]
            
            # Train model
            model = self.train_model(train_data)
            if not model:
                return False
            
            # Validate model
            validation_results = self.validate_model(model, val_data)
            
            # Save model
            metadata = {
                'training_symbols': symbols,
                'training_samples': len(train_data),
                'validation_results': validation_results,
                'feature_columns': self.feature_columns,
                'training_date': datetime.now().isoformat()
            }
            
            success = self.save_model(model, metadata)
            
            if success:
                logger.info("Model retraining completed successfully")
                return True
            else:
                return False
                
        except Exception as e:
            logger.error(f"Error in retraining pipeline: {e}")
            return False
    
    def get_feature_importance(self, model: IsolationForestModel) -> Dict:
        """Get feature importance from trained model."""
        try:
            if hasattr(model.model, 'feature_importances_'):
                importance = model.model.feature_importances_
                feature_importance = dict(zip(self.feature_columns, importance))
                
                # Sort by importance
                sorted_features = sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)
                
                logger.info("Feature importance:")
                for feature, score in sorted_features:
                    logger.info(f"  {feature}: {score:.4f}")
                
                return feature_importance
            else:
                logger.warning("Model does not have feature importances")
                return {}
                
        except Exception as e:
            logger.error(f"Error getting feature importance: {e}")
            return {}
    
    def backtest_model(self, model: IsolationForestModel, test_data: pd.DataFrame) -> Dict:
        """Backtest model on historical data."""
        logger.info("Running model backtest")
        
        try:
            features = test_data[self.feature_columns].fillna(0)
            scores = model.anomaly_score(features)
            
            # Simulate trading based on risk scores
            backtest_results = {
                'total_samples': len(features),
                'high_risk_periods': np.sum(scores > 80),
                'medium_risk_periods': np.sum((scores > 40) & (scores <= 80)),
                'low_risk_periods': np.sum(scores <= 40),
                'mean_risk_score': np.mean(scores),
                'max_risk_score': np.max(scores),
                'backtest_date': datetime.now()
            }
            
            logger.info(f"Backtest results: {backtest_results}")
            return backtest_results
            
        except Exception as e:
            logger.error(f"Error in backtest: {e}")
            return {}

# Global trainer instance
_trainer = None

def get_trainer() -> ModelTrainer:
    """Get or create trainer instance."""
    global _trainer
    if _trainer is None:
        _trainer = ModelTrainer()
    return _trainer

def train_new_model(symbols: List[str] = None) -> bool:
    """Train a new model with default settings."""
    trainer = get_trainer()
    return trainer.retrain_model(symbols)

def validate_current_model() -> Dict:
    """Validate the current loaded model."""
    trainer = get_trainer()
    model = trainer.load_model()
    
    if not model:
        return {'error': 'No model found'}
    
    # Use recent data for validation
    validation_data = trainer.collect_historical_data(['BTCUSDT'], period='5d')
    if validation_data.empty:
        return {'error': 'No validation data available'}
    
    features = trainer.prepare_training_data(validation_data)
    if features.empty:
        return {'error': 'No feature data available'}
    
    return trainer.validate_model(model, features)
