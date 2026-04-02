"""
Configuration management for MarketTrap
"""

import os
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)

@dataclass
class APIConfig:
    """API configuration."""
    binance_api_key: str = ""
    binance_secret_key: str = ""
    rate_limit_per_minute: int = 1200
    timeout_seconds: int = 30

@dataclass
class DatabaseConfig:
    """Database configuration."""
    host: str = "localhost"
    port: int = 5432
    database: str = "markettrap"
    username: str = "postgres"
    password: str = ""
    ssl_mode: str = "prefer"

@dataclass
class WebSocketConfig:
    """WebSocket configuration."""
    binance_ws_url: str = "wss://stream.binance.com:9443"
    max_reconnect_attempts: int = 5
    reconnect_delay_seconds: int = 5
    heartbeat_interval: int = 30

@dataclass
class ModelConfig:
    """Model configuration."""
    contamination: float = 0.1
    n_estimators: int = 100
    max_samples: str = "auto"
    random_state: int = 42
    retrain_interval_hours: int = 24
    feature_columns: list = None

@dataclass
class AlertConfig:
    """Alert configuration."""
    high_risk_threshold: float = 80.0
    price_spike_threshold: float = 5.0
    volume_anomaly_multiplier: float = 3.0
    email_enabled: bool = False
    slack_enabled: bool = False
    discord_enabled: bool = False

@dataclass
class DashboardConfig:
    """Dashboard configuration."""
    default_symbols: list = None
    refresh_interval_seconds: int = 2
    max_chart_points: int = 100
    theme: str = "dark"
    port: int = 8502

@dataclass
class LoggingConfig:
    """Logging configuration."""
    level: str = "INFO"
    file_path: str = "logs/markettrap.log"
    max_file_size_mb: int = 10
    backup_count: int = 5
    console_output: bool = True

@dataclass
class MarketTrapConfig:
    """Main configuration class."""
    api: APIConfig = APIConfig()
    database: DatabaseConfig = DatabaseConfig()
    websocket: WebSocketConfig = WebSocketConfig()
    model: ModelConfig = ModelConfig()
    alerts: AlertConfig = AlertConfig()
    dashboard: DashboardConfig = DashboardConfig()
    logging: LoggingConfig = LoggingConfig()

class ConfigManager:
    """Manages configuration loading and saving."""
    
    def __init__(self, config_file: str = "config.json"):
        self.config_file = Path(config_file)
        self.config = MarketTrapConfig()
        self._load_config()
    
    def _load_config(self):
        """Load configuration from file."""
        try:
            if self.config_file.exists():
                with open(self.config_file, 'r') as f:
                    config_data = json.load(f)
                
                # Update config with loaded data
                self._update_config_object(self.config, config_data)
                logger.info(f"Configuration loaded from {self.config_file}")
            else:
                logger.info("No config file found, using defaults")
                self._create_default_config()
                
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            logger.info("Using default configuration")
    
    def _update_config_object(self, config_obj: MarketTrapConfig, data: Dict):
        """Update configuration object with dictionary data."""
        for section_name, section_data in data.items():
            if hasattr(config_obj, section_name):
                section_obj = getattr(config_obj, section_name)
                for key, value in section_data.items():
                    if hasattr(section_obj, key):
                        setattr(section_obj, key, value)
    
    def _create_default_config(self):
        """Create default configuration file."""
        try:
            # Set default values
            self.config.api.binance_api_key = os.getenv('BINANCE_API_KEY', '')
            self.config.dashboard.default_symbols = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT']
            self.config.model.feature_columns = [
                'price_return', 'volume_change', 'volatility',
                'breakout_strength', 'is_breakout', 'pv_divergence'
            ]
            
            # Create directories
            Path("logs").mkdir(exist_ok=True)
            Path("models").mkdir(exist_ok=True)
            Path("data").mkdir(exist_ok=True)
            
            # Save default config
            self.save_config()
            logger.info("Default configuration created")
            
        except Exception as e:
            logger.error(f"Error creating default config: {e}")
    
    def save_config(self):
        """Save current configuration to file."""
        try:
            config_dict = asdict(self.config)
            with open(self.config_file, 'w') as f:
                json.dump(config_dict, f, indent=2, default=str)
            logger.info(f"Configuration saved to {self.config_file}")
            return True
        except Exception as e:
            logger.error(f"Error saving config: {e}")
            return False
    
    def get_config(self) -> MarketTrapConfig:
        """Get current configuration."""
        return self.config
    
    def update_api_config(self, **kwargs):
        """Update API configuration."""
        for key, value in kwargs.items():
            if hasattr(self.config.api, key):
                setattr(self.config.api, key, value)
        self.save_config()
    
    def update_model_config(self, **kwargs):
        """Update model configuration."""
        for key, value in kwargs.items():
            if hasattr(self.config.model, key):
                setattr(self.config.model, key, value)
        self.save_config()
    
    def update_alert_config(self, **kwargs):
        """Update alert configuration."""
        for key, value in kwargs.items():
            if hasattr(self.config.alerts, key):
                setattr(self.config.alerts, key, value)
        self.save_config()
    
    def update_dashboard_config(self, **kwargs):
        """Update dashboard configuration."""
        for key, value in kwargs.items():
            if hasattr(self.config.dashboard, key):
                setattr(self.config.dashboard, key, value)
        self.save_config()
    
    def get_env_config(self) -> Dict[str, Any]:
        """Get configuration from environment variables."""
        env_config = {}
        
        # API configuration
        if os.getenv('BINANCE_API_KEY'):
            env_config['api'] = {
                'binance_api_key': os.getenv('BINANCE_API_KEY'),
                'binance_secret_key': os.getenv('BINANCE_SECRET_KEY', '')
            }
        
        # Database configuration
        if os.getenv('DATABASE_URL'):
            env_config['database'] = {'database_url': os.getenv('DATABASE_URL')}
        
        # Alert configuration
        if os.getenv('ALERT_EMAIL'):
            env_config['alerts'] = {
                'email_enabled': True,
                'email_recipient': os.getenv('ALERT_EMAIL')
            }
        
        return env_config
    
    def apply_env_config(self):
        """Apply environment variable configuration."""
        env_config = self.get_env_config()
        if env_config:
            self._update_config_object(self.config, env_config)
            logger.info("Applied environment configuration")
    
    def validate_config(self) -> Dict[str, Any]:
        """Validate configuration and return issues."""
        issues = []
        
        # Check API configuration
        if not self.config.api.binance_api_key:
            issues.append("Binance API key not configured")
        
        # Check model configuration
        if self.config.model.contamination <= 0 or self.config.model.contamination >= 1:
            issues.append("Model contamination should be between 0 and 1")
        
        # Check alert thresholds
        if self.config.alerts.high_risk_threshold < 0 or self.config.alerts.high_risk_threshold > 100:
            issues.append("High risk threshold should be between 0 and 100")
        
        # Check dashboard configuration
        if self.config.dashboard.refresh_interval_seconds < 1:
            issues.append("Dashboard refresh interval should be at least 1 second")
        
        return {
            'valid': len(issues) == 0,
            'issues': issues
        }
    
    def get_database_url(self) -> str:
        """Get complete database URL."""
        if hasattr(self.config.database, 'database_url'):
            return self.config.database.database_url
        
        return (f"postgresql://{self.config.database.username}:{self.config.database.password}@"
                f"{self.config.database.host}:{self.config.database.port}/{self.config.database.database}")
    
    def setup_logging(self):
        """Setup logging based on configuration."""
        try:
            # Create logs directory
            Path("logs").mkdir(exist_ok=True)
            
            # Configure logging
            log_level = getattr(logging, self.config.logging.level.upper(), logging.INFO)
            
            handlers = []
            
            # File handler
            from logging.handlers import RotatingFileHandler
            file_handler = RotatingFileHandler(
                self.config.logging.file_path,
                maxBytes=self.config.logging.max_file_size_mb * 1024 * 1024,
                backupCount=self.config.logging.backup_count
            )
            file_handler.setLevel(log_level)
            handlers.append(file_handler)
            
            # Console handler
            if self.config.logging.console_output:
                console_handler = logging.StreamHandler()
                console_handler.setLevel(log_level)
                handlers.append(console_handler)
            
            # Configure root logger
            logging.basicConfig(
                level=log_level,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                handlers=handlers
            )
            
            logger.info("Logging configured")
            
        except Exception as e:
            logging.error(f"Error setting up logging: {e}")

# Global configuration manager
_config_manager = None

def get_config_manager(config_file: str = "config.json") -> ConfigManager:
    """Get or create configuration manager."""
    global _config_manager
    if _config_manager is None:
        _config_manager = ConfigManager(config_file)
    return _config_manager

def get_config() -> MarketTrapConfig:
    """Get current configuration."""
    return get_config_manager().get_config()

def setup_project():
    """Setup project with configuration."""
    config_manager = get_config_manager()
    config_manager.apply_env_config()
    config_manager.setup_logging()
    
    # Validate configuration
    validation = config_manager.validate_config()
    if not validation['valid']:
        logger.warning("Configuration issues found:")
        for issue in validation['issues']:
            logger.warning(f"  - {issue}")
    
    return config_manager.get_config()
