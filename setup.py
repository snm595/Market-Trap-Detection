#!/usr/bin/env python3
"""
Quick setup script for MarketTrap
"""

import sys
import logging
from model_trainer import train_new_model
from config_manager import setup_project

def main():
    """Setup MarketTrap with trained model."""
    print("🚀 Setting up MarketTrap...")
    
    # Setup project
    config = setup_project()
    
    print("📊 Training anomaly detection model...")
    print("This may take a few minutes as we collect historical data...")
    
    # Train model
    success = train_new_model()
    
    if success:
        print("✅ Model training completed successfully!")
        print("🎯 You can now run the dashboard:")
        print("   streamlit run dashboard_live.py")
        print("\n📊 Dashboard will be available at:")
        print("   http://localhost:8502")
        print("\n⚙️ Configuration saved to: config.json")
        print("📝 Logs saved to: logs/markettrap.log")
    else:
        print("❌ Model training failed!")
        print("Please check the logs for more information.")
        sys.exit(1)

if __name__ == "__main__":
    main()
