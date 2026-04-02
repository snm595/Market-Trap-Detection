"""
Alert system for MarketTrap notifications
"""

import smtplib
import requests
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

class AlertType(Enum):
    """Types of alerts."""
    HIGH_RISK = "high_risk"
    PRICE_SPIKE = "price_spike"
    VOLUME_ANOMALY = "volume_anomaly"
    CONNECTION_LOST = "connection_lost"
    TRAP_DETECTED = "trap_detected"

class AlertChannel(Enum):
    """Alert delivery channels."""
    EMAIL = "email"
    SLACK = "slack"
    DISCORD = "discord"
    CONSOLE = "console"

@dataclass
class Alert:
    """Alert data structure."""
    alert_type: AlertType
    symbol: str
    message: str
    risk_score: float
    timestamp: datetime
    data: Dict = None

class AlertManager:
    """Manages alert generation and delivery."""
    
    def __init__(self):
        self.alert_history: List[Alert] = []
        self.channels: Dict[AlertChannel, bool] = {
            AlertChannel.CONSOLE: True,
            AlertChannel.EMAIL: False,
            AlertChannel.SLACK: False,
            AlertChannel.DISCORD: False
        }
        self.thresholds = {
            'high_risk': 80.0,
            'price_spike': 5.0,  # 5% change
            'volume_anomaly': 3.0  # 3x normal volume
        }
        
        # Email configuration
        self.email_config = {
            'smtp_server': 'smtp.gmail.com',
            'smtp_port': 587,
            'sender_email': '',
            'sender_password': '',
            'recipient_emails': []
        }
        
        # Slack configuration
        self.slack_config = {
            'webhook_url': '',
            'channel': '#market-alerts'
        }
        
        # Discord configuration
        self.discord_config = {
            'webhook_url': ''
        }
    
    def configure_email(self, sender_email: str, password: str, recipients: List[str]):
        """Configure email alerts."""
        self.email_config.update({
            'sender_email': sender_email,
            'sender_password': password,
            'recipient_emails': recipients
        })
        self.channels[AlertChannel.EMAIL] = True
        logger.info("Email alerts configured")
    
    def configure_slack(self, webhook_url: str, channel: str = '#market-alerts'):
        """Configure Slack alerts."""
        self.slack_config.update({
            'webhook_url': webhook_url,
            'channel': channel
        })
        self.channels[AlertChannel.SLACK] = True
        logger.info("Slack alerts configured")
    
    def configure_discord(self, webhook_url: str):
        """Configure Discord alerts."""
        self.discord_config['webhook_url'] = webhook_url
        self.channels[AlertChannel.DISCORD] = True
        logger.info("Discord alerts configured")
    
    def set_thresholds(self, high_risk: float = 80.0, price_spike: float = 5.0, 
                     volume_anomaly: float = 3.0):
        """Set alert thresholds."""
        self.thresholds.update({
            'high_risk': high_risk,
            'price_spike': price_spike,
            'volume_anomaly': volume_anomaly
        })
    
    def check_alerts(self, market_data: Dict) -> List[Alert]:
        """Check if market data triggers any alerts."""
        alerts = []
        
        symbol = market_data.get('symbol', '')
        risk_score = market_data.get('risk_score', 0)
        price = market_data.get('price', 0)
        volume = market_data.get('volume', 0)
        change = market_data.get('change', 0)
        
        # High risk alert
        if risk_score >= self.thresholds['high_risk']:
            alerts.append(Alert(
                alert_type=AlertType.HIGH_RISK,
                symbol=symbol,
                message=f"High risk detected: {risk_score:.1f}%",
                risk_score=risk_score,
                timestamp=datetime.now(),
                data=market_data
            ))
        
        # Price spike alert
        if abs(change) >= self.thresholds['price_spike']:
            direction = "up" if change > 0 else "down"
            alerts.append(Alert(
                alert_type=AlertType.PRICE_SPIKE,
                symbol=symbol,
                message=f"Price spike detected: {abs(change):.2f}% {direction}",
                risk_score=risk_score,
                timestamp=datetime.now(),
                data=market_data
            ))
        
        # Volume anomaly alert
        # Note: Would need historical volume average for proper comparison
        # Using simple threshold for now
        if volume > 1000000:  # Simple volume threshold
            alerts.append(Alert(
                alert_type=AlertType.VOLUME_ANOMALY,
                symbol=symbol,
                message=f"Unusual volume detected: {volume:,.0f}",
                risk_score=risk_score,
                timestamp=datetime.now(),
                data=market_data
            ))
        
        return alerts
    
    def send_alert(self, alert: Alert):
        """Send alert through all configured channels."""
        # Add to history
        self.alert_history.append(alert)
        
        # Keep only last 1000 alerts
        if len(self.alert_history) > 1000:
            self.alert_history = self.alert_history[-1000:]
        
        # Send through each enabled channel
        if self.channels[AlertChannel.CONSOLE]:
            self._send_console_alert(alert)
        
        if self.channels[AlertChannel.EMAIL]:
            self._send_email_alert(alert)
        
        if self.channels[AlertChannel.SLACK]:
            self._send_slack_alert(alert)
        
        if self.channels[AlertChannel.DISCORD]:
            self._send_discord_alert(alert)
    
    def _send_console_alert(self, alert: Alert):
        """Send alert to console."""
        timestamp_str = alert.timestamp.strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n🚨 ALERT [{timestamp_str}] {alert.symbol.upper()}")
        print(f"   Type: {alert.alert_type.value}")
        print(f"   Message: {alert.message}")
        print(f"   Risk Score: {alert.risk_score:.1f}%")
        print("=" * 50)
    
    def _send_email_alert(self, alert: Alert):
        """Send alert via email."""
        try:
            msg = MIMEMultipart()
            msg['From'] = self.email_config['sender_email']
            msg['To'] = ', '.join(self.email_config['recipient_emails'])
            msg['Subject'] = f"MarketTrap Alert: {alert.symbol.upper()} - {alert.alert_type.value}"
            
            body = f"""
MarketTrap Alert Alert

Symbol: {alert.symbol.upper()}
Type: {alert.alert_type.value}
Message: {alert.message}
Risk Score: {alert.risk_score:.1f}%
Time: {alert.timestamp.strftime("%Y-%m-%d %H:%M:%S")}

This is an automated alert from MarketTrap.
            """
            
            msg.attach(MIMEText(body, 'plain'))
            
            server = smtplib.SMTP(self.email_config['smtp_server'], self.email_config['smtp_port'])
            server.starttls()
            server.login(self.email_config['sender_email'], self.email_config['sender_password'])
            text = msg.as_string()
            server.sendmail(self.email_config['sender_email'], self.email_config['recipient_emails'], text)
            server.quit()
            
            logger.info(f"Email alert sent for {alert.symbol}")
            
        except Exception as e:
            logger.error(f"Failed to send email alert: {e}")
    
    def _send_slack_alert(self, alert: Alert):
        """Send alert to Slack."""
        try:
            if not self.slack_config['webhook_url']:
                return
            
            color = 'danger' if alert.risk_score >= 70 else 'warning' if alert.risk_score >= 40 else 'good'
            
            payload = {
                "channel": self.slack_config['channel'],
                "username": "MarketTrap",
                "icon_emoji": ":chart_with_upwards_trend:",
                "attachments": [{
                    "color": color,
                    "title": f"Alert: {alert.symbol.upper()}",
                    "text": alert.message,
                    "fields": [
                        {"title": "Type", "value": alert.alert_type.value, "short": True},
                        {"title": "Risk Score", "value": f"{alert.risk_score:.1f}%", "short": True},
                        {"title": "Time", "value": alert.timestamp.strftime("%H:%M:%S"), "short": True}
                    ]
                }]
            }
            
            response = requests.post(self.slack_config['webhook_url'], json=payload)
            if response.status_code == 200:
                logger.info(f"Slack alert sent for {alert.symbol}")
            else:
                logger.error(f"Failed to send Slack alert: {response.status_code}")
                
        except Exception as e:
            logger.error(f"Error sending Slack alert: {e}")
    
    def _send_discord_alert(self, alert: Alert):
        """Send alert to Discord."""
        try:
            if not self.discord_config['webhook_url']:
                return
            
            color = 0xFF0000 if alert.risk_score >= 70 else 0xFFAA00 if alert.risk_score >= 40 else 0x00FF00
            
            payload = {
                "username": "MarketTrap",
                "avatar_url": "https://i.imgur.com/4M34hi2.png",  # Chart icon
                "embeds": [{
                    "title": f"Alert: {alert.symbol.upper()}",
                    "description": alert.message,
                    "color": color,
                    "fields": [
                        {"name": "Type", "value": alert.alert_type.value, "inline": True},
                        {"name": "Risk Score", "value": f"{alert.risk_score:.1f}%", "inline": True},
                        {"name": "Time", "value": alert.timestamp.strftime("%H:%M:%S"), "inline": True}
                    ],
                    "timestamp": alert.timestamp.isoformat()
                }]
            }
            
            response = requests.post(self.discord_config['webhook_url'], json=payload)
            if response.status_code == 204:
                logger.info(f"Discord alert sent for {alert.symbol}")
            else:
                logger.error(f"Failed to send Discord alert: {response.status_code}")
                
        except Exception as e:
            logger.error(f"Error sending Discord alert: {e}")
    
    def get_alert_history(self, limit: int = 50) -> List[Alert]:
        """Get recent alert history."""
        return self.alert_history[-limit:]
    
    def clear_alert_history(self):
        """Clear alert history."""
        self.alert_history.clear()
        logger.info("Alert history cleared")

# Global alert manager instance
_alert_manager = None

def get_alert_manager() -> AlertManager:
    """Get or create alert manager instance."""
    global _alert_manager
    if _alert_manager is None:
        _alert_manager = AlertManager()
    return _alert_manager
