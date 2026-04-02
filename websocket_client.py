"""
WebSocket implementation for real-time Binance data streaming
"""

import websocket
import json
import threading
import time
import logging
from datetime import datetime
from typing import Dict, List, Callable, Optional
import queue

logger = logging.getLogger(__name__)

class BinanceWebSocket:
    """Real-time WebSocket connection to Binance for streaming market data."""
    
    def __init__(self):
        self.ws = None
        self.is_connected = False
        self.callbacks = {}
        self.data_queue = queue.Queue()
        self.thread = None
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 5
        
    def add_callback(self, stream: str, callback: Callable):
        """Add callback for specific stream data."""
        if stream not in self.callbacks:
            self.callbacks[stream] = []
        self.callbacks[stream].append(callback)
    
    def connect(self):
        """Connect to Binance WebSocket."""
        try:
            # Binance WebSocket URL for combined streams
            socket_url = "wss://stream.binance.com:9443/ws/btcusdt@ticker/ethusdt@ticker"
            
            self.ws = websocket.WebSocketApp(
                socket_url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close
            )
            
            # Start WebSocket in separate thread
            self.thread = threading.Thread(target=self._run_websocket, daemon=True)
            self.thread.start()
            
            logger.info("WebSocket connection initiated")
            
        except Exception as e:
            logger.error(f"Failed to connect to WebSocket: {e}")
            self._schedule_reconnect()
    
    def _run_websocket(self):
        """Run WebSocket connection."""
        try:
            self.ws.run_forever()
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
            self._schedule_reconnect()
    
    def _on_open(self, ws):
        """Called when WebSocket connection is established."""
        logger.info("WebSocket connection established")
        self.is_connected = True
        self.reconnect_attempts = 0
    
    def _on_message(self, ws, message):
        """Handle incoming WebSocket messages."""
        try:
            data = json.loads(message)
            
            # Process ticker data
            if 'e' in data and data['e'] == '24hrTicker':
                self._process_ticker_data(data)
            elif 'stream' in data:
                # Handle multi-stream data
                stream_data = data['data']
                if 'e' in stream_data and stream_data['e'] == '24hrTicker':
                    self._process_ticker_data(stream_data)
                    
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    def _process_ticker_data(self, data: Dict):
        """Process ticker data and trigger callbacks."""
        try:
            # Extract relevant data
            symbol = data.get('s', '')  # Symbol
            price = float(data.get('c', 0))  # Current price
            volume = float(data.get('v', 0))  # 24h volume
            high = float(data.get('h', 0))  # 24h high
            low = float(data.get('l', 0))  # 24h low
            open_price = float(data.get('o', 0))  # 24h open
            change = float(data.get('P', 0))  # Price change percentage
            
            # Create processed data object
            processed_data = {
                'symbol': symbol,
                'price': price,
                'volume': volume,
                'high': high,
                'low': low,
                'open': open_price,
                'change': change,
                'timestamp': datetime.now()
            }
            
            # Add to queue for processing
            self.data_queue.put(processed_data)
            
            # Trigger callbacks
            if 'ticker' in self.callbacks:
                for callback in self.callbacks['ticker']:
                    callback(processed_data)
                    
        except Exception as e:
            logger.error(f"Error processing ticker data: {e}")
    
    def _on_error(self, ws, error):
        """Handle WebSocket errors."""
        logger.error(f"WebSocket error: {error}")
        self.is_connected = False
        self._schedule_reconnect()
    
    def _on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket close."""
        logger.info(f"WebSocket closed: {close_status_code} - {close_msg}")
        self.is_connected = False
        self._schedule_reconnect()
    
    def _schedule_reconnect(self):
        """Schedule reconnection attempt."""
        if self.reconnect_attempts < self.max_reconnect_attempts:
            self.reconnect_attempts += 1
            delay = min(2 ** self.reconnect_attempts, 30)  # Exponential backoff
            logger.info(f"Reconnecting in {delay} seconds (attempt {self.reconnect_attempts})")
            
            threading.Timer(delay, self.connect).start()
        else:
            logger.error("Max reconnection attempts reached")
    
    def get_latest_data(self) -> Optional[Dict]:
        """Get latest data from queue."""
        try:
            return self.data_queue.get_nowait()
        except queue.Empty:
            return None
    
    def subscribe_to_tickers(self, symbols: List[str]):
        """Subscribe to multiple ticker streams."""
        if not self.is_connected:
            logger.warning("WebSocket not connected, cannot subscribe")
            return
        
        # Create subscription message
        streams = [f"{symbol.lower()}@ticker" for symbol in symbols]
        subscribe_msg = {
            "method": "SUBSCRIBE",
            "params": streams,
            "id": 1
        }
        
        try:
            self.ws.send(json.dumps(subscribe_msg))
            logger.info(f"Subscribed to tickers: {symbols}")
        except Exception as e:
            logger.error(f"Failed to subscribe: {e}")
    
    def close(self):
        """Close WebSocket connection."""
        if self.ws:
            self.ws.close()
        self.is_connected = False
        logger.info("WebSocket connection closed")

# Global WebSocket instance
_binance_ws = None

def get_websocket() -> BinanceWebSocket:
    """Get or create WebSocket instance."""
    global _binance_ws
    if _binance_ws is None:
        _binance_ws = BinanceWebSocket()
    return _binance_ws

def start_websocket(symbols: List[str] = None):
    """Start WebSocket connection for given symbols."""
    ws = get_websocket()
    if symbols:
        ws.subscribe_to_tickers(symbols)
    if not ws.is_connected:
        ws.connect()
    return ws
