"""
Binance WebSocket client for real-time crypto trade and ticker data.
Handles connection management, buffering, and data processing.
"""

import json
import time
import threading
import logging
from collections import deque
from typing import Dict, List, Optional, Deque
import websocket
import pandas as pd
import requests
try:
    import yfinance as yf
except ImportError:
    yf = None

logger = logging.getLogger(__name__)

class BinanceWSClient:
    """Robust Binance WebSocket client for streaming market data."""
    
    def __init__(self, symbols: List[str] = None):
        self.symbols = [s.lower() for s in (symbols or ["btcusdt"])]
        self.base_url = "wss://stream.binance.com:9443"
        self.tick_buffers: Dict[str, Deque[Dict]] = {s: deque(maxlen=1000) for s in self.symbols}
        self.last_price: Dict[str, float] = {s: 0.0 for s in self.symbols}
        self.last_update: Dict[str, float] = {s: 0.0 for s in self.symbols}
        self.ws: Optional[websocket.WebSocketApp] = None
        self.is_running = False
        self.reconnect_delay = 5
        # Flag set when Binance rejects connections (e.g. HTTP 451 geographic restriction)
        self.restricted = False
        self.last_error = None
        self.fallback_active = False
        self._fallback_thread: Optional[threading.Thread] = None

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
            
            # Handle Ticker data (24hr ticker or Mini-Ticker)
            # We'll use individual symbol streams for simplicity
            stream_name = data.get("stream", "")
            msg_data = data.get("data", {})
            
            if "@ticker" in stream_name:
                symbol = msg_data.get("s", "").lower()
                if symbol in self.symbols:
                    tick = {
                        "timestamp": msg_data["E"] / 1000,
                        "price": float(msg_data["c"]),
                        "high": float(msg_data["h"]),
                        "low": float(msg_data["l"]),
                        "volume": float(msg_data["v"]),
                        "quote_volume": float(msg_data["q"]),
                        "symbol": symbol
                    }
                    self.tick_buffers[symbol].append(tick)
                    self.last_price[symbol] = tick["price"]
                    self.last_update[symbol] = tick["timestamp"]
            
            elif "@trade" in stream_name:
                symbol = msg_data.get("s", "").lower()
                if symbol in self.symbols:
                    # You could process individual trades here for higher resolution
                    pass
                    
        except Exception as e:
            logger.error(f"Error processing WS message: {e}")

    def _on_error(self, ws, error):
        err_str = str(error)
        logger.error(f"WebSocket error: {err_str}")
        # Save last error for diagnostics
        self.last_error = err_str

        # Detect Binance-restricted responses (HTTP 451 / eligibility messages)
        lower = err_str.lower()
        if '451' in err_str or 'restricted location' in lower or 'eligibility' in lower:
            self.restricted = True
            logger.error("Connection rejected by Binance: restricted location (HTTP 451).")
        else:
            # keep previous restricted flag only if we continue to see errors; otherwise clear
            # but prefer explicit detection above
            self.restricted = getattr(self, 'restricted', False)

    def _on_close(self, ws, close_status_code, close_msg):
        logger.warning(f"WebSocket closed: {close_status_code} - {close_msg}")
        if getattr(self, 'restricted', False):
            logger.error("WebSocket closed due to Binance restriction (HTTP 451). No retry will resolve this from this host.")
        if self.is_running:
            logger.info(f"Attempting to reconnect in {self.reconnect_delay} seconds...")
            time.sleep(self.reconnect_delay)
            self._connect()

    def _on_open(self, ws):
        logger.info(f"WebSocket connected for symbols: {self.symbols}")
        # Subscriptions happen via the URL in this implementation, 
        # but could also be done via send() commands.

    def _connect(self):
        # Build stream path
        # Example: btcbusd@ticker/ethbusd@ticker
        streams = "/".join([f"{s}@ticker" for s in self.symbols])
        url = f"{self.base_url}/stream?streams={streams}"
        
        self.ws = websocket.WebSocketApp(
            url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close
        )
        
        self.ws.run_forever(ping_interval=20, ping_timeout=10)

    def start(self):
        """Start the WebSocket connection in a background thread."""
        if self.is_running:
            return
        self.is_running = True
        self.thread = threading.Thread(target=self._connect, daemon=True)
        self.thread.start()
        logger.info("Binance WS Thread started.")

    def stop(self):
        """Stop the WebSocket connection."""
        self.is_running = False
        if self.ws:
            self.ws.close()
        logger.info("Binance WS client stopped.")

    def get_latest_data(self, symbol: str) -> Optional[pd.DataFrame]:
        """Return the buffered data for a symbol as a DataFrame."""
        symbol = symbol.lower()
        buffer = self.tick_buffers.get(symbol)
        if not buffer:
            return None
        
        # Lock not strictly needed due to GIL and deque thread-safety for append/pop
        # but list(buffer) might have slight race conditions during heavy updates
        df = pd.DataFrame(list(buffer))
        if df.empty and self.restricted and not self.fallback_active:
            self._start_fallback()
        return df

    def _start_fallback(self):
        """Start a background thread to fetch data from Yahoo Finance if Binance is blocked."""
        if self.fallback_active or not yf:
            return
        
        self.fallback_active = True
        self._fallback_thread = threading.Thread(target=self._fallback_loop, daemon=True)
        self._fallback_thread.start()
        logger.info("Yahoo Finance Fallback thread started.")

    def _fallback_loop(self):
        """Periodically fetch 'semi-live' data from yfinance for all symbols."""
        while self.is_running and self.fallback_active:
            try:
                for symbol in self.symbols:
                    # yfinance uses symbols like BTC-USD
                    yf_sym = symbol.upper()
                    if "USDT" in yf_sym:
                        yf_sym = yf_sym.replace("USDT", "-USD")
                    
                    ticker = yf.Ticker(yf_sym)
                    # Fetching 1m interval for last day
                    df = ticker.history(period="1d", interval="1m").tail(5)
                    if not df.empty:
                        for idx, row in df.iterrows():
                            tick = {
                                "timestamp": idx.timestamp(),
                                "price": float(row["Close"]),
                                "high": float(row["High"]),
                                "low": float(row["Low"]),
                                "volume": float(row["Volume"]),
                                "symbol": symbol.lower()
                            }
                            # Check if timestamp is already in buffer to avoid duplicates
                            last_tick = self.tick_buffers[symbol.lower()][-1] if self.tick_buffers[symbol.lower()] else None
                            if not last_tick or tick["timestamp"] > last_tick["timestamp"]:
                                self.tick_buffers[symbol.lower()].append(tick)
                                self.last_price[symbol.lower()] = tick["price"]
                                self.last_update[symbol.lower()] = tick["timestamp"]
                
                time.sleep(15) # Poll every 15s for "semi-live" feel
            except Exception as e:
                logger.error(f"Fallback fetch error: {e}")
                time.sleep(30)

if __name__ == "__main__":
    # Test script
    logging.basicConfig(level=logging.INFO)
    client = BinanceWSClient(symbols=["btcusdt", "ethusdt"])
    client.start()
    
    try:
        while True:
            time.sleep(5)
            df = client.get_latest_data("btcusdt")
            if df is not None and not df.empty:
                print(f"Latest BTC Price: {df['price'].iloc[-1]} | Ticks: {len(df)}")
            else:
                print("Connecting...")
    except KeyboardInterrupt:
        client.stop()
