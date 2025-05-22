import logging
from polygon import WebSocketClient
from typing import Callable, Dict, List
import json
import pandas as pd
from datetime import datetime
import asyncio

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class RealTimeDataHandler:
    def __init__(self, api_key: str):
        """
        Initialize the real-time data handler.
        
        Args:
            api_key (str): Your Polygon.io API key
        """
        self.api_key = api_key
        self.ws_client = None
        self.data_buffer = {}
        self.callbacks = {}
        
    async def connect(self):
        """Establish WebSocket connection to Polygon.io"""
        try:
            self.ws_client = WebSocketClient(self.api_key)
            await self.ws_client.connect()
            logging.info("Successfully connected to Polygon.io WebSocket")
        except Exception as e:
            logging.error(f"Failed to connect to Polygon.io WebSocket: {e}")
            raise

    async def subscribe(self, tickers: List[str], callback: Callable = None):
        """
        Subscribe to real-time data for specified tickers.
        
        Args:
            tickers (List[str]): List of ticker symbols to subscribe to
            callback (Callable, optional): Function to call when new data arrives
        """
        if not self.ws_client:
            await self.connect()
            
        try:
            # Subscribe to trades for each ticker
            for ticker in tickers:
                await self.ws_client.subscribe(f"T.{ticker}")
                self.data_buffer[ticker] = []
                if callback:
                    self.callbacks[ticker] = callback
                logging.info(f"Subscribed to real-time data for {ticker}")
        except Exception as e:
            logging.error(f"Failed to subscribe to tickers: {e}")
            raise

    async def handle_message(self, message: Dict):
        """Handle incoming WebSocket messages"""
        try:
            if message.get('ev') == 'T':  # Trade event
                ticker = message.get('sym')
                if ticker in self.data_buffer:
                    # Convert message to DataFrame row
                    trade_data = {
                        'timestamp': pd.to_datetime(message.get('t'), unit='ms', utc=True),
                        'price': float(message.get('p')),
                        'size': int(message.get('s')),
                        'exchange': message.get('x'),
                        'conditions': message.get('c', [])
                    }
                    
                    self.data_buffer[ticker].append(trade_data)
                    
                    # Call callback if registered
                    if ticker in self.callbacks:
                        await self.callbacks[ticker](trade_data)
                        
        except Exception as e:
            logging.error(f"Error handling message: {e}")

    async def start_streaming(self):
        """Start the WebSocket streaming"""
        if not self.ws_client:
            await self.connect()
            
        try:
            while True:
                message = await self.ws_client.recv()
                await self.handle_message(json.loads(message))
        except Exception as e:
            logging.error(f"Error in streaming: {e}")
            raise

    def get_buffer_data(self, ticker: str) -> pd.DataFrame:
        """
        Get the buffered data for a ticker as a DataFrame.
        
        Args:
            ticker (str): The ticker symbol
            
        Returns:
            pd.DataFrame: DataFrame containing the buffered data
        """
        if ticker not in self.data_buffer:
            return pd.DataFrame()
            
        df = pd.DataFrame(self.data_buffer[ticker])
        if not df.empty:
            df.set_index('timestamp', inplace=True)
        return df

    async def close(self):
        """Close the WebSocket connection"""
        if self.ws_client:
            await self.ws_client.close()
            logging.info("Closed WebSocket connection") 