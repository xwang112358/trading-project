# config.py

import os

# Get API key from environment variable
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY")
if not POLYGON_API_KEY:
    raise ValueError("POLYGON_API_KEY environment variable not set. Please set it in your environment.")

# --- Optional: Default settings for data fetching ---
DEFAULT_TICKER = "AAPL"
DEFAULT_START_DATE = "2023-01-01"
DEFAULT_END_DATE = "2024-01-01"  # Use a recent but not too distant end date
DEFAULT_TIMESPAN = "day"
DEFAULT_MULTIPLIER = 1
