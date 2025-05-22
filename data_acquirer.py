# data_acquirer.py

import pandas as pd
from polygon import RESTClient
from polygon.exceptions import BadResponse
import logging
from datetime import datetime

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_stock_data(api_key: str, ticker: str, start_date: str, end_date: str,
                       multiplier: int = 1, timespan: str = "day", adjusted: bool = True) -> pd.DataFrame | None:
    """
    Fetches historical stock data (OHLCV) from Polygon.io.

    Args:
        api_key (str): Your Polygon.io API key.
        ticker (str): The stock ticker symbol (e.g., "AAPL").
        start_date (str): The start date for the data in "YYYY-MM-DD" format.
        end_date (str): The end date for the data in "YYYY-MM-DD" format.
        multiplier (int): The size of the timespan multiplier.
        timespan (str): The size of the time window (e.g., "day", "hour", "minute").
        adjusted (bool): Whether to return adjusted (for splits/dividends) data.

    Returns:
        pd.DataFrame | None: A Pandas DataFrame with OHLCV data, or None if an error occurs.
                             The DataFrame will have a 'timestamp' (datetime) index and columns:
                             ['open', 'high', 'low', 'close', 'volume', 'vwap', 'transactions'].
    """
    logging.info(f"Attempting to fetch data for {ticker} from {start_date} to {end_date} (Adjusted: {adjusted}).")

    try:
        client = RESTClient(api_key)
        
        # Polygon's list_aggs can fetch a wide range of data.
        # For very long date ranges, it might return a generator that needs iteration.
        # The client library handles pagination for list_aggs if you iterate over it.
        aggs_iterator = client.list_aggs(
            ticker=ticker,
            multiplier=multiplier,
            timespan=timespan,
            from_=start_date,
            to=end_date,
            adjusted=adjusted,
            sort="asc",  # Ensure data is sorted chronologically
            limit=50000  # Max limit per request; the client handles pagination if more data exists
        )
        
        aggs_list = list(aggs_iterator) # Convert iterator to list to check if empty

        if not aggs_list:
            logging.warning(f"No data found for {ticker} from {start_date} to {end_date}.")
            return None

        # Convert the list of Aggregate objects to a Pandas DataFrame
        df = pd.DataFrame([{
            'timestamp': pd.to_datetime(agg.timestamp, unit='ms', utc=True), # Polygon timestamps are UNIX ms (UTC)
            'open': agg.open,
            'high': agg.high,
            'low': agg.low,
            'close': agg.close,
            'volume': agg.volume,
            'vwap': getattr(agg, 'vwap', None), # vwap might not always be present
            'transactions': getattr(agg, 'transactions', None) # transactions might not always be present
        } for agg in aggs_list])

        if df.empty:
            logging.warning(f"DataFrame is empty after processing aggregates for {ticker}.")
            return None

        # Set timestamp as index
        df = df.set_index('timestamp')
        
        logging.info(f"Successfully fetched {len(df)} data points for {ticker}.")
        return df

    except BadResponse as e:
        logging.error(f"Polygon API BadResponse for {ticker}: {e}. Check your API key and request parameters.")
        logging.error(f"Response status: {e.status}, Response text: {e.response.text if e.response else 'N/A'}")
        return None
    except ConnectionError as e:
        logging.error(f"Connection error while fetching data for {ticker}: {e}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred while fetching data for {ticker}: {e}", exc_info=True)
        return None

if __name__ == '__main__':
    # This is for testing the data_acquirer.py module directly
    # You'll need to have your config.py in the same directory or provide the key directly
    try:
        from config import POLYGON_API_KEY, DEFAULT_TICKER, DEFAULT_START_DATE, DEFAULT_END_DATE
        if POLYGON_API_KEY == "YOUR_POLYGON_API_KEY":
            print("Please set your POLYGON_API_KEY in config.py before running this test.")
        else:
            print(f"Testing data fetch for {DEFAULT_TICKER}...")
            test_df = fetch_stock_data(POLYGON_API_KEY, DEFAULT_TICKER, DEFAULT_START_DATE, DEFAULT_END_DATE)
            if test_df is not None:
                print("Sample data fetched:")
                print(test_df.head())
                print(f"\nData types:\n{test_df.dtypes}")
                print(f"\nIndex type: {type(test_df.index)}")
            else:
                print(f"Failed to fetch data for {DEFAULT_TICKER}.")
    except ImportError:
        print("config.py not found or POLYGON_API_KEY not defined. Create config.py with your API key.")
    except Exception as e:
        print(f"An error occurred during testing: {e}")
