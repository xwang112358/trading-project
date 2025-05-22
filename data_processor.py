# data_processor.py

import pandas as pd
import logging
import os

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def preprocess_data(df: pd.DataFrame, ticker: str = "UNKNOWN") -> pd.DataFrame | None:
    """
    Preprocesses the raw stock data DataFrame.

    Args:
        df (pd.DataFrame): Raw DataFrame with a datetime index and OHLCV columns.
        ticker (str): Ticker symbol for logging purposes.

    Returns:
        pd.DataFrame | None: Processed DataFrame or None if input is invalid.
    """
    if df is None or df.empty:
        logging.warning(f"Input DataFrame for {ticker} is None or empty. Skipping preprocessing.")
        return None

    logging.info(f"Starting preprocessing for {ticker} with {len(df)} rows.")
    processed_df = df.copy()

    # 1. Ensure timestamp index is a DatetimeIndex (should be already from acquirer)
    if not isinstance(processed_df.index, pd.DatetimeIndex):
        try:
            processed_df.index = pd.to_datetime(processed_df.index)
            logging.info(f"Converted index to DatetimeIndex for {ticker}.")
        except Exception as e:
            logging.error(f"Could not convert index to DatetimeIndex for {ticker}: {e}")
            return None
    
    # Ensure index is timezone-aware (UTC, as set by acquirer) or localize if needed
    if processed_df.index.tz is None:
        logging.warning(f"Index for {ticker} is timezone-naive. Assuming UTC based on Polygon.io source.")
        processed_df.index = processed_df.index.tz_localize('UTC') # Or another appropriate timezone
    elif processed_df.index.tz.tzname(None) != 'UTC': # Use tzname() method instead of zone attribute
        logging.info(f"Index for {ticker} is already timezone-aware: {processed_df.index.tz.tzname(None)}. Converting to UTC for consistency.")
        processed_df.index = processed_df.index.tz_convert('UTC')


    # 2. Handle Missing Values
    # Check for NaNs before filling
    nan_counts_before = processed_df.isnull().sum()
    if nan_counts_before.sum() > 0:
        logging.info(f"NaNs before filling for {ticker}:\n{nan_counts_before[nan_counts_before > 0]}")
        # Forward fill is a common strategy for time series price data
        processed_df.ffill(inplace=True)
        # Backward fill for any remaining NaNs at the beginning
        processed_df.bfill(inplace=True)
        nan_counts_after = processed_df.isnull().sum().sum()
        if nan_counts_after > 0:
            logging.warning(f"{nan_counts_after} NaNs remaining for {ticker} after ffill and bfill. Consider dropping or alternative imputation.")
            processed_df.dropna(inplace=True) # Drop rows if any NaNs persist
            logging.info(f"Dropped rows with remaining NaNs for {ticker}.")
        else:
            logging.info(f"NaNs filled for {ticker}.")
    else:
        logging.info(f"No NaNs found in initial data for {ticker}.")


    # 3. Ensure Correct Data Types (Polygon client usually handles this well for OHLCV)
    for col in ['open', 'high', 'low', 'close', 'volume']:
        if col in processed_df.columns:
            if not pd.api.types.is_numeric_dtype(processed_df[col]):
                try:
                    processed_df[col] = pd.to_numeric(processed_df[col])
                    logging.info(f"Converted column '{col}' to numeric for {ticker}.")
                except ValueError as e:
                    logging.error(f"Could not convert column '{col}' to numeric for {ticker}: {e}. Check data.")
                    # Decide how to handle: drop column, fill with 0, or raise error
                    processed_df[col] = 0 # Example: fill with 0, might not be ideal
                    logging.warning(f"Filled non-numeric values in '{col}' with 0 for {ticker}.")


    # 4. (Optional) Add some basic features, e.g., daily returns
    if 'close' in processed_df.columns:
        processed_df['daily_return'] = processed_df['close'].pct_change()
        # The first daily_return will be NaN, fill it with 0
        processed_df['daily_return'].fillna(0, inplace=True)
        logging.info(f"Calculated 'daily_return' for {ticker}.")

    # 5. (Optional) Outlier detection - very basic example
    # For a real system, this would be more sophisticated
    if 'daily_return' in processed_df.columns:
        # Example: Log if daily return is > 15% or < -15% (adjust threshold as needed)
        extreme_returns = processed_df[(processed_df['daily_return'] > 0.15) | (processed_df['daily_return'] < -0.15)]
        if not extreme_returns.empty:
            logging.warning(f"Potential outliers detected for {ticker} based on daily_return threshold (15%):")
            # logging.warning(extreme_returns[['close', 'daily_return']]) # This can be verbose

    logging.info(f"Preprocessing completed for {ticker}. Shape of processed data: {processed_df.shape}")
    return processed_df

def save_data_to_csv(df: pd.DataFrame, filename: str, directory: str = "data") -> bool:
    """
    Saves a DataFrame to a CSV file in the specified directory.
    Creates the directory if it doesn't exist.

    Args:
        df (pd.DataFrame): DataFrame to save.
        filename (str): Name of the CSV file (e.g., "AAPL_daily.csv").
        directory (str): Directory to save the file in.

    Returns:
        bool: True if successful, False otherwise.
    """
    if df is None or df.empty:
        logging.warning(f"DataFrame is None or empty. Cannot save to {filename}.")
        return False

    # Create directory if it doesn't exist
    if not os.path.exists(directory):
        try:
            os.makedirs(directory)
            logging.info(f"Created directory: {directory}")
        except OSError as e:
            logging.error(f"Error creating directory {directory}: {e}")
            return False
            
    filepath = os.path.join(directory, filename)
    try:
        df.to_csv(filepath)
        logging.info(f"Data successfully saved to {filepath}")
        return True
    except Exception as e:
        logging.error(f"Error saving data to {filepath}: {e}")
        return False

def load_data_from_csv(filename: str, directory: str = "data") -> pd.DataFrame | None:
    """
    Loads data from a CSV file from the specified directory.
    Assumes the first column is the index and parses it as dates.

    Args:
        filename (str): Name of the CSV file.
        directory (str): Directory where the file is located.

    Returns:
        pd.DataFrame | None: Loaded DataFrame or None if an error occurs.
    """
    filepath = os.path.join(directory, filename)
    if not os.path.exists(filepath):
        logging.warning(f"File not found: {filepath}")
        return None
    try:
        # Assuming the first column is the timestamp index
        df = pd.read_csv(filepath, index_col=0, parse_dates=True)
        # Ensure loaded index is UTC if it was saved as UTC
        if df.index.tz is None: # If read_csv doesn't preserve tz, re-localize
             df.index = df.index.tz_localize('UTC')
        elif df.index.tz.tzname(None) != 'UTC':
            df.index = df.index.tz_convert('UTC')

        logging.info(f"Data successfully loaded from {filepath}")
        return df
    except Exception as e:
        logging.error(f"Error loading data from {filepath}: {e}")
        return None

if __name__ == '__main__':
    # This is for testing the data_processor.py module directly
    print("Testing data_processor.py...")
    # Create a dummy DataFrame similar to what data_acquirer might produce
    sample_data = {
        'timestamp': pd.to_datetime(['2023-01-01 05:00:00+00:00', '2023-01-02 05:00:00+00:00', '2023-01-03 05:00:00+00:00', '2023-01-04 05:00:00+00:00']),
        'open': [150.0, 151.0, None, 153.0], # Added a None for NaN testing
        'high': [152.0, 152.5, 152.0, 154.0],
        'low': [149.0, 150.5, 150.0, 152.0],
        'close': [151.5, 151.0, 151.5, 153.5],
        'volume': [100000, 120000, 110000, 130000.0] # Added float for type testing
    }
    dummy_df = pd.DataFrame(sample_data).set_index('timestamp')
    
    print("\nOriginal dummy data:")
    print(dummy_df)
    print(f"NaNs in dummy: \n{dummy_df.isnull().sum()}")

    processed_dummy_df = preprocess_data(dummy_df.copy(), ticker="DUMMY") # Pass copy to avoid modifying original
    
    if processed_dummy_df is not None:
        print("\nProcessed dummy data:")
        print(processed_dummy_df.head())
        print(f"\nNaNs after processing: {processed_dummy_df.isnull().sum().sum()}")
        print(f"\nData types:\n{processed_dummy_df.dtypes}")

        # Test saving and loading
        test_filename = "DUMMY_processed.csv"
        if save_data_to_csv(processed_dummy_df, test_filename):
            loaded_df = load_data_from_csv(test_filename)
            if loaded_df is not None:
                print(f"\nData loaded from {test_filename}:")
                print(loaded_df.head())
                # Clean up test file
                try:
                    os.remove(os.path.join("data", test_filename))
                    logging.info(f"Cleaned up test file: data/{test_filename}")
                except OSError as e:
                    logging.warning(f"Could not remove test file data/{test_filename}: {e}")
    else:
        print("\nPreprocessing failed for dummy data.")

