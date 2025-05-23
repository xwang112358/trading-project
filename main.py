# main.py

import logging
from config import POLYGON_API_KEY, DEFAULT_TICKER, DEFAULT_START_DATE, DEFAULT_END_DATE
from data_acquirer import fetch_stock_data
from data_processor import preprocess_data, save_data_to_csv, load_data_from_csv

# Configure basic logging for the main script
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

def run_pipeline(ticker: str, start_date: str, end_date: str, api_key: str):
    """
    Runs the data acquisition and processing pipeline for a given stock.
    """
    logging.info(f"--- Starting pipeline for {ticker} ---")

    # Step 1: Data Acquisition
    logging.info(f"Acquiring data for {ticker} from {start_date} to {end_date}...")
    raw_df = fetch_stock_data(api_key, ticker, start_date, end_date, adjusted=True)

    if raw_df is None or raw_df.empty:
        logging.error(f"Failed to acquire data for {ticker}. Pipeline terminated for this ticker.")
        return
    
    logging.info(f"Successfully acquired {len(raw_df)} rows of raw data for {ticker}.")
    logging.debug(f"Raw data head for {ticker}:\n{raw_df.head()}")

    # Step 2: Data Preprocessing
    logging.info(f"Preprocessing data for {ticker}...")
    processed_df = preprocess_data(raw_df, ticker=ticker)

    if processed_df is None or processed_df.empty:
        logging.error(f"Failed to preprocess data for {ticker}. Pipeline terminated for this ticker.")
        return

    logging.info(f"Successfully preprocessed data for {ticker}. Shape: {processed_df.shape}")
    logging.debug(f"Processed data head for {ticker}:\n{processed_df.head()}")

    # Step 3: Data Storage
    # Define a filename for the processed data
    # Make sure the 'data' subdirectory exists or is created by save_data_to_csv
    output_filename = f"{ticker}_daily_adjusted_processed.csv"
    logging.info(f"Saving processed data for {ticker} to {output_filename}...")
    
    if save_data_to_csv(processed_df, output_filename, directory="data"):
        logging.info(f"Successfully saved processed data for {ticker}.")

        # Optional: Demonstrate loading the data back
        logging.info(f"Attempting to load saved data for {ticker} for verification...")
        loaded_df = load_data_from_csv(output_filename, directory="data")
        if loaded_df is not None:
            logging.info(f"Successfully loaded data for {ticker} from CSV. Shape: {loaded_df.shape}")
            logging.debug(f"Loaded data head for {ticker}:\n{loaded_df.head()}")
            # You can add more checks here, e.g., pd.testing.assert_frame_equal(processed_df, loaded_df)
            # Note: Floating point precision and timezone handling can sometimes make exact equality tricky.
            # For timezone, ensure both are UTC or handle comparison carefully.
            if processed_df.shape == loaded_df.shape: # Basic check
                 logging.info(f"Shape of processed_df {processed_df.shape} matches loaded_df {loaded_df.shape}")
            else:
                 logging.warning(f"Shape mismatch! Processed: {processed_df.shape}, Loaded: {loaded_df.shape}")

        else:
            logging.warning(f"Could not load data back for {ticker} for verification.")
    else:
        logging.error(f"Failed to save processed data for {ticker}.")

    logging.info(f"--- Pipeline finished for {ticker} ---")


if __name__ == "__main__":
    # Check if the API key is set
    if POLYGON_API_KEY == "YOUR_POLYGON_API_KEY":
        logging.error("CRITICAL: POLYGON_API_KEY is not set in config.py. Please set your API key.")
        logging.error("The application will not run without a valid API key.")
    else:
        # You can run the pipeline for one or more tickers
        tickers_to_process = [DEFAULT_TICKER, "MSFT"] # Example: Apple and Microsoft
        
        # Use dates from config or define here
        start = DEFAULT_START_DATE
        # Ensure end_date is not in the future if Polygon API has restrictions for free tier
        end = DEFAULT_END_DATE 

        for t in tickers_to_process:
            run_pipeline(ticker=t, start_date=start, end_date=end, api_key=POLYGON_API_KEY)
            logging.info("\n") # Add a newline for better readability between tickers

        logging.info("All specified pipelines have completed.")

