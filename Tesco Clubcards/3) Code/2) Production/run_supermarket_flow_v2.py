import pandas as pd
import logging
from tqdm import tqdm
import os
import time

# Import processor functions
from categorise_products_v1 import run_item_categoriser, validate_responses
from scrape_tesco_v1 import run_tesco_scraper
from probe_openfoodfacts_v1 import run_openfoodfacts_query

######################
# User definitons
######################

# Batch size for processing before save to file
batch_size = 100

# File paths
all_item_input_file = r"G:\My Drive\Wantrepreneurialism\Active\spend-analytics\Tesco Clubcards\2) Data\2) Data Preparations\all_items.xlsx"  # All items file (Excel with 'UID' and 'product name')

# Categorisation output files
categorisation_raw_output_file = r"G:\My Drive\Wantrepreneurialism\Active\spend-analytics\Tesco Clubcards\2) Data\3) Outputs\All items\raw_responses.csv"  # Raw categorisation responses
categorisation_valid_output_file = r"G:\My Drive\Wantrepreneurialism\Active\spend-analytics\Tesco Clubcards\2) Data\3) Outputs\All items\categorised_products.csv"  # Valid categorisation outputs
categorisation_invalid_output_file = r"G:\My Drive\Wantrepreneurialism\Active\spend-analytics\Tesco Clubcards\2) Data\3) Outputs\All items\invalid_results.csv"  # Invalid categorisation outputs

# Tesco and OFF output files
tesco_output_file = r"G:\My Drive\Wantrepreneurialism\Active\spend-analytics\Tesco Clubcards\2) Data\2) Data Preparations\2) Outputs\tesco_output.csv"  # Tesco output CSV
off_output_file = r"G:\My Drive\Wantrepreneurialism\Active\spend-analytics\Tesco Clubcards\2) Data\2) Data Preparations\\2) Outputs\off_output.csv"  # OFF output CSV

# Define columns for saving data
# Tesco scrape columns
TESCO_COLUMNS = ['UID', 'name', 'matched_name', 'barcode', 'brand', 'category_1', 'category_2',
                 'category_3', 'category_4', 'rating', 'match_score']
# OFF output columns
OFF_COLUMNS = [
    'UID', 'barcode', 'OFF_data',
    'OFF_additives_n', 'OFF_additives_tags', 'OFF_allergens', 'OFF_brands',
    'OFF_categories', 'OFF_ecoscore_grade', 'OFF_ingredients_n', 'OFF_ingredients_text',
    'OFF_labels', 'OFF_labels_hierarchy', 'OFF_nutrition_grades', 'OFF_nova_group',
    'carbohydrates', 'carbohydrates_100g', 'carbohydrates_unit', 'carbohydrates_value',
    'energy', 'energy-kcal', 'energy-kcal_100g', 'energy-kcal_unit', 'energy-kcal_value', 'energy-kcal_value_computed',
    'energy-kj', 'energy-kj_100g', 'energy-kj_unit', 'energy-kj_value', 'energy-kj_value_computed',
    'energy_100g', 'energy_unit', 'energy_value',
    'fat', 'fat_100g', 'fat_unit', 'fat_value',
    'fiber', 'fiber_100g', 'fiber_unit', 'fiber_value',
    'proteins', 'proteins_100g', 'proteins_unit', 'proteins_value',
    'salt', 'salt_100g', 'salt_unit', 'salt_value',
    'saturated-fat', 'saturated-fat_100g', 'saturated-fat_unit', 'saturated-fat_value',
    'sodium', 'sodium_100g', 'sodium_unit', 'sodium_value',
    'sugars', 'sugars_100g', 'sugars_unit', 'sugars_value',
    'OFF_packaging_recycling_tags', 'OFF_packaging_tags', 'OFF_stores_tags'
]
# Categorisation output columns
raw_categorised_output_cols = ['UID', 'product name', 'output']
valid_categorised_output_cols = ["UID", "category_3", "attribute type", "attribute value", "value"]
invalid_categorised_output_cols = ["UID", "product name", "reason"]

######################
# Logging setup
######################

# Custom logging handler that uses tqdm.write() to print messages above the progress bar
class TqdmLoggingHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.write(msg)  # This prints the log message without disrupting the progress bar
            self.flush()
        except Exception:
            self.handleError(record)

# Remove default handlers and set up the custom one
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
logger.handlers.clear()  # Remove any existing handlers
logger.addHandler(TqdmLoggingHandler())

######################
# Helper functions
######################

# Function to ensure the DataFrame has all required columns; adds missing columns with None values
def ensure_consistent_columns(df, columns):
    for column in columns:
        if column not in df.columns:
            df[column] = None  # Add missing column with None values
    return df[columns]

# Function to save data to a CSV file, retrying if the file is locked
def save_data(data, output_file, columns, sleep_interval=10):
    df = pd.DataFrame(data)
    df = ensure_consistent_columns(df, columns)
    while True:
        try:
            df.to_csv(output_file, mode='a', header=not os.path.exists(output_file), index=False)
            break  # Exit loop if saving succeeds
        except PermissionError:
            logging.critical(f"File '{output_file}' is currently locked. Retrying in {sleep_interval} second(s)...")
            time.sleep(sleep_interval)

# Function to retrieve a set of processed keys from an output CSV file; returns empty set if file not found
def get_processed_entries(output_file, key_column):
    try:
        df = pd.read_csv(output_file)
        return set(df[key_column])
    except FileNotFoundError:
        return set()

# Function to filter out rows in a DataFrame where the value in a specified column is in a given processed set
def filter_dataframe(df, column, processed_set):
    return df[~df[column].isin(processed_set)]

######################
# Main functions
######################

# Function to process a batch of items for categorisation: filters out already processed items, processes the batch, and saves outputs
def categorisation_run_flow(df, raw_output_file, valid_output_file, invalid_output_file):
    if df.empty:
        logging.critical("No new items to categorise in this batch.")
        return
    logging.critical(f"Categorising {len(df)} new items in current batch.")
    import asyncio
    results = asyncio.run(run_item_categoriser(df))  # Run categorisation asynchronously
    save_data(results, raw_output_file, raw_categorised_output_cols)  # Save raw responses
    valid_entries, invalid_entries = validate_responses(results)  # Validate responses
    if valid_entries:
        save_data(valid_entries, valid_output_file, valid_categorised_output_cols)
    if invalid_entries:
        save_data(invalid_entries, invalid_output_file, invalid_categorised_output_cols)

# Function to process a batch of items for Tesco processing: filters out already processed items and saves the results
def tesco_run_flow(df, output_file):
    if df.empty:
        logging.critical("No new Tesco items to process in this batch.")
        return
    # Reset index to ensure the ordering is consistent
    df = df.reset_index(drop=True)
    product_names = df['product name'].tolist()
    # Get the corresponding UIDs in order
    uids = df['UID'].tolist()
    results = run_tesco_scraper(product_names)
    # Attach UID from the same order as the input DataFrame
    for i, res in enumerate(results):
        res['UID'] = uids[i]
    save_data(results, output_file, TESCO_COLUMNS)

# Function to process a batch of items for OFF processing: filters out already processed items and saves the results
def off_run_flow(df, output_file):
    if df.empty:
        logging.critical("No new OFF items to process in this batch.")
        return
    # Reset index to preserve ordering
    df = df.reset_index(drop=True)
    barcodes = df['barcode'].tolist()
    uids = df['UID'].tolist()
    results = run_openfoodfacts_query(barcodes)
    # Reattach UID and flatten the OFF_nutriments dictionary without extra prefix
    for i, res in enumerate(results):
        res['UID'] = uids[i]
        if isinstance(res.get('OFF_nutriments'), dict):
            nutriments = res.pop('OFF_nutriments')
            for sub_key, sub_value in nutriments.items():
                res[sub_key] = sub_value
    save_data(results, output_file, OFF_COLUMNS)

######################
# Batch & run function
######################

# Function to run the entire supermarket flow in batches, sequentially processing categorisation, Tesco, then OFF stages
def run_supermarket_flow_batch(batch_size):
    # Categorisation Stage: process the entire input Excel file in batches
    # Find new items to categorise
    df_all = pd.read_excel(all_item_input_file)  # Read all items from Excel
    processed_UIDs = get_processed_entries(categorisation_valid_output_file, 'UID')  # Get already processed UIDs
    df_all = df_all[~df_all['UID'].isin(processed_UIDs)]  # Filter out processed items
    logging.critical(f"\n\nPRODUCT CATEGORISATION: {len(df_all)} new items to categorise.")
    # Begin processing in batches
    total_batches = (len(df_all) + batch_size - 1) // batch_size
    overall_pbar = tqdm(total=total_batches, desc="Product categorisation progress", unit="batch")
    for i in range(0, len(df_all), batch_size):
        batch_df = df_all.iloc[i:i+batch_size]  # Get current batch slice
        logging.info(f"Processing categorisation batch {i//batch_size + 1}/{total_batches}")
        categorisation_run_flow(batch_df, categorisation_raw_output_file, categorisation_valid_output_file, categorisation_invalid_output_file)
        overall_pbar.update(1)
    overall_pbar.close()
    
    # Tesco Stage: process the original input file in batches for Tesco scraping
    # Find new items to categorise
    df_all = pd.read_excel(all_item_input_file)  # Re-read all items to preserve original order
    processed_UIDs = get_processed_entries(categorisation_valid_output_file, 'UID')  # Get already processed UIDs
    df_all = df_all[~df_all['UID'].isin(processed_UIDs)]  # Filter out processed items
    logging.critical(f"\n\nTESCO SCRAPE: {len(df_all)} new items to scrape.")
    # Begin processing in batches
    total_batches = (len(df_all) + batch_size - 1) // batch_size
    overall_pbar = tqdm(total=total_batches, desc="Tesco scraping progress", unit="batch")
    for i in range(0, len(df_all), batch_size):
        batch_df = df_all.iloc[i:i+batch_size]
        logging.info(f"Processing Tesco batch {i//batch_size + 1}/{total_batches}")
        tesco_run_flow(batch_df, tesco_output_file)
        overall_pbar.update(1)
    overall_pbar.close()
    
    # OFF Stage: process the Tesco output CSV in batches for OFF processing
    # Find new items to categorise
    df_all = pd.read_csv(tesco_output_file)  # Read Tesco output 
    processed_UIDs = get_processed_entries(off_output_file, 'UID')
    df_all = df_all[~df_all['UID'].isin(processed_UIDs) & (df_all['barcode'] != 'unfound')] # Filter out rows with barcodes already processed and where barcode is not 'unfound'
    logging.critical(f"\n\nOFF PROBE: {len(df_all)} new items to probe.")
    # Begin processing in batches
    total_batches = (len(df_all) + batch_size - 1) // batch_size
    overall_pbar = tqdm(total=total_batches, desc="OpenFoodFacts probing progress", unit="batch")
    for i in range(0, len(df_all), batch_size):
        batch_df = df_all.iloc[i:i+batch_size]
        logging.info(f"Processing OFF batch {i//batch_size + 1}/{total_batches}")
        off_run_flow(batch_df, off_output_file)
        overall_pbar.update(1)
    overall_pbar.close()

if __name__ == "__main__":
    run_supermarket_flow_batch(batch_size)
