import pandas as pd
import logging
from tqdm import tqdm
import os

# Set up logging
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')

from scrape_tesco_v1 import run_tesco_scraper
from probe_openfoodfacts_v1 import run_openfoodfacts_query

# File processing size
chunk_size = 100

# Define the consistent set of columns
TESCO_COLUMNS = ['name', 'matched_name', 'barcode', 'brand', 'category_1', 'category_2',
                 'category_3', 'category_4', 'rating', 'match_score']
OFF_COLUMNS = ['barcode', 'OFF_data',
               'OFF_additives_n', 'OFF_additives_tags', 'OFF_allergens', 'OFF_brands',
               'OFF_categories', 'OFF_ecoscore_grade', 'OFF_ingredients_n',
               'OFF_ingredients_text', 'OFF_labels', 'OFF_labels_hierarchy',
               'OFF_nutrition_grades', 'OFF_nova_group', 'OFF_nutriments',
               'OFF_packaging_recycling_tags', 'OFF_packaging_tags', 'OFF_stores_tags']

def ensure_consistent_columns(df, columns):
    # Ensure the DataFrame has all the required columns
    for column in columns:
        if column not in df.columns:
            df[column] = None
    return df[columns]

def read_input_data_chunks(input_file, chunk_size):
    # Read input data from CSV file in chunks
    return pd.read_csv(input_file, chunksize=chunk_size)

def save_data(data, output_file, columns):
    # Save processed data to CSV file
    df = pd.DataFrame(data)
    df = ensure_consistent_columns(df, columns)
    df.to_csv(output_file, mode='a', header=not os.path.exists(output_file), index=False)

def Tesco_filter_processed_products(input_chunk, processed_names):
    # Filter out already processed items for Tesco scraping
    return input_chunk[~input_chunk['name'].isin(processed_names)]

def Tesco_get_processed_names(output_file):
    # Get a set of already processed product names from the output file
    try:
        df = pd.read_csv(output_file)
        return set(df['name'])
    except FileNotFoundError:
        return set()

def Tesco_run_flow(input_file, output_file):
    # Run the Tesco scraping flow
    processed_names = Tesco_get_processed_names(output_file)
    
    total_rows = sum(1 for _ in pd.read_csv(input_file, chunksize=1))
    
    with tqdm(total=total_rows, desc="Tesco Progress", unit="item") as pbar:
        for chunk in read_input_data_chunks(input_file, chunk_size):
            filtered_chunk = Tesco_filter_processed_products(chunk, processed_names)
            
            if not filtered_chunk.empty:
                product_names = filtered_chunk['name'].tolist()
                results = run_tesco_scraper(product_names)
                save_data(results, output_file, TESCO_COLUMNS)
                processed_names.update([r['name'] for r in results])
            
            pbar.update(len(chunk))

def OFF_filter_processed_barcodes(input_df, processed_barcodes):
    # Filter out already processed barcodes for OFF querying
    return input_df[~input_df['barcode'].isin(processed_barcodes) & (input_df['barcode'] != 'unfound')]

def OFF_get_processed_barcodes(output_file):
    # Get a set of already processed barcodes from the OFF output file
    try:
        df = pd.read_csv(output_file)
        return set(df['barcode'])
    except FileNotFoundError:
        return set()

def OFF_run_flow(input_file, output_file):
    # Run the OpenFoodFacts processing flow
    processed_barcodes = OFF_get_processed_barcodes(output_file)
    
    total_rows = sum(1 for _ in pd.read_csv(input_file, chunksize=1))
    
    with tqdm(total=total_rows, desc="OFF Progress", unit="item") as pbar:
        for chunk in read_input_data_chunks(input_file, chunk_size):
            filtered_chunk = OFF_filter_processed_barcodes(chunk, processed_barcodes)
            
            if not filtered_chunk.empty:
                barcodes_to_process = filtered_chunk['barcode'].tolist()
                results = run_openfoodfacts_query(barcodes_to_process)
                save_data(results, output_file, OFF_COLUMNS)
                processed_barcodes.update([r['barcode'] for r in results])
            
            pbar.update(len(chunk))

def run_supermarket_flow(input_file, tesco_output_file, off_output_file):
    # Main function to run the entire supermarket flow
    logging.info("Starting Tesco scraper...")
    Tesco_run_flow(input_file, tesco_output_file)
    logging.info("Tesco scraper completed.")

    logging.info("Starting OpenFoodFacts processing...")
    OFF_run_flow(tesco_output_file, off_output_file)
    logging.info("OpenFoodFacts processing completed.")

if __name__ == "__main__":
    input_file = r"C:\Users\bened\OneDrive\Documents\Businesses\spend-analytics\Tesco Clubcards\4 - Data Preparations\all_items.csv"
    tesco_output_file = r"C:\Users\bened\OneDrive\Documents\Businesses\spend-analytics\Tesco Clubcards\5 - Processed Data Files\5) Gathered Data\tesco_output.csv"
    off_output_file = r"C:\Users\bened\OneDrive\Documents\Businesses\spend-analytics\Tesco Clubcards\5 - Processed Data Files\5) Gathered Data\off_output.csv"

    run_supermarket_flow(input_file, tesco_output_file, off_output_file)


## Yaml file for all inputs
## Change method for finding which rows to scan, so it will re-try some of them after X weeks/days whatever
## Fix TQDM to be nicer, or get rid of it
## Add ID number to files so we aren't matching on item name when mapping files