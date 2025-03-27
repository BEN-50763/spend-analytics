import os
import json
import pandas as pd
import zipfile
from datetime import datetime
import csv

#############
# Unzipping functions
#############

# Function to unzip new or modified files
def unzip_new_files(files_to_unzip, userdata_unzipped_root_folder):

    newly_unzipped_list = []        # keeps track of folders unzipped this run
    # Loop through all files in the folder
    for zip_file_path in files_to_unzip:
        # Get only the name
        file_name = zip_file_path.split("\\")[-1]
            
        # Build the destination folder
        destination_folder = os.path.join(userdata_unzipped_root_folder, os.path.splitext(file_name)[0])
        newly_unzipped_list.append(destination_folder)
        os.makedirs(destination_folder, exist_ok=True)
        
        # Actually unzip the file
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(destination_folder)

    # Return the list of new folders
    return newly_unzipped_list

#############
# Data processing functions
#############

# Function to assign or generate UIDs for new products
def grab_UIDs(df_fact_transactions, df_all):
    """
    Merges fact transactions with master item list (df_all) to ensure each record 
    has a unique UID. If an item isn't found in df_all, it is added with a new UID.
    """
    # Create a lower-case, stripped join key for case-insensitive matching
    df_fact_transactions['key'] = df_fact_transactions['name'].str.lower().str.strip()
    df_all['key'] = df_all['product name'].str.lower().str.strip()

    # Perform a left join to assign UIDs to each fact transaction
    df_join = pd.merge(df_fact_transactions, df_all[['UID', 'key']], on='key', how='left')

    # Identify the keys that did not get a UID (i.e., missing matches)
    missing_keys = df_join.loc[df_join['UID'].isna(), 'key'].unique()

    if missing_keys.size:
        # For missing keys, get the original product name (preserving capitalization)
        new_products = (
            df_fact_transactions[df_fact_transactions['key'].isin(missing_keys)]
            .drop_duplicates('key')[['name', 'key']]
        )
        
        # Determine the current maximum UID number in df_all; default to 0 if empty
        max_id = (
            df_all['UID'].str.extract(r'ID_(\d+)')[0].astype(int).max() 
            if not df_all.empty else 0
        )
        
        # Generate new UIDs for the missing keys
        new_products['UID'] = [
            'ID_' + str(i) for i in range(max_id + 1, max_id + 1 + len(new_products))
        ]
        
        # Rename 'name' to 'product name' to match df_all structure
        new_products = new_products.rename(columns={'name': 'product name'})
        
        # Append the new product rows (with UID and key) to df_all
        df_all = pd.concat([df_all, new_products[['UID', 'product name', 'key']]], ignore_index=True)

    # Re-join so every fact transaction now has a valid UID
    df_fact_transactions = pd.merge(
        df_fact_transactions, df_all[['UID', 'key']], 
        on='key', how='left'
    ).drop(columns='key')
    
    # Drop the temporary 'key' column from df_all
    df_all = df_all.drop(columns='key')

    return df_fact_transactions, df_all

#############
# Data extraction
#############

# Function to extract data from newly unzipped folders
def extract_data(newly_unzipped_list, userdata_output_root_folder, df_all, all_items_input_file):
    """
    Iterates through each folder in newly_unzipped_list, finds a JSON file, 
    extracts the data into Fact (transactions) and Dim (basket info) DataFrames, 
    ensures UIDs are assigned, and saves to CSV in userdata_output_root_folder.
    """
    for unzipped_folder in newly_unzipped_list:
        # Grab any .json file inside this folder
        json_files = [f for f in os.listdir(unzipped_folder) if f.endswith('.json')]
        if not json_files:
            # No .json found, so skip
            continue

        unzipped_file = json_files[0]

        # Load the JSON data
        with open(os.path.join(unzipped_folder, unzipped_file), "r") as f:
            data = json.load(f)

        # Collect relevant bit of folder name for our output 
        destination_folder_name = unzipped_folder.split("\\")[-1]

        # Top-level identifiers
        customerId = data.get('customerId')
        requestId = data.get('requestId')

        # Lists to hold our records
        fact_transactions = []
        dim_basket = []

        # Process each purchase in the 'purchases' list
        purchases = data.get('purchases', [])
        for purchase in purchases:
            # Basket-level (DIM Basket) information
            timestamp = purchase.get('timestamp')
            purchase_type = purchase.get('type')
            says = purchase.get('says')
            basketValueGross = purchase.get('basketValueGross')
            overallBasketSavings = purchase.get('overallBasketSavings')
            basketValueNet = purchase.get('basketValueNet')
            numberOfItems = purchase.get('numberOfItems')

            # Payment info (assume first entry in 'payment' if it exists)
            payment = purchase.get('payment', [])
            if payment:
                payment_record = payment[0]
                payment_type = payment_record.get('type')
                payment_category = payment_record.get('category')
                payment_amount = payment_record.get('amount')
            else:
                payment_type = None
                payment_category = None
                payment_amount = None

            # Create a DIM Basket record
            basket_record = {
                'timestamp': timestamp,
                'type': purchase_type,
                'says': says,
                'basketValueGross': basketValueGross,
                'overallBasketSavings': overallBasketSavings,
                'basketValueNet': basketValueNet,
                'numberOfItems': numberOfItems,
                'payment_type': payment_type,
                'payment_category': payment_category,
                'payment_amount': payment_amount,
                'customerId': customerId,
                'requestId': requestId,
            }
            dim_basket.append(basket_record)
            
            # Process each item in the 'items' for Fact Transactions
            for item in purchase.get('items', []):
                fact_record = {
                    'name': item.get('name'),
                    'quantity': item.get('quantity'),
                    'price': item.get('price'),
                    'volume': item.get('volume'),
                    'timestamp': timestamp  # from basket level
                }
                fact_transactions.append(fact_record)

        # Convert lists to DataFrames
        df_fact_transactions = pd.DataFrame(fact_transactions)
        df_dim_basket = pd.DataFrame(dim_basket)

        # Collect UIDs or extend master list if new products
        df_fact_transactions, df_all = grab_UIDs(df_fact_transactions, df_all)

        # Construct a final data output path for CSV
        data_output_path = os.path.join(userdata_output_root_folder, destination_folder_name)
        data_output_path = data_output_path.replace(".json", "")
        os.makedirs(data_output_path, exist_ok=True)

        # Save to CSV
        df_fact_transactions.to_csv(os.path.join(data_output_path, "FACT_transactions.csv"), index=False, quoting=csv.QUOTE_ALL)
        df_dim_basket.to_csv(os.path.join(data_output_path, "DIM_basket.csv"), index=False, quoting=csv.QUOTE_ALL)

    return df_all

#############
# Orchestration function
#############

# Function to run the entire data-opener process
def run_data_opener(files_to_unzip, userdata_output_root_folder, userdata_unzipped_root_folder, df_all, all_items_input_file):

    # Unzip any new or updated files
    newly_unzipped_list = unzip_new_files(files_to_unzip, userdata_unzipped_root_folder)

    # Extract & process newly unzipped data
    if newly_unzipped_list:
        df_all = extract_data(newly_unzipped_list, userdata_output_root_folder, df_all, all_items_input_file)

    # Return the updated df_all and final time of
    return df_all, datetime.now().isoformat()
