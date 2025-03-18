from tqdm import tqdm
import pandas as pd
import json
from openai import AsyncOpenAI
from datetime import datetime
import asyncio

#############
# Variable declarations and environment variables (all lower case)
#############
batch_size = 20  # Number of items per GPT API batch
gpt_api_key = "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"
client = AsyncOpenAI(api_key=gpt_api_key)
gpt_model = "ft:gpt-4o-mini-2024-07-18:personal::B9eg9Tmn"

# File path for validation categories (update as needed)
validation_categories_file_path = r"G:\My Drive\Wantrepreneurialism\Active\spend-analytics\Tesco Clubcards\2) Data\2) Data Preparations\Categories.xlsx"

#############
# Load valid categories from the Excel file
#############
valid_df = pd.read_excel(validation_categories_file_path, sheet_name="Category")
valid_l3 = set(valid_df["Level 3"].dropna().astype(str).unique())

valid_characteristics_df = pd.read_excel(validation_categories_file_path, sheet_name="Characteristic")
valid_characteristics = set(valid_characteristics_df["Characteristic"].dropna().astype(str).unique())

valid_flavours_df = pd.read_excel(validation_categories_file_path, sheet_name="Flavour")
valid_flavours = set(valid_flavours_df["Flavour"].dropna().astype(str).unique())

#############
# Fixed system instruction and required keys
#############
system_message = (
    "Categorise this item strictly using only the taxonomy from your training data. "
    "Select the closest Level 3 category (most granular) and assign the corresponding fixed Level 2 (mid-level) and Level 1 (broad) categories. "
    "Include the best matching characteristics and flavours. "
    "Do not assign anything not present in your training data. "
    "Output must match the JSON structure from the training dataset."
)
required_keys = ["Level 3", "Level 2", "Level 1", "characteristics", "flavours"]

#############
# Helper function: prompt_gpt_batch
#############
async def prompt_gpt_batch(UIDs, item_names):
    """
    Processes a batch of product names using GPT API calls asynchronously.
    Returns a list of tuples (UID, product name, gpt_output).
    """
    tasks = []
    for UID, item in zip(UIDs, item_names):
        messages = [
            {"role": "system", "content": system_message},
            {"role": "user", "content": item}
        ]
        tasks.append(client.chat.completions.create(
            model=gpt_model,
            messages=messages,
            max_tokens=100,
            temperature=0
        ))
    responses = await asyncio.gather(*tasks)
    outputs = [r.choices[0].message.content for r in responses]
    return list(zip(UIDs, item_names, outputs))

#############
# Main function: run_item_categoriser
#############
async def run_item_categoriser(df_items):
    """
    Processes the input DataFrame of items in batches.
    Expects df_items to have columns 'UID' and 'product name'.
    Returns a list of processed results.
    """
    results = []
    num_items = len(df_items)
    # Loop over all items in increments of batch_size using tqdm for progress display
    for i in tqdm(range(0, num_items, batch_size), desc=f"Prompting GPT over {num_items} products, batches of {batch_size}"):
        batch = df_items.iloc[i:i + batch_size]
        UIDs = batch["UID"].tolist()
        item_names = batch["product name"].tolist()
        try:
            batch_results = await prompt_gpt_batch(UIDs, item_names)
        except Exception as e:
            # On error, mark each item as "Error"
            batch_results = [(UID, item, "Error") for UID, item in zip(UIDs, item_names)]
        results.extend(batch_results)
    return [{"UID": UID, "product name": product_name, "output": output} for UID, product_name, output in results]

#############
# Function: validate_responses
#############
def validate_responses(results):
    """
    Validates the GPT responses.
    Each result is a tuple (UID, product name, response).
    Returns two lists: valid_entries and invalid_entries.
    """
    valid_entries = []
    invalid_entries = []
    
    for result in results:
        UID = result["UID"]
        product_name = result["product name"]
        response = result["output"]
        try:
            data = json.loads(response)
            if not isinstance(data, dict):
                raise ValueError("Response is not a dictionary")
        except Exception:
            invalid_entries.append({"UID": UID, "product name": product_name, "reason": "Invalid JSON"})
            continue
        
        parsed_data = {key: data.get(key, []) if isinstance(data.get(key), (str, list)) else [] for key in required_keys}
        l3_value = parsed_data["Level 3"]
        
        if l3_value not in valid_l3:
            invalid_entries.append({"UID": UID, "product name": product_name, "reason": f"Invalid L3 Category: {l3_value}"})
            continue
        
        # Validate characteristics and flavours
        for attr_type, valid_set in [("characteristics", valid_characteristics), ("flavours", valid_flavours)]:
            for attr in parsed_data.get(attr_type, []):
                if attr in valid_set:
                    valid_entries.append({"UID": UID, "Level 3": l3_value, "attribute type": attr_type.capitalize(), "attribute value": attr, "value": 1})
                else:
                    invalid_entries.append({"UID": UID, "product name": product_name, "reason": f"Invalid {attr_type.capitalize()}: {attr}"})
    return valid_entries, invalid_entries
