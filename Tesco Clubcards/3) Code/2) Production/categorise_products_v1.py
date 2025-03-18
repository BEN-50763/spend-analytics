from tqdm import tqdm
import pandas as pd
import json
from openai import AsyncOpenAI
import asyncio
from typing import List, Literal
from pydantic import BaseModel, Field
from typing import List

#############
# Variable declarations and environment variables (all lower case)
#############
batch_size = 20  # Number of items per GPT API batch
gpt_api_key = "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"
client = AsyncOpenAI(api_key=gpt_api_key)
gpt_model = "ft:gpt-4o-mini-2024-07-18:personal::BCOsoAm5"

# File path for validation categories (update as needed)
validation_categories_file_path = r"G:\My Drive\Wantrepreneurialism\Active\spend-analytics\Tesco Clubcards\2) Data\2) Data Preparations\Categories.xlsx"

#############
# Load valid categories from the Excel file
#############
valid_df = pd.read_excel(validation_categories_file_path, sheet_name="Category")
valid_cat3 = set(valid_df["Level 3"].dropna().astype(str).unique())
valid_cat1 = set(valid_df["Level 1"].dropna().astype(str).unique())

valid_characteristics_df = pd.read_excel(validation_categories_file_path, sheet_name="Characteristic")
valid_characteristics = set(valid_characteristics_df["Characteristic"].dropna().astype(str).unique())

valid_flavours_df = pd.read_excel(validation_categories_file_path, sheet_name="Flavour")
valid_flavours = set(valid_flavours_df["Flavour"].dropna().astype(str).unique())

#############
# Pydantic schema for categorisation output
#############
class CategorySchema(BaseModel):
    category_3: Literal[*valid_cat3]  # Strict validation for Level 3
    category_2: str  # Allow any string for Level 2 (max 500 allowed across all schema)
    category_1: Literal[*valid_cat1]    # Strict validation for Level 1
    characteristics: List[Literal[*valid_characteristics]] = Field(default_factory=list)
    flavours: List[Literal[*valid_flavours]] = Field(default_factory=list)

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
required_keys = ["category_3", "category_2", "category_1", "characteristics", "flavours"]

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
        tasks.append(client.beta.chat.completions.parse(
            model=gpt_model,
            messages=messages,
            max_tokens=100,
            temperature=0,
            response_format=CategorySchema,
        ))

    responses = await asyncio.gather(*tasks)  # Run all requests in parallel

    outputs = [r.choices[0].message.content for r in responses]  # Extract all responses
    return list(zip(UIDs, item_names, outputs))  # Maintain correct UID-output mapping

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
    # Use tqdm normally
    with tqdm(total=num_items, desc=f"Prompting GPT over {num_items} products, batches of {batch_size}") as pbar:
        for i in range(0, num_items, batch_size):
            batch = df_items.iloc[i:i + batch_size]
            UIDs = batch["UID"].tolist()
            item_names = batch["product name"].tolist()
            
            try:
                batch_results = await prompt_gpt_batch(UIDs, item_names)
            except Exception as e:
                batch_results = [(UID, item, "Error") for UID, item in zip(UIDs, item_names)]
            
            results.extend(batch_results)
            pbar.update(len(batch))  # Update progress bar by batch size
        pbar.close()

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
            data = json.loads(response) if isinstance(response, str) else response
            if not isinstance(data, dict):
                raise ValueError("Response is not a dictionary")
        except Exception:
            invalid_entries.append({"UID": UID, "product name": product_name, "reason": "Invalid JSON"})
            continue
        
        parsed_data = {key: data.get(key, []) if isinstance(data.get(key), (str, list)) else [] for key in required_keys}
        category3_value = parsed_data["category_3"]
        
        if category3_value not in valid_cat3:
            invalid_entries.append({"UID": UID, "product name": product_name, "reason": f"Invalid category_3: {category3_value}"})
            continue
        
        # Validate characteristics and flavours
        for attr_type, valid_set in [("characteristics", valid_characteristics), ("flavours", valid_flavours)]:
            attributes = parsed_data.get(attr_type, [])

            # If no flavour is provided, apply "Non food"
            if attr_type == "flavours" and not attributes:
                attributes = ["Non food"]

            for attr in attributes:
                if attr in valid_set:
                    valid_entries.append({"UID": UID, "category_3": category3_value, "attribute type": attr_type.capitalize(), "attribute value": attr, "value": 1})
                else:
                    invalid_entries.append({"UID": UID, "product name": product_name, "reason": f"Invalid {attr_type.capitalize()}: {attr}"})

    return valid_entries, invalid_entries
