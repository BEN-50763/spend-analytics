import openfoodfacts
import time
from tqdm import tqdm
import logging

# Initialize the API client
api = openfoodfacts.API(user_agent='testapp - Version 1 - benedict.s@hotmail.co.uk')

# Fields to extract from OpenFoodFacts
field_options = {
    'additives_n': True,
    'additives_tags': True,
    'allergens': True,
    'brands': True,
    'categories': True,
    'ecoscore_grade': True,
    'ingredients_n': True,
    'ingredients_text': True,
    'labels': True,
    'labels_hierarchy': True,
    'nutrition_grades': True,
    'nova_group': True,
    'nutriments': True,
    'packaging_recycling_tags': True,
    'packaging_tags': True,
    'stores_tags': True,
}

class RateLimiter:
    def __init__(self, max_calls, period):
        self.max_calls = max_calls
        self.period = period
        self.calls = []

    def __call__(self, f):
        def wrapper(*args, **kwargs):
            now = time.time()
            self.calls = [call for call in self.calls if now - call < self.period]
            if len(self.calls) >= self.max_calls:
                sleep_time = self.period - (now - self.calls[0])
                time.sleep(sleep_time)
            self.calls.append(time.time())
            return f(*args, **kwargs)
        return wrapper

# Create a rate limiter for 100 requests per minute
rate_limiter = RateLimiter(max_calls=100, period=60)

@rate_limiter
def query_openfoodfacts_barcode_api(barcode, field_options):
    """Query the OpenFoodFacts API for a specific barcode."""
    try:
        response = api.product.get(barcode, fields=field_options)
        if response:
            return {
                "status": "Data Found",
                **{field: response.get(field, 'Not available') for field in field_options}
            }
        else:
            return {"status": "No Data in Website"}
    except Exception as e:
        logging.error(f"Error querying barcode {barcode}: {str(e)}")
        return {"status": f"API Call Unsuccessful: {str(e)}"}

def run_openfoodfacts_query(barcodes):
    """Main function to run the OpenFoodFacts processing."""
    results = []
    
    with tqdm(total=len(barcodes), desc=f"Processing OFF over {len(barcodes)} products") as pbar:
        for barcode in barcodes:
            result = query_openfoodfacts_barcode_api(str(barcode), field_options)
            results.append({
                'barcode': barcode, 
                'OFF_data': result['status'], 
                **{f'OFF_{k}': v for k, v in result.items() if k != 'status'}
            })
            pbar.update(1)  # Update progress bar per barcode
    
    return results
