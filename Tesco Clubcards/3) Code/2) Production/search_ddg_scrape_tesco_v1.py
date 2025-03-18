from duckduckgo_search import DDGS
from sentence_transformers import util
import random
import requests
from bs4 import BeautifulSoup
import json
from functools import wraps
import time

#pass in model, languages, clean_string, accept_languages

def retry(retries=3):
    # Retry decorator with exponential backoff
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    # On last attempt, return None instead of retrying
                    if attempt == retries - 1:
                        return None
                    # Exponential backoff: 1s, 2s, 4s between retries
                    time.sleep(2 ** attempt)
            return None
        return wrapper
    return decorator

@retry(retries=3)
def search_ddgs(search_item):
    # Search DuckDuckGo with retry wrapper
    ddgs = DDGS()
    results = list(ddgs.text(search_item, max_results=100))  
    return results

def get_url_content(URL, accept_languages):
    headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    "Accept-Language": random.choice(accept_languages),
    'priority': 'u=0, i',
    'referer': 'https://www.google.com/',
    'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'cross-site',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
    }

    response = requests.get(URL, headers=headers)

    return response.content

def extract_url_data(content):
    try:
       # Parse content
       data = json.loads(BeautifulSoup(content.decode('utf-8'), 'html.parser').find('script', type='application/discover+json').string)
       
       # Find product data 
       product_data = next(
           value for key, value in data['mfe-orchestrator']['props']['apolloCache'].items() 
           if key.startswith('ProductType:')
       )

       return {
           'item_data': {
               'matched_name': product_data.get('title'),
               'barcode': product_data.get('gtin'),
               'brand': product_data.get('brandName'),
               'category_1': product_data.get('superDepartmentName'),
               'category_2': product_data.get('departmentName'),
               'category_3': product_data.get('aisleName'),
               'category_4': product_data.get('shelfName'),
               'rating': product_data.get('reviews({"count":10,"offset":0})', {}).get('stats', {}).get('overallRating')
           }
        }
    except Exception as e:
        # print(f"Error: {str(e)}")
        return {'item_data': dict.fromkeys(['title', 'gtin', 'brandName', 'superDepartmentName', 
                                       'departmentName', 'aisleName', 'shelfName', 'rating'])}
    
def run_ddg_searcher(search_item, model, clean_string, accept_languages):

    result_dict = {}

    # Add Tesco to item
    search_item = "Tesco " + search_item

    # Probe DDG with retry
    ddg_results = search_ddgs(search_item)

    # If the dict isn't returned or there's no returned items at all, reflect no match
    if not ddg_results:
        result_dict["item_data"] = None
        result_dict["match_score"] = 0.0
        return result_dict

    # Collect only URLs which are from tesco item search
    filtered_results = [item for item in ddg_results if "www.tesco.com/groceries/en-GB/products/" in item['href']]

    # If the dict isn't returned or there's no returned items at all, reflect no match
    if not filtered_results:
        result_dict["item_data"] = None
        result_dict["match_score"] = 0.0
        return result_dict

    # Get a list of the titles and URLs from this
    titles = [result["title"] for result in filtered_results]
    URLs = [result["href"] for result in filtered_results]

    # clean all wording
    cleaned_target = clean_string(search_item)
    cleaned_titles =  [clean_string(title) for title in titles] 

    # If there is a perfect name match, extract this one
    if cleaned_target in cleaned_titles:
        matched_index = cleaned_titles.index(cleaned_target) # Find where the match occured, then find the original name of the matching item to extract its dict
        matched_url = URLs[matched_index]
        url_content = get_url_content(matched_url, accept_languages)
        result_dict = extract_url_data(url_content)
        result_dict["match_score"] = 100.0
    
    else:

        # Encode items and target then calculate similarities
        target_embedding = model.encode(cleaned_target, convert_to_tensor=True)
        candidate_embeddings = model.encode(cleaned_titles, convert_to_tensor=True)
        cosine_scores = util.pytorch_cos_sim(target_embedding, candidate_embeddings)[0]

        # Collect the best match then find the product name
        best_match_index = cosine_scores.argmax().item()
        best_match_url = URLs[best_match_index]

        # Extract the dict with the best matching name
        url_content = get_url_content(best_match_url, accept_languages)
        result_dict = extract_url_data(url_content)
        result_dict["match_score"] = round(cosine_scores[best_match_index].item() * 100, 1)

    return result_dict
