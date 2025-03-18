import requests
import random
import time
from tqdm import tqdm
import logging
import re
from search_ddg_scrape_tesco_v1 import run_ddg_searcher
from utils import get_model
from sentence_transformers import util

# Define delay range for human-like behavior
min_delay = 0.5
max_delay = 1.5

# Define user agents and accept languages
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPad; CPU OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/91.0.4472.80 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0"
]

accept_languages = [
    "en-US,en;q=0.9",
    "en-GB,en;q=0.9",
    "en-CA,en;q=0.9",
    "en-AU,en;q=0.9",
    "en-NZ,en;q=0.9",
    "en-ZA,en;q=0.9",
    "en-IE,en;q=0.9"
]

# Simulate a human-like delay between requests
def human_like_delay():
    time.sleep(random.uniform(min_delay, max_delay))

# Use origin or previous item as referrer
def generate_referrer(previous_item=None):
    if previous_item is None:
        return "https://www.tesco.com"
    else:
        return f"https://www.tesco.com/groceries/en-GB/search?query={previous_item}"

# Use origin or previous item as referrer
def query_tesco_api(search_item, referrer, count, session=None):
    url = "https://api.tesco.com/shoppingexperience"
    
    headers = {
        "Referer": referrer,
        "Origin": "https://www.tesco.com",
        "User-Agent": random.choice(user_agents),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": random.choice(accept_languages),
        "x-apikey": "TvOSZJHlEk0pjniDGQFAc9Q59WGAR4dA",
        "Content-Type": "application/json"
    }
    
    query = """
    query Search($query: String!, $page: Int = 1, $count: Int = 2, $sortBy: String) {
      search(query: $query, page: $page, count: $count, sortBy: $sortBy) {
        pageInformation: info {
          totalCount: total
          pageNo: page
          count
          __typename
        }
        results {
          node {
            ... on ProductInterface {
              gtin
              title
              brandName
              superDepartmentName
              departmentName
              aisleName
              shelfName
              reviews {
                stats {
                  overallRating
                }
              }
            }
          }
        }
      }
    }
    """
    
    body = [{
        "operationName": "Search",
        "variables": {
            "query": search_item,
            "page": 1,
            "count": count,
            "sortBy": "relevance"
        },
        "extensions": {"mfeName": "unknown"},
        "query": query
    }]
    
    if session is None:
        session = requests.Session()
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Collect the relevant section of the response
            response = session.post(url, headers=headers, json=body)
            json_response = response.json()
            data = json_response[0].get('data', {})
            search_results = data.get('search', {})
            return {
                "status": "Data Found",
                "page_information": search_results.get('pageInformation'),
                "results": search_results.get('results', [])
            }
        #If this didn't work, response isn't what we've been hoping for, so try again 
        except requests.exceptions.RequestException as e:
            logging.error(f"Error querying item {search_item}: {str(e)}")
            if attempt == max_retries - 1:
                return {"status": f"API Call Unsuccessful: {str(e)}"}
            #Exponential sleep if it fails
            time.sleep((2 ** attempt) + random.random())

# Function to select the item dict we want from the list
def extract_matching_dict(results, target):
    for item in results['results']:
        if item['node']['title'].lower() == target.lower():
            return item['node']
    return None

# Function to rename the keys from the tesco api
def swap_dict_keys(input_dict):
    key_mappings = [
        ("title", "matched_name"),
        ("gtin", "barcode"),
        ("brandName", "brand"),
        ("superDepartmentName", "category_1"),
        ("departmentName", "category_2"),
        ("aisleName", "category_3"),
        ("shelfName", "category_4"),
    ]

    result = {}
    
    # Handle the rating separately
    try:
        if 'reviews' in input_dict and 'stats' in input_dict['reviews'] and 'overallRating' in input_dict['reviews']['stats']:
            result['rating'] = input_dict['reviews']['stats']['overallRating']
    except:
        result['rating'] = None
    
    # Process other mappings
    for new_key, old_key in key_mappings:
        if input_dict:
            if new_key in input_dict:
                result[old_key] = input_dict[new_key]
    
    return result

# Function to clean excess wording from item names to improve matches
def clean_string(input_string):
    return re.sub(r'\(.*?\)', '', input_string).strip().lower()

# Function to find the best 
def extract_best_match(target, candidates_dicts, model):

    result_dict = {}

    # If the dict isn't returned or there's no returned items at all, reflect no match
    if not candidates_dicts or "page_information" not in candidates_dicts or not candidates_dicts["page_information"]:
        result_dict["item_data"] = None
        result_dict["match_score"] = 0.0
        return result_dict
    
    # Extract titles into a list
    titles = [item['node']['title'] for item in candidates_dicts['results']]

    # clean all wording
    cleaned_target = clean_string(target)
    cleaned_titles =  [clean_string(title) for title in titles] 

    # If there is a perfect name match, extract this one
    # Swap the keys for the names we want
    if cleaned_target in cleaned_titles:
        matched_index = cleaned_titles.index(cleaned_target) # Find where the match occured, then find the original name of the matching item to extract its dict
        uncleaned_match = titles[matched_index]
        result_dict["item_data"] = swap_dict_keys(extract_matching_dict(candidates_dicts, uncleaned_match))
        result_dict["match_score"] = 100.0
        return result_dict
    # Else lets find the best match
    else:
        # Encode items and target then calculate similarities
        target_embedding = model.encode(cleaned_target, convert_to_tensor=True)
        candidate_embeddings = model.encode(cleaned_titles, convert_to_tensor=True)
        cosine_scores = util.pytorch_cos_sim(target_embedding, candidate_embeddings)[0]

        # Collect the best match then find the product name
        best_match_index = cosine_scores.argmax().item()
        best_match_name = titles[best_match_index]

        # Extract the dict with the best matching name
        result_dict["item_data"] = swap_dict_keys(extract_matching_dict(candidates_dicts, best_match_name))
        result_dict["match_score"] = round(cosine_scores[best_match_index].item() * 100, 1)

        # Use alternate method of finding the item through ddg search if poor match found
        if result_dict["match_score"] < 95:
            ddg_result_dict = run_ddg_searcher(target, model, clean_string, accept_languages)

            # If we got a better match using ddg, use this result, but only if it returned actual results
            if result_dict["match_score"] < ddg_result_dict["match_score"] and ddg_result_dict["item_data"]["rating"]:
                result_dict = ddg_result_dict

    return result_dict

# Main function to run the Tesco API processing
def run_tesco_scraper(search_items):
    model = get_model()  # Load the model for use in the scraper
    results = []
    session = requests.Session()
    previous_item = None

    with tqdm(total=len(search_items), desc=f"Scraping Tesco over {len(search_items)} products") as pbar:
        for item in search_items:
            # Delay to mimic human usage
            human_like_delay()
            referrer = generate_referrer(previous_item)
            all_results = query_tesco_api(item, referrer, 100, session)

            # Find closest match from all items we have found & return its dict
            result = extract_best_match(item, all_results, model)
            # If the API gave us no results, set all to None
            if not result or not result["item_data"]:
                results.append({
                    'name': item,
                    'match_score': result["match_score"],
                    'matched_name': None, 'barcode': None, 'brand': None, 
                    'category_1': None, 'category_2': None, 'category_3': None, 
                    'category_4': None, 'rating': None
                })
            else:
                results.append({
                    'name': item,
                    'match_score': result["match_score"],
                    **{f'{k}': v for k, v in result["item_data"].items() if k != 'status'}
                })
            # Update old item so we update the referer
            previous_item = item
            pbar.update(1)  # Increment progress bar by 1 per item

    return results


## Edit method so we scrape everything then post-process to get the info (takes more storage, but more robust method)
## Improve matching section to use a trained ML model for it 
## Add something to handle very low % matches 
## Also only search DDG with low matches as it slows things down having to do it every time < 100% - currently let to 95%
## Bug testing 
## Add robustness, eg picks back up if no internet; can distingusih between API error codes (re run for some)
## Ensure these are always re-run: ('Connection aborted.', RemoteDisconnected('Remote end closed connection without response'))
## Sometimes two items at tesco have the same name. Find method of choosing which to use - possibly include price or other clubcard data we have
## Remove funky accept languages & headers with module curl-cffi
## Fix error "'NoneType' object has no attribute 'string'█" - occurs when item is out of stock maybe? seems to find item, but not get results, so must have different API for those items
