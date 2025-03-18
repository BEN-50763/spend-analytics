import pandas as pd
import re
from difflib import SequenceMatcher
from typing import Dict, List
from tqdm import tqdm
from collections import defaultdict
import keyboard
import time

def load_categories(filepath: str) -> List[str]:
    """Load unique categories from CSV."""
    df = pd.read_csv(filepath)
    return sorted(df['category_4'].dropna().unique().tolist())

def clean_text(text: str) -> str:
    """Clean text for comparison."""
    text = text.lower()
    text = re.sub(r'[&,]', ' and ', text)
    text = re.sub(r'[^a-z0-9\s]', ' ', text)
    return ' '.join(text.split())

def find_matching_words(str1: str, str2: str) -> str:
    """Find common words between two strings."""
    words1 = set(clean_text(str1).split())
    words2 = set(clean_text(str2).split())
    matching = words1.intersection(words2)
    return ' '.join(sorted(matching)) if matching else None

def save_mappings(mappings: Dict[str, str], filename: str = 'category_mappings.csv'):
    """Save current mappings to CSV."""
    df = pd.DataFrame(list(mappings.items()), columns=['Original', 'New Category'])
    df.to_csv(filename, index=False)

def load_mappings(filename: str = 'category_mappings.csv') -> Dict[str, str]:
    """Load existing mappings from CSV."""
    try:
        df = pd.read_csv(filename)
        return dict(zip(df['Original'], df['New Category']))
    except FileNotFoundError:
        return {}

def get_consolidated_categories(mappings: Dict[str, str]) -> Dict[str, List[str]]:
    """Group original categories by their consolidated category names."""
    consolidated = defaultdict(list)
    for orig, new in mappings.items():
        consolidated[new].append(orig)
    return consolidated

def find_potential_matches(categories: List[str], mappings: Dict[str, str]) -> List[tuple]:
    """Find potential category matches, prioritizing existing consolidated categories."""
    matches = []
    processed_pairs = set()  # Keep track of pairs we've already processed
    consolidated_categories = get_consolidated_categories(mappings)
    
    # For each unmapped category
    for cat in categories:
        if cat in mappings:
            continue
            
        best_match = None
        best_similarity = 0
        best_category = None
        mapped_match = False
        
        # First, try to match against existing consolidated categories
        for consolidated_cat, original_cats in consolidated_categories.items():
            all_cats_to_check = [consolidated_cat] + original_cats
            for check_cat in all_cats_to_check:
                similarity = SequenceMatcher(None, clean_text(cat), clean_text(check_cat)).ratio()
                if similarity > 0.6 and similarity > best_similarity:
                    # Create a sorted pair to check if we've seen this combination
                    pair = tuple(sorted([cat, check_cat]))
                    if pair not in processed_pairs:
                        best_similarity = similarity
                        best_match = original_cats[0]
                        best_category = consolidated_cat
                        mapped_match = True
                        processed_pairs.add(pair)
        
        # Only look for new pairs if no match found with consolidated categories
        if not mapped_match:
            for other_cat in categories:
                if other_cat == cat or other_cat in mappings:
                    continue
                    
                # Create a sorted pair to check if we've seen this combination
                pair = tuple(sorted([cat, other_cat]))
                if pair in processed_pairs:
                    continue
                    
                similarity = SequenceMatcher(None, clean_text(cat), clean_text(other_cat)).ratio()
                matching_words = find_matching_words(cat, other_cat)
                
                if similarity > 0.6 and matching_words and similarity > best_similarity:
                    best_similarity = similarity
                    best_match = other_cat
                    best_category = matching_words
                    processed_pairs.add(pair)
        
        if best_match:
            matches.append((cat, best_match, best_category, best_similarity))
    
    return sorted(matches, key=lambda x: x[3], reverse=True)

def get_user_input() -> tuple[str, str]:
    """Get user input for category matching."""
    buffer = []
    prompt = 'Type category name + ENTER to use custom name, ENTER to accept, CAPS LOCK to skip, ESC to exit: '
    print(f"\n{prompt}", end='', flush=True)
    
    while True:
        event = keyboard.read_event(suppress=True)
        if event.event_type == 'down':
            if event.name == 'enter':
                print()
                if buffer:
                    return 'custom', ''.join(buffer)
                return 'accept', None
            elif event.name == 'caps lock':
                print()
                return 'skip', None
            elif event.name == 'esc':
                print("\nExiting...")
                return 'quit', None
            elif event.name == 'backspace' and buffer:
                buffer.pop()
                # Clear line and rewrite current buffer
                print(f"\r{' ' * 150}\r{prompt}{''.join(buffer)}", end='', flush=True)
            elif event.name == 'space':
                buffer.append(' ')
                print(' ', end='', flush=True)
            elif len(event.name) == 1:
                buffer.append(event.name)
                print(event.name, end='', flush=True)

def capitalize_first(text: str) -> str:
    """Capitalize first letter of string, rest lowercase."""
    if not text:
        return text
    words = text.lower().split()
    if not words:
        return text
    words[0] = words[0].capitalize()
    return ' '.join(words)

def process_categories():
    """Main function to run the interactive consolidation process."""
    filepath = r"G:\My Drive\Wantrepreneurialism\Active\spend-analytics\Tesco Clubcards\5 - Processed Data Files\4) Gathered Data\tesco_output.csv"
    categories = load_categories(filepath)
    mappings = load_mappings()
    
    print("\nCategory Consolidation Tool")
    print("==========================")
    print(f"Loaded {len(categories)} categories and {len(mappings)} existing mappings")
    print("\nControls:")
    print("ENTER: Accept suggested category")
    print("CAPS LOCK: Skip this pair")
    print("ESC: Exit script")
    print("Type + ENTER: Use typed text as category name")
    
    try:
        while True:
            matches = find_potential_matches(categories, mappings)
            if not matches:
                print("\nNo more matches found!")
                break
                
            for cat1, cat2, suggested_category, similarity in tqdm(matches, desc="Processing matches"):
                if cat1 in mappings:
                    continue
                    
                print(f"\nPotential match (similarity: {similarity:.2f}):")
                print(f"1: {cat1}")
                if cat2 in mappings:
                    print(f"2: {cat2} (already mapped to '{mappings[cat2]}')")
                else:
                    print(f"2: {cat2}")
                    
                suggested_category = capitalize_first(suggested_category)
                print(f"Suggested category: {suggested_category}")
                
                action, custom_category = get_user_input()
                
                if action == 'quit':
                    return
                elif action == 'accept':
                    mappings[cat1] = suggested_category
                    if cat2 not in mappings:
                        mappings[cat2] = suggested_category
                    print(f"Mapped to: {suggested_category}")
                elif action == 'skip':
                    print("Skipped")
                elif action == 'custom':
                    custom_category = capitalize_first(custom_category)
                    mappings[cat1] = custom_category
                    if cat2 not in mappings:
                        mappings[cat2] = custom_category
                    print(f"Mapped to: {custom_category}")
                
                save_mappings(mappings)
                time.sleep(0.1)
            
    except KeyboardInterrupt:
        print("\nInterrupted by user")
    finally:
        save_mappings(mappings)

if __name__ == "__main__":
    process_categories()