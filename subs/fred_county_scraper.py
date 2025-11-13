# script1_scrape_fred_county_list.py

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
import json # Import json for final output

# --- Configuration ---
# The starting URL for U.S. Regional Data, which links to all states.
TOP_LEVEL_REGIONAL_URL = "https://fred.stlouisfed.org/categories/27281"
FRED_BASE_URL = "https://fred.stlouisfed.org"
OUTPUT_JSON_FILE = "fred_county_ids.json" # New output file name

# --- Helper Functions ---

def get_html_content(url):
    """
    Fetches the HTML content for a given URL and returns the BeautifulSoup object.
    Includes a short delay to be respectful of the server.
    """
    try:
        # Standard user-agent header to mimic a web browser
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for bad status codes
        
        # Pause for a short time to avoid hitting the server too hard
        time.sleep(0.5) 
        
        return BeautifulSoup(response.content, 'html.parser')
    except requests.RequestException as e:
        print(f"Error fetching URL {url}: {e}")
        return None

def extract_county_list(county_list_url, state_name):
    """
    Scrapes the provided county list URL, including both columns (<ul> lists),
    to extract county name, category ID, and series count.
    """
    soup = get_html_content(county_list_url)
    if not soup:
        return []

    county_data = []
    
    # Find ALL <ul> tags that have 'list-bullets' in their class attribute.
    list_containers = soup.find_all('ul', class_=lambda c: c and 'list-bullets' in c.split())
    
    if not list_containers:
        print(f"  -> Warning: Could not find any list containers for {state_name} at {county_list_url}")
        return []

    for ul in list_containers:
        # Find all <li> elements within the current <ul>
        for li in ul.find_all('li'):
            a_tag = li.find('a')
            
            if a_tag and a_tag.has_attr('href'):
                # 1. Extract the Category ID from the href
                href = a_tag['href']
                category_id_match = re.search(r'/categories/(\d+)', href)
                category_id = category_id_match.group(1) if category_id_match else None
                
                # 2. Extract the County Name (text inside <a>)
                county_name_full = a_tag.get_text(strip=True)
                
                # 3. Extract the series count (the number in parentheses)
                text_content = li.get_text()
                series_count_match = re.search(r'\((\d+)\)$', text_content.strip())
                series_count = series_count_match.group(1) if series_count_match else None
                
                if category_id and county_name_full:
                    county_data.append({
                        "Parent_State": state_name,
                        "County_Name": county_name_full,
                        "County_Category_ID": category_id,
                        "Series_Count": series_count,
                        "FRED_URL": FRED_BASE_URL + href
                    })

    return county_data

def get_all_state_county_pages():
    """
    Navigates from the top-level regional page to find the 'Counties' link
    for each state.
    """
    print(f"Starting crawl at {TOP_LEVEL_REGIONAL_URL}...")
    top_soup = get_html_content(TOP_LEVEL_REGIONAL_URL)
    if not top_soup:
        return {}

    state_county_urls = {}
    
    # Find all State Categories on the main page
    state_lists = top_soup.find_all('ul', class_='list-bullets')

    for ul in state_lists:
        for li in ul.find_all('li'):
            a_tag = li.find('a')
            if a_tag and a_tag.has_attr('href'):
                state_name = a_tag.get_text(strip=True)
                state_url = FRED_BASE_URL + a_tag['href']
                
                # Scrape the State URL to find the 'Counties' link
                state_soup = get_html_content(state_url)
                
                if state_soup:
                    # Find the link that contains the text 'Counties'
                    county_link = state_soup.find('a', text=re.compile(r'\b(Counties|Parishes|Boroughs)\b'))
                    
                    if county_link and county_link.has_attr('href'):
                        county_page_url = FRED_BASE_URL + county_link['href']
                        state_county_urls[state_name] = county_page_url
                        print(f"-> Found County page for {state_name}: {county_page_url}")

    return state_county_urls

# --- Main Execution ---

if __name__ == "__main__":
    
    all_county_series_data = []
    
    # Step 1: Get the specific 'Counties' page URL for every state
    state_pages = get_all_state_county_pages()
    
    # Step 2: Scrape the County page for the list of counties
    print("\n--- Scraping Individual County Lists ---")
    for state, county_url in state_pages.items():
        print(f"Processing counties for {state}...")
        county_data = extract_county_list(county_url, state)
        all_county_series_data.extend(county_data)

    df = pd.DataFrame(all_county_series_data)
    
    # Final Output
    if not df.empty:
        # Use to_json() with orient='records' for a list of objects and indent=4 for readability
        df.to_json(OUTPUT_JSON_FILE, orient='records', indent=4) 
        print(f"\n✅ SUCCESS: Scraped and saved {len(df)} county entries to {OUTPUT_JSON_FILE}")
    else:
        print("\n❌ FAILURE: Failed to retrieve any county data.")