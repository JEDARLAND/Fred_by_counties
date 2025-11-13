import json
import requests
import time
from collections import defaultdict
import os
from operator import itemgetter 

# --- Configuration ---

# Input file path provided by the user
INPUT_FILE = '/workspaces/US-County_analyses/subs/fred_fips_map.json'
# Output directory to save the state-level JSON files
OUTPUT_DIR = 'fred_county_series_output'

# FRED API endpoint for a category's series (replace with your actual FRED API key)
# NOTE: You MUST replace 'YOUR_FRED_API_KEY' with your actual key for the script to work.
FRED_API_KEY = '4e4f431f570df2e81fd7935bd7f48034'
FRED_SERIES_ENDPOINT = "https://api.stlouisfed.org/fred/category/series"

# Delay parameters (as requested)
COUNTY_QUERY_DELAY = 0.5  # seconds (delay between county-level queries)
STATE_COMPLETION_DELAY = 1.0  # seconds (delay between finishing a state and starting the next)

# --- Utility Functions ---

def fetch_fred_series(category_id: int) -> list:
    """
    Fetches the list of available FRED series for a given category ID.
    
    Args:
        category_id: The County_Category_ID from the input file.
        
    Returns:
        A list of series records (dictionaries), or an empty list on failure.
    """
    if not category_id:
        return []
        
    # Convert category ID (which might be a float) to a string for the API call
    category_id_str = str(int(category_id))
    
    params = {
        'api_key': FRED_API_KEY,
        'category_id': category_id_str,
        'file_type': 'json'
    }

    try:
        response = requests.get(FRED_SERIES_ENDPOINT, params=params)
        response.raise_for_status() # Raise exception for bad status codes
        data = response.json()
        
        if 'seriess' in data:
            return data['seriess']
        else:
            print(f"Warning: No 'seriess' found for category {category_id_str}.")
            return []

    except requests.exceptions.RequestException as err:
        print(f"Error fetching series for category {category_id_str}: {err}")
    except json.JSONDecodeError:
        print(f"Error decoding JSON response for category {category_id_str}.")
        
    return []


def process_fred_map_file():
    """
    Main function to read, sort, fetch FRED series, and composite 
    the results into state-level JSON files.
    """
    
    if FRED_API_KEY == 'YOUR_FRED_API_KEY':
        print("!!! ERROR: Please replace 'YOUR_FRED_API_KEY' with your actual FRED API key. !!!")
        return

    print(f"Starting FRED series lookup from {INPUT_FILE}...")
    
    # 1. Setup
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"Created output directory: {OUTPUT_DIR}")

    try:
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            county_data_list = json.load(f)
    except Exception as e:
        print(f"Error loading or decoding JSON from {INPUT_FILE}: {e}")
        return

    # 2. Sort the entire list by the 'State' field
    print("Sorting county data by state abbreviation...")
    sorted_county_data = sorted(county_data_list, key=itemgetter('State'))
    
    # Group the sorted list into dictionaries keyed by state for sequential processing
    state_groups = defaultdict(list)
    for record in sorted_county_data:
        state_groups[record['State']].append(record)

    total_states = len(state_groups)
    print(f"Found {total_states} unique states to process.")
    
    states_processed_count = 0

    # 3. Process State-by-State
    for state_abbr, counties_in_state in state_groups.items():
        states_processed_count += 1
        print(f"\n===== Processing State: **{state_abbr}** ({len(counties_in_state)} counties) =====")
        
        # Structure for the current state's results: Normalized_Series_Title -> [County_Record_1, ...]
        state_results = defaultdict(list)
        
        # A. Process all counties within the current state
        for i, county_record in enumerate(counties_in_state):
            county_name = county_record.get('County_Name', 'Unknown County')
            category_id = county_record.get('County_Category_ID')

            print(f"  [{i+1}/{len(counties_in_state)}] Querying {county_name}...")

            if not category_id:
                print(f"    Skipping {county_name} due to missing County_Category_ID.")
                continue
                
            series_list = fetch_fred_series(category_id)

            # B. Composite Results by Normalized Series Title
            for series in series_list:
                full_series_title = series.get('title', 'Unknown Series Title')
                
                # --- LOGIC TO EXTRACT NORMALIZED SERIES TITLE ---
                normalized_title = full_series_title
                if ' in ' in full_series_title:
                    # Find the index of the ' in ' separator
                    location_start_index = full_series_title.rfind(' in ')
                    
                    # Look back from ' in ' to find the preceding comma
                    # This ensures we stop before the county name/location starts
                    comma_index = full_series_title.rfind(',', 0, location_start_index)
                    
                    if comma_index != -1:
                        # Use the part before the last comma found
                        normalized_title = full_series_title[:comma_index].strip()
                    else:
                        # Fallback: if no comma is found, just use the part before ' in '
                        normalized_title = full_series_title.split(' in ')[0].strip()

                series_title_key = normalized_title
                # -----------------------------------------------------------
                
                # Create the county-specific series record for the output
                county_output_record = {
                    "FIPS": county_record.get('FIPS'),
                    "County_Name": county_record.get('County_Name'),
                    "FRED_ID": series.get('id'),
                    "Units": series.get('units'),
                    "Full_Series_Title": full_series_title 
                }
                
                state_results[series_title_key].append(county_output_record)

            # C. County-Level Delay
            time.sleep(COUNTY_QUERY_DELAY)
            
        # 4. Write Final State JSON File
        output_filename = os.path.join(OUTPUT_DIR, f'{state_abbr}_fred_series.json')
        print(f"--- Finished all counties for {state_abbr}. Writing output file... ---")
        try:
            with open(output_filename, 'w', encoding='utf-8') as f:
                json.dump(state_results, f, indent=4)
            print(f"Successfully saved: **{output_filename}** ({len(state_results)} unique series titles)")
        except IOError as e:
            print(f"Error writing file {output_filename}: {e}")
            
        # 5. State-Level Delay
        if states_processed_count < total_states:
            print(f"Delaying for {STATE_COMPLETION_DELAY}s before starting the next state.")
            time.sleep(STATE_COMPLETION_DELAY)

    print("\nProcessing complete! 🎉")

# --- Execution ---
if __name__ == "__main__":
    process_fred_map_file()