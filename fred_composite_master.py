import json
import os
from collections import defaultdict

# --- Configuration ---

OUTPUT_DIR = 'fred_county_series_output'
MASTER_FILENAME = 'fred_master_counties.json'

# Standard USPS abbreviations for the 50 US states and the District of Columbia
US_STATES = {
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
    # ADDITION: Include the District of Columbia
    'DC' 
}

def consolidate_and_archive_fred_data(output_dir: str, master_filename: str):
    """
    Reads all state JSON files, filters for the 50 US states PLUS D.C.,
    composites them into one master JSON, and deletes the originals.
    """
    
    if not os.path.isdir(output_dir):
        print(f"Error: Directory '{output_dir}' not found. Please run the county script first.")
        return

    # Dictionary to hold the master aggregated data
    # Structure: master_data[State_Abbreviation] = {Series_Title: [County_Records...]}
    master_data = {}
    files_to_delete = []
    
    print(f"Starting consolidation from directory: {output_dir}")

    # 1. Read and Composite Data
    for filename in os.listdir(output_dir):
        if not filename.endswith('_fred_series.json'):
            continue

        # Extract the state abbreviation (e.g., 'DC' from 'DC_fred_series.json')
        state_abbr = filename.split('_')[0]
        
        # Check against the revised set including 'DC'
        if state_abbr in US_STATES:
            filepath = os.path.join(output_dir, filename)
            
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    state_data = json.load(f)
                
                # Add the state's data to the master dictionary
                master_data[state_abbr] = state_data
                
                # Queue the file for deletion upon successful read
                files_to_delete.append(filepath)
                
                print(f"  ✓ Included {state_abbr} ({len(state_data)} unique series titles)")
                
            except json.JSONDecodeError:
                print(f"  ! Skipped {filename}: Error decoding JSON.")
            except Exception as e:
                print(f"  ! Skipped {filename}: An unexpected error occurred: {e}")
        else:
            print(f"  - Skipped {state_abbr}: Not one of the 50 US states or D.C.")

    if not master_data:
        print("\nNo state or D.C. data found. Nothing to consolidate.")
        return

    # 2. Write Master JSON File
    master_filepath = os.path.join(output_dir, master_filename)
    try:
        with open(master_filepath, 'w', encoding='utf-8') as f:
            # Sort the master keys (state abbreviations) alphabetically for cleaner archiving
            json.dump(master_data, f, indent=4, sort_keys=True)
        
        print(f"\nSuccessfully created master file: **{master_filepath}**")
        print(f"Contains data for {len(master_data)} states/districts.")
        
    except IOError as e:
        print(f"FATAL ERROR: Could not write master file {master_filepath}. Aborting file deletion. Error: {e}")
        return

    # 3. Delete Individual State Files
    print("\nStarting deletion of individual state files...")
    deleted_count = 0
    for filepath in files_to_delete:
        try:
            os.remove(filepath)
            deleted_count += 1
        except OSError as e:
            print(f"  ! Error deleting file {filepath}: {e}")

    print(f"\nConsolidation complete. Deleted {deleted_count} individual files.")


# --- Execution ---
if __name__ == "__main__":
    consolidate_and_archive_fred_data(OUTPUT_DIR, MASTER_FILENAME)