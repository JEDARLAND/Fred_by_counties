import pandas as pd
import re
import json

# Define the original file paths (JSON inputs)
FIPS_FILE_PATH = "/workspaces/US-County_analyses/subs/county_fips.json"
FRED_FILE_PATH = "/workspaces/US-County_analyses/subs/fred_county_ids.json"

# Define the output file names (UPDATED TO .json)
MAP_OUTPUT_FILE = "fred_fips_map.json"
FIPS_NO_MATCH_OUTPUT_FILE = "fips_no_match.json"
FRED_NO_MATCH_OUTPUT_FILE = "fred_no_match.json"

def save_dataframe_to_json(df, filename):
    """Helper function to save DataFrame to JSON in a readable 'records' format."""
    df.to_json(filename, orient='records', indent=4)

def generate_county_maps():
    """
    Performs a full outer join of FIPS data and FRED data on a cleaned 
    (CountyName, State) key and generates three output JSON files.
    """
    print("--- Starting US County ID Mapping Process ---")
    
    # --- 1. Load Data (Read JSON) ---
    try:
        # File 1: FIPS data (Left table for the final requested join)
        df_fips = pd.read_json(FIPS_FILE_PATH, dtype={'FIPS': str})
        
        # File 2: FRED data (Right table)
        df_fred = pd.read_json(FRED_FILE_PATH)
        
        print(f"Successfully loaded {FIPS_FILE_PATH} ({len(df_fips)} rows)")
        print(f"Successfully loaded {FRED_FILE_PATH} ({len(df_fred)} rows)")
        
    except FileNotFoundError as e:
        print(f"🛑 Error: One or both files could not be found: {e}")
        return
    except pd.errors.JSONDecodeError as e:
        print(f"🛑 Error: Failed to decode JSON from one of the files: {e}")
        return

    # --- 2. Data Preparation: Clean the FRED County Name for Joining ---
    
    # The 'County_Name' column in df_fred is in the format "County Name, State Abbreviation"
    
    # Extract the County Name (part before the comma)
    df_fred['CountyName'] = df_fred['County_Name'].str.split(',').str[0].str.strip()

    # Extract the State Abbreviation (2-letter code after the comma)
    df_fred['State'] = df_fred['County_Name'].str.extract(r',\s*([A-Z]{2})$')
    
    print("FRED County names cleaned and State codes extracted for joining.")

    # --- 3. Perform a Full Outer Join ---
    # We use 'outer' join to easily capture all three output sets
    df_full_join = pd.merge(
        df_fips,
        df_fred,
        on=['CountyName', 'State'],
        how='outer',
        suffixes=('_FIPS', '_FRED'),
        indicator=True # Adds the '_merge' column to identify the source of the row
    )
    
    # --- 4. Generate Output Files (Save JSON) ---
    
    # 4a. Left Outer Join Result: fred_fips_map.json
    # Rows found in FIPS (left) or in both. The FIPS columns will have data.
    df_left_join = df_full_join[
        df_full_join['_merge'].isin(['both', 'left_only'])
    ].copy()
    
    df_map = df_left_join.drop(columns=['_merge'])
    save_dataframe_to_json(df_map, MAP_OUTPUT_FILE)
    print(f"✅ Created {MAP_OUTPUT_FILE} ({len(df_map)} rows) - Left Outer Join result.")
    
    # 4b. File 1 (FIPS) has no match (Left Only): fips_no_match.json
    df_fips_no_match = df_full_join[df_full_join['_merge'] == 'left_only'].copy()
    
    # Select only the original FIPS columns for this file
    df_fips_no_match = df_fips_no_match[['FIPS', 'CountyName', 'State']]
    save_dataframe_to_json(df_fips_no_match, FIPS_NO_MATCH_OUTPUT_FILE)
    print(f"✅ Created {FIPS_NO_MATCH_OUTPUT_FILE} ({len(df_fips_no_match)} rows) - FIPS rows with no FRED match.")

    # 4c. File 2 (FRED) has no match (Right Only): fred_no_match.json
    df_fred_no_match = df_full_join[df_full_join['_merge'] == 'right_only'].copy()
    
    # Select the original FRED columns for this file
    fred_cols = ['Parent_State', 'County_Name', 'County_Category_ID', 'Series_Count', 'FRED_URL']
    df_fred_no_match = df_fred_no_match[fred_cols]
    save_dataframe_to_json(df_fred_no_match, FRED_NO_MATCH_OUTPUT_FILE)
    print(f"✅ Created {FRED_NO_MATCH_OUTPUT_FILE} ({len(df_fred_no_match)} rows) - FRED rows with no FIPS match.")

    print("\n--- Process Complete ---")

if __name__ == "__main__":
    generate_county_maps()