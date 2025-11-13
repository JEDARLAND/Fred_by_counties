import pandas as pd
import json
import re
from io import StringIO # Used for placeholder data when files are not found

# Define the file paths (INPUT files are now JSON)
FIPS_FILE_PATH = "/workspaces/US-County_analyses/subs/county_fips.json"
FRED_FILE_PATH = "/workspaces/US-County_analyses/subs/fred_county_ids.json"
MAP_FILE_PATH = "/workspaces/US-County_analyses/fred_name_correction_map.json"

# Define the output file names (output files are JSON)
MAP_OUTPUT_FILE = "fred_fips_map.json"
FIPS_NO_MATCH_OUTPUT_FILE = "fips_no_match.json"
FRED_NO_MATCH_OUTPUT_FILE = "fred_no_match.json"

# Placeholder JSON content for the name correction map
name_correction_json = """
{
    "Anchorage Municipality": "Anchorage",
    "Anchorage Borough/municipality": "Anchorage",
    
    "Broomfield County": "Broomfield",
    "Broomfield County/city": "Broomfield",
    
    "Honolulu County": "Honolulu",
    "Honolulu County/city": "Honolulu",
    
    "Juneau City and Borough": "Juneau",
    "Juneau Borough/city": "Juneau",
    
    "Philadelphia County": "Philadelphia",
    "Philadelphia County/city": "Philadelphia",
    
    "San Francisco County": "San Francisco",
    "San Francisco County/city": "San Francisco",
    
    "Sitka City and Borough": "Sitka",
    "Sitka Borough/city": "Sitka",
    
    "Wrangell City and Borough": "Wrangell",
    "Wrangell Borough/City": "Wrangell",
    
    "Yakutat City and Borough": "Yakutat"
}
"""

# --- NEW FUNCTION FOR CLEANING ---
def clean_county_name(name):
    """
    Standardizes county names by:
    1. Removing common non-alphanumeric characters.
    2. Converting to uppercase and removing all internal spaces for a robust join key.
    """
    if pd.isna(name):
        return name
    name = str(name).strip()
    
    # 1. Remove non-word characters and standardize multiple spaces to single space
    name = re.sub(r'\W+', ' ', name) 
    
    # 2. Remove leading/trailing spaces
    name = name.strip()
    
    # 3. Create a clean JoinKey by removing all spaces and uppercasing
    join_key = name.replace(' ', '').upper()
    return join_key

# ---------------------------------

def generate_county_maps_with_correction():
    """
    Performs a left outer join after correcting FIPS and FRED county names 
    using a lookup map and generates three output JSON files.
    """
    print("--- Starting US County ID Mapping Process with Name Correction ---")

    # --- 1. Load Data ---
    try:
        # Load Name Correction Map
        name_map = json.loads(name_correction_json) 
        
        # Load FIPS data (Left table)
        df_fips = pd.read_json(FIPS_FILE_PATH, dtype={'FIPS': str})

        # Data Cleaning for FIPS
        df_fips['CountyName'] = df_fips['CountyName'].astype(str).str.strip()
        df_fips['State'] = df_fips['State'].astype(str).str.strip()
        
        # Apply the correction map to the FIPS CountyName 
        df_fips['CountyName'] = df_fips['CountyName'].map(name_map).fillna(df_fips['CountyName'])

        # --- ADD NEW JOIN KEY FOR FIPS ---
        df_fips['JoinKey'] = df_fips['CountyName'].apply(clean_county_name)
        # ----------------------------------

        # Load FRED data (Right table)
        df_fred = pd.read_json(FRED_FILE_PATH)
        

        print(f"Successfully loaded {FIPS_FILE_PATH} ({len(df_fips)} rows)")
        print(f"Successfully loaded {FRED_FILE_PATH} ({len(df_fred)} rows)")
        print("Name correction map loaded successfully.")
        
    except FileNotFoundError as e:
        print(f"🛑 Error: One or more files could not be found. Please check paths. {e}")
        return
    except Exception as e:
        print(f"🛑 Critical Error during file loading. Is the JSON format correct? Details: {e}")
        return


    # --- 2. Data Preparation: Clean and Correct FRED County Names ---
    
    # 2a. Extract the base County Name from FRED data
    df_fred['CountyName_Base'] = df_fred['County_Name'].str.split(',').str[0].str.strip()

    # 2b. Apply the name correction map to FRED names
    df_fred['CountyName_Corrected'] = df_fred['CountyName_Base'].map(name_map).fillna(df_fred['CountyName_Base'])

    # 2c. Extract the State Abbreviation
    df_fred['State'] = df_fred['County_Name'].str.extract(r',\s*([A-Z]{2})$')
    
    # 💡 CRITICAL FIX: Manually assign the state for Yakutat where the FRED entry lacks a comma separator.
    yakutat_mask = (df_fred['CountyName_Base'] == 'Yakutat City and Borough') & (df_fred['State'].isna())
    df_fred.loc[yakutat_mask, 'State'] = 'AK'
    
    # --- ADD NEW JOIN KEY FOR FRED (Using the corrected name) ---
    df_fred['JoinKey'] = df_fred['CountyName_Corrected'].apply(clean_county_name)
    # -----------------------------------------------------------
    
    print("FRED County names cleaned, corrected, and State codes extracted for joining.")

    # --- 3. Perform a Full Outer Join (REVISED JOIN KEYS) ---
    df_full_join = pd.merge(
        df_fips,
        df_fred,
        # Join on 'JoinKey' (normalized county name) and 'State'
        on=['JoinKey', 'State'], 
        how='outer',
        suffixes=('_FIPS', '_FRED'),
        indicator='_merge'
    )
    
    # --- 4. Generate Output JSON Files (NO CHANGES NEEDED HERE) ---
    
    # Define common JSON output arguments
    json_args = {'orient': 'records', 'indent': 4}

    # 4a. Left Outer Join Result: fred_fips_map.json
    df_left_join = df_full_join[
        df_full_join['_merge'].isin(['both', 'left_only'])
    ].copy()
    
    # Drop the temporary 'JoinKey' columns
    df_map = df_left_join.drop(columns=['_merge', 'CountyName_Base', 'CountyName_Corrected', 'JoinKey'], errors='ignore')
    df_map.to_json(MAP_OUTPUT_FILE, **json_args)
    print(f"✅ Created {MAP_OUTPUT_FILE} ({len(df_map)} rows) - Left Outer Join result.")
    
    # 4b. FIPS rows with no match (Left Only): fips_no_match.json
    df_fips_no_match = df_full_join[df_full_join['_merge'] == 'left_only'].copy()
    
    # Filter to include only the 50 US states
    US_STATES_50 = [
        'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 
        'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 
        'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 
        'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
    ]
    df_fips_no_match = df_fips_no_match[df_fips_no_match['State'].isin(US_STATES_50)]
    
    # Drop the temporary 'JoinKey' column
    df_fips_no_match = df_fips_no_match[['FIPS', 'CountyName', 'State']].rename(columns={'CountyName': 'CountyName_FIPS', 'State': 'State_FIPS'})
    df_fips_no_match.to_json(FIPS_NO_MATCH_OUTPUT_FILE, **json_args)
    print(f"✅ Created {FIPS_NO_MATCH_OUTPUT_FILE} ({len(df_fips_no_match)} rows) - FIPS rows with no FRED match (50 US States only).")

    # 4c. FRED rows with no match (Right Only): fred_no_match.json
    df_fred_no_match = df_full_join[df_full_join['_merge'] == 'right_only'].copy()
    
    fred_cols = ['Parent_State', 'County_Name', 'County_Category_ID', 'Series_Count', 'FRED_URL']
    df_fred_no_match = df_fred_no_match[fred_cols]
    df_fred_no_match.to_json(FRED_NO_MATCH_OUTPUT_FILE, **json_args)
    print(f"✅ Created {FRED_NO_MATCH_OUTPUT_FILE} ({len(df_fred_no_match)} rows) - FRED rows with no FIPS match.")

    print("\n--- Process Complete ---")

if __name__ == "__main__":
    generate_county_maps_with_correction()