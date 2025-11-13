import pandas as pd

# --- Configuration ---
OUTPUT_JSON_FILE = "county_fips.json"
url = "https://www2.census.gov/geo/docs/reference/codes/files/national_county.txt"

print("--- Starting FIPS Data Processing ---")

# Read the file — it's comma-delimited, not pipe-delimited.
# 💡 FIX: Changed 'sep='|'' to 'sep=',''.
# This ensures columns are read correctly, allowing FIPS concatenation to work.
df = pd.read_csv(
    url, 
    header=None, 
    dtype=str, 
    names=["StateAbbr", "StateFP", "CountyFP", "CountyName", "ClassCode"],
    sep=',' # CORRECTED SEPARATOR
)

# Combine StateFP and CountyFP to create full 5-digit FIPS code
# This now works because StateFP and CountyFP are correctly isolated columns.
df["FIPS"] = df["StateFP"] + df["CountyFP"]

# Select and reorder columns for the final output
df_final = df[["FIPS", "CountyName", "StateAbbr"]].rename(columns={"StateAbbr": "State"})

# Export to JSON
# orient='records' exports as a list of objects, one object per county:
# [{"FIPS": "01001", "CountyName": "Autauga County", "State": "AL"}, ...]
df_final.to_json(OUTPUT_JSON_FILE, orient='records', indent=4)

print(f"✅ {OUTPUT_JSON_FILE} has been created successfully!")