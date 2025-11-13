import pandas as pd

# --- Configuration ---
OUTPUT_JSON_FILE = "2024_pres_results.json"
url_2024 = "https://raw.githubusercontent.com/tonmcg/US_County_Level_Election_Results_08-24/master/2024_US_County_Level_Presidential_Results.csv"

print("--- Starting Election Data Processing ---")

# Read CSV directly into pandas, using dtype=str to preserve FIPS codes and similar data
election_df = pd.read_csv(url_2024, dtype=str)

# Optional: preview first few rows
print(election_df.head())
print("-" * 20)

# Export to JSON
# orient='records' exports as a list of objects (one object per county/row).
# indent=4 makes the output human-readable.
election_df.to_json(OUTPUT_JSON_FILE, orient='records', indent=4)

print(f"✅ Downloaded and saved results to {OUTPUT_JSON_FILE}")