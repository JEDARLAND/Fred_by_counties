# script1_generate_county_list.py

import csv
import requests
import io
import pandas as pd

def get_county_fips_list():
    """
    Returns a DataFrame with columns: fips, county_name, state.
    Uses an existing dataset.
    """
    # Example: use the FIPS codes dataset from walker-data (tidycensus reference)
    url = "https://walker-data.com/tidycensus/reference/fips_codes.html"
    # Note: this URL is HTML; you may instead locate a CSV download or other source.
    # For demonstration, we will instead use the API at https://api.fips.codes/index
    api_url = "https://api.fips.codes/index"
    resp = requests.get(api_url)
    resp.raise_for_status()
    js = resp.json()
    
    rows = []
    for state_abbrev, stdata in js.items():
        state_name = stdata["_name"]
        for county_name, fips in stdata.items():
            if county_name.startswith("_"):
                continue
            rows.append({
                "fips": fips,
                "county_name": county_name,
                "state": state_name
            })
    df = pd.DataFrame(rows)
    return df

def save_to_csv(df, outfn="county_fips_list.csv"):
    df.to_csv(outfn, index=False, columns=["fips", "county_name", "state"])
    print(f"Saved {len(df)} rows to {outfn}")

if __name__ == "__main__":
    df = get_county_fips_list()
    save_to_csv(df)
