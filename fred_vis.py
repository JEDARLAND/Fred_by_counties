import plotly.express as px
import pandas as pd
import json
from urllib.request import urlopen
import numpy as np

# --- 1. Load GeoJSON Data for US Counties ---
# This GeoJSON file contains the county boundaries and the 'id' field is the 5-digit FIPS code.
try:
    with urlopen('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json') as response:
        counties = json.load(response)
except Exception as e:
    print(f"Error loading GeoJSON data: {e}")
    # Exit or handle error if the critical GeoJSON data can't be fetched
    exit()

# --- 2. Prepare Sample Data (Your Data Here) ---
# For demonstration, we create a dummy DataFrame with FIPS codes and a value.
# **In a real scenario, you would load your own data here.**
# The FIPS codes MUST be 5-digit strings (e.g., '01001' for Autauga County, AL).

# Get all FIPS codes from the GeoJSON features for a complete map
all_fips = [feature['id'] for feature in counties['features']]

# --- 2. Prepare Your Custom Data ---

# Define the path to your data file
data_file_path = '/workspaces/US-County_analyses/subs/fred_fips_map.json'
fips_key = 'FIPS' # Key for FIPS code in the JSON file
value_key = 'County_Category_ID' # Key for the value to plot in the JSON file

try:
    # Load your JSON file into a Pandas DataFrame
    custom_data = pd.read_json(data_file_path)

    # 1. Select the FIPS and value columns and rename the value column for clarity
    df_data = custom_data[[fips_key, value_key]].rename(columns={
        fips_key: 'FIPS',
        value_key: 'Data_Value'
    })

    # 2. Crucially, ensure FIPS codes are 5-digit strings (padding with leading zeros if necessary)
    # The Plotly GeoJSON requires 5-digit strings (e.g., '01001')
    df_data['FIPS'] = df_data['FIPS'].astype(str).str.zfill(5)

    # 3. Create the final DataFrame (df) by merging your data with all FIPS codes.
    # This ensures that even counties not in your JSON file are present, 
    # typically assigned a NaN/None value, which Plotly can handle (usually showing as gray).
    all_fips_df = pd.DataFrame({'FIPS': all_fips}) # all_fips comes from the GeoJSON loading step
    
    df = all_fips_df.merge(df_data, on='FIPS', how='left')

    # Optional: Fill missing values with a placeholder (e.g., 0 or a large number) 
    # if you want unrepresented counties to have a specific color.
    # df['Data_Value'] = df['Data_Value'].fillna(0) 

except FileNotFoundError:
    print(f"Error: The file {data_file_path} was not found.")
    # Exit or provide a fallback mechanism if the data file is critical
    exit()
except Exception as e:
    print(f"An error occurred while processing the JSON file: {e}")
    exit()

# The DataFrame 'df' is now ready for the Plotly creation step.
# It has two columns: 'FIPS' (5-digit string) and 'Data_Value' (the County_Category_ID).

# Ensure FIPS column is a string type
df['FIPS'] = df['FIPS'].astype(str)

# --- 3. Create the Choropleth Map ---
fig = px.choropleth(
    df,                               # Your DataFrame
    geojson=counties,                 # The GeoJSON data for boundaries
    locations='FIPS',                 # Column in your data that matches the GeoJSON 'id'
    color='Data_Value',               # Column to use for coloring the counties
    color_continuous_scale="Viridis", # Color scheme for the map
    scope="usa",                      # Focus the map on the USA (essential for county maps)
    labels={'Data_Value':'County Data Value'}, # Label for the color bar
    hover_name='FIPS',                # Display the FIPS code on hover
    title="US County Map by FIPS Code (Choropleth Example)"
)

# --- 4. Customize and Display the Map ---
# Update layout to remove margins and make the map clean
fig.update_layout(
    margin={"r":0,"t":40,"l":0,"b":0},
    mapbox_style="carto-positron" # You can choose a different background map style
)

# Remove county borders for a cleaner look if desired
fig.update_traces(marker_line_width=0)

# Display the interactive map
# In Codespaces, this will open the map in your browser or an output tab.
fig.show()