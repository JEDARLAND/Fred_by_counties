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

# Create a DataFrame with FIPS and random data values
np.random.seed(42) # for reproducible results
df = pd.DataFrame({
    'FIPS': all_fips,
    # Assign a random value to each county FIPS for coloring
    'Data_Value': np.random.randint(10, 100, len(all_fips)) 
})

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