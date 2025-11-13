import json
import glob
import os

def extract_series_prefix(full_title):
    """
    Extracts the leading part of the series title by removing the
    county-specific location phrase (e.g., ' in Autauga County, AL').
    The extraction works by finding the last occurrence of ' in ' in the string.
    """
    if not isinstance(full_title, str):
        return None

    # Find the index of the last occurrence of " in ".
    try:
        # This reliably splits off the county name (e.g., 'in Autauga County, AL')
        last_in_index = full_title.rindex(" in ")
        # Return the string slice up to that index, removing any potential trailing whitespace
        return full_title[:last_in_index].strip()
    except ValueError:
        # If the expected pattern " in " is not found, return the original title
        return full_title.strip()

def combine_state_data_by_series_title(
    input_directory="/Users/home/Desktop/US-County_analyses/fred_county_series_output",
    output_filename="fred_by_series_title.json"
):
    """
    Reads all JSON files matching the pattern '*_fred_series.json' in the
    specified directory, combines their data, and organizes the results
    by the series title prefix (e.g., 'Unemployment Rate') into a single
    output JSON file.
    """
    # Use glob to find all files matching the pattern
    file_pattern = os.path.join(input_directory, "*_fred_series.json")
    all_files = glob.glob(file_pattern)

    if not all_files:
        print(f"⚠️ No files found matching '{file_pattern}'. Please check the directory path.")
        return

    # Dictionary to hold the final combined data, keyed by the series title prefix
    # e.g., {"Unemployment Rate": [series_for_county_A, series_for_county_B, ...]}
    combined_data = {}

    print(f"🔍 Found {len(all_files)} files in '{input_directory}'. Starting to process...")

    # Iterate through each found file
    for filename in all_files:
        try:
            with open(filename, 'r') as f:
                data = json.load(f)

                # Assuming the top level is a dictionary like {"Category": [list of series]}
                for category, series_list in data.items():
                    # Iterate through the list of series within the category
                    for series in series_list:
                        # --- Key Logic: Use the series title prefix for the dictionary key ---
                        # CHANGED FIELD NAME HERE
                        full_title = series.get("Full_Series_Title")

                        if full_title:
                            # Extract the common part of the title
                            series_prefix = extract_series_prefix(full_title)

                            # Append the entire series object to the list for this series prefix
                            if series_prefix not in combined_data:
                                combined_data[series_prefix] = []

                            combined_data[series_prefix].append(series)
                        else:
                            print(f"Skipping a series in {filename} due to missing 'Full_Series_Title' field.")

        except json.JSONDecodeError:
            print(f"🚫 Error decoding JSON in file: {filename}")
        except Exception as e:
            print(f"🚫 An unexpected error occurred while processing {filename}: {e}")

    # Write the final combined data to the output file
    try:
        # Create the full path for the output file
        output_path = os.path.join(os.path.dirname(input_directory), output_filename)

        with open(output_path, 'w') as outfile:
            json.dump(combined_data, outfile, indent=4)

        print(f"✅ Successfully combined data from {len(all_files)} files.")
        print(f"The final data is grouped under {len(combined_data)} unique series titles.")
        print(f"Output saved to: {output_path}")

    except Exception as e:
        print(f"🚫 Error writing to output file {output_filename}: {e}")

# --- Execute the function ---
combine_state_data_by_series_title()