# This script pulls all barcodes from checked DigiApp exports within a specified date range, 
# then searches for the barcodes in the relevant barcode-guid matching databases

import pandas as pd
import sqlite3
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# Update collection name, dates, and file paths in .env file
collection = os.getenv("COLLECTION")
start_date = os.getenv("START_DATE") 
end_date = os.getenv("END_DATE")

database1 = os.getenv("DATABASE1")  
database2 = os.getenv("DATABASE2")  

base_db_path1 = os.getenv("DB_PATH1")
base_db_path2 = os.getenv("DB_PATH2")
db_path1 = base_db_path1.format(database1=database1)
db_path2 = base_db_path2.format(database2=database2)

# Construct the folder path dynamically (update the version in the .env as needed)
base_folder_path = os.getenv("FOLDER_PATH")
folder_path = base_folder_path.format(collection=collection)

output_path = os.getenv("OUTPUT_PATH")

# Output CSVs for missing and found barcodes
today = datetime.today().strftime('%Y%m%d')
output_missing_csv = f'{output_path}/{collection}_{today}_barcodesMissingFromDB.csv'
output_found_csv = f'{output_path}/{collection}_{today}_foundBarcodesWithSource.csv'

# Initialize lists to hold missing barcodes and all barcodes with their metadata
all_missing_barcodes = []
found_barcodes_with_source = []

# Function to check barcodes in a database
def check_barcodes_in_db(barcodes, db_path, batch_size=900):
    # Ensure all barcodes are strings and remove any extra spaces
    barcodes = [str(bc).strip() for bc in barcodes]

    # Connect to the database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Detect correct column name (barcode vs barcodes)
    cursor.execute("PRAGMA table_info(table1)")
    cols = [row[1] for row in cursor.fetchall()]
    if "barcodes" in cols:
        column_name = "barcodes"
    elif "barcode" in cols:
        column_name = "barcode"
    else:
        raise ValueError("Neither 'barcode' nor 'barcodes' column found in table1")

    results = []  # To store all results

    # Process barcodes in batches
    for i in range(0, len(barcodes), batch_size):
        batch = barcodes[i:i + batch_size]

        # Dynamically build the query for the current batch size
        query = "SELECT * FROM table1 WHERE " + " OR ".join([f"TRIM({column_name}) LIKE ?" for _ in batch])

        # Prepare the placeholders for the LIKE query with wildcards
        placeholders = [f"%{bc}%" for bc in batch]  # Add wildcards for LIKE

        try:
            # Execute the query for the current batch
            cursor.execute(query, placeholders)
            results.extend(cursor.fetchall())  # Collect results from this batch
        except sqlite3.Error as e:
            print(f"Error executing batch query: {e}")

    conn.close()

    # Extract and normalize barcodes from results
    existing_barcodes = set()
    for row in results:
        # Use detected column index instead of assuming column 2
        col_index = cols.index(column_name)
        existing_barcodes.update(process_barcodes(str(row[col_index]).strip()))

    return results, existing_barcodes

# Helper function to extract barcodes properly from string-like lists
def extract_barcode(barcode_str):
    # Clean the string by removing outer brackets and quotes
    cleaned_str = barcode_str.strip("[]'\"")
    return cleaned_str

# Helper function to split comma-separated barcodes and clean them
def process_barcodes(barcode_str):
    # Split by comma, remove leading/trailing spaces and leading zeros
    return [bc.strip().lstrip('0') for bc in extract_barcode(barcode_str).split(',')]

# Loop through all CSV files in the folder and filter by date range
for filename in os.listdir(folder_path):
    if filename.endswith("_checked.csv") or filename.endswith("_checked_corrected.csv"):
        print(f"Processing file: {filename}")
        
        # Split the filename to extract the date
        parts = filename.split('_')
        
        # Check if the date part is correctly formatted and is the third part
        if len(parts) > 3:
            date_str = parts[2]  # This assumes the date is in the third position
            print(f"Extracted date: {date_str}")
            
            # Check if the date falls within the specified range
            if start_date <= date_str <= end_date:
                print(f"File {filename} is within the date range.")
                csv_file = os.path.join(folder_path, filename)
                
                # Detect delimiter by reading the first row of the file
                with open(csv_file, 'r') as file:
                    first_line = file.readline()
                    if ';' in first_line:
                        delimiter = ';'
                    else:
                        delimiter = ','
                
                print(f"Detected delimiter: {delimiter} for {filename}")
                
                # Read the CSV file with the detected delimiter
                df = pd.read_csv(csv_file, delimiter=delimiter)

                # Ensure the DataFrame has enough columns
                if df.shape[1] <= 2:  # We need at least 3 columns
                    print(f"File {filename} doesn't have enough columns, skipping.")
                    continue

                # Extract the barcodes from the third column (index 2)
                barcodes = df.iloc[:, 2].tolist()  # Get the values in the third column (catalognumber)
                print(f"Found {len(barcodes)} barcodes in {filename}.")
                
                # Check barcodes in both databases
                results_db1, existing_barcodes_db1 = check_barcodes_in_db(barcodes, db_path1)
                results_db2, existing_barcodes_db2 = check_barcodes_in_db(barcodes, db_path2)

                # After retrieving barcodes from the database, clean and process them
                existing_barcodes = set()
                for bc in existing_barcodes_db1:
                    existing_barcodes.update(process_barcodes(str(bc).strip()))  # Properly process each barcode
                for bc in existing_barcodes_db2:
                    existing_barcodes.update(process_barcodes(str(bc).strip()))

                print(f"Existing barcodes found: {len(existing_barcodes)}")

                # Ensure that all barcodes are stripped of spaces before comparison
                barcodes_cleaned = [str(bc).strip() for bc in barcodes]
                missing_barcodes = [bc for bc in barcodes_cleaned if bc not in existing_barcodes]
                print(f"Total barcodes processed: {len(barcodes_cleaned)}")
                print(f"Missing barcodes: {len(missing_barcodes)}")

                # Add missing barcodes and their filenames to the list
                for barcode in missing_barcodes:
                    all_missing_barcodes.append({'missing_barcode': barcode, 'filename': filename})

                # Add all barcodes with their source information (ensure they are plain strings)
                for result in results_db1:
                    barcodes_in_result = process_barcodes(str(result[2]).strip())  # Process each result
                    for barcode in barcodes_in_result:
                        found_barcodes_with_source.append({'barcode': barcode, 'filename': filename, 'database': f'{database1}_jpgs'})
                
                for result in results_db2:
                    barcodes_in_result = process_barcodes(str(result[2]).strip())  # Process each result
                    for barcode in barcodes_in_result:
                        found_barcodes_with_source.append({'barcode': barcode, 'filename': filename, 'database': f'{database2}_jpgs'})      

# Output all missing barcodes to a DataFrame, then to a CSV
if all_missing_barcodes:
    missing_df = pd.DataFrame(all_missing_barcodes)
    missing_df.to_csv(output_missing_csv, index=False)
    print(f"Missing barcodes have been written to {output_missing_csv}")
else:
    print("No barcodes are missing from the database.")

# Output all found barcodes with their sources
if found_barcodes_with_source:
    found_barcodes_df = pd.DataFrame(found_barcodes_with_source)
    found_barcodes_df.to_csv(output_found_csv, index=False)
    print(f"All found barcodes with their source information have been written to {output_found_csv}")
else:
    print("No barcodes found in the database.") 
