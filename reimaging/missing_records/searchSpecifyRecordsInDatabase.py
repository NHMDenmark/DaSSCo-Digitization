# This script reads barcodes from a specified CSV file and checks for their presence in two specified SQLite databases.
# It outputs a CSV file listing any barcodes found in the databases that are not present in the CSV, along with all their associated metadata.

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

input_csv = os.getenv("INPUT_CSV")

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
def process_barcodes(barcode_str):
    if not barcode_str:
        return []

    # Remove brackets
    cleaned = barcode_str.replace('[', '').replace(']', '')

    # Split in case of multiple barcodes
    parts = cleaned.split(',')

    # Clean each barcode
    return [
        p.strip().replace('"', '').replace("'", '').lstrip('0')
        for p in parts
        if p.strip()
    ]

def find_db_rows_not_in_csv(db_path, csv_barcodes_set):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get column info
    cursor.execute("PRAGMA table_info(table1)")
    cols_info = cursor.fetchall()
    col_names = [col[1] for col in cols_info]

    if "barcodes" in col_names:
        column_name = "barcodes"
    elif "barcode" in col_names:
        column_name = "barcode"
    else:
        raise ValueError("Neither 'barcode' nor 'barcodes' column found in table1")

    barcode_col_index = col_names.index(column_name)

    cursor.execute(f"SELECT * FROM table1")

    rows_not_in_csv = []

    for row in cursor:
        raw_barcode_field = str(row[barcode_col_index]).strip()
        db_barcodes = process_barcodes(raw_barcode_field)

        for bc in db_barcodes:
            if bc not in csv_barcodes_set:
                # Convert row to dict so we retain all columns
                row_dict = dict(zip(col_names, row))

                # Add the specific barcode that triggered this
                row_dict["unmatched_barcode"] = bc

                rows_not_in_csv.append(row_dict)

    conn.close()
    return rows_not_in_csv

all_csv_barcodes = set()

print(f"Reading CSV file: {input_csv}")

# Detect delimiter
with open(input_csv, 'r') as file:
    first_line = file.readline()
    delimiter = ';' if ';' in first_line else ','

print(f"Detected delimiter: {delimiter}")

# Read CSV
df = pd.read_csv(input_csv, delimiter=delimiter)

# Ensure enough columns
if df.shape[1] <= 2:
    raise ValueError(f"{input_csv} does not have at least 3 columns")

# Extract barcodes (3rd column)
if 'Catalog Number' in df.columns:
    barcodes = df['Catalog Number']
else:
    barcodes = df.iloc[:, 0]  # Assuming barcodes are in the first column if 'Catalog Number' is not found

# Normalize + store in set
all_csv_barcodes = set(
    str(bc).strip().lstrip('0') for bc in barcodes if pd.notna(bc)
)

print(f"Total CSV barcodes: {len(all_csv_barcodes)}")

# Get all DB barcodes
db1_rows = find_db_rows_not_in_csv(db_path1, all_csv_barcodes)
db2_rows = find_db_rows_not_in_csv(db_path2, all_csv_barcodes)

all_rows = db1_rows + db2_rows

print(f"Total unmatched DB rows: {len(all_rows)}")

# Output to csv
output_db_not_in_csv = f'{output_path}/{collection}_{today}_barcodesInDB_NotInCSV.csv'

if all_rows:
    df = pd.DataFrame(all_rows)
    df.to_csv(output_db_not_in_csv, index=False)
    print(f"Written to {output_db_not_in_csv}")
else:
    print("No unmatched DB rows found.")
