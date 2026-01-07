# This script will take the zip files downloaded by gbifOccurrenceSearch.py 
# It will create summaries by publisher and dataset
# View and update list of publishers at the bottom

import os
import glob
import zipfile
import pandas as pd
import csv
from pygbif import registry
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# Function to fetch dataset name by key
def get_dataset_name(dataset_key):
    try:
        dataset_info = registry.datasets(uuid=dataset_key)
        return dataset_info.get("title", "Unknown Dataset")
    except Exception as e:
        print(f"Error fetching dataset name for {dataset_key}: {e}")
        return "Unknown Dataset"

def process_existing_zip_files(publishers, zip_folder_path):
    all_dataset_counts = pd.DataFrame()
    all_summaries = pd.DataFrame()
    all_duplicates = pd.DataFrame()

    for publisher in publishers:
        publisher_uuid = publisher["uuid"]
        publisher_name = publisher["name"]
        print(f"\nProcessing publisher: {publisher_name}")

        # Group zip files by publisher
        zip_files = glob.glob(os.path.join(zip_folder_path, f"{publisher_name.replace(' ', '_')}_*.zip"))
        if not zip_files:
            print(f"No ZIP files found for publisher: {publisher_name}")
            continue

        # Initialize combined df
        combined_df = pd.DataFrame()

        # Process zip files by publisher
        for zip_file in zip_files:
            print(f"Processing ZIP file: {zip_file}")
            output_folder = os.path.splitext(zip_file)[0]
            with zipfile.ZipFile(zip_file, "r") as z:
                z.extractall(output_folder)

            csv_files = glob.glob(os.path.join(output_folder, "*.csv"))
            for csv_file in csv_files:
                print(f"Reading CSV file: {csv_file}")
                
                bad_lines = []

                def bad_line_handler(line):
                    bad_lines.append(line)
                    return None

                df = pd.read_csv(
                    csv_file,
                    dtype=str,
                    sep="\t",
                    engine="python",
                    on_bad_lines=bad_line_handler,
                    quoting=csv.QUOTE_NONE,
                    escapechar="\\",
                    encoding_errors="replace",
                    usecols=["gbifID", "datasetKey", "occurrenceID", "kingdom", "phylum", "class", "order", "family", "genus",
                             "species", "infraspecificEpithet", "taxonRank", "scientificName", "publishingOrgKey",
                             "eventDate", "taxonKey", "speciesKey", "basisOfRecord", "institutionCode", "collectionCode",
                             "catalogNumber", "lastInterpreted"]
                )

                print("Rows read:", len(df))
                print("Bad rows:", len(bad_lines))

                combined_df = pd.concat([combined_df, df], ignore_index=True)

        # Create group of rows where catalogNumber is null or empty
        blank_or_null_ids = combined_df[combined_df["catalogNumber"].isnull() | (combined_df["catalogNumber"] == "")]
        # Remove duplicates from df where catalogNumber is not null or empty
        non_null_ids = combined_df[combined_df["catalogNumber"].notnull() & (combined_df["catalogNumber"] != "")]
        unique_non_null_ids = non_null_ids.drop_duplicates(
            subset=["catalogNumber", "scientificName"], 
            keep="first"
            )
        # Add rows with null or empty catalogNumber back to df after duplicates have been removed
        unique_catalogNumbers = pd.concat([unique_non_null_ids, blank_or_null_ids], ignore_index=True)

        # Save unique catalog numbers list for complete dataset
        unique_catalogNumbers.to_csv(f"{output_folder}/{publisher_name}_unique_catalogNumbers.csv", index=False)
        print(f"Unique catalogNumbers for {publisher_name} saved: {output_folder}/{publisher_name}_unique_catalogNumbers.csv")

        # Create duplicates df
        duplicates = non_null_ids[non_null_ids.duplicated(subset=["catalogNumber", "scientificName"], keep=False)]
        # Add duplicates to all_duplicates df
        if not duplicates.empty:
            duplicates = duplicates.copy()
            duplicates.to_csv(f"{output_folder}/{publisher_name}_duplicate_catalogNumbers.csv", index=False)
            print(f"Duplicate catalogNumbers for {publisher_name} saved: {output_folder}/{publisher_name}_duplicate_catalogNumbers.csv")
            duplicates.loc[:, "publisher"] = publisher_name
            all_duplicates = pd.concat([all_duplicates, duplicates], ignore_index=True)
        else:
            print(f"No duplicates found for {publisher_name}, skipping publisher assignment.")

        # Print number of duplicate rows
        print(f"Number of duplicate rows: {len(all_duplicates)}")

        # Create dataset_counts df by grouping rows by datasetKey
        dataset_counts = (
            combined_df.groupby("datasetKey")
            .size()
            .reset_index(name="totalOccurrences")
        )
        dataset_counts["publisher"] = publisher_name

        # Add dataset_counts df to combined dataset summary
        all_dataset_counts = pd.concat([all_dataset_counts, dataset_counts], ignore_index=True)
        all_dataset_counts["datasetName"] = all_dataset_counts["datasetKey"].apply(get_dataset_name)
        # Get count of duplicates by dataset
        duplicate_counts = (
            duplicates.groupby("datasetKey")
            .size()
            .reset_index(name="duplicatesCountedInOtherDatasets")
        )

        # Ensure datasetKey exists before merging
        if "datasetKey" not in all_dataset_counts.columns:
            print("Error: datasetKey column is missing in all_dataset_counts!")
            print("Current columns:", all_dataset_counts.columns)
        
        # Merge duplicate counts with dataset counts
        # If column already exists, update values instead of replacing
        if "duplicatesCountedInOtherDatasets" in all_dataset_counts.columns:
            all_dataset_counts = all_dataset_counts.merge(
                duplicate_counts, on="datasetKey", how="left", suffixes=("", "_new")
            )
            
            # If new duplicate counts exist, update the values
            all_dataset_counts["duplicatesCountedInOtherDatasets"] = (
                all_dataset_counts["duplicatesCountedInOtherDatasets"].fillna(0).astype(int) +
                all_dataset_counts["duplicatesCountedInOtherDatasets_new"].fillna(0).astype(int)
            )

            all_dataset_counts.drop(columns=["duplicatesCountedInOtherDatasets_new"], inplace=True)
        else:
            all_dataset_counts = all_dataset_counts.merge(duplicate_counts, on="datasetKey", how="left")
            all_dataset_counts["duplicatesCountedInOtherDatasets"] = all_dataset_counts["duplicatesCountedInOtherDatasets"].fillna(0).astype(int)

        # Compute unique specimen count
        all_dataset_counts["uniqueOccurrences"] = (
            all_dataset_counts["totalOccurrences"] - all_dataset_counts["duplicatesCountedInOtherDatasets"]
        ).astype(int)

        # Reorder columns in dataset summary df
        all_dataset_counts = all_dataset_counts[["publisher", "datasetName", "datasetKey", "totalOccurrences", 
                                                 "uniqueOccurrences", "duplicatesCountedInOtherDatasets"]]

        # Convert any floats to ints
        all_dataset_counts["duplicatesCountedInOtherDatasets"] = all_dataset_counts["duplicatesCountedInOtherDatasets"].fillna(0).astype(int)
        all_dataset_counts["uniqueOccurrences"] = all_dataset_counts["uniqueOccurrences"].fillna(0).astype(int)
        all_dataset_counts["totalOccurrences"] = all_dataset_counts["totalOccurrences"].fillna(0).astype(int)

        # Get total counts for pulisher level summary
        total_preserved_specimens = len(combined_df)
        total_duplicates = len(duplicates)
        unique_preserved_specimens = total_preserved_specimens - total_duplicates

        # Create publisher level summary
        summary = pd.DataFrame(
            [{
                "publisher": publisher_name, 
                "totalOccurrences": total_preserved_specimens,
                "duplicatesCountedInOtherDatasets": total_duplicates,
                "uniqueOccurrences": unique_preserved_specimens
            }]
        )

        # Add publisher level summary to combined publisher level summary
        all_summaries = pd.concat([all_summaries, summary], ignore_index=True)

    # Save all dfs to output_folder
    output_folder_path = os.getenv("OUTPUT_FOLDER_PATH")
    os.makedirs(output_folder_path, exist_ok=True)

    dataset_counts_file = os.path.join(output_folder_path, "all_publishers_dataset_counts.csv")
    summary_file = os.path.join(output_folder_path, "all_publishers_summary.csv")
    duplicates_file = os.path.join(output_folder_path, "duplicate_occurrences.csv")

    all_dataset_counts.to_csv(dataset_counts_file, index=False)
    all_summaries.to_csv(summary_file, index=False)
    all_duplicates.to_csv(duplicates_file, index=False)

    print(f"\nConsolidated dataset counts saved: {dataset_counts_file}")
    print(f"Publisher-level summary saved: {summary_file}")
    print(f"Duplicate occurrences saved: {duplicates_file}")

    return all_dataset_counts, all_summaries, all_duplicates
    
publishers = [
    {"uuid": "2e7df380-8356-4533-bcb3-5459e23c794e", "name": "Natural History Museum of Denmark"},
    {"uuid": "ba482b53-07ed-4ca4-8981-5396d1a8a6fc", "name": "Botanical Garden & Museum, Natural History Museum of Denmark"},
    {"uuid": "760d5f24-4c04-40da-9646-1b2c935da502", "name": "Natural History Museum Aarhus"},
    {"uuid": "8e1a97a0-3ca8-11d9-8439-b8a03c50a862", "name": "Herbarium of the University of Aarhus"}
]

zip_folder_path = os.getenv("ZIP_FOLDER_PATH")

all_dataset_counts, all_summaries, all_duplicates = process_existing_zip_files(publishers, zip_folder_path)