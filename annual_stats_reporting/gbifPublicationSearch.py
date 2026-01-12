import pandas as pd
import requests
import time
import os
from dotenv import load_dotenv
import re

# Load environment variables
load_dotenv()

# Get output folder path from .env
output_folder_path = os.getenv("output_folder_path", "./")
year = os.getenv("YEAR", "2025") # Default to 2024 if not set in .env

def fetch_literature(dataset_key):
    all_results = []
    offset = 0  # Allows for pagination in search results
    limit = 300  # Maximum limit allowed by GBIF API
    
    while True:
        url = f"https://api.gbif.org/v1/literature/search?gbifDatasetKey={dataset_key}&year={year}&limit={limit}&offset={offset}"
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            results = data.get("results", [])

            print(f"Fetched {len(results)} results for datasetKey {dataset_key}, offset {offset}")  # Debugging line

            if not results:
                break  # No more results, exit loop
            
            all_results.extend(results)  # Append all results
            offset += limit  # Move to the next page

            time.sleep(1)  # Avoid hitting API rate limits

        else:
            print(f"Failed to fetch data for {dataset_key}: {response.status_code}")
            break

    print(f"Total results fetched for {dataset_key}: {len(all_results)}")  # Debugging line
    return all_results

def main(input_file, output_file):
    # Read dataset keys from spreadsheet
    input_df = pd.read_csv(input_file)
    dataset_keys = input_df["datasetKey"].dropna().unique()
    
    all_results = []
    
    # Pull data for each datasetKey
    for dataset_key in dataset_keys:
        print(f"Fetching literature for datasetKey: {dataset_key}")
        publications = fetch_literature(dataset_key)
        for pub in publications:
            all_results.append({
                "datasetKey": dataset_key,  # Ensure datasetKey is correctly assigned
                "title": pub.get("title", "N/A"),
                "authors": pub.get("authors", "N/A"),
                "source": pub.get("source", "N/A"),
                "year": pub.get("year", "N/A"),
                "published": pub.get("published", "N/A"),
                "doi": pub.get("doi", "N/A"),
                "websites": pub.get("websites", "N/A"),
                "abstract": pub.get("abstract", "N/A"),
                "publisher": pub.get("publisher", "N/A"),
                "publishing_country": pub.get("publishingCountry", "N/A"),
                "open_access": pub.get("openAccess", "N/A"),
                "peer_review": pub.get("peerReview", "N/A"),
                "citation_type": pub.get("citationType", "N/A"),
                "countries_of_coverage": pub.get("countriesOfCoverage", "N/A"),
                "countries_of_researcher": pub.get("countriesOfResearcher", "N/A"),
                "keywords": pub.get("keywords", "N/A"),
                "literature_type": pub.get("literatureType", "N/A"),
                "identifiers": pub.get("identifiers", "N/A"),
                "id": pub.get("id", "N/A"),
                "topics": pub.get("topics", "N/A"),
                "gbif_download_key": pub.get("gbifDownloadKey", "N/A")
            })
    
    # Convert to DataFrame
    results_df = pd.DataFrame(all_results)

    results_df.to_csv(os.path.join(output_folder_path, "all_results.csv"), index=False)  # Save raw data to csv
    print(f"Saved raw data to {output_folder_path}/all_results.csv")

    # Add publishingInstitution and datasetName to results_df
    input_df_renamed = input_df[["datasetKey", "publisher", "datasetName"]].rename(columns={"publisher": "datasetPublisher"})
    merged_df = pd.merge(results_df, input_df_renamed, on='datasetKey', how='left')

    # Obtain count of unique publications by dataset
    unique_publications_by_dataset = (
        merged_df
        .groupby(['datasetPublisher', 'datasetName', 'datasetKey'])['title']
        .nunique()
        .reset_index()
        .rename(columns={'title': 'unique_publications_count'})
    )

    # Obtain total count of unique publications
    unique_publications_count = merged_df['title'].nunique()
    print(f"Total count of unique publications: {unique_publications_count}")

    # Create subsets of data by datasetPublisher
    publisher_groups = merged_df.groupby('datasetPublisher')

    # Obtain count of unique publications by datasetPublisher
    unique_publications_by_publisher = publisher_groups['title'].nunique().reset_index()
    unique_publications_by_publisher.columns = ['datasetPublisher', 'unique_publications_count']

    # Save subsets to separate sheets in same excel file; counts should also be saved in separate sheet
    with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
        merged_df.to_excel(writer, sheet_name='All Publications', index=False)

        # counts by publisher
        unique_publications_by_publisher.to_excel(
            writer,
            sheet_name='Unique Count by Publisher',
            index=False
        )

        # counts by dataset
        unique_publications_by_dataset.to_excel(
            writer,
            sheet_name='Unique Count by Dataset',
            index=False
        )
        
        for publisher, group in publisher_groups:
            # Get unique publications only
            unique_titles = group[['title']].drop_duplicates()
            # Sanitize sheet name (Excel limits: max 31 chars, no special chars like [\/*?:])
            sanitized_name = re.sub(r'[\\/*?:\[\]]', '_', str(publisher))[:31]
            # Save unique publications per publisher
            unique_titles.to_excel(writer, sheet_name=sanitized_name, index=False)

    print(f"Saved results to {output_file}")

if __name__ == "__main__":
    input_file = os.path.join(output_folder_path, "all_publishers_dataset_counts.csv") # Construct input file path
    output_file = os.path.join(output_folder_path, "gbif_publications.xlsx")  # Construct output file path
    main(input_file, output_file)
