# This script will pull occurrence data for all listed publishers up to a specified max date
# To obtain a publisher's UUID, run publisherUUID.py 
# See .env file for variables that need updating before this script will run

from pygbif import occurrences
import time
import requests
import pandas as pd
import os
import json
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# The following variables should be defined in the .env file
gbif_user = os.getenv("GBIF_USER")
gbif_password = os.getenv("GBIF_PASSWORD")
gbif_email = os.getenv("GBIF_EMAIL")

max_date = os.getenv("MAX_DATE")
zip_folder_path = os.getenv("ZIP_FOLDER_PATH")

# List of publisher UUIDs and names
publishers = [
    {"uuid": "2e7df380-8356-4533-bcb3-5459e23c794e", "name": "Natural History Museum of Denmark"},
    {"uuid": "ba482b53-07ed-4ca4-8981-5396d1a8a6fc", "name": "Botanical Garden & Museum, Natural History Museum of Denmark"},
    {"uuid": "760d5f24-4c04-40da-9646-1b2c935da502", "name": "Natural History Museum Aarhus"},
    {"uuid": "8e1a97a0-3ca8-11d9-8439-b8a03c50a862", "name": "Herbarium of the University of Aarhus"}
]

# Ensure the output folder exists
if not os.path.exists(zip_folder_path):
    os.makedirs(zip_folder_path)

# Function to request and download data
def download_gbif_data(publisher_uuid, publisher_name):
    # Create the download query as a tuple
    query = (
        f"publishingOrg = {publisher_uuid}",
        "basisOfRecord = PRESERVED_SPECIMEN",
        "occurrenceStatus = PRESENT",
    )
    
    # Request the download
    print(f"Requesting data for {publisher_name} ({publisher_uuid})...")
    result = occurrences.download(query, user=gbif_user, pwd=gbif_password, email=gbif_email)
    
    # Extract the download key (first element of the tuple)
    download_key = result[0]
    print(f"Download key: {download_key}")
    
    # Wait for the download to complete
    download_status = occurrences.download_meta(download_key)["status"]
    while download_status != "SUCCEEDED":
        print(f"Status: {download_status} (waiting 30 seconds)")
        time.sleep(30)
        download_status = occurrences.download_meta(download_key)["status"]
    
    # Download the ZIP file
    download_url = f"https://api.gbif.org/v1/occurrence/download/request/{download_key}"
    response = requests.get(download_url)
    zip_file = f"{zip_folder_path}{publisher_name.replace(' ', '_')}_download.zip"
    
    with open(zip_file, "wb") as file:
        file.write(response.content)
    
    print(f"Download completed for {publisher_name} ({publisher_uuid}). File saved as {zip_file}")
    return zip_file

# Download data for each publisher
for publisher in publishers:
    zip_file = download_gbif_data(publisher["uuid"], publisher["name"])
    print(f"Download ready: {zip_file}")