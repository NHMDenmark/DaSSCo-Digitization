## This folder contains scripts for obtaining annual GBIF stats for reporting purposes

A GBIF user account is required to download data using these scripts. If you don't already have one, you can obtain an account at [GBIF.org](https://www.gbif.org/).

### GBIF Occurrence Search

The [gbifOccurrenceSearch.py](https://github.com/beckerah/dassco_scripts/blob/main/annual_stats_reporting/gbifOccurrenceSearch.py) script searches for occurrence records in GBIF published by specified institutions. It makes use of GBIF's asynchronous download service.

To compile the list of occurrences by institution, you'll need the UUID for each publisher. Some institutions are listed as multiple publishers, for example NHMD publishes as both *Natural History Museum of Denmark* and *Botanical Garden & Museum, Natural History Museum of Denmark*. If you don't know a publisher's UUID, you can obtain it using the [publisherUUID.py](https://github.com/beckerah/dassco_scripts/blob/main/annual_stats_reporting/publisherUUID.py) script.

Once you have the UUID for each publisher, you should add these to the List of publisher UUIDs and names in both the [gbifOccurrenceSearch.py](https://github.com/beckerah/dassco_scripts/blob/main/annual_stats_reporting/gbifOccurrenceSearch.py) and [occurrenceProcessing.py](https://github.com/beckerah/dassco_scripts/blob/main/annual_stats_reporting/occurrenceProcessing.py) scripts. 

You'll then need to fill in the variables in the .env file:
- GBIF_USER is your the username associated with your GBIF account
- GBIF_PASSWORD is the password to log into your GBIF account
- GBIF_EMAIL is the email address associated with your GBIF account
- ZIP_FOLDER_PATH is the folder the downloaded zip files from GBIF will be saved in
- OUTPUT_FOLDER_PATH is the folder all of the processed data will be saved in

As written, the [gbifOccurrenceSearch.py](https://github.com/beckerah/dassco_scripts/blob/main/annual_stats_reporting/gbifOccurrenceSearch.py) script searches for occurrences by publisher, where the basis of record is Preserved Specimen and the occurrence status is Present. We don't include a date filter because these are unreliable in the data.

    query = (
            f"publishingOrg = {publisher_uuid}",
            "basisOfRecord = PRESERVED_SPECIMEN",
            "occurrenceStatus = PRESENT",
    )

For each publisher, a zip folder containing a CSV file listing all the search results, is downloaded from GBIF. 

### Processing Script for Occurrence Data

The [occurrenceProcessing.py](https://github.com/beckerah/dassco_scripts/blob/main/annual_stats_reporting/occurrenceProcessing.py) script takes the output from the [gbifOccurrenceSearch.py](https://github.com/beckerah/dassco_scripts/blob/main/annual_stats_reporting/gbifOccurrenceSearch.py) script and creates publisher-level and dataset-level summaries of total and unique counts.

Once you have the zip files, run the [occurrenceProcessing.py](https://github.com/beckerah/dassco_scripts/blob/main/annual_stats_reporting/occurrenceProcessing.py) script to obtain the summaries in CSV format. Three spreadhseets are the output of this script:
- all_publishers_dataset_counts.csv - This includes occurrence counts for each dataset, grouped by publisher
- all_publishers_summary.csv - This includes occurrence counts for each publisher
- duplicate_occurrences.csv - This is a complete list of all duplicate occurrences

All unique rows are also saved to CSV files for each publisher, in case that data needs to be cross-referenced. 

### GBIF Publication Search

The [gbifPublicationSearch.py](https://github.com/beckerah/dassco_scripts/blob/main/annual_stats_reporting/gbifPublicationSearch.py) script takes the dataset keys from the all_publishers_dataset_counts.csv file created by the processing script above, and searches for publications in gbif that reference those datasets. 

It is assumed that you only want to search for publications published within a calendar year, so you must set this year as the YEAR variable in the .env file. It will also use the gbif credentials and output_folder_path specified in the .env file for the GBIF Occurrence Search.

The results of this script are saved in the gbif_Publications.xlsx spreadsheet, which has multiple sheets:
- *All Publications* - This lists all the data downloaded from GBIF for each publication
- *Unique Publications Count* - This shows a count of unique publications by publishing institution
- One sheet for each publishing institution - Each of these is a list of titles referencing datasets published by the named institution