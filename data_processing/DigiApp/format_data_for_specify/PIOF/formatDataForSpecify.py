# This script processes CSV files from a PIOF pipeline, reformats them for Specify, and archives the original files.

import pandas as pd
import os
import re
import numpy as np
import shutil
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# Directory paths should be defined in the .env file
base_folder_path = os.getenv("FOLDER_PATH")
base_archive_folder = os.getenv("ARCHIVE_FOLDER")
base_output_folder = os.getenv("OUTPUT_FOLDER")
base_log_file_path = os.getenv("LOG_FILE_PATH")

# In directory paths, {collection} is replaced with the collection name
collection = os.getenv("COLLECTION")
folder_path = base_folder_path.format(collection=collection)
archive_folder = base_archive_folder.format(collection=collection)
output_folder = base_output_folder.format(collection=collection)
log_file_path = base_log_file_path.format(collection=collection)

# Ensure the log file directory exists
os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
# Ensure the archive folder exists
os.makedirs(archive_folder, exist_ok=True)

# Parse taxonfullname into genus, species, and subspecies based on rankid
def parse_taxon_name(row):
    result = {'genus': '', 'species': '', 'subspecies': ''}

    fullname = str(row.get('taxonfullname', '')).strip()
    rankid = row.get('rankid')

    if not fullname or fullname.lower() == 'nan':
        return pd.Series(result)

    parts = fullname.split()
    result['genus'] = parts[0]

    # Family
    if rankid == 140:
        result['genus'] = ''
        return pd.Series(result)

    # Genus
    if rankid == 180:
        return pd.Series(result)

    def _norm(tok):
        return tok.replace('×', 'x').lower().rstrip('.')
    
    def _collect_hybrid_epithet(start_idx):
        """
        Collects tokens for a single epithet:
        - 'name'
        - 'x name'
        - 'name x name'
        Stops after completing one epithet.
        """
        if start_idx >= len(parts):
            return []

        zone = []

        # leading hybrid: x name
        if parts[start_idx].lower() == 'x' and start_idx + 1 < len(parts):
            return parts[start_idx:start_idx + 2]

        zone.append(parts[start_idx])

        # internal hybrid: name x name
        if start_idx + 2 < len(parts) and parts[start_idx + 1].lower() == 'x':
            zone.extend(parts[start_idx + 1:start_idx + 3])

        return zone

    # ---------- species ----------
    if rankid >= 220 and len(parts) > 1:
        species_zone = _collect_hybrid_epithet(1)
        result['species'] = ' '.join(species_zone)

    # ---------- subspecies ----------
    if rankid == 230:
        subsp_start = 1 + len(species_zone)

        # stop if next token looks like an author
        if subsp_start < len(parts) and not parts[subsp_start][0].isupper():
            subspecies_zone = _collect_hybrid_epithet(subsp_start)
            result['subspecies'] = ' '.join(subspecies_zone)

    return pd.Series(result)

# Add author, taxon number, and taxon number source to appropriate columns
def assign_taxon_metadata(row):
    result = {
        'genus_author': '',
        'species_author': '',
        'subspecies_author': '',
        'genus_taxonnumber': '',
        'species_taxonnumber': '',
        'subspecies_taxonnumber': '',
        'genus_taxonnrsource': '',
        'species_taxonnrsource': '',
        'subspecies_taxonnrsource': ''
    }

    rankid = row.get('rankid')

    if rankid == 180:
        result['genus_author'] = row.get('taxonauthor', '')
        result['genus_taxonnumber'] = row.get('taxonnumber', '')
        result['genus_taxonnrsource'] = row.get('taxonnrsource', '')

    elif rankid == 220:
        result['species_author'] = row.get('taxonauthor', '')
        result['species_taxonnumber'] = row.get('taxonnumber', '')
        result['species_taxonnrsource'] = row.get('taxonnrsource', '')

    elif rankid == 230:
        result['subspecies_author'] = row.get('taxonauthor', '')
        result['subspecies_taxonnumber'] = row.get('taxonnumber', '')
        result['subspecies_taxonnrsource'] = row.get('taxonnrsource', '')

    return pd.Series(result)

# Add new genus, species, and subspecies flags based on taxonspid or taxonomyuncertain
def set_new_flags(row):
    value = row['taxonspid']
    # Normalize value to string, strip whitespace, and check for known "empty" representations
    cleaned_value = str(value).strip()
    taxonspid_missing_or_zero = (
        pd.isnull(value) or cleaned_value in {'', '0', 'None'}
    )

    taxonomy_uncertain = str(row.get('taxonomyuncertain', '')).strip().lower() == 'true'

    newgenusflag = newspeciesflag = newsubspeciesflag = ''

    if taxonspid_missing_or_zero:
        if row['rankid'] == 180:
            newgenusflag = 'True'
        elif row['rankid'] == 220:
            newspeciesflag = 'True'
        elif row['rankid'] == 230:
            newsubspeciesflag = 'True'

    return pd.Series([newgenusflag, newspeciesflag, newsubspeciesflag])

# Loop through each CSV file in the specified folder_path
for filename in os.listdir(folder_path):
    # Check if the file is a CSV file
    if filename.endswith('.csv'):
        # Read the CSV file with semicolon delimiter
        file_path = os.path.join(folder_path, filename)
        try:
            # First try reading with semicolon
            df = pd.read_csv(file_path, delimiter=';')
            # Optional sanity check: make sure it's not just one column
            if df.shape[1] == 1:
                raise ValueError("Only one column detected — probably wrong delimiter.")
        except Exception as e:
            # Fallback to comma
            print(f"Semicolon read failed or only one column detected: {e}")
            df = pd.read_csv(file_path, delimiter=',')
        # Strip any whitespace or trailing commas from column names
        df.columns = df.columns.str.strip().str.replace(',', '')
        print(df.head())

        # Confirm that numeric columns are Int64
        int_columns = ['catalognumber', 'taxonnameid', 'taxonspid', 'rankid', 'taxonnumber']
        for column in int_columns:
            if column in df.columns:
                df[column] = pd.to_numeric(df[column], errors='coerce').fillna(pd.NA).astype('Int64')

        print(df.info())

        # Modify the filename to replace 'checked' or 'checked_corrected' with 'processed.tsv'
        updated_filename = re.sub(r'checked(_corrected)?\.csv$', 'processed.tsv', filename)

        # --- Validate rankid before processing ---
        missing_rankid = df['rankid'].isna()

        if missing_rankid.any():
            bad_rows = df.loc[missing_rankid, ['taxonfullname', 'rankid']]
            row_numbers = (bad_rows.index + 2).tolist()  # +2 for Excel row numbers incl. header

            raise ValueError(
                "ERROR: One or more rows are missing a required 'rankid' value.\n"
                f"Please correct the spreadsheet and re-run the script.\n"
                f"Source file: {filename}\n"
                f"Affected Excel row(s): {row_numbers}\n"
                f"Example rows:\n{bad_rows.head(5)}"
            )
        
        # Assign taxonomic fields from 'taxonfullname' to 'genus', 'species', etc.
        df[['genus', 'species', 'subspecies']] = df.apply(parse_taxon_name, axis=1)


        df[[
            'genus_author', 'species_author', 'subspecies_author', 
            'genus_taxonnumber', 'species_taxonnumber', 'subspecies_taxonnumber',
            'genus_taxonnrsource', 'species_taxonnrsource', 'subspecies_taxonnrsource'
        ]] = df.apply(assign_taxon_metadata, axis=1)

        # Add new taxa flags as appropriate
        df[['newgenusflag', 'newspeciesflag', 'newsubspeciesflag']] = df.apply(set_new_flags, axis=1)

        # Rename some of the columns
        df = df.rename(columns={
            'familyname': 'family',
            'georegionname': 'broadgeographicalregion',
            'agentfirstname': 'cataloger_firstname',
            'agentmiddleinitial': 'cataloger_middle',
            'agentlastname': 'cataloger_lastname',
            'notes': 'remarks'
        })

        # Replace the string 'None' with an empty string in cataloger_middle
        df['cataloger_middle'] = df['cataloger_middle'].replace('None', '')

        # Create 'localityname' as a copy of 'broadgeographicalregion'
        df['localityname'] = df['broadgeographicalregion']

        # Convert recorddatetime to datetime
        df['recorddatetime'] = pd.to_datetime(
            df['recorddatetime'], utc=True, errors='coerce'
        )

        # Extract dates
        df['catalogeddate'] = df['recorddatetime'].dt.date
        df['datafile_date'] = df['recorddatetime'].dt.date

        # Add new columns with standard values
        df['project'] = 'DaSSCo'
        df['publish'] = 'True'
        df['count'] = 1
        df['storedunder'] = 'True'
        df['datafile_source'] = 'DaSSCo data file'

        # Add a column with the updated filename
        df['datafile_remark'] = updated_filename

        # Convert labelobscured and specimenobscured to boolean
        df['labelobscured'] = df['labelobscured'].astype(str).str.lower().map({'true': True, 'false': False})
        df['specimenobscured'] = df['specimenobscured'].astype(str).str.lower().map({'true': True, 'false': False})

        # Fill remark, source, and date in cases where 'obscured' values are True
        # Label obscured
        df['labelobscured_remark'] = 'Label obscured'
        df['labelobscured_remark'] = df['labelobscured_remark'].where(df['labelobscured'])
        df['labelobscured_source'] = 'DaSSCo digitisation'
        df['labelobscured_source'] = df['labelobscured_source'].where(df['labelobscured'])
        df['labelobscured_date'] = df['catalogeddate']
        df['labelobscured_date'] = df['labelobscured_date'].where(df['labelobscured'])
        # Specimen obscured
        df['specimenobscured_remark'] = 'Specimen obscured'
        df['specimenobscured_remark'] = df['specimenobscured_remark'].where(df['specimenobscured'])
        df['specimenobscured_source'] = 'DaSSCo digitisation'
        df['specimenobscured_source'] = df['specimenobscured_source'].where(df['specimenobscured'])
        df['specimenobscured_date'] = df['catalogeddate']
        df['specimenobscured_date'] = df['specimenobscured_date'].where(df['specimenobscured'])

        # Create new columns for remark date and source
        df['remark_date'] = df['catalogeddate']
        df['remark_date'] = df['remark_date'].where(df['remarks'].notna() & (df['remarks'] != ''))
        df['remark_source'] = 'DaSSCo digitisation'
        df['remark_source'] = df['remark_source'].where(df['remarks'].notna() & (df['remarks'] != ''))

        # Specify the order of columns for the final tsv file
        column_order = [
            'catalognumber', 'catalogeddate', 'cataloger_firstname', 'cataloger_middle', 'cataloger_lastname',
            'project', 'objectcondition', 'specimenobscured', 'specimenobscured_remark', 'specimenobscured_source',
            'specimenobscured_date', 'labelobscured', 'labelobscured_remark', 'labelobscured_source', 'labelobscured_date',
            'publish', 'containername', 'containertype', 'remarks', 'remark_date', 'remark_source', 'family', 'genus',
            'genus_author', 'genus_taxonnumber', 'genus_taxonnrsource', 'newgenusflag', 'species', 'species_author',
            'species_taxonnumber', 'species_taxonnrsource', 'newspeciesflag', 'subspecies', 'subspecies_author',
            'subspecies_taxonnumber', 'subspecies_taxonnrsource', 'newsubspeciesflag', 'typestatusname', 'storedunder',
            'localityname', 'broadgeographicalregion', 'localitynotes', 'preptypename', 'count', 'datafile_remark',
            'datafile_source', 'datafile_date'
        ]

        # Reorder columns
        df = df[column_order]

        # Write the df to a new CSV file in the output folder with the updated filename
        output_file_path = os.path.join(output_folder, updated_filename)
        df.to_csv(output_file_path, sep='\t', encoding='utf-8', index=False)
        
        processed_file_path = os.path.join(archive_folder, filename)
        print(f"Moving file to: {processed_file_path}")
        shutil.move(file_path, processed_file_path)

        # Log the processing
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(log_file_path, 'a') as log_file:
            log_file.write(f"{timestamp} - {filename} processed and moved to {archive_folder}\n")
            log_file.write(f"{timestamp} - {updated_filename} ready for import to Specify\n")

        