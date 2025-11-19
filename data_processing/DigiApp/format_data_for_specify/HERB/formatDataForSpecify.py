# This script processes CSV files from a HERB pipeline, reformats them for Specify, and archives the original files.

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

# Separate the taxonomic fields based on rank_terms found in 'taxonfullname' and extract qualifiers

import re
import pandas as pd

def parse_taxonfullname(row):
    fullname = str(row.get('taxonfullname', '')).strip()
    rankid = row.get('rankid', None)

    result = {'genus': '', 'species': '', 'subspecies': '', 'variety': '', 'forma': ''}

    if not fullname:
        return pd.Series(result)

    parts = fullname.split()
    result['genus'] = parts[0]  # always genus (unless only family)

    # Remove genus if rankid == 140 (Family)
    if rankid == 140:
        result['genus'] = ''

    # If rankid=180, only genus is used
    if rankid == 180:
        return pd.Series(result)

    # helper: normalized token for comparisons (treat '×' as 'x', remove trailing dot)
    def _norm(tok):
        return tok.replace('×', 'x').lower().rstrip('.')

    # helper: return tokens from start index until next rank marker or an uppercase token (likely author)
    def _collect_zone(start_idx):
        zone = []
        i = start_idx
        while i < len(parts):
            n = _norm(parts[i])
            # stop if we hit an infraspecific rank marker
            if n in ('subsp', 'ssp', 'var', 'forma', 'f'):
                break
            # stop at an author-like token (starts with uppercase or open parenthesis)
            if parts[i][0].isupper() or parts[i][0] == '(':
                break
            zone.append(parts[i])
            i += 1
        return zone

    # helper: format an epithet zone into the desired value (handles hybrids)
    def _format_epithet(zone):
        if not zone:
            return ''
        # leading hybrid marker: 'x brucheri'
        if _norm(zone[0]) == 'x':
            return zone[0] + (' ' + zone[1] if len(zone) > 1 else '')
        # hybrid inside zone: 'danica x officinalis' or 'arcuata x vulgaris'
        if any(_norm(t) == 'x' for t in zone):
            return ' '.join(zone)
        # otherwise, just first epithet
        return zone[0]

    # -------- species: extract for every rankid >= 220 ----------
    if rankid is not None and rankid >= 220:
        species_zone = _collect_zone(1)
        result['species'] = _format_epithet(species_zone)

    # -------- infraspecifics according to rankid ----------
    if rankid == 230:  # subspecies
        for i, t in enumerate(parts):
            if _norm(t) in ('subsp', 'ssp'):
                subs_zone = _collect_zone(i + 1)
                result['subspecies'] = _format_epithet(subs_zone)
                break

    elif rankid == 240:  # variety
        for i, t in enumerate(parts):
            if _norm(t) == 'var':
                var_zone = _collect_zone(i + 1)
                result['variety'] = _format_epithet(var_zone)
                # result['variety'] = var_zone[0] if var_zone else ''
                break

    elif rankid == 260:  # forma
        for i, t in enumerate(parts):
            if _norm(t) in ('forma', 'f'):
                f_zone = _collect_zone(i + 1)
                result['forma'] = f_zone[0] if f_zone else ''
                break

    return pd.Series(result)

import pandas as pd
import re

def assign_ishybrid_fields(df):
    for col in ['species', 'subspecies', 'variety', 'forma']:
        hybrid_col = f'ishybrid_{col}'
        df[hybrid_col] = pd.Series([pd.NA] * len(df), dtype='boolean')

        # Detect hybrids:
        # 1. Starts with 'x ' or '× '
        # 2. Contains ' x ' or ' × ' in the middle
        pattern = r'^(?:x|×)\s|\sx\s|\s×\s'
        is_hybrid = df[col].notna() & df[col].str.contains(pattern, flags=re.IGNORECASE, na=False)

        # True for hybrids
        df.loc[is_hybrid, hybrid_col] = True

        # False for non-empty but not hybrid
        df.loc[
            df[col].notna() &
            (df[col].str.strip() != "") &
            (df[hybrid_col].isna()),
            hybrid_col
        ] = False

    return df

# Add new taxonomy flags based on taxonspid or taxonomyuncertain
def set_new_flags(row):
    value = row['taxonspid']
    # Normalize value to string, strip whitespace, and check for known "empty" representations
    cleaned_value = str(value).strip()
    taxonspid_missing_or_zero = (
        pd.isnull(value) or cleaned_value in {'', '0', 'None'}
    )

    taxonomy_uncertain = str(row.get('taxonomyuncertain', '')).strip().lower() == 'true'

    newgenusflag = newspeciesflag = newsubspeciesflag = newvarietyflag = newformaflag = ''

    if taxonspid_missing_or_zero or taxonomy_uncertain:
        if row['rankid'] == 180:
            newgenusflag = 'True'
        elif row['rankid'] == 220:
            newspeciesflag = 'True'
        elif row['rankid'] == 230:
            newsubspeciesflag = 'True'
        elif row['rankid'] == 240:
            newvarietyflag = 'True'
        elif row['rankid'] == 260:
            newformaflag = 'True'

    return pd.Series([newgenusflag, newspeciesflag, newsubspeciesflag, newvarietyflag, newformaflag])

# Parse out storage information from 'storagefullname'
def split_storage_info(row):
    parts = str(row['storagefullname']).split(' | ')
    
    # Initialize output fields
    collection = cabinet = shelf = box = ''
    
    if len(parts) >= 3:
        collection = parts[2].strip()

        for part in parts[3:]:  # look at the remaining parts for cabinet/shelf/box
            part = part.strip()
            if part.lower().startswith('cabinet '):
                cabinet = part.split(' ', 1)[1].strip()
            elif part.lower().startswith('shelf '):
                shelf = part.split(' ', 1)[1].strip()
            elif part.lower().startswith('box '):
                box = part.split(' ', 1)[1].strip()

    return pd.Series([collection, cabinet, shelf, box])

def extract_phrases_from_notes(df):
    # Ensure 'notes' is string type
    df['notes'] = df['notes'].fillna('').astype(str)

    df['addendum'] = df['notes'].apply(
        lambda x: 'sensu lato' if 'sensu lato' in x else (
            'sensu stricto' if 'sensu stricto' in x else ''
        )
    )
    
    # For sensu phrases
    for phrase in ['sensu lato', 'sensu stricto']:
        df['notes'] = df['notes'].str.replace(phrase, '', regex=False)

    # Clean up whitespace
    df['notes'] = df['notes'].str.strip()

    return df

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
        int_columns = ['catalognumber', 'taxonnameid', 'taxonspid', 'rankid']
        for column in int_columns:
            if column in df.columns:
                df[column] = pd.to_numeric(df[column], errors='coerce').fillna(pd.NA).astype('Int64')

        print(df.info())

        # Modify the filename to replace 'checked' or 'checked_corrected' with 'processed.tsv'
        updated_filename = re.sub(r'checked(_corrected)?\.csv$', 'processed.tsv', filename)

        # Extract qualifiers from 'taxonfullname' (e.g., cf., aff., sp.)
        qual = df['taxonfullname'].str.extract(r'\b(cf|aff|sp)(\.?)(?=\s|$|[,.])', expand=True)
        df['qualifier'] = qual[0] + qual[1]
        df['taxonfullname'] = df['taxonfullname'].str.replace(r'\b(cf|aff|sp)(\.?)(?=\s|$|[,.])\s*', '', regex=True)
        df['taxonfullname'] = df['taxonfullname'].str.replace(r'\s{2,}', ' ', regex=True).str.strip()

        # Assign taxonomic fields from 'taxonfullname' to 'genus', 'species', etc.
        parsed_taxa = df.apply(parse_taxonfullname, axis=1)
        df = pd.concat([df, parsed_taxa], axis=1)

        # Add 'ishybrid' column based on whether ' x ' is in the 'species', 'subspecies', 'variety', or 'forma' column
        df = assign_ishybrid_fields(df)

        # Add author name to appropriate rank
        # For genus-level taxa
        df.loc[df['rankid'] == 180, 'genus_author'] = df['taxonauthor'].fillna('')
        # For species-level taxa
        df.loc[df['rankid'] == 220, 'species_author'] = df['taxonauthor'].fillna('')
        # For subspecies-level taxa
        df.loc[df['rankid'] == 230, 'subspecies_author'] = df['taxonauthor'].fillna('')
        # For variety-level taxa
        df.loc[df['rankid'] == 240, 'variety_author'] = df['taxonauthor'].fillna('')
        # For forma-level taxa
        df.loc[df['rankid'] == 260, 'forma_author'] = df['taxonauthor'].fillna('')

        # Add new taxa flags as appropriate (taxonspid is null or 0, or taxonomyuncertain is True)
        df[['newgenusflag', 'newspeciesflag', 'newsubspeciesflag', 'newvarietyflag', 'newformaflag']] = df.apply(set_new_flags, axis=1)

        # Split storage information into collection, cabinet, shelf, and box
        df[['collection', 'cabinet', 'shelf', 'box']] = df.apply(split_storage_info, axis=1)

        # Move specific verbiage from 'notes' to other columns
        df = extract_phrases_from_notes(df)

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
        df['recorddatetime'] = pd.to_datetime(df['recorddatetime'])
        # Extract date and assign to new columns
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
            'genus_author', 'newgenusflag', 'species', 'species_author', 'newspeciesflag', 'ishybrid_species', 'subspecies', 
            'subspecies_author', 'newsubspeciesflag', 'ishybrid_subspecies', 'variety', 'variety_author', 'newvarietyflag', 
            'ishybrid_variety', 'forma', 'forma_author', 'newformaflag', 'ishybrid_forma', 'qualifier', 'addendum', 
            'typestatusname', 'storedunder', 'localityname', 'broadgeographicalregion', 'localitynotes', 'preptypename', 
            'count', 'collection', 'cabinet', 'shelf', 'box', 'datafile_remark', 'datafile_source', 'datafile_date'
        ]

        # Ensure all columns in `desired_columns` exist in the DataFrame
        for column in column_order:
            if column not in df.columns:
                df[column] = pd.NA

        # Reorder columns
        df = df[column_order]

        # Write the df to a new CSV file in the output folder with the updated filename
        output_file_path = os.path.join(output_folder, updated_filename)
        df.to_csv(output_file_path, sep='\t', encoding='utf-8-sig', index=False)
        
        processed_file_path = os.path.join(archive_folder, filename)
        print(f"Moving file to: {processed_file_path}")
        shutil.move(file_path, processed_file_path)

        # Log the processing
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(log_file_path, 'a') as log_file:
            log_file.write(f"{timestamp} - {filename} processed and moved to {archive_folder}\n")
            log_file.write(f"{timestamp} - {updated_filename} ready for import to Specify\n")

        