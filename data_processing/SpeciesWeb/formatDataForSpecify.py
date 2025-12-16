# This script processes CSV files from a PIOF pipeline, reformats them for Specify, and archives the original files.
# It also creates a file to import synonyms to Specify, as well as a file with unique taxa for checking duplicates in Specify's taxon tree.

import pandas as pd
import os
import re
import numpy as np
import shutil
from datetime import datetime
from dotenv import load_dotenv
import codecs
import chardet
from io import StringIO
import json

# Load environment variables from the .env file
load_dotenv()

# Directory paths should be defined in the .env file
base_folder_path = os.getenv("FOLDER_PATH")
base_archive_folder = os.getenv("ARCHIVE_FOLDER")
base_output_folder = os.getenv("OUTPUT_FOLDER")
base_log_file_path = os.getenv("LOG_FILE_PATH")

# In directory paths, {collection} is replaced with the collection name
# Change this to switch collections
collection = os.getenv("COLLECTION")
folder_path = base_folder_path.format(collection=collection)
archive_folder = base_archive_folder.format(collection=collection)
output_folder = base_output_folder.format(collection=collection)
log_file_path = base_log_file_path.format(collection=collection)

# Ensure the log file directory exists
os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
# Ensure the archive folder exists
os.makedirs(archive_folder, exist_ok=True)

# Data to be extracted from gbif_match_json
keys_to_extract = [
    "kingdom", "phylum", "order", "family", "genus", "species", "scientificName", "authorship", "taxonomicStatus",
    "accepted", "class"
]

def extract_json_data(row):
    extracted_data = {}
    try:
        # Loop through the keys you want to extract
        for key in keys_to_extract:
            # Use regex to find the key-value pair in the JSON-like string
            match = re.search(rf'"{key}":\s*("[^"]*"|\d+)', row['gbif_match_json'])
            
            if match:
                value = match.group(1)
                if value.startswith('"'):
                    extracted_data[key] = value.strip('"')  # Remove quotes for string values
                else:
                    # If it's a number (no quotes), store it as a string first
                    extracted_data[key] = value if not value.isdigit() else int(value)
            else:
                extracted_data[key] = None  # If key is not found, set to None
    except Exception as e:
        # Return None for all fields if there's an error
        extracted_data = {key: None for key in keys_to_extract}

    # Convert the dictionary to a pandas Series to match the structure of the df
    return pd.Series(extracted_data)

# Extract and remove genus from species column (in extracted gbif data) if species is not blank
def update_genus_and_species(row):
    if pd.notnull(row['species']):
        # Extract genus from the 'species' field
        row['genus'] = row['species'].split()[0] if row['species'].split()[0].istitle() else row['genus']
        # Remove genus from 'species' field if it's present
        row['species'] = ' '.join(row['species'].split()[1:])
    return row

# Pull correct genus for synonyms at genus rank from scientificName
def update_genus_for_synonyms(row):
    """
    If a record is a SYNONYM at GENUS rank, copy the first word
    of scientificName into genus.
    """
    tax_status = str(row.get('taxonomicStatus', '')).upper()
    rank = str(row.get('rank', '')).upper()
    sci_name = str(row.get('scientificName', '')).strip()

    if "SYNONYM" in tax_status and rank == "GENUS" and sci_name:
        first_word = sci_name.split()[0]
        row['genus'] = first_word

    return row

# Process authorship and taxon source at rank level (for extracted gbif data)
def process_taxonomic_fields(row):
    gbif = row.get('gbif_match_json', None)

    # Skip if gbif_match_json is NA or the literal string 'null'
    if pd.isna(gbif) or str(gbif).strip().lower() == "null":
        return row
    
    if pd.isnull(row['species']):
        row['genus_author'] = row['authorship']
        row['genus_taxon_source'] = 'GBIF'
    elif "var." in row['scientificName']:
        row['variety_author'] = row['authorship']
        row['variety_taxon_source'] = 'GBIF'
    elif "subsp." in row['scientificName']:
        row['subspecies_author'] = row['authorship']
        row['subspecies_taxon_source'] = 'GBIF'
    else:
        row['species_author'] = row['authorship']
        row['species_taxon_source'] = 'GBIF'
    
    return row

# Extract subspecies and/or variety from scientific_name (in extracted gbif data)
def extract_taxon(scientific_name, rank):
    """
    Extracts either subspecies or variety name from a scientific name string.
    
    Parameters
    ----------
    scientific_name : str
        The scientific name to parse.
    rank : str
        Either "subsp" or "var".
    
    Returns
    -------
    str or None
        The extracted name, or None if not found.
    """
    if not isinstance(scientific_name, str) or pd.isna(scientific_name):
        return None

    # Hybrid case: e.g. "subsp. alba x rubra" or "var. alba x rubra"
    match = re.search(rf'\b{rank}\.\s+([a-z\s]*x\s[a-z\s]*)', scientific_name)
    if match:
        return match.group(1).strip()

    # Standard case: e.g. "subsp. alba" or "var. alba"
    match = re.search(rf'\b{rank}\.\s+(\S+)', scientific_name)
    if match:
        return match.group(1).strip()

    return None

# Create fields 'ishybrid' at rank level (either species, subspecies, or variety)
# If hybrid name detected, 'ishybrid' = True
# If non-hybrid name detected, 'ishybrid' = False
# If no name at that rank, 'ishybrid' = ""
# Note that this code does not support cross-rank hybrids at this time
def assign_ishybrid_fields(df):
    for col in ['species', 'subspecies', 'variety']:
        # Hybrid = contains ' x ' and not null
        is_hybrid = df[col].notna() & df[col].str.contains(' x ', na=False)

        # Start with empty string everywhere
        df[f'ishybrid_{col}'] = ""

        # Assign True where hybrid detected
        df.loc[is_hybrid, f'ishybrid_{col}'] = True

        # Assign False only where value exists but is not a hybrid
        df.loc[df[col].notna() & ~df[col].str.contains(' x ', na=False), f'ishybrid_{col}'] = False

    return df

# Parse accepted data from the gbif_match_json
def parse_accepted_data(row):
    accepted = row.get('accepted')

    if not isinstance(accepted, str) or not accepted.strip():
        # If 'accepted' is empty or not a valid string, return None for all fields
        row['accepted_genus'] = None
        row['accepted_species'] = None
        row['accepted_subspecies'] = None
        row['accepted_variety'] = None
        row['accepted_genus_author'] = None
        row['accepted_species_author'] = None
        row['accepted_subspecies_author'] = None
        row['accepted_variety_author'] = None
        return row

    parts = accepted.split()

    # Basic taxonomic elements
    row['accepted_genus'] = parts[0] if parts else None
    row['accepted_species'] = None
    row['accepted_subspecies'] = None
    row['accepted_variety'] = None

    # Initialize tracking
    genus = parts[0] if parts else None
    species = None
    subspecies = None
    variety = None
    author_parts = []

    i = 1
    while i < len(parts):
        part = parts[i]

        if part == 'subsp.':
            i += 1
            if i < len(parts):
                subspecies = parts[i]
        elif part == 'var.':
            i += 1
            if i < len(parts):
                variety = parts[i]
        elif not species:
            species = part
        else:
            author_parts.append(part)
        i += 1

    # Assign parsed values
    row['accepted_species'] = species
    row['accepted_subspecies'] = subspecies
    row['accepted_variety'] = variety

    # Determine where to assign the author
    author_str = ' '.join(author_parts) if author_parts else None
    row['accepted_genus_author'] = None
    row['accepted_species_author'] = None
    row['accepted_subspecies_author'] = None
    row['accepted_variety_author'] = None

    if variety:
        row['accepted_variety_author'] = author_str
    elif subspecies:
        row['accepted_subspecies_author'] = author_str
    elif species:
        row['accepted_species_author'] = author_str
    else:
        row['accepted_genus_author'] = author_str

    return row

def create_synonyms(df, output_folder, filename):
    # Filter rows where taxonomicStatus contains 'SYNONYM'
    synonym_rows = df[df['taxonomicStatus'].str.contains('SYNONYM', na=False)]

    # Apply parsing logic for synonyms to the synonyms df
    synonym_rows = synonym_rows.apply(parse_accepted_data, axis=1)

    # Ensure 'accepted_' columns exist
    accepted_columns = [
        'accepted_genus', 'accepted_species', 'accepted_subspecies', 'accepted_variety',
        'accepted_genus_author', 'accepted_genus_taxon_source',
        'accepted_species_author', 'accepted_species_taxon_source',
        'accepted_subspecies_author', 'accepted_subspecies_taxon_source', 
        'accepted_variety_author', 'accepted_variety_taxon_source'
    ]

    for col in accepted_columns:
        if col not in synonym_rows.columns:
            synonym_rows[col] = None

    # Specify columns to include in the separate synonyms CSV
    columns_to_include = [
        'kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'genus_author', 'genus_taxon_source',
        'species', 'species_author', 'species_taxon_source',
        'subspecies', 'subspecies_author', 'subspecies_taxon_source',
        'variety', 'variety_author', 'variety_taxon_source',
        'accepted_genus', 'accepted_genus_author', 'accepted_genus_taxon_source',
        'accepted_species', 'accepted_species_author', 'accepted_species_taxon_source',
        'accepted_subspecies', 'accepted_subspecies_author',
        'accepted_subspecies_taxon_source', 'accepted_variety',
        'accepted_variety_author', 'accepted_variety_taxon_source'
    ]

    # Only include column if it exists in the above list and the synonym_rows list
    columns_to_include = [col for col in columns_to_include if col in synonym_rows.columns]
    synonym_rows = synonym_rows[columns_to_include]

    # Rename columns to match Sp7ApiToolbox formatting requirements
    synonym_rows.rename(columns={'kingdom': 'Kingdom', 'phylum': 'Phylum', 'class': 'Class', 'order': 'Order', 'family': 'Family',
                                 'genus': 'Genus', 'genus_author': 'GenusAuthor', 'genus_taxon_source': 'GenusTaxonSource', 
                                 'species': 'Species', 'species_author': 'SpeciesAuthor', 'species_taxon_source': 'SpeciesTaxonSource',
                                 'subspecies': 'Subspecies', 'subspecies_author': 'SubspeciesAuthor', 'subspecies_taxon_source': 'SubspeciesTaxonSource',
                                 'variety': 'Variety', 'variety_author': 'VarietyAuthor', 'variety_taxon_source': 'VarietyTaxonSource',
                                 'accepted_genus': 'AcceptedGenus', 'accepted_genus_author': 'AcceptedGenusAuthor', 'accepted_genus_taxon_source': 'AcceptedGenusTaxonSource',
                                 'accepted_species': 'AcceptedSpecies', 'accepted_species_author': 'AcceptedSpeciesAuthor', 'accepted_species_taxon_source': 'AcceptedSpeciesTaxonSource',
                                 'accepted_subspecies': 'AcceptedSubspecies', 'accepted_subspecies_author': 'AcceptedSubspeciesAuthor', 'accepted_subspecies_taxon_source': 'AcceptedSubspeciesTaxonSource',
                                 'accepted_variety': 'AcceptedVariety', 'accepted_variety_author': 'AcceptedVarietyAuthor', 'accepted_variety_taxon_source': 'AcceptedVarietyTaxonSource'}, inplace=True)

    # For all accepted taxa, set AcceptedTaxonSource to 'GBIF'
    synonym_rows.loc[synonym_rows['AcceptedGenus'].notna() & (synonym_rows['AcceptedGenus'] != ''), 'AcceptedGenusTaxonSource'] = 'GBIF'
    synonym_rows.loc[synonym_rows['AcceptedSpecies'].notna() & (synonym_rows['AcceptedSpecies'] != ''), 'AcceptedSpeciesTaxonSource'] = 'GBIF'
    synonym_rows.loc[synonym_rows['AcceptedSubspecies'].notna() & (synonym_rows['AcceptedSubspecies'] != ''), 'AcceptedSubspeciesTaxonSource'] = 'GBIF'
    synonym_rows.loc[synonym_rows['AcceptedVariety'].notna() & (synonym_rows['AcceptedVariety'] != ''), 'AcceptedVarietyTaxonSource'] = 'GBIF'
    
    # Add the isAccepted column to the synonym rows
    synonym_rows['isAccepted'] = 'No'
    
    # Create the filename for the synonyms CSV
    synonyms_filename = updated_filename.replace('_processed.tsv', '_synonymsToImport.csv')

    # Convert all numeric columns to integers in the synonyms df
    numeric_columns_synonymns = synonym_rows.select_dtypes(include=['float64', 'int64']).columns
    for col in numeric_columns_synonymns:
        synonym_rows[col] = pd.to_numeric(synonym_rows[col], errors='coerce').fillna(pd.NA).astype(pd.Int64Dtype())

    # Drop all duplicate rowsin the synonyms df
    synonym_rows = synonym_rows.drop_duplicates()

    # Save the synonym rows to a CSV
    output_file_path_synonyms = os.path.join(output_folder, synonyms_filename)
    synonym_rows.to_csv(output_file_path_synonyms, index=False, sep=',', encoding='utf-8')

def format_digitiser(df):
    # Split the digitiser field based on underscores
    df['digitiser'] = df['digitiser'].fillna('').astype(str)

    for idx, row in df.iterrows():
        name_parts = row['digitiser'].split('_')
        
        if len(name_parts) == 2:
            # If there are two parts, first part is the first name, second part is the last name
            df.at[idx, 'cataloger_firstname'] = name_parts[0]
            df.at[idx, 'cataloger_lastname'] = name_parts[1]
        elif len(name_parts) > 2:
            # If there are more than two parts, first part is the first name, last part is the last name, everything else is the middle name
            df.at[idx, 'cataloger_firstname'] = name_parts[0]
            df.at[idx, 'cataloger_lastname'] = name_parts[-1]
            df.at[idx, 'cataloger_middle'] = '_'.join(name_parts[1:-1])  # Join middle parts if there are any
        else:
            # If the 'digitiser' is empty or does not contain an underscore
            df.at[idx, 'cataloger_firstname'] = 'Birgitte'
            df.at[idx, 'cataloger_lastname'] = 'Bergmann'

    # Add Birgitte to any empty catloger fields
    df['cataloger_firstname'] = df['cataloger_firstname'].fillna('')
    df['cataloger_lastname'] = df['cataloger_lastname'].fillna('')

    df.loc[
        (df['cataloger_firstname'] == '') & (df['cataloger_lastname'] == ''), 
        ['cataloger_firstname', 'cataloger_lastname']
    ] = ['Birgitte', 'Bergmann']

    return df

def fix_encoding_issues(s):
    """Fix misencoded characters and decode unicode escapes like \\u00e9 → é."""
    if not isinstance(s, str):
        return s
    try:
        # Step 1: Decode Unicode escapes like \u00e9 → é
        s = s.encode('utf-8').decode('unicode_escape')
    except Exception:
        pass
    try:
        # Step 2: Handle mixed or double-encoded UTF-8 / Latin-1 characters
        s = s.encode('latin1').decode('utf-8')
    except Exception:
        pass
    return s

# Fill taxonomic fields from speciesweb columns if gbif_match_json is missing or 'null'
def fill_from_speciesweb(row, df):
    gbif = row.get("gbif_match_json", None)

    # Only act if gbif_match_json is missing or the literal string 'null'
    if pd.isna(gbif) or str(gbif).strip().lower() == "null":
        mapping = {
            "family_speciesweb": "family",
            "genus_speciesweb": "genus",
            "species_speciesweb": "species",
            "variety_speciesweb": "variety",
            "subspecies_speciesweb": "subspecies",
        }

        for source, target in mapping.items():
            # Ensure target column exists
            if target not in df.columns:
                df[target] = pd.NA

            # Assign value if source exists
            if source in row and pd.notna(row[source]):
                row[target] = row[source]

    return row

# Remove genus from species in df_nulls if species is not blank
def clean_species(row):
    name = row["species_speciesweb"]
    genus = row["genus_speciesweb"]
    if not isinstance(name, str) or not isinstance(genus, str):
        return name
    
    # Strip whitespace + normalize case
    genus_clean = genus.strip().lower()
    parts = name.strip().split()
    
    if parts and parts[0].strip().lower() == genus_clean:
        return " ".join(parts[1:])  # drop genus
    return name

# Replace semicolons inside { ... } with commas
# This regex finds braces and applies a replacement only inside them
def replace_semicolons_inside_braces(s):
    def replacer(match):
        content = match.group(1)
        return '{' + content.replace(';', ',') + '}'
    return re.sub(r'\{([^{}]*)\}', replacer, s)

# Loop through each CSV file in the specified folder_path
for filename in os.listdir(folder_path):
    # Check if the file is a CSV file
    if filename.endswith('.csv'):
        # Read the CSV file with semicolon delimiter, ignoring encoding errors
        file_path = os.path.join(folder_path, filename)

        with open(file_path, encoding="utf-8") as f:
            text = f.read()

        text = replace_semicolons_inside_braces(text)

        # Detect file encoding while reading into a df
        try:
            df = pd.read_csv(
                StringIO(text), 
                delimiter=";", 
                engine='python',
                quotechar="'",
                #escapechar='\\', 
                encoding="utf-8"
            )
        except UnicodeDecodeError:
            df = pd.read_csv(
                StringIO(text), 
                delimiter=";", 
                engine='python',
                quotechar="'",
                #escapechar='\\',
                encoding="latin-1"
            )

        print(df.info())
        print(df['gbif_match_json'].head(5))
        # Fix unicode escape sequences in all string columns
        for col in df.select_dtypes(include=["object"]):
            df[col] = df[col].apply(fix_encoding_issues)
        
        # Modify the filename to replace 'checked' or 'checked_corrected' with 'processed.tsv'
        updated_filename = re.sub(r'checked(_corrected)?\.csv$', 'processed.tsv', filename)

        # Extract keys from gbif_match_json and convert floats to ints
        df[keys_to_extract] = df.apply(extract_json_data, axis=1)

        # Add a column with the updated filename
        df['datafile_remark'] = updated_filename
        # Add other pre-filled columns with specified values
        df['projectnumber'] = 'DaSSCo'
        df['publish'] = True
        df['storedunder'] = True
        df['preptypename'] = 'Sheet'
        df['count'] = 1
        df['datafile_source'] = 'DaSSCo data file'
        df['cataloger_firstname'] = None
        df['cataloger_middle'] = None
        df['cataloger_lastname'] = None

        # Convert the 'date_asset_taken' column to datetime and extract the date in 'YYYY-MM-DD' format
        # Assign this value to catalogeddate
        df['catalogeddate'] = (
            pd.to_datetime(df['date_asset_taken'], utc=True, errors='coerce')
            .dt.strftime('%Y-%m-%d')
        )

        # Convert the value in digitiser to cataloger first, middle, and last names 
        df = format_digitiser(df)

        # Update the genus and species fields
        df = df.apply(update_genus_and_species, axis=1)

        # Update genus for synonyms at genus rank
        df = df.apply(update_genus_for_synonyms, axis=1)
        
        # Replace values in 'authorship' column with NaN if they contain no letters
        df['authorship'] = df['authorship'].apply(
            lambda x: np.nan if isinstance(x, str) and not any(char.isalpha() for char in x) else x
            )

        # Remove genus from species column in rows where gbif_match_json is missing or 'null'
        mask = df["gbif_match_json"].isna() | (df["gbif_match_json"] == "null")
        df.loc[mask, "species_speciesweb"] = df.loc[mask].apply(clean_species, axis=1)

        # Fill taxonomic fields from speciesweb columns if gbif_match_json is missing or 'null'
        df = df.apply(lambda r: fill_from_speciesweb(r, df), axis=1)

        # For rows with gbif data, move the author & taxonomic info to the correct columns
        df = df.apply(process_taxonomic_fields, axis=1)

        # Extract subspecies and variety from scientificName
        df['subspecies'] = df['scientificName'].apply(lambda x: extract_taxon(x, "subsp"))
        df['variety'] = df['scientificName'].apply(lambda x: extract_taxon(x, "var"))

        # Add 'ishybrid' column based on whether ' x ' is in the 'species', 'subspecies', or 'variety' column
        df = assign_ishybrid_fields(df)

        # Fill empty taxonomic cells for rows where gbif_match_json is missing or 'null'
        fill_df = (
            df[mask]
            .merge(
                df[~mask][["family", "kingdom", "phylum", "class", "order"]],
                on="family",
                how="left",
                suffixes=("", "_filled")
            )
        )

        for col in ["kingdom", "phylum", "class", "order"]:
            df.loc[mask, col] = fill_df[col].combine_first(fill_df[f"{col}_filled"])

        # Rename barcode and area columns
        df.rename(columns={'barcode': 'catalognumber', 'area': 'broadgeographicalregion'}, inplace=True)
        df['locality'] = df['broadgeographicalregion']

        create_synonyms(df, output_folder, filename)

        # Desired column order
        desired_columns = [
            'catalognumber', 'catalogeddate', 'cataloger_firstname', 'cataloger_middle', 'cataloger_lastname',
            'projectnumber', 'publish', 'kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'genus_author', 'genus_taxon_source',
            'species', 'species_author', 'species_taxon_source', 'ishybrid_species',
            'subspecies', 'subspecies_author', 'subspecies_taxon_source', 'ishybrid_subspecies',
            'variety', 'variety_author', 'ishybrid_variety', 'variety_taxon_source', 'storedunder', 'locality', 
            'broadgeographicalregion', 'preptypename', 'count'
        ]

        # Ensure all columns in `desired_columns` exist in the DataFrame
        for column in desired_columns:
            if column not in df.columns:
                df[column] = pd.NA

        # Reorder the columns and drop any that are not needed for import to Specify
        df = df[desired_columns]
        df = df[[col for col in desired_columns if col in df.columns]]

        # Convert all numeric columns to integers
        numeric_columns = df.select_dtypes(include=['float64', 'int64']).columns
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(pd.NA).astype(pd.Int64Dtype())

        # Create a new DataFrame with unique combinations of taxonomic columns
        subset_cols = ['genus', 'genus_author', 'species', 'species_author', 'subspecies', 'subspecies_author', 'variety', 'variety_author']
        valid_cols = [col for col in subset_cols if col in df.columns]
        unique_df = df.drop_duplicates(subset=valid_cols) 
        # Write this df to a CSV file to be used to check duplicates in Specify's taxon tree
        unique_csv = os.path.join(output_folder, f'{updated_filename.replace("_processed.tsv", "")}_unique_taxa.csv')   
        unique_df.to_csv(unique_csv, index=False, sep= ';', encoding='utf-8')
        
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