# Post-processing of DaSSCo digitisation data via Species-Web

Post-processing is done via a Python script. As of right now, the script must be run manually but in the future it should be an automated process. 

The post-processing protocol can be found here: [Post-processing protocol](https://github.com/NHMDenmark/DaSSCo-Digitization/data_processing/SpeciesWeb/import_protocol_postProcessing_speciesWeb.md)

The latest version of the Python script can be found here: [Format Data For Specify](https://github.com/NHMDenmark/DaSSCo-Digitization/data_processing/SpeciesWeb/formatDataForSpecify.py)

Below you will find information on the steps performed by the Python script.

### Python script steps

1. The script locates any file ending with .csv in the specified folder
2. The data in the file is read into a pandas dataframe for the script to work with
3. The filename is stored as a variable (called updated_filename) with either 'checked.csv' or 'checked_corrected.csv' replaced with 'processed.tsv'
4. Specified keys are extracted from the gbif_match_json column and assigned to their own columns: 

     - kingdom
     - phylum
     - order
     - class
     - family
     - genus
     - species
     - scientificName
     - authorship
     - taxonomicStatus
     - accepted

5. Several columns are added to the dataframe and filled with specified values:

     - datafile_remark = updated_filename
     - projectnumber = 'DaSSCo'
     - publish = 'True'
     - storedunder = 'True'
     - preptypename = 'Sheet'
     - count = '1'
     - datafile_source = 'DaSSCo data file'
     - cataloger_firstname = None
     - cataloger_middle = None
     - cataloger_lastname = None

6. The date in the date_asset_taken column is converted to datetime, extracted in the format YYYY-MM-DD, and added to both the catalogeddate column and the datafile_date column
7. The name (if any) in the digitiser column is converted to cataloger first, middle, and last name columns
8. Authorship values that do not contain any letters are converted to null values, in order to compensate for this field occasionally containing a comma or parentheses but no actual author name
9. The genus is split out of the species column and assigned to the genus column, as this data is more reliable than any value that may be in the genus field of the gbif_match_json results. (If the taxonomic rank is genus, we pull this from the scientificName column instead)
10. In rows where 'gbif_match_json' is null, (in other words, when the taxonomic information was typed in manually in the Species-Web UI), the genus is removed from the species column
11. In rows where 'gbif_match_json' is null, taxonomic information is copied from columns ending in '_speciesweb' to the appropriate taxonomic level (for example, the value in 'family_speciesweb' is copied to 'family')
12. Author names are assigned to the appropriate taxonomic levels and source columns are filled with the value 'GBIF'
13. If the taxon is a subspecies, everything in the scientificName column after 'subsp.' gets moved to a subspecies column
14. If the taxon is a variety, everything in the scientificName column after 'var.' gets moved to a variety column
15. If the species, subspecies, or variety values contain ' x ', it is assumed they are a hybrid and the value True gets assigned to the ishybrid column at the appropriate taxonomic level
16. Sometimes when the gbif_match_json field is empty, there is no higher taxonomic information included. These are auto-filled by matching the family from other rows
17. The following columns are renamed:

     - barcode is renamed to catalognumber
     - area is renamed to broadgeographicalregion

18. A new column called locality is created and filled with the same value that is in broadgeographicalregion
19. If any rows contain the taxonomicStatus 'synonym', the following occurs:
    
     - These rows are assigned to a second dataframe called synonym_rows
     - Accepted taxon and author columns are created at each rank level: 

         - accepted_genus 
         - accepted_species
         - accepted_variety
         - accepted_subspecies

     - The taxon string in the accepted column is parsed into the above relevant accepted columns
     - The author is pulled from the accepted column and added to the relevant accepted_author column at rank level
     - All taxonomy, author, source, and accepted columns that do not contain entirely null values are included and ordered in the final version of the synonym_rows dataframe
     - Some columns are renamed to match the Sp7ApiToolbox formatting requirements
     - The accepted_taxon_source at the relevant rank level is assigned the value 'GBIF'
     - The column 'isAccepted' is added to the dataframe and filled with the value 'No'
     - The synonym_rows dataframe is saved as a comma separated CSV file in the specified output_folder with the same name as the original CSV file, but 'synonymsToImport.csv' appended to the end in place of '_checked.csv', '_checked_corrected.csv'

20. The final order of all columns in the main dataframe is created and is as follows:

     - catalognumber
     - catalogeddate
     - cataloger_firstname
     - cataloger_middle
     - cataloger_lastname
     - projectnumber
     - publish
     - kingdom
     - phylum
     - class
     - order
     - family
     - genus
     - genus author 
     - genus_taxon_source
     - species 
     - species_author 
     - species_taxon_source 
     - ishybrid_species
     - subspecies 
     - subspecies_author 
     - subspecies_taxon_source 
     - ishybrid_subspecies
     - variety 
     - variety_author 
     - variety_taxon_source
     - ishybrid_variety
     - storedunder
     - locality 
     - broadgeographicalregion 
     - preptypename 
     - count 

21. All above columns are confirmed to exist in the dataframe
22. All numeric columns are assigned the dtype int64 to prevent them from becoming floats
23. A separate CSV file with unique combinations of taxonomic columns is created and saved with the same name as the original CSV file, but '_unique_taxa.csv' appended to the end in place of '_checked.csv', '_checked_corrected.csv'
24. The dataframe is saved as a TSV file with BOM encoding in the specified output_folder with the updated_filename
25. The original CSV file is moved to the specified archive_folder
26. The log_file is updated with the original filename, updated_filename, new locations, and a message that the TSV file is ready to be imported to Specify