# Post-processing of DaSSCo digitization data for PIOF pipelines using DigiApp

Post-processing is done via a Python script. As of right now, the script must be run manually but in the future it should be an automated process. 

The post-processing protocol can be found here: [Post-processing protocol](https://github.com/NHMDenmark/DaSSCo-Digitization/data_processing/DigiApp/import_protocol_postProcessing_digiapp.md)

The latest version of the Python script can be found here: [Format Data For Specify](https://github.com/NHMDenmark/DaSSCo-Digitization/data_processing/DigiApp/format_data_for_specify/PIOF/formatDataForSpecify.py)

Below you will find information on the steps performed by the Python script.

### Python script steps

1. The script locates any file ending with .csv in the specified folder
2. The data in the file is read into a pandas dataframe for the script to work with
3. All numeric columns are converted to Int64 to prevent floats
4. The filename is stored as a variable (called updated_filename) with either 'checked.csv' or 'checked_corrected.csv' replaced with 'processed.tsv'
5. The genus, species, and subspecies are extracted from the taxonfullname column and assigned to the corresponding taxonomic column(s)
6. The author, and taxon number and source (if applicable), are assigned to the approrpriate taxonomic rank
7. If the taxonspid value is '0', null, or empty, or if the taxonomyuncertain value is 'True', the newgenusflag and/or newspeciesflag columns get the 'True' value
8. The following columns are renamed:

     - familyname: family
     - georegionname: broadgeographicalregion
     - agentfirstname: cataloger_firstname
     - agentmiddleinitial: cataloger_middle
     - agentlastname: cataloger_lastname
     - notes: remarks

9. If the cataloger_middle value is 'None', it gets replaced with an empty string
10. The value in broadgeographicalregion is copied to a new column called localityname
11. The recorddatetime value is converted to datetime and added to the new columns catalogeddate and datafile_date
12. The following columns are added to the dataframe and filled with specified values:

     - project = 'DaSSCo'
     - publish = 'True'
     - count = '1'
     - storedunder = 'True'
     - datafile_source = 'DaSSCo data file'
     - datafile_remark = updated_filename

13. If the value in labelobscured is 'True': 
     - the labelobscured_remark value is updated to 'Label obscured'  
     - the labelobscured_source is given the value 'DaSSCo digitisation'
     - the labelobscured_date is given the same value as catalogeddate
14. If the value in specimenobscured is 'True': 
     - the specimenobscured_remark value is updated to 'Specimen obscured'  
     - the specimenobscured_source is given the value 'DaSSCo digitisation'
     - the specimenobscured_date is given the same value as catalogeddate
15. If there is any value in the remarks column, the remark date field is given the same value as cataloggeddate and remark_source is filled with the value 'DaSSCo digitisation'
16. The final order of all columns in the dataframe is created and is as follows:

     - catalognumber
     - catalogeddate
     - cataloger_firstname
     - cataloger_middle
     - cataloger_lastname
     - project
     - objectcondition
     - specimenobscured
     - specimenobscured_remark
     - specimenobscured_source
     - specimenobscured_date
     - labelobscured
     - labelobscured_remark
     - labelobscured_source
     - labelobscured_date
     - publish
     - containername
     - containertype
     - remarks
     - remark_date
     - remark_source
     - family
     - genus
     - genus author 
     - genus_taxonnumber
     - genus_taxonnrsource
     - newgenusflag
     - species 
     - species_author
     - species_taxonnumber 
     - species_taxonrsource 
     - newspeciesflag
     - subspecies 
     - subspecies_author
     - subspecies_taxonnumber 
     - subspecies_taxonrsource 
     - newsubspeciesflag
     - typestatusname
     - storedunder
     - localityname
     - broadgeographicalregion 
     - localitynotes
     - preptypename 
     - count 
     - datafile_remark
     - datafile_source
     - datafile_date

17. The dataframe is saved as a TSV file with BOM encoding in the specified output_folder with the updated_filename
18. The original CSV file is moved to the specified archive_folder
19. The log_file is updated with the original filename, updated_filename, new locations, and a message that the TSV file is ready to be imported to Specify