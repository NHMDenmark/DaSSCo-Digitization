# Post-processing of DaSSCo digitization data for HERB pipelines using DigiApp

Post-processing is done via a Python script. As of right now, the script must be run manually but in the future it should be an automated process. 

The post-processing protocol can be found here: [Post-processing protocol](https://github.com/NHMDenmark/DaSSCo-Digitization/data_processing/DigiApp/import_protocol_postProcessing_digiapp.md)

The latest version of the Python script can be found here: [Format Data For Specify](https://github.com/NHMDenmark/DaSSCo-Digitization/data_processing/DigiApp/format_data_for_specify/HERB/formatDataForSpecify.py)

Below you will find information on the steps performed by the Python script.

### Python script steps

1. The script locates any file ending with .csv in the specified folder
2. The data in the file is read into a pandas dataframe for the script to work with
3. All numeric columns are converted to Int64 to prevent floats
4. The filename is stored as a variable (called updated_filename) with either 'checked.csv' or 'checked_corrected.csv' replaced with 'processed.tsv'
5. Qualifiers (e.g., cf., aff., sp.) are extracted from taxonfullname and added to a new qualifier column
6. The taxonomy is extracted from taxonfullname and assigned to the corresponding taxonomic column(s)
7. If the species, subspecies, variety, or forma values contain ' x ', it is assumed they are a hybrid and the value True gets assigned to the ishybrid column at the appropriate taxonomic level
8. The author is assigned to the approrpriate taxonomic rank
9. If the taxonspid value is '0', null, or empty, or if the taxonomyuncertain value is 'True', the newgenusflag and/or newspeciesflag columns get the 'True' value
10. Collection, cabinet, shelf, and box are extracted from the storagefullname column and added to the appropriate storage column
11. If 'sensu lato' or 'sensu stricto' are in the notes column, these are extracted and moved to a new addendum column
12. The following columns are renamed:

     - familyname: family
     - georegionname: broadgeographicalregion
     - agentfirstname: cataloger_firstname
     - agentmiddleinitial: cataloger_middle
     - agentlastname: cataloger_lastname
     - notes: remarks

13. If the cataloger_middle value is 'None', it gets replaced with an empty string
14. The value in broadgeographicalregion is copied to a new column called localityname
15. The recorddatetime value is converted to datetime and added to the new columns catalogeddate and datafile_date
16. The following columns are added to the dataframe and filled with specified values:

     - project = 'DaSSCo'
     - publish = 'True'
     - count = '1'
     - storedunder = 'True'
     - datafile_source = 'DaSSCo data file'
     - datafile_remark = updated_filename

17. If the value in labelobscured is 'True': 
     - the labelobscured_remark value is updated to 'Label obscured'  
     - the labelobscured_source is given the value 'DaSSCo digitisation'
     - the labelobscured_date is given the same value as catalogeddate
18. If the value in specimenobscured is 'True': 
     - the specimenobscured_remark value is updated to 'Specimen obscured'  
     - the specimenobscured_source is given the value 'DaSSCo digitisation'
     - the specimenobscured_date is given the same value as catalogeddate
19. If there is any value in the remarks column, the remark date field is given the same value as cataloggeddate and remark_source is filled with the value 'DaSSCo digitisation'
20. The final order of all columns in the dataframe is created and is as follows:

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
     - newgenusflag
     - species 
     - species_author
     - newspeciesflag
     - ishybrid_species
     - subspecies
     - subspecies_author
     - newsubsupeciesflag
     - ishybrid_subspecies
     - variety
     - variety_author
     - newvarietyflag
     - ishybrid_variety
     - forma
     - forma_author
     - newformaflag
     - ishybrid_forma
     - qualifier
     - addendum
     - typestatusname
     - storedunder
     - localityname 
     - broadgeographicalregion 
     - localitynotes
     - preptypename 
     - count 
     - collection
     - cabinet
     - shelf
     - box
     - datafile_remark
     - datafile_source
     - datafile_date

21. The dataframe is saved as a TSV file with BOM encoding in the specified output_folder with the updated_filename
22. The original CSV file is moved to the specified archive_folder
23. The log_file is updated with the original filename, updated_filename, new locations, and a message that the TSV file is ready to be imported to Specify