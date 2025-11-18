# Import protocol: from Species-Web to Specify


### Overview

This import protocol consists of four steps:

1. Exporting the data from Species-Web
2. Checking the export files from Species-Web and resolving any issues
3. Post-processing via a Python script
4. Importing to Specify

The Data folder on the N-drive contains all the export files generated from the Digi App and has been structured with subfolders to accomodate the different steps of the protocol. Additional subfolders have been implemented for the separate institutions/collections involved in DaSSCo. Export files should always be placed in the appropriate subfolder according to the institution/collection that the digitized specimens belong to. Because of this existing file structure, Species-Web exports will initially be added to folders with names referencing the Digi App and OpenRefine, two tools which are not used in this workflow.

A folder has been added to the folder structure called 4.ReadyForScript. The plan is to have a monitoring script running on this directory in the future but currently the folder is not being used as part of the protocol.

### Exporting the data from Species-Web

As specimens are digitized, their data will be added to the Species-Web database (called 'dassco-au'). Once a week, this data will need to be exported from Species-Web and imported into Specify. In order to export the data from Species-Web, you'll need a terminal window and MySQL Workbench.

#### Protocol

1. Open the dassco-au database following the steps below:
   - Open My SQL Workbench and connect to the dassco-au database
     - If this is your first time connecting to the database, open MySQL Workbench and click the plus sign (+) next to MySQL Connections
     - A box will pop up that says Setup New Connection. Enter the following:
       - Connection Name: dassco-au
	     - Port: 3306
	     - Username: [enter your username]
	     - Password: [enter your password]
	     - Click OK
	   - The next time you open MySQL Workbench, you will see dassco-au under MySQL Connections

2. Run the SQL query located here: [Species-Web Export Query](https://github.com/NHMDenmark/DaSSCo-Digitization/data_processing/SpeciesWeb/speciesWebExportQuery.sql), changing the date in this line: 

``WHERE folders.approved_at > '2025-01-01' LIMIT 100000``

3. Click the Export button and export the query results as a ; seperated CSV. Give the file a name matching the following format:

``AU_Herba_YYYYMMDD_HH_MM_INITIALS_original.csv``

where YYYYMMDD is the date of the export, HH is the hour of the export, MM is the minute of the export, and INITIALS is the exporter's initials

Save this file to 1.FromDigiApp/AU_Herbarium

### Checking the export files from Species-Web

Once the data has been exported, it will need to be checked over to make sure there are no obvious errors. A copy of the export will be made to ensure that the original data is not modified.

#### Protocol:

1.	Go to the 1.FromDigiApp/AU_Herbarium folder and locate the export
2.	Copy the file and paste it into the folder: 2.BeingChecked/AU_Herbarium
3.	To the end of the filename for the copied file, (the one in the 2.BeingChecked folder), append _copy so that the filename now ends with _original_copy.csv
4. Move the original file (the one without copy at the end) to the AU_Herbarium subfolder in the folder: 6.Archive
5.	Open the copied file using LibreOffice Calc and check for any errors or issues. Files with problems that require further investigation/time to resolve (e.g. files connected to github tickets) should be moved to the subfolder FilesWithProblems until the problem has been resolved
6.	When you are done checking the file, change the suffix of the filename from original_copy to checked (no corrections made) OR checked_corrected  (corrections/modifications have been made to the file)
7.	Move the file to the AU_Herbarium subfolder in the folder: 3.ReadyForOpenRefine


### Post-processing

Post-processing is done via a Python script, located here: [Format Data For Specify](https://github.com/NHMDenmark/DaSSCo-Digitization/data_processing/SpeciesWeb/formatDataForSpecify.py).

Export files that are ready to be post-processed can be found in the folder: 3.ReadyForOpenRefine/AU_Herbarium, located in the Data folder on the N-drive. As part of the script, between two and three spreadsheets (described below) will be created to be imported to Specify, and the CSV file ending in either _checked or _checked_corrected will be moved to the 6.Archive/AU_Herbarium folder. 

1. A TSV file will always be created by the script. This is the formatted data that is ready for import to Specify. This file will retain the original filename, with _checked.csv or _checked_corrected.csv replaced by _processed.tsv, and it will be saved in the folder: 5.ReadyForSpecify/AU_Herbarium.

2. If any named organisms in the export are synonyms, a second CSV file will also be created with the associated taxonomic information of the accepted names. This file will also retain the original filename, with _checked.csv or _checked_corrected.csv replaced by _synonymsToImport.csv and it will be saved in the same folder as the TSV file.

3. A CSV file will always be created listing all of the unique taxa in the exported file. This can be used to confirm or correct information like author names in the Specify taxon tree after import. This file will also retain the original filename, with _checked.csv or _checked_corrected.csv replaced by _unique_taxa.csv and it will be saved in the same folder as the TSV file.


#### Protocol:

1. Open a terminal window and navigate to where the script is stored
2. Run the script using the command `python formatDataForSpecify.py` 
3. The original CSV file (ending with _corrected.csv or checked_corrected.csv) is automatically moved to the 6.Archive/AU_Herbarium folder
4. The formatted TSV file and unique_taxa CSV file are automatically moved to the 5.ReadyForSpecify/AU_Herbarium folder
5. If a synonyms file is created, this is also saved to the 5.ReadyForSpecify/AU_Herbarium folder

### Import to Specify

Export files that are ready to be imported to Specify can be found in the 5.ReadyForSpecify folder in the Data folder on the N-drive.

If a synonyms import file was created, this will need to be imported to Specify first so the synonymized taxa can be linked. Afterwards, the specimen records can be imported.

#### Protocol (Importing Synonyms)

To import synonyms, the synonym importer tool from the Sp7ApiToolbox repository should be used. You can find this repo and the full instructions for using the tools contained therein by clicking [here](https://github.com/NHMDenmark/Sp7ApiToolbox).

1. If this is the first time using the synonym importer tool, clone the Sp7ApiToolbox repository to your computer
2. Create the config file as described in the Sp7ApiToolbox repo readme
3. Save the synonyms file to the data folder in the cloned repository
4. Correct usage of the tool is: ``python main.py [mode from config file]``
5. Follow the prompts, selecting the synonym importer tool and the correct data file when asked

#### Protocol (Importing Specimen Records):

1. Log into the Specify7 site that corresponds to the institution at which the data was generated
2. Choose the relevant collection (if applicable)
3. Click on Workbench in the leftside menu
4. Click on Import File, then choose the formatted TSV file from the last step (or drag and drop the file to be imported)
5. You will see a preview of the dataset
6. Check that everything looks as expected. If it does, click Import File
7. You need to define the mapping for the dataset. Click Create 
8. If you have imported DaSSCo export files before, click on Choose Existing Plan (if you do not have access to an existing plan, select Collection Object as a base table and find the mapping plan for the specific collection [here](https://github.com/NHMDenmark/DaSSCo-Digitization/data_processing/Specify/import_workbench_mapping))
9. Choose one of the previous imports to use the mapping plan from that import, if not creating a new mapping plan
10. Check the mapping to see if everything looks as expected. If it does, click Save
11. The data needs to go through a validation process before being imported. Click on Validate to validate the data
12. After validation, there might be errors that need to be corrected (e.g. records that need to be disambiguated). If you correct any errors, you need to click Save and then run the validation again by clicking Validate
13. Once you get the message 'Validation completed with no errors', click Upload
14. When the upload is finished, click the Results button to check the results of the import to see if everything looks as expected. You can also click Create Record set and then go through the individual records to do a more detailed quality check of the import
15. On the N-drive, move the file that was imported from one of the subfolders in 5.ReadyForSpecify to the subfolder MoveToArchive
16. If you have additional files to import, continue with the next file. Click on Workbench in the leftside menu and repeat steps 4-15
17. When finishing an importing session, on the N-drive move all the files from MoveToArchive to the appropriate subfolder in the 6.Archive folder

