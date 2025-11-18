# Import protocol: from DigiApp to Specify


### Overview

The import protocol consists of three steps:

1. Checking of the export files from the DigiApp and resolving any issues
2. Post-processing via a Python script
3. Importing to Specify

The Data folder on the N-drive contains all the export files generated from the DigiApp and has been structured with subfolders to accomodate the different steps of the protocol. Additional subfolders have been implemented for the separate institutions/collections involved in DaSSCo. Export files should always be placed in the appropriate subfolder according to the institution/collection that the digitized specimens belong to.

In the 6.Archive folder within each institution/collection subfolder, further subfolders for each version of the DigiApp have been implemented. Export files being moved to the 6.Archive folder should always be placed in the appropriate subfolder according to the institution/collection that the digitized specimens belong to AND according to the version of the DigiApp used at the time the specimens were digitized.

No automations are currently implemented as part of the import protocol. A folder has been added to the folder structure called 4.ReadyForScript. The plan is to have a monitoring script running on this directory in the future but currently the folder is not being used as part of the protocol.

### Checking of export files from DigiApp

Exported files from the DigiApp are saved to the N-drive in the appropriate subfolder in the 1.FromDigiApp folder. 

As part of the new folder structure, subfolders have been implemented for the separate institutions/collections involved in DaSSCo. Files should always be placed in the appropriate subfolder according to the institution/collection that the digitized specimens belong to (will be clear from the filename).

#### Protocol:

1.	Go to the 1.FromDigiApp folder, choose a subfolder, and choose a file to check (try to find the next in line file)
2.	Once you have decided on a specific file, make a copy of that file
3.	You now have two files. Move the original file (the one without copy at the end) to the appropriate subfolder in the 6.Archive folder (according to institution, collection, and version of the DigiApp)
4.	Move the copy you made to the appropriate subfolder in the 2.BeingChecked folder
5.	Open the file and check for any errors or issues. Files with problems that require further investigation/time to resolve (e.g. files connected to github tickets) should be moved to the subfolder FilesWithProblems until the problem has been resolved
6.	When you are done checking the file, change the suffix of the filename from original_copy to checked (no corrections made) OR checked_corrected  (corrections/modifications have been made to the file)
7.	Move the file to the appropriate subfolder in the 3.ReadyForOpenRefine folder


### Post-processing

Post-processing is done via a Python script, located here: [Format Data For Specify](https://github.com/NHMDenmark/DaSSCo-Digitization/data_processing/DigiApp/format_data_for_specify). Locate the script that matches the institution/collection you are working on. 

Export files that are ready to be post-processed can be found in the 3.ReadyForOpenRefine folder in the Data folder on the N-drive. There should be no need to check the data in detail before starting the post-processing since the export file should have been checked thoroughly before being moved to this folder.


#### Protocol:

1. Open a terminal window and navigate to where the script is stored
2. Run the script using the command `python formatDataForSpecify.py` 
3. The original CSV file (ending with _corrected.csv or checked_corrected.csv) is automatically moved to the 6.Archive folder for the specified institution/collection
4. The formatted TSV file is automatically moved to the 5.ReadyForSpecify folder for the specified collection

### Import to Specify

Imports to Specify are performed using the Specify Workbench.

IMPORTANT! The dataset records you are about to import into Specify have had their catalog numbers reserved for this task. You will need to identify the range of catalog numbers in the export file and ask the Specify team to delete those reserved dummy records in Specify. Please also mention which institution and collection the dummy records should be deleted from. Submit this to specify@snm.ku.dk for deletion. When they have been deleted, continue with the import.

Export files that are ready to be imported to Specify can be found in the 5.ReadyForSpecify folder in the Data folder on the N-drive.

#### Protocol:

1. Log into the Specify7 site that corresponds to the institution that the data was generated at 
2. Choose the relevant collection 
3. Click on Workbench in the leftside menu
4. Click on Import File, then choose one of the files containing the reserved catalog numbers you had deleted from one of the subfolders in the 5.ReadyForSpecify folder
5. You will see a preview of the dataset
6. Check that everything looks as expected. If it does, click Import File
7. You need to define the mapping for the dataset. Click Create
8. If you have imported DaSSCo export files before, click on Choose Existing Plan (if you do not have access to an existing plan, select Collection Object as a base table and find the mapping plan for the specific collection [here](https://github.com/NHMDenmark/DaSSCo-Digitization/data_processing/Specify/import_workbench_mapping))
9. Choose one of the previous imports to use the mapping plan from that import, if not creating a new mapping plan
10. Check the mapping to see if everything looks as expected. If it does, click Save
11. The data needs to go through a validation process before being imported. Click on Validate to validate the data
12. There might be errors showing up at this point (e.g. records that need to be disambiguated). If you correct any errors, you need to click Save and then run the validation again by clicking Validate
13. Once you get the message 'Validation completed with no errors', click Upload
14. When the upload is finished, click on Results to check the results of the import to see if everything looks as expected. You can also click Create Record set and then go through the individual records to do a more detailed quality check of the import
15. On the N-drive, move the file that was imported from one of the subfolders in 5.ReadyForSpecify to the subfolder MoveToArchive
16. If you have additional files to import, continue with the next file. Click on Workbench in the leftside menu and repeat steps 4-15
17. When finishing an importing session, on the N-drive move all the files from MoveToArchive to the appropriate subfolder in the 6.Archive folder

