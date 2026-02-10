QA_Images_Issues spreadsheets are housed on the N drive at: DaSSCo\Workflows and workstations\Quality assurance and control\QA_spreadsheets.
Folder structure is as follows:
1_QA_in_progress
2_QA_done
3_edits_and_reimaging_in_progress
4_reimaging_done
backup

Every quarter, a list of barcoded specimens without images will be added to the appropriate QA_Images_Issues spreadsheet, and the storage locations and taxonomy will be added to this spreadsheet for all specimens that need to be reimaged. 

For adding images missing:
1. The script to compile a list of specimens missing images is [searchBarcodesInDatabase.py](missing_images/searchBarcodesInDatabases.py).
2. The following variables will need to be adjusted in the .env file each time the script is run: collection, start_date, end_date, database1, database2, folder_path. The date fields specify a date range to search, and the others are all collection-dependent. The folder_path links to the DigiApp data archive folder.
3. Two csv files are generated each time the script is run: [collection]_[date]_foundBarcodesWithSource.csv and [collection]_[date]_barcodesMissingFromDB.csv. The missing csv will include barcodes and DigiApp exports they are found in. The found csv will include these items, as well as the database the barcode was found in.
4. Sometimes an entire DigiApp export will be missing from the database. Usually this just means that those images have not been ingested yet. Further research is needed before these are added to the QA_Images_Issues spreadsheet.
5. All other barcodes are added to the 'Specimens' tab of the QA_Images_Spreadsheet with the following information:
    - Workstation: Use any workstation that matches the collection
    - Barcode
    - GUID: following the format: 'missing_xx' where 'xx' is a unique, sequential number for the spreadsheet
    - Reason Flagged: Image missing
    - Investigation Outcome: No image in folder
    - Follow-up Action Required: Re-image

For adding location and taxonomy:
The spreadsheets are ready when they are in the 2_QA_done folder.

1. Make a copy of the QA_Images_Issues spreadsheet, add today's date to the end in YYYYMMDD format, and move it to 'backup' folder.

2. Update file_path in .env file and run [addLocationAndTaxonomy.py](add_location_and_taxonomy/addLocationAndTaxonomy.py).

3. Open updated workbook in excel and create new column 'Status' after 'Follow-up Action Required' on Specimens tab.

4. Paste formula for inserting 'reimaged' in 'Status' (paste into J2 and drag down):
```
=IF(
   OR(
      IFERROR(COUNTIFS(INDIRECT("'Reimage_Needed_AU_Herbarium'!H:H"); C2; INDIRECT("'Reimage_Needed_AU_Herbarium'!A:A"); "yes"); 0) > 0;
      IFERROR(COUNTIFS(INDIRECT("'Reimage_Needed_NHMD_Herbarium'!H:H"); C2; INDIRECT("'Reimage_Needed_NHMD_Herbarium'!A:A"); "yes"); 0) > 0;
      IFERROR(COUNTIFS(INDIRECT("'Reimage_Needed_NHMD_PinnedInsects'!H:H"); C2; INDIRECT("'Reimage_Needed_NHMD_PinnedInsects'!A:A"); "yes"); 0) > 0;
      IFERROR(COUNTIFS(INDIRECT("'Reimage_Needed_NHMA_PinnedInsects'!H:H"); C2; INDIRECT("'Reimage_Needed_NHMA_PinnedInsects'!A:A"); "yes"); 0) > 0
   );
   "reimaged";
   ""
)
```

5. Save workbook as .xlsm (macro-enabled).

6. For each of the reimage_needed sheets:
   - right-click and select "view code"
   - paste into the pop-up box:
```
Private Sub Worksheet_Change(ByVal Target As Range)

Dim rng As Range
Set rng = Intersect(Target, Me.Columns("A"))

If rng Is Nothing Then Exit Sub

Application.EnableEvents = False

Dim cell As Range
For Each cell In rng
    ' Only act on rows below header
    If cell.Row > 1 Then
        ' If "yes" is entered and date cell is empty, stamp date
        If LCase(cell.Value) = "yes" And Me.Cells(cell.Row, "B").Value = "" Then
            Me.Cells(cell.Row, "B").Value = Date
        End If
    End If
Next cell

Application.EnableEvents = True

End Sub
```

7. Taxonomy and storage location will need to be added for all specimens that were imaged without a barcode (unless the barcode is known and entered into the sheet.) Because we have the datetime for date_asset_taken of these images, the taxonomy and storage location can be estimated based on an image taken right before or right after this one. Search the barcode-guid matching database for this datetime and find a barcode of a specimen imaged close to the same time. Then, search Specify for that barcode to obtain the taxonomy and storage location. Add this to the spreadsheet for the specimen to be re-imaged. 

8. Drop rows with duplicate barcodes in the re-image sheets. 

9. Copy updated workbook to 3_reimaging_in_progress.
