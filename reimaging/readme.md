This folder contains basic workflow information and scripts for finding and organizing data about specimens that need to be re-imaged.

The add_location_and_taxonomy sub-folder contains a script that takes a list of specimens that should be re-imaged, then adds storage location and taxonomy information, and splits the specimens out into one separate sheet per institution. 

The missing_images sub-folder contains a script that takes all barcodes in DigiApp exports from a specific date range, searches for these barcodes in the associated barcode-guid matching databases, and compiles a list of all barcodes that are missing from the databases. If there is no matching barcode in the database, then there is no detected image for the specimen. 