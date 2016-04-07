# Foodworks Scripts and Libraries

Foodworks project for Baptist University

# BE SURE TO REMOVE `progress.json` AND `structure.json` WHEN EXPORTING FROM SOURCE

## Key Scripts

* `scripts/export_source_data.py` - Generate CSVs based on the Source data NGOs have contributed. 
* `scripts/report/generate_tableau_data_model.py` - Transform the records into a format that can be consumed by Tableau

Run the scripts from within the root folder so scripts have access to the `core` pacakge. E.g. to regenerate the CSVs:

`python scripts/export_source_data.py`

## Repository Structure

* `archive` - Old analysis
* `core` - Reusable libraties developed for the project
* `scripts` - Executable scripts to transform data and generate reports
    - `report` - Generate Reports
    - `transform` - Processing Scripts for Raw Data
    - `utility` - Small helper functions


## Data Structure

There are three types of data that we have in our system, all hosted on Google Drive.

1. `RAW` Data NGOs have captured in Excel sheets and other formats. These files do not follow our templates, but can be used for automatic transformation into our own records
1. Structured Data which NGOs have captured in our `SOURCE` system following any of our agreed upon schemas. 
1. Processed Data which has been taken from the Sourcing system and / or Raw Data in order to generate `RECORDS` from which reports can be generated.

Files handed over by NGOs should only ever be dropped into `RAW`, the sheets in `Source` should copied from the Master Template, and the CSVs in `RECORDS` should only ever be generated programatically.  

## Caching & Saving Progress

### structure.json

Once Google Drive has been scanned for its FoodWorks Source directory structure, the structur, along with all the IDs of the Sheets are stored in `structure.json`. These are the only sheets which will be processed if this file exists. So best to delete it when generating a fresh export.

### progress.json

Minor flaws in the data entry of the Source system will halt the export and prompt the user to inspect the data. However, to save progress on the clean spreadsheets, `progress.json` stores the combinations of `stage`, `ngo`, `programme` which have succesfully been exported.