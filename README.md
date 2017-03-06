# Foodworks Scripts and Libraries

Foodworks project for Baptist University

## Marble Control

Marble Control (`marbles`) is a command line utility for managing the Foodworks system. It allows the admin to _export data_ and _generate reports_. Run `marbles --help` for a full set of instructions.

### Data Export

Prior to generating any new reports, the data for those reports needs to be exported from Google Drive onto the local machine.

There are two files which save the 'progress' the export has made, `progress.json` AND `structure.json`.

* `structure.json` - stores references to all the source sheets it's found on the foodworks system. If new sheets have been added (e.g. NGO is running a new programme, or it's the new year) run `marbles reset structure` to refresh the structure. 
* `progress.json` - stores the state of data export per NGO programme. It is updated every time a particular `NGO/programme` is being exported. But for your purposes, it is used to control whether a `NGO/programme` will be included or skipped in an export. There are three possibilities:
	- If a `NGO/programme` is not present, `marbles export --resume` will remove previous exported data, and newly export the source sheets.
	- If a `NGO/programme` is present and `false`, `marbles export --resume` will skip over it, assuming there is a bug in the source sheet. This tells you a previous export has failed.
	- If a `NGO/programme` is present and `true`, `marbles export --resume` will skip over it, assuming the export was already done. This tells you a previous export succeeded.

So a typical use will be to open `progress.json`, and remove the `NGO/programme` you wish to re-export, and then run

```bash
marbles export --resume
```

Alternatively, you can either reset everything and start a fresh export, running 

```bash
marbles export
```

This might take up to half a day. Or if you need fine-grained control over the NGO to export for, you may use

```bash
marbles export --resume --for NGO
```

where `NGO` is the NGO id, e.g. `TSWN` or `FoodLink`.

### FoodShare Report Generation
### FoodLink Report Generation
#### Funder Reports
#### Donors & Beneficiary Reports 
### Tableau Report Generation









# Deprecated Instructions


# BE SURE TO REMOVE `progress.json` AND `structure.json` WHEN EXPORTING FROM SOURCE

## Key Scripts

* `scripts/export_source_data.py` - Generate CSVs based on the Source data NGOs have contributed. 
* `scripts/report/generate_tableau_data_model.py` - Transform the records into a format that can be consumed by Tableau

Run the scripts from within the root folder so scripts have access to the `core` pacakge. E.g. to regenerate the CSVs:

`python scripts/export_source_data.py`

## Repository Structure

* `archive` - Old analysis
* `core` - Reusable libraries developed for the project
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


## Relevant Steps for Data Entry & Processing

1. All data needs to be filled in by **NGOs** (collection, distribution, AND processing) by the `5th`

1. Data needs to be checked and validated by **Wilson** (holes, gaps, missing or misplaced information)

1. Data needs to be passed to **Droste** for exporting and processing.

1. **Daisy** needs to do the mapping 

1. Reports and Data Visualisation to be returned to NGOs on the `8th`
