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

```
marbles export --resume --for NGO
```

where `NGO` is the NGO id, e.g. `TSWN` or `FoodLink`.

### FoodLink Report Generation

FoodLink's practices are different from the FoodShare NGOs, subsequently the commands for generating their reports differ slightly.

#### Funder Reports

FoodLink is funded by ECF. Each van they operate has it's own sheet, but the ECF reports are rolled up for both `collection` and `distribution` stages respectively. They are exported as Excel files and stored as `FoodWorks/Reports/Funder/FoodLink Funder Reports/YEAR/MONTH/NGO.YEAR.MONTH.STAGE.ECF.Report.xlsx`

```
marbles report funder --for FoodLink
```

#### Donor Reports 

FoodLink provides its donors with reports on the `food` and `amenities` donated over the most recent period. The reports are simple graphs which are exported to a PDF, and stored as `FoodWorks/Reports/Donors/FoodLink Donor Reports/YEAR/MONTH/NGO.TYPE.report.pdf`

```
marbles report donor --for FoodLink
```

### FoodShare Report Generation

FoodShare reports are generated based on all three stages. They are exported as Excel files and stored as `FoodWorks/Reports/Funder/NGO Funder Reports/DATE/FoodShare.NGO.PROGRAMME.DATE.report.xlsx`

```bash
 marbles report funder --all --skip FoodLink
```

Here we skip FoodLink as they are on a different reporting cycle.

### Tableau Report Generation

TODO : DICKSON TO PROVIDE DOCUMENTATION

### Dealing with Errors

Whenever possible, we have attempted to provide a descriptive error message, and a suggestion of where to start looking. Errors are typically due to data entry issues, and may be easily spotted and correct when viewing the last sheet marble was on before it stopped working.

If you correct a data entry issue, you will have to re-export the data for that particular NGO / programme.

### Misc Commands

In addition to exporting data and generating reports, `marbles` also has some helper commands.

To print the source structure in the terminal, run 

```bash
marbles list structure
```

To print the export progress in the terminal, run 

```bash
marbles list progress
```

There's a couple of reset commands which are self-explanatory

```bash
marbles reset all         # Reset export progress, deletes exported data
marbles reset structure   # Run if new sheets were added to Source
marbles reset progress    # Run if you want to reset the export progress
marbles reset export      # Removes all exported dat
```


# Development

## Key Scripts

* `bin/marbles` - Command Line Interface for the platform
* `scripts/export_source_data.py` - Generate CSVs based on the Source data NGOs have contributed. 
* `scripts/report/generate_tableau_data_model.py` - Transform the records into a format that can be consumed by Tableau

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

## Installation

### Additional Dependencies (Fedora Only)

`sudo dnf install readline readline-devel python2-devel`

### Setup Environment

`conda env create -f environment.yml`
`conda activate foodworks`

### Add marbles to path

`pathadd $HOME/code/foodworks/bin`

### Authenticate with your Google Account

Copy paste the `get_credentials` function from [core/drive.py#L38](https://github.com/drostehk/foodworks/blob/master/core/drive.py#L38) and run `get_credentials()` in a python session to generate credentials.

### Symlink Reports

Link up the `Report` directory, e.g. from the root

`ln -s ~/m@droste.hk/FoodWorks/Reports/ data/`

or

```bash
ln -s /Users/daisytam/Google\ Drive/FoodWorks/Reports data/Reports
```

### wkhtmltopdf

```bash
sudo dnf install wkhtmltopdf
```

Download and install [wkhtmltopdf](http://wkhtmltopdf.org/downloads.html)