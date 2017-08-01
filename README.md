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


## Installation

### Setup Environment

`conda env create -f environment.yml`

### Add marbles to path

`pathadd $HOME/code/foodworks/bin`
