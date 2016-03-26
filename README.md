# foodworks
Foodworks project for Baptist University


## Data Structure

There are three types of data that we have in our system

1. `RAW` Data NGOs have captured in Excel sheets and other formats. These files do not follow our templates, but can be used for automatic transformation into our own records
1. Structured Data which NGOs have captured in our `SOURCE` system following any of our agreed upon schemas. 
1. Processed Data which has been taken from the Sourcing system and / or Raw Data in order to generate `RECORDS` from which reports can be generated.

Files handed over by NGOs should only ever be dropped into `RAW`, the sheets in `Source` should copied from the Master Template, and the CSVs in `RECORDS` should only ever be generated programatically.  

## Temp Description

### CSV Generation

`foodworks / reports / regen_data.py`

### FoodShare Report & Tableau Data Model

`foodworks / reports / foodworks / report`

### Food Link Bread Report CSV

`foodworks / reports / foodlinks / transform`
