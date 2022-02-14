# ICCAD2-Data-Extraction

R scripts used to locate, extract, clean, and inspect variables stored in a Philips ICIP system.

### Branches

Main: the most recent scripts used to extract the cardiac and general ICU variables.

PhD Toolkit: the scripts created during my PhD which were used to extract the cardiac ICU variables.

### Main Branch Scripts

Patient Data Processing Toolkit: Functions designed to find where and how data is stored in the back-end of the Philips ICIP system.

CICU Patient Data Extraction: Using the toolkit to retrieve all patient data for around 90 variables from the back-end system.

CICU Patient Data Time Point Extraction: Using the toolkit to extract patient data at 8am on a particular day.

### Plyr and dplyr interaction
Due to overlaps between the "plyr" and "dplyr" packages, only plyr should be loaded at the beginning of a session. It is best practice to begin in a fresh session with no packages loaded.

### Open Database Connectivity (ODBC) set up
The “OpenConn” function connects to the back-end database via the RODBC package.

The database is here designated as “SQL Server1” as this corresponds to the name in Window’s own internal ODBC data source list.

You should set up your own database in Window's internal ODBC data source list before running these scripts.

The uid (user Id) and pwd (password) arguments of the function should also be filled out according to your database.
