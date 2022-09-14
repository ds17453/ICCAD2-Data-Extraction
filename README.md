# ICCAD2-Data-Extraction

R scripts used to locate, extract, clean, and inspect variables stored in a Philips IntelliVue Clinical Information Portfolio (ICIP) system.

### Branches

Main: the most recent scripts used to extract the cardiac and general ICU variables.

Pre-GICU Expansion: updated versions of the scripts used when only extracting the cardiac ICU variables

PhD Toolkit: the scripts created during my PhD which were used to extract the cardiac ICU variables.

### Main Branch Files

FILL THIS IN

### Plyr and dplyr interaction
Due to overlaps between the "plyr" and "dplyr" packages, only plyr should be loaded at the beginning of a session. It is best practice to begin in a fresh session with no packages loaded.

### Open Database Connectivity (ODBC) set up
The “OpenConn” function connects to the back-end database via the DBI package.

The database is here designated as “SQL Server1” as this corresponds to the name in Window’s own internal ODBC data source list.

You should set up your own database in Window's internal ODBC data source list before running these scripts.

The uid (user Id) and pwd (password) arguments of the function should also be filled out according to your database.
