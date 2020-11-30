# CICU-Data-Extraction
Patient Data Processing Toolkit: All the functions required to find where and how data is stored in the back-end of the Philips ICIP system. Further functions cover data extraction, cleansing, and inspection.

Patient Data Extraction: Using the toolkit to retrieve all patient data for around 90 variables from the back-end system

Patient Data Time Point Extraction: Using the toolkit to extract patient data at 8am on a particular day

Required scripts:
These scripts use the “RODBC”, “plyr”, and “dplyr” packages.
The scripts will require them when necessary, and due to overlaps between the plyr and dplyr packages, only plyr should be loaded at the beginning of a session.

ODBC Set up:
The “DBConn” script either connects or disconnects to the back-end database via the RODBC package.
The database is here designated as “SQL Server1” as this corresponds to the name in Window’s own internal ODBC data source list.
You should set up your own database in Window's internal ODBC data source list before running these scripts.
The uid (user Id) and pwd (password) arguments of the DBConn (Database Connect) function should also be filled out according to your database.
