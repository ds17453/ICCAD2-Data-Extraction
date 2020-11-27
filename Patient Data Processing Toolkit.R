# Introduction ------------------------------------------------------------

# This script holds all the SQL queries that we use to gather patient data.
# Other scripts take these dataframes and clean them/extract patient data at specific times.
# This patient data is saved in: Downloads/Patient Data/Patient Data.Rdata

# load("C:/Users/shillandun/Downloads/Patient Data/Patient Data.RData")

# Connecting to the database ----------------------------------------------

# Use the RODBC package to connect to an ODBC data source.
# Our data source is an SQL database hosted by the Bristol Royal Infirmary
# You must define the ODBC data source within your operating system; look online for a guide
# We have defined it here as "SQL Server1"

library(RODBC);library(plyr);library(data.table)

# install.packages("ODBC")
# library(DBI)

DBConn <- function(x){
  
  if(! x %in% 0:1){stop("Please supply a 1 (connect) or 0 (disconnect)")}
  
  # Ensure RODBC package is loaded
  require(RODBC)

  # Connect to SQL Server
  if(x == 1){
    writeLines(text = "Connecting to CICU database...")
    conn <<- odbcConnect("SQL Server1", uid="CISReportingDB", pwd="CISReportingDB_123")
    # If the connection succeeds, the resulting object will be of "RODBC" class
    # Use this to report on the connection
    if(class(conn)=="RODBC"){writeLines("Connected to CICU database")}else{
      writeLines("Connection failed")}}
  
  # Disconnect to SQL Server
  if(x == 0){
    writeLines(text = "Disconnecting from the CICU database")
    odbcClose(conn)
    rm(conn,pos = 1)}
}

# Basic Patient Information -----------------------------------------------

# Here we extract two patient demographics (age and gender) alongside their first and last
# recorded heart rate.
# Count is the number of the recorded heart rates

DBConn(1)
LoSS <- sqlQuery(conn, "SELECT a.encounterId, a.attributeId, d.patientId, d.age, d.gender,
  MIN(charttime) AS [Start_date], MAX(charttime) AS [End_date], 
  COUNT(*) AS [Count]
  FROM PtAssessment a
  JOIN D_Encounter d on a.encounterId=d.encounterId
  WHERE attributeId IN ('27312','12754') -- use both attributes for HR
  AND clinicalUnitId = '8'
  GROUP BY a.encounterId,a.attributeId,d.patientId,d.age,d.gender");
DBConn(0)

# Remove patient with no age
LoSS <- LoSS[!is.na(LoSS$age),];
LoSS <- LoSS[!is.na(LoSS$gender),];

# Calculate Length of Stay in hours
LoSS$LoS <- round(difftime(LoSS$End_date,LoSS$Start_date,units = "hours"))


# Official Hospital Records vs Heart Rate ---------------------------------

# We want to drop any patients with more than 36 hours between admission and first recorded heart rate
# Admission times just indicate paperwork, whereas first/last heartbeat indicates physical presence in a bed

# Change start_date to be according to official hospital records validated by heart rate. Still use end heart rate as end.
DBConn(1)
Hospital_Census <- sqlQuery(conn,"
SELECT encounterId,inTime,outTime,admitSource,lengthOfStay,isDischarged,isDeceased,isTransferred 
FROM PtCensus
WHERE clinicalUnitId = '8'")
DBConn(0)

# Join the two to compare official hospital discharge records (InTime/OutTime) 
# to first/last recorded heartbeat (Start_date/End_date)
A <- join(LoSS,Hospital_Census,by="encounterId",type="inner")

# Finding re-admissions patients (multiple stays with same encounterId)
A2 <- A[A$encounterId %in% subset.data.frame(A, duplicated(A$encounterId))$encounterId,]

# Number of unique patients with multiple stays on the unit
length(unique(A2$encounterId))

# Transform backend LoS into days
# UPDATE: USING 24 RATHER THAN 60 HERE
A2$lengthOfStay <- round(A2$lengthOfStay/24)

# Find difference between first HR timestamp and official hospital records
A2$StartDateDiff <- round(as.numeric(difftime(A2$Start_date,A2$inTime,units = "hours")),2)

# Find patients with 36 hours between first HR timestamp and official hospital records
A3 <- c(subset.data.frame(A2, StartDateDiff>36)$encounterId,
        subset.data.frame(A2, StartDateDiff<(-36))$encounterId)
# Number of patients (for reporting)
length(unique(A3))

# Drop these from our current investigation of duplicated patients and from the official record
A2 <- A2[!A2$encounterId %in% A3,]
LoSS <- LoSS[!LoSS$encounterId %in% A3,]
rm(A3)


# Remaining re-admission patients: find earliest record of entry on to the ward and latest record of discharge
# UPDATE: Now using the minimum start_date and maximum end_date rather than the minimum of either the start_date/InTime and maximum of either the end_date/OutTime
i <- 1
for(i in 1:length(unique(A2$encounterId))){
  
  # Pull all duplicates of each unique patient
  A3 <- A2[A2$encounterId==unique(A2$encounterId)[i],]
  
  # Overwrite their start and end date with their respective minimum or maximum
  LoSS$Start_date[LoSS$encounterId==unique(A2$encounterId)[i]] <- min(A3$Start_date)
  LoSS$End_date[LoSS$encounterId==unique(A2$encounterId)[i]] <- max(A3$End_date)
  
  # Remove all duplicates, leaving only the original
  LoSS[LoSS$encounterId==unique(A2$encounterId)[i],][-1,] <- NA
  LoSS <- LoSS[!is.na(LoSS$Start_date),]
  
  # Tidy environment
  rm(A3,i)
}

# Keep Hospital_Census, need the number of unique patients for later data extraction
rm(A,A2)

# Remove extraneous columns from LoSS
LoSS <- LoSS[,c("encounterId","patientId","age","gender","Start_date","End_date","LoS")];



# Obtain all relevant interventionIds and attributeIds -----------------------

# The back end is messy, with no documentation to support data extraction
# Variables are duplicated, and stored under different names during different time periods


# load("C:/Users/shillandun/Downloads/Patient Data/All Attribute and InterventionId Combos.RData")

# Start with all intervention and attribute Ids
DBConn(1)
IdIntervention <- sqlQuery(conn,"SELECT interventionId,shortLabel,longLabel,conceptLabel FROM D_Intervention")
IdAttribute <- sqlQuery(conn,"SELECT attributeId,shortLabel,longLabel,conceptLabel FROM D_Attribute")
DBConn(0)

# Appropriately label both dataframes with "intervention" or "attribute"
#   as they both use shortLabel/longLabel/conceptLabel
colnames(IdIntervention) <- c("interventionId",paste0("intervention",colnames(IdIntervention)[-1]))
colnames(IdAttribute) <- c("attributeId",paste0("attribute",colnames(IdAttribute)[-1]))

# Want to find all the CICU attribute/interventionIds in seven tables
# TotalBalance,Demographic,Assessment,LabResult,Medication,SiteCare,Treatment
# All are same format except for TotalBalance, which only uses interventionId
# For each table we want to find:
# 1) the Ids used
# 2) the number of unique patients with data under each combination

# Total Balance table:
DBConn(1)
IdsTotalBalance <<- sqlQuery(
  conn,"SELECT DISTINCT interventionId, COUNT(DISTINCT encounterId) AS uniqueEncounterIds FROM (
    SELECT encounterId,interventionId
    FROM PtTotalBalance 
    WHERE clinicalUnitId = '8')a
  GROUP BY interventionId")
DBConn(0)

# Later we bind all the used Ids together, and so we need an attributeId and label column
# Leave the attributeId column as 0
IdsTotalBalance$attributeId <- as.integer(0)
# Label
IdsTotalBalance$table <- "TotalBalance"
# Re-order the columns: Ids, patients, sorting label
IdsTotalBalance <- IdsTotalBalance[,c("interventionId","attributeId","uniqueEncounterIds","table")]

# The remaining tables have the same format, but contain different variables
# Meaning we pass each table to the following function to find the used Ids combinations and the number of patients for each id combination
IdDataPull <- function(table){
  # Pass the table to the query text
  queryText <- paste0("SELECT DISTINCT interventionId,attributeId, COUNT(DISTINCT encounterId) AS uniqueEncounterIds FROM (
    SELECT encounterId, interventionId, attributeId
    FROM Pt",table," 
    WHERE clinicalUnitId = '8')a
    GROUP BY interventionId,attributeId")
  
  # Connect to the database, run the query, and then disconnect
  DBConn(1)
  A <- sqlQuery(
    conn,queryText)
  DBConn(0)
  
  return(A)
}

# Make an empty list and add each table to it
Ids <- list()
for(x in c("Demographic","Assessment","LabResult","Medication","SiteCare","Treatment")){
  A <- IdDataPull(x)
  A$table <- x
  Ids[[x]] <- A
  rm(A,x)
}

# Add the totalbalance table to the list of dataframes
Ids$TotalBalance <- IdsTotalBalance
rm(IdsTotalBalance)

# Collapse the list of dataframes
TotalIdCombos <- ldply(Ids,data.frame)
rm(Ids)

# Join Id info on 
TotalIdCombos <- join(TotalIdCombos,IdAttribute,by="attributeId")
TotalIdCombos <- join(TotalIdCombos,IdIntervention,by="interventionId")

# Re-order the columns
TotalIdCombos <- TotalIdCombos[,c("interventionId","attributeId", "interventionshortLabel", "attributeshortLabel","interventionlongLabel", "attributelongLabel","interventionconceptLabel", "attributeconceptLabel","table","uniqueEncounterIds")]

# Turn the factors to characters
TotalIdCombos[sapply(TotalIdCombos,is.factor)] <- lapply(TotalIdCombos[sapply(TotalIdCombos,is.factor)], as.character)

# format to camelCase
colnames(TotalIdCombos) <- c("interventionId", "attributeId", "interventionShortLabel", 
                             "attributeShortLabel", "interventionLongLabel", "attributeLongLabel", 
                             "interventionConceptLabel", "attributeConceptLabel", "table","uniqueEncounterIds")

# Clinical contact: Remove useless info
TotalIdCombos <- TotalIdCombos[!TotalIdCombos$attributeShortLabel %in% 
                                 c('Reference Range','Specimen Catalog',
                                   'Specimen Received Date Time','Specimen','Specimens'),]



# Percentage of Id combinations which are used for more than 10, 100, and 1,000 patients
quantile(TotalIdCombos$uniqueEncounterIds,probs = 0:20/20)

nrow(TotalIdCombos[TotalIdCombos$uniqueEncounterIds>10,])/nrow(TotalIdCombos)*100
nrow(TotalIdCombos[TotalIdCombos$uniqueEncounterIds>100,])/nrow(TotalIdCombos)*100
nrow(TotalIdCombos[TotalIdCombos$uniqueEncounterIds>1000,])/nrow(TotalIdCombos)*100

# We subset to any data with more than 5 unique patients
TotalIdCombos <- TotalIdCombos[TotalIdCombos$uniqueEncounterIds>5,]

# Alternate DF with only the shortLabels
TotalIdCombosShort <- subset.data.frame(TotalIdCombos, select=-c(interventionLongLabel,attributeLongLabel,
                                                                 interventionConceptLabel,attributeConceptLabel))

save(list=ls()[ls()%in%c("TotalIdCombos","TotalIdCombosShort","IdAttribute","IdIntervention")],
     file = "C:/Users/shillandun/Downloads/Patient Data/All Attribute and InterventionId Combos.RData")


# Data Pulls from SQL server ----------------------------------------------

# Make a generic script for pulling from each of the seven tables
# Look through the releveant part of TotalComboIds for anything to do with your variable

# Give it a word and it looks for similar words in TotalIdCombos short/longlabels
# Returns IdSearchIntIds and IdSearchAttIds
# Prints the shortlabels so you can see if its pulling anything you don't want

IdSearch <- function(TableSearch,Variable){
  
  require(plyr)
  require(data.table)
  # Make a list to store all outputs
  IdSearchOut <<- list()
  
  # Narrow TotalIdCombos to just the table you want to search
  A <- TotalIdCombos[TotalIdCombos$table==TableSearch,]
  
  # Search through short and long labels for your term
  A1 <- A[(A$interventionShortLabel %like% `Variable`|
           A$attributeShortLabel %like% `Variable`|
           A$interventionLongLabel %like% `Variable`|
           A$attributeLongLabel %like% `Variable`),]
  
  # Pull back the intervention and attribute Ids
  # Ensure there's an apostrophe around them
  IdSearchOut$IdSearchIntIds <<- unique(A1$interventionId)
  IdSearchOut$IdSearchAttIds <<- unique(A1$attributeId)
  
  # Data frames for checking further information on the retrieved terms
  IdSearchOut$IdSearchIntIdsDetail <<- unique(join(data.frame("interventionId" = unique(A1$interventionId)),subset.data.frame(TotalIdCombos,select=c(interventionId,interventionShortLabel,interventionLongLabel,interventionConceptLabel)),by="interventionId"))
  IdSearchOut$IdSearchAttIdsDetail <<- unique(join(data.frame("attributeId" = unique(A1$attributeId)),subset.data.frame(TotalIdCombos,select=c(attributeId,attributeShortLabel,attributeLongLabel,attributeConceptLabel)),by="attributeId"))
  
  A11 <- subset.data.frame(A1,select=c(interventionId,interventionShortLabel,attributeId,attributeShortLabel,uniqueEncounterIds))
  A11 <- A11[order(A11$uniqueEncounterIds,decreasing = T),]
  A11$percentage <- round(A11$uniqueEncounterIds/length(unique(Hospital_Census$encounterId)),4)*100
  colnames(A11)[colnames(A11)=="uniqueEncounterIds"] = "patients"
  
  IdSearchOut$IdSearchUsedCombos <<- A11
  
  IdSearchOut$TotalPatients <<- sum(A11$patients)
  IdSearchOut$TotalPatientsPercentage <<- sum(A11$percentage)
  
  rm(A11)
  writeLines("Id Search complete. Please investigate IdSearchOut for further information\n")
  print(IdSearchOut$IdSearchUsedCombos)
  writeLines(text = paste0("\nTotal encounterIds sum to: ",IdSearchOut$TotalPatients," (",IdSearchOut$TotalPatientsPercentage,"%)",
                           "\nThis sum does not indicate overlap"))
  }

# Finds Ids like urine|Urine and removes them
DeleteUrineIds <- function(x){
  
  require(plyr)
  require(data.table)
  A <- IdSearchOut$IdSearchIntIdsDetail[IdSearchOut$IdSearchIntIdsDetail$interventionShortLabel %like% "urine|Urine",]
  writeLines("Removing the following interventionIds")
  print(A)
  IdSearchOut$IdSearchIntIds <<- IdSearchOut$IdSearchIntIds[!IdSearchOut$IdSearchIntIds%in%A$interventionId]
  IdSearchOut$IdSearchIntIdsDetail <<- IdSearchOut$IdSearchIntIdsDetail[!IdSearchOut$IdSearchIntIdsDetail%in%A$interventionId]
  IdSearchOut$IdSearchUsedCombos <<- IdSearchOut$IdSearchUsedCombos[!IdSearchOut$IdSearchUsedCombos$interventionId%in%A$interventionId,]
  
  A <- IdSearchOut$IdSearchAttIdsDetail[IdSearchOut$IdSearchAttIdsDetail$attributeShortLabel %like% "urine|Urine",]
  writeLines("Removing the following attributeIds")
  print(A)
  IdSearchOut$IdSearchAttIds <<- IdSearchOut$IdSearchAttIds[!IdSearchOut$IdSearchAttIds%in%A$attributeId]
  IdSearchOut$IdSearchAttIdsDetail <<- IdSearchOut$IdSearchAttIdsDetail[!IdSearchOut$IdSearchAttIdsDetail%in%A$attributeId]
  IdSearchOut$IdSearchUsedCombos <<- IdSearchOut$IdSearchUsedCombos[!IdSearchOut$IdSearchUsedCombos$attributeId%in%A$attributeId,]
  
}

# Runs a generic sqlQuery with a specified set of int/att ids and table name
# The int/att ids are determined by your search. 
# We keep the search and the query separate so that you may investigate the returned terms
# to ensure your data pull is accurate
SqlMutate <- function(Table){
  
  # Note the time
  currentTime <- Sys.time()
  
  queryText <- paste0(
    "SELECT interventionId,attributeId,encounterId,chartTime,
              terseForm, verboseForm, valueNumber, unitOfMeasure, baseValueNumber, baseUOM, valueString
              FROM Pt",Table,
              " WHERE (interventionId IN (",paste0("\'",unique(IdSearchOut$IdSearchIntIds),"\'",collapse = ","),")
              AND attributeId IN (",paste0("\'",unique(IdSearchOut$IdSearchAttIds),"\'",collapse = ","),")) 
              AND clinicalUnitId = '8'")
  queryText <- gsub(queryText,pattern = "\n             ",replacement = "")
  
  # Connect to server
  DBConn(1)
  # Run query
  writeLines(text = paste0("\nQuerying..."))
  A <- sqlQuery(conn,queryText)
  
  # Print info about time spent
  writeLines(text=paste0(
    "\nQuery complete in ",
    round(difftime(Sys.time(),currentTime,units = "secs"),2)," seconds\nAt: ",format(Sys.time(),"%H:%M:%S"),"\n "),sep = "\n")
  
  # Disconnect from server
  DBConn(0)
  
  # If NAs in all possible data fields, then remove
  A <- A[!with(A,is.na(terseForm) & is.na(verboseForm) & is.na(valueNumber) & 
                  is.na(baseValueNumber) & is.na(valueString)),]
  
  return(A)
  }



# Data may be stored in one of five columns
# We need to find where it is to target this column for extraction

# The "DataFind" function details two things:
# Firstly it shows the amount of data in each column
# Secondly, it shows a cross table of unique patients per attribute and intervention combination 
#   for each column
# If a subsequent column has *exactly* the same amount of data,
#   then the data has been duplicated across the columns,
#   and we do not show a further crosstable

DataFind <- function(A){
  # Show how much data is in each column
  for(x in c("terseForm","verboseForm","valueNumber","baseValueNumber","valueString")){
    
    # count when data is present (ie - NA FALSE) is each of the above
    # Round to 10 decimal places, then truncate. Ensure an output of 100% is accurate
    x1 <- round(count(is.na(A[,x])[is.na(A[,x])==FALSE])/nrow(A)*100,10)[1,2]
    x1 <- round_any(x1,accuracy = .01,f = floor)
    
    count(is.na(A[,x]))
    x1[is.na(x1)] <- "0"
    print(paste0(x," has ",x1,"% of rows filled"));rm(x)
    # Mark used percentages - if data is in terseForm its likely to be in verboseform as well, so don't repeat
    UsedPercents <- c()}
  
  # Output is a list
  DataFindOut <<- list()
  # For any col with >20% data, crosstable shortlabels
  for(x in c("terseForm","verboseForm","valueNumber","baseValueNumber","valueString")){
    
    # count when data is present (ie - NA FALSE) is each of the above
    x1 <- round(count(is.na(A[,x])[is.na(A[,x])==FALSE])/nrow(A)*100,2)[1,2]
    x1[is.na(x1)] <- "0"
    
    # If one of the data places has more than 20% data, crosstable the shortlabels
    if(as.numeric(x1)>20){
      NewPercent <- x1
      # Don't run a cross-table for a column if its a duplicate of another column
      if(!(NewPercent %in% UsedPercents)){
        A11 <- A[!is.na(A[,x]),]
        A12 <- TotalIdCombos[TotalIdCombos$interventionId %in% A11$interventionId & TotalIdCombos$attributeId %in% A11$attributeId,]
        A14 <- with(A[A$encounterId %in% LoSS$encounterId,], tapply(encounterId[encounterId%in%LoSS$encounterId],list(interventionId,attributeId), FUN = function(x) length(unique(x))))
        
        # Alter dimension names to have shortlabels on
        # 1st list = InterventionIds
        A12 <- unique(subset.data.frame(TotalIdCombos,select=c(interventionId,interventionShortLabel),subset = TotalIdCombos$interventionId%in%dimnames(A14)[[1]]))
        A12 <- A12[order(A12$interventionId),]
        dimnames(A14)[[1]] <- paste(A12$interventionId,A12$interventionShortLabel)
        # 2nd list = attributeIds
        A13 <- dimnames(A14)[[2]]
        A13 <- unique(subset.data.frame(TotalIdCombos,select=c(attributeId,attributeShortLabel),subset = TotalIdCombos$attributeId%in%A13))
        A13 <- A13[order(A13$attributeId),]
        dimnames(A14)[[2]] <- paste(A13$attributeId,A13$attributeShortLabel)
        
        print(paste0("Unique patients for int/att combos in ",x))
        print(A14)
        writeLines("")
        DataFindOut[[x]] <<- as.data.frame(A14)
        rm(x)
      }
      UsedPercents <- c(UsedPercents,NewPercent)
    }
  }
  # Unique patients per idcombo
  A11 <- with(A[A$encounterId %in% LoSS$encounterId,], tapply(encounterId[encounterId%in%LoSS$encounterId],list(interventionId,attributeId), FUN = function(x) length(unique(x))))
  # Alter dimension names to have shortlabels on
  # 1st list = InterventionIds
  A12 <- unique(subset.data.frame(TotalIdCombos,select=c(interventionId,interventionShortLabel),subset = TotalIdCombos$interventionId%in%dimnames(A11)[[1]]))
  A12 <- A12[order(A12$interventionId),]
  dimnames(A11)[[1]] <- paste(A12$interventionId,A12$interventionShortLabel)
  # 2nd list = attributeIds
  A13 <- dimnames(A11)[[2]]
  A13 <- unique(subset.data.frame(TotalIdCombos,select=c(attributeId,attributeShortLabel),subset = TotalIdCombos$attributeId%in%A13))
  A13 <- A13[order(A13$attributeId),]
  dimnames(A11)[[2]] <- paste(A13$attributeId,A13$attributeShortLabel)
  print("Unique patients per Id Combo across whole table")
  print(A11)
}

# For further information on what is available in each Id combo, use QuantileColumn
# Pass it a data frame and a column and it will show the number of unique patients,
# whether the data fluctuates at all or is static, and then deciles of the data
# first sorted by interventionId then each attributeId that is associated with it
QuantileColumn <- function(dataframe,column){
  B <- data.frame("IntId"= character(),"IntShortLabel" = character(), 
                   "AttId" = character(), "AttShortLabel" = character(),
                   "UniqueEncounterIds" = numeric(),"UnitOfMeasure" = character(),
                   "Dec0" = character(),"Dec1" = character(),"Dec2" = character(),"Dec3" = character(),"Dec4" = character(),
                   "Dec5" = character(),"Dec6" = character(),"Dec7" = character(),"Dec8" = character(),"Dec9" = character(),
                   "Dec10" = character(),stringsAsFactors = F)
  # For each unique interventionId, find each of its' attributeIds and the deciles of its values
  for(x in unique(dataframe$interventionId)){
    A1 <- dataframe[dataframe$interventionId==x,]
    for(x1 in unique(A1$attributeId)){
      A2 <- A1[A1$attributeId==x1,]
      A3 <- quantile(as.numeric(A2[[column]]),probs = 0:10/10, na.rm=T)
      B1 <- data.frame(
        "IntId" = x,
        "IntShortLabel" = unique(TotalIdCombos$interventionShortLabel[TotalIdCombos$interventionId==x]),
        "AttId" = x1,
        "AttShortLabel" = unique(TotalIdCombos$attributeShortLabel[TotalIdCombos$attributeId==x1]),
        "UniqueEncounterIds" = TotalIdCombos$uniqueEncounterIds[TotalIdCombos$interventionId==x & TotalIdCombos$attributeId==x1],
        "UnitOfMeasure" = paste0(unique(A1$unitOfMeasure),collapse = ","),
        "Dec0" = A3[1],"Dec1" = A3[2],"Dec2" = A3[3],"Dec3" = A3[4],"Dec4" = A3[5],
        "Dec5" = A3[6],"Dec6" = A3[7],"Dec7" = A3[8],"Dec8" = A3[9],"Dec9" = A3[10],"Dec10" = A3[11],
        row.names = NULL)[1,]
      B <- rbind(B,B1)
    }
  }
  rm(x,x1)
  B[7:17] <- round(B[7:17],2)
  # Determine if the data is all the same
  B$AllSame <- duplicated(B[6:16])
  B$AllSame[B$AllSame==F] <- 0
  B$AllSame[B$AllSame==T] <- 1
  # Re-order columns to give all information then the quantiles
  B <- B[,c("IntId", "IntShortLabel", "AttId", "AttShortLabel", "UniqueEncounterIds", 
            "UnitOfMeasure","AllSame","Dec0", "Dec1", "Dec2", "Dec3", "Dec4", "Dec5", "Dec6", "Dec7", 
            "Dec8", "Dec9", "Dec10")]
  QuantileColumnOut <<- B
  View(QuantileColumnOut)
  writeLines("Quantile analysis complete. Please investigate \"QuantileColumnOut\" for further information")
}

# Performs a count of the responses
# Then transforms the responses to numeric (count's output will make all responses a factor)
# Returns any data that cannot be transformed to numeric
NonNumericFinder <- function(dataframe,column){
  # Find anything that can't be turned numeric
  A1 <- count(dataframe[[column]])
  A1 <- A1[order(A1$freq,decreasing = T),]
  suppressWarnings(A1 <- A1[is.na(as.numeric(as.character(A1$x))),])
  rownames(A1) <- NULL
  return(A1)
}

# When handling blood gas data, we also only want to extract Arterial data
# Pulling this to add to blood gases to find and remove BLDO and Venous sample types.
DBConn(1)
SampleType <- sqlQuery(conn, "
SELECT p.encounterId, p.chartTime, p.terseForm
FROM PtLabResult p
JOIN D_Intervention i ON p.interventionId = i.interventionId
JOIN D_Attribute a ON p.attributeId = a.attributeId
WHERE (i.interventionId in ('4882','6270','6401')
AND terseForm IN ('Arterial', 'BLDO', 'Venous')
AND clinicalUnitId = '8')
");
DBConn(0)


SampleType$terseForm[SampleType$terseForm == "venous"] <- "Venous";
colnames(SampleType) = c("encounterId","chartTime","SampleType");

# Pass a blood gas dataframe to this function to have it scrub the non-arterial values
BloodGasArterial <- function(dataframe){
  
  # join the patient and sample type data to determine the data's origin
  x <- nrow(dataframe)
  writeLines("Attaching blood gas sample type data")
  dataframe <- join(dataframe,SampleType,by=c("encounterId","chartTime"),type="inner");
  x1 <- nrow(dataframe)
  # Report on how much data does not have an origin
  # This is normal and expected, but should be low
  writeLines(text = paste0(x-x1," rows (",round((x-x1)/x,4)*100,"%) did not have sample type data"))
  
  # Subset to only arterial data and report on how much data remains
  # Again, if this is very low, examine your data to find out what went wrong
  # Its probably not blood gas data
  writeLines("Restricting to arterial data")
  dataframe <- dataframe[dataframe$SampleType == "Arterial",]
  x2 <- nrow(dataframe)
  writeLines(text = paste0(x1-x2," of the remaining rows (",round((x1-x2)/x1,4)*100,"%) were not arterial data\n",
                           length(unique(A$encounterId))," patients remain, with an average of ",round(x2/length(unique(A$encounterId)),2)," readings each"))
  dataframe <- subset.data.frame(dataframe, select = -SampleType)
  
  return(dataframe)
}

# Typical data extraction goes:
# IdSearch for variable
# Delete urine ids if necessary (blood analysis are mixed in with urine analysis in the DB)
# Inspect IdSearchOut$IdSearchUsedCombos for anything out of the ordinary
# Remove Ids from the vector of Ids if necessary to remove something from the query
# Run SqlMutate, which inherits Ids from the IdSearchOut list
# Use the DataFind function to locate data (it may be in one of five columns)
# Use NonNumericFinder to look for any cleaning that might be needed
# Clean as necessary
# Use QuantileColumn to gain insight into each Id combination if you are unsure what
#   the data holds
# If data is not in the valueNumber column, copy it there to ensure consistency
