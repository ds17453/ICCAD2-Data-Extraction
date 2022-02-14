# Set-up ------------------------------------------------------------------

# Clear environment
rm(list=ls())

# Working directory in oneDrive/Research Associate 
setwd("C:/Users/ShillanDun/OneDrive - University of Bristol/NHS Research Associate/R Scripts/ICU Data Extraction and Cleaning")

# Id Combinations in use on Unit ------------------------------------------

# The output of this section is saved here to prevent re-runs
# Saved the Used Id Combinations on the CICU:
load("CICU Used Id Combinations.RData")

# Generation script for the Used Id Combinations on the CICU
if(!all(c("OpenConnection", "conn", "UsedIds", "UsedIdsShort")%in%ls())){
  
  # Connecting to the ICU SQL Server 
  # The data source name is set up as a Microsoft ODBC Data Source
  OpenConnection <- function(){
    require(DBI);require(odbc)
    conn <<- dbConnect(odbc(),dsn = "SQL Server1",
                      UID = "CISReportingDB",PWD = "CISReportingDB_123",timeout = 10)}
  OpenConnection()
  # Start with retrieving the intervention and attribute Id data dictionaries
  # InterventionIds: Events on the unit (Lab tests, medications, etc)
  # AttributeIds: Information surrounding the event (Location of an IV site, volume of meds administered)
  # Values themselves are stored in further tables
  
  IdIntervention <- dbGetQuery(
    conn,statement = 
      sqlInterpolate(ANSI(),
                     "SELECT interventionId,shortLabel,longLabel,conceptLabel FROM D_Intervention"))
  
  IdAttribute <- dbGetQuery(
    conn,statement = 
      sqlInterpolate(ANSI(),
                     "SELECT attributeId,shortLabel,longLabel,conceptLabel FROM D_Attribute"))
  
  # Prefix the column names for each df according to intervention or attribute
  colnames(IdIntervention) <- c("interventionId",paste0("intervention",colnames(IdIntervention)[-1]))
  colnames(IdAttribute) <- c("attributeId",paste0("attribute",colnames(IdAttribute)[-1]))
  
  # We want to locate which combinations of these Ids are used across 7 tables:
  # demographics, census, assessments, lab results, medications, site care, and total balance
  
  # All tables use a combination of intervention and attribute Ids
  #   except for TotalBalance, which only uses interventionId
  
  # We extract the Ids used in each table separately 
  #   and then use rbind to convert them to 1 dataframe
  
  # List for outputs (UsedIds = Ids used on the CICU)
  UsedIds <- list()
  
  # Do Total Balance first as it doesn't use attributeIds
  UsedIds$TotalBalance <- dbGetQuery(
    conn,statement = sqlInterpolate(
      ANSI(),
      "
      -- Meta query here
      -- pull the unique ids, how many unique stays have values stored under each id
      SELECT DISTINCT interventionId, COUNT(DISTINCT encounterId) AS uniqueEncounterIds,
      -- How many rows of values are stored using each id
      COUNT (*) AS totalRows, 
      -- The date range of each id
      MIN(chartTime) AS minChartTime, MAX(chartTime) as maxChartTime,
      -- hourTotal and cumTotal are the value columns. Lets you know which column stores values
      COUNT(hourTotal) AS hourTotalCount, COUNT(cumTotal) AS cumTotalCount
      FROM (
      -- Initial Data Query
      -- Want the patient stay id, the variable identifier, timestamp of data entry,
      -- hourTotal and cumTotal are value columns
      SELECT encounterId,interventionId, chartTime,hourTotal,cumTotal
      -- clinicalUnitId 8 = CICU, 5 is GICU.
      FROM PtTotalBalance WHERE clinicalUnitId = '8')a 
      GROUP BY interventionId"))
  
  # Formatting
  UsedIds$TotalBalance$hourTotalCount <- round(UsedIds$TotalBalance$hourTotalCount/UsedIds$TotalBalance$totalRows,3)*100
  UsedIds$TotalBalance$cumTotalCount <- round(UsedIds$TotalBalance$cumTotalCount/UsedIds$TotalBalance$totalRows,3)*100
  
  # General mean of all patients
  UsedIds$TotalBalance$meanRows <- round(UsedIds$TotalBalance$totalRows/
                                           UsedIds$TotalBalance$uniqueEncounterIds,1)
  UsedIds$TotalBalance <- subset.data.frame(UsedIds$TotalBalance,select = -totalRows)
  
  # Make the attributeId column and NA it for later rbind
  UsedIds$TotalBalance$attributeId <- NA
  
  # Label the table in a new column
  UsedIds$TotalBalance$table <- "PtTotalBalance"
  
  # Loop the other 6 tables
  # We're doing the same as above, but with both ids
  ICUDataPull <- function(Table){
    
    # Create and run an SQL query
    x <- dbGetQuery(
      # Connect to the database via the "conn" object
      conn,
      # Use DBI package's sqlInterpolate function to create an SQL Query
      statement = sqlInterpolate(ANSI(),
        "
        -- Meta query here, data pull later
        -- Pull each unique Id combination
        SELECT DISTINCT interventionId, attributeId,
        -- Total number of encounters with information under this combination
        COUNT(DISTINCT encounterId) AS uniqueEncounterIds,
        -- Number of rows with values+NAs for the id combination
        COUNT(*) as totalRows,
        -- Range of chartTimes to see when the id combo is used
        MIN(chartTime) AS minChartTime, MAX(chartTime) as maxChartTime,
        -- Number of rows with values in each value entry column
        COUNT(terseForm) AS terseFormCount,
        COUNT(verboseForm) AS verboseFormCount,
        COUNT(valueNumber) AS valueNumberCount,
        COUNT(baseValueNumber) AS baseValueNumberCount,
        COUNT(valueString) AS valueStringCount
        FROM (
          SELECT encounterId, interventionId, attributeId, chartTime,
          terseForm, verboseForm, valueNumber, baseValueNumber, valueString
          FROM ?Table 
          WHERE clinicalUnitId = '8'
        )a
        GROUP BY interventionId, attributeId",
        # insert the function's "table" argument in to the "FROM ?TABLE" section
        # via a parameterised injection
        Table = SQL(Table)))
    
    # Determine mean number of values per stay
    x$meanRows <- round(x$totalRows/x$uniqueEncounterIds,1)
    
    # COUNT columns show where values are stored
    # Convert the COUNT columns to be a percentage of the total rows
    for(z in paste0(c("terseForm", "verboseForm", "valueNumber", "baseValueNumber", "valueString"),"Count")){
      x[[z]] <- round(x[[z]]/x$totalRows,3)*100}
    
    # Tidy dataframe and note which table it refers to 
    x <- subset.data.frame(x,select = -totalRows)
    x$table <- Table
    return(x)
  }
  
  OpenConnection()
  
  x <- "Demographic"
  for(x in c("Demographic","Assessment","LabResult","Medication","SiteCare","Treatment")){
    x1 <- paste0("Pt",x)
    UsedIds[[x]] <- ICUDataPull(Table = x1)
    rm(x,x1)}
  View(UsedIds[UsedIds$table=="PtTreatment",])
  View(UsedIds$Demographic)
  rm(ICUDataPull)
  
  # Once you have the dictionary for all 7 tables, rbind them together into one dataframe
  # Total balance uses different value column names than all other tables
  # Want the same columns in each dataframe 
  for(x in names(UsedIds)){
    if(x =="TotalBalance"){
      UsedIds$TotalBalance$terseFormCount <- NA
      UsedIds$TotalBalance$verboseFormCount <- NA
      UsedIds$TotalBalance$valueNumberCount <- NA
      UsedIds$TotalBalance$baseValueNumberCount <- NA
      UsedIds$TotalBalance$valueStringCount <- NA
    }else{
      UsedIds[[x]]$hourTotalCount <-  NA
      UsedIds[[x]]$cumTotalCount <-  NA
    }
  }
  
  # Rbind all dataframes in a list to one dataframe
  UsedIds <- do.call(rbind.data.frame, UsedIds)
  
  # The above rbind changes the rownames to "[original list] + [rownumber in original list]"
  # We don't need that, so just set rownumbers to 1:nrow(usedIds)
  rownames(UsedIds) <- 1:nrow(UsedIds)
  
  # Join Id info on via plyr's "join" (akin to SQL)
  require(plyr)
  UsedIds <- join(UsedIds,IdAttribute,by="attributeId")
  UsedIds <- join(UsedIds,IdIntervention,by="interventionId")
  rm(IdIntervention,IdAttribute)
  
  # Re-order the columns
  dput(colnames(UsedIds))
  UsedIds <- UsedIds[,c("interventionId","attributeId",
                        "uniqueEncounterIds","meanRows",
                        "interventionshortLabel", "attributeshortLabel",
                        "interventionlongLabel", "attributelongLabel",
                        "interventionconceptLabel", "attributeconceptLabel",
                        "terseFormCount", "verboseFormCount",
                        "valueNumberCount", "baseValueNumberCount", "valueStringCount",
                        "hourTotalCount","cumTotalCount","minChartTime","maxChartTime","table")]
  
  # Formatting column names: Correct capitalisation
  colnames(UsedIds) <- gsub(colnames(UsedIds),pattern = "shortLabel",replacement = "ShortLabel")
  colnames(UsedIds) <- gsub(colnames(UsedIds),pattern = "longLabel",replacement = "LongLabel")
  colnames(UsedIds) <- gsub(colnames(UsedIds),pattern = "conceptLabel",replacement = "ConceptLabel")
  
  # Remove superfluous information
  UsedIds <- UsedIds[!UsedIds$attributeShortLabel %in% 
                       c('Reference Range','Specimen Catalog',
                         'Specimen Received Date Time','Specimen','Specimens'),]
  
  # Ventiles of the number of unique stays per Id
  quantile(UsedIds$uniqueEncounterIds,probs = 0:20/20)
  # The vast majority of the used Ids are for 1 patient
  
  # We subset to any data with at least 5 unique patients
  UsedIds <- UsedIds[UsedIds$uniqueEncounterIds>=5,]
  
  UsedIdsShort <- subset.data.frame(UsedIds,
                                    select=-c(interventionLongLabel,attributeLongLabel,
                                              interventionConceptLabel,attributeConceptLabel))
  save.image("CICU Used Id Combinations.RData")
}

# Patients admitted to the unit -------------------------------------------

# Official hospital records are timestamps of when nurses check patients in and out
# Additionally, clerical errors can occur due to admitting a patient to the wrong unit
# They must be discharged and then admitted to the correct unit
# Therefore official hospital records are noisy and unreliable

# We instead validate patient stay in the ICU and length of stay
#   via the range of their heart rate measurements

rm(list=ls())
load("CICU Used Id Combinations.RData")
OpenConnection()

# PtLoS = Patient + length of stay
# "Count" column = number of heart rate recordings per patient
PtLoS <- 
  dbGetQuery(conn,statement = sqlInterpolate(ANSI(),
  "SELECT * FROM (
  SELECT a.encounterId, a.attributeId, d.patientId, d.age, d.gender, 
  MIN(charttime) AS [Start_date], 
  MAX(charttime) AS [End_date], 
  COUNT(*) AS [Count]
  FROM PtAssessment a
  JOIN D_Encounter d on a.encounterId=d.encounterId
  WHERE attributeId IN ('27312','12754') -- use both attributes for HR
  AND clinicalUnitId = '8'
  GROUP BY a.encounterId,a.attributeId,d.patientId,d.age,d.gender)a;"))

# Remove patient with no age value
PtLoS <- PtLoS[!is.na(PtLoS$age),];

# Generate length of stay
PtLoS$LoS <- as.numeric(round(difftime(PtLoS$End_date,PtLoS$Start_date,units = "hours")))

# Official Hospital Records vs Heart Rate ---------------------------------

require(plyr)

# Want to remove patients with >36 hours between admittance to the unit and first heart rate
Hospital_Startdates <- dbGetQuery(
  conn,statement = sqlInterpolate(ANSI(),
  "SELECT encounterId,inTime,outTime,admitSource,lengthOfStay,
          isDischarged,isDeceased,isTransferred 
  FROM PtCensus
WHERE clinicalUnitId = '8'"))

# Use some temporary dataframes for finding patients with excessive differences
A <- join(PtLoS,Hospital_Startdates,by="encounterId",type="inner")

# Finding re-admissions patients (multiple stays with same encounterId)
A2 <- A[A$encounterId %in% subset.data.frame(A, duplicated(A$encounterId))$encounterId,]
A2 <- A2[order(A2$patientId),]

# Only require the patient and stay identifiers alongside the two records of admittance/discharge
A2 <- subset.data.frame(A2, select=c(patientId,encounterId,Start_date, inTime,End_date,outTime))

# Find difference between first HR timestamp and official hospital records
A2$StartDateDiff <- round(as.numeric(difftime(A2$Start_date,A2$inTime,units = "hours")),2)

# for each patient find the lowest difference between HR timestamp and official hospital records
for(x in unique(A2$encounterId)){
  # Subset to this unique stay
  A3 <- A2[A2$encounterId==x,]
  # Find which is the closest to 0
  x1 <- A3$StartDateDiff[which(abs(A3$StartDateDiff)==min(abs(A3$StartDateDiff)))]
  # Note this in the original dataframe
  A2$StartDateDiffLowest[A2$encounterId==x] <- x1
  # Tidy environment
  rm(A3,x,x1)
}

# Subset the dataframe to the lowest difference per stay
A2 <- A2[A2$StartDateDiff==A2$StartDateDiffLowest,]

# Note those with more than 36 hours between first HR timestamp and official hospital records
A3 <- c(subset.data.frame(A2, StartDateDiff>36)$encounterId,
        subset.data.frame(A2, StartDateDiff<(-36))$encounterId)

# Drop A3 (stays with with more than 36 hours between first HR timestamp and official hospital records
PtLoS <- PtLoS[!PtLoS$encounterId %in% A3,]

# remove count of heart rate recordings per patient and the attributeId for heart rate
PtLoS <- subset.data.frame(PtLoS,select = -c(attributeId,Count))

# Find duplicated encounterIds --------------------------------------------

# Duplicated encounterIds always due to multiple stays in unit via start/enddate
# Want: for each unique encounterId, calculate LoS as earliest start_date to latest end_date
A <- PtLoS

for(x in A$encounterId){
  A1 <- A[A$encounterId==x,]
  
  A$Start_date[A$encounterId==x] <- min(A1$Start_date)
  A$End_date[A$encounterId==x] <- max(A1$End_date)
  
  A$LoS1[A$encounterId==x] <- as.integer(difftime(max(A1$End_date),min(A1$Start_date),units = "hours"))
  rm(x,A1)
}

# Check through the patients with multiple stays to ensure that their max-min dates are reasonable
A1 <- A[duplicated(A$encounterId),]
A1 <- A[A$encounterId%in%A1$encounterId,]
rm(A1)

A$LoS <- A$LoS1
A <- subset.data.frame(A,select=-LoS1)
A <- A[!duplicated(A),]
PtLoS <- A
rm(A)

UsedIds$uniqueEncounterIdsPercent <- round(UsedIds$uniqueEncounterIds/nrow(PtLoS),3)*100
dput(colnames(UsedIds))

UsedIds <- UsedIds[,c(
  "interventionId", "attributeId", "uniqueEncounterIds", 
  "uniqueEncounterIdsPercent","meanRows", 
  "interventionShortLabel", "attributeShortLabel", "interventionLongLabel", 
  "attributeLongLabel", "interventionConceptLabel", "attributeConceptLabel", 
  "terseFormCount", "verboseFormCount", "valueNumberCount", "baseValueNumberCount", 
  "valueStringCount", "hourTotalCount", "cumTotalCount", "minChartTime", 
  "maxChartTime", "table")]


# Collecting CICU info to a single list -----------------------------------

# Patients and Ids list
CICUBasic <- list()
CICUBasic$PtLoS <- PtLoS
CICUBasic$UsedIds <- UsedIds
CICUBasic$UsedIdsShort <- UsedIdsShort
CICUBasic$OpenConn <- OpenConnection
rm(PtLoS,UsedIds,UsedIdsShort,A,A2,Hospital_Startdates,A3,OpenConnection)

# Data Query Functions ----------------------------------------------------

# Several functions to search for and extract variables from the backend database
# Designed to be run sequentially. Some are mandatory and some optional.

# IdSearch to find the ids associated with a search term
# If unwanted results are also pulled:
#   repeat the IdSearch using the IntIdDrop/AttIdDrop/DeleteUrine optional arguments

# Run SQLQuery to retrieve the values associated with the Id combos

# Use the DataFind function to locate the values (it may be in one of five columns)
# DataFind auto-runs after SQLQuery
# You'll usually want to work with the column that has the highest number of values
# Sometimes the values will be spread across multiple columns
# If values are not in the valueNumber column, copy them there to ensure consistency
# for later extraction

# Use NonNumericFinder to look for any cleaning that might be needed
# ValueCleaner has two options: Remove > and < symbols or all non-numeric data
# You can also manually clean as necessary

# Use QuantileColumn to inspect each Id combination
# Ensure that the values for each combination look roughly the same
# If not, further transformation required

# There's an example variable extraction after the function definitions

# List for functions and outputs
DQFunctions <- list()

# Custom function to search through the main dictionary's short and long labels for a term
# Pulls back all the Ids associated with the term
# Then provides a detailed crosstable of how the Id combinations have been used to record values
# All arguments are optional (hence the " = NULL")
#   In R, you check for optional arguments via a check if they're not NULL [!is.null(argument)]

# TableSearch and Variable are mandatory, but can be inherited from the DQFunctions list

# Note: functions in R run in their own temporary environment
# "<-" will assign a variable within this temporary environment
# "<<-" will assign a variable in the environment the function is run from

# Temporary arguments for debugging
# TableSearch <- "PtLabResult"
# Variable <- "APTT"

DQFunctions$IdSearch <- function(
  TableSearch = NULL,Variable  = NULL,DontPermuteVarSearch = NULL,
  IntIdsDrop = NULL,AttIdsDrop = NULL,DeleteUrineIds = NULL){

  # TableSearch and Variable arguments can be inherited from DQFunctions list
  if(is.null(TableSearch)){
    writeLines("TableSearch argument not provided. Inheriting argument from DQFunctions$TableSearch\n")
    TableSearch <- DQFunctions$TableSearch
    }else{DQFunctions$TableSearch <<- TableSearch
    }
  
  # Argument check: TableSearch needs to be one of the 7 tables
  if(!TableSearch%in%unique(CICUBasic$UsedIds$table))stop(
    paste("TableSearch must be one of the following:",paste(unique(CICUBasic$UsedIds$table),collapse = ", ")))
  
  if(is.null(Variable)){
    writeLines("Variable argument not provided. Inheriting argument from DQFunctions$Var\n")
    Variable <- DQFunctions$Var}else{
      DQFunctions$Var <<- Variable}
  
  # Packages
  suppressWarnings(require(data.table))
  
  # Output
  DQFunctions$IdSearchOutput <<- list()
  
  # Narrow the UsedIds dataframe to just the table you want to search
  A <- CICUBasic$UsedIds[CICUBasic$UsedIds$table==TableSearch,]
  
  # Remove Ids as appropriate
  if(!is.null(IntIdsDrop)){A <- A[!A$interventionId%in%IntIdsDrop,]}
  if(!is.null(AttIdsDrop)){A <- A[!A$attributeId%in%AttIdsDrop,]}
  
  # Delete urine Ids argument: Search through the 6 Id label columns for Urine/urine labels and remove
  if(!is.null(DeleteUrineIds)){
    for(x in colnames(A)[colnames(A)%like%"Label"]){
      A <- A[!A[[x]] %like% "Urine|urine",]}
  }
  
  # Search terms: Include the title case and lowercase version of the term
  if(is.null(DontPermuteVarSearch)){
    SearchTerms <- paste(Variable,tools::toTitleCase(Variable),tolower(Variable),sep = "|")
  }else{SearchTerms <- Variable}
  
  # Search through short and long labels for your term (A1)
  A1 <- A[(A$interventionShortLabel %like% `SearchTerms`|A$attributeShortLabel %like% `SearchTerms`|
           A$interventionLongLabel %like% `SearchTerms` |A$attributeLongLabel  %like% `SearchTerms`),]
  
  if(nrow(A1)==0){stop("No Ids found for labels like the supplied variable after removing specified Ids")}
  
  # Report on the combinations of Ids found
  writeLines(paste0(
    "Values found for ",sum(A1$uniqueEncounterIds)," patients (",
    round(sum(A1$uniqueEncounterIds)/length(unique(CICUBasic$PtLoS$encounterId))*100,1),
    "% of the ",length(unique(CICUBasic$PtLoS$encounterId))," CICU patients)\n",
    "    This assumes no overlap between the Id combinations' encounterIds\n"))
  
  # Pull back the intervention and attribute Ids alongside their shortlabels
  IntIdsDetail <- unique(CICUBasic$UsedIds[CICUBasic$UsedIds$interventionId%in%A1$interventionId,c("interventionId","interventionShortLabel")])
  AttIdsDetail <- unique(CICUBasic$UsedIds[CICUBasic$UsedIds$attributeId%in%A1$attributeId,c("attributeId","attributeShortLabel")])
  
  IntIdsDetail <- IntIdsDetail[order(IntIdsDetail$interventionId),]
  AttIdsDetail <- AttIdsDetail[order(AttIdsDetail$attributeId),]
  
  # Crosstable of which intervention and attribute Id combinations have been used
  # Add on the Id shortlabels to the column and row names of the table to ease manual inspection
  # Also replace the binary "This combination was used" with the number of patients and mean rows
  A11 <- table(A1$interventionId,A1$attributeId)
  
  # Replace the binary "This combo was used" indicator
  #   with the number of patients under the combo
  for(x in 1:nrow(A)){
    A11[dimnames(A11)[[1]]==A$interventionId[x],dimnames(A11)[[2]] == A$attributeId[x]] <- 
      paste0(A$uniqueEncounterIds[x]," (",A$meanRows[x],")")
    rm(x)
  }
  
  # Alter dimension names to have shortlabels on
  # InterventionIds:
  dimnames(A11)[[1]] <- paste(IntIdsDetail$interventionId,IntIdsDetail$interventionShortLabel)
  # AttributeIds
  dimnames(A11)[[2]] <- paste(AttIdsDetail$attributeId,AttIdsDetail$attributeShortLabel)
  
  # Alternate summary of the crosstable of int vs att ids as a dataframe
  A2 <- as.data.frame(table(A1$interventionId,A1$attributeId))
  # output list
  x1 <- list()
  for(x in unique(A2$Var1)){
    # Data frame of the attribute, how many interventions it is related to, and what they are
    x1[[x]] <- data.frame("Att"=x,
                          "IntIdsRelated"=sum(A2$Freq[A2$Var1==x]),
                          "IntIds"=paste(A2$Var2[A2$Var1==x & A2$Freq==1],collapse = "," ))
    rm(x)
  }
  x1 <- do.call(rbind.data.frame,x1);rownames(x1) <- NULL
  colnames(x1) <- c("Int","AttIdsRelated","AttIds")
  
  # Outputs
  DQFunctions$IdSearchOutput$Variable <<- c(`Variable`)
  DQFunctions$IdSearchOutput$IntIdsDetail <<- IntIdsDetail
  DQFunctions$IdSearchOutput$AttIdsDetail <<- AttIdsDetail
  DQFunctions$IdSearchOutput$IntAttIdsTable <<- A11
  DQFunctions$IdSearchOutput$IntAttIdsTableSummary <<- x1
  
  writeLines("Cross table of the unique Id combinations:\n    Values are the number of unique encounterIds (mean number of rows)")
  print(A11)
  writeLines("\nFurther information may be found in \"DQFunctions$IdSearchOutput\"")
  writeLines("To refine this query, use the optional arguments \"IntIdsDrop\", \"AttIdsDrop\", and \"DeleteUrineIds\"")
}


# Inherits Ids from the output of IdSearch (above) and runs a SQL Query to pull the values
# With further inspection of the new main dictionary, could look at automating data collation
# terseForm always has more values than valueNumber
x <- CICUBasic$UsedIds
x <- x[!is.na(x$attributeId),]
nrow(x[x$valueNumberCount<x$terseFormCount,])
nrow(x[x$valueNumberCount>x$terseFormCount,])
nrow(x[x$valueNumberCount==0 & x$terseFormCount>0,])

DQFunctions$SQLQuery <- function(IntIds = NULL,AttIds = NULL,TableSearch = NULL){
  require(DBI);require(odbc)
  
  # Connect to the database if not already connected
  if(!DBI::dbIsValid(conn)){CICUBasic$OpenConn()}

  # Pull arguments from DQFunctions if not provided
  if(is.null(TableSearch)){
    writeLines("TableSearch argument not provided. Inheriting argument from DQFunctions$TableSearch")
    TableSearch <- DQFunctions$TableSearch}
  
  if(is.null(IntIds)){
    writeLines("IntIds argument not provided. Inheriting argument from DQFunctions$IdSearchOutput")
    IntIds <- paste0("'",DQFunctions$IdSearchOutput$IntIdsDetail$interventionId,"'",collapse = ",")
  }else{IntIds <- paste0("'",IntIds,"'",collapse = ",")}
  
  if(is.null(AttIds)){
    writeLines("AttIds argument not provided. Inheriting argument from DQFunctions$IdSearchOutput")
    AttIds <- paste0("'",DQFunctions$IdSearchOutput$AttIdsDetail$attributeId,"'",collapse = ",")
  }else{AttIds <- paste0("'",AttIds,"'",collapse = ",")}
  
  # Make the sql statement
  # Use SQL() or dbQuoteIdentifier() to avoid escaping
  x1 <- sqlInterpolate(ANSI(),
          "SELECT interventionId,attributeId,encounterId,chartTime, 
          terseForm, verboseForm, valueNumber, unitOfMeasure, baseValueNumber, baseUOM, valueString 
          FROM ?TableSearch 
          WHERE (interventionId IN (?IntIds) AND attributeId IN (?AttIds))
          AND clinicalUnitId = '8'",
          TableSearch = SQL(TableSearch),
          IntIds = SQL(IntIds),
          AttIds = SQL(AttIds))
  
  # Run the query
  startTime <- Sys.time()
  writeLines("Running Query...")
  x2 <- dbGetQuery(conn,statement = x1)
  writeLines(text = paste0("Query completed in ",format(round(difftime(Sys.time(),startTime),2))))
  rm(startTime)
  
  # check the encounterIds against the admitted patients
  x2 <- x2[x2$encounterId %in% CICUBasic$PtLoS$encounterId,]
  
  # Remove any rows where all the value columns are NA
  x2 <- x2[!(is.na(x2$terseForm) + is.na(x2$verboseForm) +
          is.na(x2$valueNumber) + is.na(x2$baseValueNumber) + 
          is.na(x2$valueString)) == 5,]
  
  # Automatically run the DataFind function
  writeLines("\nAutomatically running the DataFind function:\n")
  DQFunctions$DataFind(x2)
  
  return(x2)
}

# Searches through the dataframe and reports on the id combos within
# Very similar to the crosstable made in IdSearch
DQFunctions$DataFind <- function(Dataframe){
  
  A <- Dataframe
  
  # Packages
  suppressWarnings(require(plyr))
  
  # Function for adding the shortLabels to some Ids we retrieve in a cross-table
  CrossTableLabelFix <- function(CrossTable,IdType,Dataframe){
    
    # Argument formatting
    if(!IdType%in%c("intervention","attribute")){
      stop("Please supply either \"intervention\" or \"attribute\" 
                 to the \"IdType\" argument")}
    IdType1 <- paste0(IdType,"Id")
    
    # Obtain the Id and short label from the UsedIds dataframe
    IdsShortLabel <- CICUBasic$UsedIds[
      CICUBasic$UsedIds[[IdType1]]%in%Dataframe[[IdType1]],]
    
    # Order by interventionId so that these line up with the cross-table
    IdsShortLabel <- IdsShortLabel[order(IdsShortLabel[[IdType1]]),]
    
    # Only pull the shortlabels
    IdsShortLabel <- unique(IdsShortLabel[colnames(IdsShortLabel) %in%
                                            c(IdType1,paste0(IdType,"ShortLabel"))])
    
    NewLabels <- paste(IdsShortLabel[[IdType1]],IdsShortLabel[[paste0(IdType,"ShortLabel")]])
    
    if(IdType == "intervention"){rownames(CrossTable) <- NewLabels}
    if(IdType == "attribute"){colnames(CrossTable) <- NewLabels}
    return(CrossTable)
  }
  
  # Mark used percentages 
  # Avoid repetitions of data analysis when identical values are written to multiple columns
  # E.g., if data is in terseForm its likely to be in verboseform as well
  UsedPercents <- c()
  DataFind <- list()
  
  # Show how much data is in each column
  x <- "terseForm"
  x <- "valueNumber"
  
  for(x in c("terseForm","verboseForm","valueNumber","baseValueNumber","valueString")){
    
    # Determine the percentage of values in the column where data is present
    # count when data is present (ie - NA FALSE) is each of the above
    # Round to 10 decimal places, then truncate. Ensure an output of 100% is accurate
    x1 <- 100-sum(is.na(A[,x]))/nrow(A)*100
    x1 <- round_any(x1,accuracy = .01,f = floor)
    x1[is.na(x1)] <- "0"
    
    writeLines(paste0(x," has ",x1,"% of values filled"))
    
    # Used in output:
    if(x == "terseForm" & x1 == "100"){DQFunctions$terseFormDefault <- 1}
    if(x == "valueNumber" & x1 == "100"){DQFunctions$valueNumberDefault <- 1}
    
    # For any col with >20% data, crosstable shortlabels
    if(as.numeric(x1)>20){
      NewPercent <- x1
      # Check that you're not duplicating a previous output
      if(!(NewPercent %in% UsedPercents)){
        
        # Remove the NA values in this column
        A1 <- A[!is.na(A[,x]),]
        
        # Find the number of patients with information under each combination
        # InterventionIds: rows, AttributeIds: columns
        CrossTable <- as.data.frame(with(A1,tapply(encounterId,list(interventionId,attributeId),
                             FUN = function(x) length(unique(x)))))
        
        # Alter dimension names to have shortlabels on:
        CrossTable <- CrossTableLabelFix(CrossTable = CrossTable,IdType = "intervention",Dataframe = A1)
        CrossTable <- CrossTableLabelFix(CrossTable = CrossTable,IdType = "attribute",Dataframe = A1)
        
        writeLines(paste0("Unique patients for int/att combos in ",x,":"))
        print(CrossTable)
        writeLines("")
        
        DataFind[[x]] <- CrossTable
        rm(x)
      }else{writeLines(paste("   cross table not displayed due to data duplicated across a previous columns\n"))}
      # Record the percentage of filled values to prevent duplication in the loop
      UsedPercents <- c(UsedPercents,NewPercent)
    }else{writeLines("")}
  }
  
  # Unique patients per id combo for the whole table
  CrossTable <- as.data.frame(with(A,tapply(encounterId,list(interventionId,attributeId),
                                       FUN = function(x) length(unique(x)))))
  CrossTable <- CrossTableLabelFix(CrossTable = CrossTable,IdType = "intervention",Dataframe = A)
  CrossTable <- CrossTableLabelFix(CrossTable = CrossTable,IdType = "attribute",Dataframe = A)
  
  writeLines(paste0("Unique patients for int/att combos across whole table:"))
  print(CrossTable)
  writeLines("")
  
  DataFind$AllColumns <- CrossTable
  
  # Report on how much of the CICU population have values
  writeLines(paste0("Data found for ",length(unique(A$encounterId))," of ",length(unique(CICUBasic$PtLoS$encounterId))," verified patients (",
                    round(
                      length(unique(A$encounterId))/length(unique(CICUBasic$PtLoS$encounterId))*100,2),"%; ",
                    length(unique(CICUBasic$PtLoS$encounterId))-length(unique(A$encounterId))," patients missing values)"))
  
  if(!is.null(DQFunctions$terseFormDefault)){
    writeLines("All values in terseForm. Need to move to valueNumber for later extraction")}
  DQFunctions$terseFormDefault <<- NULL
  
  if(!is.null(DQFunctions$valueNumberDefault)){
    writeLines("All values in valueNumber, no need to move values")}
    DQFunctions$valueNumberDefault <<- NULL
}

# Run the non-numeric finder to see if anything can be salvaged
DQFunctions$NonNumericFinder <- function(dataframe,column){
  # Find anything that can't be turned numeric
  A1 <- count(dataframe[[column]])
  A1 <- A1[order(A1$freq,decreasing = T),]
  suppressWarnings(A1 <- A1[is.na(as.numeric(as.character(A1$x))),])
  rownames(A1) <- NULL
  DQFunctions$NonNumericFinderOutput <<- A1
  # Output
  if(nrow(DQFunctions$NonNumericFinderOutput)==0){
    writeLines("No non-numeric values found")}else{return(A1)}
}

DQFunctions$ValueCleaner <- function(dataframe,column,symbolClean = NULL,rmNonNumeric = NULL){
  if(!is.null(symbolClean)){
    writeLines("removing all instances of \"<\" and \">\" from supplied column")
    dataframe[[column]] <- gsub(dataframe[[column]],pattern = "<| <|< |>| >|> ",replacement = "")}
  if(!is.null(rmNonNumeric)){
    writeLines("Subsetting dataframe to remove non-numeric values from supplied column")
    dataframe <- dataframe[!dataframe[[column]]%in%DQFunctions$NonNumericFinderOutput$x,]}
  return(dataframe)
}

# Inspection of each combo's values
# All values will collated to "valueNumber" by now
# Useful for detecting if values seem similar for a number of id combos
# also for finding rogue indicator variables
# Decile valuenumber by int and then att id
DQFunctions$QuantileValueNumber <- function(df){

  A <- list()
  
  # Find all unique Id combinations in the dataframe
  x <- unique(df[,c("interventionId","attributeId")])
  
  # Work through each and quantile the results
  x1 <- 1
  for(x1 in 1:nrow(x)){
    # Subset to the Id combination
    A1 <- df[df$interventionId == x$interventionId[x1] & df$attributeId == x$attributeId[x1],]
    A2 <- quantile(as.numeric(A1$valueNumber),probs = 0:10/10,na.rm = T)

    A21 <- unique(CICUBasic$UsedIds$interventionShortLabel[
      CICUBasic$UsedIds$interventionId==x$interventionId[x1]])
    A22 <- unique(CICUBasic$UsedIds$attributeShortLabel[
      CICUBasic$UsedIds$attributeId==x$attributeId[x1]])
    A21 <- A21[!is.na(A21)]
    A22 <- A22[!is.na(A22)]
    
    A4 <- df$unitOfMeasure[df$interventionId==x$interventionId[x1] & df$attributeId == x$attributeId[x1]]
    
    A3 <- data.frame(
      "IntId" = x$interventionId[x1],
      "IntShortLabel" = A21,
      "AttId" = x$attributeId[x1],
      "AttShortLabel" = A22,
      "UnitOfMeasure" = A4)[1,]
    
    for(z in 0:10){
      A3[[paste0("Dec",z)]] <- round(as.numeric(A2)[z+1],2)}
    A[[x1]] <- A3
    rm(z,A3)
  }
  
  A <- do.call(rbind.data.frame,A)
  DQFunctions$QuantileValueNumberOutput <<- A
  print(A[,!colnames(A)%like%"Dec"])
  writeLines("")
  print(A[,colnames(A)%like%"Dec"])
  
  # Add a graph and an "all numbers the same?" check
  require(ggplot2)
  A1 <- list()
  for(x in 1:nrow(DQFunctions$QuantileValueNumberOutput)){
    A2 <- DQFunctions$QuantileValueNumberOutput[x,]
    
    A21 <- A2[,c(colnames(A2)%like%"Dec")]
    A21 <- as.data.frame(t(A21))
    A21$Decile <- as.integer(gsub(rownames(A21),pattern = "Dec",replacement = ""))
    rownames(A21) <- NULL
    A21$IdCombo <- paste(A2$IntId,A2$AttId)
    colnames(A21) <- c("Value","Decile","IdCombo")
    A1[[x]] <- A21
    rm(A2,A21)
  }
  A1 <- do.call(rbind.data.frame,A1)
  
  A2 <- ggplot(data = A1,aes(x = Decile,y = Value, group = IdCombo)) + 
    geom_line(aes(colour = IdCombo)) + geom_point(aes(colour = IdCombo),size = 1) + 
    scale_x_continuous(n.breaks = 11) + 
    guides(colour = guide_legend(title="Id Combo\n(Int + Att Id)",title.hjust = 0.5))

  DQFunctions$QuantileValueNumberGraph <<- A2
  print(A2)
}

# When handling blood gas data, we also only want to extract Arterial data
# Pulling this to add to blood gases to find and remove BLDO and Venous sample types.
CICUBasic$OpenConn()
CICUBasic$sampleType <- dbGetQuery(conn,statement = sqlInterpolate(ANSI(),
  "SELECT p.encounterId, p.chartTime, p.terseForm
FROM PtLabResult p
JOIN D_Intervention i ON p.interventionId = i.interventionId
JOIN D_Attribute a ON p.attributeId = a.attributeId
WHERE (i.interventionId in ('4882','6270','6401')
AND terseForm IN ('Arterial', 'BLDO', 'Venous')
AND clinicalUnitId = '8')"));

CICUBasic$sampleType$terseForm[CICUBasic$sampleType$terseForm == "venous"] <- "Venous";
colnames(CICUBasic$sampleType) = c("encounterId","chartTime","SampleType");

# Pass a blood gas dataframe to this function to have it scrub the non-arterial values
DQFunctions$BloodGasArterial <- function(dataframe){
  
  # join the patient and sample type data to determine the data's origin
  x <- nrow(dataframe)
  writeLines("Attaching blood gas sample type data")
  dataframe <- join(dataframe,CICUBasic$sampleType,by=c("encounterId","chartTime"),type="inner");
  
  x1 <- nrow(dataframe)
  # Report on how much data does not have an origin
  # This is normal and expected, but should be low
  writeLines(text = paste0(x-x1," rows (",round((x-x1)/x,4)*100,"%) did not have sample type data"))
  
  # Subset to only arterial data and report on how much data remains
  # Again, if this is very low, examine your data to find out what went wrong
  # The original data is probably not blood gas data
  writeLines("Restricting to arterial data")
  dataframe <- dataframe[dataframe$SampleType == "Arterial",]
  x2 <- nrow(dataframe)
  writeLines(text = paste0(x1-x2," of the remaining rows (",round((x1-x2)/x1,4)*100,"%) were not arterial data\n",
                           length(unique(A$encounterId))," patients remain, with an average of ",round(x2/length(unique(A$encounterId)),2)," readings each"))
  dataframe <- subset.data.frame(dataframe, select = -SampleType)
  
  return(dataframe)
}

save.image("CICUBasic and Data Query Functions.RData")

# Walkthrough of Typical Use of the Toolkit -------------------------------
names(DQFunctions)

# IdSearch for variable
# If unwanted results are also pulled, use the IntIdDrop/AttIdDrop/DeleteUrine optional arguments

# Run SQLQuery to retrieve the values associated with the Id combos

# Use the DataFind function to locate the values (it may be in one of five columns)
# DataFind auto-runs after SQLQuery
# You'll usually want to work with the column that has the highest number of values
# Sometimes the values will be spread across multiple columns
# If values are not in the valueNumber column, copy them there to ensure consistency
# for later extraction

# Use NonNumericFinder to look for any cleaning that might be needed
# ValueCleaner has two options: Remove > and < symbols or all non-numeric data
# You can also manually clean as necessary

# Use QuantileColumn to inspect each Id combination
# Ensure that the values for each combination look roughly the same
# If not, further transformation required

# Example use
# Use ID search to find the relevant IDs
DQFunctions$IdSearch(TableSearch = "PtLabResult",Variable = "APTT")
DQFunctions$IdSearchOutput
# Then, after inspecting what IdSearch returns you may run the query
# Remember the optional arguments for dropping Ids and anything to do with urine in 
# OpenConnection()
A <- DQFunctions$SQLQuery(
  TableSearch = "PtLabResult",
  IntIds = DQFunctions$IdSearchOutput$IntIdsDetail$interventionId,
  AttIds = DQFunctions$IdSearchOutput$AttIdsDetail$attributeId)

# Use DataFind to show you where the data is per combo
DQFunctions$DataFind(A)

# Appears that the data is in terse, verbose, and valueString
DQFunctions$NonNumericFinder(A,"valueString")
A <- DQFunctions$ValueCleaner(A,column = "valueString",symbolClean = 1)
DQFunctions$NonNumericFinder(A,"valueString")
A <- DQFunctions$ValueCleaner(A,column = "valueString",rmNonNumeric = 1)

# Move to valueNumber
A$valueNumber <- A$valueString

# Quantile the valueNumber
DQFunctions$QuantileValueNumber(A)


# Surgery Nature ----------------------------------------------------------

# We only want validated stays in CICUBasic$PtLoS
# We need to drop patients who were transferred to the unit

# Old Surgery Nature
SurgeryNature1 <- dbGetQuery(
  conn,statement = sqlInterpolate(
    ANSI(),"SELECT encounterId, interventionId, attributeId, terseForm, verboseForm, valueNumber, unitOfMeasure, baseValueNumber, baseUOM, valueString
    FROM ptassessment
    WHERE (attributeID = '1816' OR interventionID = '1516')
    AND ClinicalUnitID = '8';"))

SurgeryNature1 <- data.frame(encounterId = SurgeryNature1$encounterId,
                             valueNumber = SurgeryNature1$terseForm)

A <- dbGetQuery(
  conn,statement = sqlInterpolate(
    ANSI(),"SELECT encounterId, interventionId, attributeId, terseForm, verboseForm, valueNumber, unitOfMeasure, baseValueNumber, baseUOM, valueString
    FROM PtAssessment
    WHERE attributeID IN ('505','1870','9088')
    AND clinicalUnitId = '8'"))
DQFunctions$DataFind(A)
# Non-numeric data: Go in manually
count(A$terseForm)
# Drop general theatres and re-format
A <- A[!A$terseForm %in% c("Elective - General Theatres","Emergency - General Theatres"),]
A$valueNumber[A$terseForm == "Elective - Cardiac Theatres"] <- "Elective"
A$valueNumber[A$terseForm %in% c("Emergency - Cardiac Theatres","Emergency - Cardiology")] <- "Emergency"
A$valueNumber[A$terseForm == "Hospital Transfer"] <- NA
# Hospital transfers should be only NAs in valueNumber
count(A$terseForm)
count(A$valueNumber)

A <- subset.data.frame(A, select = c(encounterId,valueNumber))
colnames(SurgeryNature1) <- c("encounterId","SurgeryNature1")
colnames(A) <- c("encounterId","SurgeryNature2")
A <- join(SurgeryNature1,A,by="encounterId",match="all",type="full")

# Information from first Surgery Nature
count(A$SurgeryNature1)
# Information from second Surgery Nature
count(A$SurgeryNature2)

# What info do we have in surgery 1 when we have no info in surgery 2?
A1 <- count(A$SurgeryNature1[is.na(A$SurgeryNature2)])
A1 <- A1[order(A1$freq,decreasing = T),]
A1

AA <- A[is.na(A$SurgeryNature2),]
AA$SurgeryNature2[AA$SurgeryNature1 %in% 
                    c("Elective","Urgent","Scheduled")] <- "Elective"
AA$SurgeryNature2[AA$SurgeryNature1=="Not from Theatre"] <- NA
AA$SurgeryNature2[AA$SurgeryNature1=="Emergency"] <- "Emergency"
table(AA$SurgeryNature1,AA$SurgeryNature2,useNA = "always")
rm(A1,AA)

# Make sure to REMOVE SurgeryNature1 "Not From Theatre" from all further analysis
NotFromTheatrePatients <- subset(A,SurgeryNature1=="Not from Theatre",select = encounterId)$encounterId
A <- A[!A$encounterId %in% NotFromTheatrePatients,]

# Remove the hospital transfer patients
CICUBasic$PtLoS <- CICUBasic$PtLoS[!CICUBasic$PtLoS$encounterId %in% NotFromTheatrePatients,]
rm(NotFromTheatrePatients)

# Clinical contact: For those with missing data in nature of surgery 2, use the data from nature of surgery 1
A$SurgeryNature2[is.na(A$SurgeryNature2) & 
                   A$SurgeryNature1 %in% c("Elective","Urgent","Scheduled")] <- "Elective"
A$SurgeryNature2[is.na(A$SurgeryNature2) & A$SurgeryNature1=="Emergency"] <- "Emergency"
count(A$SurgeryNature1[is.na(A$SurgeryNature2)]) # We have no further information we can pull

# NA = Most common group = elective
count(A$SurgeryNature2)
A$SurgeryNature2[is.na(A$SurgeryNature2)] <- "Elective"

A <- subset.data.frame(A,select=-SurgeryNature1)
colnames(A) = c("encounterId","SurgeryNature")
CICUBasic$SurgeryNature <- A

rm(SurgeryNature1,A)

save.image("CICUBasic and Data Query Functions.RData")
