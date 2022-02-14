# Introduction ------------------------------------------------------------
# Patient data is stored in individual dataframes
# This script pulls each one and extracts 8am data from them
# Four windows are currently used depending on what is needed
# 1) Most recent data at 8am or earlier
# 2) Maximum/median over 24 hours
# 3) Most recent data in four hour window
# 4) Data at 8am specifically

rm(list=ls())

# loading the basic information and the time series datasets
# This takes a while to run, so only run it the first time and save the output
if(file.exists("CICUBasic and Raw Data for Timepoint Extraction.RData")){
  load("CICUBasic and Raw Data for Timepoint Extraction.RData")
}else{
  # Basic CICU information and functions for data extraction
  load("CICUBasic and Data Query Functions.RData")
  # Drop the database connection and data query functions as we do not use them here
  DBI::dbDisconnect(conn)
  rm(conn,DQFunctions)
  
  # Load the raw data
  # Time series data for all patients is stored in a list according to its location in the unit
  # This loads all the .RData files in the "CICU Raw Data" folder
  CICURaw <- new.env()
  lapply(dir(path = "CICU Raw Data/",pattern = ".RData",full.names = T), load, envir = CICURaw)
  CICURaw <- as.list(CICURaw)
  save.image("CICUBasic and Raw Data for Timepoint Extraction.RData")
}

# Timepoint Extraction ----------------------------------------------------

# This extensive function finds the most recent value per patient
# Specifics are in the function, but it does the following:
# 1) Removes NAs and values below 0 (unless specified to keep them in, see optional arguments)
# 2) Adds the patient admit/discharge times and removes values from before/after these, respectively
# 3) Finds most recent value from 1 of 4 windows:
# Default: Most recent to 8am on extractionDay
# Specific Windows: 
# 4 hour window = [extractionDay] 4:00am - 8:00am
# 8am: Day = [extractionDay] 8:00am
# 24hrs: Day = [extractionDay]-1 + 8:00am onwards to [extractionDay] + midnight - 8:00am
# 4) Optionally, can also find the maximum or median per patient of this data

TimepointExtraction <- function(df,Var,ExtractionDay,
                                keepLessThanZero = NULL,SpecificWindow = NULL,FindMaxMedian = NULL){
  
  require(lubridate)
  require(plyr)
  
  # Ensure correct arguments
  if(!is.null(FindMaxMedian)){
    if(!FindMaxMedian %in% c("Max","Median")){
      stop("Please supply \"Max\", or \"Median\" to the \"FindMaxMedian\" argument")}}
  
  if(!is.null(SpecificWindow)){
    if(!SpecificWindow %in% c("8am","4hourWindow","24hrs")){
      stop("Please supply \"8am\", \"4hourWindow\", \"24hrs\" to the \"Specific Window\" argument")}}
    
  if(!ExtractionDay%%1==0){
    stop("Please supply an integer to the \"ExtractionDay\" argument")}
  
  # Drop useless value columns
  A <- df[,!colnames(df)%in%c("terseForm", "verboseForm", "baseValueNumber", "baseUOM", "valueString")]
  
  # Report on number of NAs and remove
  if(sum(is.na(A$valueNumber))>0){
    writeLines(paste0(sum(is.na(A$valueNumber))," NA values found (",
                      round(sum(is.na(A$valueNumber))/nrow(A),4)*100,"%)\nRemoving NAs."))
    A <- A[!is.na(A$valueNumber),]
    }else{writeLines("0 NAs found.")}
  
  # Report on values of 0 and under, then remove
  if(is.null(keepLessThanZero)){
    writeLines(paste0(sum(A$valueNumber<0)," values under 0 found (",
                      round(sum(A$valueNumber<0)/nrow(A),4)*100,"%). Removing all."))
    A <- subset.data.frame(A, A$valueNumber>0)
    }else{
      writeLines(paste0(sum(A$valueNumber<0)," values under 0 found (",
                        round(sum(A$valueNumber<0)/nrow(A),4)*100,"%). Keeping all."))}
  
  # Join patient data
  A <- join(
    subset(CICUBasic$PtLoS,select = -c(age,gender)), A,
    by = "encounterId", type = "inner", match = "all")
  
  # Difference between chartTime and Start_date to find the hours since they
  # were admitted that the event happened
  A$TDiff <- as.numeric(round(difftime(A$chartTime, A$Start_date, units = "hour"),2))
  A$TDiff[is.na(A$TDiff)] <- 0;
  
  # Find the LoS remaining from each event
  A$LoSR <- as.numeric(A$LoS - A$TDiff);
  
  # Remove anything with a TDiff of under 0 (event uploaded before they were admitted)
  writeLines(paste0(nrow(A[A$TDiff<0,])," (",round(nrow(A[A$TDiff<0,])/nrow(A),4)*100,"%)",
                    " rows omitted (event uploaded before first heartbeat)"))
  A <- A[A$TDiff>=0,]
  
  # Remove anything with a LoSR of under 0 (event uploaded after they were discharged)
  writeLines(paste0(nrow(A[A$LoSR<0,])," (",round(nrow(A[A$LoSR<0,])/nrow(A),4)*100,"%)",
                    " rows omitted (event uploaded after last heartbeat)"))
  A <- A[A$LoSR>=0,]
  
  # Extract hour and minute of the event separately
  A$chartTimeHour <- as.integer(strftime(A$chartTime, format="%H",tz = attr(A$chartTime,"tzone")))
  A$chartTimeMinute <- as.integer(strftime(A$chartTime, format="%M"))
  
  # extract number of midnights that have passed from entering the ward (DIW: Days in ward)
  A$DIW <- as.integer(difftime(as.Date(A$chartTime), as.Date(A$Start_date), units = "days"));
  
  # Default extraction: Most recent value from 8am on extractionDay
    # Three kinds of values here: 
    # All values from day 0 < extractionDay-1
    # Day = extractionDay + hours = 0-7am + minutes = any
    # Day = extractionDay + hours = 8am + minutes = 0
  
  if(is.null(SpecificWindow)){
    A <- subset.data.frame(A, A$DIW%in%0:(ExtractionDay-1)|
                             (A$DIW==ExtractionDay & A$chartTimeHour <8)|
                             (A$DIW==ExtractionDay & A$chartTimeHour == 8 & A$chartTimeMinute == 0))
  }else{
    # Optional argument of SpecificWindow finds one of the following:
    # 4hour Window: Day = extractionDay + 4:00am - 8:00am
    # 8am: Day = extractionDay + 8am + 0 minutes
    # 24hrs: Day = extractionDay-1 + 8am onwards to extractionDay + up to 8am
    if(SpecificWindow=="4hourWindow"){
      writeLines("Finding data from 4am to 8am")
      # Pull extraction day data
      A <- subset.data.frame(A, A$DIW==ExtractionDay)
      # Pull 4:00-8:59am data
      A <- subset.data.frame(A, A$chartTimeHour<=8 & A$chartTimeHour>=4)
      # Drop 8:01 onwards
      A <- subset.data.frame(A, !(A$chartTimeHour==8 & A$chartTimeMinute>0))
    }
    if(SpecificWindow=="8am"){
      writeLines("Finding valueus at 8:00am")
      # USED TO JUST FIND 8:00 - 8:59, NOW WE GET 8:00 SPECIFICALLY
      # IF THIS DOESN'T WORK, GET 7am - 8am
      A <- A[A$DIW==ExtractionDay & A$chartTimeHour==8 & A$chartTimeMinute==0,]
    }
    if(SpecificWindow=="24hrs"){
      writeLines(paste0("Finding values from 8am on Day ",ExtractionDay-1," to 8am on Day ",ExtractionDay))
      A <- A[(A$DIW == ExtractionDay-1 & A$chartTimeHour>=8)|
               (A$DIW == ExtractionDay & A$chartTimeHour< 8)|
               (A$DIW == ExtractionDay & A$chartTimeHour == 8 & A$chartTimeMinute==0),]
    }
  }
  
  # Check the "FindMaxMedian" optional argument and find their max or median
  # Otherwise, find the most recent piece of information
  if(!is.null(FindMaxMedian)){
    A <- as.data.frame(aggregate(A$valueNumber, by=list(A$encounterId), FUN=tolower(FindMaxMedian)))
    }else{
      A <- A[order(A$encounterId, A$chartTime, decreasing = T),]
      A <- A[!duplicated(A$encounterId),]}
  
  # Invasive ventilation needs EtCO2 kept
  # Check raw data extraction
  if("Invasive Ventilation"%in%Var){
    A <- A[,c("encounterId","valueNumber","LoSR","EtCO2Valid")]
    colnames(A) <- c("encounterId",Var,paste0(Var," LoSR"),"EtCO2Valid")  
  }else{
    if(!is.null(FindMaxMedian)){
      colnames(A) <- c("encounterId",Var)
    }else{
      A <- A[,c("encounterId","valueNumber","LoSR")]
      colnames(A) <- c("encounterId",Var,paste0(Var," LoSR"))}}
  return(A)
}

# Timepoint Extraction Prep -----------------------------------------------

# This creates a dataframe of all the lists, their variables, and whether they require optional arguments
{
# Output list
z <- list()

# Work through the lists in CICU raw data
for(x in names(CICURaw)){
  if(is.list(CICURaw[[x]])){
    x1 <- names(CICURaw[[x]])
    
    # For each item in the list, pull which list its in, what variable it is, and its unit of measure
    for(x2 in x1){
      
      # unit of measure pre-process
      z1 <- unique(CICURaw[[x]][[x2]]$unitOfMeasure)
      if(is.null(z1)){z1 <- "Null"}
      if(!is.numeric(CICURaw[[x]][[x2]]$valueNumber)){z1 <- "Null for non-numeric"}
      z1 <- paste0(z1,collapse = ", ")
      
      z[[x2]] <- data.frame("origin"=x,"var"=x2,"UoM"=z1)
    }
  }
}
rm(x,x1,x2,z1)
z <- do.call(rbind.data.frame,z);rownames(z) <- NULL

z$keepLessThanZero <- "NULL"
z$SpecifcWindow <- "NULL"
z$FindMaxMedian <- "NULL"
z$outputName <- z$var

require(data.table)

# Resp rate is done by default, median, and maximum.
z1 <- z2 <- z[z$var%like%"Respiratory Rate",]
z1$FindMaxMedian <- "Max";z1$SpecifcWindow <- "24hrs";z1$outputName <- "Max Resp Rate"
z2$FindMaxMedian <- "Median";z2$SpecifcWindow <- "24hrs";z2$outputName <- "Median Resp Rate"
z <- rbind(z,z1,z2);rm(z1,z2)

# BEecf we don't remove below 0
z$keepLessThanZero[z$var=="BEecf"] <- 1

# Mechanical ventilation is 4 hour window
z$SpecifcWindow[z$var=="Mechanical Ventilation"] <- "4hourWindow"

# All Drug Infusions by 8am
z$SpecifcWindow[z$origin=="Medication"] <- "8am"

# Remove building blocks
# z <- z[!z$var%like%" PCo2",]
z <- z[!z$var%like%"SurgeryType",]
z <- z[!z$var%like%"LVEF",]

rownames(z) <- NULL
}

# Timepoint Extraction Run ------------------------------------------------

ExtractionDay <- 1

# Attributes that doesn't change:
# Patient weight, SurgeryNature, SurgeryType, LVEF, and IABP

CICUTimeExtract <- list()
for(x in 1:nrow(z)){
  FindMaxMedian <- z$FindMaxMedian[x]
  keepLessThanZero <- z$keepLessThanZero[x]
  SpecificWindow <- z$SpecifcWindow[x]
  if(FindMaxMedian=="NULL"){FindMaxMedian <- NULL}
  if(keepLessThanZero=="NULL"){keepLessThanZero <- NULL}
  if(SpecificWindow=="NULL"){SpecificWindow <- NULL}
  
  writeLines(paste0("Extracting 8am Day ",ExtractionDay," values for ",z$origin[x],": ",z$outputName[x]))
  
  CICUTimeExtract[[z$outputName[x]]] <- 
    TimepointExtraction(ExtractionDay = ExtractionDay,
                        df = CICURaw[[z$origin[x]]][[z$var[x]]],
                        Var = z$outputName[x],
                        keepLessThanZero = keepLessThanZero,
                        SpecificWindow = SpecificWindow,
                        FindMaxMedian = FindMaxMedian)
  writeLines("")
  rm(FindMaxMedian,keepLessThanZero,SpecificWindow)
}



# Function to use some value if available, but back up to another if not
# Temporary arguments for debugging
Primary <- "Arterial PCo2";Secondary <- "Venous PCo2"
rm(Primary,Secondary)

PrimarySecondary <- function(Primary,Secondary,MergedVarName){
  
  # Pull all primary and secondary values from their relevant dataframes
  A <- join(CICUTimeExtract[[Primary]],CICUTimeExtract[[Secondary]],by="encounterId",type="full")
  
  # When primary is missing, replace with secondary
  A[[Primary]][is.na(A[[Primary]])] <- A[[Secondary]][is.na(A[[Primary]])]
  
  # Update the relevant remaining length of stay (LoSR)
  A[[paste0(Primary," LoSR")]][is.na(paste0(Primary," LoSR"))] <- 
    A[[paste0(Secondary, "LoSR")]][is.na(paste0(Primary, " LoSR"))]
  
  # New data frame containing the merged columns
  A1 <- data.frame("encounterId" = A$encounterId)
  A1[[paste0(MergedVarName)]] <- A[[Primary]]
  A1[[paste0(MergedVarName," LoSR")]] <- A[[paste0(Primary," LoSR")]]
  
  # Remove the primary and secondary dataframes and add the new merged dataframe
  CICUTimeExtract[[Primary]] <<- NULL
  CICUTimeExtract[[Secondary]] <<- NULL
  CICUTimeExtract[[paste0(MergedVarName)]] <<- A1
}

PrimarySecondary(Primary = "Arterial PCo2",Secondary = "Venous PCo2",MergedVarName = "PCo2")
PrimarySecondary(Primary = "Central Temp",Secondary = "Peripheral Temp",MergedVarName = "Temperature (Final)")
PrimarySecondary(Primary = "pH Arterial",Secondary = "pH Venous",MergedVarName = "pH")
PrimarySecondary(Primary = "Glu",Secondary = "GluBMStick",MergedVarName = "Glu")

rm(PrimarySecondary)

# Extraction Day Dataframe ------------------------------------------------

# Link together the variables' dataframes
# Start with the timepoint extraction data, then add on variables that don't require timepoint extraction

for(x in names(CICUTimeExtract)){
  A <- join(A,CICUTimeExtract[[x]],by="encounterId")
  rm(x)
}

# Set the LoS remaining aside
LoSRs <- A[,colnames(A)%like%"LoSR"]
A <- A[,!colnames(A)%like%"LoSR"]

# Update the ventilation values
A$`Mechanical Ventilation`[is.na(A$`Mechanical Ventilation`)] <- "Not Received"
A$`Invasive Ventilation`[is.na(A$`Invasive Ventilation`)] <- "Not Received"
A$`Mechanical Ventilation`[A$`Mechanical Ventilation`=="Mechanical"] <- "Received"
A$`Invasive Ventilation`[A$`Invasive Ventilation`=="Invasive"] <- "Received"

# Remove ATV, MAP, and PEEP for patients who aren't ventilated
A1 <- A$encounterId[A$`Mechanical Ventilation`=="Not Received" & A$`Invasive Ventilation` == "Not Received"]
A$`Actual Tidal Volume`[A$encounterId %in% A1] <- NA
A$`Mean Airway Pressure`[A$encounterId %in% A1] <- NA
A$PEEP[A$encounterId %in% A1] <- NA
# EtCO2 only to those with a tube or thracheostomy
A$EtCO2[A$EtCO2Valid==0] <- NA
A$EtCO2[is.na(A$EtCO2Valid)] <- NA


# These are time series data.
# Currently not using
A <- subset.data.frame(A,select = -c(Indwelling,`Chest: Mediastinal`))

colnames(A)[colnames(A)=="Ca\\+\\+"] <- "Ca2"
colnames(A)[colnames(A)=="Ca2"] <- "Ca++"

# Variables that did not require timepoint extraction (what surgery they had etc)
CICURaw$Assessment$SurgeryType <- CICURaw$Assessment$SurgeryType[!duplicated(CICURaw$Assessment$SurgeryType),]
CICURaw$Assessment$`LVEF Pre-Op` <- CICURaw$Assessment$`LVEF Pre-Op`[!duplicated(CICURaw$Assessment$`LVEF Pre-Op`),]

A <- join(A,CICURaw$Assessment$SurgeryType,by="encounterId")
A <- join(A,CICURaw$Assessment$`LVEF Pre-Op`,by="encounterId")

# Find patients with discrepancies and assign to worst category (emergency)
A1 <- CICUBasic$SurgeryNature
A1 <- A1[!duplicated(A1),]
A1 <- A1[A1$encounterId %in% A1$encounterId[duplicated(A1$encounterId)],]
CICUBasic$SurgeryNature$SurgeryNature[CICUBasic$SurgeryNature$encounterId%in%A1$encounterId] <-  "Emergency"
CICUBasic$SurgeryNature <- CICUBasic$SurgeryNature[!duplicated(CICUBasic$SurgeryNature),]

A <- join(A,CICUBasic$SurgeryNature,by="encounterId")
A$SurgeryNature

A <- subset.data.frame(A,select = -c(pO2,FiO2,EtCO2Valid,chartTime))

# Adjust LoS per patient for 8am D[extractionDay] -------------------------
rm(CICUTimeExtract,CICURaw,z,A1,A2,ModelData,TimepointExtraction)

# Calculate the time between first heart rate and 8am D1 per patient
require(lubridate)
# Extract the year/month/day of their admit time, then add 8:00am on the extraction Day in hours
A$Start1 <- strptime(A$Start_date,format = "%Y-%m-%d",tz="GMT") + (((ExtractionDay*24)+8)*60*60)
# Calculate the time between first heart rate and 8am D1
A$TimeDiff <- as.integer(difftime(A$Start1,A$Start_date))
# Adjust LoS accordingly
A$LoS8amD1 <- as.integer(A$LoS-A$TimeDiff)
A <- subset.data.frame(A,select = -c(Start1,TimeDiff))

# Misc cleaning -----------------------------------------------------------
A$IABP[is.na(A$IABP)] <- 0
A$`LVEF Pre-Op` <- as.factor(A$`LVEF Pre-Op`)
A$SurgeryNature <- as.factor(A$SurgeryNature)
A$`Mechanical Ventilation` <- as.factor(A$`Mechanical Ventilation`)
A$`Invasive Ventilation` <- as.factor(A$`Invasive Ventilation`)
A$gender <- as.factor(A$gender)
A$MCH <- as.numeric(A$MCH)
A$`Total Protein` <- as.numeric(A$`Total Protein`)

# Non-Research variables:
# encounterId,patientId,Start_date,End_date,LoS,TotalSurgeries

.GlobalEnv[[paste0("CICUD",ExtractionDay,"8am")]] <- A
.GlobalEnv[[paste0("LoSRs",ExtractionDay,"8am")]] <- LoSRs
rm(A,LoSRs)

save(paste0("LoSRs",ExtractionDay,"8am"),
     file = paste0("CICU Timepoint Extraction Data/LoSRs",ExtractionDay,".RData"))
save(paste0("CICUD",ExtractionDay,"8am"),
     file = paste0("CICU Timepoint Extraction Data/CICU Day ",ExtractionDay,".RData"))

