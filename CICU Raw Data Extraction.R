# This script uses the patient data processing toolkit to retrieve data from the
# back-end of the philips ICIP system located in the BRI's CICU 
rm(list=ls())
# Basic CICU information and functions for data extraction
load("CICUBasic and Data Query Functions.RData")
CICUBasic$OpenConn()

# Lab Tests ---------------------------------------------------------------

# List to fill with each variable's dataframe
LabTests <- list()
# Set the table you are pulling data from
DQFunctions$TableSearch <- "PtLabResult"

# Lab Tests: Coagulation --------------------------------------------------

DQFunctions$Var <- "APTT"

# Search the table for the variable
DQFunctions$IdSearch()

# Make any edits to the intervention or attribute Ids that you wish
# When you're happy with your terms, run the query.
A <- DQFunctions$SQLQuery()

# Once the query is run, find which columns contain values
DQFunctions$DataFind(A) # terseForm is 99.99% values and valueString is all values

A[is.na(A$terseForm),c("terseForm","valueString")] # when terseForm = NA, valueString = " "
# Drop this
A <- A[!is.na(A$terseForm),]

# Check for non-numeric data, clean symbols, then remove non-numeric values 
DQFunctions$NonNumericFinder(A,"terseForm")
A <- DQFunctions$ValueCleaner(A,"terseForm",symbolClean = 1)
A <- DQFunctions$ValueCleaner(A,"terseForm",rmNonNumeric = 1)
DQFunctions$NonNumericFinder(A,"terseForm")

A$valueNumber <- as.numeric(A$terseForm)

DQFunctions$QuantileValueNumber(A)

# Here all our data appears to be from the same distribution despite the 3 Id combinations
# No need to split the dataframe or do further work looking for missed values

# Ensure unitOfMeasure is also consistent
# You can either infer this if one of the Ids has unitOfMeasure filled in, or from a clinician
A$unitOfMeasure <- "s"

# Save from temporary data frame to the LabTests list
LabTests[[DQFunctions$Var]] <- A
rm(A)


DQFunctions$Var <- "Prothrombin"
DQFunctions$IdSearch()
A <- DQFunctions$SQLQuery()
DQFunctions$DataFind(A)
DQFunctions$NonNumericFinder(A,"valueNumber")
DQFunctions$QuantileValueNumber(A)
# Both seem similar, its a case of duplicated Ids
LabTests[["Prothrombin Time"]] <- A


# Lab Tests: Biochemistry -------------------------------------------------

DQFunctions$Var <- "Albumin"
DQFunctions$IdSearch()
A <- DQFunctions$SQLQuery()
DQFunctions$DataFind(A)
DQFunctions$NonNumericFinder(A,"valueNumber")
DQFunctions$QuantileValueNumber(A)
LabTests[[DQFunctions$Var]] <- A


DQFunctions$Var <- "Alkaline"
DQFunctions$IdSearch()
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"valueNumber")
DQFunctions$QuantileValueNumber(A)
A$unitOfMeasure <- "U/L"
LabTests[["Alkaline Phosphate"]] <- A

DQFunctions$Var <- "ALT"
DQFunctions$IdSearch()
DQFunctions$IdSearch(AttIdsDrop = 16240)
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"valueNumber")
DQFunctions$QuantileValueNumber(A)
A$unitOfMeasure <-  "U/L"
LabTests[["ALT (SGPT)"]] <- A


DQFunctions$Var <- "Bicarbonate"
DQFunctions$IdSearch()
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"valueNumber")
DQFunctions$QuantileValueNumber(A)
LabTests[[DQFunctions$Var]] <- A

DQFunctions$IdSearch(Variable = "Calcium")
# Three variables here, Calcium, Ca++, Calcium (adjusted)
# We do Ca++ in haematology
DQFunctions$IdSearch(Variable = "Calcium",IntIdsDrop = 721)
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"terseForm")
A$valueNumber <- as.numeric(A$terseForm)
DQFunctions$QuantileValueNumber(A)
LabTests[["Calcium (adjusted)"]] <- A


DQFunctions$IdSearch(Variable = "C-reactive")
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"valueString")
A <- DQFunctions$ValueCleaner(A,"valueString",symbolClean = 1)
A <- DQFunctions$ValueCleaner(A,"valueString",rmNonNumeric = 1)
A$valueNumber <- as.numeric(A$valueString)
DQFunctions$QuantileValueNumber(A)
A$unitOfMeasure <- "mg/L"
LabTests[["C-reactive Protein"]] <- A


DQFunctions$IdSearch(Variable = "Chloride")
# 200% of patients found, 2 variables here
# Clinical contact: 
# Cl- is a serum (blood gas) while Chloride is a biochemistry test
# They both measure the same thing but with different equipment, hence the similar values
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"valueNumber")
DQFunctions$QuantileValueNumber(A)
LabTests[["Chloride"]] <- A[A$interventionId==630,]
A <- A[!A$interventionId==630,]
A <- DQFunctions$BloodGasArterial(A)
LabTests[["Cl-"]] <- A

DQFunctions$IdSearch(Variable = "eGFR")
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"terseForm")
A <- DQFunctions$ValueCleaner(A,"terseForm",symbolClean = 1)
A$terseForm[A$terseForm == "eCFR 29"] <- 29
A <- DQFunctions$ValueCleaner(A,"terseForm",rmNonNumeric = 1)
A$valueNumber <- as.numeric(A$terseForm)
DQFunctions$QuantileValueNumber(A)
A$unitOfMeasure <- "ml/kg"
LabTests[[DQFunctions$Var]] <- A



DQFunctions$IdSearch(Variable = "Magnesium")
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"valueNumber")
DQFunctions$QuantileValueNumber(A)
LabTests[[DQFunctions$Var]] <- A


DQFunctions$IdSearch(Variable = "Phosphate")
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"valueNumber")
DQFunctions$QuantileValueNumber(A)
LabTests[[DQFunctions$Var]] <- A


DQFunctions$IdSearch(Variable = "Potassium")
DQFunctions$IdSearch(Variable = "Potassium",AttIdsDrop = 16240)
# Clinical contact: Potassium is biochem, K = blood gas
# K needs arterialBloodGas processing, potassium does not
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"terseForm")
A$valueNumber <- as.numeric(A$terseForm)

# Set aside Potassium
A1 <- A[A$interventionId==510,]
A <- A[!A$interventionId==510,]
A <- DQFunctions$BloodGasArterial(A)
DQFunctions$QuantileValueNumber(A)
LabTests[["K"]] <- A
A <- A1
rm(A1)
DQFunctions$QuantileValueNumber(A)
LabTests[[DQFunctions$Var]] <- A


DQFunctions$IdSearch(Variable = "Sodium")
# Sodium is biochem, Na is blood gas.
DQFunctions$IdSearch(Variable = "Sodium",IntIdsDrop = c(4394,19540))
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"terseForm")
A$valueNumber <- as.numeric(A$terseForm)

# Separate
A1 <- A[A$interventionId==726,]
A <- A[!A$interventionId==726,]
A1 <- DQFunctions$BloodGasArterial(A1)
DQFunctions$QuantileValueNumber(A)
DQFunctions$QuantileValueNumber(A1)

LabTests[["Sodium"]] <- A
LabTests[["Na"]] <- A1



DQFunctions$IdSearch(Variable = "Bilirubin")
DQFunctions$IdSearch(Variable = "Bilirubin",IntIdsDrop = 19512)
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"terseForm")
A <- DQFunctions$ValueCleaner(A,"terseForm",symbolClean = 1)
A <- DQFunctions$ValueCleaner(A,"terseForm",rmNonNumeric = 1)
DQFunctions$NonNumericFinder(A,"terseForm")
A$valueNumber <- as.numeric(A$terseForm)
DQFunctions$QuantileValueNumber(A)
A$unitOfMeasure <- "mmol/L"
LabTests[["Total Bilirubin"]] <- A

DQFunctions$IdSearch(Variable = "Total protein")
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"valueNumber")
DQFunctions$QuantileValueNumber(A)
A$valueNumber[is.na(A$valueNumber)] <- A$terseForm[is.na(A$valueNumber)]
LabTests[["Total Protein"]] <- A

DQFunctions$IdSearch(Variable = "Urea")
DQFunctions$IdSearch(Variable = "Urea",DeleteUrineIds = 1)
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"terseForm")
A$valueNumber <- as.numeric(A$terseForm)
DQFunctions$QuantileValueNumber(A)
LabTests[[DQFunctions$Var]] <- A

rm(A,A1)

# Lab Tests: Blood Gases --------------------------------------------------

DQFunctions$IdSearch(Variable = "Basophils")
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"terseForm")
A$valueNumber <- as.numeric(A$terseForm)
DQFunctions$QuantileValueNumber(A)
A$unitOfMeasure <- "10*9/L"
LabTests[[DQFunctions$Var]] <- A

DQFunctions$IdSearch(Variable = "Lymphocyte")
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"terseForm")
A$terseForm[A$terseForm==DQFunctions$NonNumericFinderOutput$x] <- 1
A$valueNumber <- as.numeric(A$terseForm)
DQFunctions$QuantileValueNumber(A)
A$unitOfMeasure <- "10*9/L"
LabTests[["Lymphocytes"]] <- A

# Haeamatocrit = haematology lab test, Hct = blood gas, divide hct by 100
DQFunctions$IdSearch(Variable = "Haematocrit")
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"valueNumber")

A1 <- A[A$interventionId==724,]
A <- A[!A$interventionId==724,]

A1$valueNumber <- A1$valueNumber/100
A1 <- DQFunctions$BloodGasArterial(A1)

LabTests[["Hct"]] <- A1
LabTests[["Haematocrit"]] <- A1

rm(A,A1)


DQFunctions$IdSearch(Variable = "Haemoglobin")
DQFunctions$IdSearch(Variable = "Haemoglobin",IntIdsDrop = c(4350,4416,20306,20361))
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"valueNumber")

LabTests[["Haemoglobin"]] <- A[A$interventionId==756,]
LabTests[["tHb"]] <- DQFunctions$BloodGasArterial(A[A$interventionId==766,])


DQFunctions$IdSearch(Variable = "MCH")
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"valueNumber")

# We pulled MCH and MCHC
LabTests[["MCH"]] <- A[!A$interventionId==634,]
LabTests[["MCHC"]] <- A[A$interventionId==634,]
LabTests[["MCH"]]$unitOfMeasure <- "pg"
LabTests[["MCH"]]$valueNumber[is.na(LabTests[["MCH"]]$valueNumber)] <- 
  LabTests[["MCH"]]$terseForm[is.na(LabTests[["MCH"]]$valueNumber)]



DQFunctions$IdSearch(Variable = "Monocytes")
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"terseForm")
A$valueNumber <- as.numeric(A$terseForm)
DQFunctions$QuantileValueNumber(A)
A$unitOfMeasure <- "10*9/L"
LabTests[[DQFunctions$Var]] <- A

DQFunctions$IdSearch(Variable = "Neutrophils")
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"terseForm")
A$valueNumber <- as.numeric(A$terseForm)
DQFunctions$QuantileValueNumber(A)
A$unitOfMeasure <- "10*9/L"
LabTests[[DQFunctions$Var]] <- A


DQFunctions$IdSearch(Variable = "Platelets")
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"terseForm")
A$valueNumber <- as.numeric(A$terseForm)
DQFunctions$QuantileValueNumber(A)
A$unitOfMeasure <- "10*9/L"
LabTests[[DQFunctions$Var]] <- A


DQFunctions$IdSearch(Variable = "WBC")
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"terseForm")
A$valueNumber <- as.numeric(A$terseForm)
DQFunctions$QuantileValueNumber(A)
# AttId 16240 looks different but its due to small sample size
A$unitOfMeasure <- "10*9/L"
LabTests[[DQFunctions$Var]] <- A



# Lab Tests: Haematology --------------------------------------------------

DQFunctions$IdSearch(Variable = "BEecf")
A <- DQFunctions$SQLQuery()
DQFunctions$DataFind(A)
DQFunctions$NonNumericFinder(A,"valueNumber")
DQFunctions$QuantileValueNumber(A)
LabTests[[DQFunctions$Var]] <- A

# Have to escape symbols for "Ca++"
DQFunctions$IdSearch(Variable = "Ca\\+\\+")
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"terseForm")
A <- DQFunctions$ValueCleaner(A,"terseForm",symbolClean = 1)
DQFunctions$DataFind(A)
A[A$terseForm=="No result",]
A <- A[!A$terseForm=="No result",]
A$valueNumber <- as.numeric(A$terseForm)
DQFunctions$QuantileValueNumber(A)
A$unitOfMeasure <- "mmol/L"
LabTests[[DQFunctions$Var]] <- DQFunctions$BloodGasArterial(A)


DQFunctions$IdSearch(Variable = "Glu")
# Clinical Lead: Use Glu and then BM stick if glu missing
x <- DQFunctions$IdSearchOutput$IntIdsDetail$interventionId
x <- x[!x%in%c("744","31620","740")]
DQFunctions$IdSearch(Variable = "Glu",IntIdsDrop = x)
rm(x)
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"terseForm")
A <- DQFunctions$ValueCleaner(A,"terseForm",symbolClean = 1)
A <- A[!A$terseForm%in%DQFunctions$NonNumericFinderOutput$x,]
DQFunctions$NonNumericFinder(A,"terseForm")
A$valueNumber <- as.numeric(A$terseForm)
DQFunctions$QuantileValueNumber(A)

LabTests[[DQFunctions$Var]] <- DQFunctions$BloodGasArterial(A[!A$interventionId==740,])
LabTests$Glu$unitOfMeasure <- "mmol/L"
LabTests[["GluBMStick"]] <- A[A$interventionId==740,]


DQFunctions$IdSearch(Variable = "HCO3")
DQFunctions$IdSearch(Variable = "HCO3",IntIdsDrop = c(4328,48958))
A <- DQFunctions$SQLQuery()

DQFunctions$NonNumericFinder(A,"terseForm")
A <- DQFunctions$ValueCleaner(A,"terseForm",rmNonNumeric = 1)
A$valueNumber <- as.numeric(A$terseForm)
DQFunctions$QuantileValueNumber(A)
A$unitOfMeasure <- "mmol/L"
LabTests[["HCO3-std"]] <- A


DQFunctions$IdSearch(Variable = "Lac")
DQFunctions$IdSearch(Variable = "Lac",IntIdsDrop = c(4426,4499))
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"terseForm")
A <- DQFunctions$ValueCleaner(A,"terseForm",symbolClean = 1)
A <- DQFunctions$ValueCleaner(A,"terseForm",rmNonNumeric = 1)
A$valueNumber <- as.numeric(A$terseForm)
DQFunctions$QuantileValueNumber(A)
A <- A[!A$interventionId==4495,]
DQFunctions$QuantileValueNumber(A)
LabTests[["Lactic Acid"]] <- DQFunctions$BloodGasArterial(A)

DQFunctions$IdSearch(Variable = "pCO2")
# 16k patients = multiple variables here
# Clinical lead: We default to arterial PCO2 and then use venous to fill in the gaps.
A <- DQFunctions$SQLQuery()

DQFunctions$NonNumericFinder(A,"terseForm")
A <- DQFunctions$ValueCleaner(A,"terseForm",rmNonNumeric = 1)
A$valueNumber <- as.numeric(A$terseForm)
DQFunctions$QuantileValueNumber(A)
A <- DQFunctions$BloodGasArterial(A)
A$unitOfMeasure <- "kPa"
DQFunctions$DataFind(A)

LabTests[["Arterial PCo2"]] <- A[A$interventionId%in%c(732,13966,48949,48967),]
LabTests[["Venous PCo2"]] <- A[!A$interventionId%in%c(732,13966,48949,48967),]

DQFunctions$IdSearch(Variable = "pH",DontPermuteVarSearch = 1)
# remove NICU var
DQFunctions$IdSearch(Variable = "pH",DontPermuteVarSearch = 1,IntIdsDrop = 56029)
# 16k, again 2 vars here, arterial and venous.
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"terseForm")
A <- DQFunctions$ValueCleaner(A,"terseForm",symbolClean = 1)
A <- DQFunctions$ValueCleaner(A,"terseForm",rmNonNumeric = 1)
A$valueNumber <- as.numeric(A$terseForm)
A <- DQFunctions$BloodGasArterial(A)
DQFunctions$QuantileValueNumber(A)
DQFunctions$DataFind(A)

LabTests[["pH Arterial"]] <- A[A$interventionId%in%c(730,13965,48948,48966),]
LabTests[["pH Venous"]] <- A[!A$interventionId%in%c(730,13965,48948,48966),]

# Lab Tests: Misc ---------------------------------------------------------

DQFunctions$IdSearch(Variable = "COHb")
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"terseForm")
A <- DQFunctions$ValueCleaner(A,"terseForm",rmNonNumeric = 1)
A$valueNumber <- as.numeric(A$terseForm)
DQFunctions$QuantileValueNumber(A)
A$unitOfMeasure <- "%"
LabTests[[DQFunctions$Var]] <- A


DQFunctions$IdSearch(Variable = "Eosinophils")
A <- DQFunctions$SQLQuery()

DQFunctions$NonNumericFinder(A,"terseForm")
A$valueNumber <- as.numeric(A$terseForm)
DQFunctions$QuantileValueNumber(A)
A$unitOfMeasure <- "10*9/L"
LabTests[[DQFunctions$Var]] <- A


DQFunctions$IdSearch(Variable = "Globulin")
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"valueNumber")
DQFunctions$QuantileValueNumber(A)
LabTests[[DQFunctions$Var]] <- A

DQFunctions$IdSearch(Variable = "pO2")
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"terseForm")
A <- DQFunctions$ValueCleaner(A,"terseForm",rmNonNumeric = 1)
A$valueNumber <- as.numeric(A$terseForm)
# Clinical lead: do not use venous
A <- A[A$interventionId%in%c(734,13963,48950,48968),]
A <- DQFunctions$BloodGasArterial(A)

DQFunctions$QuantileValueNumber(A)
A$unitOfMeasure <- "kPa"

LabTests[[DQFunctions$Var]] <- A

# Lab Tests: Discretionary ------------------------------------------------
# Discretionary i.e., only given to certain patients under a clinician's orders
DQFunctions$TableSearch <- "PtLabResult"

DQFunctions$IdSearch(Variable = "Anion")
A <- DQFunctions$SQLQuery()

DQFunctions$NonNumericFinder(A,"terseForm")
A$valueNumber <- as.numeric(A$terseForm)
DQFunctions$QuantileValueNumber(A)
A$unitOfMeasure <- "No Units"
LabTests[["Anion-Gap"]] <- A


DQFunctions$IdSearch(Variable = "Fibrinogen")
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"valueNumber")
DQFunctions$QuantileValueNumber(A)
LabTests[[DQFunctions$Var]] <- A

DQFunctions$IdSearch(Variable = "Uric")
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"terseForm")
A$valueNumber <- as.numeric(A$terseForm)
DQFunctions$QuantileValueNumber(A)
A$unitOfMeasure <- "mg/dL"
LabTests[["Urate [Uric Acid]"]] <- A


DQFunctions$IdSearch(Variable = "Gamma-GT|GGT")
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"terseForm")
A$valueNumber <- as.numeric(A$terseForm)
DQFunctions$QuantileValueNumber(A)
A$unitOfMeasure <- "Units/L"
LabTests[["Gamma-GT (GGT)"]] <- A

save(list = "LabTests",file = "CICU Raw Data/Lab Tests.RData")
rm(LabTests);gc()

# Vital Signs -------------------------------------------------------------
VitalSigns <- list()

DQFunctions$TableSearch <- "PtAssessment"

DQFunctions$IdSearch(Variable = "Respiratory Rate")
# Only want resp rate not Spont or Total
DQFunctions$IdSearch(Variable = "Respiratory Rate",IntIdsDrop = c(2199,3451))
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"valueNumber")
DQFunctions$QuantileValueNumber(A)
VitalSigns[[DQFunctions$Var]] <- A

DQFunctions$IdSearch(Variable = "SpO2")
DQFunctions$IdSearch(Variable = "SpO2",IntIdsDrop = 5382)
A <- DQFunctions$SQLQuery()

DQFunctions$NonNumericFinder(A,"valueNumber")
DQFunctions$QuantileValueNumber(A)
VitalSigns[[DQFunctions$Var]] <- A


DQFunctions$IdSearch(Variable = "FiO2")
DQFunctions$IdSearch(Variable = "FiO2",IntIdsDrop = c(1980,2973,3117,3124))
A <- DQFunctions$SQLQuery()

DQFunctions$NonNumericFinder(A,"valueNumber")
DQFunctions$QuantileValueNumber(A)
VitalSigns[[DQFunctions$Var]] <- A

# pO2/FiO2 ratio
require(lubridate)
load("CICU Raw Data/Lab Tests.RData")
A1 <- LabTests$pO2
rm(LabTests);gc()

A <- A[,!colnames(A)%in%c("terseForm","verboseForm","baseValueNumber","baseUOM","valueString")]
A1 <- A1[,!colnames(A1)%in%c("terseForm","verboseForm","baseValueNumber","baseUOM","valueString")]
colnames(A)[5:6] = c("pO2","pO2unitOfMeasure")
colnames(A1)[5:6] = c("FiO2","FiO2unitOfMeasure")

require(lubridate);
A$chartTimeR <- round_date(A$chartTime,unit = "hour")
A1$chartTimeR <- round_date(A1$chartTime,unit = "hour")

# Floor the timestamps to the nearest 2 hours
for(x in seq(1,23,2)){
  x1 <- x-1
  if(x<10){x <- paste0("0",x)}
  if(x1<10){x1 <- paste0("0",x1)}
  
  A$chartTimeR <- gsub(x = A$chartTimeR, pattern = paste0(x,":00:00"),
                       replacement = paste0(x1,":00:00"))
  A1$chartTimeR <- gsub(x = A1$chartTimeR, pattern = paste0(x,":00:00"),
                       replacement = paste0(x1,":00:00"))
  rm(x,x1)
}

# order by encounterId and chartTime, then join on encounterId
C <- join(A,A1,by=c("encounterId","chartTimeR"),type="inner")

C$po2fio2ratio <- round(C$pO2/C$FiO2,4)
C <- C[!is.na(C$po2fio2ratio),]
count(C$FiO2unitOfMeasure)
count(C$pO2unitOfMeasure)
colnames(C)
C <- subset.data.frame(C,select = c(encounterId,chartTimeR,po2fio2ratio))
colnames(C) <- c("encounterId","chartTime","valueNumber")
VitalSigns[["pO2 FiO2 Ratio"]] <- C
rm(A,A1,C)

# VitalSigns chartTime not kept as posixct
VitalSigns$`pO2 FiO2 Ratio`$chartTime <- as.POSIXct(VitalSigns$`pO2 FiO2 Ratio`$chartTime,tz = "UTC")

DQFunctions$IdSearch(Variable = "Heart Rate")
DQFunctions$IdSearch(Variable = "Heart Rate",IntIdsDrop = 3090)
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"valueNumber")
DQFunctions$QuantileValueNumber(A)
VitalSigns[[DQFunctions$Var]] <- A


DQFunctions$IdSearch(Variable = "Arterial")
DQFunctions$IdSearch(Variable = "Arterial",IntIdsDrop = c(25742,3431),AttIdsDrop = c(27370,43795,61))
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"valueNumber")
DQFunctions$QuantileValueNumber(A)

x <- "Systolic"
for(x in c("Systolic","Diastolic","Mean")){
  
  x1 <- DQFunctions$IdSearchOutput$AttIdsDetail
  x1 <- x1$attributeId[x1$attributeShortLabel==x]
  VitalSigns[[paste0("Arterial Blood Pressure ",x)]] <- A[A$attributeId%in%x1,]
  rm(x,x1)
}
# Clinical Contact: drop diastolic
VitalSigns$`Arterial Blood Pressure Diastolic` <- NULL

DQFunctions$IdSearch(Variable = "CVP")
DQFunctions$IdSearch(Variable = "CVP",IntIdsDrop = c(25738,3048,2988))
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"valueNumber")
DQFunctions$QuantileValueNumber(A)
A$unitOfMeasure <- "mmHg"
VitalSigns[[DQFunctions$Var]] <- A



DQFunctions$IdSearch(Variable = "Central Temp")
DQFunctions$IdSearch(Variable = "Central Temp",AttIdsDrop = c(32674,48876,21425))
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"valueNumber")
DQFunctions$QuantileValueNumber(A)
VitalSigns[[DQFunctions$Var]] <- A


DQFunctions$IdSearch(Variable = "Peripheral Temp")
DQFunctions$IdSearch(Variable = "Peripheral Temp",AttIdsDrop = c(32689,48829,14090))
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"valueNumber")
DQFunctions$QuantileValueNumber(A)
VitalSigns[[DQFunctions$Var]] <- A


# Ventilation -------------------------------------------------------------

DQFunctions$TableSearch <- "PtAssessment"

DQFunctions$IdSearch(Variable = "Airway")
# Narrow to explicitly "airway" IntId 2098
DQFunctions$IdSearchOutput$IntAttIdsTableSummary
A <- DQFunctions$SQLQuery(IntIds = 2098,AttIds = 8590)
DQFunctions$NonNumericFinder(A,"terseForm")
# Categorical data, so no need to clean
A$valueNumber <- A$terseForm
# Group into invasive ventilation vs natural breathing
A$valueNumber[A$terseForm%in%c(
  "CPAP Hood","CPAP Mask","Mini-Trach","Nasal ETT","Oral ETT","Trach")] <- "Invasive"
A$valueNumber[is.na(A$valueNumber)] <- "Natural"
count(A$valueNumber)

# Mark the patients valid for EtCO2 data
A$EtCO2Valid[A$terseForm=="Trach"] <- 1
A$EtCO2Valid[A$terseForm=="Oral ETT"] <- 1
A$EtCO2Valid[A$terseForm=="Nasal ETT"] <- 1
A$EtCO2Valid[A$terseForm=="Mini-Trach"] <- 1
A$EtCO2Valid[is.na(A$EtCO2Valid)] <- 0
VitalSigns[["Invasive Ventilation"]] <- A


DQFunctions$IdSearch(Variable = "EtCO2")
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"valueNumber")
DQFunctions$QuantileValueNumber(A)

# Clinical lead: EtCO2 data only valid for those with certain ventilation types
A1 <-  VitalSigns$`Invasive Ventilation`
A1 <- A1$encounterId[A1$EtCO2Valid==1]

A <- A[A$encounterId%in%A1,]

VitalSigns[["EtCO2"]] <- A



DQFunctions$IdSearch(Variable = "Actual Tidal Volume")
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"valueNumber")
DQFunctions$QuantileValueNumber(A)
VitalSigns[[DQFunctions$Var]] <- A

DQFunctions$IdSearch(Variable = "Mean Airway Pressure")
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"valueNumber")
DQFunctions$QuantileValueNumber(A)
VitalSigns[[DQFunctions$Var]] <- A

DQFunctions$IdSearch(Variable = "PEEP")
DQFunctions$IdSearch(Variable = "PEEP",IntIdsDrop = 3244)
A <- DQFunctions$SQLQuery()

DQFunctions$NonNumericFinder(A,"valueNumber")
DQFunctions$QuantileValueNumber(A)
VitalSigns[[DQFunctions$Var]] <- A


# Mechanical ventilation
DQFunctions$IdSearch(Variable = "Ventilation")
# Restrict to ventilation mode/vent mode
A <- DQFunctions$SQLQuery(AttIds = c(26466,26464))

# AttId 26464 == Mechanical ventilation part 2
A1 <- A[A$attributeId==26464,]
A <- A[!A$attributeId==26464,]

A$valueNumber[A$terseForm%in%c("APRV","BIPAP.","CPAP","CPAP + Pressure Support","Duo-PAP","HFOV","PC-SIMV")] <- "Mechanical"
A$valueNumber[A$terseForm=="Self Vent"] <- "Not Received"
A$valueNumber[is.na(A$valueNumber)] <- "Not Received"
count(A$valueNumber)
A <- A[,c("encounterId","terseForm","valueNumber","chartTime")]
MechVent1 <- A

A <- A1
A <- A[!is.na(A$terseForm),]
A <- A[!A$terseForm%in%c("NC","Mode APNEA VENTILATION",)]

# Find the ventilation data with more than 100 uses
A1 <- as.data.frame(count(as.character(A$terseForm)))
A1 <- A1[A1$freq>=100,]
A <- subset.data.frame(A,A$terseForm %in% A1$x)

# Sorting the ventilation data into Mechanical, Non-Mechanical, Self Ventilating, and Apnea
MechanicalVentilationTypes <- c("Mode SIMV/ASB/AutoFlow","Mode VC-SIMV +PS","Mode VC-SIMV +PS +AF","Mode SIMV/AutoFlow",
                                "PC-SIMV","Mode PC-SIMV+PS","Mode PC-SIMV +PS","SIMV/ASB","VC-SIMV","SIMV","VC SIMV","Mode BIPAP/ASB",
                                "Mode PC-BIPAP +PS","Mode BIPAP","Mode CPAP/ASB","CPAP/ASB","CPAP ASB","CPAP/PS",
                                "Mode APRV","Mode SPN-CPAP +PS","Mode CPAP","Mode SPN-CPAP","CPAP","SPN-CPAP","face mask")
SelfVent <- c("Mode BIPAP standby","Mode BIPAP/ASB standby","Mode PC-BIPAP standby +PS","TM","Self Vent","self ventilation",
              "Mode SIMV/ASB/AutoFlow standby","Mode CPAP standby","Mode O2 Therapy","Mode SPN-CPAP standby +PS",
              "Mode CPAP/ASB standby","Ventilator STANDBY")

A$valueNumber[A$terseForm %in% MechanicalVentilationTypes] <- "Mechanical"
A$valueNumber[A$terseForm %in% SelfVent] <- "Self Vent"
A$valueNumber[is.na(A$valueNumber)] <- "Self Vent"
A <- A[,c("encounterId","terseForm","valueNumber","chartTime")]
MechVent2 <- A
rm(MechanicalVentilationTypes,SelfVent,A1)

# Combining Ventilation Mode and Vent Mode data
# Then default to Ventilation Mode then Vent mode
A <- subset.data.frame(MechVent1, select = -terseForm)
B <- subset.data.frame(MechVent2, select = -terseForm)
colnames(A) <- c("encounterId","VentMode1","chartTime")
colnames(B) <- c("encounterId","VentMode2","chartTime")
C <- merge(A,B,by=c("encounterId","chartTime"),sort = TRUE,all = TRUE)

C$MechVentCombos <- C$VentMode2
C$MechVentCombos[is.na(C$MechVentCombos)] <- C$VentMode1[is.na(C$MechVentCombos)]
C <- subset.data.frame(C, select=c(encounterId,MechVentCombos,chartTime))
VitalSigns[["Mechanical Ventilation"]] <- C
rm(A,B,C,MechVent1,MechVent2)

colnames(VitalSigns[["Mechanical Ventilation"]])[2] <- "valueNumber"

# Intra-Aortic Balloon Pump -----------------------------------------------
DQFunctions$TableSearch <- "PtAssessment"

# All we need is a binary "IABP was there"
DQFunctions$IdSearch(Variable = "IABP")
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"valueNumber")
A$valueNumber <- 1
A <- subset.data.frame(A, select=c("encounterId","chartTime","valueNumber"))
VitalSigns[[DQFunctions$Var]] <- A
A1 <- A[,1:2]
unique(A1)

# Haemofiltration ---------------------------------------------------------

DQFunctions$TableSearch <- "PtSiteCare"

DQFunctions$IdSearch(Variable = "Filtration Out: Hourly Fluid")
A <- DQFunctions$SQLQuery(AttIds = 10258)
DQFunctions$NonNumericFinder(A,"valueNumber")
# We only want a binary of whether a patient received filtration
A$valueNumber <- 1
VitalSigns[["Haemofiltration"]] <- A

save(list = "VitalSigns",file = "CICU Raw Data/Vital Signs.RData")
rm(VitalSigns);gc()

# Drug Infusions: Preliminary ---------------------------------------------

# We want drug dose/weight 
# Unfortunately some only have dose
# So need to get patient weight

DQFunctions$TableSearch <- "PtDemographic"

Demographics <- list()

DQFunctions$IdSearch(Variable = "Weight \\(A|Weight \\(a")
# 200% of patients: Two variables
# Want them both
A <- DQFunctions$SQLQuery()

DQFunctions$NonNumericFinder(A,"valueNumber")
DQFunctions$QuantileValueNumber(A)
Demographics[["Weight"]] <- A



DQFunctions$TableSearch <- "PtMedication"

A <- dbGetQuery(conn,statement = sqlInterpolate(ANSI(),"
 SELECT DISTINCT encounterId, MIN(valueNumber) as [MinWeight], MAX(valueNumber) as [MaxWeight] FROM (
 SELECT a.encounterId, a.interventionId, a.[I-Shortlabel], a.attributeId, a.[A-Shortlabel], a.charttime, a.valueNumber, a.unitOfMeasure FROM (
 SELECT p.interventionId, i.shortlabel AS [I-Shortlabel], i.conceptLabel, p.attributeId, a.shortLabel AS [A-Shortlabel], p.encounterId, p.chartTime, p.terseForm, p.verboseForm, p.valueNumber, p.unitOfMeasure, p.baseValueNumber, p.baseUOM
 FROM PtMedication p
 JOIN D_Intervention i ON p.interventionId = i.interventionId
 JOIN D_Attribute a ON p.attributeId = a.attributeId
 WHERE p.unitOfMeasure IN ('kg')
 AND clinicalunitId = '8'
 )a
 GROUP BY a.encounterId, a.interventionId, a.[I-Shortlabel], a.attributeId, a.[A-Shortlabel], a.charttime, a.valueNumber, a.unitOfMeasure
 --ORDER BY a.encounterId, a.charttime
 )b
 GROUP BY encounterId
 "))
`PA Weight (Alt)`
# # Use this to find the 16 patients who we have a variable weight for. Currently just using minimum.
# PA Weight (Final) should be used rather than PA Weight or PA Weight (Alt)

B <- Demographics$Weight

A$Check <- (A$MaxWeight - A$MinWeight);
A <- A[A$Check!=0,];
A1 <- data.frame("encounterId" = setdiff(A$encounterId,B$encounterId))
A2 <- join(CICUBasic$PtLoS,A1,by="encounterId",type="inner");
A3 <- join(A,A1,by="encounterId",type="inner");
A4 <- join(CICUBasic$PtLoS,A,by="encounterId",type="inner");
A5 <- A3[,1:2];
colnames(A5) = c("encounterId","valueNumber");
A5$`I-Shortlabel` = "Weight (Admission)";
A5$`A-Shortlabel` = "Weight";
A5$verboseForm = 0;
A6 <- join(CICUBasic$PtLoS,A5,by="encounterId",type="inner");
A7 <- A6[,c("encounterId","Start_date","valueNumber","I-Shortlabel","A-Shortlabel","verboseForm")];
A7$terseForm = 0
A7$unitOfMeasure = "kg"
A7$baseValueNumber <- A7$valueNumber
A7$baseUOM <- A7$unitOfMeasure
A7$valueString <- NA

dput(colnames(B))
colnames(A7)

A7 <- A7[,c("I-Shortlabel", "A-Shortlabel","encounterId", "Start_date","terseForm","verboseForm","valueNumber",
            "unitOfMeasure", "baseValueNumber","baseUOM","valueString")]
colnames(A7) = c("interventionId", "attributeId", "encounterId", "chartTime", 
                 "terseForm", "verboseForm", "valueNumber", "unitOfMeasure", "baseValueNumber", 
                 "baseUOM", "valueString")
B$verboseForm <- as.character(B$verboseForm)
B$unitOfMeasure <- as.character(B$unitOfMeasure)
B$baseUOM <- as.character(B$baseUOM)

A8 <- rbind(B,A7);
Demographics[["Weight (Final)"]] <- A8
remove(A,A1,A2,A3,A4,A5,A6,A7,A8,B)
Demographics$Weight <- NULL

A <- Demographics$`Weight (Final)`
A <- A[!is.na(A$unitOfMeasure),]
Demographics$`Weight (Final)` <- A
rm(A)


# Drug Infusions ----------------------------------------------------------
require(plyr)
Medication <- list()

DQFunctions$TableSearch <- "PtMedication"


# Drug infusions are a wreck.
# Example:
DQFunctions$IdSearch(Variable = "Labetalol")
# interventionIds sometimes have values mixed in with them:
count(DQFunctions$IdSearchOutput$IntIdsDetail)

# attributeIds are massively over-duplicated
count(DQFunctions$IdSearchOutput$AttIdsDetail$attributeShortLabel)

# Clinical lead: Look in dose/weight
#   for those with less than 90% of patients with dose/weight: use dose
#   If still no data: use ((volume adm * conc) / patient weight)
#   (note: volume adm = volume administered)

# Pull the attributes with the following shortLabels
AShortLabels <- c("Dose/Weight","Dose","Volume Adm","Conc")

x <- subset(DQFunctions$IdSearchOutput$AttIdsDetail,
            attributeShortLabel%in%AShortLabels)
A <- DQFunctions$SQLQuery(AttIds = c(x$attributeId))

# Overwrite the duplicates with the first of its kind
x1 <- AShortLabels[1]
count(A$attributeId)
for(x1 in AShortLabels){
  # Set original labels aside
  A$originalAttributes <- A$attributeId
  # Pull all the attributeIds that have the shortLabel
  x <- as.numeric(unique(subset(CICUBasic$UsedIdsShort,attributeShortLabel==x1)$attributeId))
  x <- x[!is.na(x)]
  # Overwrite all the different attributeIds for that shortlabel with the first
  A$attributeId[A$attributeId%in%x] <- x[1]
}
count(A$attributeId)
count(A$originalAttributes)

# Number of patients for each Id
x <- 1
for(x in 1:length(unique(A$attributeId))){
  x1 <- unique(A$attributeId)[x]
  x2 <- CICUBasic$UsedIdsShort
  x2 <- x2[x2$attributeId==x1 & !is.na(x2$attributeId),]
  x2 <- unique(x2$attributeShortLabel)
  writeLines(paste0(length(unique(A$encounterId[A$attributeId==x1]))," patients with values for ",x1," ",x2))
  rm(x,x1,x2)
}

# No dose/weight, but we do have dose, so use that
A <- A[A$attributeId==13521,]
Medication[[DQFunctions$Var]] <- A



# I've done the above for the rest of the drug infusions
# The following three lists show which have dose/weight, dose, and ((volume adm * conc) / patient weight)
# Want to use different data for different drug infusions
AShortLabels <- c("Dose/Weight","Dose","Volume Adm","Conc")
DrugInfusionsDoseWeight <- c("Adrenaline", "Dexmedetomidine", "Dobutamine", "Dopamine", "Enoximone",
                             "Levosimendan", "Milrinone", "Noradrenaline","Sodium nitroprusside")
DrugInfusionsDose <- c("Glyceryl Trinitrate","Labetalol","Propofol","Vasopressin")
DrugInfusionsCalculate <- "Clonidine"

Variable <- DrugInfusionsDoseWeight[1]
Term = "Dose/Weight"
MedicationFullIds <- list()

DrugInfusions <- function(Variable,Term){
  
  # Ensure correct arguments
  if(!Term%in%c("Dose","Dose/Weight","Calculate"))stop("Please supply either \"Dose\", \"Dose/Weight\" or \"Calculate\" to the \"Term\" argument")
  
  # Search for the variable
  
  if(Variable%in%c("Adrenaline","Noradrenaline")){
    DQFunctions$IdSearch(Variable = Variable,DontPermuteVarSearch = T)
  }else{DQFunctions$IdSearch(Variable = Variable)}
  
  A1 <- DQFunctions$IdSearchOutput$IntIdsDetail$interventionId
  A2 <- DQFunctions$IdSearchOutput$AttIdsDetail$attributeId
  A3 <- CICUBasic$UsedIds[CICUBasic$UsedIds$interventionId %in% A1 & 
                          CICUBasic$UsedIds$attributeId %in% A2,]
  A3 <- A3[,c("interventionId","attributeId")]
  A3 <- unique(A3)
  MedicationFullIds[[paste0(Variable,"AllIds")]] <<- A3
  rm(A1,A2,A3)
  
  # Calculate needs volume admitted and concentration attributes
  # Dose and dose/weight just need those terms in attribute shortlabels
  if(Term=="Calculate"){
    DQFunctions$IdSearchOutput$AttIdsDetail <<- 
      subset(DQFunctions$IdSearchOutput$AttIdsDetail,attributeShortLabel %in% c("Volume Adm","Conc"))
  }else{DQFunctions$IdSearchOutput$AttIdsDetail <<- 
      subset(DQFunctions$IdSearchOutput$AttIdsDetail,attributeShortLabel %like% Term)}
  
  # Pull the data
  A <- DQFunctions$SQLQuery()
  
  # Save Ids
  MedicationFullIds[[paste0(Variable,gsub(Term,pattern = "/",replacement = ""))]] <<- unique(A[,c("interventionId","attributeId")])
  
  # Overwrite the duplicates with the first of its kind
  x1 <- AShortLabels[1]
  for(x1 in AShortLabels){
    # Pull all the attributeIds that have the shortLabel
    x <- as.numeric(unique(
      CICUBasic$UsedIdsShort[CICUBasic$UsedIdsShort$attributeShortLabel==x1,]$attributeId))
    x <- x[!is.na(x)]
    # Overwrite all the different attributeIds for that shortlabel with the first
    A$attributeId[A$attributeId%in%x] <- x[1]
  }
  return(A)
}

for(x in DrugInfusionsDoseWeight){
  Medication[[x]] <- DrugInfusions(Variable = x,Term = "Dose/Weight")}

for(x in DrugInfusionsDose){
  Medication[[x]] <- DrugInfusions(Variable = x,Term = "Dose")}

for(x in DrugInfusionsCalculate){
  Medication[[x]] <- DrugInfusions(Variable = x,Term = "Calculate")
}

save(MedicationFullIds,
     file = "C://Users//ShillanDun//OneDrive - University of Bristol//NHS Research Associate//R Scripts//ICU Data Extraction and Cleaning//MedicationFullIds.RData")

for(x in names(Medication)){
  writeLines(paste0(x,": ",
                    sum(count(unique(Medication[[x]]$encounterId))$freq),
                    " patients"))
  rm(x)
}

# Calculate: VolAdm*Conc/Weight
VolAdmConcWeight <- function(Drug,VolAdmAttIds,ConAttIds){
  
  A <- Medication[[Drug]]
  # Vol adm
  A1 <- A[A$attributeId %in% VolAdmAttIds,]
  # Conc
  A2 <- A[A$attributeId %in% ConAttIds,]
  # Weight
  A3 <- Demographics$`Weight (Final)`
  A3 <- A3[!A3$valueNumber==0,]
  A3 <- A3[!duplicated(A3$encounterId),]
  
  # Subset to encounterId, chartTime, valueNumber, and re-name
  A1 <- data.frame("encounterId" = A1$encounterId,"chartTime" = A1$chartTime,
                   "VolAdm" = A1$valueNumber)
  A2 <- data.frame("encounterId" = A2$encounterId,"chartTime" = A2$chartTime,
                   "Conc" = A2$valueNumber)
  A3 <- data.frame("encounterId" = A3$encounterId,
                   "Weight" = A3$valueNumber)
  
  A4 <- join(A1,A2,by=c("encounterId","chartTime"))
  A4 <- join(A4,A3,by="encounterId")
  rm(A1,A2,A3)
  
  # Remove NAs and any time that volume = 0 
  A4 <- A4[!is.na(A4$Conc),];A4 <- A4[!is.na(A4$Weight),];A4 <- A4[!A4$VolAdm==0,]
  
  A4$Calc <- (A4$VolAdm*A4$Conc)/A4$Weight
  
  A4 <- data.frame("encounterId" = A4$encounterId,"chartTime" = A4$chartTime,"valueNumber" = A4$Calc)
  
  return(A4)
}

# Running each drug that requires calculation through the function
for(x in DrugInfusionsCalculate){
  # Find the volume admitted and concentration attributeIds
  A <- subset(CICUBasic$UsedIdsShort,attributeId %in% Medication[[x]]$attributeId)
  
  Medication[[x]] <- VolAdmConcWeight(Drug = DrugInfusionsCalculate,
                        VolAdmAttIds = A$attributeId[A$attributeShortLabel=="Volume Adm"],
                        ConAttIds = A$attributeId[A$attributeShortLabel== "Conc"])
}

for(x in names(Medication)){
  writeLines(text = paste0(x,": ",length(unique(Medication[[x]]$encounterId))," patients\n"))
  rm(x)}


# Altering Vasopressin to only use units/min
A <- Medication$Vasopressin
count(A$unitOfMeasure)
count(A$baseUOM)

A$valueNumber <- A$baseValueNumber
A$terseForm <- A$baseValueNumber
A$verboseForm <- A$baseValueNumber
A$unitOfMeasure <- A$baseUOM

Medication$Vasopressin <- A

# Glyceryl trinitrate, propofol and labetalol have mg/hr and mg
# Subset to mg/hr
for(x in DrugInfusionsDose[1:3]){
  Medication[[x]] <- subset(Medication[[x]],unitOfMeasure == "mg/hr")
  rm(x)}


rm(DrugInfusions,DrugInfusionsCalculate,DrugInfusionsDose,DrugInfusionsDoseWeight,
   AShortLabels,VolAdmConcWeight)

Medication[["Clonidine"]]$unitOfMeasure <- "mcg/kg/hr"

save(list = "Medication",file = "CICU Raw Data/Medication.RData")
rm(Medication)
rm(x1,x2)

# Output ------------------------------------------------------------------
Output <- list()

DQFunctions$TableSearch <- "PtSiteCare"

DQFunctions$IdSearch(Variable = "Indwelling")
DQFunctions$IdSearchOutput$AttIdsDetail <- 
  subset(DQFunctions$IdSearchOutput$AttIdsDetail,attributeShortLabel %like% "Volume|volume")
A <- DQFunctions$SQLQuery()
DQFunctions$NonNumericFinder(A,"valueNumber")
DQFunctions$QuantileValueNumber(A)
Output[[DQFunctions$Var]] <- A

DQFunctions$IdSearch(Variable = "Chest: Mediastinal")
DQFunctions$IdSearchOutput$AttIdsDetail <- 
  subset(DQFunctions$IdSearchOutput$AttIdsDetail,attributeShortLabel %like% "Volume|volume")
A <- DQFunctions$SQLQuery()
# narrow down to just chest: mediastinal
A <- A[A$interventionId==5803,]
DQFunctions$NonNumericFinder(A,"valueNumber")
DQFunctions$QuantileValueNumber(A)
Output[[DQFunctions$Var]] <- A


DQFunctions$TableSearch <- "PtTotalBalance"
# Can't use the regular DQFunctions here: This table does not use attributeIds
DQFunctions$IdSearch(Variable = "Net Body Balance \\(L")
writeLines("The above error is fine:\n   DataFind errors due to a lack of interventionIds in the Total Balance table.\n   We do extraction here manually")

A <- CICUBasic$UsedIds
A <- A[A$interventionShortLabel %like% "Net Body Balance \\(L",]
A$interventionId

A <- dbGetQuery(
  conn,statement = sqlInterpolate(
    ANSI(),
    "SELECT encounterId,interventionId,chartTime,
    hourTotal,cumTotal,unitOfMeasure,
    baseHourTotal, baseCumTotal,baseUOM
    FROM PtTotalBalance
    WHERE (interventionId IN (?IntIds))
    AND clinicalUnitId = '8'",
    IntIds = SQL(paste0("'",A$interventionId,"'",collapse = ","))))

Output[["Net Body Balance"]] <- A
# We use cumulative total as the valueNumber
colnames(Output$`Net Body Balance`)[colnames(Output$`Net Body Balance`)=="cumTotal"] <- "valueNumber"

save(list="Output",file = "CICU Raw Data/Output.RData")
rm(Output);gc()

# Surgical Procedures -----------------------------------------------------

Assessment <- list()
DQFunctions$TableSearch <- "PtAssessment"

DQFunctions$IdSearch(Variable = "Operation \\(OPCS Code\\)")

A <- DQFunctions$SQLQuery()
require(plyr)
count(A$terseForm)

# remove free-text notes
A <- subset.data.frame(A, !A$terseForm == "Other (See Free-Text Note)")

# Divide up into Ben's groupings
# Then the number of procedures per patient
A$SurgeryType[A$terseForm %in% c(
  "(K10.1) Repair ASD with Prosthet","(K10.2) Repair ASD with Pericard",
  "(K10.3)RepairASDWithTissueGraft","(K10.4) Primary Repair ASD",
  "(K11.1)RepairVSDWithTissuePatch","(K11.2)RepairVSDPericardialPatch",
  "(K11.1)RepairVSDProstheticPatch","(K11.4) Primary Repair VSD")] <- "Structural Defect Repair"

A$SurgeryType[A$terseForm %in% c(
  "(K25.1) MVR Allograft","(K25.2) MVR Bioprosthetic","(K25.5) MV Repair",
  "(K26.1) AV Repair","(K26.1) AVR Allograft","(K26.2) AVR Bioprosthetic",
  "(K26.3) AVR Mechanical","(K26.4,K19.3,Y01.1)RossProcedure","(K27.2) TVR Bioprosthetic",
  "(K27.6) TV Repair","(K28.1) PVR Allograft","(K28.2) PVR Bioprosthetic","(K28.3) PVR Mechanical",
  "(K28.5) PVR Repair","(K32.1) Closed Mitral Valvotomy","(K34.1)AnnuloplastyMitralValve",
  "(K34.2)AnnuloplastyTricuspidValv","(K52.3) MVR Mechanical")] <- "Valve Surgery"

# x1 = 1 cabg, x2 = 2 cabg
# Change count of CABGs to be according to x1,x2 etc, rather than number of 3s
A$SurgeryType[A$terseForm == "(K40.1) CABG - SVGx1"] <- "CABG";
A$SurgeryType[A$terseForm == "(K40.2) CABG - SVGx2"] <- paste0(rep("CABG",2),collapse = ", ")
A$SurgeryType[A$terseForm == "(K40.3) CABG - SVGx3"] <- paste0(rep("CABG",3),collapse = ", ")
A$SurgeryType[A$terseForm == "(K40.4) CABG - SVGx4 or more"] <- paste0(rep("CABG",4),collapse = ", ")
A$SurgeryType[A$terseForm == "(K41.1) CABG - Rad Artx1"] <- "CABG";
A$SurgeryType[A$terseForm == "(K41.2) CABG - Rad Artx2"] <- paste0(rep("CABG",2),collapse = ", ")
A$SurgeryType[A$terseForm == "(K45.1) BIMA"] <- "CABG";
A$SurgeryType[A$terseForm == "(K45.3) LIMA to LAD"] <- "CABG";
A$SurgeryType[A$terseForm == "(K45.3)MammaryArteryCoronaryArte"] <- "CABG";

A$SurgeryType[A$terseForm %in% c(
  "(K23.3)BiopsyLesionWallHeart","(K67.1)ExcisionLesionPericardium",
  "(K68.1)DecompresCardiacTamponade","(K68.8)OtherPericardiumDrainage",
  "(K68.9) UnspecPericardDrainage","(K69.1)FreeingPericardAdhesions",
  "(K69.2) Fenestration of Pericard","(K71.1)BiopsyLesionsPericardium",
  "(K71.4)ExplorationPericardium")] <- "Other"

A$SurgeryType[A$terseForm %in% c(
  "(L18.1)EmergAscAorticAneurysm","(L19.1)ElectAscAorticAneurysm",
  "(L19.9)OtherRepAneurysmSegment")] <- "Replacement of Part of the Aorta"


#remove duplicate encounterIds but keep them when the terseForm changes
library("dplyr")
A <- A %>% distinct(encounterId, terseForm, .keep_all = TRUE)
detach("package:dplyr", unload=TRUE)

A <- join(A, count(A,"encounterId"), type = "right", match="all", by = "encounterId")
colnames(A)[colnames(A)=="freq"] <- "NumberOfSurgeries"

# Descriptor column: concatenation of all received surgeries.
A1 <- aggregate(SurgeryType ~ encounterId, data = A, toString)
colnames(A1) <- c("encounterId","TotalSurgeries")

A <- join(A,A1,by="encounterId",type="left",match = "first")
A <- subset.data.frame(A, !duplicated(A$encounterId))
A <- subset.data.frame(A, select = c(encounterId,NumberOfSurgeries,TotalSurgeries))
rm(A1)

# Changing surgerytype to have one variable per number of each surgery 
# Find the number of each type of surgery each patient had
TypesOfSurgery <- c("CABG","Valve Surgery","Structural Defect Repair","Replacement of Part of the Aorta","Other")
TypesOfSurgeryNoSpace <- gsub(TypesOfSurgery,pattern=" ", replacement = "")
SurgerySequences <- paste0(TypesOfSurgeryNoSpace,"Seq")

# Count the number of each type of surgery in A$TotalSurgeries per patient.
for(x1 in 1:length(TypesOfSurgery)){
  A[[paste0(TypesOfSurgeryNoSpace[x1],"Number")]] <- 
    sapply(gregexpr(TypesOfSurgery[x1],A$TotalSurgeries),
           function(x) sum(x != -1))
  rm(x1)
}

# Any CABG above 4 recoded to 4
writeLines(text=paste0(nrow(A[A$CABGNumber>4,])," patients with CABG>4"))
A$CABGNumber[A$CABGNumber>4] <- 4

# Valve surgery > 2 = 2
paste0(nrow(A[A$ValveSurgeryNumber>2,])," patients with Valve>2")
A$ValveSurgeryNumber[A$ValveSurgeryNumber>2] <- 2

# Structural Defect Repair > 1 = 1
paste0(nrow(A[A$StructuralDefectRepairNumber>1,])," patients with StructuralDefect>1")
A$StructuralDefectRepairNumber[A$StructuralDefectRepairNumber>1] <- 1

# Replacement of Part of the Aorta > 1 = 1
paste0(nrow(A[A$ReplacementofPartoftheAortaNumber>1,])," patients with ReplacementofPartoftheAortaNumber>1")
A$ReplacementofPartoftheAortaNumber[A$ReplacementofPartoftheAortaNumber>1] <- 1

# Other > 1 = 1
paste0(nrow(A[A$OtherNumber>1,])," patients with OtherNumber>1")
A$OtherNumber[A$OtherNumber>1] <- 1



Seq <- list()
for(x1 in 1:length(TypesOfSurgery)){
  Seq[[TypesOfSurgeryNoSpace[x1]]] <- 
    min(A[[paste0(TypesOfSurgeryNoSpace[x1],"Number")]],na.rm=T):
    max(A[[paste0(TypesOfSurgeryNoSpace[x1],"Number")]],na.rm=T)
  rm(x1)
}

# Makes a binary indicator showing which surgery was recieved
# So two CABGs would be "CABGx2 = 1/0"
for(x1 in 1:length(TypesOfSurgery)){
  for(x2 in 1:length(Seq[[x1]])){
    # The current surgery type when it is equal to the current number in the current sequence
    A[[paste0(TypesOfSurgeryNoSpace[x1],"x",Seq[[x1]][x2])]][
      A[[paste0(TypesOfSurgeryNoSpace[x1],"Number")]]==Seq[[x1]][x2]] <- 1
    # When it is NA, replace with 0
    A[[paste0(TypesOfSurgeryNoSpace[x1],"x",Seq[[x1]][x2])]][is.na(A[[paste0(TypesOfSurgeryNoSpace[x1],"x",Seq[[x1]][x2])]])] <- 0
  }
}

A1 <- subset.data.frame(A, select = c("encounterId","TotalSurgeries",
                                      "CABGNumber","ValveSurgeryNumber",
                                      "StructuralDefectRepairNumber",
                                      "ReplacementofPartoftheAortaNumber",
                                      "OtherNumber"))
A1 <- join(subset.data.frame(CICUBasic$PtLoS, select = encounterId),A1,
           by="encounterId",type="left")
count(A1$TotalSurgeries)


MissingSurgeryData <- A1$encounterId[is.na(A1$TotalSurgeries)]

# Re-do TotalSurgeries to be CABGx2, Valve Repairx1 etc.
A1$TotalSurgeriesCABG <- paste0("CABG x",A1$CABGNumber)
A1$TotalSurgeriesValve <- paste0("ValveSurgery x",A1$ValveSurgeryNumber)
A1$TotalSurgeriesStruc <- paste0("StructuralDefectRepair x",A1$StructuralDefectRepairNumber)
A1$TotalSurgeriesReplace <- paste0("Replacement of Part of the Aorta x",A1$ReplacementofPartoftheAortaNumber)
A1$TotalSurgeriesOther <- paste0("Other x",A1$OtherNumber)

A1$TotalSurgeriesCABG[A1$TotalSurgeriesCABG %like% "x0"] <- ""
A1$TotalSurgeriesValve[A1$TotalSurgeriesValve %like% "x0"] <- ""
A1$TotalSurgeriesStruc[A1$TotalSurgeriesStruc %like% "x0"] <- ""
A1$TotalSurgeriesReplace[A1$TotalSurgeriesReplace %like% "x0"] <- ""
A1$TotalSurgeriesOther[A1$TotalSurgeriesOther %like% "x0"] <- ""

A1$TotalSurgeries <- paste0(A1$TotalSurgeriesCABG,A1$TotalSurgeriesValve,A1$TotalSurgeriesStruc,
                            A1$TotalSurgeriesReplace,A1$TotalSurgeriesOther)

A1$TotalSurgeries <- gsub(A1$TotalSurgeries,pattern = "x1",replacement = "x1 ")
A1$TotalSurgeries <- gsub(A1$TotalSurgeries,pattern = "x2",replacement = "x2 ")
A1$TotalSurgeries <- gsub(A1$TotalSurgeries,pattern = "x3",replacement = "x3 ")
A1$TotalSurgeries <- gsub(A1$TotalSurgeries,pattern = "x4",replacement = "x4 ")

count(A1$TotalSurgeries)

# Remove temp columns
A1 <- subset.data.frame(A1,select=-c(TotalSurgeriesCABG,TotalSurgeriesValve,TotalSurgeriesStruc,
                                     TotalSurgeriesReplace,TotalSurgeriesOther))
# Redo A1$Numberofsurgeries
A1$NumberOfSurgeries <- A1$CABGNumber+A1$ValveSurgeryNumber+A1$StructuralDefectRepairNumber+A1$ReplacementofPartoftheAortaNumber+A1$OtherNumber
A1$NumberOfSurgeries[A1$NumberOfSurgeries>4] <- 4

Assessment$SurgeryType <- A1

save(list="MissingSurgeryData",file = "CICU Raw Data/MissingSurgeryData.RData")
rm(A,A1,TypesOfSurgery,TypesOfSurgeryNoSpace,SurgerySequences,Seq,x1,x2,MissingSurgeryData)

# Left Ventricular Ejection Fraction --------------------------------------

DQFunctions$TableSearch <- "PtAssessment"
DQFunctions$IdSearch(Variable = "Pre-op LV")
A <- DQFunctions$SQLQuery()

# Sort out various columns, remove NAs, and ensure factor is grouped properly
A$valueNumber <- as.character(A$terseForm)
A$baseValueNumber <- as.character(A$terseForm)
A$unitOfMeasure = "N/A"
A <- subset.data.frame(A, !is.na(A$valueNumber))
count(A$valueNumber)

A$valueNumber[A$valueNumber%like%"Normal|Good"] <- "Good (>50%)"
A$valueNumber[A$valueNumber%like%"Mildly|Moderately"] <- "Moderately Impaired (30-50%)"
A$valueNumber[A$valueNumber%like%"Severely Impaired"] <- "Severely Impaired (<30%)"
count(A$valueNumber)

Assessment[["LVEF"]] <- A
A <- Assessment[["LVEF"]]

# Using LoSS join with D1 Extraction. LVEF only has pre-op, so just find latest LVEF.
A <- subset.data.frame(A, select = -c(terseForm,baseValueNumber,verboseForm))
A <- join(CICUBasic$PtLoS, A, by = "encounterId", type = "inner", match = "all");

# extract data from first day and closest to 8am
# Number of midnights since admittance to ward
# DIW = Days In Ward (number of midnights that have passed)
A$DIW <- as.integer(difftime(as.Date(A$chartTime),
                             as.Date(A$Start_date), units = "days"));
# the hour of the day the record was made.
A$DIW2 <- as.integer(strftime(A$chartTime, format="%H"));

#Create difference between chartTime and Start_date to find the hours since they were admitted that the event happened
A$TDiff <- difftime(A$chartTime, A$Start_date, units = "hour");

#Generate a new LoS (LoSR) for remaining LoS from the event
A$LoSR <- as.numeric(A$LoS - A$TDiff);

# Narrow to within 5 days of surgery
B <- subset.data.frame(A, A$DIW<=5)
B <- subset.data.frame(B, B$DIW>= -5)
# Order by encounterId and TDiff so that you can remove duplicates to get nearest data to Op
B <- B[order(B$encounterId, B$TDiff, decreasing = TRUE),];
B <- B[!duplicated(B$encounterId),]
B <- subset.data.frame(B, select=c(encounterId,valueNumber,chartTime))
colnames(B) = c("encounterId","LVEF Pre-Op","chartTime")

# Join on to LoSS
B <- join(x = data.frame(encounterId = CICUBasic$PtLoS$encounterId),y = B,by="encounterId")
count(B$`LVEF Pre-Op`)
Assessment$`LVEF Pre-Op` <- B
Assessment[["LVEF"]] <- NULL
rm(A,B)

save(list = "Assessment",file = "CICU Raw Data/Assessment.RData")
save(list = "Demographics",file = "CICU Raw Data/Demographics.RData")

rm(Assessment)
rm(Demographics)

# Generic Script ----------------------------------------------------------

# DQFunctions$IdSearch(Variable = "")
# A <- DQFunctions$SQLQuery()
# 
# DQFunctions$NonNumericFinder(A,"terseForm")
# # A <- DQFunctions$ValueCleaner(A,"terseForm",symbolClean = 1)
# # A <- DQFunctions$ValueCleaner(A,"terseForm",rmNonNumeric = )
# A$valueNumber <- as.numeric(A$terseForm)
# 
# DQFunctions$NonNumericFinder(A,"valueNumber")
# # A <- DQFunctions$ValueCleaner(A,"valueNumber",symbolClean = 1)
# # A <- DQFunctions$ValueCleaner(A,"valueNumber",rmNonNumeric = 1)
# 
# DQFunctions$QuantileValueNumber(A)
# # A <- DQFunctions$BloodGasArterial(A)
# # A$unitOfMeasure <- ""
# ENSURE THE LIST YOU WRITE TO IS CORRECT
# Output[[DQFunctions$Var]] <- A

