# This script uses the patient data processing toolkit to retrieve data from the
# back-end of the philips ICIP system located in the BRI's CICU 

# Lab Tests ---------------------------------------------------------------

# List to fill with each variable's dataframe
LabTests <- list()
# Set the table you are pulling data from
TableSearch <- "LabResult"

# Coagulation -------------------------------------------------------------

# Set the variable you are searching for
Var <- "APTT"

# Search the table for the variable
IdSearch(TableSearch = TableSearch,Variable = Var)

# Make any edits to the intervention or attribute Ids that you wish
# When you're happy with your terms, run the query.
LabTests[[Var]] <- SqlMutate(Table = TableSearch)

# Once the query is run, find where the data is
DataFind(LabTests[[Var]]) # terseForm holds 99.99% of data

A <- LabTests[[Var]]
A[is.na(A$terseForm),]
# the only row where terseForm is NA is one where valueString is empty
# Drop this
A <- A[!is.na(A$terseForm),]

# Find any non-numeric data
NonNumericFinder(A,"terseForm")

# Delete erroneous entries
A <- subset.data.frame(A, !terseForm %in% c(" W4","`","+++vfv","t","r","ERROR","5po "))
# Remove symbols
A$terseForm <- gsub(A$terseForm,pattern = "<|>| ",replacement = "")

# Re-check for non-numeric data
NonNumericFinder(A,"terseForm") # none left

# Ensure data is stored as numeric
A$terseForm <- as.numeric(A$terseForm)

# This dataframe has three different Id combos. Check that they're all roughly similar
# If they're not, consider splitting this into two data extractions for different data
# Also consider transforming your data. Looking at unit of measure will help determine if
#   a transformation is necessary
QuantileColumn(A,"terseForm") # similar enough to warrant being from the same distribution
write.table(QuantileColumnOut,"C:/Users/shillandun/Downloads/QuantileColumnOut.csv", sep=",", row.names = FALSE)
# Check the unitOfMeasure from QuantileColumn, ensure it is consistent and appropriate
# Here all our data appears to be from the same distribution despite the four Id combinations
# Therefore ensure unitOfMeasure is also consistent
A$unitOfMeasure <- "s"

# Move data to the valueNumber column for later extraction
A$valueNumber <- as.numeric(A$terseForm);

# Save from temporary data frame to the LabTests list
LabTests[[Var]] <- A

Var <- "Prothrombin"
IdSearch(TableSearch = TableSearch,Variable = Var)
LabTests[[Var]] <- SqlMutate(Table = TableSearch)
DataFind(LabTests[[Var]])
A <- LabTests[[Var]]
NonNumericFinder(A,"valueNumber")
QuantileColumn(dataframe = A,column = "valueNumber")
# Both seem similar, its a case of duplicated Ids
LabTests[[Var]] <- A

# Biochemistry ------------------------------------------------------------
Var <- "Albumin"
IdSearch(TableSearch = TableSearch,Variable = Var)
LabTests[[Var]] <- SqlMutate(Table = TableSearch)
DataFind(LabTests[[Var]])
A <- LabTests[[Var]]
NonNumericFinder(A,"valueNumber")
QuantileColumn(A,"valueNumber")
# No further action needed

Var <- "Alkaline"
IdSearch(TableSearch = TableSearch,Variable = Var)
LabTests[[Var]] <- SqlMutate(Table = TableSearch)
DataFind(LabTests[[Var]])
A <- LabTests[[Var]]
NonNumericFinder(A,"valueNumber")
QuantileColumn(A,"valueNumber")
# Similar enough to say duplicates
# No further action needed
names(LabTests[[Var]])
LabTests$`Alkaline Phosphate` <- LabTests[[Var]]
LabTests[[Var]] <- NULL

Var <- "ALT"
IdSearch(TableSearch = TableSearch,Variable = Var)
LabTests[[Var]] <- SqlMutate(Table = TableSearch)
DataFind(LabTests[[Var]])
A <- LabTests[[Var]]
NonNumericFinder(A,"valueNumber")
QuantileColumn(A,"valueNumber")
# Similar enough to say duplicates
# No further action needed
LabTests$`ALT (SGPT)` <- LabTests[[Var]]
LabTests[[Var]] <- NULL

Var <- "Bicarbonate"
IdSearch(TableSearch = TableSearch,Variable = Var)
LabTests[[Var]] <- SqlMutate(Table = TableSearch)
DataFind(LabTests[[Var]])
A <- LabTests[[Var]]
NonNumericFinder(A,"valueNumber")
QuantileColumn(A,"valueNumber")
# No further action needed


Var <- "Calcium "
IdSearch(TableSearch = TableSearch,Variable = Var)
LabTests[[Var]] <- SqlMutate(Table = TableSearch)

A <- LabTests[[Var]]
DataFind(LabTests[[Var]])
NonNumericFinder(A,"terseForm")
QuantileColumn(A,"terseForm")
# No data clensing needed, and both vars seem similar
A$unitOfMeasure <- "mmol/L"
A$valueNumber <- A$terseForm

# Rename
LabTests$`Calcium Adjusted` <- A
LabTests[[Var]] <- NULL


Var <- "C-reactive"
IdSearch(TableSearch = TableSearch,Variable = Var)
LabTests[[Var]] <- SqlMutate(Table = TableSearch)

A <- LabTests[[Var]]
DataFind(LabTests[[Var]])
NonNumericFinder(A,"terseForm")
QuantileColumn(A,"terseForm")
A <- A[!is.na(A$terseForm),]
A$terseForm <- as.character(A$terseForm)
A <- A[!A$terseForm%in%c("33 today","66 (down)","66 yesterd","80 (unchan"),]
A$terseForm <- gsub(A$terseForm,pattern = "<|>| ",replacement = "")
NonNumericFinder(A,"terseForm")
A$unitOfMeasure <- "mg/L"
LabTests$`C-reactive protein` <- A
LabTests[[Var]] <- NULL


Var <- "Chloride"
IdSearch(TableSearch = TableSearch,Variable = Var)
LabTests[[Var]] <- SqlMutate(Table = TableSearch)

A <- LabTests[[Var]]
DataFind(LabTests[[Var]]) # both have data for nearly everyone in our population
# DataFind indicates that these might be two different variables

# From talking to clinical contact:
# Cl- is a serum (blood gas) while Chloride is a biochemistry test
# They both measure the same thing but with different equipment, hence the similar values
NonNumericFinder(A,"terseForm")
QuantileColumn(A,"terseForm")

LabTests$Chloride <- A[A$interventionId==630,]
Cl <- A[A$interventionId==727,]


A <- Cl
DataFind(A)
# Blood gas results need to be restricted to arterial measurements
A <- BloodGasArterial(A)
LabTests$`Cl-` <- A
rm(Chloride,Cl)



Var <- "eGFR"
IdSearch(TableSearch = TableSearch,Variable = Var)
LabTests[[Var]] <- SqlMutate(Table = TableSearch)

A <- LabTests[[Var]]
DataFind(A)
NonNumericFinder(A,"terseForm")
A$terseForm[A$terseForm == "eCFR 29"] <- 29;
A$terseForm <- as.character(A$terseForm)
A$terseForm <- gsub(A$terseForm,pattern = "<|>| ",replacement = "")
A <- A[!A$terseForm%in%c("Not Applicable","1qp0"),]
A$unitOfMeasure = "Not Available";
A$valueNumber <- as.numeric(A$terseForm);
QuantileColumn(A,"valueNumber")
LabTests$eGFR <- A



Var <- "Magnesium"
IdSearch(TableSearch = TableSearch,Variable = Var)
LabTests[[Var]] <- SqlMutate(Table = TableSearch)

A <- LabTests[[Var]]
DataFind(A)
NonNumericFinder(A,"valueNumber")
QuantileColumn(A,"valueNumber")
# No need for adjustment etc


Var <- "Phosphate"
IdSearch(TableSearch = TableSearch,Variable = Var)
LabTests[[Var]] <- SqlMutate(Table = TableSearch)

A <- LabTests[[Var]]
DataFind(A)
NonNumericFinder(A,"valueNumber")
QuantileColumn(A,"valueNumber")
# No need for adjustment etc



Var <- "Potassium"
IdSearch(TableSearch = TableSearch,Variable = Var) # 14k patients indicates 2 separate vars
DeleteUrineIds()
LabTests[[Var]] <- SqlMutate(Table = TableSearch)

A <- LabTests[[Var]]
DataFind(A)
NonNumericFinder(A,"valueNumber")
QuantileColumn(A,"valueNumber")
# Clinical contact: Potassium is biochem, K = blood gas
K <- A[A$interventionId==725,]
Potassium <- A[A$interventionId==510,]

# Handle each separately

A <- Potassium
DataFind(A)
count(A$unitOfMeasure)
# Data already in valueNumber with correct unitOfMeasure
LabTests$Potassium <- A
rm(Potassium)

A <- K
DataFind(A)
A <- BloodGasArterial(A)
LabTests$K <- A
rm(K)

# Sodium is biochem, Na is blood gas.
Var <- "Sodium"
IdSearch(TableSearch = TableSearch,Variable = Var) # 14k patients indicates 2 separate vars
DeleteUrineIds()
LabTests[[Var]] <- SqlMutate(Table = TableSearch)

A <- LabTests[[Var]]
DataFind(A)
NonNumericFinder(A,"valueNumber")
QuantileColumn(A,"valueNumber")

Na <- A[A$interventionId==726,]
LabTests$Sodium <- A[A$interventionId==706,]
LabTests$Na <- BloodGasArterial(Na)
rm(Na)


Var <- "Bilirubin"
IdSearch(TableSearch = TableSearch,Variable = Var)
IdSearchOut$IdSearchIntIds <- IdSearchOut$IdSearchIntIds[!IdSearchOut$IdSearchIntIds==19512]
LabTests[[Var]] <- SqlMutate(Table = TableSearch)

A <- LabTests[[Var]]
DataFind(A)
NonNumericFinder(A,"terseForm")
A$terseForm <- gsub(A$terseForm,pattern = "<| ",replacement = "")
A <- A[!A$terseForm=="**",]
QuantileColumn(A,"terseForm")
A$unitOfMeasure <- "Not Available"
A$valueNumber <- A$terseForm
LabTests[[Var]] <- A



Var <- "Total protein"
IdSearch(TableSearch = TableSearch,Variable = Var)
LabTests[[Var]] <- SqlMutate(Table = TableSearch)

A <- LabTests[[Var]]
DataFind(A)
NonNumericFinder(A,"valueNumber")
QuantileColumn(A,"valueNumber")
# No further action needed


Var <- "Urea"
IdSearch(TableSearch = TableSearch,Variable = Var)
DeleteUrineIds()
LabTests[[Var]] <- SqlMutate(Table = TableSearch)

A <- LabTests[[Var]]
DataFind(A)
NonNumericFinder(A,"terseForm")
QuantileColumn(A,"terseForm")
# No further action needed

# Blood Gases -------------------------------------------------------------

Var <- "Basophils"
IdSearch(TableSearch = TableSearch,Variable = Var)
LabTests[[Var]] <- SqlMutate(Table = TableSearch)

A <- LabTests[[Var]]
DataFind(A)
NonNumericFinder(A,"terseForm")
QuantileColumn(A,"terseForm")
A$unitOfMeasure <- "10*9/L"
A$valueNumber <- A$terseForm
LabTests[[Var]] <- A

Var <- "Lymphocyte"
IdSearch(TableSearch = TableSearch,Variable = Var)
LabTests[[Var]] <- SqlMutate(Table = TableSearch)

A <- LabTests[[Var]]
DataFind(A)
NonNumericFinder(A,"terseForm")
A <- A[!A$terseForm=="1%",]
QuantileColumn(A,"terseForm")
A$unitOfMeasure <- "10*9/L"
A$valueNumber <- A$terseForm
LabTests[[Var]] <- A



# Haeamatocrit = haematology lab test, Hct = blood gas, divide hct by 100
Var <- "Haematocrit"
IdSearch(TableSearch = TableSearch,Variable = Var)
LabTests[[Var]] <- SqlMutate(Table = TableSearch)

A <- LabTests[[Var]]
DataFind(A)
NonNumericFinder(A,"valueNumber")
QuantileColumn(A,"valueNumber")

A$valueNumber[A$interventionId==724] <- A$valueNumber[A$interventionId==724]/100
LabTests$Hct <- A[A$interventionId==724,]
LabTests$Haematocrit <- A[A$interventionId==4650,]
LabTests$Hct <- BloodGasArterial(LabTests$Hct)


Var <- "Haemoglobin"
IdSearch(TableSearch = TableSearch,Variable = Var)
IdSearchOut$IdSearchIntIds <- IdSearchOut$IdSearchIntIds[!IdSearchOut$IdSearchIntIds%in%
                                                           c("4416","20306")]
A <- SqlMutate(Table = TableSearch)
DataFind(A)
NonNumericFinder(A,"terseForm")
QuantileColumn(A,"terseForm") # A1c (IFCC) seems very different
# After discussion: drop
A <- A[!A$interventionId==4350,]
LabTests$Haemoglobin <- A[A$interventionId==756,]
LabTests$tHb <- BloodGasArterial(A[A$interventionId==766,])


Var <- "MCH"
IdSearch(TableSearch = TableSearch,Variable = Var)
LabTests[[Var]] <- SqlMutate(Table = TableSearch)

A <- LabTests[[Var]]
DataFind(A)
NonNumericFinder(A,"terseForm")
QuantileColumn(A,"terseForm")

# We pulled MCH and MCHC
LabTests[[Var]] <- A[!A$interventionId==634,]
LabTests$MCHC <- A[A$interventionId==634,]




Var <- "Monocytes"
IdSearch(TableSearch = TableSearch,Variable = Var)
LabTests[[Var]] <- SqlMutate(Table = TableSearch)

A <- LabTests[[Var]]
DataFind(A)
NonNumericFinder(A,"terseForm")
QuantileColumn(A,"terseForm")
A$unitOfMeasure <- "10*9/L"
LabTests[[Var]] <- A


Var <- "Neutrophils"
IdSearch(TableSearch = TableSearch,Variable = Var)
LabTests[[Var]] <- SqlMutate(Table = TableSearch)

A <- LabTests[[Var]]
DataFind(A)
NonNumericFinder(A,"terseForm")
QuantileColumn(A,"terseForm")
A$unitOfMeasure <- "10*9/L"
LabTests[[Var]] <- A


Var <- "Platelets"
IdSearch(TableSearch = TableSearch,Variable = Var)
LabTests[[Var]] <- SqlMutate(Table = TableSearch)

A <- LabTests[[Var]]
DataFind(A)
NonNumericFinder(A,"terseForm")
QuantileColumn(A,"terseForm")
A$unitOfMeasure <- "10*9/L"
LabTests[[Var]] <- A



Var <- "WBC"
IdSearch(TableSearch = TableSearch,Variable = Var)
LabTests[[Var]] <- SqlMutate(Table = TableSearch)

A <- LabTests[[Var]]
DataFind(A)
NonNumericFinder(A,"terseForm")
QuantileColumn(A,"terseForm")
# No further action needed


# Haematology -------------------------------------------------------------

Var <- "BEecf"
IdSearch(TableSearch = TableSearch,Variable = Var)
LabTests[[Var]] <- SqlMutate(Table = TableSearch)

A <- LabTests[[Var]]
DataFind(A)
NonNumericFinder(A,"terseForm")
QuantileColumn(A,"terseForm")
# No further action needed

Var <- "Ca\\+\\+"
IdSearch(TableSearch = TableSearch,Variable = Var)
A <- SqlMutate(Table = TableSearch)

DataFind(A)
NonNumericFinder(A,"terseForm")
A <- A[!A$terseForm=="No result",]
A$terseForm <- gsub(A$terseForm,pattern = "<",replacement = "")
QuantileColumn(A,"terseForm")
A$unitOfMeasure <- "mmol/L"
A$valueNumber <- A$terseForm
LabTests$`Ca++` <- BloodGasArterial(A)


Var <- "Glu"
IdSearch(TableSearch = TableSearch,Variable = Var)
# Clinical Lead: Use Glu and then BM stick if glu missing
IdSearchOut$IdSearchIntIds <- c(744,31620,740)
LabTests[[Var]] <- SqlMutate(Table = TableSearch)

A <- LabTests[[Var]]
DataFind(A)
NonNumericFinder(A,"terseForm")
A <- A[!A$terseForm=="No result",]
A$terseForm <- gsub(A$terseForm,pattern = "<|>",replacement = "")
QuantileColumn(A,"terseForm")
LabTests$Glu <- BloodGasArterial(A[!A$interventionId==740,])
LabTests$GluBMStick <- A[A$interventionId==740,]

rm(B,B1)




Var <- "HCO3"
IdSearch(TableSearch = TableSearch,Variable = Var)
LabTests[[Var]] <- SqlMutate(Table = TableSearch)

A <- LabTests[[Var]]
DataFind(A)
NonNumericFinder(A,"terseForm")
A <- A[!A$terseForm=="No result",]
QuantileColumn(A,"terseForm")
# remove HCO3-(c)
A <- A[!A$interventionId%in%c(4328,48958),]
A$unitOfMeasure <- "mmol/L"
A$valueNumber <- A$terseForm
LabTests$`HCO3-std` <- BloodGasArterial(A)
LabTests[[Var]] <- NULL


Var <- "Lac"
IdSearch(TableSearch = TableSearch,Variable = Var)
IdSearchOut$IdSearchIntIds <- IdSearchOut$IdSearchIntIds[!IdSearchOut$IdSearchIntIds%in%
                                                           c(4499,4426)]
LabTests[[Var]] <- SqlMutate(Table = TableSearch)

A <- LabTests[[Var]]
DataFind(A)
NonNumericFinder(A,"terseForm")
A <- A[!A$terseForm=="No result",]
A$terseForm <- gsub(A$terseForm,pattern = ">",replacement = "")
QuantileColumn(A,"terseForm")
A$unitOfMeasure <- "mmol/L"
A$valueNumber <- A$terseForm
LabTests$`Lactic Acid` <- BloodGasArterial(A)



Var <- "pCO2"
IdSearch(TableSearch = TableSearch,Variable = Var)
LabTests[[Var]] <- SqlMutate(Table = TableSearch) # 16k patients means there's multiple vars

A <- LabTests[[Var]]
DataFind(A)
NonNumericFinder(A,"terseForm")
A <- A[!A$terseForm=="No result",]
QuantileColumn(A,"terseForm")
A <- BloodGasArterial(A)
# Clinical lead: We default to arterial PCO2 and then use venous to fill in the gaps.
A$valueNumber <- A$terseForm

LabTests$`pCO2 Arterial` <- A[A$interventionId%in%c(48949,48967,13966,732),]
LabTests$`pCO2 Venous` <- A[A$interventionId%in%c(48949,48967,13966,732),]
LabTests[[Var]] <- NULL



Var <- "pH"
IdSearch(TableSearch = TableSearch,Variable = Var)
# Remove the NICU one
IdSearchOut$IdSearchIntIds <- IdSearchOut$IdSearchIntIds[!IdSearchOut$IdSearchIntIds%in%
                                                           c(56029)]
LabTests[[Var]] <- SqlMutate(Table = TableSearch)

A <- LabTests[[Var]]
DataFind(A)
NonNumericFinder(A,"terseForm")
A <- A[!A$terseForm=="No result",]
A$terseForm <- gsub(A$terseForm,pattern = "<",replacement = "")
A <- BloodGasArterial(A)
QuantileColumn(A,"terseForm")
A$unitOfMeasure <- "Not Available"
A$valueNumber <- A$terseForm

A$ArterialpH[!A$interventionId%in%c(418,48973)] <- A$valueNumber[!A$interventionId%in%c(418,48973)]
A$VenouspH[A$interventionId%in%c(418,48973)] <- A$valueNumber[A$interventionId%in%c(418,48973)]
# First use arterial, then pull in venous for missing
A$pH <- A$ArterialpH
A$pH[is.na(A$pH)] <- A$VenouspH[is.na(A$pH)]
A$valueNumber <- NA
A$valueNumber <- A$pH
A <- subset.data.frame(A, select=-c(pH,VenouspH,ArterialpH))
LabTests$pH <- A



# Misc Lab ----------------------------------------------------------------

Var <- "COHb"
IdSearch(TableSearch = TableSearch,Variable = Var)
LabTests[[Var]] <- SqlMutate(Table = TableSearch)

A <- LabTests[[Var]]
DataFind(A)
NonNumericFinder(A,"terseForm")
A <- A[!A$terseForm=="No result",]
QuantileColumn(A,"terseForm")
A$unitOfMeasure <- "%"
A$valueNumber <- A$terseForm
LabTests[[Var]] <- A

Var <- "Eosinophils"
IdSearch(TableSearch = TableSearch,Variable = Var)
LabTests[[Var]] <- SqlMutate(Table = TableSearch)

A <- LabTests[[Var]]
DataFind(A)
NonNumericFinder(A,"terseForm")
QuantileColumn(A,"terseForm")
A$unitOfMeasure <- "10*9/L"
A$valueNumber <- A$terseForm
LabTests[[Var]] <- A


Var <- "Globulin"
IdSearch(TableSearch = TableSearch,Variable = Var)
LabTests[[Var]] <- SqlMutate(Table = TableSearch)

A <- LabTests[[Var]]
DataFind(A)
NonNumericFinder(A,"terseForm")
QuantileColumn(A,"terseForm")
# No further action needed

Var <- "pO2"
IdSearch(TableSearch = TableSearch,Variable = Var)
# Clinical lead: do not use venous
IdSearchOut$IdSearchIntIds <- 
  IdSearchOut$IdSearchIntIds[!IdSearchOut$IdSearchIntIds%in%c(4652,48975,48972)]
LabTests[[Var]] <- SqlMutate(Table = TableSearch)

A <- LabTests[[Var]]
DataFind(A)
NonNumericFinder(A,"terseForm")
A <- A[!A$terseForm=="No result",]
QuantileColumn(A,"terseForm")
A <- BloodGasArterial(A)
LabTests[[Var]] <- A





# Vital Signs -------------------------------------------------------------
VitalSigns <- list()

TableSearch <- "Assessment"


Var <- "Respiratory Rate"
IdSearch(TableSearch = TableSearch,Variable = Var)
# Only want resp rate not Spont or Total
IdSearchOut$IdSearchIntIds <- 
  IdSearchOut$IdSearchIntIds[!IdSearchOut$IdSearchIntIds%in%c(2199,3451)]
VitalSigns[[Var]] <- SqlMutate(Table = TableSearch)

A <- VitalSigns[[Var]]
DataFind(A)
NonNumericFinder(A,"valueNumber")
QuantileColumn(A,"valueNumber")


Var <- "SpO2"
IdSearch(TableSearch = TableSearch,Variable = Var)
# Remove "Monitor SpO2"
IdSearchOut$IdSearchIntIds <- 
  IdSearchOut$IdSearchIntIds[!IdSearchOut$IdSearchIntIds%in%c(5382)]
VitalSigns[[Var]] <- SqlMutate(Table = TableSearch)

A <- VitalSigns[[Var]]
DataFind(A)
NonNumericFinder(A,"valueNumber")
QuantileColumn(A,"valueNumber")


Var <- "FiO2"
IdSearch(TableSearch = TableSearch,Variable = Var)
# Just want "Fio2"
IdSearchOut$IdSearchIntIds <- 
  IdSearchOut$IdSearchIntIds[!IdSearchOut$IdSearchIntIds%in%c(1980,2973,3117,3124)]
VitalSigns[[Var]] <- SqlMutate(Table = TableSearch)

A <- VitalSigns[[Var]]
DataFind(A)
NonNumericFinder(A,"valueNumber")
QuantileColumn(A,"valueNumber")
# No further action needed

# pO2/FiO2 ratio
A <- subset.data.frame(LabTests$pO2,select = c(interventionId,attributeId,encounterId,chartTime,valueNumber,unitOfMeasure))
B <- subset.data.frame(VitalSigns$FiO2,select = c(interventionId,attributeId,encounterId,chartTime,valueNumber,unitOfMeasure))
colnames(A) = c("interventionId","attributeId","encounterId","chartTime","pO2","pO2unitOfMeasure")
colnames(B) = c("interventionId","attributeId","encounterId","chartTime","FiO2","FiO2unitOfMeasure")

library(lubridate);
A$chartTimeR <- round_date(A$chartTime,unit = "hour")
B$chartTimeR <- round_date(B$chartTime,unit = "hour")

# Floor the timestamps to the nearest 2 hours
for(x in seq(1,23,2)){
  x1 <- x-1
  if(x<10){x <- paste0("0",x)}
  if(x1<10){x1 <- paste0("0",x1)}
  
  A$chartTimeR <- gsub(x = A$chartTimeR, pattern = paste0(x,":00:00"),
                       replacement = paste0(x1,":00:00"))
  B$chartTimeR <- gsub(x = B$chartTimeR, pattern = paste0(x,":00:00"),
                       replacement = paste0(x1,":00:00"))
  }

# order A and B by encounterId and chartTime, then join on encounterId
C <- join(A,B,by=c("encounterId","chartTimeR"),type="inner")

C$po2fio2ratio <- round(C$pO2/C$FiO2,4)
C <- C[!is.na(C$po2fio2ratio),]
count(C$FiO2unitOfMeasure)
count(C$pO2unitOfMeasure)
colnames(C)
C <- subset.data.frame(C,select = c(encounterId,chartTimeR,po2fio2ratio))
colnames(C) <- c("encounterId","chartTime","valueNumber")
VitalSigns$`pO2 FiO2 Ratio` <- C




Var <- "Heart Rate"
IdSearch(TableSearch = TableSearch,Variable = Var)
IdSearchOut$IdSearchIntIds <- 
  IdSearchOut$IdSearchIntIds[!IdSearchOut$IdSearchIntIds%in%c(3090)]
VitalSigns[[Var]] <- SqlMutate(Table = TableSearch)

A <- VitalSigns[[Var]]
DataFind(A)
NonNumericFinder(A,"valueNumber")
QuantileColumn(A,"valueNumber")
# No further action needed

Var <- "Arterial"
IdSearch(TableSearch = TableSearch,Variable = Var)
# Remove arterial line removed and access arterial pressure
IdSearchOut$IdSearchIntIds <- 
  IdSearchOut$IdSearchIntIds[!IdSearchOut$IdSearchIntIds%in%c(25742,3431)]
# Remove site and status
IdSearchOut$IdSearchAttIds <- 
  IdSearchOut$IdSearchAttIds[!IdSearchOut$IdSearchAttIds%in%c(27370,43795,61)]
VitalSigns[[Var]] <- SqlMutate(Table = TableSearch)

A <- VitalSigns[[Var]]
DataFind(A)
NonNumericFinder(A,"valueNumber")
QuantileColumn(A,"valueNumber")
VitalSigns$`Arterial Blood Pressure Systolic` <- 
  A[A$attributeId%in%IdSearchOut$IdSearchAttIdsDetail$attributeId[IdSearchOut$IdSearchAttIdsDetail$attributeShortLabel=="Systolic"],]
VitalSigns$`Arterial Blood Pressure Diastolic` <- 
  A[A$attributeId%in%IdSearchOut$IdSearchAttIdsDetail$attributeId[IdSearchOut$IdSearchAttIdsDetail$attributeShortLabel=="Diastolic"],]
VitalSigns$`Arterial Blood Pressure Mean` <-
  A[A$attributeId%in%IdSearchOut$IdSearchAttIdsDetail$attributeId[IdSearchOut$IdSearchAttIdsDetail$attributeShortLabel=="Mean"],]
VitalSigns[[Var]] <- NULL




Var <- "CVP"
IdSearch(TableSearch = TableSearch,Variable = Var)
IdSearchOut$IdSearchIntIds <- 
  IdSearchOut$IdSearchIntIds[!IdSearchOut$IdSearchIntIds%in%c(25738,3048,2988)]
VitalSigns[[Var]] <- SqlMutate(Table = TableSearch)

A <- VitalSigns[[Var]]
DataFind(A)
NonNumericFinder(A,"valueNumber")
QuantileColumn(A,"valueNumber")
# No further action needed



Var <- "Central Temp"
IdSearch(TableSearch = TableSearch,Variable = Var)
IdSearchOut$IdSearchAttIds <- 
  IdSearchOut$IdSearchAttIds[!IdSearchOut$IdSearchAttIds%in%c(32674,48876,21425)]
VitalSigns[[Var]] <- SqlMutate(Table = TableSearch)

A <- VitalSigns[[Var]]
DataFind(A)
NonNumericFinder(A,"valueNumber")
QuantileColumn(A,"valueNumber")
# No further action needed


Var <- "Peripheral Temp"
IdSearch(TableSearch = TableSearch,Variable = Var)
IdSearchOut$IdSearchAttIds <- 
  IdSearchOut$IdSearchAttIds[!IdSearchOut$IdSearchAttIds%in%c(32689,48829,14090)]
VitalSigns[[Var]] <- SqlMutate(Table = TableSearch)

A <- VitalSigns[[Var]]
DataFind(A)
NonNumericFinder(A,"valueNumber")
QuantileColumn(A,"valueNumber")
# No further action needed


# Ventilation -------------------------------------------------------------

TableSearch <- "Assessment"

Var <- "Airway"
IdSearch(TableSearch = TableSearch,Variable = Var)
# Narrow to explicitly "airway"
IdSearchOut$IdSearchIntIds <- 2098
IdSearchOut$IdSearchAttIds <- 8590
VitalSigns[[Var]] <- SqlMutate(Table = TableSearch)

A <- VitalSigns[[Var]]
DataFind(A)
NonNumericFinder(A,"terseForm")
# Data here is categorical strings rather than numeric

# Group into invasive vs natural
A$valueNumber[A$terseForm%in%c(
  "CPAP Hood","CPAP Mask","Mini-Trach","Nasal ETT","Oral ETT","Trach")] <- "Invasive"
A$valueNumber[is.na(A$valueNumber)] <- "Natural"
count(A$valueNumber)
# Mark the patients valid for EtCO2 data
A$terseForm <- as.character(A$terseForm)
A$EtCO2Valid[A$terseForm=="Trach"] <- 1
A$EtCO2Valid[A$terseForm=="Oral ETT"] <- 1
A$EtCO2Valid[A$terseForm=="Nasal ETT"] <- 1
A$EtCO2Valid[A$terseForm=="Mini-Trach"] <- 1
A$EtCO2Valid[is.na(A$EtCO2Valid)] <- 0
VitalSigns$`Invasive Ventilation` <- A

Var <- "EtCO2"
IdSearch(TableSearch = TableSearch,Variable = Var)
VitalSigns[[Var]] <- SqlMutate(Table = TableSearch)

A <- VitalSigns[[Var]]
DataFind(A)
NonNumericFinder(A,"valueNumber")
QuantileColumn(A,"valueNumber")
# Clinical lead: EtCO2 data only valid for those with certain ventilation types
B <- A

# Handle invasive ventilation
A <- VitalSigns$`Invasive Ventilation`

B <- B[B$encounterId%in%A$encounterId[A$EtCO2Valid==1],]
VitalSigns$EtCO2 <- B
rm(A,B)
# Remove the raw invasive ventilation data now that we have the transformed version and etco2
VitalSigns$Airway <- NULL


Var <- "Actual Tidal Volume"
IdSearch(TableSearch = TableSearch,Variable = Var)
VitalSigns[[Var]] <- SqlMutate(Table = TableSearch)

A <- VitalSigns[[Var]]
DataFind(A)
NonNumericFinder(A,"valueNumber")
QuantileColumn(A,"valueNumber")
# No further action needed

Var <- "Mean Airway Pressure"
IdSearch(TableSearch = TableSearch,Variable = Var)
VitalSigns[[Var]] <- SqlMutate(Table = TableSearch)

A <- VitalSigns[[Var]]
DataFind(A)
NonNumericFinder(A,"valueNumber")
QuantileColumn(A,"valueNumber")
# No further action needed

Var <- "PEEP"
IdSearch(TableSearch = TableSearch,Variable = Var)
IdSearchOut$IdSearchIntIds <- 3036
VitalSigns[[Var]] <- SqlMutate(Table = TableSearch)

A <- VitalSigns[[Var]]
DataFind(A)
NonNumericFinder(A,"valueNumber")
QuantileColumn(A,"valueNumber")
# No further action needed

Var <- "Ventilation"
IdSearch(TableSearch = TableSearch,Variable = Var)
# Restrict to ventilation mode/vent mode
IdSearchOut$IdSearchAttIds <- c(26466,26464)
VitalSigns[[Var]] <- SqlMutate(Table = TableSearch)

A <- VitalSigns[[Var]]
DataFind(A)
# Categorical string, so no numerical data
# AttId 26464 == Mechanical ventilation part 2
# All other attids == Mechanical ventilation part 1
A <- A[!A$attributeId==26464,]

A$terseForm <- as.character(A$terseForm)

A$valueNumber[A$terseForm%in%c("APRV","BIPAP.","CPAP","CPAP + Pressure Support","Duo-PAP","HFOV","PC-SIMV")] <- "Mechanical"
A$valueNumber[A$terseForm=="Self Vent"] <- "Self Vent"
A$valueNumber[is.na(A$valueNumber)] <- "Self Vent"

A <- A[,c("encounterId","terseForm","valueNumber","chartTime")]
MechVent1 <- A

A <- VitalSigns[[Var]]
A <- A[A$attributeId==26464,]
A$terseForm <- as.character(A$terseForm)
# Drop NAs
A <- A[!is.na(A$terseForm),]
count(A$terseForm)
# Clinical contact doesn't know what NC is, so we drop
A <- subset.data.frame(A, !terseForm=="NC")
# Drop Apnea
A <- subset.data.frame(A, !terseForm=="Mode APNEA VENTILATION")

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
colnames(A) <- c("encounterId","Vent Mode","chartTime")
colnames(B) <- c("encounterId","Ventilation Mode","chartTime")
C <- merge(A,B,by=c("encounterId","chartTime"),sort = TRUE,all = TRUE)

C$MechVentCombos <- C$`Ventilation Mode`
C$MechVentCombos[is.na(C$MechVentCombos)] <- C$`Vent Mode`[is.na(C$MechVentCombos)]
C <- subset.data.frame(C, select=c(encounterId,MechVentCombos,chartTime))
VitalSigns$`Mechanical Ventilation` <- C
rm(A,B,C,MechVent1,MechVent2)


# Haemofiltration ---------------------------------------------------------

TableSearch <- "SiteCare"

Var <- "Filtration Out: Hourly Fluid"
IdSearch(TableSearch = TableSearch,Variable = Var)
# Restrict to "Volume"
IdSearchOut$IdSearchAttIds <- 10258
VitalSigns[[Var]] <- SqlMutate(Table = TableSearch)

A <- VitalSigns[[Var]]
DataFind(A)
NonNumericFinder(A,"valueNumber")
QuantileColumn(A,"valueNumber")
# Just want a binary recieved/did not receive
A$valueNumber <- 1

# No further action needed
VitalSigns$Haemofiltration <- A
VitalSigns[[Var]] <- NULL


# Drug Infusions Preliminary ----------------------------------------------

TableSearch <- "Demographic"

Demographics <- list()

Var <- "Weight \\(A|Weight \\(a"
IdSearch(TableSearch = TableSearch,Variable = Var)
# Admission weight
A <- SqlMutate(Table = TableSearch)

DataFind(A)
NonNumericFinder(A,"valueNumber")
QuantileColumn(A,"valueNumber")
Demographics$Weight <- A


TableSearch <- "Medication"

DBConn(1)
`PA Weight (Alt)` <- sqlQuery(conn, "
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
 ")
DBConn(0)

# # Use this to find the 16 patients who we have a variable weight for. Currently just using minimum.
# PA Weight (Final) should be used rather than PA Weight or PA Weight (Alt)
B <- Demographics$Weight

A <- `PA Weight (Alt)`;
A$Check <- (A$MaxWeight - A$MinWeight);
A <- A[A$Check!=0,];
A1 <- data.frame("encounterId" = setdiff(A$encounterId,B$encounterId))
A2 <- join(LoSS,A1,by="encounterId",type="inner");
A3 <- join(A,A1,type="inner");
A4 <- join(LoSS,A,type="inner");
A5 <- A3[,1:2];
colnames(A5) = c("encounterId","valueNumber");
A5$`I-Shortlabel` = "Weight (Admission)";
A5$`A-Shortlabel` = "Weight";
A5$verboseForm = 0;
A6 <- join(LoSS,A5,type="inner");
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
Demographics$`PA Weight (Final)` <- A8;
remove(A,A1,A2,A3,A4,A5,A6,A7,A8);

rm(`PA Weight (Alt)`)
Demographics$Weight <- NULL
`PA Weight (Final)` <- `PA Weight (Final)`[!is.na(`PA Weight (Final)`$unitOfMeasure),]
Demographics$`Weight (Final)` <- `PA Weight (Final)`
rm(`PA Weight (Final)`)

# Drug Infusions ----------------------------------------------------------
Medication <- list()

TableSearch <- "Medication"

# Drug infusions are a wreck.
# Example:
Var <- "Labetalol"
IdSearch(TableSearch = TableSearch,Variable = Var)
# interventionIds have values mixed in with them:
count(IdSearchOut$IdSearchIntIdsDetail$interventionShortLabel)
# attributeIds are massively over-duplicated
count(IdSearchOut$IdSearchAttIdsDetail$attributeShortLabel)

# Clinical lead: Look in dose/weight
#   for those with less than 90% of patients with dose/weight: use dose
#   If still no data: use ((volume adm * conc) / patient weight)

# Pull the attributes with the following shortLabels
AShortLabels <- c("Dose/Weight","Dose","Volume Adm","Conc")

IdSearchOut$IdSearchAttIds <- 
  IdSearchOut$IdSearchAttIdsDetail$attributeId[IdSearchOut$IdSearchAttIdsDetail$attributeShortLabel%in%AShortLabels]
Medication[[Var]] <- SqlMutate(Table = TableSearch)

A <- Medication[[Var]]

# Overwrite the duplicates with the first of its kind
x1 <- AShortLabels[1]
count(A$attributeId)
for(x1 in AShortLabels){
  # Pull all the attributeIds that have the shortLabel
  x <- as.numeric(unique(TotalIdCombosShort[TotalIdCombosShort$attributeShortLabel==x1,]$attributeId))
  # Overwrite all the different attributeIds for that shortlabel with the first
  A$attributeId[A$attributeId%in%x] <- x[1]
}
count(A$attributeId)
# Number of patients for each Id
length(unique(A$encounterId[A$attributeId==unique(A$attributeId)[1]]))
length(unique(A$encounterId[A$attributeId==unique(A$attributeId)[2]]))
length(unique(A$encounterId[A$attributeId==unique(A$attributeId)[3]]))
# No dose/weight, but we do have dose, so use that
A <- A[A$attributeId==13521,]
Medication[[Var]] <- A


# I've done the above for the rest of the drug infusions
# The following three lists show which have dose/weight, dose, and ((volume adm * conc) / patient weight)
# Want to use different data for different drug infusions
DrugInfusionsDoseWeight <- c("Adrenaline", "Dexmedetomidine", "Dobutamine", "Dopamine", "Enoximone",
                             "Levosimendan", "Milrinone", "Noradrenaline","Sodium nitroprusside")
DrugInfusionsDose <- c("Glyceryl Trinitrate","Labetalol","Propofol","Vasopressin")
DrugInfusionsCalculate <- "Clonidine"

for(x in DrugInfusionsDoseWeight){
  Var <- x
  IdSearch(TableSearch = TableSearch,Variable = Var)
  IdSearchOut$IdSearchAttIds <- 
    IdSearchOut$IdSearchAttIdsDetail$attributeId[IdSearchOut$IdSearchAttIdsDetail$attributeShortLabel%like%"Dose/Weight"]
  
  Medication[[Var]] <- SqlMutate(Table = TableSearch)
  
  A <- Medication[[Var]]
  
  # Overwrite the duplicates with the first of its kind
  x1 <- AShortLabels[1]
  count(A$attributeId)
  for(x1 in AShortLabels){
    # Pull all the attributeIds that have the shortLabel
    x <- as.numeric(unique(TotalIdCombosShort[TotalIdCombosShort$attributeShortLabel==x1,]$attributeId))
    # Overwrite all the different attributeIds for that shortlabel with the first
    A$attributeId[A$attributeId%in%x] <- x[1]
  }
  Medication[[Var]] <- A
}

for(x in DrugInfusionsDose){
  Var <- x
  IdSearch(TableSearch = TableSearch,Variable = Var)
  IdSearchOut$IdSearchAttIds <- 
    IdSearchOut$IdSearchAttIdsDetail$attributeId[IdSearchOut$IdSearchAttIdsDetail$attributeShortLabel%like%"Dose"]
  
  Medication[[Var]] <- SqlMutate(Table = TableSearch)
  
  A <- Medication[[Var]]
  
  # Overwrite the duplicates with the first of its kind
  x1 <- AShortLabels[1]
  count(A$attributeId)
  for(x1 in AShortLabels){
    # Pull all the attributeIds that have the shortLabel
    x <- as.numeric(unique(TotalIdCombosShort[TotalIdCombosShort$attributeShortLabel==x1,]$attributeId))
    # Overwrite all the different attributeIds for that shortlabel with the first
    A$attributeId[A$attributeId%in%x] <- x[1]
  }
  Medication[[Var]] <- A
}

# Extract the Vol Adm and Concentration
x <- DrugInfusionsCalculate[1]
for(x in DrugInfusionsCalculate){
  Var <- x
  IdSearch(TableSearch = TableSearch,Variable = Var)
  count(IdSearchOut$IdSearchAttIdsDetail$attributeShortLabel)
  IdSearchOut$IdSearchAttIds <- 
    IdSearchOut$IdSearchAttIdsDetail$attributeId[IdSearchOut$IdSearchAttIdsDetail$attributeShortLabel%in%c("Volume Adm","Conc")]
  Medication[[Var]] <- SqlMutate(Table = TableSearch)
  
  A <- Medication[[Var]]
  
  # Overwrite the duplicates with the first of its kind
  count(A$attributeId)
  for(x1 in AShortLabels){
    # Pull all the attributeIds that have the shortLabel
    x <- as.numeric(unique(TotalIdCombosShort[TotalIdCombosShort$attributeShortLabel==x1,]$attributeId))
    # Overwrite all the different attributeIds for that shortlabel with the first
    A$attributeId[A$attributeId%in%x] <- x[1]
  }
  count(A$attributeId)
  Medication[[Var]] <- A
}
TotalIdCombos$attributeShortLabel[TotalIdCombos$attributeId==13550]

for(x in names(Medication)){
  print(x)
  print(length(unique(Medication[[x]]$encounterId)))
  print("")
}


# Calculate: VolAdm*Conc/Weight
{
  # Vol adm
  A1 <- Medication$Clonidine[Medication$Clonidine$attributeId==13550,]
  # Conc
  A2 <- Medication$Clonidine[!Medication$Clonidine$attributeId==13550,]
  # Weight
  A3 <- Demographics$`Weight (Final)`
  A3 <- A3[!A3$valueNumber==0,]
  A3 <- A3[!duplicated(A3$encounterId),]
  
  # Subset to encounterId, chartTime, valueNumber, and re-name
  A1 <- data.frame("encounterId" = A1$encounterId,"chartTime" = A1$chartTime,"VolAdm" = A1$valueNumber)
  A2 <- data.frame("encounterId" = A2$encounterId,"chartTime" = A2$chartTime,"Conc" = A2$valueNumber)
  A3 <- data.frame("encounterId" = A3$encounterId,"Weight" = A3$valueNumber)
  
  A4 <- join(A1,A2,by=c("encounterId","chartTime"))
  A4 <- join(A4,A3,by="encounterId")
  rm(A1,A2,A3)
  
  # Remove NAs and any time that volume = 0 
  A4 <- A4[!is.na(A4$Conc),];A4 <- A4[!is.na(A4$Weight),];A4 <- A4[!A4$VolAdm==0,]
  
  A4$Calc <- (A4$VolAdm*A4$Conc)/A4$Weight
  
  A4 <- data.frame("encounterId" = A4$encounterId,"chartTime" = A4$chartTime,"valueNumber" = A4$Calc)
  
  Medication$Clonidine <- A4
  rm(A4)

}

rm(DrugInfusions,DrugInfusionsCalculate,DrugInfusionsDose,DrugInfusionsDoseWeight)

# Clinical contact:
# 1) how to transform the IV infusion
# Delete the IV Infusion, just leave drug infusion name and merge

# 2) what to do with noradrenaline mcg/kg/min unlabelled.
# Delete the mg/ml and merge

# 3) Can we merge anything together? How do we differentiate between 4mg/8mg? "received" = 0/1/2/3?
# Just want received yes/no

# Check your quantileValueNumber df (B) to see if these are all roughly similar
# If so: Narrow your drug infusion db down to these


# Altering Vasopressin to only use units/min
A <- Medication$Vasopressin

A$valueNumber <- A$baseValueNumber
A$terseForm <- A$baseValueNumber
A$verboseForm <- A$baseValueNumber
A$unitOfMeasure <- A$baseUOM

count(A$unitOfMeasure)
Medication$Vasopressin <- A

# Glyceryl trinitrate and propofol have mg/hr and mg
A <- Medication$`Glyceryl Trinitrate`
A <- A[!is.na(A$terseForm),]
A <- A[!is.na(A$unitOfMeasure),]
A <- A[!A$unitOfMeasure=="mg",]
Medication$`Glyceryl Trinitrate` <- A

A <- Medication$Propofol
A <- A[!A$unitOfMeasure=="mg",]
A <- A[!A$unitOfMeasure=="mg/kg/hr",] # only 41 measurements in /kg/
Medication$Propofol <- A

A <- Medication$Labetalol
A <- A[!is.na(A$terseForm),]
A <- A[!is.na(A$unitOfMeasure),]
A <- A[!A$unitOfMeasure=="mg",]
Medication$Labetalol <- A

# Output ------------------------------------------------------------------
Output <- list()

TableSearch <- "SiteCare"

Var <- "Indwelling"
IdSearch(TableSearch = TableSearch,Variable = Var)
# Restrict to "Volume"
IdSearchOut$IdSearchAttIds <- 
  IdSearchOut$IdSearchAttIdsDetail$attributeId[
    IdSearchOut$IdSearchAttIdsDetail$attributeShortLabel%like%"Volume|volume"]
Output[[Var]] <- SqlMutate(Table = TableSearch)

A <- Output[[Var]]
DataFind(A)
NonNumericFinder(A,"valueNumber")
QuantileColumn(A,"valueNumber")
# No further action needed


Var <- "Chest: Mediastinal"
IdSearch(TableSearch = TableSearch,Variable = Var)
# Restrict to "Volume"
IdSearchOut$IdSearchAttIds <- 
  IdSearchOut$IdSearchAttIdsDetail$attributeId[
    IdSearchOut$IdSearchAttIdsDetail$attributeShortLabel%like%"Volume|volume"]
Output[[Var]] <- SqlMutate(Table = TableSearch)

A <- Output[[Var]]
# narrow down to just chest: mediastinal
A <- A[A$interventionId==5803,]
QuantileColumn(A,"valueNumber")
NonNumericFinder(A,"valueNumber")
Output[[Var]] <- A


TableSearch <- "TotalBalance"
Var <- "Net Body Balance \\(L"
IdSearch(TableSearch = TableSearch,Variable = Var)

SqlMutate
# Need to edit the sqlmutate to exclude attribute data
DBConn(1)
`PA Net Body Balance` <- sqlQuery(
  conn, paste0("SELECT * 
  FROM PtTotalBalance 
  WHERE (interventionId IN (",paste0("'",IdSearchOut$IdSearchIntIds,"'",collapse = ","),"))
  AND clinicalUnitId = '8'"))
DBConn(0)

A <- `PA Net Body Balance`
A <- subset.data.frame(A, select=c("interventionId","encounterId","chartTime","hourTotal","cumTotal","unitOfMeasure",
                                   "baseHourTotal", "baseCumTotal","baseUOM"))
Output$`Net Body Balance (LOS)` <- A
rm(`PA Net Body Balance`)

# Ben's Variables ---------------------------------------------------------

# Irregular Lab Tests -----------------------------------------------------


TableSearch <- "LabResult"

Var <- "Anion"
IdSearch(TableSearch = TableSearch,Variable = Var)
LabTests[[Var]] <- SqlMutate(Table = TableSearch)

A <- LabTests[[Var]]
DataFind(A)
A$valueNumber <- A$terseForm
NonNumericFinder(A,"valueNumber")
QuantileColumn(A,"valueNumber")
A$unitOfMeasure <- "No Units"
LabTests[[Var]] <- A
# No further action needed


Var <- "Fibrinogen"
IdSearch(TableSearch = TableSearch,Variable = Var)
A <- SqlMutate(Table = TableSearch)
DataFind(A)
A$valueNumber <- A$terseForm
NonNumericFinder(A,"valueNumber")
QuantileColumn(A,"valueNumber")
LabTests$`Fibrinogen (Clauss)` <- A
# No further action needed


Var <- "Uric"
IdSearch(TableSearch = TableSearch,Variable = Var)
A <- SqlMutate(Table = TableSearch)
DataFind(A)
A$valueNumber <- A$terseForm
NonNumericFinder(A,"valueNumber")
QuantileColumn(A,"valueNumber")
LabTests$`Urate [Uric Acid]` <- A



Var <- "Gamma-GT|GGT"
IdSearch(TableSearch = TableSearch,Variable = Var)
A <- SqlMutate(Table = TableSearch)
DataFind(A)
A$valueNumber <- A$terseForm
NonNumericFinder(A,"valueNumber")
QuantileColumn(A,"valueNumber")
LabTests[[Var]] <- A



# IABP --------------------------------------------------------------------
# All we need is a binary "IABP was there"

TableSearch <- "Assessment"
Var <- "IABP"
IdSearch(TableSearch = TableSearch,Variable = Var)
A <- SqlMutate(Table = TableSearch)
DataFind(A)
# Just want a binary recieved/not received
A$valueNumber <- 1
A <- subset.data.frame(A, select=c("encounterId","chartTime","valueNumber"))
LabTests[[Var]] <- A



# Surgery Nature ----------------------------------------------------------

Assessment <- list()

DBConn(1)
# Old Surgery Nature
SurgeryNature1 <- sqlQuery(conn, "
SELECT attributeId, interventionid, encounterid AS [encounterId], clinicalunitId, terseForm, verboseform, valuestring
FROM ptassessment
WHERE (attributeID = '1816' OR interventionID = '1516')
AND ClinicalUnitID = '8';
");

SurgeryNature1 <- data.frame(encounterId = SurgeryNature1$encounterId,
                             valueNumber = SurgeryNature1$terseForm)

SurgeryNature2 <- sqlQuery(conn, "
SELECT attributeId, interventionid, encounterid AS [encounterId], clinicalunitId, terseForm, verboseform, valuestring
FROM ptassessment
WHERE attributeID IN ('505','1870','9088')
AND ClinicalUnitID = '8'
")

A <- SurgeryNature2
count(A$terseForm)
# Drop general theatres and re-format
A <- A[!A$terseForm=="Elective - General Theatres",]
A <- A[!A$terseForm=="Emergency - General Theatres",]
A$valueNumber[A$terseForm == "Elective - Cardiac Theatres"] <- "Elective"
A$valueNumber[A$terseForm == "Emergency - Cardiac Theatres"] <- "Emergency"
A$valueNumber[A$terseForm == "Emergency - Cardiology"] <- "Emergency"
A$valueNumber[A$terseForm == "Hospital Transfer"] <- NA

A <- subset.data.frame(A, select = c(encounterId,valueNumber))
SurgeryNature2 <- A

A <- join(data.frame(encounterId = SurgeryNature1$encounterId,
                     SurgeryNature1 = SurgeryNature1$valueNumber),
          data.frame(encounterId = SurgeryNature2$encounterId,
                     SurgeryNature2 = SurgeryNature2$valueNumber),
          by="encounterId",match="all",type="full")

# Information from first Surgery Nature
count(A$SurgeryNature1)[order(count(A$SurgeryNature1)$freq,decreasing = TRUE),]
# Information from second Surgery Nature
count(A$SurgeryNature2)[order(count(A$SurgeryNature2)$freq,decreasing = TRUE),]

# What info do we have in surgery 1 when we have no info in surgery 2?
A1 <- count(A$SurgeryNature1[is.na(A$SurgeryNature2)])
A1 <- A1[order(A1$freq,decreasing = T),]

AA <- A[is.na(A$SurgeryNature2),]
AA$SurgeryNature2[AA$SurgeryNature1=="Elective"] <- "Elective"
AA$SurgeryNature2[AA$SurgeryNature1=="Not from Theatre"] <- NA
AA$SurgeryNature2[AA$SurgeryNature1=="Emergency"] <- "Emergency"
AA$SurgeryNature2[AA$SurgeryNature1=="Urgent"] <- "Elective"
AA$SurgeryNature2[AA$SurgeryNature1=="Scheduled"] <- "Elective"
table(AA$SurgeryNature1,AA$SurgeryNature2,useNA = "always")
rm(AA)

# Make sure to REMOVE SurgeryNature1 "Not From Theatre" from all further analysis
NotFromTheatrePatients <- A$encounterId[A$SurgeryNature1=="Not from Theatre" & !is.na(A$SurgeryNature1)]
A <- A[!A$encounterId %in% NotFromTheatrePatients,]

# Remove the hospital transfer patients
LoSS <- LoSS[!LoSS$encounterId %in% NotFromTheatrePatients,]
rm(NotFromTheatrePatients)


# Clinical contact: For those with missing data in nature of surgery 2, use the data from nature of surgery 1
A$SurgeryNature2[is.na(A$SurgeryNature2) & A$SurgeryNature1=="Elective"] <- "Elective"
A$SurgeryNature2[is.na(A$SurgeryNature2) & A$SurgeryNature1=="Urgent"] <- "Elective"
A$SurgeryNature2[is.na(A$SurgeryNature2) & A$SurgeryNature1=="Scheduled"] <- "Elective"
A$SurgeryNature2[is.na(A$SurgeryNature2) & A$SurgeryNature1=="Emergency"] <- "Emergency"
count(A$SurgeryNature1[is.na(A$SurgeryNature2)]) # We have no further information we can pull
# NA = Most common group = elective
count(A$SurgeryNature2)
A$SurgeryNature2[is.na(A$SurgeryNature2)] <- "Elective"

A <- subset.data.frame(A,select=-SurgeryNature1)
colnames(A) = c("encounterId","SurgeryNature")
Assessment$SurgeryNature <- A

rm(SurgeryNature1,SurgeryNature2,A)


# Surgical Procedures -----------------------------------------------------

TableSearch <- "Assessment"

Var <- "Operation \\(OPCS Code\\)"
IdSearch(TableSearch = TableSearch,Variable = Var)
A <- SqlMutate(Table = TableSearch)
DataFind(A)
# Backup
SurgeryType <- A
A <- SurgeryType

count(A$terseForm)
A$terseForm <- as.character(A$terseForm)

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

A$SurgeryType <- as.character(A$SurgeryType)

# Change further scripts to Valve Surgery

#remove duplicate encounterIds but keep them when the terseForm changes
library("dplyr")
A <- A %>% distinct(encounterId, terseForm, .keep_all = TRUE)

detach("package:dplyr", unload=TRUE)

count(A$SurgeryType)

Z <- count(A, "encounterId")

A <- join(A, Z, type = "right", match="all", by = "encounterId")
remove(Z)

colnames(A)[colnames(A)=="freq"] <- "NumberOfSurgeries"
A$NumberOfSurgeries <- as.numeric(as.character(A$NumberOfSurgeries))

# Want to also make a column that tells me all the different kinds of surgery they had at once.
A1 <- aggregate(SurgeryType ~ encounterId, data = A, toString)
colnames(A1) <- c("encounterId","TotalSurgeries")

A2 <- join(A,A1,by="encounterId",type="left",match = "first")
A2 <- subset.data.frame(A2, !duplicated(A2$encounterId))
A2 <- subset.data.frame(A2, select = c(encounterId,NumberOfSurgeries,TotalSurgeries))

`PA SurgeryType` <- A2
rm(A,A1,A2)

# Changing surgerytype to have one variable per number of each surgery 
# Find the number of each type of surgery each patient had
A <- `PA SurgeryType`
# A <- A[!is.na(A$TotalSurgeries),]

# Vectors of surgery types we're looking for
TypesOfSurgery <- c("CABG","Valve Surgery","Structural Defect Repair","Replacement of Part of the Aorta","Other")
TypesOfSurgeryNoSpace <- gsub(TypesOfSurgery,pattern=" ", replacement = "")
SurgerySequences <- paste0(TypesOfSurgeryNoSpace,"Seq")

# This code counts the number of each type of surgery in TotalSurgeries per patient.
for(x1 in 1:length(TypesOfSurgery)){
  A[[paste0(TypesOfSurgeryNoSpace[x1],"Number")]] <- 
    sapply(gregexpr(TypesOfSurgery[x1],A$TotalSurgeries),
           function(x) sum(x != -1))
}

# Any CABG above 4 recoded to 4
writeLines(text=paste0(nrow(A[A$CABGNumber>4,])," patients with CABG>4"))
A$CABGNumber[A$CABGNumber>4] <- 4

# Makes a sequence going up in 1s for the number of each surgery (so 0 - 3 CABGs etc). Used in later code.
Seq <- list()
for(x1 in 1:length(TypesOfSurgery)){
  Seq[[TypesOfSurgeryNoSpace[x1]]] <- 
    min(A[[paste0(TypesOfSurgeryNoSpace[x1],"Number")]],na.rm=T):
    max(A[[paste0(TypesOfSurgeryNoSpace[x1],"Number")]],na.rm=T)
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
A1 <- join(subset.data.frame(LoSS, select = encounterId),A1,by="encounterId",type="left")
count(A1$TotalSurgeries)

MissingSurgeryData <- A1$encounterId[is.na(A1$TotalSurgeries)]
# Patients with missing data are assigned most common group (Valve Surgery x1)
A1$ValveSurgeryNumber[is.na(A1$ValveSurgeryNumber)] <- 1
A1[is.na(A1)] <- 0

# Valve surgery > 2 = 2
paste0(nrow(A1[A1$ValveSurgeryNumber>2,])," patients with Valve>2")
A1$ValveSurgeryNumber[A1$ValveSurgeryNumber>2] <- 2

# Structural Defect Repair > 1 = 1
paste0(nrow(A1[A1$StructuralDefectRepairNumber>1,])," patients with StructuralDefect>1")
A1$StructuralDefectRepairNumber[A1$StructuralDefectRepairNumber>1] <- 1

# Replacement of Part of the Aorta > 1 = 1
paste0(nrow(A1[A1$ReplacementofPartoftheAortaNumber>1,])," patients with ReplacementofPartoftheAortaNumber>1")
A1$ReplacementofPartoftheAortaNumber[A1$ReplacementofPartoftheAortaNumber>1] <- 1

# Other > 1 = 1
paste0(nrow(A1[A1$OtherNumber>1,])," patients with OtherNumber>1")
A1$OtherNumber[A1$OtherNumber>1] <- 1

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

A11 <- count(A1$TotalSurgeries)
# Remove temp columns
A1 <- subset.data.frame(A1,select=-c(TotalSurgeriesCABG,TotalSurgeriesValve,TotalSurgeriesStruc,
                                     TotalSurgeriesReplace,TotalSurgeriesOther))
# Redo A1$Numberofsurgeries
A1$NumberOfSurgeries <- A1$CABGNumber+A1$ValveSurgeryNumber+A1$StructuralDefectRepairNumber+A1$ReplacementofPartoftheAortaNumber+A1$OtherNumber
A1$NumberOfSurgeries[A1$NumberOfSurgeries>4] <- 4

Assessment$SurgeryType <- A1

rm(A1,A,IndividualSurgeryCodes,TypesOfSurgery,TypesOfSurgeryNoSpace,SurgerySequences,Seq)
rm(SurgeryType,`PA SurgeryType`)

# Left Ventricular Ejection Fraction --------------------------------------

TableSearch <- "Assessment"
Var <- "Pre-op LV"
IdSearch(TableSearch = TableSearch,Variable = Var)
A <- SqlMutate(Table = TableSearch)
DataFind(A)

# Sort out various columns, remove NAs, and ensure factor is grouped properly
A$valueNumber <- as.character(A$terseForm)
A$baseValueNumber <- as.character(A$terseForm)
A$unitOfMeasure = "N/A"
A <- subset.data.frame(A, !is.na(A$valueNumber))
count(A$valueNumber)

A$valueNumber[A$valueNumber%like%"Normal|Good"] <- "Good (>50%)"
A$valueNumber[A$valueNumber%like%"Mildly Impaired|Moderately Impaired"] <- "Moderately Impaired (45-50%)"
A$valueNumber[A$valueNumber%like%"Severely Impaired"] <- "Severely Impaired (<30%)"
count(A$valueNumber)

Assessment[["LVEF"]] <- A

# Using LoSS join with D1 Extraction. LVEF only has pre-op, so just find latest LVEF.
A <- subset.data.frame(A, select = -c(terseForm,baseValueNumber,verboseForm))
A <- join(LoSS, A, by = "encounterId", type = "inner", match = "all");

# extract data from first day and closest to 8am
# Number of midnights since admittance to ward
# DIW = Days In Ward (number of midnights that have passed)
A$DIW <- as.integer(difftime(as.Date(A$chartTime), as.Date(A$Start_date), units = "days"));
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
B <- join(x = data.frame(encounterId = LoSS$encounterId),y = B,by="encounterId")
count(B$`LVEF Pre-Op`)
Assessment$`LVEF Pre-Op` <- B
Assessment[["LVEF"]] <- NULL
rm(A,B)

# Patient Census ----------------------------------------------------------

# DBConn(1)
# PtCensus <- sqlQuery(conn,
#   "SELECT encounterId, inTime, outTime, lengthOfStay,is24HrReAdmit, admitSource,
#   isDischarged, isDeceased, isTransferred
#   FROM PtCensus WHERE clinicalUnitId = '8'")
# DBConn(0)
# rm(PtCensus)

# Tidying Up --------------------------------------------------------------

rm(x,x1,x2,A11,AShortLabels,MissingSurgeryData,NotFromTheatrePatients,Var,TableSearch)

# Saving ------------------------------------------------------------------
save.image("C:/Users/shillandun/Downloads/Patient Data/Patient Data Update.RData")
