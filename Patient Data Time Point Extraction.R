# Introduction ------------------------------------------------------------
# Patient data is stored in individual dataframes
# This script pulls each one and extracts 8am data from them
# Four windows are currently used depending on what is needed
# 1) Most recent data
# 2) Maximum/median over 24 hours
# 3) Most recent data in four hour window
# 4) Data at 8am

# Then collects them all together into `PA 8am`
# Then adds extraction day onto the name at the end: `PA 8am D1`/`PA 8am D2` etc

rm(list=ls())
# Load the raw dataframes
load("C:/Users/shillandun/Downloads/Patient Data/Patient Data Update.RData")

# Collapse lists of dataframes to PD (Patient Data)
PD <- c(LabTests,VitalSigns,Demographics,Medication,Output,Assessment)

names(PD)[names(PD)=="Anion"] <- "Anion-Gap"
names(PD)[names(PD)=="Gamma-GT|GGT"] <- "Gamma-GT (GGT)"

# Tidy
rm(list=ls()[ls() %in% c("LabTests","VitalSigns","Demographics","Medication","Output","Assessment")])
rm(BloodGasArterial,DBConn,IdDataPull,IdSearch,NonNumericFinder,QuantileColumnOut,
   QuantileColumn,DeleteUrineIds,SqlMutate,TableSearch,Var,DataFind,DataFindOut,
   TotalIdCombos,TotalIdCombosShort,Hospital_Census,IdAttribute,IdIntervention,
   IdSearchOut,SampleType)

# Set the day you want to extract 8am data from
ExtractionDay <- 1

# Various edits to the dataframes
{
  # Correct unit of measure
  PD$`Gamma-GT (GGT)`$unitOfMeasure <- "Units/L"
  PD$`Alkaline Phosphate`$unitOfMeasure <- "U/L"
  PD$`ALT (SGPT)`$unitOfMeasure <- "U/L"
  PD$eGFR$unitOfMeasure <- "ml/kg"
  PD$MCH$unitOfMeasure <- "pg"
  PD$Clonidine$unitOfMeasure <- "mcg/kg/hr"
  PD$Bilirubin$unitOfMeasure <- "mmol/L"
  PD$Glu$unitOfMeasure <- "mmol/L"
  PD$Lac$unitOfMeasure <- "mmol/L"
  PD$`pCO2 Arterial`$unitOfMeasure <- "kPa"
  PD$`pCO2 Venous`$unitOfMeasure <- "kPa"
  PD$pO2$unitOfMeasure <- "kPa"
  PD$CVP$unitOfMeasure <- "mmHg"
  PD$`Anion-Gap`$unitOfMeasure <- "No Units"
  PD$`Urate [Uric Acid]`$unitOfMeasure <- "mg/dL"
  
  # Ensure all data is in valueNumber
  PD$Bilirubin$valueNumber <- as.numeric(PD$Bilirubin$valueNumber)
  PD$Lymphocyte$valueNumber <- as.numeric(PD$Lymphocyte$valueNumber)
  PD$`HCO3-std`$valueNumber <- as.numeric(PD$`HCO3-std`$valueNumber)
  PD$`Lactic Acid`$valueNumber <- as.numeric(PD$`Lactic Acid`$valueNumber)
  PD$`pCO2 Arterial`$valueNumber <- as.numeric(PD$`pCO2 Arterial`$valueNumber)
  PD$`pCO2 Venous`$valueNumber <- as.numeric(PD$`pCO2 Venous`$valueNumber)
  PD$pH$valueNumber <- as.numeric(PD$pH$valueNumber)
  PD$COHb$valueNumber <- as.numeric(PD$COHb$valueNumber)
  PD$`Ca++`$valueNumber <- as.numeric(PD$`Ca++`$valueNumber)
  PD$`Invasive Ventilation`$valueNumber
  PD$`C-reactive protein`$valueNumber <- as.numeric(PD$`C-reactive protein`$terseForm)
  PD$Monocytes$valueNumber <- as.numeric(PD$Monocytes$terseForm)
  PD$Glu$valueNumber <- as.numeric(PD$Glu$terseForm)
  PD$Lac$valueNumber <- as.numeric(PD$Lac$terseForm)
  PD$pO2$valueNumber <- as.numeric(PD$pO2$terseForm)
  PD$Ventilation$valueNumber <- PD$Ventilation$terseForm
  PD$`Net Body Balance (LOS)`$valueNumber <- PD$`Net Body Balance (LOS)`$cumTotal
  PD$SurgeryNature$valueNumber <- PD$SurgeryNature$SurgeryNature
  
  # Correct labelling
  names(PD)[names(PD)=="Calcium Adjusted"] <- "Calcium (adjusted)"
  names(PD)[names(PD)=="Prothrombin"] <- "Prothrombin Time"
  names(PD)[names(PD)=="Total protein"] <- "Total Protein"
  names(PD)[names(PD)=="Bilirubin"] <- "Total Bilirubin"
  names(PD)[names(PD)=="pCO2 Arterial"] <- "Arterial PCo2"
  names(PD)[names(PD)=="pCO2 Venous"] <- "Venous PCo2"
  names(PD)[names(PD)=="Lymphocyte"] <- "Lymphocytes"
  }


# Extract all Units of Measure
UoM <- data.frame("var" = character(),"Units" = character(),stringsAsFactors = F);

x <- 1
for(x in 1:length(PD)){
  # Find all units of Measure (should only be one)
  x1 <- unique(PD[[x]]$unitOfMeasure)
  x1 <- paste0(x1,collapse = ", ")
  # Add the variable and the unit of measure to the UoM df
  UoM[nrow(UoM)+1,] <- c(names(PD[x]),x1)
  rm(x,x1)
}
UoM[UoM$Units%like%",",]
UoM[UoM$Units%like%"NA",]

# These have yet to be calculated, add them in
UoM[nrow(UoM)+1,] <- c("Median Respiratory Rate","RPM")
UoM[nrow(UoM)+1,] <- c("Maximum Respiratory Rate","RPM")

# Looking for large amounts of NAs
for(x in 1:length(PD)){
  A <- PD[[x]]$valueNumber
  
  writeLines(text = paste0(names(PD[x])))
  if(names(PD[x])%in%c("Invasive Ventilation","Ventilation","SurgeryNature")){
    writeLines("Data is a  factor")}else{
    print(quantile(A,na.rm = T))
      }
  if(NA%in%A){
    writeLines(text = paste0("!!! NA PRESENT !!!\n",sum(is.na(A))," rows NA, (",round(sum(is.na(A))/nrow(PD[[x]]),4)*100,"%)"))
  }
  writeLines(text = "\n")
  rm(x)
}

dataframe = PD$`Respiratory Rate`
VarName = "Respiratory Rate"
FindMaxMedian = "Maximum"

# This function runs code to take a variable, clean nas, numbers below 0, extract nearest
# data to 8am D1, re-names the columns, and re-names the dataframe.
# For this to work you need to have LoSS available and have set an ExtractionDay
TimepointExtraction <- function(dataframe,VarName,rmlessthan0 = NULL,FindMaxMedian = NULL,SpecificWindow = NULL){
  
  A <- dataframe
  A <- A[!is.na(A$valueNumber),]
  if(is.null(rmlessthan0)){
  A <- subset.data.frame(A, A$valueNumber>0)}
  
  # Join patient data on
  A <- join(LoSS, A, by = "encounterId", type = "inner", match = "all");
  
  # extract number of midnights that have passed from entering the ward
  # DIW: Days in ward
  A$DIW <- as.integer(difftime(as.Date(A$chartTime), as.Date(A$Start_date), units = "days"));
  # determine the hour of the day that the measurement was taken at
  A$DIW2 <- as.integer(strftime(A$chartTime, format="%H"))
  
  # Usually want from admitted until 8am D1.
  if(is.null(SpecificWindow)){
    A <- subset.data.frame(A, A$DIW%in%0:(ExtractionDay-1)|(A$DIW==ExtractionDay & A$DIW2 <= 8))
  }else{
    if(SpecificWindow==1){
      writeLines("Finding data from 4am to 8am")
      # If SpecificWindow is given as an argument, go for extraction day and 4am - 8am
      A <- subset.data.frame(A, A$DIW==ExtractionDay)
      A <- subset.data.frame(A, A$DIW2<=8 & A$DIW2>=4)
    }
    if(SpecificWindow==2){
      writeLines("Finding 8am data specifically")
      A <- A[A$DIW==1,]
      A <- A[A$DIW2==8,]
    }
  }
  
  # Create difference between chartTime and Start_date to find the hours since they
  # were admitted that the event happened
  A$TDiff <- as.numeric(difftime(A$chartTime, A$Start_date, units = "hour"));
  A$TDiff[is.na(A$TDiff)] <- 0;
  # Remove anything with a TDiff of under 0 (event uploaded after they were discharged)
  A <- subset.data.frame(A, TDiff >= 0);
  
  # Find the LoS remaining from each event
  A$LoSR <- as.numeric(A$LoS - A$TDiff);
  # Remove anything with a LoSR of under 0 (event uploaded after they were discharged)
  A <- subset.data.frame(A, A$LoSR>=0);
  
  
  # optional argument = 1, finds max
  if(!is.null(FindMaxMedian)){
    if(FindMaxMedian=="Maximum"){
      A <- as.data.frame(aggregate(A$valueNumber, by=list(A$encounterId), FUN=max))
      colnames(A) <- c("encounterId",paste0(FindMaxMedian," ",VarName))
      }
    
    # optional argument = 2, finds median
    if(FindMaxMedian=="Median"){
      A <- as.data.frame(aggregate(A$valueNumber, by=list(A$encounterId), FUN=median))
      colnames(A) <- c("encounterId",paste0(FindMaxMedian," ",VarName))}
  }

  if(is.null(FindMaxMedian)){
    # Find the most recent piece of information
    A <- A[order(A$encounterId, A$TDiff, decreasing = TRUE),];
    A <- A[!duplicated(A$encounterId),];
    
    # Invasive ventilation needs EtCO2 kept
    if("EtCO2Valid"%in%colnames(A)){
      A <- A[,c("encounterId","valueNumber","LoSR","EtCO2Valid")]
      colnames(A) <- c("encounterId",VarName,paste0(VarName," LoSR"),"EtCO2Valid")  
    }else{
      A <- A[,c("encounterId","valueNumber","LoSR")]
      colnames(A) <- c("encounterId",VarName,paste0(VarName," LoSR"))  
    }
  }
  return(A)
}


# Lab Tests ---------------------------------------------------------------
# Coagulation
VLLabTestCoag <- c("APTT","Prothrombin Time")

# BioChemistry
VLLabTestBioChem <- c("Albumin","Alkaline Phosphate","ALT (SGPT)","Bicarbonate","C-reactive protein",
        "Calcium (adjusted)","Chloride","eGFR","Globulin","Magnesium","Phosphate","Potassium",
        "Sodium","Total Bilirubin","Total Protein","Urea")

# Blood Gases
VLLabTestBloodGas <- c("BEecf","Ca++","COHb","Cl-","Glu","Haematocrit","HCO3-std","K",
                       "Lactic Acid","Na","Arterial PCo2","Venous PCo2","pH")

# Haematology
VLLabTestHaematology <- c("Basophils","Eosinophils","Hct","Haemoglobin","Lymphocytes","MCH","MCHC",
                     "Monocytes","Neutrophils","Platelets","WBC")

# 8am D1 Extraction
# Link all character strings together
VLLabTestAll <- c(VLLabTestCoag,VLLabTestBioChem,VLLabTestBloodGas,VLLabTestHaematology)

# # Check no duplicates
# length(VLLabTestAll) == length(unique(VLLabTestAll))
# sum(duplicated(VLLabTestAll))

# # Check that these exist:
# length(VLLabTestAll)
# x <- 1
# for(x1 in VLLabTestAll){
#   print(x)
#   writeLines(text = paste0(x1," rows: ", nrow(PD[[x1]])))
#   x <- x+1
#   }

PDE <- list()
# Extract 8am D1 data
x <- 1
# BEecf needs to have the rmLessThan0 optional argument set to 0:
#   The valueNumber>0 isn't applicable here.
for(x in VLLabTestAll){
  if(names(PD[x])=="BEecf"){
    PDE[[names(PD[x])]] <- TimepointExtraction(PD[[x]],VarName = names(PD[x]),rmlessthan0 = 0)
  }else{
    PDE[[names(PD[x])]] <- TimepointExtraction(PD[[x]],VarName = names(PD[x]))
  }
}


# Blood Gas Part 2
# PCo2: Default to Arterial, fill in the gaps with Venous
A <- join(PDE$`Arterial PCo2`,PDE$`Venous PCo2`,by="encounterId",type = "full")
A$`Arterial PCo2`[is.na(A$`Arterial PCo2`)] <- A$`Venous PCo2`[is.na(A$`Arterial PCo2`)]
A <- subset.data.frame(A,select=-c(`Venous PCo2`,`Venous PCo2 LoSR`))
colnames(A) = c("encounterId","pCo2","pCo2 LoSR")
PDE$PCo2 <- A
PDE$`Arterial PCo2` <- NULL
PDE$`Venous PCo2` <- NULL

# Remove arterial PCo2 and Venous PCo2 from LabTestHaematology, and add BEecf and PCo2
VLLabTestBloodGas <- VLLabTestBloodGas[!VLLabTestBloodGas %in% c("Arterial PCo2","Venous PCo2")]
VLLabTestBloodGas <- c(VLLabTestBloodGas,"pCo2")

# Lab Tests Summary -------------------------------------------------

# Take LabTestAll String and add in the values that were separately processed
VLLabTestAll <- c(VLLabTestCoag,VLLabTestBioChem,VLLabTestBloodGas,VLLabTestHaematology)

# Get encounterIds to join on
A <- data.frame("encounterId" = LoSS$encounterId)

# Add each dataframe on to our temp df "A"
for(x in 1:length(VLLabTestAll)){
  A <- join(A,PDE[[x]],by="encounterId")
  rm(x)
}
PDELabTests <- A

# Tidy up global env
rm(A,PDE,x1)
rm(list=ls()[ls()%like%"VLLab"])

# Non-Standard Lab Tests -----------------------------------------------------
# Following were previously removed under Ben's guidance: "Anion-Gap","Urate [Uric acid]","Gamma-GT (GGT)"
# Should we remove them again?
VLLabTestNonStd <- c("Fibrinogen (Clauss)","Urate [Uric Acid]","Anion-Gap","Gamma-GT (GGT)")

PDE <- list()
# Extract 8am D1 data
x <- VLLabTestNonStd[1]
for(x in VLLabTestNonStd){
  PDE[[names(PD[x])]] <- TimepointExtraction(PD[[x]],VarName = names(PD[x]))
  }

# Non-Standard Lab Tests Summary ------------------------------------------

# Get encounterIds to join on
A <- data.frame("encounterId" = LoSS$encounterId)

# Add each dataframe on to our temp df "A"
for(x in 1:length(VLLabTestNonStd)){
  A <- join(A,PDE[[x]],by="encounterId")
  rm(x)
}
PDELabTestsNonStd <- A

# Tidy up global env
rm(A,VLLabTestNonStd,PDE)


# Vital Signs -------------------------------------------------------------
PDE <- list()


# Respiratory rate: Highest in last 24 hours, median over last 24 hours, and closest to 8am
# Highest in last 24 hours
PDE$`Respiratory Rate Maximum` <- TimepointExtraction(dataframe = PD$`Respiratory Rate`,
                         VarName = "Respiratory Rate",FindMaxMedian = "Maximum")

# Median in last 24 hours
PDE$`Respiratory Rate Median` <- TimepointExtraction(dataframe = PD$`Respiratory Rate`,
                                                      VarName = "Respiratory Rate",FindMaxMedian = "Median")

# Closest to 8am
PDE$`Respiratory Rate` <- TimepointExtraction(PD$`Respiratory Rate`,VarName = "Respiratory Rate")

# Joining them together
A <- PDE$`Respiratory Rate`
B <- PDE$`Respiratory Rate Median`
C <- PDE$`Respiratory Rate Maximum`
D <- join(A,B,by="encounterId")
D <- join(D,C,by="encounterId")

# Editing respiratory rate losr
D$`Median Respiratory Rate LoSR` <- D$`Respiratory Rate LoSR`
D$`Maximum Respiratory Rate LoSR` <- D$`Respiratory Rate LoSR`

PDE <- list()
PDE$`Respiratory Rate Final` <- D

remove(A,B,C,D)

# PO2/FiO2 ratio:
PDE$`pO2 FiO2 Ratio` <- TimepointExtraction(PD$`pO2 FiO2 Ratio`,VarName = "pO2 FiO2 Ratio")

# Arterial Blood Pressure: Systolic
PDE$`Art BP Systolic` <- 
  TimepointExtraction(PD$`Arterial Blood Pressure Systolic`,VarName = "Art BP Systolic")

# Arterial Blood Pressure: Mean
# Subset to just mean data (attributeIds obtained via TotalIdCombos from Physiological Variables):
# TotalIdCombosShort[TotalIdCombosShort$attributeShortLabel=="Mean",]$attributeId
A <- PD$`Arterial Blood Pressure Mean`
A <- subset.data.frame(A, A$attributeId %in% c(43388, 27356, 43801, 43797, 71, 27264, 10660, 43409, 43381, 11930))
PDE$`Art BP Mean` <- 
  TimepointExtraction(A,VarName = "Art BP Mean")

# Clinical Contact: don't use art bp diastolic or Non-invasive BP

# Central Temperature
PDE$`Temp (Central)` <- TimepointExtraction(PD$`Central Temp`,VarName = "Central Temp")

# Peripheral Temperature
PDE$`Temp (Peripheral)` <- TimepointExtraction(PD$`Central Temp`,VarName = "Peripheral Temp")

# Temperature (Final)
A <- PDE$`Temp (Central)`
B <- PDE$`Temp (Peripheral)`
C <- join(A,B,by="encounterId",type="full")

D <- C[is.na(C$`Central Temp`),]
C <- C[!is.na(C$`Central Temp`),]

D$`Central Temp` <- D$`Peripheral Temp`
D$`Central Temp LoSR` <- D$`Peripheral Temp LoSR`
A <- rbind(C,D)
A <- A[,c("encounterId","Central Temp","Central Temp LoSR")]
PDE$`Temp (Central)` <- NULL
PDE$`Temp (Peripheral)` <- NULL
PDE$`Temperature (Final)` <- A
remove(A,B,C,D)

# Extract the rest of the 8am D1 data
for(x in c("SpO2","Heart Rate","CVP")){
  PDE[[names(PD[x])]] <- TimepointExtraction(PD[[x]],VarName = names(PD[x]))
}



# Vital Signs Summary DF --------------------------------------------------

# Make a string of the colnames you attributed to the Vital Signs
# String of the PA X 8am D1 dfs
VLVitalSignsAll <- c("Respiratory Rate Final","SpO2",
                          "pO2 FiO2 Ratio","Heart Rate",
                          "Art BP Mean", "Art BP Systolic", "CVP", 
                          "Temperature (Final)")

# Get encounterIds to join on
A <- data.frame("encounterId" = LoSS$encounterId)

# Add each dataframe on to our temp df "A"
for(x in VLVitalSignsAll){
  A <- join(A,PDE[[x]],by="encounterId")
  rm(x)
}

PDEVitalSigns <- A

# Tidy up global env
rm(A,PDE,VLVitalSignsAll)

# Ventilation -------------------------------------------------------------

PDE <- list()


# Actual tidal volume, mean airway pressure, and PEEP: give values to those ventilated
# EtCO2 valid as an extra column here
PDE$`Invasive Ventilation` <- TimepointExtraction(dataframe = PD$`Invasive Ventilation`,VarName = "Ventilation")

# Mechanical vs Non-mechanical
# HAS A FOUR HOUR WINDOW. NO DATA = NOT MECHANICALLY VENTILATED.
A <- PD$`Mechanical Ventilation`
colnames(A) <- c("encounterId","valueNumber","chartTime")
A <- TimepointExtraction(dataframe = A,VarName = "Mechanical Ventilation",SpecificWindow = 1)
A <- join(data.frame("encounterId" = LoSS$encounterId),A,by="encounterId")
A$`Mechanical Ventilation`[is.na(A$`Mechanical Ventilation`)] <- "Self Vent"
PDE$`Mechanical Ventilation` <- A

# Actual tidal volume, mean airway pressure, and PEEP attached to non-natural breathers.
# Extract the rest of the 8am D1 data
for(x in c("Actual Tidal Volume","Mean Airway Pressure","PEEP","EtCO2")){
  PDE[[x]] <- TimepointExtraction(PD[[x]],VarName = names(PD[x]))
  }

# Ventilation Summary DF -----------------------------------------------

# Get encounterIds to join on
A <- data.frame("encounterId" = LoSS$encounterId)

# Add each dataframe on to our temp df "A"
for(x in 1:length(PDE)){
  A <- join(A,PDE[[x]],by="encounterId")
  rm(x)
  }

A$Ventilation[is.na(A$Ventilation)] <- "Natural"

# Remove ATV, MAP, and PEEP for patients who aren't ventilated
A1 <- A[A$Ventilation=="Natural",]
A1 <- unique(A1$encounterId[A1$`Mechanical Ventilation`=="Self Vent"])

A$`Actual Tidal Volume`[A$encounterId %in% A1] <- NA
A$`Actual Tidal Volume LoSR`[A$encounterId %in% A1] <- NA
A$`Mean Airway Pressure`[A$encounterId %in% A1] <- NA
A$`Mean Airway Pressure LoSR`[A$encounterId %in% A1] <- NA
A$PEEP[A$encounterId %in% A1] <- NA
A$`PEEP LoSR`[A$encounterId %in% A1] <- NA

# EtCO2 only to those with a tube or thracheostomy
A$EtCO2[A$EtCO2Valid==0] <- NA
A$EtCO2[is.na(A$EtCO2Valid)] <- NA

PDEVentilation <- A

# Tidy up global env
rm(A,A1,PDE)

# Drug Infusions ----------------------------------------------------------

PDE <- list()

# Find the dose (if any) at 8am

# Link all character strings together
VLDrugInfusionsEffects <- c(
  "Labetalol","Glyceryl Trinitrate","Sodium nitroprusside","Propofol",
  "Dexmedetomidine","Clonidine","Noradrenaline","Adrenaline","Dobutamine","Dopamine",
  "Enoximone","Levosimendan","Milrinone","Vasopressin")

# Extract 8am D1 data
for(x in VLDrugInfusionsEffects){
  PDE[[x]] <- TimepointExtraction(dataframe = PD[[x]],VarName = names(PD[x]),SpecificWindow = 2)
  rm(x)
}
PDE$Vasopressin <- TimepointExtraction(dataframe = PD$Vasopressin,VarName = "Vasopressin",SpecificWindow = 2)

# Drug Infusion Summary ---------------------------------------------------

# Get encounterIds to join on
A <- data.frame("encounterId" = LoSS$encounterId)

# Add each dataframe on to our temp df "A"
for(x in VLDrugInfusionsEffects){
  A <- join(A,PDE[[x]],by="encounterId")
}

PDEDrugInfusions <- A

# Tidy up global env
rm(A,VLDrugInfusionsEffects,PDE)

# Output ------------------------------------------------------------------

PDE <- list()

# These are time series data.
# A <- `PA Indwelling Catheter`
# A <- `PA Chest Mediastinal`
# Currently not using
PD$Indwelling <- NULL
PD$`Chest: Mediastinal` <- NULL

A <- subset.data.frame(PD$`Weight (Final)`, select=c("encounterId","valueNumber"))
colnames(A) = c("encounterId","Weight")
A <- A[!duplicated(A$encounterId),]
PDE$Weight <- A

A <- PD$`Net Body Balance (LOS)`
A <- subset.data.frame(A, select=c("encounterId","valueNumber","chartTime"))
A <- TimepointExtraction(dataframe = A,VarName = "Net Body Balance")
PDE$`Net Body Balance (LOS)` <- A

A <- data.frame("encounterId" = LoSS$encounterId)
A <- join(A,PDE$Weight,by="encounterId")
A <- join(A,PDE$`Net Body Balance (LOS)`,by="encounterId")

PDEOutput <- A

# Tidy up global env
rm(A,PDE)

# Haemofiltration ---------------------------------------------------------
PDE <- list()
PDE$Haemofiltration <- TimepointExtraction(PD$Haemofiltration,VarName = "Haemofiltration")

# Ben's Variables ---------------------------------------------------------
# Find if they had IABP in
PDE$IABP <- TimepointExtraction(PD$IABP,VarName = "IABP")

# `PA SurgeryNature` is an attribute that doesn't change
# `PA SurgeryType` is an attribute that doesn't change
# `PA LVEF Pre-Op` is an attribute that doesn't change
# `PA SurgeryNature` is an attribute that doesn't change

A <- data.frame("encounterId" = LoSS$encounterId)

A <- join(A,PD$SurgeryType,by="encounterId")
A <- join(A,PD$SurgeryNature,by="encounterId")
A <- join(A,PD$`LVEF Pre-Op`,by="encounterId")
A <- join(A,PDE$Haemofiltration,by="encounterId")
A <- join(A,PDE$IABP,by="encounterId")

PDEBen <- subset.data.frame(A, select=-chartTime)
PDEBen <- PDEBen[!duplicated(PDEBen$encounterId),]

# Tidy up global env
rm(A,PDE)

# Final Dataframe ---------------------------------------------------------

# Once you have all of them to 8am D1, link on to PA 8AM D1 and run backwards elimination.

# Join Lab Tests, Vital Signs, Ventilation, Drug infusions, Output, and Irregular Lab Tests
# Make into `PA 8am D1` <- A

A <- data.frame("encounterId" = LoSS$encounterId)

A <- join(A,PDELabTests,by="encounterId")
A <- join(A,PDEVitalSigns,by="encounterId")
A <- join(A,PDEVentilation,by="encounterId")
A <- join(A,PDEDrugInfusions,by="encounterId")
A <- join(A,PDEOutput,by="encounterId")
A <- join(A,PDELabTestsNonStd,by="encounterId")
A <- join(A,PDEBen,by="encounterId")

`PA 8am` <- A

rm(A)

# Ensure Vent Pressure data removed from natural breathers ------------------------

# Ensure self-breathers have no data
# Get list of patients who are not ventilated
A1 <- `PA 8am`
B1 <- A1$encounterId[A1$Ventilation == "Natural" & A1$`Mechanical Ventilation` == "Self Vent"]

for(x in c("Actual Tidal Volume","Mean Airway Pressure","PEEP","EtCO2")){
  print(count(A1[[x]]==0))
  A1[[x]][A1$encounterId%in%B1] <- NA
  print(count(A1[[x]]==0))
}


# Adjust LoS per patient for 8am D1 ---------------------------------------

# Calculate the time between first heart rate and 8am D1 per patient
library(lubridate)

A <- join(LoSS,`PA 8am`,type="left",match="all",by="encounterId")
# Subset to LoS data only
A <- A[, c("encounterId","Start_date","LoS",grep("LoSR",colnames(A),value = TRUE))];

# Find longest amount of time to get a reading
A1 <- paste0("A$`",grep("LoSR",colnames(A),value=TRUE),"`",collapse = ", ")
eval(parse(text=paste0("A$HighestLoSR <- round(pmin(",A1,",na.rm=TRUE),2)")))

# Difference in hours between admittance to ward and this reading
A$LoSRTDiff <- round(A$LoS-A$HighestLoSR,0)
A$StartDate2 <- A$Start_date+hours(A$LoSRTDiff)
count(A$StartDate2)
# Now find difference between startDate2 and next 8am D1.
# Calculate time until 8am and if its negative take that number from 24.
A$StartDate28am <- strptime(A$StartDate2,format = "%Y-%m-%d",tz="GMT") + hours(8)
A$TimeUntil8am <- round(as.numeric(difftime(time1 = A$StartDate2,time2 = A$StartDate28am),units="hours"),0)
A$TimeUntil8am[A$TimeUntil8am<0] <- A$TimeUntil8am[A$TimeUntil8am<0]+24
count(A$TimeUntil8am)
A$LoS8amD1 <- A$LoS-A$TimeUntil8am
count(A$LoS8amD1)
A$LoS8amD1[A$LoS8amD1<0] <- A$LoS8amD1[A$LoS8amD1<0]+24

rbind(quantile(A$LoS8amD1,probs = seq(0,1,0.01)),
      quantile(A$LoS,probs = seq(0,1,0.01)))

# Add LoS8amD1 to LoSS
A <- subset.data.frame(A,select=c("encounterId","LoS8amD1"))
A <- join(LoSS,A,by="encounterId")
LoSS <- A

# Then subset LoSRs to other dataframe for storage.
`PA LoSRs` <- `PA 8am`[, c("encounterId",grep("LoSR", colnames(`PA 8am`),value = TRUE))];

# Then remove LoSRs from PA 8am D1
`PA 8am` <- `PA 8am`[, -grep("LoSR", colnames(`PA 8am`))];

# Remove EtCO2Valid
`PA 8am` <- subset.data.frame(`PA 8am`,select=-EtCO2Valid)

# Tidy Up -----------------------------------------------------------------
rm(A,A1,B1,x,TimepointExtraction)
rm(list=ls()[ls()%like%"PDE"])

# Name Appropriately ------------------------------------------------------
eval(parse(text=paste0("`PA 8am D",ExtractionDay,"` <- `PA 8am`")))
eval(parse(text=paste0("`PA LoSRs 8am D",ExtractionDay,"` <- `PA LoSRs`")))
rm(`PA 8am`,`PA LoSRs`)

eval(parse(text=paste0("save.image(\"C:/Users/shillandun/Downloads/Patient Data/Patient Data 8am D",ExtractionDay," Update.RData\")")))
eval(parse(text=paste0("save.image(\"C:/Users/shillandun/Downloads/Patient Data/Patient Data 8am D",ExtractionDay," Update Backup.RData\")")))

