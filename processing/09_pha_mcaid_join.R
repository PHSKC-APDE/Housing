###############################################################################
# OVERVIEW:
# Code to create a cleaned person table from the combined 
# King County Housing Authority and Seattle Housing Authority data sets
# Aim is to have a single row per contiguous time in a house per person
#
# STEPS:
# 01 - Process raw KCHA data and load to SQL database
# 02 - Process raw SHA data and load to SQL database
# 03 - Bring in individual PHA datasets and combine into a single file
# 04 - Deduplicate data and tidy up via matching process
# 05 - Recode race and other demographics
# 06 - Clean up addresses
# 06a - Geocode addresses
# 07 - Consolidate data rows
# 08 - Add in final data elements and set up analyses
# 09 - Join with Medicaid eligibility data ### (THIS CODE) ###
# 10 - Set up joint housing/Medicaid analyses
#
# Alastair Matheson (PHSKC-APDE)
# alastair.matheson@kingcounty.gov
# 2016-08-13, split into separate files 2017-10
# 
###############################################################################


##### Set up global parameter and call in libraries #####
options(max.print = 350, tibble.print_max = 30, scipen = 999)
housing_path <- "//phdata01/DROF_DATA/DOH DATA/Housing/Organized_data"

library(odbc) # Used to connect to SQL server
library(openxlsx) # Used to import/export Excel files
library(glue) # Used to put together SQL queries
library(lubridate) # Used to manipulate dates
library(tidyverse) # Used to manipulate data
library(data.table) # Used to manipulate data
library(RecordLinkage) # used to make the linkage
library(phonics) # used to extract phonetic version of names


##### Connect to the servers #####
db_apde51 <- dbConnect(odbc(), "PH_APDEStore51")
db_claims51 <- dbConnect(odbc(), "PHClaims51")


##### Bring in data #####
### Housing ----
# use stage schema for now but switch to final once QA approach is sorted
pha <- setDT(DBI::dbReadTable(db_apde51, DBI::Id(schema = "stage", table = "pha")))

# convert dates from class==chr to class==Date
pha[, c("dob_m6", "enddate") := lapply(.SD, as.Date), .SDcols = c("dob_m6", "enddate")]

# subset to columns of interest and drop if enddate <2012 (b/c no Mcaid data <2012)
pha <- pha[year(enddate) >= 2012, 
                              .(pid, ssn_id_m6, lname_new_m6, fname_new_m6, mname_new_m6, dob_m6, gender_new_m6, enddate)]

# drop duplicate data
pha <- unique(pha)

# sort data and then keep only the most recent version of the demographic (e.g,, gender, middle name, etc.) 
# method to preserve 1 row of data per person with maximum information
setorder(pha, pid, enddate, na.last = FALSE)
pha <- pha[pha[, .I[.N], by = .(pid)]$V1] 
pha[, enddate := NULL]

# standardize column names
setnames(pha, 
         old = c("pid", "ssn_id_m6", "lname_new_m6", "fname_new_m6", "mname_new_m6", 
                 "dob_m6", "gender_new_m6"),
         new = c("pid", "ssn_new", "lname_new", "fname_new", "mname_new", "dob", "gender_new"))

# split dob data
pha[, ':=' (
  dob = as.Date(dob), dob_y = year(dob), dob_m = month(dob), dob_d = day(dob), id_mcaid = "")]
setcolorder(pha, c("id_mcaid", "ssn_new", "lname_new", "fname_new", "mname_new", 'gender_new', "dob", "dob_y", "dob_m", "dob_d"))

# set gender to integer
pha[, gender_new := as.integer(gender_new)]


### Medicaid ----
### Basic Medicaid eligibility table with link to names (~60 secs)
system.time(mcaid <- 
              setDT(dbGetQuery(db_claims51,
              "SELECT DISTINCT b.id_mcaid, c.lname_m, c.fname_m, c.mname_m, c.ssn_new
              FROM 
                (SELECT a.id_mcaid, max(calmo) as maxdate
                FROM 
                  (SELECT MEDICAID_RECIPIENT_ID AS id_mcaid, CLNDR_YEAR_MNTH as calmo
                  FROM stage.mcaid_elig) a
                  GROUP BY a.id_mcaid) b
                  LEFT JOIN 
                    (SELECT MEDICAID_RECIPIENT_ID AS id_mcaid, 
                    LAST_NAME AS lname_m, FIRST_NAME AS fname_m, 
                    MIDDLE_NAME AS mname_m, SOCIAL_SECURITY_NMBR AS ssn_new,
                    CLNDR_YEAR_MNTH
                    FROM stage.mcaid_elig) c
                  ON b.id_mcaid = c.id_mcaid AND b.maxdate = c.CLNDR_YEAR_MNTH
              ORDER BY b.id_mcaid, c.lname_m, c.fname_m, c.mname_m, c.ssn_new")))

### Processed demographics (~6 secs)
system.time(mcaid_demog <- setDT(dbGetQuery(
  db_claims51, "SELECT id_mcaid, dob, gender_me FROM final.mcaid_elig_demo")))

  # standardize gender
    mcaid_demog[gender_me == "Female", gender_new := 1L]
    mcaid_demog[gender_me == "Male", gender_new := 2L]
    mcaid_demog[gender_me == "Multiple", gender_new := 3L]
    mcaid_demog[, gender_me := NULL]

#### Join Mcaid demographics to Mcaid IDs ####
mcaid <- setDT(merge(mcaid, mcaid_demog, by = "id_mcaid"))
rm(mcaid_demog)
setnames(mcaid, names(mcaid), gsub("_m$", "_new", names(mcaid)))
mcaid[, dob := as.Date(dob)]
mcaid[, ':=' (dob = as.Date(dob), dob_y = year(dob), dob_m = month(dob), dob_d = day(dob))]
mcaid[, pid := NA_character_]
setcolorder(mcaid, names(pha))

# fix error 
combo <- rbind(mcaid, pha, fill = TRUE)


#### Cleaning data for RecordLinkage ####
### Create cleaning function
  cleaning.function <- function(dt){
  # Last names
  dt[, lname_new := gsub("[0-9]", "", lname_new)]  # remove any numbers that may be present in last name
  dt[, lname_new := gsub("'", "", lname_new)]  # remove apostrophes from last name, e.g., O'BRIEN >> OBRIEN
  dt[, lname_new := gsub("\\.", "", lname_new)]  # remove periods from last name, e.g., JONES JR. >> JONES JR
  dt[, lname_new := gsub(" SR$", "", lname_new)]  # remove all " SR" suffixes from last name
  dt[, lname_new := gsub(" JR$", "", lname_new)]  # remove all " JR" suffixes from last name
  dt[, lname_new := gsub("-JR$", "", lname_new)]  # remove all "-JR" suffixes from last name
  dt[, lname_new := gsub("JR$", "", lname_new)]  # remove all "JR" suffixes from last name
  dt[, lname_new := gsub("JR I$", "", lname_new)]  # remove all "JR I" suffixes from last name
  dt[, lname_new := gsub("JR II$", "", lname_new)]  # remove all "JR II" suffixes from last name
  dt[, lname_new := gsub("JR III$", "", lname_new)]  # remove all "JR III" suffixes from last name
  dt[, lname_new := gsub(" II$", "", lname_new)]  # remove all " II" suffixes from last name
  dt[, lname_new := gsub(" III$", "", lname_new)]  # remove all " III" suffixes from last name
  dt[, lname_new := gsub(" IV$", "", lname_new)]  # remove all " IV" suffixes from last name
  #dt[, fname_new := gsub("-", " ", fname_new)]  # commented out but left here to document that this made the matching worse
  
  # First names
  dt[, fname_new := gsub("[0-9]", "", fname_new)]  # remove any numbers that may be present in first names
  dt[, fname_new := gsub("\\.", "", fname_new)]  # remove periods from first name, e.g., JONES JR. >> JONES JR
  dt[fname_new!="JR", fname_new := gsub("JR$", "", fname_new)]  # remove all "JR" suffixes from first name, but keep if it is the full first name
  dt[, fname_new := gsub(" JR$", "", fname_new)]  # remove all " JR" suffixes from first name
  dt[, fname_new := gsub("-JR$", "", fname_new)]  # remove all "-JR" suffixes from first name
  dt[, fname_new := gsub("JR I$", "", fname_new)]  # remove all "JR I" suffixes from first name
  dt[, fname_new := gsub("JR II$", "", fname_new)]  # remove all "JR II" suffixes from first name
  dt[, fname_new := gsub("JR III$", "", fname_new)]  # remove all "JR III" suffixes from first name
  dt[, fname_new := gsub("-", "", fname_new)]  # remove all hyphens from first names because use is inconsistent. Found that replacing with a space was worse
  
  # Middle initials
  dt[, mname_new := gsub("[0-9]", "", mname_new)]  # remove any numbers that may be present in middle initial
  dt[!(grep("[A-Z]", mname_new)), mname_new := NA] # when middle initial is not a letter, replace it with NA
  
  # Ensure SSN are nine digits by adding preceding zeros
  dt[nchar(ssn_new) < 9, ssn_new := str_pad(ssn_new, 9, pad = "0")]
  
  # Not useable if missing SSN & DOB
  #dt <- dt[!(is.na(ssn) & is.na(dob_y) & is.na(dob_m) & is.na(dob_d))]
  
}

### Run cleaning function
  cleaning.function(mcaid)
  cleaning.function(pha)

#### MATCH 0: Perfect / Determinitistic ####
  match0 <- merge(mcaid, pha, by = c("ssn_new", "dob_y", "dob_m", "dob_d", "gender_new", "lname_new", "fname_new", "mname_new"), all=FALSE) 
  match0 <- match0[, .(pid.y, id_mcaid.x)] # keep the paired ids only
  setnames(match0, c("pid.y", "id_mcaid.x"), c("pid", "id_mcaid"))
  
  #removing the perfectly linked data from the Mcaid & PHA datasets because there is no need to perform additional record linkage functions on them
  mcaid <- mcaid[!(id_mcaid %in% match0$id_mcaid)]
  pha <- pha[!(pid %in% match0$pid)]
  
#### MATCH 1: Almost perfect / Determinitistic (ignore middle initial) ####
  match1 <- merge(mcaid, pha, by = c("ssn_new", "dob_y", "dob_m", "dob_d", "gender_new", "lname_new", "fname_new"), all=FALSE) 
  match1 <- match1[, .(pid.y, id_mcaid.x)] # keep the paired ids only
  setnames(match1, c("pid.y", "id_mcaid.x"), c("pid", "id_mcaid"))
  
  #removing the (near) perfectly linked data from the Mcaid & PHA datasets because there is no need to perform additional record linkage functions on them
  mcaid <- mcaid[!(id_mcaid %in% match1$id_mcaid)]
  pha <- pha[!(pid %in% match1$pid)]
  
#### MATCH 2: Probabilistic ####
# Block on SSN, match other vars
match2 <- compare.linkage(pha, mcaid, blockfld = c("ssn_new"),
                strcmp = c("mname_new", "gender_new", "dob_y", "dob_m", "dob_d"),
                phonetic = c("lname_new", "fname_new"), phonfun = soundex,
                exclude = c("dob", "id_mcaid", "pid"))
  
# Using EpiLink approach
match2.weights <- epiWeights(match2)
summary(match2.weights)
  
# browse potential matches to identify a cutpoint
match2.pairs <- setDT(getPairs(match2.weights, single.rows = TRUE))
View(match2.pairs[as.numeric(as.character(Weight)) > 0.45, .(Weight, lname_new.1,lname_new.2, fname_new.1, fname_new.2, mname_new.1, mname_new.2, dob.1, dob.2, gender_new.1, gender_new.2)])

classify2 <- epiClassify(match2.weights, threshold.upper = 0.45)
summary(classify2)
match2 <- getPairs(classify2, single.rows = TRUE)

# select the rows with matches that we trust 
match2 <- setDT(match2 %>%
  filter(
    # Looks like 0.45 is a good cutoff when SSN and DOBs match exactly
    (Weight >= 0.45 & dob.1 == dob.2) |
      # Can use 0.49 when SSN and YOB match
      (Weight >= 0.49 & dob_y.1 == dob_y.2) |
      # When SSN, MOB, and DOB match but YOB is 1-2 years off
      (Weight <= 0.49 & dob_y.1 != dob_y.2 & 
         dob_m.1 == dob_m.2 & dob_d.1 == dob_d.2 &
         abs(dob.1 - dob.2) <= 731)
  )) 

# When an id matched > 1x, keep the row with the higher weight / probability 
match2[, dup.mcaid := 1:.N, by = id_mcaid.2] # already sorted by Weight
match2[, dup.pid := 1:.N, by = pid.1] # already sorted by Weight
match2 <- match2[dup.mcaid==1 & dup.pid == 1] # many if not all of these are a single person with different ids

# keep and standardize id pairs
setnames(match2, c("pid.1", "id_mcaid.2"), c("pid", "id_mcaid"))
match2 <- match2[, c("pid", "id_mcaid")]

# remove pairs just matched from the universe of possible matching data
mcaid <- mcaid[!(id_mcaid %in% match2$id_mcaid)]
pha <- pha[!(pid %in% match2$pid)]


#### MATCH 3 #####
# Block on soundex last name, match other vars
  
# Restrict to PHA-generate IDs to avoid memory issues
pha_merge_id <- pha %>%
  filter(str_detect(ssn_new, "[:alpha:]+"))


match3 <- compare.linkage(pha, mcaid, blockfld = c("lname_new"),
                          strcmp = c("mname_new", "gender_new", "dob_y", "dob_m", "dob_d"),
                          phonetic = c("fname_new"), phonfun = soundex,
                          exclude = c("dob", "ssn_new", "id_mcaid", "pid"))

# Using EpiLink approach
match3.weights <- epiWeights(match3)
summary(match3.weights)

# browse potential matches to identify a cutpoint
match3.pairs <- setDT(getPairs(match3.weights, single.rows = TRUE))
View(match3.pairs[as.numeric(as.character(Weight)) > 0.85, .(Weight, fname_new.1, fname_new.2, mname_new.1, mname_new.2, dob.1, dob.2, gender_new.1, gender_new.2, ssn_new.1, ssn_new.2)])

classify2 <- epiClassify(match3.weights, threshold.upper = 0.85)
summary(classify2)
match3 <- getPairs(classify2, single.rows = TRUE)


# Looks like 0.85 is a good cutoff here, captures 1 twin pair still
# Allow for DOB date/month swaps but otherwise have stricter criteria for DOB differences
match3 <- setDT(match3 %>%
  filter(Weight >= 0.85 & abs(dob.1 - dob.2) <= 30))

# keep and standardize id pairs
setnames(match3, c("pid.1", "id_mcaid.2"), c("pid", "id_mcaid"))
match3 <- match3[, c("pid", "id_mcaid")]

# remove pairs just matched from the universe of possible matching data
mcaid <- mcaid[!(id_mcaid %in% match3$id_mcaid)]
pha <- pha[!(pid %in% match3$pid)]

##### END OF MATCHING #####
#### Combine all ID pairs ####
xwalk <- unique(rbind(match0, match1, match2, match3))

#### Load xwalk table to SQL ####
# create last_run timestamp
xwalk[, last_run := Sys.time()]

# create table ID for SQL
tbl_id <- DBI::Id(schema = "stage", table = "xwalk_linkage_mcaid_pha")  

# Identify the column types to be created in SQL
sql.columns <- c("pid" = "integer", "id_mcaid" = "char(11)", "last_run" = "datetime")  

# ensure column order in R is the same as that in SQL
setcolorder(xwalk, names(sql.columns))

# Write table to SQL
dbWriteTable(db_apde51, 
             tbl_id, 
             value = as.data.frame(xwalk),
             overwrite = T, append = F, 
             field.types = sql.columns)

# Confirm that all rows were loaded to sql
stage.count <- as.numeric(odbc::dbGetQuery(db_apde51, 
                                           "SELECT COUNT (*) FROM stage.xwalk_linkage_mcaid_pha"))
if(stage.count != nrow(xwalk))
  stop("Mismatching row count, error writing or reading data")      

dbDisconnect(xwalk_linkage_mcaid_pha)  

#### Save point ####
#         saveRDS(pha_mcaid_merge, file = file.path(housing_path, "pha_mcaid_01_merged.Rda"))

#### Save point ####
#          saveRDS(pha_mcaid_join, file = file.path(housing_path, "pha_mcaid_02_consolidated.Rda"))
