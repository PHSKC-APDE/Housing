# OVERVIEW:
# Code to create a cleaned waitlist dataset from the SHA and KCHA data with health outcomes
#
# STEPS:
# 01 - Process raw KCHA and SHA data, combine and load to SQL database ### (THIS CODE) ###
# 02 - Join to existing housing data
# 03 - Join to 
#
#
# Alastair Matheson (PHSKC-APDE)
# alastair.matheson@kingcounty.gov
# 2020-12

# BRING IN LIBRARIES AND SET CONNECTIONS ----
library(tidyverse) # Manipulate data
library(odbc) # Read to and write from SQL
library(configr) # Read in YAML files
library(glue) # Safely combine SQL code

db_apde51 <- dbConnect(odbc(), "PH_APDEStore51")


# BRING IN DATA ----
# Waitlist data
waitlist <- DBI::dbGetQuery(db_apde51, "SELECT * FROM stage.pha_waitlist")


# PHA data
pha <- DBI::dbGetQuery(db_apde51, 
                       SELECT * FROM stage.pha_waitlist)
