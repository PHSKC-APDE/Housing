# OVERVIEW:
# Code to create a cleaned waitlist dataset from the SHA and KCHA data
#
# STEPS:
# 01 - Process raw KCHA and SHA  data and load to SQL database
# 02 - Process raw SHA data and load to SQL database ### (THIS CODE) ###
# 03 - Bring in individual PHA data sets and combine into a single file
#
#
# Alastair Matheson (PHSKC-APDE)
# alastair.matheson@kingcounty.gov
# 2020-12

#### BRING IN LIBRARIES AND SET CONNECTIONS ####
library(tidyverse) # Manipulate data
library(odbc) # Read to and write from SQL
library(configr) # Read in YAML files
library(glue) # Safely combine SQL code



#### BRING IN DATA ####
# KCHA

# SHA
sha_waitlist <- read.csv("//phdata01/DROF_DATA/DOH DATA/Housing/SHA/Original_data/2017 HCV Waitlist with Flag for TBV Lease Up.csv")

# Bring in variable name mapping table
fields <- read.csv(text = RCurl::getURL("https://raw.githubusercontent.com/PHSKC-APDE/Housing/master/processing/waitlist_data/waitlist_field_name_mapping.csv"), 
                   header = TRUE, stringsAsFactors = FALSE)


#### PROCESS KCHA DATA ####


#### PROCESS SHA DATA ####