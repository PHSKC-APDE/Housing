# OVERVIEW ----
# Code to create cleaned tables from the combined King County Housing Authority 
# and Seattle Housing Authority data sets.
# Aim is to have a single row per contiguous time in a house per person
#
# COMPONENTS:
# - Load 'raw' KCHA calendar year files (with some transformation to create a standardized file)
# - Load 'raw' SHA CY files (with some transformation to create a standardized file)
# - [OPTIONAL] Join exit data with KCHA and SHA admin data
# - Combine CY files together for KCHA and SHA separately
# - Combine PHA identities
# - Combine PHA CY files and create demographic and time-varying analytic tables
# - Combine PHA identities with Medicaid and Medicare
# - Combine PHA, Medicaid, and Medicare data and create analytic tables
#
# This script is the main 'control tower' for all the component scripts that load combined PHA data.
# Other scripts exist to load SHA and KCHA data.
#
# Alastair Matheson (PHSKC-APDE)
# alastair.matheson@kingcounty.gov
# 2021-06
# 

# LOAD LIBRARIES AND FUNCTIONS ----
library(data.table) # Manipulate data
library(tidyverse) # Manipulate data
library(odbc) # Read to and write from SQL
library(configr) # Read in YAML files
library(glue) # Safely combine SQL code
library(keyring) # Access stored credentials
library(apde) # Handy functions for working with data in APDE
library(RecordLinkage) # Manage identities across data sources


# SET UP VARIABLES AND CONNECTIONS ----
options(max.print = 350, tibble.print_max = 50, warning.length = 8170,
        scipen = 999)

# Assuming we are working in HHSAW so can pre-load these values
qa_schema <- "pha"
qa_table <- "metadata_qa"
etl_table <- "metadata_etl_log"

# Connect to HHSAW
db_hhsaw <- DBI::dbConnect(odbc::odbc(),
                           driver = "ODBC Driver 17 for SQL Server",
                           server = "tcp:kcitazrhpasqldev20.database.windows.net,1433",
                           database = "hhs_analytics_workspace",
                           uid = keyring::key_list("hhsaw")[["username"]],
                           pwd = keyring::key_get("hhsaw", keyring::key_list("hhsaw")[["username"]]),
                           Encrypt = "yes",
                           TrustServerCertificate = "yes",
                           Authentication = "ActiveDirectoryPassword")


# RUN IDENTITY MATCHING PROCESS ----
## Stage ----


## Final ----
# Manually for now, fix later
if (dbExistsTable(db_hhsaw, DBI::Id(schema = "pha", table = "final_identities"))) {
  stop("Sort out the archiving process")
} else {
  dbExecute(db_hhsaw, "SELECT * INTO pha.final_identities FROM pha.stage_identities")
}



# MAKE DEMO EVER TABLE ----
# Consolidate non- and mostly non-time varying demographics
## Stage ----
# Bring in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/azure2019/etl/stage/load_stage.pha_demo.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/azure2019/etl/stage/qa_stage.pha_demo.R")

# Run function
load_stage_demo(conn = db_hhsaw)

# QA stage table
qa_stage_pha_demo(conn = db_hhsaw, load_only = T)


## Final ----



# MERGE AND CONSOLIDATE KCHA AND PHA DATA ----
## Stage ----


## Final ----


