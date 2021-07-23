# OVERVIEW ----
# Code to create cleaned wait list tables from the combined King County Housing Authority 
# and Seattle Housing Authority data sets.
# Aim is to have a single row per contiguous time in a house per person
#
# COMPONENTS:
# - Load KCHA waitlist files direct to stage (with transformation)
# - Load SHA waitlist files direct to stage (with transformation)
# - Combine PHA identities
# - Combine PHA WL files and create demographic and time-varying analytic tables
# - Combine PHA identities with Medicaid
# - Combine PHA and Medicaid data and create analytic tables
#
# This script is the main 'control tower' for all the component scripts that load 
# wait list data.
#
# Alastair Matheson (PHSKC-APDE)
# alastair.matheson@kingcounty.gov
# 2021-07
# 

# LOAD LIBRARIES AND FUNCTIONS ----
library(data.table) # Manipulate data
library(tidyverse) # Manipulate data
library(lubridate) # Work with dates
library(odbc) # Read to and write from SQL
library(configr) # Read in YAML files
library(glue) # Safely combine SQL code
library(keyring) # Access stored credentials
library(apde) # Handy functions for working with data in APDE
library(RecordLinkage) # Manage identities across data sources


# SET UP VARIABLES AND CONNECTIONS ----
options(max.print = 350, tibble.print_max = 50, warning.length = 8170, scipen = 999)

# Assuming we are working in HHSAW so can pre-load these values
qa_schema <- "pha"
qa_table <- "metadata_qa"
etl_table <- "metadata_etl_log"

# Connect to HHSAW
db_hhsaw <- DBI::dbConnect(odbc::odbc(),
                           driver = "ODBC Driver 17 for SQL Server",
                           server = "tcp:kcitazrhpasqlprp16.azds.kingcounty.gov,1433",
                           database = "hhs_analytics_workspace",
                           uid = keyring::key_list("hhsaw")[["username"]],
                           pwd = keyring::key_get("hhsaw", keyring::key_list("hhsaw")[["username"]]),
                           Encrypt = "yes",
                           TrustServerCertificate = "yes",
                           Authentication = "ActiveDirectoryPassword")

# RAW FILES ----
# Bring in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/master/etl/raw/load_raw_kcha_waitlist_2017.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/master/etl/raw/load_raw_sha_waitlist_2017.R")

load_raw_kcha_waitlist_2017(conn = db_hhsaw, to_schema = "pha", to_table = "raw_kcha_waitlist_2017")
load_raw_sha_waitlist_2017(conn = db_hhsaw, to_schema = "pha", to_table = "raw_sha_waitlist_2017")


# COMBINED STAGE TABLE ----
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/master/etl/stage/load_stage_pha_waitlist.R")

load_stage_pha_waitlist(conn = db_hhsaw, to_schema = "pha", to_table = "stage_pha_waitlist")


# RUN IDENTITY MATCHING PROCESS ----
## Stage ----


## Final identities ----
# Manually for now, fix later
if (dbExistsTable(db_hhsaw, DBI::Id(schema = "pha", table = "final_identities"))) {
  # First back up existing archive table
  if (dbExistsTable(db_hhsaw, DBI::Id(schema = "pha", table = "archive_identities"))) {
    dbExecute(db_hhsaw, "EXEC sp_rename 'pha.archive_identities', 'archive_identities_bak'")
  }
  # Then move final table to archive
  dbExecute(db_hhsaw, "SELECT * INTO pha.archive_identities FROM pha.final_identities")
  
  # Check final table is backed up proper
  rows_archive <- as.integer(dbGetQuery(db_hhsaw, "SELECT COUNT (*) AS cnt FROM pha.archive_identities"))
  rows_final <- as.integer(dbGetQuery(db_hhsaw, "SELECT COUNT (*) AS cnt FROM pha.final_identities"))
  
  if (rows_archive != rows_final | rows_final == 0) {
    stop("Something went wrong backing up pha.final_identities")
  } else {
    message("pha.final_identities backed up, deleting archive backup (if it exists)")
    if (dbExistsTable(db_hhsaw, DBI::Id(schema = "pha", table = "archive_identities_bak"))) {
      dbExecute(db_hhsaw, "DROP TABLE pha.archive_identities_bak")
    }
    rm(rows_archive, rows_final)
  }
  
  # Then truncate final in preparation for loading stage
  dbExecute(db_hhsaw, "DROP TABLE pha.final_identities")
}
# Load from stage to final
dbExecute(db_hhsaw, "SELECT * INTO pha.final_identities FROM pha.stage_identities")


## Final identity history ----
# Manually for now, fix later
if (dbExistsTable(db_hhsaw, DBI::Id(schema = "pha", table = "final_identities_history"))) {
  # First back up existing archive table
  if (dbExistsTable(db_hhsaw, DBI::Id(schema = "pha", table = "archive_identities_history"))) {
    dbExecute(db_hhsaw, "EXEC sp_rename 'pha.archive_identities_history', 'archive_identities_history_bak'")
  }
  # Then move final table to archive
  dbExecute(db_hhsaw, "SELECT * INTO pha.archive_identities_history FROM pha.final_identities_history")
  
  # Check final table is backed up proper
  rows_archive <- as.integer(dbGetQuery(db_hhsaw, "SELECT COUNT (*) AS cnt FROM pha.archive_identities_history"))
  rows_final <- as.integer(dbGetQuery(db_hhsaw, "SELECT COUNT (*) AS cnt FROM pha.final_identities_history"))
  
  if (rows_archive != rows_final | rows_final == 0) {
    stop("Something went wrong backing up pha.final_identities_history")
  } else {
    message("pha.final_identities_history backed up, deleting archive backup (if it exists)")
    if (dbExistsTable(db_hhsaw, DBI::Id(schema = "pha", table = "archive_identities_history_bak"))) {
      dbExecute(db_hhsaw, "DROP TABLE pha.archive_identities_history_bak")
    }
    rm(rows_archive, rows_final)
  }
  
  # Then truncate final in preparation for loading stage
  dbExecute(db_hhsaw, "DROP TABLE pha.final_identities_history")
}
# Load from stage to final
dbExecute(db_hhsaw, "SELECT * INTO pha.final_identities_history FROM pha.stage_identities_history")




# MAKE DEMO EVER TABLE ----
# Consolidate non- and mostly non-time varying demographics
## Stage ----
# Bring in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/master/etl/stage/load_stage.pha_demo.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/master/etl/stage/qa_stage.pha_demo.R")

# Run function
load_stage_demo(conn = db_hhsaw)

# QA stage table
qa_stage_pha_demo(conn = db_hhsaw, load_only = T)


## Final ----
# Manually for now, fix later
if (dbExistsTable(db_hhsaw, DBI::Id(schema = "pha", table = "final_demo"))) {
  dbExecute(db_hhsaw, "DROP TABLE pha.final_demo")
}
dbExecute(db_hhsaw, "SELECT * INTO pha.final_demo FROM pha.stage_demo")


# MAKE TIMEVARE TABLE ----
# Consolidate time varying data
## Stage ----
# Bring in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/master/etl/stage/load_stage.pha_timevar.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/master/etl/stage/qa_stage.pha_timevar.R")

# Run function
load_stage_timevar(conn = db_hhsaw)

# QA stage table
qa_stage_pha_timevar(conn = db_hhsaw, load_only = T)


## Final ----
# Manually for now, fix later
if (dbExistsTable(db_hhsaw, DBI::Id(schema = "pha", table = "final_timevar"))) {
  dbExecute(db_hhsaw, "DROP TABLE pha.final_timevar")
}
dbExecute(db_hhsaw, "SELECT * INTO pha.final_timevar FROM pha.stage_timevar")

