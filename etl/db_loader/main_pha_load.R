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
# Alastair Matheson (PHSKC-APDE), alastair.matheson@kingcounty.gov, 2021-06
# Upated by Danny Colombara (PHSKC-APDE), dcolombara@kingcounty.gov, 2023-08

# LOAD LIBRARIES AND FUNCTIONS ----
library(data.table) # Manipulate data
library(tidyverse) # Manipulate data
library(lubridate) # Work with dates
library(odbc) # Read to and write from SQL
library(configr) # Read in YAML files
library(glue) # Safely combine SQL code
library(keyring) # Access stored credentials
library(housing) # Has some functions specific to the PHA data
# library(apde) # Handy functions for working with data in APDE
library(RecordLinkage) # Manage identities across data sources
library(rads) # primarily for rounding function (https://github.com/PHSKC-APDE/rads)

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


# RUN IDENTITY MATCHING PROCESS ----
  ## Stage ----
    # devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/main/etl/stage/load_stage_pha_identities.R")
    source(file.path(here::here(), "/etl/stage/load_stage_pha_identities.R"))
  
  
  ## Final identities archived and replaced with stage----
      if (dbExistsTable(db_hhsaw, DBI::Id(schema = "pha", table = "final_identities"))) {
        # First back up existing archive table
        if (dbExistsTable(db_hhsaw, DBI::Id(schema = "pha", table = "archive_identities"))) {
          dbExecute(db_hhsaw, "EXEC sp_rename 'pha.archive_identities', 'archive_identities_bak'")
        }
        # Then move final table to archive
        dbExecute(db_hhsaw, "SELECT * INTO pha.archive_identities FROM pha.final_identities")
        
        # Check final table is backed up properly
        rows_archive <- as.integer(dbGetQuery(db_hhsaw, "SELECT COUNT (*) AS cnt FROM pha.archive_identities"))
        rows_final <- as.integer(dbGetQuery(db_hhsaw, "SELECT COUNT (*) AS cnt FROM pha.final_identities"))
        
        if (rows_archive != rows_final | rows_final == 0) {
          stop("\n\U2620 Something went wrong backing up pha.final_identities")
        } else {
          message("pha.final_identities backed up properly, deleting archive backup (if it exists)")
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
    source(file.path(here::here(), "/etl/stage/load_stage_pha_demo.R"))
    source(file.path(here::here(), "/etl/stage/qa_stage_pha_demo.R"))
      
  # Run function
    load_stage_demo(conn = db_hhsaw)
  
  # QA stage table
    qa_stage_pha_demo(conn = db_hhsaw, load_only = F)
  
  
  ## Final ----
  # Manually for now, fix later
  if (dbExistsTable(db_hhsaw, DBI::Id(schema = "pha", table = "final_demo"))) {
    dbExecute(db_hhsaw, "DROP TABLE pha.final_demo")
  }
  dbExecute(db_hhsaw, "SELECT * INTO pha.final_demo FROM pha.stage_demo")
  
  
# MAKE TIMEVAR TABLE ----
  # Consolidate time varying data
  ## Stage ----
    # Bring in functions
    source(file.path(here::here(), "/etl/stage/load_stage_pha_timevar.R"))
    
    source(file.path(here::here(), "/etl/stage/qa_stage_pha_timevar.R"))
    
    # Run function
    load_stage_timevar(conn = db_hhsaw)
    
    # QA stage table
    qa_stage_pha_timevar(conn = db_hhsaw, load_only = F)
  
  
  ## Final ----
    # Manually for now, fix later
    if (dbExistsTable(db_hhsaw, DBI::Id(schema = "pha", table = "final_timevar"))) {
      dbExecute(db_hhsaw, "DROP TABLE pha.final_timevar")
    }
    dbExecute(db_hhsaw, "SELECT * INTO pha.final_timevar FROM pha.stage_timevar")
    
  
# MAKE CALYEAR TABLE ----
# Set up pre-analyzed calendar year tables
  ## Stage ----
  # Bring in functions
    source(file.path(here::here(), "/etl/stage/load_stage_pha_calyear.R"))
    source(file.path(here::here(), "/etl/stage/qa_stage_pha_calyear.R"))

  # Run function
      load_stage_pha_calyear(conn = db_hhsaw, max_year = 2022)

  # QA stage table
      qa_stage_pha_calyear(conn = db_hhsaw, load_only = F)
      
  ## Final ----
    # Manually for now, fix later
    if (dbExistsTable(db_hhsaw, DBI::Id(schema = "pha", table = "final_calyear"))) {
      dbExecute(db_hhsaw, "DROP TABLE pha.final_calyear")
    }
    dbExecute(db_hhsaw, "SELECT * INTO pha.final_calyear FROM pha.stage_calyear")

# The end!
    message("\U0001f642 \U0001f308 \U0001f973")
    