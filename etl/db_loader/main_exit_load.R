# OVERVIEW ----
# Code to create staged tables from the combined King County Housing Authority 
# and Seattle Housing Authority exit data sets.
#
# This script is the main 'control tower' for all the component scripts that load exit data.
#
# Alastair Matheson (PHSKC-APDE)
# alastair.matheson@kingcounty.gov
# 2021-08
# 

# LOAD LIBRARIES AND FUNCTIONS ----
library(data.table) # Manipulate data
library(tidyverse) # Manipulate data
library(odbc) # Read to and write from SQL
library(configr) # Read in YAML files
library(readxl) # Read Excel files
library(glue) # Safely combine SQL code
library(keyring) # Access stored credentials
library(apde) # Handy functions for working with data in APDE


devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/master/etl/db_loader/etl_log.R")


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


# LOAD KCHA EXIT DATA ----
# Bring in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/master/etl/raw/load_raw_kcha_exit.R")

# Set up etl_batch_id
etl_batch_id_kcha_exit <- load_metadata_etl_log(conn = db_hhsaw,
                                              to_schema = qa_schema,
                                              to_table = etl_table,
                                              data_source = "kcha",
                                              data_type = "exit",
                                              date_min = "2015-01-01",
                                              date_max = "2020-12-31",
                                              date_delivery = "2021-05-26",
                                              note = "Initial delivery of data")

# Run function
load_raw_kcha_exit(conn = db_hhsaw,
                   to_schema = "pha",
                   to_table = "raw_kcha_exit",
                   qa_schema = qa_schema,
                   qa_table = qa_table,
                   etl_batch_id = etl_batch_id_kcha_exit)


# Clean up
rm(etl_batch_id_kcha_exit, load_raw_kcha_exit)


# LOAD SHA EXIT DATA ----
# Bring in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/master/etl/raw/load_raw_sha_exit.R")

# Set up etl_batch_id
etl_batch_id_sha_exit <- load_metadata_etl_log(conn = db_hhsaw,
                                                to_schema = qa_schema,
                                                to_table = etl_table,
                                                data_source = "sha",
                                                data_type = "exit",
                                                date_min = "2012-01-01",
                                                date_max = "2020-12-31",
                                                date_delivery = "2021-05-25",
                                                note = "Initial delivery of data")

# Run function
load_raw_sha_exit(conn = db_hhsaw,
                   to_schema = "pha",
                   to_table = "raw_sha_exit",
                   qa_schema = qa_schema,
                   qa_table = qa_table,
                   etl_batch_id = etl_batch_id_sha_exit)


# Clean up
rm(etl_batch_id_sha_exit, load_raw_sha_exit)



# MAKE STAGE COMBINED TABLE ----
# Bring in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/master/etl/stage/load_stage_pha_exit.R")

# Run function
load_stage_pha_exit(conn = db_hhsaw,
                    to_schema = "pha",
                    to_table = "stage_pha_exit",
                    from_schema = "pha",
                    from_table_kcha = "raw_kcha_exit",
                    from_table_sha = "raw_sha_exit",
                    qa_schema = qa_schema,
                    qa_table = qa_table)

