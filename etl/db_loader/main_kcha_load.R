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
# This script is the main 'control tower' for all the component scripts that load KCHA data.
# Other scripts exist to load SHA data and bring it all together.
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


devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/azure2019/etl/db_loader/etl_log.R")




# SET UP VARIABLES AND CONNECTIONS ----
options(max.print = 350, tibble.print_max = 50, warning.length = 8170,
        scipen = 999)

# Assuming we are working in HHSAW so can pre-load these values
qa_schema <- "pha"
qa_table <- "metadata_qa"
etl_table <- "metadata_etl_log"
file_path_kcha <- "//phdata01/DROF_DATA/DOH DATA/Housing/KCHA/Original_data"

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


# LOAD 2004-2015 DATA ----
# Bring in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/azure2019/etl/raw/load_raw.kcha_2004_2015.R")

# Set up etl_batch_id
etl_batch_id_2015 <- load_metadata_etl_log(conn = db_hhsaw,
                                           to_schema = qa_schema,
                                           to_table = etl_table,
                                           data_source = "kcha",
                                           data_type = "hcv_ph",
                                           date_min = "2004-01-01",
                                           date_max = "2015-12-31",
                                           date_delivery = "2017-03-07",
                                           note = "Initial delivery of data")

# Run function
load_raw.kcha_2004_2015(conn = db_hhsaw,
                        to_schema = "pha",
                        to_table = "raw_kcha_2004_2015",
                        qa_schema = qa_schema,
                        qa_table = qa_table,
                        file_path = file_path_kcha,
                        etl_batch_id = etl_batch_id_2015)


# Clean up
rm(load_raw.kcha_2004_2015, etl_batch_id_2015)


# LOAD 2016 DATA ----
# Bring in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/azure2019/etl/raw/load_raw.kcha_2016.R")

# Set up etl_batch_id
etl_batch_id_2016 <- load_metadata_etl_log(conn = db_hhsaw,
                                           to_schema = qa_schema,
                                           to_table = etl_table,
                                           data_source = "kcha",
                                           data_type = "hcv_ph",
                                           date_min = "2016-01-01",
                                           date_max = "2016-12-31",
                                           date_delivery = "2017-05-04",
                                           note = "Initial delivery of data")

# Run function
load_raw.kcha_2016(conn = db_hhsaw,
                   to_schema = "pha",
                   to_table = "raw_kcha_2016",
                   qa_schema = qa_schema,
                   qa_table = qa_table,
                   file_path = file_path_kcha,
                   etl_batch_id = etl_batch_id_2016)


# Clean up
rm(load_raw.kcha_2016, etl_batch_id_2016)


# LOAD 2017 DATA ----
# Bring in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/azure2019/etl/raw/load_raw.kcha_2017.R")

# Set up etl_batch_id
etl_batch_id_2017 <- load_metadata_etl_log(conn = db_hhsaw,
                                           to_schema = qa_schema,
                                           to_table = etl_table,
                                           data_source = "kcha",
                                           data_type = "hcv_ph",
                                           date_min = "2017-01-01",
                                           date_max = "2017-12-31",
                                           date_delivery = "2018-04-23",
                                           note = "Initial delivery of data")

# Run function
load_raw.kcha_2017(conn = db_hhsaw,
                   to_schema = "pha",
                   to_table = "raw_kcha_2017",
                   qa_schema = qa_schema,
                   qa_table = qa_table,
                   file_path = file_path_kcha,
                   etl_batch_id = etl_batch_id_2017)


# Clean up
rm(load_raw.kcha_2017, etl_batch_id_2017)


# LOAD 2018 DATA ----
# Bring in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/azure2019/etl/raw/load_raw.kcha_2018.R")

# Set up etl_batch_id
etl_batch_id_2018 <- load_metadata_etl_log(conn = db_hhsaw,
                                           to_schema = qa_schema,
                                           to_table = etl_table,
                                           data_source = "kcha",
                                           data_type = "hcv_ph",
                                           date_min = "2018-01-01",
                                           date_max = "2018-12-31",
                                           date_delivery = "2019-03-25",
                                           note = "Initial delivery of data")

# Run function
load_raw.kcha_2018(conn = db_hhsaw,
                   to_schema = "pha",
                   to_table = "raw_kcha_2018",
                   qa_schema = qa_schema,
                   qa_table = qa_table,
                   file_path = file_path_kcha,
                   etl_batch_id = etl_batch_id_2018)


# Clean up
rm(load_raw.kcha_2018, etl_batch_id_2018)


# LOAD 2019 DATA ----
# Bring in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/azure2019/etl/raw/load_raw.kcha_2019.R")

# Set up etl_batch_id
etl_batch_id_2019 <- load_metadata_etl_log(conn = db_hhsaw,
                                           to_schema = qa_schema,
                                           to_table = etl_table,
                                           data_source = "kcha",
                                           data_type = "hcv_ph",
                                           date_min = "2019-01-01",
                                           date_max = "2019-12-31",
                                           date_delivery = "2020-09-10",
                                           note = "Initial delivery of data")

# Run function
load_raw.kcha_2019(conn = db_hhsaw,
                   to_schema = "pha",
                   to_table = "raw_kcha_2019",
                   qa_schema = qa_schema,
                   qa_table = qa_table,
                   file_path = file_path_kcha,
                   etl_batch_id = etl_batch_id_2019)


# Clean up
rm(load_raw.kcha_2019, etl_batch_id_2019)


# LOAD 2020 DATA ----
# Bring in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/azure2020/etl/raw/load_raw.kcha_2020.R")

# Set up etl_batch_id
etl_batch_id_2020 <- load_metadata_etl_log(conn = db_hhsaw,
                                           to_schema = qa_schema,
                                           to_table = etl_table,
                                           data_source = "kcha",
                                           data_type = "hcv_ph",
                                           date_min = "2020-01-01",
                                           date_max = "2020-12-31",
                                           date_delivery = "2021-05-25",
                                           note = "Initial delivery of data")

# Run function
load_raw.kcha_2020(conn = db_hhsaw,
                   to_schema = "pha",
                   to_table = "raw_kcha_2020",
                   qa_schema = qa_schema,
                   qa_table = qa_table,
                   file_path = file_path_kcha,
                   date_min = "2020-01-01",
                   date_max = "2020-12-31",
                   etl_batch_id = etl_batch_id_2020)


# Clean up
rm(load_raw.kcha_2020, etl_batch_id_2020)


# MAKE STAGE KCHA TABLE ----
# Bring in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/azure2019/etl/stage/load_stage.kcha.R")

# Run function
load_stage.kcha(conn = db_hhsaw,
                to_schema = "pha",
                to_table = "stage_kcha",
                from_schema = "pha",
                from_table = "raw_kcha",
                qa_schema = qa_schema,
                qa_table = qa_table,
                years = c(2015:2020),
                truncate = T)

