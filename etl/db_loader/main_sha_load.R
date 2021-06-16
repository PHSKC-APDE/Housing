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
# This script is the main 'control tower' for all the component scripts that load SHA data.
# Other scripts exist to load KCHA data and bring it all together.
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
file_path_sha <- "//phdata01/DROF_DATA/DOH DATA/Housing/SHA/Original_data"

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


# LOAD 2004-2006 PH DATA ----
# Bring in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/azure2019/etl/raw/load_raw.sha_ph_2004_2006.R")

# Set up etl_batch_id
etl_batch_id_ph_2006 <- load_metadata_etl_log(conn = db_hhsaw,
                                           to_schema = qa_schema,
                                           to_table = etl_table,
                                           data_source = "sha",
                                           data_type = "ph",
                                           date_min = "2004-01-01",
                                           date_max = "2006-12-31",
                                           date_delivery = "2016-05-11",
                                           note = "Initial delivery of data")

# Run function
load_raw.sha_ph_2004_2006(conn = db_hhsaw,
                        to_schema = "pha",
                        to_table = "raw_sha_ph_2004_2006",
                        qa_schema = qa_schema,
                        qa_table = qa_table,
                        file_path = file_path_sha,
                        etl_batch_id = etl_batch_id_ph_2006)


# Clean up
rm(load_raw.sha_ph_2004_2006, etl_batch_id_ph_2006)


# LOAD 2004-2006 HCV DATA ----
# Bring in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/azure2019/etl/raw/load_raw.sha_hcv_2004_2006.R")

# Set up etl_batch_id
etl_batch_id_hcv_2006 <- load_metadata_etl_log(conn = db_hhsaw,
                                               to_schema = qa_schema,
                                               to_table = etl_table,
                                               data_source = "sha",
                                               data_type = "hcv",
                                               date_min = "2004-01-01",
                                               date_max = "2006-12-31",
                                               date_delivery = "2016-05-25",
                                               note = "Initial delivery of data")

# Run function
load_raw.sha_hcv_2004_2006(conn = db_hhsaw,
                           to_schema = "pha",
                           to_table = "raw_sha_hcv_2004_2006",
                           qa_schema = qa_schema,
                           qa_table = qa_table,
                           file_path = file_path_sha,
                           etl_batch_id = etl_batch_id_hcv_2006)


# Clean up
rm(load_raw.sha_hcv_2004_2006, etl_batch_id_hcv_2006)


# LOAD 2006-2017 HCV DATA ----
# Bring in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/azure2019/etl/raw/load_raw.sha_hcv_2006_2017.R")

# Set up etl_batch_id
etl_batch_id_hcv_2017 <- load_metadata_etl_log(conn = db_hhsaw,
                                               to_schema = qa_schema,
                                               to_table = etl_table,
                                               data_source = "sha",
                                               data_type = "hcv",
                                               date_min = "2006-01-01",
                                               date_max = "2017-12-31",
                                               date_delivery = "2018-04-20",
                                               note = "Initial delivery of data, also covers 2006-2016 file delivered 2017-03-31")

# Run function
load_raw.sha_hcv_2006_2017(conn = db_hhsaw,
                           to_schema = "pha",
                           to_table = "raw_sha_hcv_2006_2017",
                           qa_schema = qa_schema,
                           qa_table = qa_table,
                           file_path = file_path_sha,
                           etl_batch_id = etl_batch_id_hcv_2017)

# Clean up
rm(load_raw.sha_hcv_2006_2017, etl_batch_id_hcv_2017)


# LOAD 2007-2012 PH DATA ----
# Bring in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/azure2019/etl/raw/load_raw.sha_ph_2007_2012.R")

# Set up etl_batch_id
etl_batch_id_ph_2012 <- load_metadata_etl_log(conn = db_hhsaw,
                                              to_schema = qa_schema,
                                              to_table = etl_table,
                                              data_source = "sha",
                                              data_type = "ph",
                                              date_min = "2007-01-01",
                                              date_max = "2012-09-30",
                                              date_delivery = "2016-05-11",
                                              note = "Initial delivery of data")

# Run function
load_raw.sha_ph_2007_2012(conn = db_hhsaw,
                          to_schema = "pha",
                          to_table = "raw_sha_ph_2007_2012",
                          qa_schema = qa_schema,
                          qa_table = qa_table,
                          file_path = file_path_sha,
                          etl_batch_id = etl_batch_id_ph_2012)


# Clean up
rm(load_raw.sha_ph_2007_2012, etl_batch_id_ph_2012)



# LOAD 2012-2017 PH DATA ----
# Bring in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/azure2019/etl/raw/load_raw.sha_ph_2012_2017.R")

# Set up etl_batch_id
etl_batch_id_ph_2017 <- load_metadata_etl_log(conn = db_hhsaw,
                                              to_schema = qa_schema,
                                              to_table = etl_table,
                                              data_source = "sha",
                                              data_type = "ph",
                                              date_min = "2012-10-01",
                                              date_max = "2017-12-30",
                                              date_delivery = "2018-04-25",
                                              note = "Initial delivery of data")

# Run function
load_raw.sha_ph_2012_2017(conn = db_hhsaw,
                          to_schema = "pha",
                          to_table = "raw_sha_ph_2012_2017",
                          qa_schema = qa_schema,
                          qa_table = qa_table,
                          file_path = file_path_sha,
                          etl_batch_id = etl_batch_id_ph_2017)


# Clean up
rm(load_raw.sha_ph_2012_2017, etl_batch_id_ph_2012)


# LOAD 2018 HCV DATA ----
# Bring in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/azure2019/etl/raw/load_raw.sha_hcv_2018.R")

# Set up etl_batch_id
etl_batch_id_hcv_2018 <- load_metadata_etl_log(conn = db_hhsaw,
                                               to_schema = qa_schema,
                                               to_table = etl_table,
                                               data_source = "sha",
                                               data_type = "hcv",
                                               date_min = "2018-01-01",
                                               date_max = "2018-12-31",
                                               date_delivery = "2019-03-19",
                                               note = "Initial delivery of data")

# Run function
load_raw.sha_hcv_2018(conn = db_hhsaw,
                      to_schema = "pha",
                      to_table = "raw_sha_hcv_2018",
                      qa_schema = qa_schema,
                      qa_table = qa_table,
                      file_path = file_path_sha,
                      etl_batch_id = etl_batch_id_hcv_2018)


# Clean up
rm(load_raw.sha_hcv_2018, etl_batch_id_hcv_2018)


# LOAD 2018 PH DATA ----
# Bring in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/azure2019/etl/raw/load_raw.sha_ph_2018.R")

# Set up etl_batch_id
etl_batch_id_ph_2018 <- load_metadata_etl_log(conn = db_hhsaw,
                                              to_schema = qa_schema,
                                              to_table = etl_table,
                                              data_source = "sha",
                                              data_type = "ph",
                                              date_min = "2018-10-01",
                                              date_max = "2018-12-30",
                                              date_delivery = "2019-03-19",
                                              note = "Initial delivery of data")

# Run function
load_raw.sha_ph_2018(conn = db_hhsaw,
                          to_schema = "pha",
                          to_table = "raw_sha_ph_2018",
                          qa_schema = qa_schema,
                          qa_table = qa_table,
                          file_path = file_path_sha,
                          etl_batch_id = etl_batch_id_ph_2018)


# Clean up
rm(load_raw.sha_ph_2018, etl_batch_id_ph_2018)


# LOAD 2019 HCV AND PH DATA ----
# Bring in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/azure2019/etl/raw/load_raw.sha_2019.R")

# Set up etl_batch_id
etl_batch_id_2019 <- load_metadata_etl_log(conn = db_hhsaw,
                                           to_schema = qa_schema,
                                           to_table = etl_table,
                                           data_source = "sha",
                                           data_type = "hcv_ph",
                                           date_min = "2019-01-01",
                                           date_max = "2019-12-31",
                                           date_delivery = "2020-11-03",
                                           note = "Initial delivery of data")

# Run function
load_raw.sha_2019(conn = db_hhsaw,
                  to_schema = "pha",
                  to_table = "raw_sha_2019",
                  qa_schema = qa_schema,
                  qa_table = qa_table,
                  file_path = file_path_sha,
                  etl_batch_id = etl_batch_id_2019)


# Clean up
rm(load_raw.sha_2019, etl_batch_id_2019)


# LOAD 2020 PH DATA ----
# Bring in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/azure2019/etl/raw/load_raw.sha_ph_2020.R")

# Set up etl_batch_id
etl_batch_id_ph_2020 <- load_metadata_etl_log(conn = db_hhsaw,
                                              to_schema = qa_schema,
                                              to_table = etl_table,
                                              data_source = "sha",
                                              data_type = "ph",
                                              date_min = "2020-10-01",
                                              date_max = "2020-12-30",
                                              date_delivery = "2021-06-15",
                                              note = "Initial delivery of data")

# Run function
load_raw.sha_ph_2020(conn = db_hhsaw,
                     to_schema = "pha",
                     to_table = "raw_sha_ph_2020",
                     qa_schema = qa_schema,
                     qa_table = qa_table,
                     file_path = file_path_sha,
                     etl_batch_id = etl_batch_id_ph_2020)


# Clean up
rm(load_raw.sha_ph_2020, etl_batch_id_ph_2020)


# LOAD 2020 HCV DATA ----
# Bring in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/azure2019/etl/raw/load_raw.sha_hcv_2020.R")

# Set up etl_batch_id
etl_batch_id_hcv_2020 <- load_metadata_etl_log(conn = db_hhsaw,
                                               to_schema = qa_schema,
                                               to_table = etl_table,
                                               data_source = "sha",
                                               data_type = "hcv",
                                               date_min = "2020-01-01",
                                               date_max = "2020-12-31",
                                               date_delivery = "2021-06-15",
                                               note = "Initial delivery of data")

# Run function
load_raw.sha_hcv_2020(conn = db_hhsaw,
                      to_schema = "pha",
                      to_table = "raw_sha_hcv_2020",
                      qa_schema = qa_schema,
                      qa_table = qa_table,
                      file_path = file_path_sha,
                      etl_batch_id = etl_batch_id_hcv_2020)


# Clean up
rm(load_raw.sha_hcv_2020, etl_batch_id_hcv_2020)



# MAKE STAGE SHA TABLE ----
# Bring in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/azure2019/etl/stage/load_stage.sha.R")

# Run function
load_stage.sha(conn = db_hhsaw,
                to_schema = "pha",
                to_table = "stage_sha",
                from_schema = "pha",
                from_table = "raw_sha",
                qa_schema = qa_schema,
                qa_table = qa_table,
                years = c(2015:2020),
                truncate = T)

