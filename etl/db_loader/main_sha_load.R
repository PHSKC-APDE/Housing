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
# Revised: Danny Colombara, PHSKC-APDE, dcolombara@kingcounty.gov
# 2022-11-08

# CLEAR ENVIRONMENT ----
  rm(list=ls())

# LOAD LIBRARIES AND FUNCTIONS ----
library(rads) # Access handy tools like sql_clean
library(data.table) # Manipulate data
library(tidyverse) # Manipulate data
library(odbc) # Read to and write from SQL
library(configr) # Read in YAML files
library(glue) # Safely combine SQL code
library(keyring) # Access stored credentials
library(kcgeocode) # Easily geocode addresses
library(retry) # has function `wait_until` ... important for waiting for geocoding to complete
# library(apde) # Handy functions for working with data in APDE


devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/main/etl/db_loader/etl_log.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/create_db_connection.R")

# SET UP VARIABLES AND CONNECTIONS ----
options(max.print = 350, tibble.print_max = 50, warning.length = 8170, scipen = 999)

# Assuming we are working in HHSAW so can pre-load these values
qa_schema <- "pha"
qa_table <- "metadata_qa"
etl_table <- "metadata_etl_log"
file_path_sha <- "//phdata01/DROF_DATA/DOH DATA/Housing/SHA/Original_data/"

# Connect to HHSAW
db_hhsaw <- create_db_connection(server = 'hhsaw', interactive = F, prod = T)

# LOAD 2004-2006 PH DATA ----
  sql.2004_2006 <- tryCatch(odbc::dbGetQuery(conn = db_hhsaw, "SELECT TOP(0) * FROM pha.raw_sha_ph_2004_2006"), 
                            error = function(e)
                              print("The [pha].[raw_sha_ph_2004_2006] table does not exist so data will be loaded"))
  
  if(class(sql.2004_2006) != "data.frame"){
    # Bring in function
    devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/main/etl/raw/load_raw_sha_ph_2004_2006.R")
    
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
    load_raw_sha_ph_2004_2006(conn = db_hhsaw,
                              to_schema = "pha",
                              to_table = "raw_sha_ph_2004_2006",
                              qa_schema = qa_schema,
                              qa_table = qa_table,
                              file_path = file_path_sha,
                              etl_batch_id = etl_batch_id_ph_2006)
    # Clean up
    rm(load_raw_sha_ph_2004_2006, etl_batch_id_ph_2006)  
  } else {print("The [pha].[raw_sha_ph_2004_2006] table already exists and will not be reloaded.")}
  rm(sql.2004_2006)

  
# LOAD 2004-2006 HCV DATA ----
  sql.2004_2006 <- tryCatch(odbc::dbGetQuery(conn = db_hhsaw, "SELECT TOP(0) * FROM pha.raw_sha_hcv_2004_2006"), 
                            error = function(e)
                              print("The [pha].[raw_sha_hcv_2004_2006] table does not exist so data will be loaded"))
  
  if(class(sql.2004_2006) != "data.frame"){
    # Bring in function
    devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/main/etl/raw/load_raw_sha_hcv_2004_2006.R")
    
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
    load_raw_sha_hcv_2004_2006(conn = db_hhsaw,
                               to_schema = "pha",
                               to_table = "raw_sha_hcv_2004_2006",
                               qa_schema = qa_schema,
                               qa_table = qa_table,
                               file_path = file_path_sha,
                               etl_batch_id = etl_batch_id_hcv_2006)
    # Clean up
    rm(load_raw_sha_hcv_2004_2006, etl_batch_id_hcv_2006)
    
  } else {print("The [pha].[raw_sha_hcv_2004_2006] table already exists and will not be reloaded.")}
  rm(sql.2004_2006)
  

# LOAD 2006-2017 HCV DATA ----
  sql.2006_2017 <- tryCatch(odbc::dbGetQuery(conn = db_hhsaw, "SELECT TOP(0) * FROM pha.raw_sha_hcv_2006_2017"), 
                            error = function(e)
                              print("The [pha].[raw_sha_hcv_2006_2017] table does not exist so data will be loaded"))
  
  if(class(sql.2006_2017) != "data.frame"){
    # Bring in function
    devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/main/etl/raw/load_raw_sha_hcv_2006_2017.R")
    
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
    load_raw_sha_hcv_2006_2017(conn = db_hhsaw,
                               to_schema = "pha",
                               to_table = "raw_sha_hcv_2006_2017",
                               qa_schema = qa_schema,
                               qa_table = qa_table,
                               file_path = file_path_sha,
                               etl_batch_id = etl_batch_id_hcv_2017)
    
    # Clean up
    rm(load_raw_sha_hcv_2006_2017, etl_batch_id_hcv_2017)
    
  } else {print("The [pha].[raw_sha_hcv_2006_2017] table already exists and will not be reloaded.")}
  rm(sql.2006_2017)

  
# LOAD 2007-2012 PH DATA ----
  sql.2007_2012 <- tryCatch(odbc::dbGetQuery(conn = db_hhsaw, "SELECT TOP(0) * FROM pha.raw_sha_ph_2007_2012"), 
                            error = function(e)
                              print("The [pha].[raw_sha_ph_2007_2012] table does not exist so data will be loaded"))
  
  if(class(sql.2007_2012) != "data.frame"){
    # Bring in function
    devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/main/etl/raw/load_raw_sha_ph_2007_2012.R")
    
    # Set up etl_batch_id
    etl_batch_id_ph_2012 <- load_metadata_etl_log(conn = db_hhsaw,
                                                  to_schema = qa_schema,
                                                  to_table = etl_table,
                                                  data_source = "sha",
                                                  data_type = "ph",
                                                  date_min = "2007-01-01",
                                                  date_max = "2012-09-30",
                                                  date_delivery = "2016-05-11",
                                                  note = "New run with old data because updated field name mapping")
    # Run function
    load_raw_sha_ph_2007_2012(conn = db_hhsaw,
                              to_schema = "pha",
                              to_table = "raw_sha_ph_2007_2012",
                              qa_schema = qa_schema,
                              qa_table = qa_table,
                              file_path = file_path_sha,
                              etl_batch_id = etl_batch_id_ph_2012)
    # Clean up
    rm(load_raw_sha_ph_2007_2012, etl_batch_id_ph_2012)
  } else {print("The [pha].[raw_sha_ph_2007_2012] table already exists and will not be reloaded.")}
  rm(sql.2007_2012)
  
  
# LOAD 2012-2017 PH DATA ----
  sql.2012_2017 <- tryCatch(odbc::dbGetQuery(conn = db_hhsaw, "SELECT TOP(0) * FROM pha.raw_sha_ph_2012_2017"), 
                            error = function(e)
                              print("The [pha].[raw_sha_ph_2012_2017] table does not exist so data will be loaded"))
  
  if(class(sql.2012_2017) != "data.frame"){
    # Bring in function
    devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/main/etl/raw/load_raw_sha_ph_2012_2017.R")
    
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
    load_raw_sha_ph_2012_2017(conn = db_hhsaw,
                              to_schema = "pha",
                              to_table = "raw_sha_ph_2012_2017",
                              qa_schema = qa_schema,
                              qa_table = qa_table,
                              file_path = file_path_sha,
                              etl_batch_id = etl_batch_id_ph_2017)
    # Clean up
    rm(load_raw_sha_ph_2012_2017, etl_batch_id_ph_2017)
    
  } else {print("The [pha].[raw_sha_ph_2012_2017] table already exists and will not be reloaded.")}
  rm(sql.2012_2017)
  
  
# LOAD 2018 HCV DATA ----
  sql.2018 <- tryCatch(odbc::dbGetQuery(conn = db_hhsaw, "SELECT TOP(0) * FROM pha.raw_sha_hcv_2018"), 
                       error = function(e)
                         print("The [pha].[raw_sha_hcv_2018] table does not exist so data will be loaded"))
  
  if(class(sql.2018) != "data.frame"){
    # Bring in function
    devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/main/etl/raw/load_raw_sha_hcv_2018.R")
    
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
    load_raw_sha_hcv_2018(conn = db_hhsaw,
                          to_schema = "pha",
                          to_table = "raw_sha_hcv_2018",
                          qa_schema = qa_schema,
                          qa_table = qa_table,
                          file_path = file_path_sha,
                          etl_batch_id = etl_batch_id_hcv_2018)
    # Clean up
    rm(load_raw_sha_hcv_2018, etl_batch_id_hcv_2018)
    
  } else {print("The [pha].[raw_sha_hcv_2018] table already exists and will not be reloaded.")}
  rm(sql.2018)
  
  
# LOAD 2018 PH DATA ----
  sql.2018 <- tryCatch(odbc::dbGetQuery(conn = db_hhsaw, "SELECT TOP(0) * FROM pha.raw_sha_ph_2018"), 
                       error = function(e)
                         print("The [pha].[raw_sha_ph_2018] table does not exist so data will be loaded"))
  
  if(class(sql.2018) != "data.frame"){
    # Bring in function
    devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/main/etl/raw/load_raw_sha_ph_2018.R")
    
    # Set up etl_batch_id
    etl_batch_id_ph_2018 <- load_metadata_etl_log(conn = db_hhsaw,
                                                  to_schema = qa_schema,
                                                  to_table = etl_table,
                                                  data_source = "sha",
                                                  data_type = "ph",
                                                  date_min = "2018-01-01",
                                                  date_max = "2018-12-31",
                                                  date_delivery = "2019-03-19",
                                                  note = "Initial delivery of data")
    # Run function
    load_raw_sha_ph_2018(conn = db_hhsaw,
                         to_schema = "pha",
                         to_table = "raw_sha_ph_2018",
                         qa_schema = qa_schema,
                         qa_table = qa_table,
                         file_path = file_path_sha,
                         etl_batch_id = etl_batch_id_ph_2018)
    # Clean up
    rm(load_raw_sha_ph_2018, etl_batch_id_ph_2018)
  } else {print("The [pha].[raw_sha_ph_2018] table already exists and will not be reloaded.")}
  rm(sql.2018)
  

# LOAD 2019 HCV AND PH DATA ----
  sql.2019 <- tryCatch(odbc::dbGetQuery(conn = db_hhsaw, "SELECT TOP(0) * FROM pha.raw_sha_2019"), 
                       error = function(e)
                         print("The [pha].[raw_sha_2019] table does not exist so data will be loaded"))
  
  if(class(sql.2019) != "data.frame"){
    # Bring in function
    devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/main/etl/raw/load_raw_sha_2019.R")
    
    # Set up etl_batch_id
    etl_batch_id_2019 <- load_metadata_etl_log(conn = db_hhsaw,
                                               to_schema = qa_schema,
                                               to_table = etl_table,
                                               data_source = "sha",
                                               data_type = "hcv_ph",
                                               date_min = "2019-01-01",
                                               date_max = "2019-12-31",
                                               date_delivery = "2023-02-24",
                                               note = "Refresh/replace 2019 raw data, 2nd try")
    # Run function
    load_raw_sha_2019(conn = db_hhsaw,
                      to_schema = "pha",
                      to_table = "raw_sha_2019",
                      qa_schema = qa_schema,
                      qa_table = qa_table,
                      file_path = file_path_sha,
                      date_min = "2019-01-01",
                      date_max = "2019-12-31",
                      etl_batch_id = etl_batch_id_2019)
    # Clean up
    rm(load_raw_sha_2019, etl_batch_id_2019)
    
  } else {print("The [pha].[raw_sha_2019] table already exists and will not be reloaded.")}
  rm(sql.2019)
  

# LOAD 2020 HCV AND PH DATA ----
  sql.2020 <- tryCatch(odbc::dbGetQuery(conn = db_hhsaw, "SELECT TOP(0) * FROM pha.raw_sha_2020"), 
                       error = function(e)
                         print("The [pha].[raw_sha_2020] table does not exist so data will be loaded"))
  
  if(class(sql.2020) != "data.frame"){
    # Bring in function
    devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/main/etl/raw/load_raw_sha_2020.R")
    
    # Set up etl_batch_id
    etl_batch_id_2020 <- load_metadata_etl_log(conn = db_hhsaw,
                                                  to_schema = qa_schema,
                                                  to_table = etl_table,
                                                  data_source = "sha",
                                                  data_type = "hcv_ph",
                                                  date_min = "2020-01-01",
                                                  date_max = "2020-12-31",
                                                  date_delivery = "2023-02-24",
                                                  note = "Refresh/replace 2020 raw data, 2nd try")
    # Run function
    load_raw_sha_2020(conn = db_hhsaw,
                         to_schema = "pha",
                         to_table = "raw_sha_2020",
                         qa_schema = qa_schema,
                         qa_table = qa_table,
                         file_path = file_path_sha,
                         date_min = "2020-01-01",
                         date_max = "2020-12-31",
                         etl_batch_id = etl_batch_id_2020)
    # Clean up
    rm(load_raw_sha_2020, etl_batch_id_2020)
    
  } else {print("The [pha].[raw_sha_2020] table already exists and will not be reloaded.")}
  rm(sql.2020)

# LOAD 2021 HCV AND PH DATA ----
  sql.2021 <- tryCatch(odbc::dbGetQuery(conn = db_hhsaw, "SELECT TOP(0) * FROM pha.raw_sha_2021"), 
                       error = function(e)
                         print("The [pha].[raw_sha_2021] table does not exist so data will be loaded"))
  
  if(class(sql.2021) != "data.frame"){
    # Bring in function
    devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/main/etl/raw/load_raw_sha_2021.R")
    
    # Set up etl_batch_id
    etl_batch_id_2021 <- load_metadata_etl_log(conn = db_hhsaw,
                                                  to_schema = qa_schema,
                                                  to_table = etl_table,
                                                  data_source = "sha",
                                                  data_type = "hcv_ph",
                                                  date_min = "2021-01-01",
                                                  date_max = "2021-12-31",
                                                  date_delivery = "2023-02-24",
                                                  note = "Refresh/replace 2021 raw data, 2nd try")
    # Run function
    load_raw_sha_2021(conn = db_hhsaw,
                         to_schema = "pha",
                         to_table = "raw_sha_2021",
                         qa_schema = qa_schema,
                         qa_table = qa_table,
                         file_path = file_path_sha,
                         date_min = "2021-01-01",
                         date_max = "2021-12-31",
                         etl_batch_id = etl_batch_id_2021)
    # Clean up
    rm(load_raw_sha_2021, etl_batch_id_2021)
    
  } else {print("The [pha].[raw_sha_2021] table already exists and will not be reloaded.")}
  rm(sql.2021)

# LOAD 2022 HCV AND PH DATA ----
  sql.2022 <- tryCatch(odbc::dbGetQuery(conn = db_hhsaw, "SELECT TOP(0) * FROM pha.raw_sha_2022"), 
                       error = function(e)
                         print("The [pha].[raw_sha_2022] table does not exist so data will be loaded"))
  
  if(class(sql.2022) != "data.frame"){
    # Bring in function
    devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/main/etl/raw/load_raw_sha_2022.R")
    
    # Set up etl_batch_id
    etl_batch_id_2022 <- load_metadata_etl_log(conn = db_hhsaw,
                                               to_schema = qa_schema,
                                               to_table = etl_table,
                                               data_source = "sha",
                                               data_type = "hcv_ph",
                                               date_min = "2022-01-01",
                                               date_max = "2022-12-31",
                                               date_delivery = "2023-02-24",
                                               note = "Inital ETL SHA 2022 raw data")
    # Run function
    load_raw_sha_2022(conn = db_hhsaw,
                      to_schema = "pha",
                      to_table = "raw_sha_2022",
                      qa_schema = qa_schema,
                      qa_table = qa_table,
                      file_path = file_path_sha,
                      date_min = "2022-01-01",
                      date_max = "2022-12-31",
                      etl_batch_id = etl_batch_id_2022)
    # Clean up
    rm(load_raw_sha_2022, etl_batch_id_2022)
    
  } else {print("The [pha].[raw_sha_2022] table already exists and will not be reloaded.")}
  rm(sql.2022)

# MAKE STAGE SHA TABLE ----
  # Bring in function
  devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/main/etl/stage/load_stage_sha.R")
  
  # Run function
  load_stage_sha(conn = db_hhsaw,
                 to_schema = "pha",
                 to_table = "stage_sha",
                 from_schema = "pha",
                 from_table = "raw_sha",
                 qa_schema = qa_schema,
                 qa_table = qa_table,
                 hcv_years = c(2006, 2017, 2018),
                 ph_years = c(2006, 2012, 2017, 2018),
                 hcv_ph_years = c(2019:2022),
                 truncate = T)

