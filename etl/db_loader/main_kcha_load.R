# OVERVIEW ----
# Code to create cleaned tables from King County Housing Authority data. 
# This will later be combined with cleaned Seattle Housing Authority data.
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
# This will load year specific raw data to the [pha] schema on 
# kcitazrhpasqlprp16.azds.kingcounty.gov and create a combined stage table 
# within the same schema
#
# By default, this will only load data if it has not been loaded previously.
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
library(data.table) # Manipulate data
library(tidyverse) # Manipulate data
library(odbc) # Read to and write from SQL
library(configr) # Read in YAML files
library(glue) # Safely combine SQL code
library(keyring) # Access stored credentials
library(kcgeocode) # geocode / clean address data
library(housing) # custom functions
# library(apde) # Handy functions for working with data in APDE


devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/main/etl/db_loader/etl_log.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/create_db_connection.R")

# SET UP VARIABLES AND CONNECTIONS ----
options(max.print = 350, tibble.print_max = 50, warning.length = 8170, scipen = 999)

# Assuming we are working in HHSAW so can pre-load these values
qa_schema <- "pha"
qa_table <- "metadata_qa"
etl_table <- "metadata_etl_log"
file_path_kcha <- "//phdata01/DROF_DATA/DOH DATA/Housing/KCHA/Original_data"

# Connect to HHSAW
db_hhsaw <- create_db_connection(server = 'hhsaw', interactive = F, prod = T)

# LOAD 2004-2015 DATA ----
  sql.2014_2015 <- tryCatch(odbc::dbGetQuery(conn = db_hhsaw, "SELECT TOP(0) * FROM pha.raw_kcha_2004_2015"), 
                               error = function(e)
                                 print("The [pha].[raw_kcha_2004_2015] table does not exist so data will be loaded"))

  if(class(sql.2014_2015) != "data.frame"){
    # Bring in function
    devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/main/etl/raw/load_raw_kcha_2004_2015.R")
    
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
    load_raw_kcha_2004_2015(conn = db_hhsaw,
                            to_schema = "pha",
                            to_table = "raw_kcha_2004_2015",
                            qa_schema = qa_schema,
                            qa_table = qa_table,
                            file_path = file_path_kcha,
                            etl_batch_id = etl_batch_id_2015)
    
    
    # Clean up
    rm(load_raw_kcha_2004_2015, etl_batch_id_2015)
  } else {print("The [pha].[raw_kcha_2004_2015] table already exists and will not be reloaded.")}
  rm(sql.2014_2015)

# LOAD 2016 DATA ----
  sql.2016 <- tryCatch(odbc::dbGetQuery(conn = db_hhsaw, "SELECT TOP(0) * FROM pha.raw_kcha_2016"), 
                            error = function(e)
                              print("The [pha].[raw_kcha_2016] table does not exist so data will be loaded"))

  if(class(sql.2016) != "data.frame"){
    # Bring in function
    devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/main/etl/raw/load_raw_kcha_2016.R")
    
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
    load_raw_kcha_2016(conn = db_hhsaw,
                       to_schema = "pha",
                       to_table = "raw_kcha_2016",
                       qa_schema = qa_schema,
                       qa_table = qa_table,
                       file_path = file_path_kcha,
                       etl_batch_id = etl_batch_id_2016)
    
    
    # Clean up
    rm(load_raw_kcha_2016, etl_batch_id_2016)
    
  } else {print("The [pha].[raw_kcha_2016] table already exists and will not be reloaded.")}
  rm(sql.2016)

# LOAD 2017 DATA ----
  sql.2017 <- tryCatch(odbc::dbGetQuery(conn = db_hhsaw, "SELECT TOP(0) * FROM pha.raw_kcha_2017"), 
                       error = function(e)
                         print("The [pha].[raw_kcha_2017] table does not exist so data will be loaded"))

  if(class(sql.2017) != "data.frame"){
    # Bring in function
    devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/main/etl/raw/load_raw_kcha_2017.R")
    
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
    load_raw_kcha_2017(conn = db_hhsaw,
                       to_schema = "pha",
                       to_table = "raw_kcha_2017",
                       qa_schema = qa_schema,
                       qa_table = qa_table,
                       file_path = file_path_kcha,
                       etl_batch_id = etl_batch_id_2017)
  
    # Clean up
    rm(load_raw_kcha_2017, etl_batch_id_2017)
  
  } else {print("The [pha].[raw_kcha_2017] table already exists and will not be reloaded.")}
  rm(sql.2017)

# LOAD 2018 DATA ----
  sql.2018 <- tryCatch(odbc::dbGetQuery(conn = db_hhsaw, "SELECT TOP(0) * FROM pha.raw_kcha_2018"), 
                       error = function(e)
                         print("The [pha].[raw_kcha_2018] table does not exist so data will be loaded"))
  
  if(class(sql.2018) != "data.frame"){
    # Bring in function
    # devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/main/etl/raw/load_raw_kcha_2018.R")
    source(file.path(here::here(), "/etl/raw/load_raw_kcha_2018.R"))
    
    # Set up etl_batch_id
    etl_batch_id_2018 <- load_metadata_etl_log(conn = db_hhsaw,
                                               to_schema = qa_schema,
                                               to_table = etl_table,
                                               data_source = "kcha",
                                               data_type = "hcv_ph",
                                               date_min = "2018-01-01",
                                               date_max = "2018-12-31",
                                               date_delivery = "2023-03-29",
                                               note = "Complete refresh with fixes from KCHA")
    
    # Run function
    load_raw_kcha_2018(conn = db_hhsaw,
                       to_schema = "pha",
                       to_table = "raw_kcha_2018",
                       qa_schema = qa_schema,
                       qa_table = qa_table,
                       file_path = file_path_kcha,
                       etl_batch_id = etl_batch_id_2018)
    
    
    # Clean up
    rm(load_raw_kcha_2018, etl_batch_id_2018)
    
  } else {print("The [pha].[raw_kcha_2018] table already exists and will not be reloaded.")}
  rm(sql.2018)

# LOAD 2019 DATA ----
  sql.2019 <- tryCatch(odbc::dbGetQuery(conn = db_hhsaw, "SELECT TOP(0) * FROM pha.raw_kcha_2019"), 
                       error = function(e)
                         print("The [pha].[raw_kcha_2019] table does not exist so data will be loaded"))

  if(class(sql.2019) != "data.frame"){
    # Bring in function
    # devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/main/etl/raw/load_raw_kcha_2019.R")
    source(file.path(here::here(), "/etl/raw/load_raw_kcha_2019.R"))
    
    # Set up etl_batch_id
    etl_batch_id_2019 <- load_metadata_etl_log(conn = db_hhsaw,
                                               to_schema = qa_schema,
                                               to_table = etl_table,
                                               data_source = "kcha",
                                               data_type = "hcv_ph",
                                               date_min = "2019-01-01",
                                               date_max = "2019-12-31",
                                               date_delivery = "2023-03-29",
                                               note = "Complete refresh with fixes from KCHA")
    
    # Run function
    load_raw_kcha_2019(conn = db_hhsaw,
                       to_schema = "pha",
                       to_table = "raw_kcha_2019",
                       qa_schema = qa_schema,
                       qa_table = qa_table,
                       file_path = file_path_kcha,
                       etl_batch_id = etl_batch_id_2019)
    
    
    # Clean up
    rm(load_raw_kcha_2019, etl_batch_id_2019)    
  } else {print("The [pha].[raw_kcha_2019] table already exists and will not be reloaded.")}
  rm(sql.2019)

# LOAD 2020 DATA ----
  sql.2020 <- tryCatch(odbc::dbGetQuery(conn = db_hhsaw, "SELECT TOP(0) * FROM pha.raw_kcha_2020"), 
                       error = function(e)
                         print("The [pha].[raw_kcha_2020] table does not exist so data will be loaded"))

  if(class(sql.2020) != "data.frame"){
    # Bring in function
    # devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/main/etl/raw/load_raw_kcha_2020.R")
    source(file.path(here::here(), "/etl/raw/load_raw_kcha_2020.R"))
    
    # Set up etl_batch_id
    etl_batch_id_2020 <- load_metadata_etl_log(conn = db_hhsaw,
                                               to_schema = qa_schema,
                                               to_table = etl_table,
                                               data_source = "kcha",
                                               data_type = "hcv_ph",
                                               date_min = "2020-01-01",
                                               date_max = "2020-12-31",
                                               date_delivery = "2023-03-29",
                                               note = "Complete refresh with fixes from KCHA")
    
    # Run function
    load_raw_kcha_2020(conn = db_hhsaw,
                       to_schema = "pha",
                       to_table = "raw_kcha_2020",
                       qa_schema = qa_schema,
                       qa_table = qa_table,
                       file_path = file_path_kcha,
                       date_min = "2020-01-01",
                       date_max = "2020-12-31",
                       etl_batch_id = etl_batch_id_2020)
    
    # Clean up
    rm(load_raw_kcha_2020, etl_batch_id_2020)  
  } else {print("The [pha].[raw_kcha_2020] table already exists and will not be reloaded.")}
  rm(sql.2020)

# LOAD 2021 DATA ----
  sql.2021 <- tryCatch(odbc::dbGetQuery(conn = db_hhsaw, "SELECT TOP(0) * FROM pha.raw_kcha_2021"), 
                       error = function(e)
                         print("The [pha].[raw_kcha_2021] table does not exist so data will be loaded"))
  
  if(class(sql.2021) != "data.frame"){
    # Bring in function
    # devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/main/etl/raw/load_raw_kcha_2021.R") # commented out because development not on main branch
    source(file.path(here::here(), 'etl', 'raw', 'load_raw_kcha_2021.R'))
    
    # Set up etl_batch_id
    etl_batch_id_2021 <- load_metadata_etl_log(conn = db_hhsaw,
                                               to_schema = qa_schema,
                                               to_table = etl_table,
                                               data_source = "kcha",
                                               data_type = "hcv_ph",
                                               date_min = "2021-01-01",
                                               date_max = "2021-12-31",
                                               date_delivery = "2023-03-29",
                                               note = "Complete refresh with fixes from KCHA")
    
    # Run function
    load_raw_kcha_2021(conn = db_hhsaw,
                       to_schema = "pha",
                       to_table = "raw_kcha_2021",
                       qa_schema = qa_schema,
                       qa_table = qa_table,
                       file_path = file_path_kcha,
                       date_min = "2021-01-01",
                       date_max = "2021-12-31",
                       etl_batch_id = etl_batch_id_2021)
    
    # Clean up
    rm(load_raw_kcha_2021, etl_batch_id_2021)  
  } else {print("The [pha].[raw_kcha_2021] table already exists and will not be reloaded.")}
  rm(sql.2021)

# LOAD 2022 DATA ----
  sql.2022 <- tryCatch(odbc::dbGetQuery(conn = db_hhsaw, "SELECT TOP(0) * FROM pha.raw_kcha_2022"), 
                       error = function(e)
                         print("The [pha].[raw_kcha_2022] table does not exist so data will be loaded"))
  
  if(class(sql.2022) != "data.frame"){
    # Bring in function
    # devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/main/etl/raw/load_raw_kcha_2022.R") # commented out because development not on main branch
    source(file.path(here::here(), 'etl', 'raw', 'load_raw_kcha_2022.R'))
    
    # Set up etl_batch_id
    etl_batch_id_2022 <- load_metadata_etl_log(conn = db_hhsaw,
                                               to_schema = qa_schema,
                                               to_table = etl_table,
                                               data_source = "kcha",
                                               data_type = "hcv_ph",
                                               date_min = "2022-01-01",
                                               date_max = "2022-12-31",
                                               date_delivery = "2023-03-29",
                                               note = "Initial delivery of data")
    
    # Run function
    load_raw_kcha_2022(conn = db_hhsaw,
                       to_schema = "pha",
                       to_table = "raw_kcha_2022",
                       qa_schema = qa_schema,
                       qa_table = qa_table,
                       file_path = file_path_kcha,
                       date_min = "2022-01-01",
                       date_max = "2022-12-31",
                       etl_batch_id = etl_batch_id_2022)
    
    # Clean up
    rm(load_raw_kcha_2022, etl_batch_id_2022)  
  } else {print("The [pha].[raw_kcha_2022] table already exists and will not be reloaded.")}
  rm(sql.2022)

# MAKE STAGE KCHA TABLE ----
  stage.warning <- glue::glue("\U00026A0 
  Do you really want to run this without updating the load_stage_kcha.R file?
  There are probably addresses that need to be geocoded, so you might want to
  run it manually up to that point and wait for the geocoding process to continue.")
  warning(stage.warning)

  proceed <- askYesNo(msg = stage.warning)
  if(isTRUE(proceed)){
      # Bring in function
        devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Housing/main/etl/stage/load_stage_kcha.R")
      
      # Run function
        load_stage_kcha(conn = db_hhsaw,
                        to_schema = "pha",
                        to_table = "stage_kcha",
                        from_schema = "pha",
                        from_table = "raw_kcha",
                        qa_schema = qa_schema,
                        qa_table = qa_table,
                        years = c(2015:2022),
                        truncate = T)
  }else{message("Please run load_stage_kcha.R manually")}

