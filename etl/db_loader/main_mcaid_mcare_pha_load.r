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
# This script is the main 'control tower' for scripts that load combined claims/PHA data.
# Other scripts exist to load SHA, KCHA, and combined PHA data.
#
# Alastair Matheson (PHSKC-APDE)
# 2021-06
# 
# Revised by Danny Colombara (dcolombara [at] kingcounty.gov)
# 2024-01

##### !!!URGENT!!! Make sure the underlying data is up to date! #####
# Ensure the master cross-walk table linking Medicaid, Medicare, and Public housing IDs is up to date
# - code 1: https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/phclaims/stage/tables/load_stage.xwalk_apde_mcaid_mcare_pha.r
# - code 2: https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/phclaims/final/tables/load_final.xwalk_apde_mcaid_mcare_pha.sql
# - SQL: [claims].[final].[xwalk_apde_mcaid_mcare_pha]

# Ensure the joint Medicaid-Medicare elig_demo table is up to date
# - code 1: https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/phclaims/stage/tables/load_stage.mcaid_mcare_elig_demo.R
# - code 2: https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/phclaims/final/tables/load_final.mcaid_mcare_elig_demo.sql
# - SQL: [claims].[final].[mcaid_mcare_elig_demo]

# Ensure the joint Medicaid-Medicare elig_timevar table is up to date
# - code 1: https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/phclaims/stage/tables/load_stage.mcaid_mcare_elig_timevar.R
# - code 2: https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/phclaims/final/tables/load_final.mcaid_mcare_elig_timevar.sql
# - SQL: [claims].[final].[mcaid_mcare_elig_timevar]


# SET UP ----
    options(max.print = 350, tibble.print_max = 30, scipen = 999)
    
    library(openxlsx) # Used to import/export Excel files
    library(odbc) # Used to connect to SQL server
    library(glue) # Used to put together SQL queries
    library(lubridate) # Used to manipulate dates
    library(tidyverse) # Used to manipulate data
    library(data.table) # Used to manipulate data
    library(future) # Used for parallel processing to speed bottlenecks
    library(future.apply) # Used for parallel processing to speed bottlenecks
    library(housing) # contains many useful functions for analyzing housing/Medicaid data
    
    source(paste0(here::here(), "/R/chunk_loader.R"))
    source("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/add_index.R")
    
    ## Ask before loading to final schema? (must be T or F)
    ask_first = T

    ## Set up ODBC connections
    db_hhsaw <- rads::validate_hhsaw_key() # connects to Azure 16 HHSAW
    
    db_idh <- DBI::dbConnect(odbc::odbc(), driver = "ODBC Driver 17 for SQL Server", 
                             server = "tcp:kcitazrhpasqlprp16.azds.kingcounty.gov,1433", 
                             database = "inthealth_dwhealth", 
                             uid = keyring::key_list("hhsaw")[["username"]], 
                             pwd = keyring::key_get("hhsaw", keyring::key_list("hhsaw")[["username"]]), 
                             Encrypt = "yes", TrustServerCertificate = "yes", 
                             Authentication = "ActiveDirectoryPassword")

# CREATE DEMO TABLE ----
  ## Stage ----
    # Bring in and run script (will take a long time)
      source(paste0(here::here(), "/etl/stage/load_stage_mcaid_mcare_pha_elig_demo.R"))

  ## Create summary of errors ----
    problems <- glue::glue(
      problem.demo.row_diff, "\n",
      problem.demo.id_diff
    )

    if(problems >1){
      message(glue::glue("\U00026A0\U00026A0\U00026A0\U00026A0\U00026A0\U00026A0\nWARNING!!!\n\U00026A0\U00026A0\U00026A0\U00026A0\U00026A0\U00026A0\n", 
                         ">>> MCAID_MCARE_PHA_ELIG_DEMO FAILED AT LEAST ONE QA TEST", "\n",
                         ">>> Summary of problems in new tables: ", "\n\n", 
                         problems))
    } else {
      message("\U0001f642 Staged MCAID_MCARE_PHA_ELIG_DEMO passed all QA tests")}

  ## Load to final schema ----
    message('Note! If the code seems to have frozen, it is most likely because a pop-up window needs your response. It may be hidden behind your other windows.')
    
    if(ask_first == T){
      load_final_demo <- askYesNo(msg = 'Do you want to update [claims].[final_mcaid_mcare_pha_elig_demo] with data from [claims].[stage_mcaid_mcare_pha_elig_demo]?')
    } else {load_final_demo = T}
    
    if(load_final_demo == T){
      
      # Drop final table if it exists and copy data from stage into final
        if (dbExistsTable(db_hhsaw, DBI::Id(schema = "claims", table = "final_mcaid_mcare_pha_elig_demo"))) {DBI::dbExecute(db_hhsaw, "DROP TABLE [claims].[final_mcaid_mcare_pha_elig_demo]")}
        DBI::dbExecute(conn = db_hhsaw, "SELECT * INTO [claims].[final_mcaid_mcare_pha_elig_demo] FROM [claims].[stage_mcaid_mcare_pha_elig_demo]")
        
      # Add index
        table_config_demo$table <- gsub("stage", "final", table_config_demo$table) # need to update table name in the config file first
        add_index_f(db_hhsaw, table_config = table_config_demo)
    } else {stop("\n\U0001f626\n You choose not to update the [claims].[final_mcaid_mcare_pha_elig_demo]. Fix whatever is is wrong and rerun the 'CREATE DEMO TABLE' portion of this code.")}

  ## Clean up environment ----
    rm(list = setdiff(ls(), c('db_hhsaw', 'db_idh', 'chunk_loader', 'add_index_f')))
    
# CREATE TIMEVAR TABLE ----
  ## Stage ----
    # Bring in and run script (will take a long time)
      source(paste0(here::here(), "/etl/stage/load_stage_mcaid_mcare_pha_elig_timevar.R")) 

  ## Create summary of errors ----
    problems <- glue::glue(
      problem.timevar.row_diff, "\n",
      problem.timevar.id_diff
    )

    if(problems >1){
      message(glue::glue("\U00026A0\U00026A0\U00026A0\U00026A0\U00026A0\U00026A0\nWARNING!!!\n\U00026A0\U00026A0\U00026A0\U00026A0\U00026A0\U00026A0\n", 
                         ">>> MCAID_MCARE_PHA_ELIG_TIMEVAR FAILED AT LEAST ONE QA TEST", "\n",
                         ">>> Summary of problems in new tables: ", "\n\n", 
                         problems))
    } else {
      message("\U0001f642 Staged MCAID_MCARE_PHA_ELIG_TIMEVAR passed all QA tests")}

  ## Load to final schema ----
    message('Note! If the code seems to have frozen, it is most likely because a pop-up window needs your response. It may be hidden behind your other windows.')
    
    if(ask_first == T){
      load_final_timevar <- askYesNo(msg = 'Do you want to update [claims].[final_mcaid_mcare_pha_elig_timevar] with data from [claims].[stage_mcaid_mcare_pha_elig_timevar]?')
    } else {load_final_timevar = T}
    
    if(load_final_timevar == T){
      
      # Drop final table if it exists and copy data from stage into final
        if (dbExistsTable(db_hhsaw, DBI::Id(schema = "claims", table = "final_mcaid_mcare_pha_elig_timevar"))) {DBI::dbExecute(db_hhsaw, "DROP TABLE [claims].[final_mcaid_mcare_pha_elig_timevar]")}
        DBI::dbExecute(conn = db_hhsaw, "SELECT * INTO [claims].[final_mcaid_mcare_pha_elig_timevar] FROM [claims].[stage_mcaid_mcare_pha_elig_timevar]")
        
      # Add index
        table_config_timevar$table <- gsub("stage", "final", table_config_timevar$table) # need to update table name in the config file first
        add_index_f(db_hhsaw, table_config = table_config_timevar)
    } else {stop("\n\U0001f626\n You choose not to update the [claims].[final_mcaid_mcare_pha_elig_timevar]. Fix whatever is is wrong and rerun the 'CREATE TIMEVAR TABLE' portion of this code.")}
    
  ## Clean up environment ----
    rm(list = setdiff(ls(), c('db_hhsaw', 'db_idh', 'chunk_loader', 'add_index_f')))

# CREATE CALYEAR TABLE ----
  ## Stage ----
    # Bring in and run script (will take a long time)
    maxyear = 2022
    source(paste0(here::here(), "/etl/stage/load_stage_mcaid_mcare_pha_elig_calyear.R")) 

  ## Create summary of errors ----
    problems <- glue::glue(
      problem.calyear.row_diff, "\n",
      problem.calyear.row_diff_year, "\n",
      problem.calyear.row_diff_source, "\n",
      problem.calyear.id_diff, "\n",
      problem.calyear.id_diff_year, "\n",
      problem.calyear.id_diff_source    )
    
    if(problems >1){
      message(glue::glue("\U00026A0\U00026A0\U00026A0\U00026A0\U00026A0\U00026A0\nWARNING!!!\n\U00026A0\U00026A0\U00026A0\U00026A0\U00026A0\U00026A0\n", 
                         ">>> MCAID_MCARE_PHA_ELIG_CALYEAR FAILED AT LEAST ONE QA TEST", "\n",
                         ">>> Summary of problems in new tables: ", "\n\n", 
                         problems))
    } else {
      message("\U0001f642 Staged MCAID_MCARE_PHA_ELIG_CALYEAR passed all QA tests")}
    
    
  ## Load to final schema ----
    message('Note! If the code seems to have frozen, it is most likely because a pop-up window needs your response. It may be hidden behind your other windows.')
    
    if(ask_first == T){
      load_final_calyear <- askYesNo(msg = 'Do you want to update [claims].[final_mcaid_mcare_pha_elig_calyear] with data from [claims].[stage_mcaid_mcare_pha_elig_calyear]?')
    } else {load_final_calyear = T}
    
    if(load_final_calyear == T){
      
      # Drop final table if it exists and copy data from stage into final
      if (dbExistsTable(db_hhsaw, DBI::Id(schema = "claims", table = "final_mcaid_mcare_pha_elig_calyear"))) {DBI::dbExecute(db_hhsaw, "DROP TABLE [claims].[final_mcaid_mcare_pha_elig_calyear]")}
      DBI::dbExecute(conn = db_hhsaw, "SELECT * INTO [claims].[final_mcaid_mcare_pha_elig_calyear] FROM [claims].[stage_mcaid_mcare_pha_elig_calyear]")
      
      # Add index
      table_config_calyear$table <- gsub("stage", "final", table_config_calyear$table) # need to update table name in the config file first
      add_index_f(db_hhsaw, table_config = table_config_calyear)
    } else {stop("\n\U0001f626\n You choose not to update the [claims].[final_mcaid_mcare_pha_elig_calyear]. Fix whatever is is wrong and rerun the 'CREATE CALYEAR TABLE' portion of this code.")}
    
  ## Clean up environment ----
    rm(list = setdiff(ls(), c('db_hhsaw', 'db_idh', 'chunk_loader', 'add_index_f')))
    
    