# HEADER ----
# Author: Danny Colombara (dcolombara [at] kincounty.org), based heavily upon work by Alastair Matheson
# Date: January 2, 2024
# R version: 4.3.1
# Purpose: Creation of a PHA-Claims joint eligibility demographics (`elig_demo`) table 
#          Contains time invariant data. Should be 1 row per unique individual.
#
#          This code should be run from `Housing\etl\db_loader\main_mcaid_mcare_pha_load.r` in order to 
#          load the proper packages.
#
#          The code combines data from 
#          - SHA & KCHA combined ([pha].[final_demo])
#          - Medicaid & Medicare combined ([claims].[final_mcaid_elig_demo])
#          - A crosswalk from the Integrated Data Hub (IDH) ([IDMatch].[IM_HISTORY_TABLE])
#          The result is a single table with the KCMASTER_ID serving as the unique identifier
#
#          *ACHTUNG! 重要! ¡Escucha bien!
#           When Medicare data is available, this code will have to change to use a KCMASTER_ID, Medicaid, 
#           Medicare, id_apde crosswalk table and the id_apde will serve as a unique identifier
# 
# Note: This code include some basic QA that is written to the [claims]. metadatatables. There are no distinct QA scripts. 
#
# Note: This is based on code that was previously embedded in Housing\etl\db_loader\main_mcaid_mcare_pha_load.R
#       It has been moved here because that file was supposed to be a 'control tower' for the process of 
#       combining PHA and claims data


# LOAD YAML ----
  yaml_elig <- paste0(here::here(), "/etl/stage/create_stage_mcaid_mcare_pha_elig_demo.yaml") 
  table_config_demo <- yaml::read_yaml(yaml_elig)
  
# LOAD DATA FROM SQL ----
  message('\U00026A0\U00026A0\n*ACHTUNG! 重要! ¡Escucha bien!\nMake sure the underlying data is up to date!')
  
  ## xwalk: Medicaid-Medicare <> KCMASTER_ID crosswalk ----
    message('This IDH crosswalk table will be replaced by a crosswalk table created \nby the claims-data repo once Medicare data are available.')
    xwalk <- setDT(odbc::dbGetQuery(db_idh, "SELECT DISTINCT KCMASTER_ID, id_mcaid = MEDICAID_ID, updated = [SOURCE_LAST_UPDATED] 
                                          FROM [IDMatch].[IM_HISTORY_TABLE] WHERE MEDICAID_ID IS NOT NULL AND SOURCE_SYSTEM = 'MEDICAID'"))
  
  ## demo.pha: PHA Demographics ----
    demo.pha <- setDT(dbGetQuery(db_hhsaw, 
                                 "SELECT KCMASTER_ID, dob, admit_date_all, gender_me, gender_recent, gender_female, gender_male, 
                                        race_me, race_eth_me, race_recent, race_eth_recent, race_aian, race_asian, race_black, race_latino, 
                                        race_nhpi, race_white, race_unk
                                        FROM pha.final_demo"))
  
  ## demo.mm: Joint Medicaid-Medicare Demographics ----
    demo.mm <- setDT(odbc::dbGetQuery(db_hhsaw, "SELECT * FROM [claims].[final_mcaid_elig_demo]"))
    demo.mm[, c("last_run") := NULL]

# CLEAN / PREP IMPORTED DATA -----
  ## xwalk ----
    xwalk[, updated := as.Date(updated, format = "%m/%d/%Y")]
  
    # keep the most recent copy for each combination of ids
    setorder(xwalk, -updated)  
    xwalk <- xwalk[, .SD[1], by = .(KCMASTER_ID, id_mcaid)]
    
    # keep the most recent linkage when id_mcaid occurs more than once (should not happen, but it does)  
    setorder(xwalk, id_mcaid, -updated)  
    xwalk <- xwalk[, .SD[1], by = .(id_mcaid)] 
    
    # keep the most recent linkage when KCMASTER_ID occures more than once (this is not necessarily an error, but it can cause problems and is rare)
    setorder(xwalk, KCMASTER_ID, -updated)  
    xwalk <- xwalk[, .SD[1], by = .(KCMASTER_ID)] 
    
    xwalk[, updated := NULL]

    # quality checks
    if(identical(nrow(xwalk), uniqueN(xwalk$id_mcaid))){
      message("\U0001f642 The number of rows in the xwalk table equals the number of unique values of id_mcaid")}else{
        warning("\U00026A0 The number of rows in the xwalk table does not equal the number of unique values of id_mcaid")
      }
    
    if(identical(nrow(xwalk), uniqueN(xwalk$KCMASTER_ID))){
      message("\U0001f642 The number of rows in the xwalk table equals the number of unique values of KCMASTER_ID")}else{
        warning("\U00026A0 The number of rows in the xwalk table does not equal the number of unique values of KCMASTER_ID")
      }

  ## demo.pha ----
    # merge on id_mcaid
    demo.pha <- merge(demo.pha, xwalk, by = "KCMASTER_ID", all.x = TRUE, all.y = FALSE)

    # Some PHA IDs linked to the same KCMASTER_ID so need to take remove dups in demo
    demo.pha <- unique(demo.pha)
    setorder(demo.pha, KCMASTER_ID, id_mcaid, na.last = T) # ordered by mcaid id, so not truly random but want to make sure we preferentially select rows with an ID match
    demo.pha <- demo.pha[, .SD[1], by = .(KCMASTER_ID)] # keeep the first row per KCMASTER_ID
    
    setnames(demo.pha, "admit_date_all", "start_housing")
    
    demo.pha[, geo_kc_ever := 1] # PHA data is always 1 because everyone lived or lives in either Seattle or King County Pubic Housing

  ## demo.mm ----
    demo.mm[, dob := as.Date(dob)]

# CREATE MCAID-MCARE-PHA ELIG_DEMO -----
  ## Identify IDs in both Mcaid-Mcare & PHA and split from non-linked IDs ----
    linked.id <- unique(intersect(demo.mm$id_mcaid, demo.pha$id_mcaid))
    
    demo.pha.solo <- demo.pha[!id_mcaid %in% linked.id]
    demo.pha.linked <- demo.pha[id_mcaid %in% linked.id]
    
    demo.mm.solo <- demo.mm[!id_mcaid %in% linked.id]  
    demo.mm.linked <- demo.mm[id_mcaid %in% linked.id]
  
  ## Combine the data for linked IDs ----
    # some data is assumed to be more reliable in one dataset compared to the other
    linked <- merge(x = demo.mm.linked, 
                    y = demo.pha.linked, 
                    by.x = "id_mcaid", 
                    by.y = 'id_mcaid', 
                    all = T)
    setnames(linked, names(linked), gsub("\\.x$", ".demo.mm", names(linked))) # clean up suffixes to eliminate confusion
    setnames(linked, names(linked), gsub("\\.y$", ".demo.pha", names(linked))) # clean up suffixes to eliminate confusion
    
    # loop for vars that default to Mcaid-Mcare data
    for(i in c("dob", "gender_me", "gender_female", "gender_male", "gender_recent", "race_eth_recent", "race_recent",
               "race_me", "race_eth_me", "race_aian", "race_asian", "race_black", "race_nhpi", "race_white", "race_latino")){
      linked[, paste0(i) := get(paste0(i, ".demo.mm"))] # default is to use Mcaid-Mcare data
      linked[is.na(get(paste0(i))), paste0(i) := get(paste0(i, ".demo.pha"))] # If NA b/c missing Mcaid-Mcare data, then fill with PHA data
      linked[, paste0(i, ".demo.mm") := NULL][, paste0(i, ".demo.pha") := NULL]
    }    
    
    # loop for vars that default to PHA data
    if('geo_kc_ever.demo.pha' %in% names(linked)){
      for(i in c("geo_kc_ever")){
        linked[, paste0(i) := get(paste0(i, ".demo.pha"))] # default is to use PHA data
        linked[is.na(get(paste0(i))), paste0(i) := get(paste0(i, ".demo.mm"))] # If NA b/c missing PHA data, then fill with PHA data
        linked[, paste0(i, ".demo.pha") := NULL][, paste0(i, ".demo.mm") := NULL]
      } 
    }
    
    message("Need to update. \nCreating a flag for triple or double linkage is more or less useless now, until we get the Medicare data.")
    linked[, mcaid_mcare_pha := 1] 
    linked[, mcaid_pha := 1]
  
  ## Append the linked to the non-linked ----
    elig <- rbindlist(list(linked, demo.mm.solo, demo.pha.solo), use.names = TRUE, fill = TRUE)
  
  ## Quick logic check ----
    # rows should be equal to unique combinations of KCMASTER_ID and id_mcaid
    elig[, mygroup := .GRP, .(KCMASTER_ID, id_mcaid)]
    if(nrow(elig) == uniqueN(elig$mygroup)){message("\U0001f642 The elig_demo table has a row for the unique combination of ID variables.")}else{
      stop("\n\U1F6D1 The number of rows in elig_demo does not equal the number of unique combinations of ID variables. Fix the error and try again.")}
    elig[, mygroup := NULL]
  
  ## Create flag for linkage types ----
    message("Need to update when have Medicare data. \nCreating a flag for triple or double linkage is more or less useless now, until we get the Medicare data.")
    elig[is.na(mcaid_mcare_pha), mcaid_mcare_pha := 0] # fill in duals flag    
    elig[is.na(mcaid_pha), mcaid_pha := 0] # fill in pha-medicaid flag    
  
  ## Prep for pushing to SQL ----
    # recreate race unknown indicator
    elig[, race_unk := 0]
    if(!"race_asian_pi" %in% names(elig)){elig[, race_asian_pi := 0]} # race_asian_pi is from Medicare. If processing only Medicaid, it would be missing, so fill it with zero so rest of the code works. 
    elig[race_aian==0 & race_asian==0 & race_asian_pi==0 & race_black==0 & race_latino==0 & race_nhpi==0 & race_white==0, race_unk := 1] 
    
    # create time stamp
    elig[, last_run := Sys.time()]  
    
    # order columns
    if(!"apde_dual" %in% names(elig)){elig[, apde_dual := NA]} # apde_dual exists if have both Mcaid and Mcare. If processing only Mcaid, it would be missing, so fill it with NA so rest of the code works. 
    setcolorder(elig, c("KCMASTER_ID", "mcaid_mcare_pha", "apde_dual"))
    
    # Need identifier for when claims data does not link with PHA data
    message("The following will need to be replaced with the creation of a new APDE_ID of some sort since the Medicare data will not have a KCMASTER_ID unless it was linked to Medicaid")
    elig <- merge(elig, xwalk[, .(KCMASTER_ID.fill = KCMASTER_ID, id_mcaid)], by = 'id_mcaid', all.x = T, all.y = F)
    elig[is.na(KCMASTER_ID), KCMASTER_ID := KCMASTER_ID.fill]
    elig[, KCMASTER_ID.fill := NULL]
    elig <- elig[!is.na(KCMASTER_ID)] # will lose about 0.1%. Temporary until get Mcare and need to make my own APDE ID
    
    # clean objects no longer used
    rm(demo.mm, demo.mm.linked, demo.mm.solo, demo.pha, demo.pha.linked, demo.pha.solo, linked, yaml_elig)
  
  
# NORMALIZE PHA VARIABLES (if needed) ----
  message('No variables need to be normalized')

# WRITE ELIG_DEMO TO SQL ----
  # Ensure columns are in same order in R & SQL & that we drop extraneous variables
  keep.elig <- names(table_config_demo$vars)
  varmiss <- setdiff(keep.elig, names(elig))
  if(length(varmiss) > 0){
    for(varmissx in varmiss){
      message("Your elig table was missing ", varmissx, " and it has been added with 100% NAs to push to SQL")
      if(varmissx == 'death_dt'){elig[, paste0(varmissx) := NA_Date_]}else{elig[, paste0(varmissx) := NA_integer_]}
    }
  }
  
  elig <- elig[, ..keep.elig]
  
  # Set up SQL connection
  # db_hhsaw already created above
  
  ## Write table to SQL ----
  message("Loading elig_demo data to SQL")
  chunk_loader(DTx = elig, # R data.frame/data.table
               connx = db_hhsaw, # connection name
               chunk.size = 10000, 
               schemax = table_config_demo$schema, # schema name
               tablex =  table_config_demo$table, # table name
               overwritex = T, # overwrite?
               appendx = F, # append?
               field.typesx = unlist(table_config_demo$vars))   
  
  ## Simple QA ----
    ### confirm that all rows were loaded to SQL ----
    stage.count <- as.numeric(odbc::dbGetQuery(db_hhsaw, "SELECT COUNT (*) FROM claims.stage_mcaid_mcare_pha_elig_demo"))
    if(stage.count != nrow(elig)) {
      stop("Mismatching row count, error writing data")  
    }else{message("\U0001f642 All elig_demo rows were loaded to SQL")}
    
    
    ### check that rows in stage are not less than the last time that it was created ----
    last_run <- as.POSIXct(odbc::dbGetQuery(db_hhsaw, "SELECT MAX (last_run) FROM claims.stage_mcaid_mcare_pha_elig_demo")[[1]]) # data for the run that was just uploaded
    
    # count number of rows
    previous_rows <- as.numeric(
      odbc::dbGetQuery(db_hhsaw, 
                       "SELECT c.qa_value from
                                       (SELECT a.* FROM
                                       (SELECT * FROM claims.metadata_qa_mcaid_mcare_pha_values
                                       WHERE table_name = 'claims.stage_mcaid_mcare_pha_elig_demo' AND
                                       qa_item = 'row_count') a
                                       INNER JOIN
                                       (SELECT MAX(qa_date) AS max_date 
                                       FROM claims.metadata_qa_mcaid_mcare_pha_values
                                       WHERE table_name = 'claims.stage_mcaid_mcare_pha_elig_demo' AND
                                       qa_item = 'row_count') b
                                       ON a.qa_date = b.max_date)c"))
    
    if(is.na(previous_rows)){previous_rows = 0}
    
    row_diff <- stage.count - previous_rows
    
    if (row_diff < 0) {
      temp_qa_dt <- data.table(last_run = last_run, 
                               table_name = 'claims.stage_mcaid_mcare_pha_elig_demo', 
                               qa_item = 'Number new rows compared to most recent run', 
                               qa_result = 'FAIL', 
                               qa_date = Sys.time(), 
                               note = paste0('There were ', row_diff, ' fewer rows in the most recent table (', 
                                             stage.count, ' vs. ', previous_rows, ')'))
      
      problem.demo.row_diff <- glue::glue("Fewer rows than found last time in elig_demo.  
                                                     Check metadata.qa_mcaid_mcare_pha for details (last_run = {last_run})
                                                     \n")
    } else {
      temp_qa_dt <- data.table(last_run = last_run, 
                               table_name = 'claims.stage_mcaid_mcare_pha_elig_demo', 
                               qa_item = 'Number new rows compared to most recent run', 
                               qa_result = 'PASS', 
                               qa_date = Sys.time(), 
                               note = paste0('There were ', row_diff, ' more rows in the most recent table (', 
                                             stage.count, ' vs. ', previous_rows, ')'))
      
      problem.demo.row_diff <- glue::glue(" ") # no problem, so empty error message
      
    }
    
    odbc::dbWriteTable(conn = db_hhsaw, 
                       name =DBI::Id(schema = 'claims', table = 'metadata_qa_mcaid_mcare_pha'), 
                       value = as.data.frame(temp_qa_dt), 
                       append = T, 
                       overwrite = F)
    
    ### check that the number of distinct IDs not less than the last time that it was created ----
    # get count of unique id 
    current.unique.id <- as.numeric(odbc::dbGetQuery(
      db_hhsaw, "SELECT COUNT (DISTINCT KCMASTER_ID) 
                      FROM claims.stage_mcaid_mcare_pha_elig_demo"))
    
    previous.unique.id <- as.numeric(
      odbc::dbGetQuery(db_hhsaw, 
                       "SELECT c.qa_value from
                                       (SELECT a.* FROM
                                       (SELECT * FROM claims.metadata_qa_mcaid_mcare_pha_values
                                       WHERE table_name = 'claims.stage_mcaid_mcare_pha_elig_demo' AND
                                       qa_item = 'id_count') a
                                       INNER JOIN
                                       (SELECT MAX(qa_date) AS max_date 
                                       FROM claims.metadata_qa_mcaid_mcare_pha_values
                                       WHERE table_name = 'claims.stage_mcaid_mcare_pha_elig_demo' AND
                                       qa_item = 'id_count') b
                                       ON a.qa_date = b.max_date)c"))
    
    if(is.na(previous.unique.id)){previous.unique.id = 0}
    
    id_diff <- current.unique.id - previous.unique.id
    
    if (id_diff < 0) {
      temp_qa_dt <- data.table(last_run = last_run, 
                               table_name = 'claims.stage_mcaid_mcare_pha_elig_demo', 
                               qa_item = 'Number distinct IDs compared to most recent run', 
                               qa_result = 'FAIL', 
                               qa_date = Sys.time(), 
                               note = paste0('There were ', id_diff, ' fewer IDs in the most recent table (', 
                                             current.unique.id, ' vs. ', previous.unique.id, ')'))
      
      problem.demo.id_diff <- glue::glue("Fewer unique IDs than found last time in elig_demo.  
                                                     Check metadata.qa_mcaid_mcare_pha for details (last_run = {last_run})
                                                     \n")
    } else {
      temp_qa_dt <- data.table(last_run = last_run, 
                               table_name = 'claims.stage_mcaid_mcare_pha_elig_demo', 
                               qa_item = 'Number distinct IDs compared to most recent run', 
                               qa_result = 'PASS', 
                               qa_date = Sys.time(), 
                               note = paste0('There were ', id_diff, ' more IDs in the most recent table (', 
                                             current.unique.id, ' vs. ', previous.unique.id, ')'))
      
      problem.demo.id_diff <- glue::glue(" ") # no problem, so empty error message
    }
    
    odbc::dbWriteTable(conn = db_hhsaw, 
                       name =DBI::Id(schema = 'claims', table = 'metadata_qa_mcaid_mcare_pha'), 
                       value = as.data.frame(temp_qa_dt), 
                       append = T, 
                       overwrite = F)
    
    ### Fill qa_mcare_values table ----
    temp_qa_values <- data.table(table_name = 'claims.stage_mcaid_mcare_pha_elig_demo', 
                                 qa_item = 'row_count', 
                                 qa_value = as.integer(stage.count), 
                                 qa_date = Sys.time(), 
                                 note = '')
    
    odbc::dbWriteTable(conn = db_hhsaw, 
                       name = DBI::Id(schema = 'claims', table = 'metadata_qa_mcaid_mcare_pha_values'), 
                       value = as.data.frame(temp_qa_values), 
                       append = T, 
                       overwrite = F)
    
    temp_qa_values2 <- data.table(table_name = 'claims.stage_mcaid_mcare_pha_elig_demo', 
                                  qa_item = 'id_count', 
                                  qa_value = as.integer(current.unique.id), 
                                  qa_date = Sys.time(), 
                                  note = '')
    
    odbc::dbWriteTable(conn = db_hhsaw, 
                       name = DBI::Id(schema = 'claims', table = 'metadata_qa_mcaid_mcare_pha_values'), 
                       value = as.data.frame(temp_qa_values2), 
                       append = T, 
                       overwrite = F)
    
    
    
    
# THE END! ----
