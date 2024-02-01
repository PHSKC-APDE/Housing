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
#          The result is a single table with id_apde serving as the unique identifier
#
#          *ACHTUNG! 重要! ¡Escucha bien!
#           When Medicare data is available, this code will have to change to use a Medicaid, 
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
  
  ## xwalk: id_apde <> KCMASTER_ID <> id_mcaid <> id_mcare crosswalk ----
    xwalk <- setDT(odbc::dbGetQuery(db_hhsaw, "SELECT DISTINCT * from [claims].[final_xwalk_apde_mcaid_mcare_pha]"))
  
  ## demo.pha: PHA Demographics ----
    demo.pha <- setDT(dbGetQuery(db_hhsaw, 
                                 "SELECT KCMASTER_ID, id_apde, dob, admit_date_all, gender_me, gender_recent, gender_female, gender_male, 
                                        race_me, race_eth_me, race_recent, race_eth_recent, race_aian, race_asian, race_black, race_latino, 
                                        race_nhpi, race_white, race_unk
                                        FROM pha.final_demo"))
  
  ## demo.mm: Joint Medicaid-Medicare Demographics ----
    demo.mm <- setDT(odbc::dbGetQuery(db_hhsaw, "SELECT * FROM [claims].[final_mcaid_elig_demo]"))
    demo.mm[, c("last_run") := NULL]

# CLEAN / PREP IMPORTED DATA -----
  ## xwalk ----
    xwalk[, updated := as.Date(last_run)][, last_run := NULL]
  
    # keep the most recent copy for each combination of ids
    setorder(xwalk, -updated)  
    xwalk <- xwalk[, .SD[1], by = .(KCMASTER_ID, id_mcaid)]
    
    # keep the most recent linkage when id_mcaid occurs more than once (should not happen, but it does)  
    setorder(xwalk, id_mcaid, -updated)  
    xwalk <- xwalk[!is.na(id_mcaid), .SD[1], by = .(id_mcaid)] 
    
    # keep the most recent linkage when KCMASTER_ID occurs more than once (this is not necessarily an error, but it can cause problems and is rare)
    # setorder(xwalk, KCMASTER_ID, -updated)  
    # xwalk <- xwalk[, .SD[1], by = .(KCMASTER_ID)] 
    message('1/31/2024 - decided with Alastair that >1 id_mcaid that links to KCMASTER_ID is correct, so will have to collapse them in the Mcaid data')
    
    xwalk[, updated := NULL]

    # quality checks
    if(identical(nrow(xwalk), uniqueN(xwalk$id_mcaid))){
      message("\U0001f642 The number of rows in the xwalk table equals the number of unique values of id_mcaid")}else{
        warning("\U00026A0 The number of rows in the xwalk table does not equal the number of unique values of id_mcaid")
      }
    
    if(identical(nrow(xwalk), uniqueN(xwalk$KCMASTER_ID))){
      message("\U0001f642 The number of rows in the xwalk table equals the number of unique values of KCMASTER_ID")}else{
        warning("\U00026A0 The number of rows in the xwalk table does not equal the number of unique values of KCMASTER_ID, this means there are >1 id_mcaid for some KCMASTER_ID")
      }

  ## demo.pha ----
    # merge on id_mcaid
    demo.pha <- merge(demo.pha, xwalk, by = c("KCMASTER_ID", "id_apde"), all.x = TRUE, all.y = FALSE)

    # Some PHA IDs linked to the same KCMASTER_ID so need to take remove dups in demo
    demo.pha <- unique(demo.pha)
    setorder(demo.pha, KCMASTER_ID, id_mcaid, na.last = T) # ordered by mcaid id, so not truly random but want to make sure we preferentially select rows with an ID match
    demo.pha <- demo.pha[, .SD[1], by = .(KCMASTER_ID)] # keeep the first row per KCMASTER_ID
    
    setnames(demo.pha, "admit_date_all", "start_housing")
    
    demo.pha[, geo_kc_ever := 1] # PHA data is always 1 because everyone lived or lives in either Seattle or King County Pubic Housing

  ## demo.mm ----
    demo.mm[, dob := as.Date(dob)]
    
    # collapse demo.mm into one person when IDH identified >1 id_mcaid linked to a single KCMASTER_ID
    mcaidDups <- setorder(xwalk[, dup := .N, .(KCMASTER_ID)][dup != 1, .(KCMASTER_ID, id_mcaid)], -KCMASTER_ID) # identify id_mcaid that link to a common KCMASTER_ID
    demo.mm_orig <- demo.mm[!id_mcaid %in% mcaidDups$id_mcaid] # split off data that has a 1:1 match and leave it alone
    demo.mm_dups <- setorder(merge(demo.mm[id_mcaid %in% mcaidDups$id_mcaid], # split off data that has >1 match with KCMASTER_ID
                                   mcaidDups, 
                                   by = 'id_mcaid', 
                                   all = T), KCMASTER_ID, id_mcaid)
    # Function to prefer non-'Unknown' if available
    selectValue <- function(x) {
      values <- na.omit(x) # Remove NAs
      if (length(values) == 0) {
        return(NA)
      }
      knownValues <- values[values != 'Unknown']
      if (length(knownValues) > 0) {
        return(knownValues[1]) # Return first non-'Unknown'
      } else {
        return(values[1]) # All are 'Unknown', return first 'Unknown'
      }
    }
    
    # Collapse the table to one row per KCMASTER_ID
    demo.mm_dups <- demo.mm_dups[, .(
      id_mcaid = first(na.omit(id_mcaid)), # take the first id_mcaid, which is the most recent because of setorder command above
      dob = first(na.omit(dob)),
      gender_me = selectValue(gender_me),
      gender_recent = selectValue(gender_recent),
      race_me = selectValue(race_me),
      race_eth_me = selectValue(race_eth_me),
      race_recent = selectValue(race_recent),
      race_eth_recent = selectValue(race_eth_recent),
      lang_max = selectValue(lang_max),
      gender_female = as.integer(max(gender_female, na.rm = TRUE)),
      gender_male = as.integer(max(gender_male, na.rm = TRUE)),
      gender_female_t = max(gender_female_t, na.rm = TRUE),
      gender_male_t = max(gender_male_t, na.rm = TRUE),
      race_aian = as.integer(max(race_aian, na.rm = TRUE)),
      race_asian = as.integer(max(race_asian, na.rm = TRUE)),
      race_black = as.integer(max(race_black, na.rm = TRUE)),
      race_latino = as.integer(max(race_latino, na.rm = TRUE)),
      race_nhpi = as.integer(max(race_nhpi, na.rm = TRUE)),
      race_white = as.integer(max(race_white, na.rm = TRUE)),
      race_unk = as.integer(max(race_unk, na.rm = TRUE)),
      race_eth_unk = as.integer(max(race_eth_unk, na.rm = TRUE)),
      race_aian_t = max(race_aian_t, na.rm = TRUE),
      race_asian_t = max(race_asian_t, na.rm = TRUE),
      race_black_t = max(race_black_t, na.rm = TRUE),
      race_latino_t = max(race_latino_t, na.rm = TRUE),
      race_nhpi_t = max(race_nhpi_t, na.rm = TRUE),
      race_white_t = max(race_white_t, na.rm = TRUE),
      lang_amharic = as.integer(max(lang_amharic, na.rm = TRUE)),
      lang_arabic = as.integer(max(lang_arabic, na.rm = TRUE)),
      lang_chinese = as.integer(max(lang_chinese, na.rm = TRUE)),
      lang_korean = as.integer(max(lang_korean, na.rm = TRUE)),
      lang_english = as.integer(max(lang_english, na.rm = TRUE)),
      lang_russian = as.integer(max(lang_russian, na.rm = TRUE)),
      lang_somali = as.integer(max(lang_somali, na.rm = TRUE)),
      lang_spanish = as.integer(max(lang_spanish, na.rm = TRUE)),
      lang_ukrainian = as.integer(max(lang_ukrainian, na.rm = TRUE)),
      lang_vietnamese = as.integer(max(lang_vietnamese, na.rm = TRUE)),
      lang_amharic_t = max(lang_amharic_t, na.rm = TRUE),
      lang_arabic_t = max(lang_arabic_t, na.rm = TRUE),
      lang_chinese_t = max(lang_chinese_t, na.rm = TRUE),
      lang_korean_t = max(lang_korean_t, na.rm = TRUE),
      lang_english_t = max(lang_english_t, na.rm = TRUE),
      lang_russian_t = max(lang_russian_t, na.rm = TRUE),
      lang_somali_t = max(lang_somali_t, na.rm = TRUE),
      lang_spanish_t = max(lang_spanish_t, na.rm = TRUE),
      lang_ukrainian_t = max(lang_ukrainian_t, na.rm = TRUE),
      lang_vietnamese_t = max(lang_vietnamese_t, na.rm = TRUE)
    ), by = .(KCMASTER_ID)][, KCMASTER_ID := NULL]
    
    demo.mm <- rbind(demo.mm_orig, 
                     demo.mm_dups)
    
    
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
    elig <- rbindlist(list(linked, 
                           merge(demo.mm.solo, xwalk[, .(id_mcaid, KCMASTER_ID, id_apde)], by = 'id_mcaid', all.x = T, all.y = F), 
                           demo.pha.solo), 
                      use.names = TRUE, 
                      fill = TRUE)
  
  ## Quick logic check ----
    # rows should be equal to unique combinations of KCMASTER_ID and id_mcaid
    if(nrow(elig) == nrow(unique(elig[, .(KCMASTER_ID, id_apde, id_mcaid)]))){message("\U0001f642 The elig_demo table has a row for the unique combination of ID variables.")}else{
      stop("\n\U1F6D1 The number of rows in elig_demo does not equal the number of unique combinations of ID variables. Fix the error and try again.")}

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
    
    # Need to create id_apde for when claims data (speciically id_mcare) does not have a corresponding KCMASTER_ID
    message("Will need to add the creation of a new APDE_ID of some sort since the Medicare data will not have a KCMASTER_ID unless it was linked to Medicaid")

    # clean objects no longer used
    rm(demo.mm, demo.mm.linked, demo.mm.solo, demo.pha, demo.pha.linked, demo.pha.solo, linked, yaml_elig)
  
  
# NORMALIZE PHA VARIABLES (if needed) ----
  message('death_dt is from Medicare, so it will be missing until Medicare is integrated into this pipeline')
  if(!'death_dt' %in% names(elig)){
    elig[, death_dt := NA_Date_]
  }  

# WRITE ELIG_DEMO TO SQL ----
  # Ensure columns are in same order in R & SQL & that we drop extraneous variables
  keep.elig <- names(table_config_demo$vars)
  elig <- elig[, ..keep.elig]
  rads::validate_yaml_data(DF = elig, YML = table_config_demo, VARS = 'vars')

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
                       "SELECT c.qa_value
                          FROM (
                          	SELECT a.*
                          	FROM (
                          		SELECT *
                          		FROM claims.metadata_qa_mcaid_mcare_pha_values
                          		WHERE table_name = 'claims.stage_mcaid_mcare_pha_elig_demo'
                          			AND qa_item = 'row_count'
                          		) a
                          	INNER JOIN (
                          		SELECT MAX(qa_date) AS max_date
                          		FROM claims.metadata_qa_mcaid_mcare_pha_values
                          		WHERE table_name = 'claims.stage_mcaid_mcare_pha_elig_demo'
                          			AND qa_item = 'row_count'
                          		) b ON a.qa_date = b.max_date
                          	) c"))
    
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
