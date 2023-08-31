#### CODE TO COMBINE KING COUNTY HOUSING AUTHORITY AND SEATTLE HOUSING AUTHORITY IDENTITIES USING IDH----
# Alastair Matheson, PHSKC (APDE), 2021/06
# Major revision by Danny Colombara, PHSKC (APDE), 2023/08
# Now uses linkages identified by the integrated data hub rather than creating our own linkages in house
# 

### Run from main_pha_load script
# https://github.com/PHSKC-APDE/Housing/blob/main/claims_db/etl/db_loader/main_pha_load.R
# Assumes relevant libraries are already loaded


# db_hhsaw = ODBC db_hhsaw connection to use (change to conn if this becomes a function)
# to_schema = name of the schema to load data to
# to_table = name of the table to load data to
# from_schema = name of the schema the input data are in
# from_table = common prefix of the table the input data are in
# qa_schema = name of the schema the QA lives in (likely the same as the to_schema if working in HHSAW)
# qa_table = name of the table that holds QA outcomes

  
# SET ARGUMENTS ----
    qa_schema <- "pha"
    qa_table <- "metadata_qa"
    from_schema <- "pha"
    from_table <- "stage_"
    to_schema <- "pha"
    to_table <- "stage_identities"
    final_schema <- "pha"
    final_table <- "final_identities"
    
# IMPORT IN DATA ----
    # Pull ID list from IDH so we do not have to replicate the matches----
      idh.ids <- setDT(
        odbc::dbGetQuery(conn = db_idh, 
                         statement = "SELECT DISTINCT KCMASTER_ID, id_hash = PHOUSING_ID, 
                                      ssn = SSN, lname = LAST_NAME_ORIG, 
                                      fname = FIRST_NAME_ORIG, mname = MIDDLE_NAME_ORIG,
                                      dob = DOB, GENDER
                                      FROM [inthealth_dwhealth].[IDMatch].[IM_HISTORY_TABLE]
                                      WHERE SOURCE_SYSTEM = 'PHA_CLIENT'"))
      idh.ids[, female := fcase(GENDER == 'F', 1, 
                                GENDER == 'M', 0)][, GENDER := NULL]
    
    # Pull IDs from KCHA & SHA stage tables ----
        kcha.ids <- setDT(
          odbc::dbGetQuery(conn = db_hhsaw, 
                           statement = paste0("SELECT DISTINCT kcha = agency, id_hash, kcha_id = pha_id
                                              FROM [", from_schema, "].[", from_table, "kcha]")
          ))
        
        sha.ids <-  setDT(
          odbc::dbGetQuery(conn = db_hhsaw, 
                           statement = paste0("SELECT DISTINCT sha = agency, id_hash, sha_id = pha_id
                                              FROM [", from_schema, "].[", from_table,"sha]")
          ))
        
    # Pull IDs from Waitlist and Exit data (to be sure all PHA data in IDH is accounted for) ----
        wait.ids <- setDT(
          odbc::dbGetQuery(conn = db_hhsaw, 
                           statement = paste0("SELECT DISTINCT wait = 'WAIT', id_hash FROM [", from_schema, "].[", from_table,"pha_waitlist]")
          ))
        
        exit.ids <- setDT(
          odbc::dbGetQuery(conn = db_hhsaw, 
                           statement = paste0("SELECT * FROM [", from_schema, "].[", from_table,"pha_exit]"))
        )
        exit.ids[, female := fcase(hh_gender == 'Female', 1, 
                                   hh_gender == 'Male', 0)]
        # need to create id_hash for exits
        exit.ids[, id_hash := as.character(toupper(openssl::sha256(paste(stringr::str_replace_na(ssn, ''),
                                                                         stringr::str_replace_na(pha_id, ''),
                                                                         stringr::str_replace_na(lname, ''),
                                                                         stringr::str_replace_na(fname, ''),
                                                                         stringr::str_replace_na(mname, ''),
                                                                         stringr::str_replace_na(dob, ''),
                                                                         stringr::str_replace_na(female, ''),
                                                                         sep = "|"))))]
        exit.ids <- unique(exit.ids[, .(exit = 'EXIT', id_hash)])
      
# MERGE IMPORTED TABLES TO LINK PHA `id_hash` TO KCMASTER_ID ----    
    # Merge ID lists ----
        linkages <- merge(idh.ids, 
                          kcha.ids, 
                          by = 'id_hash', 
                          all = T)
        
        linkages <- merge(linkages, 
                          sha.ids, 
                          by = 'id_hash', 
                          all = T)
        
        linkages <- merge(linkages, 
                          wait.ids, 
                          by = 'id_hash', 
                          all.x = T, all.y = F)
        
        linkages <- merge(linkages, 
                          exit.ids, 
                          by = 'id_hash', 
                          all.x = T, all.y = F)
        
        linkages[, agency := fcase(is.na(kcha) & is.na(sha), 'Neither', 
                                   !is.na(kcha) & !is.na(sha), 'Both', 
                                   !is.na(kcha) & is.na(sha), 'KCHA', 
                                   is.na(kcha) & !is.na(sha), 'SHA')]
        
    # Check if all IDH SOURCE_SYSTEM = 'PHA_CLIENT' rows can be matched to PHA data ----
        idh.check = linkages[is.na(wait) & is.na(exit), .(N = .N, freq = rads::round2(100*.N/nrow(linkages))), agency]
        linkages[is.na(wait) & is.na(exit) & agency == 'Neither']
        message(paste0("'Neither' PHA was was able to link to ~", idh.check[agency == 'Neither']$freq, 
                       "% (n=", idh.check[agency == 'Neither']$N,") of the relevant IDH PHA data"))
        
    # Check if there are PHA data that are not in the IDH ----
        kcha.missing <- linkages[is.na(KCMASTER_ID) & agency %in% c('KCHA', 'Both')]$id_hash
        sha.missing <- linkages[is.na(KCMASTER_ID) & agency %in% c('SHA', 'Both')]$id_hash
        pha.not.in.idh <- rbind(
          setDT(odbc::dbGetQuery(conn = db_hhsaw, 
                                 statement = glue::glue_sql("SELECT DISTINCT ssn, pha_id, lname, fname,
                                         mname, dob, female, id_hash, agency = 'KCHA'
                                         FROM [pha].[stage_kcha]
                                         WHERE id_hash IN ({kcha.missing*})", .con = db_hhsaw))), 
          setDT(odbc::dbGetQuery(conn = db_hhsaw, 
                                 statement = glue::glue_sql("SELECT DISTINCT ssn, pha_id, lname, fname,
                                         mname, dob, female, id_hash, agency = 'SHA'
                                         FROM [pha].[stage_sha]
                                         WHERE id_hash IN ({sha.missing*})", .con = db_hhsaw))))
        pha.not.in.idh <- unique(pha.not.in.idh[id_hash %in% kcha.missing & id_hash %in% sha.missing, agency := 'Both'])
 
        pha.check <- linkages[is.na(KCMASTER_ID), .N, agency]
        message(paste0(sum(pha.check$N), " (", round(sum(pha.check$N) / nrow(linkages)),
                       "%) of the PHA IDs could not be linked to the IDH"))
        print(pha.check)
        
        message("Here are some IDs with SSN that are in the PHA data that did not link to the IDH")
        pha.not.in.idh[!is.na(ssn)] 
        
        message("Here are the contents of the PHA-IDH linked table with these same SSN")
        linkages[ssn %in% pha.not.in.idh[!is.na(ssn)]$ssn] 
  
        
# TIDY THE LINKAGE TABLE ----
    # linkages <- linkages[agency != 'Neither'] # if I do this, I will drop all the linkages to the Exit and Waitlist data too
    linkages[, pha_id := kcha_id]
    linkages[is.na(pha_id), pha_id := sha_id] # replace with sha_id if there is not kcha_id. This is arbitrary
    run_time <- Sys.time()
    linkages <- unique(linkages[, .(ssn, pha_id, lname, fname, mname, dob, female, id_hash, KCMASTER_ID, last_run = run_time)])
    
    # As of 8/31/2023, there were a couple of times when a single id_hash was liked to > 1 KCMASTER_ID ... this should not be!
    linkages <- linkages[!duplicated(id_hash), ]

    setorder(linkages, lname, fname, mname, na.last = T)
        
# UPDATE stage_identities_history TABLE ----
    # Want to keep a record of a person's ID over time
    # There are five potential components to the updated history table
    # hx1 = historic data where the id_hash no longer exists in the new linkage table (unlikely, but anything is possible!)
    # hx2 = new id_hash with their corresponding KCMASTER_ID
    # hx3 = changed previously (pairs of id_hash and KCMASTER_ID previously designated no longer valid)
    # hx4 = change current (pairs of id_hash and KCMASTER_ID that are no longer valid)
    # hx5 = unchanged current (pairs of id_hash and KCMASTER_ID that remain the same as the previous run)

    # Bring in history table
    id_history <- setDT(dbGetQuery(
      db_hhsaw, glue_sql("SELECT * FROM {`final_schema`}.{DBI::SQL(paste0(final_table, '_history'))}",
                         .con = db_hhsaw)))
    id_history.count <- nrow(copy(id_history))
    
    # Fix any format issues
    id_history[, c("from_date", "to_date", "last_run") := lapply(.SD, as.POSIXct), .SDcols = c("from_date", "to_date", "last_run")]

    # get hx1 (historic data where the id_hash is no longer in the current linkage data)
    hx1 <- id_history[!id_hash %in% linkages$id_hash]
    id_history <- fsetdiff(id_history, hx1)
    hx1[!is.na(id_kc_pha) & is.na(KCMASTER_ID) & is.na(to_date), # only needed when first after transition to KCMASTER_ID
        to_date := as.POSIXct(run_time, origin = "1970-01-01", tz = "utc")]
    
    # get hx2 (new id_hash that do not exist in historic data)
    hx2 <- linkages[!id_hash %in% id_history$id_hash]
    hx2 <- hx2[, .(id_hash, KCMASTER_ID, id_kc_pha = NA_character_, 
                   from_date = as.POSIXct(run_time, origin = "1970-01-01", tz = "utc"), 
                   to_date = NA_POSIXct_, last_run = NA_POSIXct_)]
    
    # get hx3 (ID pairings that already have a to_date (i.e., were previously changed))
    hx3 <- id_history[!is.na(to_date)]
    id_history <- id_history[is.na(to_date)]
    
    # get hx4 & hx5 (hx4 = ID pairs that changed; hx5 = ID pairs that remained the same)
    hx4_5 <- merge(linkages[, .(id_hash, KCMASTER_ID)], 
                        id_history[, .(id_hash, KCMASTER_ID_old = KCMASTER_ID, id_kc_pha, to_date)], 
                        by = 'id_hash', 
                        all.x = F, 
                        all.y = T)
    
    hx4_5[(KCMASTER_ID != KCMASTER_ID_old  & is.na(to_date))|
                               (!is.na(id_kc_pha) & is.na(to_date)),  # only need this condition 1x > when switching from id_kc_pha to KCMASTER_ID
                              changed := 1L]
    
    hx4_5 <- hx4_5[, .(id_hash, KCMASTER_ID = KCMASTER_ID_old, id_kc_pha, changed)]
    
    hx4_5 <- merge(id_history, 
                        hx4_5, 
                        by = c('id_hash', 'KCMASTER_ID', 'id_kc_pha'), 
                        all = T)
    
    hx4_5[changed == 1, to_date := as.POSIXct(run_time, origin = "1970-01-01", tz = "utc")]
    hx4_5[, changed := NULL]
    
    # Create updated history table
    history_updated <- rbind(hx1, hx2, hx3, hx4_5)
    history_updated[, last_run := as.POSIXct(run_time, origin = "1970-01-01", tz = "utc")]
    
    # Quick QA
    if(nrow(history_updated) == id_history.count + nrow(hx2)){ # new table should have all rows from old table plus new IDs
      message('\U0001f642 ... all rows from the pre-existing history table are accounted for.')
    } else {stop("\n\U0001f47f ... there is a problem in the construction of your new history table. The total rows are not what was expected.")}

# QA FINAL DATA ----
    # Check number of id_hashes compared to the number of KCMASTER_ID ----
        message("There are ", format(n_distinct(linkages$id_hash), big.mark = ','), " IDs and ", 
                format(n_distinct(linkages$KCMASTER_ID), big.mark = ','), " KCMASTER_IDs")
    
    # Check that the combined pha.stage_identifies table is longer than the the previous time ----
        ident_cnt_prev <- as.integer(
          dbGetQuery(db_hhsaw,
                     glue_sql("SELECT qa_result
                                FROM ", {paste0(qa_schema, '.', qa_table)}, "
                                WHERE table_name = {paste0(to_schema, '.', to_table)} 
                                  AND qa_type = 'value' 
                                  AND qa_item = 'row_count' 
                                  AND qa_date = (
                                    SELECT MAX(qa_date) 
                                    FROM ", {paste0(qa_schema, '.', qa_table)}, " 
                                    WHERE table_name = {paste0(to_schema, '.', to_table)} 
                                      AND qa_type = 'value' 
                                      AND qa_item = 'row_count')", .con = db_hhsaw)
          ))
        
        if (is.na(ident_cnt_prev)) {
          qa_row_diff_result <- "PASS"
          qa_row_diff_note <- glue("There was no existing row data to compare to last time")
          message(paste("\U0001f642", qa_row_diff_note))
        } else if (!is.na(ident_cnt_prev) & nrow(linkages) >= ident_cnt_prev) {
          qa_row_diff_result <- "PASS"
          qa_row_diff_note <- glue("There are {format(nrow(linkages) - ident_cnt_prev, big.mark = ',')}", 
                                   " more rows in the latest stage_identities table")
          message(paste("\U0001f642", qa_row_diff_note))
        } else if (!is.na(ident_cnt_prev) & nrow(linkages) < ident_cnt_prev) {
          qa_row_diff_result <- "FAIL"
          qa_row_diff_note <- glue("There were {format(ident_cnt_prev - nrow(linkages), big.mark = ',')}", 
                                   " fewer rows in the latest stage_identities table. See why this is.")
          warning(paste("\U00026A0", qa_row_diff_note))
        } 
        
        DBI::dbExecute(db_hhsaw,
                       glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`} 
                                    (etl_batch_id, last_run, table_name, qa_type, qa_item, qa_result, qa_date, note) 
                                    VALUES (NULL, {min(linkages$last_run)}, '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}', 'result', 
                                    'row_count_vs_previous', {qa_row_diff_result}, {Sys.time()}, {qa_row_diff_note})",
                                .con = db_hhsaw))
    
    # Check that number of unique KCMASTER_ID is greater than in the previous time ----
        pha_ID_cnt_prev <- as.integer(
          dbGetQuery(db_hhsaw,
                     glue_sql("SELECT qa_result
                                FROM ", {paste0(qa_schema, '.', qa_table)}, "
                                WHERE table_name = {paste0(to_schema, '.', to_table)} 
                                  AND qa_type = 'value' 
                                  AND qa_item = 'id_count' 
                                  AND qa_date = (
                                    SELECT MAX(qa_date) 
                                    FROM ", {paste0(qa_schema, '.', qa_table)}, " 
                                    WHERE table_name = {paste0(to_schema, '.', to_table)} 
                                      AND qa_type = 'value' 
                                      AND qa_item = 'id_count')", .con = db_hhsaw)
          ))
        
        if (is.na(pha_ID_cnt_prev)) {
          qa_id_diff_result <- "PASS"
          qa_id_diff_note <- glue("There was no existing count of uniue IDs in stage_identities to compare to last time")
          message(paste("\U0001f642", qa_id_diff_note))
        } else if (!is.na(pha_ID_cnt_prev) & uniqueN(linkages$KCMASTER_ID) >= pha_ID_cnt_prev) {
          qa_id_diff_result <- "PASS"
          qa_id_diff_note <- glue("There are {format(uniqueN(linkages$KCMASTER_ID) - pha_ID_cnt_prev, big.mark = ',')}", 
                                  " more unique KCMASTER_ID in the latest stage_identities table")
          message(paste("\U0001f642", qa_id_diff_note))
        } else if (!is.na(pha_ID_cnt_prev) & uniqueN(linkages$KCMASTER_ID) < pha_ID_cnt_prev) {
          qa_id_diff_result <- "FAIL"
          qa_id_diff_note <- glue("There were {format(pha_ID_cnt_prev - uniqueN(linkages$KCMASTER_ID), big.mark = ',')}", 
                                  " fewer unique KCMASTER_ID in the latest stage_identities table. See why this is.")
          warning(paste("\U00026A0", qa_id_diff_note))
        } 
        
        DBI::dbExecute(db_hhsaw,
                       glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`} 
                                    (etl_batch_id, last_run, table_name, qa_type, qa_item, qa_result, qa_date, note) 
                                    VALUES (NULL, {min(linkages$last_run)}, '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}', 'result', 
                                    'id_count_vs_previous', {qa_id_diff_result}, {Sys.time()}, {qa_id_diff_note})",
                                .con = db_hhsaw))
      
# ADD VALUES TO METADATA ----
    # Row counts
    DBI::dbExecute(db_hhsaw,
                   glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`} 
                            (etl_batch_id, last_run, table_name, 
                              qa_type, qa_item, qa_result, qa_date, note) 
                            VALUES (NULL, {min(linkages$last_run)}, 
                                    '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                                    'value', 'row_count', {nrow(linkages)},
                                    {Sys.time()}, NULL)",
                            .con = db_hhsaw))
    
    # Number of IDs
    DBI::dbExecute(db_hhsaw,
                   glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`} 
                            (etl_batch_id, last_run, table_name, 
                              qa_type, qa_item, qa_result, qa_date, note) 
                            VALUES (NULL, {min(linkages$last_run)}, 
                                    '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                                    'value', 'id_count', {n_distinct(linkages$KC_MASTERID)},
                                    {Sys.time()}, NULL)",
                            .con = db_hhsaw))
  
  
# CHECK QA PASSED ----
  # Stop processing if one or more QA check failed
  if (min(qa_id_diff_result, qa_row_diff_result) == "FAIL") {
    stop(paste0("\n\U0001f47f One or more QA checks failed on ", to_schema, ".", to_table, ". Check\nSELECT * FROM ", qa_schema, ".", qa_table, " WHERE table_name = 'pha.stage_identities' ORDER BY qa_date desc \nfor more details."))
  } else {message(paste0("\U0001f642 Basic QA checks for ", to_schema, ".", to_table, "passed! Check\nSELECT * FROM ", qa_schema, ".", qa_table, " WHERE table_name = 'pha.stage_identities' ORDER BY qa_date desc \nfor more details."))}
  
  
# LOAD DATA TO SQL ----
  ## Identities ----
    # Split into smaller tables to avoid SQL db_hhsaw connection issues
    start <- 1L
    max_rows <- 50000L
    cycles <- ceiling(nrow(linkages)/max_rows)
    
    lapply(seq(start, cycles), function(i) {
      start_row <- ifelse(i == 1, 1L, max_rows * (i-1) + 1)
      end_row <- min(nrow(linkages), max_rows * i)
      
      message("Loading cycle ", i, " of ", cycles)
      if (i == 1) {
        dbWriteTable(db_hhsaw,
                     name = DBI::Id(schema = to_schema, table = to_table),
                     value = as.data.frame(linkages[start_row:end_row, ]),
                     overwrite = T, append = F)
      } else {
        dbWriteTable(db_hhsaw,
                     name = DBI::Id(schema = to_schema, table = to_table),
                     value = as.data.frame(linkages[start_row:end_row ,]),
                     overwrite = F, append = T)
      }
    })
  
  ## Identity history ----
    # Split into smaller tables to avoid SQL db_hhsaw connection issues
    start <- 1L
    max_rows <- 50000L
    cycles <- ceiling(nrow(history_updated)/max_rows)
    
    lapply(seq(start, cycles), function(i) {
      start_row <- ifelse(i == 1, 1L, max_rows * (i-1) + 1)
      end_row <- min(nrow(history_updated), max_rows * i)
      
      message("Loading cycle ", i, " of ", cycles)
      if (i == 1) {
        dbWriteTable(db_hhsaw,
                     name = DBI::Id(schema = to_schema, table = paste0(to_table, "_history")),
                     value = as.data.frame(history_updated[start_row:end_row, ]),
                     overwrite = T, append = F)
      } else {
        dbWriteTable(db_hhsaw,
                     name = DBI::Id(schema = to_schema, table = paste0(to_table, "_history")),
                     value = as.data.frame(history_updated[start_row:end_row ,]),
                     overwrite = F, append = T)
      }
    })
  