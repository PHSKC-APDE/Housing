## Script name: HUD HEARS PROCESSING - KCHA RAW
##
## Purpose of script: Combine and process raw KCHA and SHA exit data
##
## Author: Alastair Matheson, Public Health - Seattle & King County
## Date Created: 2021-08-11
## Email: alastair.matheson@kingcounty.gov
##
## Notes:
# Run from main_exit_load script
# https://github.com/PHSKC-APDE/Housing/blob/master/claims_db/etl/db_loader/main_exit_load.R
# Assumes relevant libraries are already loaded

load_stage_pha_exit <- function(conn = NULL,
                                from_schema = NULL,
                                from_table_kcha = NULL,
                                from_table_sha = NULL,
                                to_schema = NULL,
                                to_table = NULL,
                                qa_schema = NULL,
                                qa_table = NULL) {
  
  # BRING IN DATA ----
  ## KCHA ----
  kcha_exit <- dbGetQuery(conn, 
                          glue_sql("SELECT * FROM {`from_schema`}.{`from_table_kcha`}", .con = conn))
  
  ## SHA ----
  sha_exit <- dbGetQuery(conn, 
                         glue_sql("SELECT * FROM {`from_schema`}.{`from_table_sha`}", .con = conn))
  
  
  # COMBINE AND CLEAN DATA ----
  pha_exit = list("kcha_exit" = kcha_exit, "sha_exit" = sha_exit)
  
  ## Names ----
  # Set up suffixes to remove
  suffix <- c(" SR", " JR", "-JR", "JR", "JR I", "JR II", "JR III", " II", " III", " IV")
  
  pha_exit <- pha_exit %>%
    map(~ .x %>%
          mutate(across(any_of(c("hh_lname", "hh_fname", "hh_mname", "lname", "fname", "mname")), 
                        ~ toupper(.)),
                 across(any_of(c("hh_lname", "hh_fname", "hh_mname", "lname", "fname", "mname")), 
                        ~ str_replace(., "_|-", " ")),
                 across(any_of(c("hh_lname", "hh_fname", "hh_mname", "lname", "fname", "mname")), 
                        ~ str_replace_all(., "NULL|\\.|\"|\\\\|'|`|[0-9]|\\*", "")),
                 across(any_of(c("hh_lname", "hh_fname", "hh_mname", "lname", "fname", "mname")), 
                        ~ str_replace_all(., paste(suffix, "$", collapse="|", sep = ""), "")),
                 across(any_of(c("hh_lname", "hh_fname", "hh_mname", "lname", "fname", "mname")), 
                        ~ str_squish(.)),
                 across(any_of(c("hh_lname", "hh_fname", "hh_mname", "lname", "fname", "mname")), 
                        ~ ifelse(. == "", NA_character_, .)))) %>%
    # Clean up where middle initial seems to be in first name field
    # NOTE: There are many rows with a middle initial in the fname field AND 
    # the mname field (and the initials are not always the same).
    # In those situations, keep existing middle initial and drop from fname if
    # the initial is the same
    map(~ if ("fname" %in% names(.x)) {
      .x %>% 
        mutate(f_init = str_sub(fname, -2, -1),
               mname = case_when(str_detect(f_init, "[:space:][A-Z]") & is.na(mname) ~ str_sub(fname, -1),
                                 TRUE ~ mname),
               fname = case_when(str_detect(f_init, "[:space:][A-Z]") & str_sub(fname, -1) == mname ~ 
                                   str_sub(fname, 1, -3),
                                 TRUE ~ fname),
               # when middle initial is not a letter, replace it with NA
               mname = ifelse(str_detect(mname, "[A-Z]", negate = T), NA, mname),
               # Remove any first name unknown values
               fname = ifelse(fname == "FNU", NA_character_, fname),
               # Flag baby and institutional names for cleaning
               drop_name = case_when(fname %in% c("UNBORN", "BABY", "MIRACLE", "CHILD") & 
                                       lname %in% c("UNBORN", "BABY", "CHILD") ~ 1L,
                                     lname == "YWCA" ~ 1L,
                                     lname == "ELDER" & fname == "PLACE" ~ 1L,
                                     lname == "ELDER PLACE" ~ 1L,
                                     str_detect(lname, "LIVE IN") ~ 1L,
                                     str_detect(fname, "LIVE IN") ~ 1L,
                                     TRUE ~ 0L),
               # Clean baby and other names
               across(any_of(c("lname", "fname", "mname")), ~ ifelse(drop_name == 1, NA, .)),
               fname = ifelse(fname %in% c("UNBORN", "BABY"), NA, fname)
        ) %>%
        select(-f_init, -drop_name) %>% 
        filter(!(lname == "DUFUS" & fname == "IAM"))
    } else {.x}) %>%
    map(~ if ("hh_fname" %in% names(.x)) {
      .x %>% 
        mutate(f_init_hh = str_sub(hh_fname, -2, -1),
               hh_mname = case_when(str_detect(f_init_hh, "[:space:][A-Z]") ~ str_sub(hh_fname, -1),
                                    TRUE ~ NA_character_),
               hh_fname = case_when(str_detect(f_init_hh, "[:space:][A-Z]") & str_sub(hh_fname, -1) == hh_mname ~ 
                                      str_sub(hh_fname, 1, -3),
                                    TRUE ~ hh_fname),
               # when middle initial is not a letter, replace it with NA
               hh_mname = ifelse(str_detect(hh_mname, "[A-Z]", negate = T), NA, hh_mname),
               # Remove any first name unknown values
               hh_fname = ifelse(hh_fname == "FNU", NA_character_, hh_fname),
               # Flag baby and institutional names for cleaning
               drop_name_hh = case_when(hh_fname %in% c("UNBORN", "BABY", "MIRACLE", "CHILD") & 
                                          hh_lname %in% c("UNBORN", "BABY", "CHILD") ~ 1L,
                                        hh_lname == "YWCA" ~ 1L,
                                        hh_lname == "ELDER" & hh_fname == "PLACE" ~ 1L,
                                        hh_lname == "ELDER PLACE" ~ 1L,
                                        str_detect(hh_lname, "LIVE IN") ~ 1L,
                                        str_detect(hh_fname, "LIVE IN") ~ 1L,
                                        TRUE ~ 0L),
               # Clean baby and other names
               across(any_of(c("hh_lname", "hh_fname", "hh_mname")), ~ ifelse(drop_name_hh == 1, NA, .))
        ) %>%
        select(-f_init_hh, -drop_name_hh)
    } else {.x})
  
  
  ## SSN ----
  pha_exit <- pha_exit %>%
    map(~ .x %>%
          mutate(across(any_of(c("ssn", "hh_ssn")), ~ str_replace_all(., "-", "")),
                 across(any_of(c("ssn", "hh_ssn")), 
                        ~ case_when(. %in% c("010010101", "011111111", "011223333", 
                                             "111111111", "112234455", "111119999", "123121234", "123123123", "123456789", 
                                             "222111212", "222332222",
                                             "333333333", "444444444", 
                                             "555112222", "555115555", "555555555", "555555566",
                                             "699999999",  
                                             "888888888", "898989898", "898888899") ~ NA_character_,
                                    as.numeric(.) < 1000000 | as.numeric(.) >= 900000000 ~ NA_character_,
                                    between(as.numeric(.), 666000000, 666999999) ~ NA_character_,
                                    str_sub(., -4, -1) == "0000" | str_sub(., 1, 3) == "999" |
                                      str_detect(., "XXX|A00-0|A000") ~ NA_character_,
                                    TRUE ~ .)),
                 across(any_of(c("ssn", "hh_ssn")), ~ ifelse(str_detect(., "[:alpha:]"), ., NA_character_),
                        .names = "{.col}_new"),
                 across(any_of(c("ssn", "hh_ssn")),
                        ~ str_pad(round(as.numeric(.), digits = 0), width = 9, side = "left", pad = "0")))) %>%
    # Change new column names
    map(~ .x %>% rename_with(., ~ str_replace(., "ssn_new", "pha_id"), .cols = matches("ssn_new")))
  
  
  # ADDRESS CLEANING ----
  ## Make a geo_hash_raw field for easier joining
  # NB. Adding WA to geo_state_raw manually since it is missing
  message("Cleaning addresses")
  pha_exit <- pha_exit %>%
    map(~ if ("geo_add1_raw" %in% names(.x)) {
      .x %>% 
        mutate(across(starts_with("geo_"), toupper)) %>%
        mutate(geo_hash_raw = as.character(toupper(openssl::sha256(
          paste(str_replace_na(geo_add1_raw, ''),
                if ("geo_add2_raw" %in% names(.x)) {str_replace_na(geo_add2_raw, '')} else {''},
                if ("geo_add3_raw" %in% names(.x)) {str_replace_na(geo_add3_raw, '')} else {''},
                str_replace_na(geo_city_raw, ''),
                if ("geo_state_raw" %in% names(.x)) {str_replace_na(geo_state_raw, 'WA')} else {'WA'},
                str_replace_na(geo_zip_raw, ''),
                sep = "|")))))
    } else {.x})
  
  ## Pull out all unique addresses ----
  adds_distinct <- pha_exit %>%
    map(~ .x %>% distinct(across(matches("^geo_")))) %>%
    bind_rows() %>%
    distinct() %>%
    select(geo_add1_raw, geo_add2_raw, geo_add3_raw, geo_city_raw, 
           geo_state_raw, geo_zip_raw, geo_hash_raw) %>%
    filter(!is.na(geo_hash_raw))
  
  
  ## Load to a temp SQL table for cleaning (have to use prod HHSAW here) ----
  db_hhsaw_prod <- dbConnect(odbc(), "hhsaw_prod", uid = keyring::key_list("hhsaw")[["username"]])
  
  try(dbRemoveTable(db_hhsaw_prod, "##pha_exit_adds"), silent = T)
  odbc::dbWriteTable(db_hhsaw_prod,
                     name = "##pha_exit_adds",
                     value = adds_distinct,
                     overwrite = T)
  
  ## Pull in clean addresses ----
  adds_already_clean <- DBI::dbGetQuery(db_hhsaw_prod,
                                        "SELECT b.* FROM 
                                 (SELECT geo_hash_raw FROM ##pha_exit_adds) a
                                 INNER JOIN
                                 (SELECT * FROM ref.address_clean) b
                                 ON a.geo_hash_raw = b.geo_hash_raw")
  
  ## Pull in addresses that need cleaning ----
  adds_to_clean <- DBI::dbGetQuery(db_hhsaw_prod,
                                   "SELECT a.* FROM 
                                 (SELECT * FROM ##pha_exit_adds) a
                                 LEFT JOIN
                                 (SELECT geo_hash_raw, 1 AS clean
                                 FROM ref.address_clean) b
                                 ON a.geo_hash_raw = b.geo_hash_raw
                                 WHERE b.clean IS NULL")
  
  ## Load to Informatica for cleaning ----
  if (nrow(adds_to_clean) > 0) {
    message("Loading addresses for cleaning in Informatica")
    # Add new addresses that need cleaning into the Informatica table
    timestamp <- Sys.time()
    
    adds_to_clean <- adds_to_clean %>% mutate(geo_source = NA, timestamp = timestamp)
    
    DBI::dbWriteTable(db_hhsaw_prod, 
                      name = DBI::Id(schema = "ref", table = "informatica_address_input"),
                      value = adds_to_clean,
                      overwrite = F, append = T)
  }
  
  ## Retrieve cleaned addresses ----
  if (nrow(adds_to_clean) > 0) {
    adds_clean <- dbGetQuery(db_hhsaw_prod,
                             glue::glue_sql("SELECT * FROM ref.informatica_address_output
                           WHERE convert(varchar, timestamp, 20) = {lubridate::with_tz(timestamp, 'utc')}",
                                            .con = db_hhsaw_prod))
    
    # Keep checking each hour until the addresses have been cleaned
    while (nrow(adds_clean) == 0) {
      message("Waiting on Informatica to clean addresses. Will check again in 1 hour.")
      Sys.sleep(3600)
      
      # Will need to reconnect to the DB since it will have timed out
      db_hhsaw_prod <- dbConnect(odbc(), "hhsaw_prod", uid = keyring::key_list("hhsaw")[["username"]])
      
      adds_clean <- dbGetQuery(db_hhsaw_prod,
                               glue::glue_sql("SELECT * FROM ref.informatica_address_output
                           WHERE convert(varchar, timestamp, 20) = {lubridate::with_tz(timestamp, 'utc')}",
                                              .con = db_hhsaw_prod))
    }
    
    # Informatica seems to drop secondary designators when they start with #
    # Move over from old address
    adds_clean <- adds_clean %>%
      mutate(geo_add2_clean = ifelse(is.na(geo_add2_clean) & str_detect(geo_add1_raw, "^#"),
                                     geo_add1_raw, geo_add2_clean))
    
    # Tidy up some PO box messiness
    adds_clean <- adds_clean %>%
      mutate(geo_add1_clean = case_when((is.na(geo_add1_clean) | geo_add1_clean == "") & 
                                          !is.na(geo_po_box_clean) ~ geo_po_box_clean,
                                        TRUE ~ geo_add1_clean),
             geo_add2_clean = case_when(
               geo_add1_clean == geo_po_box_clean ~ geo_add2_clean,
               (is.na(geo_add2_clean) | geo_add2_clean == "") & !is.na(geo_po_box_clean) & 
                 !is.na(geo_add1_clean) ~ geo_po_box_clean,
               !is.na(geo_add2_clean) & !is.na(geo_po_box_clean) & 
                 !is.na(geo_add1_clean) ~ paste(geo_add2_clean, geo_po_box_clean, sep = " "),
               TRUE ~ geo_add2_clean)
      )
    
    # Set up variables of interest
    adds_clean <- adds_clean %>%
      mutate(geo_geocode_skip = 0L,
             across(where(is.character) & contains("clean"), str_squish),
             geo_hash_clean = as.character(toupper(openssl::sha256(paste(stringr::str_replace_na(geo_add1_clean, ''), 
                                                                         stringr::str_replace_na(geo_add2_clean, ''), 
                                                                         stringr::str_replace_na(geo_city_clean, ''), 
                                                                         stringr::str_replace_na(geo_state_clean, ''), 
                                                                         stringr::str_replace_na(geo_zip_clean, ''), 
                                                                         sep = "|")))),
             geo_hash_geocode = as.character(toupper(openssl::sha256(paste(stringr::str_replace_na(geo_add1_clean, ''),  
                                                                           stringr::str_replace_na(geo_city_clean, ''), 
                                                                           stringr::str_replace_na(geo_state_clean, ''), 
                                                                           stringr::str_replace_na(geo_zip_clean, ''), 
                                                                           sep = "|")))),
             last_run = Sys.time()) %>%
      select(geo_add1_raw, geo_add2_raw, geo_add3_raw, geo_city_raw, 
             geo_state_raw, geo_zip_raw, geo_hash_raw,
             geo_add1_clean, geo_add2_clean, geo_city_clean, 
             geo_state_clean, geo_zip_clean, geo_hash_clean, geo_hash_geocode,
             geo_geocode_skip, last_run) %>%
      # Convert all blank fields to be NA
      mutate_if(is.character, list(~ ifelse(. == "", NA_character_, .)))
    
    
    # Add to ref table
    dbWriteTable(db_hhsaw_prod, 
                 name = DBI::Id(schema = "ref",  table = "stage_address_clean"),
                 adds_clean,
                 overwrite = F, append = T)
    
    # Do some basic QA
    rows_stage <- as.integer(dbGetQuery(db_hhsaw_prod, "SELECT COUNT (*) AS row_cnt FROM ref.stage_address_clean"))
    rows_ref <- as.integer(dbGetQuery(db_hhsaw_prod, "SELECT COUNT (*) AS row_cnt FROM ref.address_clean"))
    
    if (rows_stage > rows_ref) {
      dbWriteTable(db_hhsaw_prod, 
                   name = DBI::Id(schema = "ref",  table = "address_clean"),
                   adds_clean,
                   overwrite = F, append = T)
    }
    
    # Don't need to geocode at this point so skip that part
  }
  
  ## Bring it all together ----
  if (nrow(adds_to_clean) > 0) {
    adds_final <- bind_rows(adds_already_clean, adds_clean)
  } else {
    adds_final <- adds_already_clean
  }
  
  ## Join back to original data ----
  pha_exit <- pha_exit %>%
    map(~ if ("geo_hash_raw" %in% names(.x)) {
      .x %>% left_join(., 
                       select(adds_final, geo_hash_raw:geo_hash_geocode),
                       by = "geo_hash_raw") %>%
        select(-matches("geo_(.)*_raw"))
    } else {.x})
  
  # Add in geo_blank
  pha_exit <- pha_exit %>%
    map(~ if ("geo_hash_clean" %in% names(.x)) {
      .x %>% mutate(geo_blank = ifelse(
        geo_hash_clean %in% c("45CA31C3315A5978F40438AAB46040D75E99C9B125C2FD01DB6E10AC80BEF906",
                              "8926262F06508A0E264BC13D340FD8FAB9291001FC06341D2E687BD9C3AF6104"),
        1L, 0L))
    } else {.x}) 
  
  
  # MAKE FINAL DATA FRAME AND ADD USEFUL VARIABLES ----
  last_run <- Sys.time()
  
  pha_exit_long <- pha_exit %>%
    bind_rows() %>%
    mutate(last_run = last_run) %>%
    distinct()
  
  
  # ADD VALUES TO METADATA ----
  # Row counts
  DBI::dbExecute(conn,
                 glue_sql("INSERT INTO pha.metadata_qa 
                          (etl_batch_id, last_run, table_name, 
                            qa_type, qa_item, qa_result, qa_date, note) 
                          VALUES (NULL, {last_run}, '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                                  'value', 'row_count', {nrow(pha_exit_long)},
                                  {Sys.time()}, NULL)",
                          .con = conn))
  
  
  # LOAD DATA TO SQL ----
  # Split into smaller tables to avoid SQL connection issues
  start <- 1L
  max_rows <- 50000L
  cycles <- ceiling(nrow(pha_exit_long)/max_rows)
  
  lapply(seq(start, cycles), function(i) {
    start_row <- ifelse(i == 1, 1L, max_rows * (i-1) + 1)
    end_row <- min(nrow(pha_exit_long), max_rows * i)
    
    message("Loading cycle ", i, " of ", cycles)
    if (i == 1) {
      dbWriteTable(db_hhsaw,
                   name = DBI::Id(schema = to_schema, table = to_table),
                   value = as.data.frame(pha_exit_long[start_row:end_row, ]),
                   overwrite = T, append = F)
    } else {
      dbWriteTable(db_hhsaw,
                   name = DBI::Id(schema = to_schema, table = to_table),
                   value = as.data.frame(pha_exit_long[start_row:end_row ,]),
                   overwrite = F, append = T)
    }
  })
}
