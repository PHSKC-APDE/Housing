# OVERVIEW:
# Code to create a cleaned waitlist dataset from the SHA and KCHA data with health outcomes
#
# Run from main_pha_waitlist_load script
# https://github.com/PHSKC-APDE/Housing/blob/master/claims_db/etl/db_loader/main_pha_waitlist_load.R
# Assumes relevant libraries are already loaded
#
# Alastair Matheson (PHSKC-APDE)
# alastair.matheson@kingcounty.gov
# 2021-07


load_stage_pha_waitlist <- function(conn = NULL,
                                    to_schema = "pha",
                                    to_table = "stage_pha_waitlist") {
  
  # Hard coding it all for now, could make more dynamic later
  
  # BRING IN DATA ----
  # KCHA
  kcha <- dbGetQuery(conn, "SELECT * FROM pha.raw_kcha_waitlist_2017")
  
  # SHA
  sha <- dbGetQuery(conn, "SELECT * FROM pha.raw_sha_waitlist_2017")
  
  # Bring in variable name mapping table
  fields_waitlist <- read.csv(file.path(here::here(), "etl/ref/waitlist_field_name_mapping.csv"), 
                              header = TRUE, stringsAsFactors = FALSE)
  
  
  # RENAME FIELDS ----
  kcha <- data.table::setnames(kcha, fields_waitlist$common_name[match(names(kcha), fields_waitlist$kcha_2017)])
  sha <- data.table::setnames(sha, fields_waitlist$common_name[match(names(sha), fields_waitlist$sha_2017)])
  
  
  # FIX UP SHA HEADS OF HOUSEHOLD ----
  # The SHA data has some issues with the head of household data:
  # If the HH size > 1, there is no row for the HoH
  # If the the HH size = 1, there is a single row but all the non-HoH fields (e.g., lname, SSN) are blank
  
  # Make one row per HoH
  sha_hh <- sha %>%
    distinct() %>%
    mutate(app_mbr_num = as.character(app_num),
           ssn = hh_ssn,
           fname = hh_fname,
           lname = hh_lname,
           mname = hh_mname,
           dob = hh_dob,
           gender = hh_gender,
           relcode = "Head of Household",
           disability = hh_disability,
           r_white = hh_r_white,
           r_black = hh_r_black,
           r_aian = hh_r_aian,
           r_asian = hh_r_asian,
           r_nhpi = hh_r_nhpi,
           r_hisp = hh_r_hisp) %>%
    distinct()
  
  # Remove rows where the hh_size is 1 and replace with total HoH data
  sha <- sha %>% 
    filter(!(ssn == "NULL" & fname == "NULL" & lname == "NULL")) %>%
    bind_rows(., sha_hh)
  
  # Clean up
  rm(sha_hh)
  
  
  # BRING TOGETHER AND PROCESS DATA ----
  pha <- list("kcha" = mutate(kcha, agency = "KCHA"), 
              "sha" = mutate(sha, agency = "SHA"))
  
  ## Drop fields that are not needed ----
  pha <- pha %>% map(~ .x %>% select(-starts_with("drop")))
  
  
  ## General tidying ----
  pha <- pha %>%
    map(~ .x %>%
          mutate(across(where(is.character), ~ str_squish(.)),
                 across(where(is.character), ~ ifelse(. == "NULL", NA_character_, .)),
                 across(where(is.character), ~ ifelse(. == "", NA_character_, .)),
                 app_mbr_num = as.numeric(app_mbr_num))
    )
  
  ## Set up fields for identity matching ----
  # Clean up fields
  pha <- pha %>%
    map(~ .x %>%
          mutate(ssn = str_replace_all(ssn, "-", ""),
                 # Remove accents etc on names and set to caps
                 across(contains("name"), ~ toupper(iconv(., from = '', to = 'ASCII//TRANSLIT'))),
                 across(contains("name"), ~ str_remove_all(., "\\.")),
                 across(contains("name"), ~ str_squish(.)),
                 across(contains("name"), ~ ifelse(. == "", NA_character_, .)),
                 # Remove first name unknown acronyms
                 fname = ifelse (fname == "FNU", NA_character_, fname))
    )
  
  
  # Run function to flag junk SSNs (more comprehensive than the existing ssn_missing flag)
  pha <- pha %>% map(~ housing::junk_ssn_num(.x, ssn))
  
  pha <- pha %>% map(~ .x %>% mutate(ssn = ifelse(ssn_junk == 1, NA_character_, ssn)))
  
  
  ## Set up demographics ----
  # For KCHA, the r_eth field contains summary race but it doesn't always line up with the individual
  # race/ethniciity fields, so create new version
  pha <- pha %>%
    map(~ .x %>%
          mutate(across(any_of(c("disability", "r_aian", "r_asian", "r_black", "r_nhpi", "r_white", 
                                 "hh_r_aian", "hh_r_asian", "hh_r_black", "hh_r_nhpi", "hh_r_white", 
                                 "lang_eng", "lang_interpreter", "single_parent", "issuance", "lease_up", 
                                 "hcv_standard")) &
                          where(is.character),
                        ~ case_when(. %in% c("Y", "1") ~ 1L,
                                    . %in% c("N", "0") ~ 0L,
                                    TRUE ~ NA_integer_)),
                 across(contains("r_hisp"), 
                        ~ case_when(. %in% c("Y", "Hispanic") ~ 1L,
                                    . %in% c("N", "Non-Hispanic") ~ 0L,
                                    TRUE ~ NA_integer_)),
                 r_eth = case_when(r_hisp == 1 ~ "Hispanic",
                                   r_aian + r_asian + r_black + r_nhpi + r_white > 1 ~ "Multiple race",
                                   r_aian == 1 ~ "AIAN only",
                                   r_asian == 1 ~ "Asian only",
                                   r_black == 1 ~ "Black only",
                                   r_nhpi == 1 ~ "NHPI only",
                                   r_white == 1 ~ "White only",
                                   TRUE ~ NA_character_),
                 across(any_of(c("dob", "hh_dob")), ~ as.Date(., format = "%m/%d/%Y")),
                 gender = case_when(gender %in% c("F", "Female") ~ 1L,
                                    gender %in% c("M", "Male") ~ 2L,
                                    TRUE ~ NA_integer_))) %>%
    map(~ if ("lang_eng" %in% names(.x) == F) {
      .x %>% mutate(lang_eng = case_when(lang == "ENG" ~ 1L, 
                                         lang != "ENG" ~ 0L,
                                         TRUE ~ NA_integer_))
    } else {.x}) %>%
    map(~ if ("hh_r_aian" %in% names(.x)) {
      .x %>% mutate(hh_r_eth = case_when(hh_r_hisp == 1 ~ "Hispanic",
                                         hh_r_aian + hh_r_asian + hh_r_black + hh_r_nhpi + hh_r_white > 1 ~ "Multiple race",
                                         hh_r_aian == 1 ~ "AIAN only",
                                         hh_r_asian == 1 ~ "Asian only",
                                         hh_r_black == 1 ~ "Black only",
                                         hh_r_nhpi == 1 ~ "NHPI only",
                                         hh_r_white == 1 ~ "White only",
                                         TRUE ~ NA_character_),)
    } else {.x})
  
  
  ## Household size ----
  # Some households are missing a household size so need to add it in
  # OK to overwrite existing hh_size value since it matches the calculated one if present
  pha <- pha %>%
    map(~ .x %>%
          mutate(hh_size = as.numeric(hh_size)) %>%
          group_by(app_num) %>%
          mutate(hh_size = n()) %>%
          ungroup()
    )
  
  # Check household sizes look correct
  pha %>% map(~ .x %>% summarise(count_hh = n_distinct(app_num), count_ind = n_distinct(app_mbr_num)))
  pha %>% map(~ .x %>% group_by(hh_size) %>% summarise(count = n()) %>% ungroup() %>% 
                mutate(hh_cnt = count / hh_size, hh_tot = sum(hh_cnt, na.rm = T)))  # hh_tot should match count_hh
  
  
  ## SSNs ----
  # Some applicants used the same SSN for all household members
  # Keep it for head of household and remove for others
  pha <- pha %>% map(~ .x %>%
                       group_by(app_num) %>%
                       mutate(ssn_cnt = n_distinct(ssn, na.rm = T)) %>%
                       ungroup() %>%
                       mutate(ssn = case_when(ssn_cnt > 1 | hh_size == 1 ~ ssn,
                                              ssn_cnt == 1 & relcode %in% c("H", "Head of Household") ~ ssn,
                                              TRUE ~ NA_character_)) %>%
                       select(-ssn_cnt) %>%
                       mutate(across(any_of(c("ssn", "hh_ssn")),
                                     ~ str_pad(round(as.numeric(.), digits = 0), width = 9, side = "left", pad = "0"))))
  
  
  ## Living situation ----
  # Defer to JHU on how they want to process it, just leave relevant fields n
  
  
  ## Household income ----
  # Defer to JHU, just leave relevant fields in and fix formats
  pha <- pha %>%
    map(~ .x %>%
          mutate(across(any_of(c("inc_disability", "inc_other", "inc_pension", 
                                 "inc_ssi", "inc_tanf", "inc_wage",
                                 "hh_inc_tot"))  &
                          where(is.character),
                        ~ as.numeric(.)))
    )
  
  
  # ADDRESSES ----
  pha <- pha %>%
    map(~ if ("geo_add3_raw" %in% names(.x) == F) {
      .x %>% mutate(geo_add3_raw = NA_character_,
                    geo_mail_add3_raw = NA_character_)
    } else {.x}) %>%
    map(~ .x %>%
          mutate(across(contains("geo") & where(is.character), ~ toupper(.)),
                 across(contains("zip"), ~ as.character(.)),
                 across(contains("zip"), ~ str_remove_all(., "-| ")),
                 across(contains("zip"), ~ str_sub(., 1, 5)),
                 across(contains("geo_"), ~ trimws(toupper(.))),
                 across(contains("geo_"), ~ str_remove_all(., "\\.")),
                 across(contains("geo_"), ~ ifelse(. == "", NA_character_, .)),
                 geo_add2_raw = ifelse(is.na(geo_add2_raw) & !is.na(geo_add3_raw), 
                                       geo_add3_raw, geo_add2_raw),
                 geo_add3_raw = ifelse(geo_add2_raw  == geo_add3_raw, NA_character_, geo_add3_raw),
                 geo_mail_add2_raw = ifelse(is.na(geo_mail_add2_raw) & !is.na(geo_mail_add3_raw), 
                                            geo_mail_add3_raw, geo_mail_add2_raw),
                 geo_mail_add3_raw = ifelse(geo_mail_add2_raw  == geo_mail_add3_raw, NA_character_, geo_mail_add3_raw),
                 geo_hash_raw = as.character(toupper(openssl::sha256(
                   paste(str_replace_na(geo_add1_raw, ''),
                         str_replace_na(geo_add2_raw, ''),
                         str_replace_na(geo_add3_raw, ''),
                         str_replace_na(geo_city_raw, ''),
                         str_replace_na(geo_state_raw, ''),
                         str_replace_na(geo_zip_raw, ''),
                         sep = "|")))),
                 geo_mail_hash_raw = as.character(toupper(openssl::sha256(
                   paste(str_replace_na(geo_mail_add1_raw, ''),
                         str_replace_na(geo_mail_add2_raw, ''),
                         str_replace_na(geo_mail_add3_raw, ''),
                         str_replace_na(geo_mail_city_raw, ''),
                         str_replace_na(geo_mail_state_raw, ''),
                         str_replace_na(geo_mail_zip_raw, ''),
                         sep = "|"))))
          )
    )
  
  
  ## Pull out all unique addresses ----
  adds_distinct <- pha %>%
    map(~ .x %>% 
          select(geo_add1_raw, geo_add2_raw, geo_add3_raw, geo_city_raw, 
                 geo_state_raw, geo_zip_raw, geo_hash_raw) %>%
          distinct() %>%
          bind_rows()) %>%
    bind_rows() %>%
    distinct()
  
  
  adds_mail_distinct <- pha %>%
    map(~ .x %>% 
          select(geo_mail_add1_raw, geo_mail_add2_raw, geo_mail_add3_raw, geo_mail_city_raw, 
                 geo_mail_state_raw, geo_mail_zip_raw, geo_mail_hash_raw) %>%
          distinct() %>%
          rename(geo_add1_raw = geo_mail_add1_raw,
                 geo_add2_raw = geo_mail_add2_raw,
                 geo_add3_raw = geo_mail_add3_raw,
                 geo_city_raw = geo_mail_city_raw,
                 geo_state_raw = geo_mail_state_raw,
                 geo_zip_raw = geo_mail_zip_raw,
                 geo_hash_raw = geo_mail_hash_raw)) %>%
    bind_rows() %>%
    distinct() %>%
    select(geo_add1_raw, geo_add2_raw, geo_add3_raw, geo_city_raw, 
           geo_state_raw, geo_zip_raw, geo_hash_raw)
  
  adds_distinct <- bind_rows(adds_distinct, adds_mail_distinct) %>%
    distinct()
  
  
  ## Load to a temp SQL table for cleaning (have to use prod HHSAW here) ----
  db_hhsaw_prod <- dbConnect(odbc(), "hhsaw_prod", uid = keyring::key_list("hhsaw")[["username"]])
  
  try(dbRemoveTable(db_hhsaw_prod, "##pha_adds_waitlist"), silent = T)
  odbc::dbWriteTable(db_hhsaw_prod,
                     name = "##pha_adds_waitlist",
                     value = adds_distinct,
                     overwrite = T)
  
  ## Pull in clean addresses ----
  adds_already_clean <- DBI::dbGetQuery(db_hhsaw_prod,
                                        "SELECT b.* FROM 
                                 (SELECT geo_hash_raw FROM ##pha_adds_waitlist) a
                                 INNER JOIN
                                 (SELECT * FROM ref.address_clean) b
                                 ON a.geo_hash_raw = b.geo_hash_raw")
  
  ## Pull in addresses that need cleaning ----
  adds_to_clean <- DBI::dbGetQuery(db_hhsaw_prod,
                                   "SELECT a.* FROM 
                                 (SELECT * FROM ##pha_adds_waitlist) a
                                 LEFT JOIN
                                 (SELECT geo_hash_raw, 1 AS clean
                                 FROM ref.address_clean) b
                                 ON a.geo_hash_raw = b.geo_hash_raw
                                 WHERE b.clean IS NULL")
  
  ## Load to Informatica for cleaning ----
  if (nrow(adds_to_clean) > 0) {
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
    
    if (rows_stage - rows_ref == nrow(adds_clean)) {
      dbWriteTable(db_hhsaw_prod, 
                   name = DBI::Id(schema = "ref",  table = "address_clean"),
                   adds_clean,
                   overwrite = F, append = T)
    }
    
    # Don't need to geocode at this point so skip that part
  }
  
  ## Bring clean addresses together ----
  if (nrow(adds_to_clean) > 0) {
    adds_final <- bind_rows(adds_already_clean, adds_clean)
  } else {
    adds_final <- adds_already_clean
  }
  
  
  ## Join back to original data ----
  pha <- pha %>%
    map(~ .x %>%
          left_join(., 
                    select(adds_final, geo_hash_raw:geo_hash_geocode),
                    by = c("geo_mail_hash_raw" = "geo_hash_raw")) %>%
          rename(geo_mail_add1_clean = geo_add1_clean,
                 geo_mail_add2_clean = geo_add2_clean,
                 geo_mail_city_clean = geo_city_clean,
                 geo_mail_state_clean = geo_state_clean,
                 geo_mail_zip_clean = geo_zip_clean,
                 geo_mail_hash_clean = geo_hash_clean,
                 geo_mail_hash_geocode = geo_hash_geocode) %>%
          left_join(., 
                    select(adds_final, geo_hash_raw:geo_hash_geocode),
                    by = "geo_hash_raw") %>%
          select(-matches("geo_(.)*_raw"))
    )
  
  
  ## Move over mailing address if other address is blank and flag ----
  pha <- pha %>%
    map(~ .x %>%
          mutate(geo_add_mailing = 
                   ifelse(geo_hash_clean %in% c("45CA31C3315A5978F40438AAB46040D75E99C9B125C2FD01DB6E10AC80BEF906",
                                                "8926262F06508A0E264BC13D340FD8FAB9291001FC06341D2E687BD9C3AF6104") &
                            !geo_mail_hash_clean %in% c("45CA31C3315A5978F40438AAB46040D75E99C9B125C2FD01DB6E10AC80BEF906",
                                                        "8926262F06508A0E264BC13D340FD8FAB9291001FC06341D2E687BD9C3AF6104"),
                          1L, 0L),
                 geo_add1_clean = ifelse(geo_add_mailing == 1, geo_mail_add1_clean, geo_add1_clean),
                 geo_add2_clean = ifelse(geo_add_mailing == 1, geo_mail_add2_clean, geo_add2_clean),
                 geo_city_clean = ifelse(geo_add_mailing == 1, geo_mail_city_clean, geo_city_clean),
                 geo_state_clean = ifelse(geo_add_mailing == 1, geo_mail_state_clean, geo_state_clean),
                 geo_mail_zip_clean = ifelse(geo_add_mailing == 1, geo_mail_zip_clean, geo_mail_zip_clean),
                 geo_hash_clean = ifelse(geo_add_mailing == 1, geo_mail_hash_clean, geo_hash_clean),
                 geo_hash_geocode = ifelse(geo_add_mailing == 1, geo_mail_hash_geocode, geo_hash_geocode),
                 # Add in geo_blank
                 geo_blank = ifelse(
                   geo_hash_clean %in% c("45CA31C3315A5978F40438AAB46040D75E99C9B125C2FD01DB6E10AC80BEF906",
                                         "8926262F06508A0E264BC13D340FD8FAB9291001FC06341D2E687BD9C3AF6104"),
                   1L, 0L)
          )
    )
  
  
  
  # COMBINE LOAD TO SQL ----
  waitlist <- pha %>% bind_rows() %>%
    mutate(year = 2017L) # hard code year for now, change if  we get future waitlist data
  
  
  ## Select final columns and add data source ----
  # For now just load all columns, sort this later
  
  
  ## Load to SQL ----
  # Split into smaller tables to avoid SQL connection issues
  start <- 1L
  max_rows <- 50000L
  cycles <- ceiling(nrow(waitlist)/max_rows)
  
  lapply(seq(start, cycles), function(i) {
    start_row <- ifelse(i == 1, 1L, max_rows * (i-1) + 1)
    end_row <- min(nrow(waitlist), max_rows * i)
    
    message("Loading cycle ", i, " of ", cycles)
    if (i == 1) {
      dbWriteTable(conn,
                   name = DBI::Id(schema = to_schema,  table = to_table),
                   value = as.data.frame(waitlist[start_row:end_row, ]),
                   overwrite = T, append = F)
    } else {
      dbWriteTable(conn,
                   name = DBI::Id(schema = to_schema,  table = to_table),
                   value = as.data.frame(waitlist[start_row:end_row ,]),
                   overwrite = F, append = T)
    }
  })
}
