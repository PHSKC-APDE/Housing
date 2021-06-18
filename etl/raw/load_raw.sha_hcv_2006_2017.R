#### CODE TO LOAD 2006-2017 SEATTLE HOUSING AUTHORITY VOUCHER DATA
# Alastair Matheson, PHSKC (APDE)
#
# 2021-06

### Run from main_sha_load script
# https://github.com/PHSKC-APDE/Housing/blob/master/claims_db/etl/db_loader/main_sha_load.R
# Assumes relevant libraries are already loaded

# conn = ODBC connection to use
# to_schema = name of the schema to load data to
# to_table = name of the table to load data to
# qa_schema = name of the schema the QA lives in (likely the same as the to_schema if working in HHSAW)
# qa_table = name of the table that holds QA outcomes
# file_path = where the SHA data files live (note that the file names themselves are hard coded for now)
# date_min = the minimum action date expected in the data
# date_max = the maximum action date expected in the data
# etl_batch_id = the value in the ETL logging table that corresponds to these data

load_raw.sha_hcv_2006_2017 <- function(conn = NULL,
                                    to_schema = NULL,
                                    to_table = NULL,
                                    qa_schema = NULL,
                                    qa_table = NULL,
                                    file_path = NULL,
                                    etl_batch_id = NULL) {
  
  # BRING IN DATA ----
  sha_hcv_p1_2006_2016 <- fread(file = file.path(file_path, "sha_hcv_2006_2016_panel_01_household.csv"), 
                                na.strings = c("NA", "", "NULL", "N/A", "."), 
                                stringsAsFactors = F)
  sha_hcv_p2_2006_2016 <- fread(file = file.path(file_path, "sha_hcv_2006_2016_panel_02_income_assets.csv"), 
                                na.strings = c("NA", "", "NULL", "N/A", "."), 
                                stringsAsFactors = F)
  
  
  sha_hcv_p1_2006_2017 <- fread(file = file.path(file_path, "sha_hcv_2006_2017_panel_01_household.csv"), 
                             na.strings = c("NA", "", "NULL", "N/A", "."), 
                             stringsAsFactors = F)
  sha_hcv_p2_2006_2017 <- fread(file = file.path(file_path, "sha_hcv_2006_2017_panel_02_income_assets.csv"), 
                               na.strings = c("NA", "", "NULL", "N/A", "."), 
                               stringsAsFactors = F)
  
  # Bring in field names
  fields <- read.csv(file.path(here::here(), "etl/ref", "field_name_mapping.csv"))
  
  
  # QA CHECKS ----
  # Older SHA files have already been fairly robustly examined so no specific QA 
  # is included here. Some QA values are loaded for future checks.
  # However, things to consider for new years are as follows:
  # - Row counts (ideally is the same across all panels but isn't always)
  # - Column names (do they line up with the expected fields in the mapping csv?)
  # - Number of IDs (how does it compare to last year?)
  # - Action dates (do they fall in the expected range?)
  # - Program types (are there some typos/inconsistencies?)
  # - Portfolio names, action codes (do they have the same structure as before?)
  
  ## Add QA values ----
  # Row counts
  DBI::dbExecute(conn,
                 glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`} 
                          (etl_batch_id, last_run, table_name, 
                            qa_type, qa_item, qa_result, qa_date, note) 
                          VALUES ({etl_batch_id}, NULL, '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                                  'value', 'row_count', {nrow(sha_hcv_p1_2006_2017)},
                                  {Sys.time()}, NULL)",
                          .con = conn))
  
  
  
  # RENAME FIELDS ----
  # Make list of data frames to apply multiple processes to
  sha_hcv_2006_2017 <- list(sha_hcv_p1_2006_2016 = sha_hcv_p1_2006_2016, 
                            sha_hcv_p2_2006_2016 = sha_hcv_p2_2006_2016,
                            sha_hcv_p1_2006_2017 = sha_hcv_p1_2006_2017, 
                            sha_hcv_p2_2006_2017 = sha_hcv_p2_2006_2017)
  
  # Get rid of spaces, characters, and capitals in existing names
  # Makes it easier to accommodate changes in names provided by SHA
  sha_hcv_2006_2017 <- sha_hcv_2006_2017 %>%
    map(~ .x %>% 
          rename_with(., ~ str_replace_all(.,"[:punct:]|[:space:]", "")) %>%
          rename_with(., tolower)) %>%
    map(~ setnames(.x, fields$common_name[match(names(.x), fields$sha_hcv_2006_2018)]))
  
  
  # DATA CLEANING ----
  ## Deduplicate data to avoid extra rows when joining ----
  sha_hcv_2006_2017 <- sha_hcv_2006_2017 %>% 
    map(~ .x %>% 
          mutate(across(where(is.character), str_squish)) %>% 
          mutate(across(where(is.character), ~ ifelse(.x == "", NA_character_, .x))) %>% 
          distinct())
  
  # Bring back to individual objects to make using data table easier
  list2env(sha_hcv_2006_2017, envir = environment())
  rm(sha_hcv_2006_2017)
  
  
  ## Panel 1: 2006-2016 ----
  # Fix up date and ZIP formats (NB. dates look different in each file)
  sha_hcv_p1_2006_2016 <- sha_hcv_p1_2006_2016 %>%
    mutate(across(c(ends_with("_date"), dob), ~ as.Date(.x, format = "%m/%d/%Y")),
           tb_util_allow = round(as.numeric(tb_util_allow), 1),
           geo_zip_raw = as.character(geo_zip_raw))
  
  
  ## Restructure race field so people with multiple races are not repeated
  # Use data table since it is much faster
  sha_hcv_p1_2006_2016_race <- setDT(sha_hcv_p1_2006_2016 %>% distinct(cert_id, hh_id, mbr_id, race))
  sha_hcv_p1_2006_2016_race[, ':=' (r_white = ifelse(race == 1 & !is.na(race), 1L, 0L),
                                    r_black = ifelse(race == 2 & !is.na(race), 1L, 0L),
                                    r_aian = ifelse(race == 3 & !is.na(race), 1L, 0L),
                                    r_asian = ifelse(race == 4 & !is.na(race), 1L, 0L),
                                    r_nhpi = ifelse(race == 5 & !is.na(race), 1L, 0L))]
  sha_hcv_p1_2006_2016_race[, race := NULL]
  sha_hcv_p1_2006_2016_race[, ':=' (r_white = max(r_white),
                                    r_black = max(r_black),
                                    r_aian = max(r_aian),
                                    r_asian = max(r_asian),
                                    r_nhpi = max(r_nhpi)),
                            by = c("cert_id", "hh_id", "mbr_id")]
  sha_hcv_p1_2006_2016_race <- unique(sha_hcv_p1_2006_2016_race)
  
  # Join back and remove duplicates
  sha_hcv_p1_2006_2016 <- left_join(sha_hcv_p1_2006_2016, sha_hcv_p1_2006_2016_race,
                                    by = c("cert_id", "hh_id", "mbr_id")) %>%
    select(-race) %>% distinct()
  rm(sha_hcv_p1_2006_2016_race)
  
  
  ## Panel 1: 2006-2017 ----
  # Fix up date and ZIP formats (NB. dates look different in each file)
  sha_hcv_p1_2006_2017 <- sha_hcv_p1_2006_2017 %>%
    mutate(across(c(ends_with("_date"), dob), ~ as.Date(.x, format = "%Y-%m-%d")),
           tb_util_allow = round(as.numeric(tb_util_allow), 1),
           geo_zip_raw = as.character(geo_zip_raw))
  
  
  ## Restructure race field so people with multiple races are not repeated
  # Use data table since it is much faster
  sha_hcv_p1_2006_2017_race <- setDT(sha_hcv_p1_2006_2017 %>% distinct(cert_id, hh_id, mbr_id, race))
  sha_hcv_p1_2006_2017_race[, ':=' (r_white = ifelse(race == 1 & !is.na(race), 1L, 0L),
                                    r_black = ifelse(race == 2 & !is.na(race), 1L, 0L),
                                    r_aian = ifelse(race == 3 & !is.na(race), 1L, 0L),
                                    r_asian = ifelse(race == 4 & !is.na(race), 1L, 0L),
                                    r_nhpi = ifelse(race == 5 & !is.na(race), 1L, 0L))]
  sha_hcv_p1_2006_2017_race[, race := NULL]
  sha_hcv_p1_2006_2017_race[, ':=' (r_white = max(r_white),
                                    r_black = max(r_black),
                                    r_aian = max(r_aian),
                                    r_asian = max(r_asian),
                                    r_nhpi = max(r_nhpi)),
                            by = c("cert_id", "hh_id", "mbr_id")]
  sha_hcv_p1_2006_2017_race <- unique(sha_hcv_p1_2006_2017_race)
  
  # Join back and remove duplicates
  sha_hcv_p1_2006_2017 <- left_join(sha_hcv_p1_2006_2017, sha_hcv_p1_2006_2017_race,
                                    by = c("cert_id", "hh_id", "mbr_id")) %>%
    select(-race) %>% distinct()
  rm(sha_hcv_p1_2006_2017_race)
  
  
  ## Panel 2: 2006-2016 ----
  # Need to do the following:
  # 1) Tidy up and recode some fields
  # 2) Identify people with income from a fixed source
  # 3) Summarize income/assets for a given time point to reduce duplicated rows
  sha_hcv_p2_2006_2016_inc <- sha_hcv_p2_2006_2016 %>%
    mutate(inc_fixed = ifelse(tolower(inc_code) %in% c("p", "pension", "s", "ssi", "ss", "social security"), 
                              1, 0),
           across(c(inc, inc_excl, inc_adj), ~ as.numeric(.x))) %>%
    group_by(cert_id, mbr_id) %>%
    summarise(inc = sum(inc, na.rm = T), 
              inc_excl = sum(inc_excl, na.rm = T),
              inc_adj = sum(inc_adj, na.rm = T),
              inc_fixed = min(inc_fixed, na.rm = T)) %>%
    group_by(cert_id) %>%
    mutate(hh_inc_calc = sum(inc, na.rm = T), 
           hh_inc_excl_calc = sum(inc_excl, na.rm = T),
           hh_inc_adj_calc = sum(inc_adj, na.rm = T),
           hh_inc_fixed = min(inc_fixed, na.rm = T)) %>%
    ungroup()
  
  sha_hcv_p2_2006_2016_asset <- sha_hcv_p2_2006_2016 %>%
    mutate(across(c(asset_val, asset_inc), ~ as.numeric(.x))) %>%
    group_by(cert_id, mbr_id) %>%
    summarise(asset_val = sum(asset_val, na.rm = T), 
              asset_inc = sum(asset_inc, na.rm = T)) %>%
    group_by(cert_id) %>%
    mutate(hh_asset_val_calc = sum(asset_val, na.rm = T),
           hh_asset_inc_calc = sum(asset_inc, na.rm = T)) %>%
    ungroup()
  
  
  ## Panel 2: 2006-2017 ----
  # Need to do the following:
  # 1) Tidy up and recode some fields
  # 2) Identify people with income from a fixed source
  # 3) Summarize income/assets for a given time point to reduce duplicated rows
  # NB. the 2006-2017 data has an increment field and income seems to be listed 2+ times
  #      where a person has 2+ increments, leading to an overestimate oin the hh_inc field.
  #      Use increment as a grouping variable to avoid this in the hh_inc_calc field.
  sha_hcv_p2_2006_2017_inc <- sha_hcv_p2_2006_2017 %>%
    mutate(inc_fixed = ifelse(tolower(inc_code) %in% c("p", "pension", "s", "ssi", "ss", "social security"), 
                              1, 0),
           across(c(inc, inc_excl, inc_adj), ~ as.numeric(.x))) %>%
    group_by(cert_id, mbr_id, increment) %>%
    summarise(inc = sum(inc, na.rm = T), 
              inc_excl = sum(inc_excl, na.rm = T),
              inc_adj = sum(inc_adj, na.rm = T),
              inc_fixed = min(inc_fixed, na.rm = T)) %>%
    group_by(cert_id, increment) %>%
    mutate(hh_inc_calc = sum(inc, na.rm = T), 
           hh_inc_excl_calc = sum(inc_excl, na.rm = T),
           hh_inc_adj_calc = sum(inc_adj, na.rm = T),
           hh_inc_fixed = min(inc_fixed, na.rm = T)) %>%
    ungroup()
  
  sha_hcv_p2_2006_2017_asset <- sha_hcv_p2_2006_2017 %>%
    mutate(across(c(asset_val, asset_inc), ~ as.numeric(.x))) %>%
    group_by(cert_id, mbr_id, increment) %>%
    summarise(asset_val = sum(asset_val, na.rm = T), 
              asset_inc = sum(asset_inc, na.rm = T)) %>%
    group_by(cert_id, increment) %>%
    mutate(hh_asset_val_calc = sum(asset_val, na.rm = T),
           hh_asset_inc_calc = sum(asset_inc, na.rm = T)) %>%
    ungroup()
  
  
  # COMBINE DATA ----
  ## Join into a single file for each extract ----
  # Using a left_join because without the panel 1 info (names, SSN, etc.) the info is not much help
  # Join hh-level income info separately to avoid NAs on hh members who don't appear in panel 2
  sha_hcv_2006_2016 <- left_join(sha_hcv_p1_2006_2016, 
                                 select(sha_hcv_p2_2006_2016_inc, cert_id, mbr_id, starts_with("inc")) %>% distinct(), 
                                 by = c("cert_id", "mbr_id")) %>%
    left_join(., select(sha_hcv_p2_2006_2016_inc, cert_id, starts_with("hh_")) %>% distinct(),
              by = "cert_id") %>%
    left_join(., select(sha_hcv_p2_2006_2016_asset, cert_id, mbr_id, starts_with("asset")), 
              by = c("cert_id", "mbr_id")) %>%
    left_join(., select(sha_hcv_p2_2006_2016_asset, cert_id, starts_with("hh_")) %>% distinct(),
              by = "cert_id")
  
  
  sha_hcv_2006_2017 <- left_join(sha_hcv_p1_2006_2017, 
                                select(sha_hcv_p2_2006_2017_inc, cert_id, mbr_id, starts_with("inc")) %>% distinct(), 
                                by = c("cert_id", "mbr_id", "increment")) %>%
    left_join(., select(sha_hcv_p2_2006_2017_inc, cert_id, increment, starts_with("hh_")) %>% distinct(),
              by = c("cert_id", "increment")) %>%
    left_join(., select(sha_hcv_p2_2006_2017_asset, cert_id, mbr_id, increment, starts_with("asset")), 
              by = c("cert_id", "mbr_id", "increment")) %>%
    left_join(., select(sha_hcv_p2_2006_2017_asset, cert_id, increment, starts_with("hh_")) %>% distinct(),
              by = c("cert_id", "increment"))
  
  
  ## Join 2006-2016 and 2006-2017 data ----
  # Really just need to add in rows from 2006-2016 that are missing in later file
  # Add a source here to track where data came from (though use same ETL ID later)
  sha_hcv_2006_2016_join <- sha_hcv_2006_2016 %>%
    distinct(cert_id, hh_id, mbr_id, act_date) %>%
    anti_join(.,
              select(sha_hcv_p1_2006_2017, cert_id, hh_id, mbr_id, act_date) %>% distinct()) %>%
    mutate(keep = 1L,
           pha_source = "sha2016_hcv") %>%
    right_join(., sha_hcv_2006_2016, by = c("cert_id", "hh_id", "mbr_id", "act_date")) %>%
    filter(keep == 1) %>%
    select(-keep)
  
  # Add to existing data
  sha_hcv_2006_2017 <- sha_hcv_2006_2017 %>%
    mutate(pha_source = "sha2017_hcv") %>%
    bind_rows(., sha_hcv_2006_2016_join)
  
  
  
  # LOAD DATA TO SQL ----
  # Add source field to track where each row came from
  sha_hcv_2006_2017 <- sha_hcv_2006_2017 %>% 
    mutate(etl_batch_id = etl_batch_id)
  
  # Load data
  # Split into smaller tables to avoid SQL connection issues
  start <- 1L
  max_rows <- 50000L
  cycles <- ceiling(nrow(sha_hcv_2006_2017)/max_rows)
  
  lapply(seq(start, cycles), function(i) {
    start_row <- ifelse(i == 1, 1L, max_rows * (i-1) + 1)
    end_row <- min(nrow(sha_hcv_2006_2017), max_rows * i)
    
    message("Loading cycle ", i, " of ", cycles)
    if (i == 1) {
      dbWriteTable(conn,
                   name = DBI::Id(schema = to_schema, table = to_table),
                   value = as.data.frame(sha_hcv_2006_2017[start_row:end_row, ]),
                   overwrite = T, append = F)
    } else {
      dbWriteTable(conn,
                   name = DBI::Id(schema = to_schema, table = to_table),
                   value = as.data.frame(sha_hcv_2006_2017[start_row:end_row ,]),
                   overwrite = F, append = T)
    }
  })
}
