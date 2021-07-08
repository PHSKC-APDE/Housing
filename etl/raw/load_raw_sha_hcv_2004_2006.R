#### CODE TO LOAD 2004-2006 SEATTLE HOUSING AUTHORITY VOUCHER DATA
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

load_raw_sha_hcv_2004_2006 <- function(conn = NULL,
                                    to_schema = NULL,
                                    to_table = NULL,
                                    qa_schema = NULL,
                                    qa_table = NULL,
                                    file_path = NULL,
                                    etl_batch_id = NULL) {
  
  # BRING IN DATA ----
  # Note the PH income and asset data are also joined to the HCV data for 2004-2006
  sha_hcv_p1_2004_2006 <- fread(file = file.path(file_path, "sha_hcv_2004_2006_suffix_corrected.csv"), 
                             na.strings = c("NA", "", "NULL", "N/A", "."), 
                             stringsAsFactors = F)
  sha_hcv_p2_2004_2006 <- fread(file = file.path(file_path, "sha_ph_2004_2006_panel_02_income_suffix_corrected.csv"), 
                               na.strings = c("NA", "", "NULL", "N/A", "."), 
                               stringsAsFactors = F)
  sha_hcv_p3_2004_2006 <- fread(file = file.path(file_path, "sha_ph_2004_2006_panel_03_assets_suffix_corrected.csv"), 
                               na.strings = c("NA", "", "NULL", "N/A", "."), 
                               stringsAsFactors = F)
  
  # Some of the SHA program/voucher data is missing from the original extract
  sha_vouchers <- fread(file = file.path(file_path, "sha_hcv_increment_program_voucher_types_2018-01-26.csv"),
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
                                  'value', 'row_count', {nrow(sha_hcv_p1_2004_2006)},
                                  {Sys.time()}, NULL)",
                          .con = conn))
  
  
  
  # RENAME FIELDS ----
  # Make list of data frames to apply multiple processes to
  sha_hcv_2004_2006 <- list(sha_hcv_p1_2004_2006 = sha_hcv_p1_2004_2006, 
                            sha_hcv_p2_2004_2006 = sha_hcv_p2_2004_2006, 
                            sha_hcv_p3_2004_2006 = sha_hcv_p3_2004_2006)
  
  # Get rid of spaces, characters, and capitals in existing names
  # Makes it easier to accommodate changes in names provided by SHA
  sha_hcv_2004_2006 <- sha_hcv_2004_2006 %>%
    map(~ .x %>% 
          rename_with(., ~ str_replace_all(.,"[:punct:]|[:space:]", "")) %>%
          rename_with(., tolower)) %>%
    map(~ setnames(.x, fields$common_name[match(names(.x), fields$sha_2004_2012)]))
  
  # Do the same for the voucher code data (the portfolio data is already named appropriately)
  sha_vouchers <- sha_vouchers %>% 
    rename_with(., ~ str_replace_all(.,"[:punct:]|[:space:]", "")) %>%
    rename_with(., tolower)
  
  sha_vouchers <- setnames(sha_vouchers, 
                           fields$common_name[match(names(sha_vouchers), fields$sha_prog_port_codes)])
  
  
  # PANEL DATA CLEANING ----
  ## Deduplicate data to avoid extra rows when joining ----
  sha_hcv_2004_2006 <- sha_hcv_2004_2006 %>% map(~ .x %>% distinct())
  
  # Bring back to individual objects to make using data table easier
  list2env(sha_hcv_2004_2006, envir = environment())
  rm(sha_hcv_2004_2006)
  
  
  ## Panel 1 ----
  # Fix up date formats and truncate increment numbers to match the reference list when joined
  sha_hcv_p1_2004_2006 <- sha_hcv_p1_2004_2006 %>%
    mutate(increment_old = increment,
           increment = str_sub(increment, 1, 5),
           across(c(ends_with("_date"), dob), ~ as.Date(.x, format = "%m/%d/%Y")),
           geo_zip_raw = as.character(geo_zip_raw))
  
  
  ## Panel 2 ----
  # Need to do the following:
  # 1) Tidy up and recode some fields
  # 2) Identify people with income from a fixed source
  # 3) Summarize income/assets for a given time point to reduce duplicated rows
  sha_hcv_p2_2004_2006 <- sha_hcv_p2_2004_2006 %>%
    mutate(inc_fixed := ifelse(inc_code %in% c("P", "S", "SS"), 1, 0)) %>%
    group_by(incasset_id, inc_mbr_num) %>%
    summarise(inc = sum(inc, na.rm = T), 
              inc_excl = sum(inc_excl, na.rm = T),
              inc_adj = sum(inc_adj, na.rm = T),
              inc_fixed = min(inc_fixed, na.rm = T)) %>%
    group_by(incasset_id) %>%
    mutate(hh_inc_calc = sum(inc, na.rm = T), 
           hh_inc_excl_calc = sum(inc_excl, na.rm = T),
           hh_inc_adj_calc = sum(inc_adj, na.rm = T),
           hh_inc_fixed = min(inc_fixed, na.rm = T)) %>%
    ungroup()
  
  
  ## Panel 3 ----
  # Only have asset information by household so sum at this level
  sha_hcv_p3_2004_2006 <- sha_hcv_p3_2004_2006 %>%
    group_by(incasset_id) %>%
    summarise(hh_asset_val = sum(asset_val, na.rm = T),
              hh_asset_inc = sum(asset_inc, na.rm = T)) %>%
    ungroup() %>%
    distinct()
  
  
  # COMBINE DATA ----
  ## Join into a single file for each extract ----
  # Using a left_join because without the panel 1 info (names, SSN, etc.) the info is not much help
  # Join hh-level income info separately to avoid NAs on hh members who don't appear in panel 2
  sha_hcv_2004_2006 <- left_join(sha_hcv_p1_2004_2006, 
                                select(sha_hcv_p2_2004_2006, starts_with("inc")) %>% distinct(), 
                                by = c("incasset_id", "mbr_num" = "inc_mbr_num")) %>%
    left_join(., select(sha_hcv_p2_2004_2006, incasset_id, starts_with("hh_")) %>% distinct(),
              by = "incasset_id") %>%
    left_join(., sha_hcv_p3_2004_2006, by = "incasset_id")
  
  
  ## Join with voucher data ----
  sha_hcv_2004_2006 <- left_join(sha_hcv_2004_2006, sha_vouchers, by = c("increment"))
  
  
  # ADD MISSING VOUCHER DATA ----
  # Some exits and other actions are missing prog_type and vouch_type fields
  # Use previous rows to fill them in
  sha_hcv_2004_2006 <- sha_hcv_2004_2006 %>%
    arrange(ssn, lname, fname, dob, act_date) %>%
    mutate(across(c(prog_type, vouch_type),
                  ~ ifelse(is.na(.x) & !is.na(lag(.x, 1)) & geo_add1_raw == lag(geo_add1_raw, 1), 
                           lag(.x, 1), .x)))
  
  
  # LOAD DATA TO SQL ----
  # Add source field to track where each row came from
  sha_hcv_2004_2006 <- sha_hcv_2004_2006 %>% 
    mutate(pha_source = "sha2006_hcv",
           etl_batch_id = etl_batch_id)
  
  # Load data
  # Split into smaller tables to avoid SQL connection issues
  start <- 1L
  max_rows <- 50000L
  cycles <- ceiling(nrow(sha_hcv_2004_2006)/max_rows)
  
  lapply(seq(start, cycles), function(i) {
    start_row <- ifelse(i == 1, 1L, max_rows * (i-1) + 1)
    end_row <- min(nrow(sha_hcv_2004_2006), max_rows * i)
    
    message("Loading cycle ", i, " of ", cycles)
    if (i == 1) {
      dbWriteTable(conn,
                   name = DBI::Id(schema = to_schema, table = to_table),
                   value = as.data.frame(sha_hcv_2004_2006[start_row:end_row, ]),
                   overwrite = T, append = F)
    } else {
      dbWriteTable(conn,
                   name = DBI::Id(schema = to_schema, table = to_table),
                   value = as.data.frame(sha_hcv_2004_2006[start_row:end_row ,]),
                   overwrite = F, append = T)
    }
  })
}
