#### CODE TO LOAD 2018 SEATTLE HOUSING AUTHORITY PUBLIC HOUSING DATA
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

load_raw_sha_ph_2018 <- function(conn = NULL,
                                    to_schema = NULL,
                                    to_table = NULL,
                                    qa_schema = NULL,
                                    qa_table = NULL,
                                    file_path = NULL,
                                    etl_batch_id = NULL) {
  
  # BRING IN DATA ----
  sha_ph_p1_2018 <- fread(file = file.path(file_path, "sha_ph_2018_panel_01_household.csv"), 
                             na.strings = c("NA", "", "NULL", "N/A", "."), 
                             stringsAsFactors = F)
  sha_ph_p2_2018 <- fread(file = file.path(file_path, "sha_ph_2018_panel_02_income_assets.csv"), 
                             na.strings = c("NA", "", "NULL", "N/A", "."), 
                             stringsAsFactors = F)
  
  # Bring in mapping of building/property IDs and portfolios/program types
  sha_portfolios <- dbGetQuery(conn, "SELECT * FROM pha.ref_sha_portfolio_codes")
  
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
                                  'value', 'row_count', {nrow(sha_ph_p1_2018)},
                                  {Sys.time()}, 'Only applies to panel 1')",
                          .con = conn))
  
  
  
  # RENAME FIELDS ----
  # Make list of data frames to apply multiple processes to
  sha_ph_2018 <- list(sha_ph_p1_2018 = sha_ph_p1_2018, 
                      sha_ph_p2_2018 = sha_ph_p2_2018)
  
  # Get rid of spaces, characters, and capitals in existing names
  # Makes it easier to accommodate changes in names provided by SHA
  sha_ph_2018 <- sha_ph_2018 %>%
    map(~ .x %>% 
          rename_with(., ~ str_replace_all(.,"[:punct:]|[:space:]", "")) %>%
          rename_with(., tolower)) %>%
    map(~ setnames(.x, fields$common_name[match(names(.x), fields$sha_ph_2012_2018)]))
  
  
  # PANEL DATA CLEANING ----
  ## Deduplicate data to avoid extra rows when joining ----
  sha_ph_2018 <- sha_ph_2018 %>% map(~ .x %>% distinct())
  
  # Bring back to individual objects to make using data table easier
  list2env(sha_ph_2018, envir = environment())
  rm(sha_ph_2018)
  
  
  ## Panel 1 ----
  # Fix up date formats
  sha_ph_p1_2018 <- sha_ph_p1_2018 %>%
    mutate(across(c(ends_with("_date"), dob), ~ as.Date(.x, format = "%m/%d/%Y")),
           geo_zip_raw = as.character(geo_zip_raw),
           property_id = as.character(property_id),
           act_type = as.integer(ifelse(act_type == "E", 3, act_type)))
  
  
  ## Panel 2 ----
  # Need to do the following:
  # 1) Tidy up and recode some fields
  # 2) Identify people with income from a fixed source
  # 3) Summarize income/assets for a given time point to reduce duplicated rows
  sha_ph_p2_2018_inc <- sha_ph_p2_2018 %>%
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
  
  sha_ph_p2_2018_asset <- sha_ph_p2_2018 %>%
    group_by(incasset_id, inc_mbr_num) %>%
    summarise(asset_val = sum(asset_val, na.rm = T), 
              asset_inc = sum(asset_inc, na.rm = T)) %>%
    group_by(incasset_id) %>%
    mutate(hh_asset_val_calc = sum(asset_val, na.rm = T),
           hh_asset_inc_calc = sum(asset_inc, na.rm = T)) %>%
    ungroup()
  
  
  
  # COMBINE DATA ----
  ### Join into a single file for each extract
  # Using a left_join because without the panel 1 info (names, SSN, etc.) the info is not much help
  # Join hh-level income info separately to avoid NAs on hh members who don't appear in panel 2
  sha_ph_2018 <- left_join(sha_ph_p1_2018, 
                             select(sha_ph_p2_2018_inc, starts_with("inc")) %>% distinct(), 
                             by = c("incasset_id", "mbr_num" = "inc_mbr_num")) %>%
    left_join(., select(sha_ph_p2_2018_inc, incasset_id, starts_with("hh_")) %>% distinct(),
              by = "incasset_id") %>%
    left_join(., select(sha_ph_p2_2018_asset, starts_with("inc"), starts_with("asset")), 
              by = c("incasset_id", "mbr_num" = "inc_mbr_num")) %>%
    left_join(., select(sha_ph_p2_2018_asset, incasset_id, starts_with("hh_")) %>% distinct(),
              by = "incasset_id")
  
  
  ## Join with portfolio data ----
  sha_ph_2018 <- left_join(sha_ph_2018, 
                    distinct(sha_portfolios, property_id, property_name, prog_type, portfolio), 
                    by = c("property_id"))
  
  
  # LOAD DATA TO SQL ----
  # Add source field to track where each row came from
  sha_ph_2018 <- sha_ph_2018 %>% 
    mutate(pha_source = "sha2018_ph",
           etl_batch_id = etl_batch_id)
  
  # Load data
  dbWriteTable(conn,
               name = DBI::Id(schema = to_schema, table = to_table),
               value = as.data.frame(sha_ph_2018),
               overwrite = T, append = F)
}
