#### CODE TO LOAD 2018 KING COUNTY HOUSING AUTHORITY DATA
# Alastair Matheson, PHSKC (APDE)
#
# 2021-06

### Run from main_kcha_load script
# https://github.com/PHSKC-APDE/Housing/blob/main/claims_db/etl/db_loader/main_kcha_load.R
# Assumes relevant libraries are already loaded

# conn = ODBC connection to use
# to_schema = name of the schema to load data to
# to_table = name of the table to load data to
# qa_schema = name of the schema the QA lives in (likely the same as the to_schema if working in HHSAW)
# qa_table = name of the table that holds QA outcomes
# file_path = where the KCHA data files live (note that the file names themselves are hard coded for now)
# date_min = the minimum action date expected in the data
# date_max = the maximum action date expected in the data
# etl_batch_id = the value in the ETL logging table that corresponds to these data

load_raw_kcha_2018 <- function(conn = NULL,
                               to_schema = NULL,
                               to_table = NULL,
                               qa_schema = NULL,
                               qa_table = NULL,
                               file_path = NULL,
                               etl_batch_id = NULL) {
  
  # BRING IN DATA ----
  kcha_p1_2018 <- fread(file = file.path(file_path, "kcha_2018_panel_01.csv"), 
                        na.strings = c("NA", "", "NULL", "N/A", "."), 
                        stringsAsFactors = F)
  kcha_p2_2018 <- fread(file = file.path(file_path, "kcha_2018_panel_02.csv"), 
                        na.strings = c("NA", "", "NULL", "N/A", "."), 
                        stringsAsFactors = F)
  kcha_p3_2018 <- fread(file = file.path(file_path, "kcha_2018_panel_03.csv"), 
                        na.strings = c("NA", "", "NULL", "N/A", "."), 
                        stringsAsFactors = F)
  
  
  
  # QA CHECKS ----
  # Older KCHA files have already been fairly robustly examined so no specific QA 
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
                                  'value', 'row_count', {nrow(kcha_p1_2018)},
                                  {Sys.time()}, NULL)",
                          .con = conn))
  
  
  
  # PANEL DATA CLEANING ----
  ### Program types ----
  # Some typos/inconsistencies in the program types
  kcha_p1_2018 <- kcha_p1_2018 %>%
    mutate(program_type = case_when(program_type == "P" ~ "PH",
                                    program_type == "PR" ~ "PBS8",
                                    program_type %in% c("T", "VO") ~ "TBS8",
                                    TRUE ~ program_type))
  
  
  # COMBINE DATA ----
  ### Join into a single file for each extract
  # Using a left_join because without the panel 1 info (names, SSN, etc.) the info is not much help
  kcha_2018_full <- list(kcha_p1_2018, kcha_p2_2018, kcha_p3_2018) %>%
    Reduce(function(dtf1, dtf2) left_join(
      dtf1, dtf2, by = c("householdid", "certificationid", "vouchernumber", "h2a", "h2b")), .)
  
  
  # FORMAT DATA ----
  ## Dates ----
  kcha_2018_full <- kcha_2018_full %>%
    mutate_at(vars(h2b, h2h, starts_with("h3e")),
              list(~ as.Date(., format = "%m/%d/%Y")))
  
  
  # SET UP HEAD OF HOUSEHOLD DATA ----
  # Helpful for joining to EOP data and reshaping
  kcha_2018_full <- kcha_2018_full %>%
    mutate(hh_lname = h3b01,
           hh_fname = h3c01,
           hh_mname = h3d01,
           hh_ssn = h3n01,
           hh_dob = h3e01)
  
  
  # LOAD DATA TO SQL ----
  # Add source field to track where each row came from
  kcha_2018_full <- kcha_2018_full %>% 
    mutate(pha_source = "kcha2018",
           etl_batch_id = etl_batch_id)
  
  # Load data
  dbWriteTable(conn,
               name = DBI::Id(schema = to_schema, table = to_table),
               value = as.data.frame(kcha_2018_full),
               overwrite = T, append = F)
}
