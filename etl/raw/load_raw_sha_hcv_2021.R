#### CODE TO LOAD 2021 SEATTLE HOUSING AUTHORITY VOUCHER DATA
# Alastair Matheson, PHSKC (APDE)
#
# 2022-04

### Run from main_sha_load script
# https://github.com/PHSKC-APDE/Housing/blob/main/claims_db/etl/db_loader/main_sha_load.R
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

load_raw_sha_hcv_2021 <- function(conn = NULL,
                              to_schema = NULL,
                              to_table = NULL,
                              qa_schema = NULL,
                              qa_table = NULL,
                              file_path = NULL,
                              date_min = "2021-01-01",
                              date_max = "2021-12-31",
                              etl_batch_id = NULL) {
  
  # SET UP VARIABLES ----
  date_min <- as.Date(date_min, format = "%Y-%m-%d")
  date_max <- as.Date(date_max, format = "%Y-%m-%d")
  
  
  # BRING IN DATA ----
  sha_hcv_2021 <- fread(file = file.path(file_path, "sha_hcv_2021.csv"), 
                    na.strings = c("NA", "", "NULL", "N/A", "."), 
                    stringsAsFactors = F)
  
  # Bring in mapping of building/property IDs and portfolios/program types
  sha_portfolios <- dbGetQuery(conn, "SELECT * FROM pha.ref_sha_portfolio_codes")
  
  # Bring in field names
  fields <- read.csv(file.path(here::here(), "etl/ref", "field_name_mapping.csv"))
  
  
  # QA CHECKS ----
  # Fewer QA checks this time since there is no comparison to previous years
  # Last year HCV and PH were combined so hard to compare row and ID counts
  
  ## Field names ----
  # Are there any new names not seen before?
  names <- names(sha_hcv_2021)
  names <- tolower(str_replace_all(names,"[:punct:]|[:space:]", ""))
  
  if (length(names[names %in% unique(fields$sha) == F]) > 0) {
    qa_names_result <- "FAIL"
    qa_names_note <- glue("The following new columns were detected: ", 
                          "{glue_collapse(names[names %in% unique(fields$sha) == F], sep = ', ', last = ', and ')}. ",
                          "Update the field_name_mapping.csv file.")
    warning(qa_names_note)
  } else {
    qa_names_result <- "PASS"
    qa_names_note <- "No new columns detected"
    message(qa_names_note)
  }
  
  DBI::dbExecute(conn,
                 glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`} 
                          (etl_batch_id, last_run, table_name, qa_type, qa_item, qa_result, qa_date, note) 
                          VALUES ({etl_batch_id}, NULL, '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}', 'result', 
                          'field_names', {qa_names_result}, {Sys.time()}, {qa_names_note})",
                          .con = conn))
  rm(names)
  
  
  ## Action dates ----
  # Do they fall in the expected range?
  # Some may be from earlier years so will want to manually check them 
  # (and see if they appear in previous data)
  dates <- sha_hcv_2021 %>%
    mutate(EFFECTIVE_DATE = as.Date(EFFECTIVE_DATE, format = "%m/%d/%Y")) %>%
    summarise(date_min = min(EFFECTIVE_DATE, na.rm = T),
              date_max = max(EFFECTIVE_DATE, na.rm = T))
  
  if (dates$date_min < date_min | dates$date_max > date_max) {
    qa_date_note <- glue("Dates fell outside the expected range: ", 
                         "min date = {dates$date_min} (expected {date_min}), ",
                         "max date = {dates$date_max} (expected {date_max})")
    qa_date_result <- "FAIL"
    warning(qa_date_note)
  } else if (dates$date_min - date_min > 30 | date_max - dates$date_max > 30) {
    qa_date_note <- glue("Large gap between expected and actual min or max date: ", 
                         "min date = {dates$date_min} (expected {date_min}), ",
                         "max date = {dates$date_max} (expected {date_max})")
    qa_date_result <- "FAIL"
    warning(qa_date_note)
  } else {
    qa_date_note <- glue("Date fell in expected range: ", 
                         "min date = {dates$date_min} (expected {date_min}), ",
                         "max date = {dates$date_max} (expected {date_max})")
    qa_date_result <- "PASS"
    message(qa_date_note)
  }
  
  DBI::dbExecute(conn,
                 glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`} 
                          (etl_batch_id, last_run, table_name, qa_type, qa_item, qa_result, qa_date, note) 
                          VALUES ({etl_batch_id}, NULL, '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}', 'result', 
                          'date_range', {qa_date_result}, {Sys.time()}, {qa_date_note})",
                          .con = conn))
  
  ## Action codes/types ----
  # Are there any new codes/types not seen before?
  act_types <- sort(unique(sha_hcv_2021$CERT_TYPE[!is.na(sha_hcv_2021$CERT_TYPE)]))
  act_types_expected <- c(
    "Annual HQS Inspection Only", "AR", "Annual Recertification", "Annual Reexamination", "Annual Reexamination Searching", 
    "End Participation", "Expiration of Voucher", "FSS/MTW Self-Sufficiency Only", "FSS/WtW Addendum Only",  
    "Gross Rent Change", "Historical Adjustment",  "Interim Reexamination", "Issuance of Voucher", 
    "MI", "Move In", "Move Out", "New Admission", "Other Change of Unit", "Port-Out Update (Not Submitted To MTCS)", 
    "Portability Move-in", "Portability Move-out", "Termination", "Unit Transfer", "Void",
    "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "99")
  
  
  if (is.character(sha_hcv_2021$CERT_TYPE) & length(act_types[act_types %in% act_types_expected == F]) > 0) {
    qa_act_note <- glue("The following unexpected action types were present: ",
                        "{glue_collapse(act_types[act_types %in% act_types_expected == F], sep = ', ')}. ", 
                        "Update stage.sha recoding as appropriate.")
    qa_act_result <- "FAIL"
    warning(qa_act_note)
  } else if (is.integer(sha_hcv_2021$CERT_TYPE) & min(act_types %in% c(1:15, 99)) == 0) {
    qa_act_note <- glue("The following unexpected action types were present: ",
                        "{glue_collapse(act_types[act_types %in% c(1:15, 99) == FALSE], sep = ', ')}")
    qa_act_result <- "FAIL"
    warning(qa_act_note)
  } else {
    qa_act_note <- "There were no unexpected action types."
    qa_act_result <- "PASS"
    message(qa_act_note)    
  }
  
  DBI::dbExecute(conn,
                 glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`} 
                          (etl_batch_id, last_run, table_name, qa_type, qa_item, qa_result, qa_date, note) 
                          VALUES ({etl_batch_id}, NULL, '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}', 'result', 
                          'action_types', {qa_act_result}, {Sys.time()}, {qa_act_note})",
                          .con = conn))
  
  
  ## Program types ----
  # Are there any new program types not seen before?
  prog_types <- sort(unique(sha_hcv_2021$PROGRAM_TYPE[!is.na(sha_hcv_2021$PROGRAM_TYPE)]))
  prog_types_expected = c("Collaborative Housing", "Collaborative_Housing", "Partnership_Units", 
                          "SHA Owned and Managed", "SHA_Owned_Managed", "Tenant Based", "Tenant_Based")
  
  if (length(prog_types[prog_types %in% prog_types_expected == F]) > 0) {
    qa_prog_note <- glue("The following unexpected program types were present: ",
                         "{glue_collapse(prog_types[prog_types %in% prog_types_expected == F], sep = ', ')}. ", 
                         "Update stage.sha recoding as appropriate.")
    qa_prog_result <- "FAIL"
    warning(qa_prog_note)
  } else {
    qa_prog_note <- "There were no unexpected program types."
    qa_prog_result <- "PASS"
    message(qa_prog_note)
  }
  
  DBI::dbExecute(conn,
                 glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`} 
                          (etl_batch_id, last_run, table_name, qa_type, qa_item, qa_result, qa_date, note) 
                          VALUES ({etl_batch_id}, NULL, '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}', 'result', 
                          'program_types', {qa_prog_result}, {Sys.time()}, {qa_prog_note})",
                          .con = conn))
  
  
  ## Portfolios/building IDs ----
  # Do any building IDs/property IDs fail to join to the ref table?
  portfolio_miss <- sha_hcv_2021 %>%
    filter(str_detect(tolower(PROGRAM_TYPE), "owned") & str_detect(tolower(PROGRAM_TYPE), "managed")) %>%
    distinct(BUILDING_ID) %>%
    mutate(BUILDING_ID = as.character(BUILDING_ID)) %>%
    left_join(., sha_portfolios, by = c("BUILDING_ID" = "building_id")) %>%
    filter(is.na(portfolio))
  
  portfolio_impact <- inner_join(portfolio_miss, 
                                 select(sha_hcv_2021, BUILDING_ID) %>% 
                                   mutate(BUILDING_ID = as.character(BUILDING_ID)))
  
  if (nrow(portfolio_miss) > 0) {
    qa_portfolio_note <- glue("There were {nrow(portfolio_miss)} PH building IDs found in {nrow(portfolio_impact)} ",
                              "rows that did not match to a portfolio. Need to update ref table.")
    qa_portfolio_result <- "FAIL"
    warning(qa_portfolio_note)
  } else {
    qa_portfolio_note <- "There were no unexpected building IDs."
    qa_portfolio_result <- "PASS"
    message(qa_portfolio_note)
  }
  
  DBI::dbExecute(conn,
                 glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`} 
                          (etl_batch_id, last_run, table_name, qa_type, qa_item, qa_result, qa_date, note) 
                          VALUES ({etl_batch_id}, NULL, '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}', 'result', 
                          'building_ids', {qa_portfolio_result}, {Sys.time()}, {qa_portfolio_note})",
                          .con = conn))
  
  
  # ADD VALUES TO METADATA ----
  # Row counts
  DBI::dbExecute(conn,
                 glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`} 
                          (etl_batch_id, last_run, table_name, qa_type, qa_item, qa_result, qa_date, note) 
                          VALUES ({etl_batch_id}, NULL, '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                                  'value', 'row_count', {nrow(sha_hcv_2021)},
                                  {Sys.time()}, NULL)",
                          .con = conn))
  
  # Distinct HH IDs
  DBI::dbExecute(conn,
                 glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`} 
                          (etl_batch_id, last_run, table_name, qa_type, qa_item, qa_result, qa_date, note) 
                          VALUES ({etl_batch_id}, NULL, '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                                  'value', 'household_count', {length(unique(sha_hcv_2021$HH_CERT_ID))},
                                  {Sys.time()}, NULL)",
                          .con = conn))
  
  
  # CHECK QA PASSED ----
  # Stop processing if one or more QA check failed
  if (min(qa_names_result, qa_date_result, qa_act_result, qa_prog_result, qa_portfolio_result) == "FAIL") {
    stop(glue("One or more QA checks failed on {to_schema}.{to_table}. See {`qa_schema`}.{`qa_table`} for more details."))
  } else {
    # Clean up QA objects if everything passed
    rm(list = ls(pattern = "^qa_"))
    rm(names)
    rm(dates)
    rm(act_types, act_types_expected)
    rm(prog_types, prog_types_expected)
    rm(portfolio_miss, portfolio_impact)
  }
  
  
  # RENAME FIELDS ----
  # Get rid of spaces, characters, and capitals in existing names
  # Makes it easier to accommodate changes in names provided by SHA
  sha_hcv_2021 <- sha_hcv_2021 %>%
    rename_with(., ~ str_replace_all(.,"[:punct:]|[:space:]", "")) %>%
    rename_with(., tolower) %>%
    setnames(., fields$common_name[match(names(.), fields$sha)])
  
  
  # DATA CLEANING ----
  ## Move in dates ----
  # Seem to be multiple move-in dates for a given person, take the min
  sha_hcv_2021 <- sha_hcv_2021 %>% 
    group_by(ssn, cert_id) %>%
    mutate(move_in_date = min(move_in_date, na.rm = T)) %>%
    ungroup() %>%
    distinct()
  
  
  ## Fix up formats ----
  sha_hcv_2021 <- sha_hcv_2021 %>%
    mutate(across(c(ends_with("_date"), dob), ~ as.Date(.x, format = "%m/%d/%Y")),
           geo_zip_raw = as.character(geo_zip_raw),
           building_id = as.character(building_id))
  
  
  ## Income ----
  # Need to do the following:
  # 1) Reshape income from wide to long
  # 2) Identify people with income from a fixed source
  # 3) Summarize income/assets for a given time point to reduce duplicated rows
  sha_hcv_2021_inc <- sha_hcv_2021 %>%
    select(cert_id, act_date, ssn, starts_with("inc_")) %>%
    # Duplicate versions of one income so need to drop one and avoid overcounting income
    select(-inc_i2) %>%
    pivot_longer(cols = starts_with("inc_"),
                 names_to = "inc_code",
                 names_prefix = "inc_",
                 values_to = "inc",
                 values_drop_na = TRUE) %>%
    mutate(inc_fixed = ifelse(tolower(inc_code) %in% c("p", "pension", "s", "ssi", "ss", "social security"), 
                              1, 0)) %>%
    group_by(cert_id, act_date, ssn) %>%
    summarise(inc = sum(inc, na.rm = T), 
              inc_fixed = min(inc_fixed, na.rm = T)) %>%
    group_by(cert_id) %>%
    mutate(hh_inc_calc = sum(inc, na.rm = T), 
           hh_inc_fixed = min(inc_fixed, na.rm = T)) %>%
    ungroup()
  
  
  # COMBINE DATA ----
  ### Join into a single file for each extract
  # Using a left_join because without the panel 1 info (names, SSN, etc.) the info is not much help
  # Join hh-level income info separately to avoid NAs on hh members who don't appear in panel 2
  sha_hcv_2021 <- left_join(sha_hcv_2021, 
                        select(sha_hcv_2021_inc, cert_id, act_date, ssn, starts_with("inc")) %>% distinct(), 
                        by = c("cert_id", "act_date", "ssn")) %>%
    left_join(., select(sha_hcv_2021_inc, cert_id, act_date, starts_with("hh_")) %>% distinct(),
              by = c("cert_id", "act_date"))
  
  
  # LOAD DATA TO SQL ----
  # Add source field to track where each row came from
  sha_hcv_2021 <- sha_hcv_2021 %>% 
    mutate(pha_source = "sha2021_hcv",
           etl_batch_id = etl_batch_id)
  
  # Load data
  dbWriteTable(conn,
               name = DBI::Id(schema = to_schema, table = to_table),
               value = as.data.frame(sha_hcv_2021),
               overwrite = T, append = F)
}
