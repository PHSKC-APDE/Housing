#### CODE TO LOAD 2020 KING COUNTY HOUSING AUTHORITY DATA
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

load_raw_kcha_2020 <- function(conn = NULL,
                               to_schema = NULL,
                               to_table = NULL,
                               qa_schema = NULL,
                               qa_table = NULL,
                               file_path = NULL,
                               date_min = "2020-01-01",
                               date_max = "2020-12-31",
                               etl_batch_id = NULL) {
  
  # SET UP VARIABLES ----
  date_min <- as.Date(date_min, format = "%Y-%m-%d")
  date_max <- as.Date(date_max, format = "%Y-%m-%d")
  
  # BRING IN DATA ----
  kcha_p1_2020 <- fread(file = file.path(file_path, "kcha_2020_panel_01.csv"), 
                        na.strings = c("NA", "", "NULL", "N/A", "."), 
                        stringsAsFactors = F)
  kcha_p2_2020 <- fread(file = file.path(file_path, "kcha_2020_panel_02.csv"), 
                        na.strings = c("NA", "", "NULL", "N/A", "."), 
                        stringsAsFactors = F)
  kcha_p3_2020 <- fread(file = file.path(file_path, "kcha_2020_panel_03.csv"), 
                        na.strings = c("NA", "", "NULL", "N/A", "."), 
                        stringsAsFactors = F)
  
  
  fields <- rads::sql_clean(setDT(read.csv(file.path(here::here(), "etl/ref", "field_name_mapping.csv"))))
  
  
  # QA CHECKS ----
  ## Row counts across panels ----
  # Do the number of rows match between each panel?
  if (nrow(kcha_p1_2020) == nrow(kcha_p2_2020) & nrow(kcha_p1_2020) == nrow(kcha_p3_2020)) {
    qa_row_result <- "PASS"
    qa_row_note <- glue("Equal number of rows across all 3 panels: {format(nrow(kcha_p1_2020), big.mark = ',')}")
    message(qa_row_note)
  } else if (nrow(kcha_p1_2020) != nrow(kcha_p1_2020) | nrow(kcha_p1_2020) != nrow(kcha_p3_2020)) {
    qa_row_result <- "FAIL"
    qa_row_note <- glue("Unequal number of rows across all 3 panels: Panel 1 = {format(nrow(kcha_p1_2020), big.mark = ',')}, ",
                   "Panel 2 = {format(nrow(kcha_p2_2020), big.mark = ',')}, ", 
                   "Panel 3 = {format(nrow(kcha_p3_2020), big.mark = ',')}")
    warning(qa_row_note)
  }
  
  DBI::dbExecute(conn,
                 glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`} 
                          (etl_batch_id, last_run, table_name, qa_type, qa_item, qa_result, qa_date, note) 
                          VALUES ({etl_batch_id}, NULL, '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}', 'result', 
                          'row_count', {qa_row_result}, {Sys.time()}, {qa_row_note})",
                          .con = conn))
  
  
  ## Row counts vs. previous year ----
  # How do the number of rows compare to last time?
  rows_2019 <- as.integer(dbGetQuery(conn,
    glue_sql("SELECT qa_result FROM {`qa_schema`}.{`qa_table`} 
             WHERE table_name = 'pha.raw_kcha_2019' AND qa_type = 'value' AND 
             qa_item = 'row_count'", .con = conn)))
  
  row_diff <- nrow(kcha_p1_2020) - rows_2019
  row_pct <- round(abs(row_diff) / nrow(kcha_p1_2020) * 100, 1)
  qa_row_diff_note <- glue("There were {row_pct}% ({format(abs(row_diff), big.mark = ',')}) ", 
                      "{ifelse(row_diff < 0, 'fewer', 'more')} rows in 2020 than 2019")
  
  # Arbitrarily set 10% changes as threshold for alert
  if (!is.na(row_pct) & row_pct > 10) {
    qa_row_diff_result <- "FAIL"
    warning(qa_row_diff_note)
  } else if (!is.na(row_pct) & row_pct <= 10) {
    qa_row_diff_result <- "PASS"
    message(qa_row_diff_note)
  } else {
    qa_row_diff_result <- "FAIL"
    message("Something went wrong when checking the number of last year's rows. Check code.")
  }
  
  DBI::dbExecute(conn,
                 glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`} 
                          (etl_batch_id, last_run, table_name, qa_type, qa_item, qa_result, qa_date, note) 
                          VALUES ({etl_batch_id}, NULL, '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}', 'result', 
                          'row_count_vs_previous', {qa_row_diff_result}, {Sys.time()}, {qa_row_diff_note})",
                          .con = conn))
  
  
  ## Field names ----
  # Are there any new names not seen before?
  # Note that the fields list has names for when the data are pivoted so need to account for this
  names <- c(names(kcha_p1_2020), names(kcha_p2_2020), names(kcha_p3_2020))
  names[str_detect(names, "^h[0-9]{1}[a-z][0-9]{2}$")] <- str_sub(names[str_detect(names, "^h[0-9]{1}[a-z][0-9]{2}$")], 1, 3)
  names[str_detect(names, "^h[0-9]{2}[a-z][0-9]{2}$")] <- str_sub(names[str_detect(names, "^h[0-9]{2}[a-z][0-9]{2}$")], 1, 4)
  # Just simplify all the expected race/eth and income groups into one
  names[str_detect(names, "^h3k[0-9]{2}[a-e]")] <- "h3k1"
  names[str_detect(names, "^h19[a-f][0-9]{1,2}[a-b]")] <- "h19a1"
  names <- unique(names)
  
  if (length(names[names %in% fields$kcha_modified == F]) > 0) {
    qa_names_result <- "FAIL"
    qa_names_note <- glue("The following new columns were detected: ", 
                          "{glue_collapse(names[names %in% fields$kcha_modified == F], sep = ', ', last = ', and ')}. ",
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
  
  
  ## Action dates ----
  # Do they fall in the expected range?
  # Some may be from earlier years so will want to manually check them 
  # (and see if they appear in previous data)
  dates <- kcha_p1_2020 %>%
    mutate(h2b = as.Date(h2b, format = "%m/%d/%Y")) %>%
    summarise(date_min = min(h2b, na.rm = T),
              date_max = max(h2b, na.rm = T))
  
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
  
  
  ## Program types ----
  # Make sure there aren't any new variations
  prog_types <- unique(kcha_p1_2020$program_type)
  
  if (min(prog_types %in% c("P", "PBS8", "PH", "PR", "T", "TBS8", "VO")) == 0) {
    qa_prog_note <- glue("The following unexpected program types were present: ",
                         "{glue_collapse(prog_types[prog_types %in% c('P', 'PBS8', 'PH', 'PR', 'T', 'TBS8', 'VO') == FALSE], 
                         sep = ', ')}")
    qa_prog_result <- "FAIL"
    warning(qa_prog_note)
  } else {
    qa_prog_note <- "There were no unexpected program types"
    qa_prog_result <- "PASS"
    message(qa_prog_note)
  }
  
  DBI::dbExecute(conn,
                 glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`} 
                          (etl_batch_id, last_run, table_name, qa_type, qa_item, qa_result, qa_date, note) 
                          VALUES ({etl_batch_id}, NULL, '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}', 'result', 
                          'date_range', {qa_prog_result}, {Sys.time()}, {qa_prog_note})",
                          .con = conn))
  
  
  ## Action codes ----
  # Do they have the expected range?
  act_types <- sort(unique(kcha_p1_2020$h2a[!is.na(kcha_p1_2020$h2a)]))
  
  if (min(act_types %in% 1:14) == 0) {
    qa_act_note <- glue("The following unexpected action types were present: ",
                         "{glue_collapse(act_types[act_types %in% 1:14 == FALSE], sep = ', ')}")
    qa_act_result <- "FAIL"
    warning(qa_act_note)
  } else {
    qa_act_note <- "There were no unexpected action types (all between 1 and 14)"
    qa_act_result <- "PASS"
    message(qa_act_note)
  }
  
  DBI::dbExecute(conn,
                 glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`} 
                          (etl_batch_id, last_run, table_name, qa_type, qa_item, qa_result, qa_date, note) 
                          VALUES ({etl_batch_id}, NULL, '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}', 'result', 
                          'action_types', {qa_act_result}, {Sys.time()}, {qa_act_note})",
                          .con = conn))
  
  
  # ADD VALUES TO METADATA ----
  # Row counts
  DBI::dbExecute(conn,
                 glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`} 
                          (etl_batch_id, last_run, table_name, 
                            qa_type, qa_item, qa_result, qa_date, note) 
                          VALUES ({etl_batch_id}, NULL, '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                                  'value', 'row_count', {nrow(kcha_p1_2020)},
                                  {Sys.time()}, NULL)",
                          .con = conn))
  
  
  
  # CHECK QA PASSED ----
  # Stop processing if one or more QA check failed
  if (min(qa_row_result, qa_row_diff_result, qa_names_result, qa_date_result) == "FAIL") {
    stop(glue("One or more QA checks failed on {to_schema}.{to_table}. See {`qa_schema`}.{`qa_table`} for more details."))
  } else {
    # Clean up QA objects if everything passed
    rm(list = ls(pattern = "^qa_"))
    rm(list = ls(pattern = "^row(s)?_"))
    rm(names)
    rm(dates)
    rm(prog_types)
    rm(act_types)
  }
  
  
  # PANEL DATA CLEANING ----
  ### Program types ----
  # Some typos/inconsistencies in the program types
  kcha_p1_2020 <- kcha_p1_2020 %>%
    mutate(program_type = case_when(program_type == "P" ~ "PH",
                                    program_type == "PR" ~ "PBS8",
                                    program_type %in% c("T", "VO") ~ "TBS8",
                                    TRUE ~ program_type))
  
  
  # COMBINE DATA ----
  ### Join into a single file for each extract
  # Using a left_join because without the panel 1 info (names, SSN, etc.) the info is not much help
  kcha_2020_full <- list(kcha_p1_2020, kcha_p2_2020, kcha_p3_2020) %>%
    Reduce(function(dtf1, dtf2) left_join(
      dtf1, dtf2, by = c("householdid", "certificationid", "vouchernumber", "h2a", "h2b")), .)
  
  # Check there wasn't a blowout in the number of rows due to join issues
  if (nrow(kcha_2020_full) != nrow(kcha_p1_2020)) {
    stop("Joining the panels together produced an unexpected number of rows")
  }
  
  # Confirm that that dataset contains voucher_type ----
  if(length(intersect(fields[common_name %like% 'vouch_type' & !is.na(kcha_modified)]$kcha_modified, names(kcha_2020_full)))==0){
    stop("\n\U0001f47f You are column corresponding to 'vouch_type', which is a critical variable. Do not continue without correcting the code or updating the data.")
  } else {message("\U0001f642 You have a column corresponding to 'vouch_type', which is a critical variable.")}
  
  
  # FORMAT DATA ----
  ## Dates ----
  kcha_2020_full <- kcha_2020_full %>%
    mutate_at(vars(h2b, h2h, starts_with("h3e")),
              list(~ as.Date(., format = "%m/%d/%Y")))
  
  
  # SET UP HEAD OF HOUSEHOLD DATA ----
  # Helpful for joining to EOP data and reshaping
  kcha_2020_full <- kcha_2020_full %>%
    mutate(hh_lname = h3b01,
           hh_fname = h3c01,
           hh_mname = h3d01,
           hh_ssn = h3n01,
           hh_dob = h3e01)
  
  
  # LOAD DATA TO SQL ----
  # Add source field to track where each row came from
  kcha_2020_full <- kcha_2020_full %>% 
    mutate(pha_source = "kcha2020",
           etl_batch_id = etl_batch_id)
  
  # Load data
  dbWriteTable(conn,
               name = DBI::Id(schema = to_schema, table = to_table),
               value = as.data.frame(kcha_2020_full),
               overwrite = T, append = F)
}
