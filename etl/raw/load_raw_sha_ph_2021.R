#### CODE TO LOAD 2021 SEATTLE HOUSING AUTHORITY PUBLIC HOUSING DATA
# Alastair Matheson, PHSKC (APDE)
#
# 2021-06

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

load_raw_sha_ph_2021 <- function(conn = NULL,
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
  sha_ph_2021 <- fread(file = file.path(file_path, "sha_ph_2021.csv"), 
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
  names <- names(sha_ph_2021)
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
  effective_date_index <- which(str_detect(names, "2b") | names == "effectivedate")
  
  if (length(effective_date_index) > 0) {
    dates <- sha_ph_2021 %>%
      select(date_var = effective_date_index[[1]]) %>%
      mutate(effective_date = as.Date(date_var, format = "%m/%d/%Y")) %>%
      summarise(date_min = min(effective_date, na.rm = T),
                date_max = max(effective_date, na.rm = T))
    
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
  } else {
    qa_date_note <- "Could not find columns with action dates Check data names"
    qa_date_result <- "FAIL"
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
  cert_type_index <- which(str_detect(names, "2a") | names == "certtype")
  
  if (length(cert_type_index) > 0) {
    act_types <- sort(unlist(unique(sha_ph_2021[, cert_type_index, with = F]), use.names = F))
    act_types <- act_types[!is.na(act_types)]
    
    act_types_expected <- c(
      "Annual HQS Inspection Only", "AR", "Annual Recertification", "Annual Reexamination", "Annual Reexamination Searching", 
      "End Participation", "Expiration of Voucher", "FSS/MTW Self-Sufficiency Only", "FSS/WtW Addendum Only",  
      "Gross Rent Change", "Historical Adjustment",  "Interim Reexamination", "Issuance of Voucher", 
      "MI", "Move In", "Move Out", "New Admission", "Other Change of Unit", "Port-Out Update (Not Submitted To MTCS)", 
      "Portability Move-in", "Portability Move-out", "Termination", "Unit Transfer", "Void",
      "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "99")
    
    
    if (length(act_types[act_types %in% act_types_expected == F]) > 0) {
      qa_act_note <- glue("The following unexpected action types were present: ",
                          "{glue_collapse(act_types[act_types %in% act_types_expected == F], sep = ', ')}. ", 
                          "Update stage.sha recoding as appropriate.")
      qa_act_result <- "FAIL"
      warning(qa_act_note)
    } else {
      qa_act_note <- "There were no unexpected action types."
      qa_act_result <- "PASS"
      message(qa_act_note)    
    }  
  } else {
    qa_act_note <- "Could not find columns with action types. Check data names"
    qa_act_result <- "FAIL"
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
  prog_type_index <- which(str_detect(names, "programtype") | names == "programtype")
  
  if (length(prog_type_index) > 0) {
    prog_types <- sort(unlist(unique(sha_ph_2021[, prog_type_index, with = F]), use.names = F))
    prog_types <- prog_types[!is.na(prog_types)]
    
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
  } else {
    qa_prog_note <- "Could not find columns with program types. Check data names"
    qa_prog_result <- "FAIL"
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
  building_index <- which(str_detect(names, "buildingid") | names == "buildingid")
  
  if (length(prog_type_index) > 0 & length(building_index) > 0) {
    building_df <- sha_ph_2021 %>%
      select(prog_var = prog_type_index[[1]],
             building_var = building_index[[1]]) %>%
      mutate(building_var = as.character(building_var))
    
    portfolio_miss <- building_df %>%
      filter(str_detect(tolower(prog_var), "owned") & str_detect(tolower(prog_var), "managed")) %>%
      distinct(building_var) %>%
      left_join(., sha_portfolios, by = c("building_var" = "building_id")) %>%
      filter(is.na(portfolio))
    
    portfolio_impact <- inner_join(portfolio_miss, 
                                   select(building_df, building_var),
                                   by = "building_var")
    
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
  } else if (length(building_index) == 0) {
    qa_portfolio_note <- "Could not find columns with building IDs. Check data names"
    qa_portfolio_note <- "FAIL"
    message(qa_portfolio_note) 
  } else {
    qa_portfolio_note <- "Building ID column(s) found but not program columns, so could not check portfolios. Check data names"
    qa_portfolio_note <- "FAIL"
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
                                  'value', 'row_count', {nrow(sha_ph_2021)},
                                  {Sys.time()}, NULL)",
                          .con = conn))
  
  # Distinct HH IDs
  DBI::dbExecute(conn,
                 glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`} 
                          (etl_batch_id, last_run, table_name, qa_type, qa_item, qa_result, qa_date, note) 
                          VALUES ({etl_batch_id}, NULL, '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                                  'value', 'household_count', {length(unique(sha_ph_2021$HH_CERT_ID))},
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
    rm(portfolio_miss, portfolios_impact)
  }
  
  
  # RENAME FIELDS ----
  # Get rid of spaces, characters, and capitals in existing names
  # Makes it easier to accommodate changes in names provided by SHA
  sha_ph_2021 <- sha_ph_2021 %>%
    rename_with(., ~ str_replace_all(.,"[:punct:]|[:space:]", "")) %>%
    rename_with(., tolower) %>%
    setnames(., fields$common_name[match(names(.), fields$sha)])
  
  
  # DATA CLEANING ----
  ## Deduplicate data to avoid extra rows when joining ----
  sha_ph_2021 <- sha_ph_2021 %>% distinct()
  
  
  ## Fix up date formats ----
  # Dates that were previously offset due to name suffixes or commas in the building name
  #   have a different format so need to account for that.
  # There will be a warning message about NAs introduced by coercion but there 
  #   don't appear to be any NAs actually generated.
  sha_ph_2021 <- sha_ph_2021 %>%
    mutate(across(c(ends_with("_date"), dob), 
                  ~ case_when(str_detect(.x, "/") ~ as.Date(.x, format = "%m/%d/%Y"),
                              TRUE ~ as.Date(as.integer(.x), origin = "1899-12-30"))),
           geo_zip_raw = as.character(geo_zip_raw))
  
  
  
  ## Income ----
  # Need to do the following:
  # 1) Reshape income from wide to long
  # 2) Identify people with income from a fixed source
  # 3) Summarize income/assets for a given time point to reduce duplicated rows
  sha_ph_2021_inc <- sha_ph_2021 %>%
    select(cert_id, act_date, ssn, starts_with("inc_")) %>%
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
  sha_ph_2021 <- left_join(sha_ph_2021, 
                        select(sha_ph_2021_inc, cert_id, act_date, ssn, starts_with("inc")) %>% distinct(), 
                        by = c("cert_id", "act_date", "ssn")) %>%
    left_join(., select(sha_ph_2021_inc, cert_id, act_date, starts_with("hh_")) %>% distinct(),
              by = c("cert_id", "act_date"))
  
  
  ## Join with portfolio data ----
  sha_ph_2021 <- left_join(sha_ph_2021, 
                        distinct(sha_portfolios, building_id, building_name, property_id, property_name, prog_type, portfolio) %>%
                          filter(!(is.na(building_id) & is.na(building_name))), 
                        by = c("building_id", "building_name")) %>%
    mutate(prog_type = ifelse(!is.na(prog_type.y), prog_type.y, prog_type.x)) %>%
    select(-prog_type.x, -prog_type.y)
  
  
  # LOAD DATA TO SQL ----
  # Add source field to track where each row came from
  sha_ph_2021 <- sha_ph_2021 %>% 
    mutate(pha_source = "sha2021_ph",
           etl_batch_id = etl_batch_id)
  
  # Load data
  dbWriteTable(conn,
               name = DBI::Id(schema = to_schema, table = to_table),
               value = as.data.frame(sha_ph_2021),
               overwrite = T, append = F)
}
