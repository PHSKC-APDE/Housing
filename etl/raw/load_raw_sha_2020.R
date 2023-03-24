#### CODE TO LOAD 2020 SEATTLE HOUSING AUTHORITY PUBLIC HOUSING AND VOUCHER DATA
# Alastair Matheson, PHSKC (APDE)
# 2021-06
# Revised by Danny Colombara, 2023-03-01 due to new data

### Run from main_sha_load script
# https://github.com/PHSKC-APDE/Housing/blob/main/etl/db_loader/main_sha_load.R
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

load_raw_sha_2020 <- function(conn = NULL,
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
  sha_2020 <- fread(file = file.path(file_path, "sha_hcv_ph_2020.csv"), 
                    na.strings = c("NA", "", "NULL", "N/A", "."), 
                    stringsAsFactors = F, colClasses = 'character')
  rads::sql_clean(sha_2020)
  
  # Bring in mapping of building/property IDs and portfolios/program types
  sha_portfolios <- setDT(dbGetQuery(conn, "SELECT * FROM pha.ref_sha_portfolio_codes"))
  
  # Ensure all income columns are integers
  for(incvar in grep('income', names(sha_2020), value = T, ignore.case = T)){
    sha_2020[, paste0(incvar) := as.integer(get(gsub(",", "", incvar)))]
  }
  
  # Bring in field names
  fields <- read.csv(file.path(here::here(), "etl/ref", "field_name_mapping.csv"))
  
  
  # QA CHECKS ----
    # Fewer QA checks this time since there is no comparison to previous years
  
    ## Field names ----
    # Are there any new names not seen before?
    namez <- names(sha_2020)
    namez <- tolower(str_replace_all(namez,"[:punct:]|[:space:]", ""))
    
    if (length(namez[!namez %in% fields$sha]) > 0) {
      qa_names_result <- "FAIL"
      qa_names_note <- glue("The following new columns were detected: ", 
                            "{glue_collapse(namez[!namez %in% fields$sha], sep = ', ', last = ', and ')}. ",
                            "Update the field_name_mapping.csv file.")
      warning(paste('\U00026A0', qa_names_note))
    } else {
      qa_names_result <- "PASS"
      qa_names_note <- "No new columns detected"
      message(paste('\U0001f600', qa_names_note))
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
    sha_2020[class(EFFECTIVE_DATE) != 'Date', EFFECTIVE_DATE := as.Date(as.integer(EFFECTIVE_DATE), origin = '1899-12-30')]
    dates <- sha_2020 %>%
      summarise(date_min = min(EFFECTIVE_DATE, na.rm = T),
                date_max = max(EFFECTIVE_DATE, na.rm = T))
    
    if (dates$date_min < date_min | dates$date_max > date_max) {
      qa_date_note <- glue("Dates fell outside the expected range: ", 
                           "min date = {dates$date_min} (expected {date_min}), ",
                           "max date = {dates$date_max} (expected {date_max})")
      qa_date_result <- "FAIL"
      warning("\U00026A0 Dates fall outside the expected range")
    } else if (dates$date_min - date_min > 30 | date_max - dates$date_max > 30) {
      qa_date_note <- glue("Large gap between expected and actual min or max date: ", 
                           "min date = {dates$date_min} (expected {date_min}), ",
                           "max date = {dates$date_max} (expected {date_max})")
      qa_date_result <- "FAIL"
      warning("\U00026A0 Dates fall outside the expected range")
    } else {
      qa_date_note <- glue("Date fell in expected range: ", 
                           "min date = {dates$date_min} (expected {date_min}), ",
                           "max date = {dates$date_max} (expected {date_max})")
      qa_date_result <- "PASS"
      message(paste('\U0001f600', qa_date_note))
    }
    
    DBI::dbExecute(conn,
                   glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`} 
                            (etl_batch_id, last_run, table_name, qa_type, qa_item, qa_result, qa_date, note) 
                            VALUES ({etl_batch_id}, NULL, '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}', 'result', 
                            'date_range', {qa_date_result}, {Sys.time()}, {qa_date_note})",
                            .con = conn))
    
    ## Action codes/types ----
    # Are there any new codes/types not seen before?
    act_types <- sort(unique(sha_2020$CERT_TYPE[!is.na(sha_2020$CERT_TYPE)]))
    act_types_expected <- c(
      "Annual HQS Inspection Only", "Annual Recertification", "Annual Reexamination", "Annual Reexamination Searching", 
      "End Participation", "Expiration of Voucher", "FSS/MTW Self-Sufficiency Only", "FSS/WtW Addendum Only",  
      "Gross Rent Change", "Historical Adjustment",  "Interim Reexamination", "Issuance of Voucher", 
      "Move In", "Move Out", "New Admission", "Other Change of Unit", "Port-Out Update (Not Submitted To MTCS)", 
      "Portability Move-in", "Portability Move-out", "Termination", "Unit Transfer", "Void", 
      "Initial Certificaton", "Interim Recertification")
    
    if (is.character(sha_2020$CERT_TYPE) & length(act_types[act_types %in% act_types_expected == F]) > 0) {
      qa_act_note <- glue("The following unexpected action types were present: ",
                          "{glue_collapse(act_types[act_types %in% act_types_expected == F], sep = ', ')}. ", 
                          "Update etl/stage/load_stage_sha.R & stage.sha recoding as appropriate.")
      qa_act_result <- "FAIL"
      warning(paste('\U00026A0', qa_act_note))
    } else if (is.integer(sha_2020$CERT_TYPE) & min(act_types %in% 1:15) == 0) {
      qa_act_note <- glue("The following unexpected action types were present: ",
                          "{glue_collapse(act_types[act_types %in% 1:15 == FALSE], sep = ', ')}")
      qa_act_result <- "FAIL"
      warning(paste('\U00026A0', qa_act_note))
    } else {
      qa_act_note <- "There were no unexpected action types."
      qa_act_result <- "PASS"
      message(paste('\U0001f642', qa_act_note))    
    }
    
    DBI::dbExecute(conn,
                   glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`} 
                            (etl_batch_id, last_run, table_name, qa_type, qa_item, qa_result, qa_date, note) 
                            VALUES ({etl_batch_id}, NULL, '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}', 'result', 
                            'action_types', {qa_act_result}, {Sys.time()}, {qa_act_note})",
                            .con = conn))
    
    
    ## Program types ----
    # Are there any new program types not seen before?
    prog_types <- sort(unique(sha_2020$PROGRAM_TYPE[!is.na(sha_2020$PROGRAM_TYPE)]))
    prog_types_expected = c("Collaborative Housing", "SHA Owned and Managed", "Tenant Based")
    
    if (length(prog_types[!prog_types %in% prog_types_expected]) > 0) {
      qa_prog_note <- glue("The following unexpected program types were present: ",
                           "{glue_collapse(prog_types[prog_types %in% prog_types_expected == F], sep = ', ')}. ", 
                           "Update stage.sha recoding as appropriate.")
      qa_prog_result <- "FAIL"
      warning(paste('\U00026A0', qa_prog_note))
    } else {
      qa_prog_note <- "There were no unexpected program types."
      qa_prog_result <- "PASS"
      message(paste('\U0001f642', qa_prog_note))
    }
    
    DBI::dbExecute(conn,
                   glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`} 
                            (etl_batch_id, last_run, table_name, qa_type, qa_item, qa_result, qa_date, note) 
                            VALUES ({etl_batch_id}, NULL, '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}', 'result', 
                            'program_types', {qa_prog_result}, {Sys.time()}, {qa_prog_note})",
                            .con = conn))
    
    
    ## Portfolios/building IDs ----
    # Do any building IDs/property IDs fail to join to the ref table?
    portfolios_miss <- sha_2020 %>%
      filter(PROGRAM_TYPE == "SHA Owned and Managed") %>%
      distinct(BUILDING_ID) %>%
      left_join(., sha_portfolios, by = c("BUILDING_ID" = "building_id")) %>%
      filter(is.na(portfolio))
    
    portfolio_impact <- inner_join(portfolios_miss, 
                                   select(sha_2020, BUILDING_ID) %>% 
                                     mutate(BUILDING_ID = as.character(BUILDING_ID)))
    
    if (nrow(portfolios_miss) > 0) {
      qa_portfolio_note <- glue("There were {nrow(portfolios_miss)} PH building IDs found in {nrow(portfolio_impact)} ",
                                "rows that did not match to a portfolio. Need to update ref table.")
      qa_portfolio_result <- "FAIL"
      warning(paste('\U00026A0', qa_portfolio_note))
    } else {
      qa_portfolio_note <- "There were no unexpected building IDs."
      qa_portfolio_result <- "PASS"
      message(paste('\U0001f642', qa_portfolio_note))
    }
    
    DBI::dbExecute(conn,
                   glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`} 
                            (etl_batch_id, last_run, table_name, qa_type, qa_item, qa_result, qa_date, note) 
                            VALUES ({etl_batch_id}, NULL, '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}', 'result', 
                            'building_ids', {qa_portfolio_result}, {Sys.time()}, {qa_portfolio_note})",
                            .con = conn))

  # CHECK QA PASSED ----
    # Stop processing if one or more QA check failed
    if (min(qa_names_result, qa_date_result, qa_act_result, qa_prog_result, qa_portfolio_result) == "FAIL") {
      stop(glue("One or more QA checks failed on {to_schema}.{to_table}. See {`qa_schema`}.{`qa_table`} for more details."))
    } else {
      # Clean up QA objects if everything passed
      rm(list = ls(pattern = "^qa_"))
      rm(namez)
      rm(dates)
      rm(act_types, act_types_expected)
      rm(prog_types, prog_types_expected)
      rm(portfolios_miss, portfolios_impact)
    }
    
  # ADD VALUES TO METADATA ----
    ## Row counts
    DBI::dbExecute(conn,
                   glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`} 
                            (etl_batch_id, last_run, table_name, 
                              qa_type, qa_item, qa_result, qa_date, note) 
                            VALUES ({etl_batch_id}, NULL, '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                                    'value', 'row_count', {nrow(sha_2020)},
                                    {Sys.time()}, 'HCV and PH both included')",
                            .con = conn))
    
    ## Distinct HH IDs
    DBI::dbExecute(conn,
                   glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`} 
                            (etl_batch_id, last_run, table_name, 
                              qa_type, qa_item, qa_result, qa_date, note) 
                            VALUES ({etl_batch_id}, NULL, '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                                    'value', 'household_count', {length(unique(sha_2020$HH_CERT_ID))},
                                    {Sys.time()}, 'HCV and PH both included')",
                            .con = conn))
  
  # RENAME FIELDS ----
    # Get rid of spaces, characters, and capitals in existing names
    # Makes it easier to accommodate changes in names provided by SHA
    sha_2020 <- sha_2020 %>%
      rename_with(., ~ str_replace_all(.,"[:punct:]|[:space:]", "")) %>%
      rename_with(., tolower) %>%
      setnames(., fields$common_name[match(names(.), fields$sha)])
    
    if(!'vouch_type' %in% names(sha_2020)){
      stop("\n\U0001f47f You are missing 'vouch_type', which is a critical variable. Do not continue without correcting the code or updating the data.")
    }
  
  # DATA CLEANING ----
    ## Replace "NULL" with true NA ----
      for(nombre in names(sha_2020)[sapply(sha_2020, is.character)]){
        sha_2020[get(nombre) == 'NULL', paste0(nombre) := NA_character_]
      }
      
    
    ## Deduplicate data to avoid extra rows when joining ----
      sha_2020 <- unique(sha_2020)
  
  
    ## Fix up date formats ----
      for(datevar in c('dob', grep('_date$', names(sha_2020), value = T))){
        sha_2020[class(get(datevar)) != 'Date', paste0(datevar) := as.Date(as.integer(get(datevar)), origin = '1899-12-30')]
      }

    ## Income ----
      # 1) Reshape income from wide to long
        sha_2020_inc <- melt(sha_2020, 
                             id.vars = c('cert_id', 'act_date', 'ssn'), 
                             measure.vars = grep("^inc_", names(sha_2020), value = T), 
                             value.name = 'inc',
                             variable.name = 'inc_code', 
                             na.rm = T)
        sha_2020_inc[, inc_code := gsub('inc_', '', inc_code)]    
      # 2) Identify people with income from a fixed source
        sha_2020_inc[, inc_fixed := 0][tolower(inc_code) %in% c("p", "pension", "s", "ssi", "ss", "social security"), inc_fixed := 1]
      # 3) Summarize income/assets for a given time point to reduce duplicated rows
        sha_2020_inc <- sha_2020_inc[, .(inc = sum(inc, na.rm = T), inc_fixed = min(inc_fixed, na.rm = T)), .(cert_id, act_date, ssn)]
        sha_2020_inc[, hh_inc_calc := sum(inc, na.rm = T), cert_id]
        sha_2020_inc[, hh_inc_fixed := min(inc_fixed, na.rm = T), cert_id]

  # COMBINE DATA ----
    ## Join/Merge main data with income data ----
      # Using a left_join because without the panel 1 info (names, SSN, etc.) the info is not much help
        sha_2020 <- merge(sha_2020, 
                            sha_2020_inc[, .(cert_id, act_date, ssn, inc, inc_fixed)], 
                            by = c("cert_id", "act_date", "ssn"), 
                            all.x = T, all.y = F)
      # Join hh-level income info separately to avoid NAs on hh members who don't appear in panel 2
        sha_2020 <- merge(sha_2020, 
                      unique(sha_2020_inc[, .(cert_id, act_date, hh_inc_calc, hh_inc_fixed)]), 
                      by = c("cert_id", "act_date"), 
                      all.x = T, all.y = F)

    ## Join/Merge main data with portfolio data ----
      sha_2020 <- merge(sha_2020, 
                        unique(sha_portfolios[!(is.na(building_id) & is.na(building_name)), .(building_id, building_name, property_id, property_name, prog_type, portfolio)]), 
                        by = c("building_id", "building_name"), 
                        all.x = T, all.y = F)    
      sha_2020[, prog_type := prog_type.x][!is.na(prog_type.y), prog_type := prog_type.y]
      sha_2020[, property_id := property_id.x][!is.na(property_id.y), property_id := property_id.y]
      sha_2020[, c('prog_type.x', 'prog_type.y', 'property_id.x', 'property_id.y') := NULL]
    
        
  # LOAD DATA TO SQL ----
  # Add source field to track where each row came from
  sha_2020 <- sha_2020 %>% 
    mutate(pha_source = "sha2020",
           etl_batch_id = etl_batch_id)
  
  # Load data
  dbWriteTable(conn,
               name = DBI::Id(schema = to_schema, table = to_table),
               value = as.data.frame(sha_2020),
               overwrite = T, append = F)
}
