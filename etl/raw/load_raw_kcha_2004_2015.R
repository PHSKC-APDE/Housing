#### CODE TO LOAD 2015 AND EARLIER KING COUNTY HOUSING AUTHORITY DATA
# Alastair Matheson, PHSKC (APDE)
#
# 2021-06

### Run from main_kcha_load script
# https://github.com/PHSKC-APDE/Housing/blob/main/claims_db/etl/db_loader/main_kcha_load.R
# Assumes relevant libraries are already loaded

# conn = ODBC connection to use
# to_schema = name of the schema to load ata to
# to_table = name of the table to load data to
# qa_schema = name of the schema the QA lives in (likely the same as the to_schema if working in HHSAW)
# qa_table = name of the table that holds QA outcomes
# file_path = where the KCHA data files live (note that the file names themselves are hard coded for now)
# date_min = the minimum action date expected in the data
# date_max = the maximum action date expected in the data
# etl_batch_id = the value in the ETL logging table that corresponds to these data

load_raw_kcha_2004_2015 <- function(conn = NULL,
                                    to_schema = NULL,
                                    to_table = NULL,
                                    qa_schema = NULL,
                                    qa_table = NULL,
                                    file_path = NULL,
                                    etl_batch_id = NULL) {
  
  # BRING IN DATA ----
  kcha_p1_2004_2015 <- fread(file = file.path(file_path, "kcha_2004_2015_panel_01.csv"), 
                             na.strings = c("NA", "", "NULL", "N/A", "."), 
                             stringsAsFactors = F)
  kcha_p2_2004_2015 <- fread(file = file.path(file_path, "kcha_2004_2015_panel_02.csv"), 
                             na.strings = c("NA", "", "NULL", "N/A", "."), 
                             stringsAsFactors = F)
  kcha_p3_2004_2015 <- fread(file = file.path(file_path, "kcha_2004_2015_panel_03.csv"), 
                             na.strings = c("NA", "", "NULL", "N/A", "."), 
                             stringsAsFactors = F)
  
  # Some of the KCHA end of participation data is missing from the original extract
  kcha_eop <- fread(file = file.path(file_path, "EOP Certifications_received_2017-10-05.csv"),
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
                                  'value', 'row_count', {nrow(kcha_p1_2004_2015)},
                                  {Sys.time()}, 'Only applies to panel 1')",
                          .con = conn))
  
  
  
  # PANEL DATA CLEANING ----
  ## Deduplicate data to avoid extra rows when joining ----
  # Make list of data frames to deduplicate (not needed with newer data)
  dfs <- list(kcha_p1_2004_2015 = kcha_p1_2004_2015, 
              kcha_p2_2004_2015 = kcha_p2_2004_2015, 
              kcha_p3_2004_2015 = kcha_p3_2004_2015)
  
  # Clean up member numbers that are showing 0 and deduplicate data
  df_dedups <- lapply(dfs, function(data) {
    data <- data %>% 
      mutate_at(vars(contains("h3a"), contains("h3e"), contains("h3m"),
                     contains("h5j"), contains("h19")), 
                list(~ifelse(. == 0, NA, .))) %>%
      distinct()
    return(data)
  })
  
  # Bring back data frames from list
  list2env(df_dedups, envir = environment())
  rm(dfs)
  rm(df_dedups)
  
  
  ## Panel 1 ----
  ### Remove duplicates in panel 1 ----
  kcha_p1_2004_2015 <- kcha_p1_2004_2015 %>%
    arrange(subsidy_id, h2a, h2b) %>%
    mutate(drop = case_when(
      # Some rows are duplicated except one is missing gender and citizenship
      !is.na(lead(subsidy_id, 1)) & subsidy_id == lead(subsidy_id, 1) & 
        h2a == lead(h2a, 1) & h2b == lead(h2b, 1) &
        is.na(h3g01) & !is.na(lead(h3g01, 1)) & is.na(h3i01) & !is.na(lead(h3i01, 1)) ~ 1,
      !is.na(lag(subsidy_id, 1)) & subsidy_id == lag(subsidy_id, 1) & 
        h2a == lag(h2a, 1) & h2b == lag(h2b, 1) &
        is.na(h3g01) & !is.na(lag(h3g01, 1)) & is.na(h3i01) & !is.na(lag(h3i01, 1)) ~ 1,
      # Others are duplicated except field h2h (date admitted to program) is missing
      !is.na(lead(subsidy_id, 1)) & subsidy_id == lead(subsidy_id, 1) & 
        h2a == lead(h2a, 1) & h2b == lead(h2b, 1) & is.na(h2h) & !is.na(lead(h2h, 1)) ~ 1,
      !is.na(lag(subsidy_id, 1)) & subsidy_id == lag(subsidy_id, 1) & 
        h2a == lag(h2a, 1) & h2b == lag(h2b, 1) & is.na(h2h) & !is.na(lag(h2h, 1)) ~ 1,
      TRUE ~ 0
    )) %>%
    filter(drop == 0) %>%
    select(-drop)
  
  # Set up row numbers for when there are duplicates on the join fields
  # Have to hope that the order remains consistent across panels
  kcha_p1_2004_2015 <- kcha_p1_2004_2015 %>%
    group_by(subsidy_id, h2a, h2b) %>%
    mutate(row_n = row_number()) %>%
    ungroup()
  
  ### Program types ----
  # Some typos/inconsistencies in the program types
  kcha_p1_2004_2015 <- kcha_p1_2004_2015 %>%
    mutate(program_type = case_when(program_type == "P" ~ "PH",
                                    program_type == "PR" ~ "PBS8",
                                    program_type %in% c("T", "VO") ~ "TBS8",
                                    TRUE ~ program_type))
  
  
  ## Panel 2 ----
  # Remove duplicates in panel 2
  kcha_p2_2004_2015 <- kcha_p2_2004_2015 %>%
    # Remove white space in lines to further reduce duplicates
    mutate(h5a1a = str_squish(h5a1a),
           h5a1b = str_squish(h5a1b),
           # Some rows are identical except for missing addresses
           drop = case_when(
             !is.na(lead(subsidy_id, 1)) & subsidy_id == lead(subsidy_id, 1) & 
               h2a == lead(h2a, 1) & h2b == lead(h2b, 1) &
               is.na(h5a1a) & !is.na(lead(h5a1a, 1)) & h5d == 0 & lead(h5d, 1) == 1 ~ 1,
             !is.na(lag(subsidy_id, 1)) & subsidy_id == lag(subsidy_id, 1) & 
               h2a == lag(h2a, 1) & h2b == lag(h2b, 1) &
               is.na(h5a1a) & !is.na(lag(h5a1a, 1)) & h5d == 0 & lag(h5d, 1) == 1 ~ 1,
             TRUE ~ 0
           )) %>%
    filter(drop == 0) %>%
    select(-drop)
  
  # Set up row numbers for when there are duplicates on the join fields
  # Have to hope that the order remains consistent across panels
  kcha_p2_2004_2015 <- kcha_p2_2004_2015 %>%
    group_by(subsidy_id, h2a, h2b) %>%
    mutate(row_n = row_number()) %>%
    ungroup()
  
  
  ## Panel 3 ----
  ### Blank fields ----
  # Some fields have no values in them and can be dropped
  kcha_p3_2004_2015 <- kcha_p3_2004_2015 %>% 
    select(-(contains("15")), -(contains("16")))
  
  ### Remove duplicates in panel 3 ----
  # Some rows are identical except for missing total dollar columns (h19g, h19i)
  dollar_dups <- kcha_p3_2004_2015 %>%
    filter(!is.na(h19g) & !is.na(h19i)) %>%
    distinct() %>% group_by(subsidy_id, h2a, h2b) %>%
    summarise(rows_new = n()) %>% ungroup()
  
  kcha_p3_2004_2015 <- kcha_p3_2004_2015 %>%
    group_by(subsidy_id, h2a, h2b) %>% mutate(rows_old = n()) %>%
    ungroup() %>% 
    left_join(., dollar_dups, by = c("subsidy_id", "h2a", "h2b")) %>%
    # Keep original rows plus ones where dollar amounts are not missing
    filter(rows_old == rows_new | is.na(rows_new) |
             (rows_old != rows_new & !is.na(h19g) & !is.na(h19i))) %>%
    select(-rows_old, -rows_new)
  
  rm(dollar_dups)
  
  # Some rows are identical except for missing rent/utility info (h21i-h21k)
  rent_dups <- kcha_p3_2004_2015 %>%
    group_by(subsidy_id, h2a, h2b) %>% mutate(rows = n()) %>% ungroup() %>%
    filter(rows > 1) %>% arrange(subsidy_id, h2a, h2b) %>%
    # Find rows where key vars are identical except for rent info
    mutate(drop = case_when(
      !is.na(lead(subsidy_id, 1)) & subsidy_id == lead(subsidy_id, 1) & 
        h2a == lead(h2a, 1) & h2b == lead(h2b, 1) &
        h19g == lead(h19g, 1) & h19i == lead(h19i, 1) &
        (h20b == lead(h20b, 1) | (is.na(h20b) & is.na(lead(h20b, 1)))) &
        is.na(h21i) & !is.na(lead(h21i, 1)) & h21n == 0 & lead(h21n, 1) > 0 ~ 1,
      !is.na(lag(subsidy_id, 1)) & subsidy_id == lag(subsidy_id, 1) & 
        h2a == lag(h2a, 1) & h2b == lag(h2b, 1) &
        h19g == lag(h19g, 1) & h19i == lag(h19i, 1) &
        (h20b == lag(h20b, 1) | (is.na(h20b) & is.na(lag(h20b, 1)))) &
        is.na(h21i) & !is.na(lag(h21i, 1)) & h21n == 0 & lag(h21n, 1) > 0 ~ 1,
      TRUE ~ 0
    )) %>%
    select(subsidy_id, h2a, h2b, h19g, h19i, h21i, h21j, h21k, h21n, drop) %>%
    filter(drop == 1)
  
  kcha_p3_2004_2015 <- 
    left_join(kcha_p3_2004_2015, rent_dups, by = c("subsidy_id", "h2a", "h2b", "h19g", 
                                                   "h19i", "h21i", "h21j", "h21k", "h21n")) %>%
    filter(drop == 0 | is.na(drop)) %>%
    select(-drop)
  
  rm(rent_dups)
  
  # Set up row numbers for when there are duplicates on the join fields
  # Have to hope that the order remains consistent across panels
  kcha_p3_2004_2015 <- kcha_p3_2004_2015 %>%
    group_by(subsidy_id, h2a, h2b) %>%
    mutate(row_n = row_number()) %>%
    ungroup()
  
  
  # COMBINE DATA ----
  ### Join into a single file for each extract
  # Using a left_join because without the panel 1 info (names, SSN, etc.) the info is not much help
  kcha_2004_2015_full <- list(kcha_p1_2004_2015, kcha_p2_2004_2015, kcha_p3_2004_2015) %>%
    Reduce(function(dtf1, dtf2) left_join(
      dtf1, dtf2, by = c("subsidy_id", "h2a", "h2b", "row_n")), .)
  
  
  # FORMAT DATA ----
  ## Inconsistent date structure ----
  # Dates in older data come as integers with dropped leading zeros
  # (and sometimes dropped second zero before the days)
  kcha_2004_2015_full <- kcha_2004_2015_full %>%
    mutate_at(vars(h2b, h2h, starts_with("h3e")),
              list(~ as.Date(
                case_when(
                  . == 0 ~ NA_character_,
                  nchar(as.character(.)) == 7 ~ paste0("0", as.character(.)),
                  nchar(as.character(.)) == 6 ~ paste0("0", str_sub(., 1, 1), "0", str_sub(., 2, 6)),
                  TRUE ~ as.character(.)), 
                "%m%d%Y")))
  
  ## Inconsistent field names ----
  # Fix up some inconsistent naming in income fields of <2015 data
  kcha_2004_2015_full <- kcha_2004_2015_full %>%
    select(-h19a10a, -h19a11a, -h19a12a, -h19a13a, -h19a14a, 
           -h19a10b, -h19a11b, -h19a12b, -h19a13b, -h19a14b) %>%
    rename(h19a10a = h1910a, 
           h19a11a = h1911a, 
           h19a12a = h1912a, 
           h19a13a = h1913a,
           h19a14a = h1914a, 
           h19a10b = h1910b, 
           h19a11b = h1911b, 
           h19a12b = h1912b, 
           h19a13b = h1913b,
           h19a14b = h1914b)
  
  
  # SET UP HEAD OF HOUSEHOLD DATA ----
  # Helpful for joining to EOP data and reshaping
  kcha_2004_2015_full <- kcha_2004_2015_full %>%
    mutate(hh_lname = h3b01,
           hh_fname = h3c01,
           hh_mname = h3d01,
           hh_ssn = h3n01,
           hh_dob = h3e01)
  
  
  # ADD MISSING END OF PARTICIPATION (EOP) CERTS ----
  # Rename EOP fields to match KCHA
  # NB. No longer using names from the fields csv so that the bind_rows command works
  kcha_eop <- kcha_eop %>%
    rename(householdid = `Household ID`, 
           vouch_num = `Voucher Number`,
           hh_ssn = `HOH SSN`, 
           hh_dob = `HOH Birthdate`, 
           hh_name = `HOH Full Name`, 
           program_type = `Program Type`,
           h2a = `HUD-50058 2a Type of Action`, 
           h2b = `Effective Date`)
  
  # Pull out name components into separate fields
  kcha_eop <- kcha_eop %>%
    mutate(hh_name = toupper(hh_name),
           # Extract middle initial if it exists
           m_init_chk = str_detect(hh_name, " \\w "),
           m_init_pos = str_locate(hh_name, " \\w ")[,1] + 1,
           hh_mname = ifelse(m_init_chk == T, str_sub(hh_name, m_init_pos, m_init_pos), NA),
           hh_name = ifelse(m_init_chk == T, str_replace(hh_name, " \\w ", " "), hh_name),
           # Extract first name (assume up until space is first name)
           hh_fname = str_sub(hh_name, 1, str_locate(hh_name, " ")[,1] - 1),
           # Put all remaining name parts in the last name field
           hh_lname = str_sub(hh_name, str_locate(hh_name, " ")[,1] + 1, nchar(hh_name))
    )
  
  # Fix up variable types
  kcha_eop <- kcha_eop %>%
    mutate(
      hh_ssn = str_replace_all(hh_ssn, "-", ""),
      hh_dob = as.Date(hh_dob, format = "%m/%d/%Y"),
      h2b = as.Date(h2b, format = "%m/%d/%Y"),
      eop_source = "eop",
      program_type = recode(program_type, 
           "MTW Tenant-Based Assistance" = "TBS8",
           "MTW Project-Based Assistance" = "PBS8",
           "MTW Public Housing" = "PH")) %>%
    # Restrict to necessary columns
    select(householdid, vouch_num, hh_ssn, hh_dob, hh_lname, hh_fname, hh_mname,
           program_type, h2a, h2b, eop_source) %>%
    # Drop missing SSNs and restrict to years relevant for this file
    filter(!is.na(hh_ssn) & year(h2b) <= 2015)
  
  # Join EOP and HH info together
  kcha_2004_2015_full <- bind_rows(kcha_2004_2015_full, kcha_eop) %>% 
    arrange(hh_ssn, h2b, h2a, eop_source)
  
  # Decide which row to keep when the EOP is already captured
  kcha_2004_2015_full <- kcha_2004_2015_full %>%
    mutate(drop = case_when(
      # Keep EOP if names are missing from main data
      hh_ssn == lag(hh_ssn, 1) & h2b == lag(h2b, 1) & h2a == lag(h2a, 1) &
        is.na(hh_lname) & !is.na(lag(hh_lname, 1)) & lag(eop_source, 1) == "eop" ~ 1,
      # Keep main data in other circumstances
      hh_ssn == lead(hh_ssn, 1) & h2b == lead(h2b) & h2a == lead(h2a, 1) &
        !is.na(lead(hh_lname, 1)) & eop_source == "eop" ~ 1,
      TRUE ~ 0
    )) %>%
    # Remove records that should be dropped
    filter(drop == 0) %>%
    select(-drop)
  
  
  # Transfer household data from row immediately prior to EOP row
  kcha_2004_2015_full <- kcha_2004_2015_full %>%
    mutate_at(vars(h1a, h2h, contains("h3"), contains("h5"), contains("h19"),
                   contains("h20"), contains("h21"), hh_lname, hh_fname, 
                   hh_mname, contains("hh_inc"), spec_vouch),
              list(~ ifelse(hh_ssn == lag(hh_ssn, 1) & eop_source == "eop" & !is.na(eop_source), 
                            lag(., 1), .))) %>%
    # Fix up the date format that broke here
    mutate_at(vars(h2h, starts_with("h3e")),
              list(~ as.Date(., origin = "1970-01-01")))
    
  
  
  # LOAD DATA TO SQL ----
  # Add source field to track where each row came from
  kcha_2004_2015_full <- kcha_2004_2015_full %>% 
    mutate(pha_source = "kcha2015",
           etl_batch_id = etl_batch_id)
  
  # Load data
  # Split into smaller tables to avoid SQL connection issues
  start <- 1L
  max_rows <- 50000L
  cycles <- ceiling(nrow(kcha_2004_2015_full)/max_rows)
  
  lapply(seq(start, cycles), function(i) {
    start_row <- ifelse(i == 1, 1L, max_rows * (i-1) + 1)
    end_row <- min(nrow(kcha_2004_2015_full), max_rows * i)
    
    message("Loading cycle ", i, " of ", cycles)
    if (i == 1) {
      dbWriteTable(conn,
                   name = DBI::Id(schema = to_schema, table = to_table),
                   value = as.data.frame(kcha_2004_2015_full[start_row:end_row, ]),
                   overwrite = T, append = F)
    } else {
      dbWriteTable(conn,
                   name = DBI::Id(schema = to_schema, table = to_table),
                   value = as.data.frame(kcha_2004_2015_full[start_row:end_row ,]),
                   overwrite = F, append = T)
    }
  })
}
