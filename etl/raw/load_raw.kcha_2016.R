#### CODE TO LOAD 2016 KING COUNTY HOUSING AUTHORITY DATA
# Alastair Matheson, PHSKC (APDE)
#
# 2021-06

### Run from main_kcha_load script
# https://github.com/PHSKC-APDE/Housing/blob/master/claims_db/etl/db_loader/main_kcha_load.R
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

load_raw.kcha_2016 <- function(conn = NULL,
                               to_schema = NULL,
                               to_table = NULL,
                               qa_schema = NULL,
                               qa_table = NULL,
                               file_path = NULL,
                               date_min = "2016-01-01",
                               date_max = "2016-12-31",
                               etl_batch_id = NULL) {
  
  # BRING IN DATA ----
  kcha_p1_2016 <- fread(file = file.path(file_path, "kcha_panel_01_2016.csv"), 
                        na.strings = c("NA", "", "NULL", "N/A", "."), 
                        stringsAsFactors = F)
  kcha_p2_2016 <- fread(file = file.path(file_path, "kcha_panel_02_2016.csv"), 
                        na.strings = c("NA", "", "NULL", "N/A", "."), 
                        stringsAsFactors = F)
  kcha_p3_2016 <- fread(file = file.path(file_path, "kcha_panel_03_2016.csv"), 
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
                          VALUES ({etl_batch_id}, NULL, 'raw.kcha_2016',
                                  'value', 'row_count', {nrow(kcha_p1_2016)},
                                  {Sys.time()}, 'Only applies to panel 1')",
                          .con = conn))
  
  
  
  # PANEL DATA CLEANING ----
  ### Program types ----
  # Some typos/inconsistencies in the program types
  kcha_p1_2016 <- kcha_p1_2016 %>%
    mutate(program_type = case_when(program_type == "P" ~ "PH",
                                    program_type == "PR" ~ "PBS8",
                                    program_type %in% c("T", "VO") ~ "TBS8",
                                    TRUE ~ program_type))
  
  
  # COMBINE DATA ----
  ### Join into a single file for each extract
  # Using a left_join because without the panel 1 info (names, SSN, etc.) the info is not much help
  kcha_2016_full <- list(kcha_p1_2016, kcha_p2_2016, kcha_p3_2016) %>%
    Reduce(function(dtf1, dtf2) left_join(
      dtf1, dtf2, by = c("householdid", "certificationid", "h2a", "h2b")), .)
  
  
  # FORMAT DATA ----
  ## Dates ----
  kcha_2016_full <- kcha_2016_full %>%
    mutate_at(vars(h2b, h2h, starts_with("h3e")),
              list(~ as.Date(., format = "%m/%d/%Y")))
  
  ## Field names ----
  # The city variable seems misnamed in 2016 data (ok in 2017)
  kcha_2016_full <- kcha_2016_full %>% rename(h5a3 = h5a2)
  
  
  # SET UP HEAD OF HOUSEHOLD DATA ----
  # Helpful for joining to EOP data and reshaping
  kcha_2016_full <- kcha_2016_full %>%
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
    filter(!is.na(hh_ssn) & year(h2b) == 2016)
  
  # Join EOP and HH info together
  kcha_2016_full <- bind_rows(kcha_2016_full, kcha_eop) %>% 
    arrange(hh_ssn, h2b, h2a, eop_source)
  
  # Decide which row to keep when the EOP is already captured
  kcha_2016_full <- kcha_2016_full %>%
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
  kcha_2016_full <- kcha_2016_full %>%
    mutate_at(vars(h1a, h2h, contains("h3"), contains("h5"), contains("h19"),
                   contains("h20"), contains("h21"), hh_lname, hh_fname, 
                   hh_mname, contains("hh_inc"), spec_vouch),
              list(~ ifelse(hh_ssn == lag(hh_ssn, 1) & eop_source == "eop" & !is.na(eop_source), 
                            lag(., 1), .)))
  
  
  # LOAD DATA TO SQL ----
  # Add source field to track where each row came from
  kcha_2016_full <- kcha_2016_full %>% 
    mutate(pha_source = "kcha2016",
           etl_batch_id = etl_batch_id)
  
  # Load data
  dbWriteTable(conn,
               name = DBI::Id(schema = to_schema, table = to_table),
               value = as.data.frame(kcha_2016_full),
               overwrite = T, append = F)
}
