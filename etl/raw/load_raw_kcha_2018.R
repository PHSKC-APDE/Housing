#### CODE TO LOAD 2018 KING COUNTY HOUSING AUTHORITY DATA
# Alastair Matheson, PHSKC (APDE) / revised by Danny Colombara 
#
# 2021-06 / revised 2023-03

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
    
    # Bring in field names
    fields <- rads::sql_clean(setDT(read.csv(file.path(here::here(), "etl/ref", "field_name_mapping.csv"))))
    
  
  # Initial QA checks ----
    # Row counts same across panels ----
      if(nrow(kcha_p1_2018) == nrow(kcha_p2_2018) && nrow(kcha_p2_2018) == nrow(kcha_p3_2018)){
        message("\U0001f642 each panel has the same number of rows")
      } else {
        warning("\U00026A0 the three panels do not have the same number of rows")
      }
  
    # Check that all column names are in last year's SQL database ----
      old.colnames <- paste0("SELECT top(0) * FROM [pha].[raw_kcha_", as.integer(rads::substrRight(ls(pattern = 'kcha_p1_'), 1, 4))-1, "]")
      old.colnames <- names(DBI::dbGetQuery(conn = db_hhsaw, old.colnames))
      old.colnames <- tolower(str_replace_all(old.colnames,"[:punct:]|[:space:]", ""))

      mypanels <- grep("^kcha_p[1-3]_", ls(), value = T)
      new.colnames <- c()
      for(mypanel in mypanels){
        namez <- names(get(mypanel))
        namez <- tolower(str_replace_all(namez,"[:punct:]|[:space:]", ""))
        new.colnames <- unique(c(new.colnames, namez))
      }
      
      extra.old <- setdiff(setdiff(old.colnames, new.colnames), c("hhlname", "hhfname", "hhmname", "hhssn", "hhdob", "phasource", "etlbatchid"))
      extra.new <- setdiff(new.colnames, old.colnames)
    
      if(length(extra.old) > 0){
        message(paste0("\U00026A0 the following exist in the previous year's raw SQL table: ", extra.old))} else {
         message("\U0001f642 there are no columns missing from this year's table vs the previous raw SQL table") 
        }
      
      if(length(extra.new) > 0){
        message(paste0("\U00026A0 the following exist in this year's table but are missing in the previous year's raw SQL table: ", paste0(extra.new, collapse = ', ')))} else {
          message("\U0001f642 there are no extra columns in this year's table vs the previous raw SQL table") 
        }

    # Compare number of ids that had some action to last years ----
        new.id_hh <- length(unique(kcha_p1_2018$householdid))
        old.query <- paste0("SELECT counter = count(distinct(householdid)) FROM [pha].[raw_kcha_", as.integer(rads::substrRight(ls(pattern = 'kcha_p1_'), 1, 4))-1, "]")
        old.id.hh <- DBI::dbGetQuery(conn = db_hhsaw, old.query)$counter
        diff.id.hh <- new.id_hh - old.id.hh
        if(diff.id.hh < 0){
          message(paste0("There are ", abs(diff.id.hh), " LESS unique household ids compared to the previous year" ))
        } else {message(paste0("There are ", abs(diff.id.hh), " MORE unique household ids compared to the previous year" ))}

    # Check range of action dates ----    
      action.dates <- as.Date(kcha_p1_2018$h2b, "%m/%d/%Y")
      this.year <- as.integer(rads::substrRight(ls(pattern = 'kcha_p1_'), 1, 4))
      if(max(action.dates) > as.Date(paste0(this.year, "-12-31")) ||
         min(action.dates) < as.Date(paste0(this.year, "-01-01")) ){
        warning(paste0("\U0001f47f either your minimum (", min(action.dates), ") or maximum (", max(action.dates), ") are out of range."))
      } else { message(paste0("\U0001f642 both your minimum (", min(action.dates), ") and maximum (", max(action.dates), ") are within range."))}

    # Check program_type values ----
      if(identical(sort(unique(kcha_p1_2018$program_type)), c("P", "PR", "T"))){
        message("\U0001f642 `program_type` is limited to the three standard program_type values: P, PR, T")
      } else {warning("\U00026A0 `program_type` is not consistent with the three standard program_type values: P, PR, T")}
      
    # Check action codes vs previous year's data ----
      action.codes.new <- sort(unique(kcha_p1_2018$h2a))
      old.query <- paste0("SELECT distinct(h2a) FROM [pha].[raw_kcha_", as.integer(rads::substrRight(ls(pattern = 'kcha_p1_'), 1, 4))-1, "]")
      action.codes.old <- sort(DBI::dbGetQuery(conn = db_hhsaw, old.query)[]$h2a)
      if(identical(action.codes.new, action.codes.old)){
        message("\U0001f642 the action code (h2a) in this year's data match those in the previous year's data")
      } else {
        warning(paste0("\U0001f47f the action codes (h2a) in this year's data DO NOT match those in the previous year's data: \n   this year: ", 
                       paste(action.codes.new, collapse = ",")), 
                "\n   last year: ", paste(action.codes.old, collapse = ","))
      }
      
    # Check portfolio ids vs previous year ----
      message("\U00026A0 cannot check whether portfolio information is present since, beginning with 2018, 
              we use the actual address rather than any id code for matching with portfolio")
      

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
  
  # Confirm that that dataset contains voucher_type ----
    if(length(intersect(fields[common_name %like% 'vouch_type' & !is.na(kcha_modified)]$kcha_modified, names(kcha_2018_full)))==0){
      stop("\n\U0001f47f You are column corresponding to 'vouch_type', which is a critical variable. Do not continue without correcting the code or updating the data.")
    } else {message("\U0001f642 You have a column corresponding to 'vouch_type', which is a critical variable.")}
      
  
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
