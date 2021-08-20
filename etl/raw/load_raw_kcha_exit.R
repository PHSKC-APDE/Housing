## Script name: HUD HEARS PROCESSING - KCHA RAW
##
## Purpose of script: Load raw KCHA exit data
##
## Author: Alastair Matheson, Public Health - Seattle & King County
## Date Created: 2021-08-11
## Email: alastair.matheson@kingcounty.gov
##
## Notes:
# Run from main_exit_load script
# https://github.com/PHSKC-APDE/Housing/blob/master/claims_db/etl/db_loader/main_exit_load.R
# Assumes relevant libraries are already loaded

load_raw_kcha_exit <- function(conn = NULL,
                               to_schema = NULL,
                               to_table = NULL,
                               qa_schema = NULL,
                               qa_table = NULL,
                               file_path = "//phdata01/DROF_DATA/DOH DATA/Housing",
                               etl_batch_id = NULL) {
  
  # BRING IN DATA ----
  ## Reference data ----
  fields <- read.csv(file.path(here::here(), "etl/ref", "field_name_mapping.csv"))
  
  ## KCHA ----
  # Create a list of exit files
  kcha_exit_files <- list.files(path = file.path(pha_path, "KCHA/Original_data/Exit data"), 
                                pattern = "Exit Data(.*)Cleaned.xlsx",
                                full.names = T)
  
  # Bring in data and rename
  kcha_exit <- lapply(kcha_exit_files, function(x) read_xlsx(x))
  names(kcha_exit) <- paste0("kcha_exit_", 2015:2020)
  
  kcha_exit <- kcha_exit %>%
    map(~ setnames(.x, fields$common_name[match(names(.x), fields$kcha_exit)]))
  
  
  # FORMAT DATA ----
  # Just fix up dates here, rest in stage
  kcha_exit <- kcha_exit %>%
    map(~ .x %>%
          mutate(across(c(contains("date"), contains("dob")), 
                        ~ as.Date(., format = "%m/%d/%Y"))))
  
  
  # MAKE FINAL DATA FRAME AND ADD USEFUL VARIABLES ----
  kcha_exit_long <- kcha_exit %>%
    bind_rows() %>%
    mutate(pha_source = "kcha_exit",
           etl_batch_id = etl_batch_id) %>%
    distinct()
  
  
  # ADD VALUES TO METADATA ----
  # Row counts
  DBI::dbExecute(conn,
                 glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`} 
                          (etl_batch_id, last_run, table_name, 
                            qa_type, qa_item, qa_result, qa_date, note) 
                          VALUES ({etl_batch_id}, NULL, '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                                  'value', 'row_count', {nrow(kcha_exit_long)},
                                  {Sys.time()}, NULL)",
                          .con = conn))
  
  # LOAD DATA TO SQL ----
  # Split into smaller tables to avoid SQL connection issues
  start <- 1L
  max_rows <- 50000L
  cycles <- ceiling(nrow(kcha_exit_long)/max_rows)
  
  lapply(seq(start, cycles), function(i) {
    start_row <- ifelse(i == 1, 1L, max_rows * (i-1) + 1)
    end_row <- min(nrow(kcha_exit_long), max_rows * i)
    
    message("Loading cycle ", i, " of ", cycles)
    if (i == 1) {
      dbWriteTable(db_hhsaw,
                   name = DBI::Id(schema = to_schema, table = to_table),
                   value = as.data.frame(kcha_exit_long[start_row:end_row, ]),
                   overwrite = T, append = F)
    } else {
      dbWriteTable(db_hhsaw,
                   name = DBI::Id(schema = to_schema, table = to_table),
                   value = as.data.frame(kcha_exit_long[start_row:end_row ,]),
                   overwrite = F, append = T)
    }
  })
}
