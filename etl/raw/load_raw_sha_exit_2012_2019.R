## Script name: HUD HEARS PROCESSING - SHA RAW
##
## Purpose of script: Load raw SHA exit data
##
## Author: Alastair Matheson, Public Health - Seattle & King County
## Date Created: 2021-08-11
## Email: alastair.matheson@kingcounty.gov
##
## Notes:
# Run from main_exit_load script
# https://github.com/PHSKC-APDE/Housing/blob/master/claims_db/etl/db_loader/main_exit_load.R
# Assumes relevant libraries are already loaded

load_raw_sha_exit_2012_2019 <- function(conn = NULL,
                                        to_schema = NULL,
                                        to_table = NULL,
                                        qa_schema = NULL,
                                        qa_table = NULL,
                                        file_path = "//phdata01/DROF_DATA/DOH DATA/Housing/SHA/Original_data/Exit data/2012 to 2019 exit data",
                                        etl_batch_id = NULL) {
  
  # BRING IN DATA ----
  ## Reference data ----
  fields <- read.csv(file.path(here::here(), "etl/ref", "field_name_mapping.csv"))
  
  ## SHA ----
  # Create a list of exit files
  sha_exit_files <- list.files(path = file_path, 
                               pattern = ".xlsx",
                               full.names = T)
  
  # Bring in data and rename
  sha_exit <- lapply(sha_exit_files, function(x) read_xlsx(x))
  names(sha_exit) <- c("sha_exit_hcv", "sha_exit_ph")
  
  sha_exit <- sha_exit %>%
    map(~ setnames(.x, fields$common_name[match(names(.x), fields$sha_exit)]))
  
  # FORMAT DATA ----
  # Have to set DOB format here to join files
  sha_exit <- sha_exit %>%
    map(~ if (is.character(.x$dob) == T) {
      .x %>% mutate(dob = as.Date(as.numeric(dob), origin = "1899-12-30"),
                    across(c("move_in_date", "act_date"), ~ as.Date(.)))
    } else {.x}) %>%
    map(~ .x %>%
          mutate(across(c("dob", "act_date"), ~ as.Date(.))))
  
  
  # MAKE FINAL DATA FRAME AND ADD USEFUL VARIABLES ----
  sha_exit_long <- sha_exit %>%
    bind_rows() %>%
    mutate(pha_source = "sha_exit",
           etl_batch_id = etl_batch_id) %>%
    distinct()
  
  
  # ADD VALUES TO METADATA ----
  # Row counts
  DBI::dbExecute(conn,
                 glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`} 
                          (etl_batch_id, last_run, table_name, 
                            qa_type, qa_item, qa_result, qa_date, note) 
                          VALUES ({etl_batch_id}, NULL, '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                                  'value', 'row_count', {nrow(sha_exit_long)},
                                  {Sys.time()}, NULL)",
                          .con = conn))
  
  # LOAD DATA TO SQL ----
  # Split into smaller tables to avoid SQL connection issues
  start <- 1L
  max_rows <- 50000L
  cycles <- ceiling(nrow(sha_exit_long)/max_rows)
  
  lapply(seq(start, cycles), function(i) {
    start_row <- ifelse(i == 1, 1L, max_rows * (i-1) + 1)
    end_row <- min(nrow(sha_exit_long), max_rows * i)
    
    message("Loading cycle ", i, " of ", cycles)
    if (i == 1) {
      dbWriteTable(db_hhsaw,
                   name = DBI::Id(schema = to_schema, table = to_table),
                   value = as.data.frame(sha_exit_long[start_row:end_row, ]),
                   overwrite = T, append = F)
    } else {
      dbWriteTable(db_hhsaw,
                   name = DBI::Id(schema = to_schema, table = to_table),
                   value = as.data.frame(sha_exit_long[start_row:end_row ,]),
                   overwrite = F, append = T)
    }
  })
}
