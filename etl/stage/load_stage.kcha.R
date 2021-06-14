#### CODE TO COMBINE KING COUNTY HOUSING AUTHORITY DATA
# Alastair Matheson, PHSKC (APDE)
#
# 2021-06

### Run from main_kcha_load script
# https://github.com/PHSKC-APDE/Housing/blob/master/claims_db/etl/db_loader/main_kcha_load.R
# Assumes relevant libraries are already loaded


# function to pick which years to load
# bring in relevant raw years
# do usual income work and reshaping
# truncate any data in stage table from selected years
# add selected years to stage table

# conn = ODBC connection to use
# to_schema = name of the schema to load data to
# to_table = name of the table to load data to
# from_schema = name of the schema the raw data are in
# from_table = name of the table the raw data are in (assumes a common table name prefix)
# qa_schema = name of the schema the QA lives in (likely the same as the to_schema if working in HHSAW)
# qa_table = name of the table that holds QA outcomes
# file_path = where the KCHA data files live (note that the file names themselves are hard coded for now)
# years = which years to load to the stage table
# truncate = whether to remove existing stage data from selected years first (default is TRUE).
#    NB. any existing data from other years will remain intact regardless

load_stage_kcha <- function(conn = NULL,
                            to_schema = NULL,
                            to_table = NULL,
                            from_schema = NULL,
                            from_table = NULL,
                            qa_schema = NULL,
                            qa_table = NULL,
                            years = c(2015:2020),
                            truncate = T) {
  
  
  # BRING IN DATA ----
  kcha_raw <- map(years, function(x) {
    message("Working on ", x, " data")
    
    # Need special consideration for table that spans multiple years
    if (x == 2015) {
      table_to_fetch <- paste0(from_table, "_", "2004_2015")
    } else {
      table_to_fetch <- paste0(from_table, "_", x)
    }
    
    table_yr <- dbGetQuery(conn,
                           glue_sql("SELECT * FROM {`from_schema`}.{`table_to_fetch`}",
                                    .con = conn))
    
    return(table_yr)
  })
  
  # Name with each year
  names(kcha_raw) <- paste0("kcha_", years)
  
  
  # Bring in field names
  fields <- read.csv(file.path(here::here(), "etl/ref", "field_name_mapping.csv"))
  
  
  # CLEAN UP ----
  ## ZIPs ----
  # There are some 5+4 ZIPs in 2018+ data data, remove and make character length = 5
  kcha_raw <- kcha_raw %>%
    map(~ mutate(.x, h5a5 = if_else(str_detect(h5a5, "-"), 
                                    str_sub(h5a5, 1, 5), 
                                    as.character(h5a5))))
  
  ## Field names ----
  # Rename some variables to have consistent format of h<q_number><q_number_sub-part>_p<2-digit_person_number>
  # e.g., h3k1_p05 = Q h3k, sub-part 1, person 05
  # This is necessary so the reshaping runs smoothly 
  # Not needed for the income fields (h19) since they won't be used when reshaping.
  kcha_raw <- kcha_raw %>%
    map(~ .x %>%
          rename_with(., ~ str_replace(., str_sub(., 1, 3), str_c(str_sub(., 1, 3), "_p")), 
                      .cols = matches("h3([a-j,l-z]){1}[0-9]{2}")) %>%
          rename_with(., ~ str_replace(., "h3k", "h3k1_p"), .cols = matches("h3k[0-9]*a")) %>%
          rename_with(., ~ str_replace(., "h3k", "h3k2_p"), .cols = matches("h3k[0-9]*b")) %>%
          rename_with(., ~ str_replace(., "h3k", "h3k3_p"), .cols = matches("h3k[0-9]*c")) %>%
          rename_with(., ~ str_replace(., "h3k", "h3k4_p"), .cols = matches("h3k[0-9]*d")) %>%
          rename_with(., ~ str_replace(., "h3k", "h3k5_p"), .cols = matches("h3k[0-9]*e")) %>%
          rename_with(., ~ str_replace(., "h19a", "h19a10"), .cols = matches("h19a[0-9]{1}[a]")) %>%
          rename_with(., ~ str_replace(., "h19a", "h19a1"), .cols = matches("h19a[0-9]{2}[a]")) %>%
          rename_with(., ~ str_replace(., "h19a", "h19a20"), .cols = matches("h19a[0-9]{1}[b]")) %>%
          rename_with(., ~ str_replace(., "h19a", "h19a2"), .cols = matches("h19a[0-9]{2}[b]")) %>%
          rename_with(., ~ str_sub(., 1, -2) , .cols = matches("(h3k)|(h19a)"))
        )
  
  
  ## Field types ----
  # Need to get some fields to have the same type so they pivot properly
  kcha_raw <- kcha_raw %>%
    map(~ .x %>%
          mutate(across(starts_with("h3n"), ~ as.character(.x)))
    )
  
  
  ## Clean up white space ----
  kcha_raw <- kcha_raw %>%
    map(~ .x %>%
          mutate(across(where(is_character), ~ str_squish(.x)))
    )
  
  
  # COMBINE HOUSEHOLD INCOME SOURCES BEFORE RESHAPING ----
  # Much easier to do when the entire household is on a single row
  # NB. There are many household/date combos repeated due to minor differences
  # in rows, e.g., addresses formatted differently. This will mean household
  # income is repeated until rows are cleaned up.
  
  # So far, all years after 2015 are missing the totals for each household (i.e., h19g)
  # Even in 2004-2015 data, the value in h19g doesn't always add to the sums of all the h19d values
  # Similarly, the sum of the h19f fields doesn't always match h19h, if it is present.
  # Therefore need to calculate own values of h19g and h19h
  
  # Also need to calculate the value of fixed and varying income for calculations later on
  # that are used to determine length of time between examinations.
  
  # Only keeping household income rather than trying to match specific incomes back to individuals.
  # Can therefore drop the individual incomes at this point
  
  kcha_raw <- kcha_raw %>%
    map(~ .x %>% 
          mutate(hh_inc_calc = rowSums(across(starts_with("h19d")), na.rm = T),
                 hh_inc_adj_calc = rowSums(across(starts_with("h19f")), na.rm = T),
                 # This looks complicated but basically does the following:
                 # 1) recodes income type to be binary 1/0 (fixed/not fixed)
                 # 2) does a matrix multiplication of income source and income so any non-fixed income is 0
                 # 3) adds up the total fixed income for the household
                 # This approach cuts 45+ lines of code to 18 while being many times faster
                 hh_inc_fixed = rowSums(
                   .x %>% select(starts_with("h19b")) %>% 
                     mutate(across(everything(), ~ ifelse(. %in% c("G", "P", "S", "SS"), 1L, 0L))) *
                     .x %>% select(starts_with("h19d")), 
                   na.rm = T),
                 hh_inc_adj_fixed = rowSums(
                   .x %>% select(starts_with("h19b")) %>% 
                     mutate(across(everything(), ~ ifelse(. %in% c("G", "P", "S", "SS"), 1L, 0L))) *
                     .x %>% select(starts_with("h19f")), 
                   na.rm = T),
                 # Repeat but with varying income (this time fixed income is set to 0)
                 hh_inc_vary = rowSums(
                   .x %>% select(starts_with("h19b")) %>% 
                     mutate(across(everything(), ~ ifelse(. %in% c("G", "P", "S", "SS"), 0L, 1L))) *
                     .x %>% select(starts_with("h19d")), 
                   na.rm = T),
                 hh_inc_adj_vary = rowSums(
                   .x %>% select(starts_with("h19b")) %>% 
                     mutate(across(everything(), ~ ifelse(. %in% c("G", "P", "S", "SS"), 0L, 1L))) *
                     .x %>% select(starts_with("h19f")), 
                   na.rm = T)) %>%
          select(-starts_with("h19a"), -starts_with("h19b"), -starts_with("h19d"), -starts_with("h19f"))
        )

  
  # ADDRESS CLEANING ----
  ## Make a geo_hash_raw field for easier joining
  kcha_raw <- kcha_raw %>%
    map(~ .x %>% 
          mutate(across(starts_with("h5a"), toupper)) %>%
          mutate(geo_hash_raw = as.character(toupper(openssl::sha256(paste(str_replace_na(h5a1a, ''),
                                                              str_replace_na(h5a1b, ''),
                                                              if ("h5a2" %in% names(.x)) {str_replace_na(h5a2, '')} else {''},
                                                              str_replace_na(h5a3, ''),
                                                              str_replace_na(h5a4, ''),
                                                              str_replace_na(h5a5, ''),
                                                              sep = "|")))))
    )
  
  ## Pull out all unique addresses ----
  adds_distinct <- kcha_raw %>%
    map(~ .x %>% distinct(across(matches("^h5a|^geo_hash")))) %>%
    bind_rows() %>%
    distinct() %>%
    rename(geo_add1_raw = h5a1a,
           geo_add2_raw = h5a1b,
           geo_add3_raw = h5a2,
           geo_city_raw = h5a3,
           geo_state_raw = h5a4,
           geo_zip_raw = h5a5)
  
  
  ## Load to a temp SQL table for cleaning (have to use prod HHSAW here) ----
  db_hhsaw_prod <- dbConnect(odbc(), "hhsaw_prod", uid = keyring::key_list("hhsaw")[["username"]])
  
  try(dbRemoveTable(db_hhsaw_prod, "##kcha_adds"))
  odbc::dbWriteTable(db_hhsaw_prod,
                     name = "##kcha_adds",
                     value = adds_distinct,
                     overwrite = T)

  ## Pull in clean addresses ----
  adds_already_clean <- DBI::dbGetQuery(db_hhsaw_prod,
                                        "SELECT b.* FROM 
                                 (SELECT geo_hash_raw FROM ##kcha_adds) a
                                 INNER JOIN
                                 (SELECT * FROM ref.address_clean) b
                                 ON a.geo_hash_raw = b.geo_hash_raw")
  
  ## Pull in addresses that need cleaning ----
  adds_to_clean <- DBI::dbGetQuery(db_hhsaw_prod,
                                   "SELECT a.* FROM 
                                 (SELECT * FROM ##kcha_adds) a
                                 LEFT JOIN
                                 (SELECT geo_hash_raw, 1 AS clean
                                 FROM ref.address_clean) b
                                 ON a.geo_hash_raw = b.geo_hash_raw
                                 WHERE b.clean IS NULL")
  
  ## Load to Informatica for cleaning ----
  if (nrow(adds_to_clean) > 0) {
    # Add new addresses that need cleaning into the Informatica table
    timestamp <- Sys.time()
    
    adds_to_clean <- adds_to_clean %>% mutate(geo_source = NA, timestamp = timestamp)
    
    DBI::dbWriteTable(db_hhsaw_prod, 
                      name = DBI::Id(schema = "ref", table = "informatica_address_input"),
                      value = adds_to_clean,
                      overwrite = F, append = T)
  }
  
  ## Retrieve cleaned addresses ----
  if (nrow(adds_to_clean) > 0) {
    adds_clean <- dbGetQuery(db_hhsaw_prod,
                             glue::glue_sql("SELECT * FROM ref.informatica_address_output
                           WHERE convert(varchar, timestamp, 20) = {lubridate::with_tz(timestamp, 'utc')}",
                                            .con = db_hhsaw_prod))
    
    # Keep checking each hour until the addresses have been cleaned
    while (nrow(adds_clean) == 0) {
      message("Waiting on Informatica to clean addresses. Will check again in 1 hour.")
      Sys.sleep(3600)
      
      # Will need to reconnect to the DB since it will have timed out
      db_hhsaw_prod <- dbConnect(odbc(), "hhsaw_prod", uid = keyring::key_list("hhsaw")[["username"]])
      
      adds_clean <- dbGetQuery(db_hhsaw_prod,
                               glue::glue_sql("SELECT * FROM ref.informatica_address_output
                           WHERE convert(varchar, timestamp, 20) = {lubridate::with_tz(timestamp, 'utc')}",
                                              .con = db_hhsaw_prod))
    }
    
    # Informatica seems to drop secondary designators when they start with #
    # Move over from old address
    adds_clean <- adds_clean %>%
      mutate(geo_add2_clean = ifelse(is.na(geo_add2_clean) & str_detect(geo_add1_raw, "^#"),
                                     geo_add1_raw, geo_add2_clean))
    
    # Tidy up some PO box messiness
    adds_clean <- adds_clean %>%
      mutate(geo_add1_clean = case_when((is.na(geo_add1_clean) | geo_add1_clean == "") & 
                                          !is.na(geo_po_box_clean) ~ geo_po_box_clean,
                                        TRUE ~ geo_add1_clean),
             geo_add2_clean = case_when(
               geo_add1_clean == geo_po_box_clean ~ geo_add2_clean,
               (is.na(geo_add2_clean) | geo_add2_clean == "") & !is.na(geo_po_box_clean) & 
                 !is.na(geo_add1_clean) ~ geo_po_box_clean,
               !is.na(geo_add2_clean) & !is.na(geo_po_box_clean) & 
                 !is.na(geo_add1_clean) ~ paste(geo_add2_clean, geo_po_box_clean, sep = " "),
               TRUE ~ geo_add2_clean)
      )
    
    # Set up variables of interest
    adds_clean <- adds_clean %>%
      mutate(geo_geocode_skip = 0L,
             geo_hash_clean = as.character(toupper(openssl::sha256(paste(stringr::str_replace_na(geo_add1_clean, ''), 
                                                            stringr::str_replace_na(geo_add2_clean, ''), 
                                                            stringr::str_replace_na(geo_city_clean, ''), 
                                                            stringr::str_replace_na(geo_state_clean, ''), 
                                                            stringr::str_replace_na(geo_zip_clean, ''), 
                                                            sep = "|")))),
             geo_hash_geocode = as.character(toupper(openssl::sha256(paste(stringr::str_replace_na(geo_add1_clean, ''),  
                                                              stringr::str_replace_na(geo_city_clean, ''), 
                                                              stringr::str_replace_na(geo_state_clean, ''), 
                                                              stringr::str_replace_na(geo_zip_clean, ''), 
                                                              sep = "|")))),
             last_run = Sys.time()) %>%
      select(geo_add1_raw, geo_add2_raw, geo_add3_raw, geo_city_raw, 
             geo_state_raw, geo_zip_raw, geo_hash_raw,
             geo_add1_clean, geo_add2_clean, geo_city_clean, 
             geo_state_clean, geo_zip_clean, geo_hash_clean, geo_hash_geocode,
             geo_geocode_skip, last_run) %>%
      # Convert all blank fields to be NA
      mutate_if(is.character, list(~ ifelse(. == "", NA_character_, .)))
    
    
    dbWriteTable(db_hhsaw_prod, 
                 name = DBI::Id(schema = "ref",  table = "address_clean"),
                 adds_clean,
                 overwrite = F, append = T)
    
    # Don't need to geocode at this point so skip that part
  }
  
  ## Bring it all together ----
  if (nrow(adds_to_clean) > 0) {
    adds_final <- bind_rows(adds_already_clean, adds_clean)
  } else {
    adds_final <- adds_already_clean
  }
  
  ## Join back to original data ----
  kcha_raw <- kcha_raw %>%
    map(~ .x %>% left_join(., 
                           select(adds_final, geo_hash_raw:geo_hash_geocode),
                           by = "geo_hash_raw"))
  
  
  # RESHAPE AND REORGANIZE ----
  # The data initially has household members in wide format
  # Need to reshape to give one hhold member per row but retain head of hhold info
  
  # Make temporary record of the row each new row came from when reshaped
  # This will be used to count household sizes
  kcha_long <- kcha_raw %>%
    map(~ .x %>%
          arrange(hh_ssn, h2b, h2a) %>%
          mutate(hh_id_temp = row_number()))
    
  
  # Only need to shape fields with individual values (h3)
  kcha_long <- kcha_long %>%
    map(~ .x %>%
          pivot_longer(cols = matches("h3([a-j,l-z]{1}|k[0-9]{1})"),
                       names_to = c(".value", "p"),
                       names_sep = "_",
                       values_drop_na = TRUE,
                       names_transform = list(h3n = as.character)) %>%
          rename(mbr_num = p)
        )
  
    
  # Get rid of empty rows (note: some rows have a SSN but no name or address, 
  # others have an address but no name or other details, keeping those for now)
  kcha_long <- kcha_long %>%
    map(~ .x %>%
          filter(!((is.na(h3a) | h3a == 0) & h3b == "" & h3c == "" & h3n == "")) %>%
          group_by(hh_id_temp) %>%
          mutate(hh_size = n()) %>%
          ungroup())
  
  
  # RENAME VARIABLES ----
  kcha_long <- kcha_long %>%
    map(~ setnames(.x, fields$common_name[match(names(.x), fields$kcha_modified)]))
  
  
  
  # JOIN WITH PROPERTY LISTS ----
  ## Years that join on subsidy_id ----
  # Just 2015, the rest use the portfolio table below
  
  # Bring in data and rename variables
  # Note ref table is currently hard coded could switch to function input and/or YAML config
  # Also hard coded to HHSAW prod since that's where the table is
  kcha_portfolio_codes <- dbGetQuery(db_hhsaw_prod, "SELECT * FROM pha.ref_kcha_portfolio_codes")
  
  # Join and clean up duplicate variables
  kcha_long <- kcha_long %>% 
    map(~ if ("subsidy_id" %in% names(.x)) {
      .x %>% 
        mutate(property_id = as.numeric(ifelse(str_detect(subsidy_id, "^[0-9]-") == T, 
                                               str_sub(subsidy_id, 3, 5), 
                                               NA))) %>%
        left_join(., kcha_portfolio_codes, by = "property_id")
    } else {.x}
    )
  
  
  ## Years that have a property name or join on address ----
  # 2016 has property name to join on
  # 2017 already has a portfolio_type field
  # 2018 onward uses only uses address to get these fields
  
  
  # Bring in data and rename variables
  # Note ref table is currently hard coded could switch to function input and/or YAML config
  # Also hard coded to HHSAW prod since that's where the table is
  kcha_dev_adds <- dbGetQuery(db_hhsaw_prod, 
    "SELECT property_name, portfolio, portfolio_type, bed_cnt, geo_hash_clean  
    FROM pha.ref_kcha_development_adds")
  
  # Join and clean up duplicate variables
  kcha_long <- kcha_long %>% 
    map(~ if ("subsidy_id" %in% names(.x) == F & "property_name" %in% names(.x)) {
      # Seems to just be 2016
      .x %>% left_join(., distinct(kcha_dev_adds, property_name, portfolio, portfolio_type), 
                       by = "property_name")
    } else if ("subsidy_id" %in% names(.x) == F & "portfolio_type" %in% names(.x)) {
      # 2017 already has portfolio_type
      .x
    } else if ("subsidy_id" %in% names(.x) == F) {
      # Other years 2018 onward
      .x %>% left_join(., distinct(kcha_dev_adds, geo_hash_clean, property_name, portfolio, portfolio_type), 
                       by = "geo_hash_clean")
    } else {.x} # 2015
    )
  
  # Use an address join on all years to try and fill in gaps
  kcha_long <- kcha_long %>% 
    map(~ .x %>% left_join(., distinct(kcha_dev_adds, geo_hash_clean, property_name, portfolio, portfolio_type), 
                           by = "geo_hash_clean") %>%
          # Need to clean up fields differently depending on year
          mutate(portfolio_type = ifelse(is.na(portfolio_type.x) & !is.na(portfolio_type.y),
                                         portfolio_type.y, portfolio_type.x)) %>%
          select(-portfolio_type.x, -portfolio_type.y)) %>%
    map(~ if ("portfolio.x" %in% names(.x)) {
      .x %>% mutate(portfolio = ifelse(is.na(portfolio.x) & !is.na(portfolio.y),
                                       portfolio.y, portfolio.x)) %>%
        select(-portfolio.x, -portfolio.y)
      } else {.x}) %>%
    map(~ if ("property_name.x" %in% names(.x)) {
      .x %>% mutate(property_name = ifelse(is.na(property_name.x) & !is.na(property_name.y),
                                           property_name.y, property_name.x)) %>%
        select(-property_name.x, -property_name.y)
      } else {.x})
  

  # MAKE FINAL DATA FRAME AND ADD USEFUL VARIABLES ----
  last_run <- Sys.time()
  
  kcha_long <- kcha_long %>%
    bind_rows() %>%
    mutate(major_prog = ifelse(prog_type == "PH", "PH", "HCV"),
           last_run = last_run)
  
  
  # QA FINAL DATA ----
  ## Row counts compared to last time ----
  rows_existing <- as.integer(dbGetQuery(conn,
                              glue_sql("SELECT qa_result FROM {`qa_schema`}.{`qa_table`}
                                       WHERE qa_type = 'value' AND qa_item = 'row_count' AND
                                       table_name = '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}'
                                       ORDER BY qa_date desc",
                                       .con = conn))[1])
  
  if (is.na(rows_existing)) {
    qa_row_diff_result <- "PASS"
    qa_row_diff_note <- glue("There was no existing row data to compare to last time")
  } else if (!is.na(rows_existing) & nrow(kcha_long) >= rows_existing) {
    qa_row_diff_result <- "PASS"
    qa_row_diff_note <- glue("There were {format(nrow(kcha_long)-rows_existing, big.mark = ',')}", 
                             " more rows in the lastest stage table")
  } else if (!is.na(rows_existing) & nrow(kcha_long) < rows_existing) {
    qa_row_diff_result <- "FAIL"
    qa_row_diff_note <- glue("There were {rows_existing-format(nrow(kcha_long), big.mark = ',')}", 
                             " fewer rows in the lastest stage table. See why this is.")
  } 
  
  message(qa_row_diff_note)
  DBI::dbExecute(conn,
                 glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`} 
                          (etl_batch_id, last_run, table_name, qa_type, qa_item, qa_result, qa_date, note) 
                          VALUES (NULL, {last_run}, '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}', 'result', 
                          'row_count_vs_previous', {qa_row_diff_result}, {Sys.time()}, {qa_row_diff_note})",
                          .con = conn))
  
  
  ## Columns match existing table ----
  cols_current <- try(dbGetQuery(conn,
                             glue_sql("SELECT TOP 0 * FROM {`to_schema`}.{`to_table`}",
                                      .con = conn)))
  
  if (str_detect(cols_current, "Error", negate = T)) {
    if (length(names(cols_current)[names(cols_current) %in% names(kcha_long) == F]) > 1) {
      qa_names_result <- "FAIL"
      qa_names_note <- glue("The existing stage table has columns not found in the new data")
    } else if (length(names(kcha_long)[names(kcha_long) %in% names(cols_current) == F]) > 1) {
      qa_names_result <- "FAIL"
      qa_names_note <- glue("The new stage table has columns not found in the current data")
    } else {
      qa_names_result <- "PASS"
      qa_names_note <- glue("The existing and new stage tables have matching columns")
    }
  } else {
    qa_names_result <- "PASS"
    qa_names_note <- glue("There was no existing column data to compare to last time")
  }
  
  message(qa_names_note)
  DBI::dbExecute(conn,
                 glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`} 
                          (etl_batch_id, last_run, table_name, qa_type, qa_item, qa_result, qa_date, note) 
                          VALUES (NULL, {last_run}, '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}', 'result', 
                          'row_count_vs_previous', {qa_names_result}, {Sys.time()}, {qa_names_note})",
                          .con = conn))
  
  
  # ADD VALUES TO METADATA ----
  # Row counts
  DBI::dbExecute(conn,
                 glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`} 
                          (etl_batch_id, last_run, table_name, 
                            qa_type, qa_item, qa_result, qa_date, note) 
                          VALUES (NULL, {last_run}, '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                                  'value', 'row_count', {nrow(kcha_long)},
                                  {Sys.time()}, NULL)",
                          .con = conn))
  
  
  # CHECK QA PASSED ----
  # Stop processing if one or more QA check failed
  if (min(qa_row_diff_result, qa_names_result) == "FAIL") {
    stop(glue("One or more QA checks failed on {to_schema}.{to_table}. See {`qa_schema`}.{`qa_table`} for more details."))
  } else {
    # Clean up QA objects if everything passed
    rm(list = ls(pattern = "^qa_"))
  }
  
  
  # LOAD DATA TO SQL ----
  ## Remove any rows from selected years ----
  # Need to figure out how this works
  # - use etl batch ID? won't work if a year is updated
  # - use action dates? won't work since data cover multiple years
  if (truncate == T) {
    message("Have not worked out how to implement this yet")
  }
  
  
  ## Load data ----
  # Split into smaller tables to avoid SQL connection issues
  start <- 1L
  max_rows <- 50000L
  cycles <- ceiling(nrow(kcha_long)/max_rows)
  
  lapply(seq(start, cycles), function(i) {
    start_row <- ifelse(i == 1, 1L, max_rows * (i-1) + 1)
    end_row <- min(nrow(kcha_long), max_rows * i)
    
    message("Loading cycle ", i, " of ", cycles)
    if (i == 1) {
      dbWriteTable(conn,
                   name = DBI::Id(schema = to_schema, table = to_table),
                   value = as.data.frame(kcha_long[start_row:end_row, ]),
                   overwrite = T, append = F)
    } else {
      dbWriteTable(conn,
                   name = DBI::Id(schema = to_schema, table = to_table),
                   value = as.data.frame(kcha_long[start_row:end_row ,]),
                   overwrite = F, append = T)
    }
  })
  
}
