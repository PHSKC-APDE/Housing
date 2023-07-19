#### CODE TO COMBINE KING COUNTY HOUSING AUTHORITY DATA ----
# Alastair Matheson, PHSKC (APDE)
#
# 2021-06

### Run from main_kcha_load script (description) ----
# https://github.com/PHSKC-APDE/Housing/blob/main/claims_db/etl/db_loader/main_kcha_load.R
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

# Coding note. purrr::map does the same thing as lapply and follows same syntax: 
# lapply(list, function) == map(list, function)

### Function ----
load_stage_kcha <- function(conn = NULL,
                            to_schema = NULL,
                            to_table = NULL,
                            from_schema = NULL,
                            from_table = NULL,
                            qa_schema = NULL,
                            qa_table = NULL,
                            years = c(2015:2021),
                            truncate = T) {
  
  # CHECK INPUTS ----
    # Only process years if they have not been processed before 
    db_hhsaw_prod <- create_db_connection(server = 'hhsaw', interactive = F, prod = T)
    
    already <- tryCatch(dbGetQuery(conn = db_hhsaw_prod, "SELECT DISTINCT pha_source from [pha].[stage_kcha]")[]$pha_source, 
                        error = function(e)
                          message("The [pha].[stage_sha] table does not exist so all years of data requested in arguments will be loaded"))
    
    if(exists("already")){
      already <- as.integer(gsub("^kcha", "", already))
      if(length(setdiff(years, already)) == 0){
        stop(paste('\n\U0001f6d1 All the submitted years have already been processed and will not be reuploaded.\n',
                   'If you want to update these data in the stage table, first delete from the SQL table. For example,\n', 
                   'dbGetQuery(conn = db_hhsaw_prod, "DELETE FROM [pha].[stage_kcha] WHERE pha_source = "kcha2014")'))
      }
      
      if( length(intersect(years, already)) > 0){
        message(paste0("\n\U00026A0 The raw KCHA data for the following years already exists and will not be reloaded: ", 
                       paste(sort(intersect(years, already)), collapse = ", "), ".\n", 
                       "If you want to update these data in the stage table, first delete from the SQL table. For example,\n", 
                       'dbGetQuery(conn = db_hhsaw_prod, "DELETE FROM [pha].[stage_kcha] WHERE pha_source = "kcha2014")'))
        years <- setdiff(years, intersect(years, already))
      }
    }
    
    
  
  # BRING IN DATA as a list of data.tables ----
  kcha_raw <- lapply(years, function(x) {
    message("Working on ", x, " data")
    
    # Need special consideration for table that spans multiple years
    if (x == 2015) {
      table_to_fetch <- paste0(from_table, "_", "2004_2015")
    } else {
      table_to_fetch <- paste0(from_table, "_", x)
    }
    
    table_yr <- setDT(dbGetQuery(conn,
                           glue_sql("SELECT * FROM {`from_schema`}.{`table_to_fetch`}",
                                    .con = conn)))
    
    return(table_yr)
  })
  
  # Name data.table within the list of KCHA raw data
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
      purrr::map(~ .x %>%
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
  
    ## Agency ----
    kcha_raw <- kcha_raw %>%
      map(~ .x %>%
            mutate(h1a = case_when(str_detect(tolower(h1a), "sedro") ~ "SWHA",
                                      TRUE ~ "KCHA")))
    
    
    ## Port ins/outs ----
    kcha_raw <- kcha_raw %>%
      map(~ .x %>%
            mutate(
              # Port in
              port_in = case_when(
                program_type == "PORT" ~ 1L,
                h2a == 4 ~ 1L,
                h21f != "" & h21f != "WA002" ~ 1L,
                TRUE ~ 0L),
              # Port out
              port_out_kcha = case_when(
                str_detect(h5a1a, "PORTABLE") ~ 1L,
                h2a == 5 ~ 1L,
                TRUE ~ 0L)
            ))
   
    
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
    ## Make a geo_hash_raw field for easier joining ----
      # create geo_add2_raw if needed
        kcha_raw <- kcha_raw %>%
          map(~ if (!"h5a1b" %in% names(.x)) {
            .x %>% mutate(h5a1b = NA_character_)
          } else {.x} )
        
      # create geo_add3_raw if needed
        kcha_raw <- kcha_raw %>%
          map(~ if (!"h5a2" %in% names(.x)) {
            .x %>% mutate(h5a2 = NA_character_)
          } else {.x} )
        
      # create geo_hash_raw with kcgeocode package
        kcha_raw <- kcha_raw %>%
          map(~ .x %>%
                mutate(geo_hash_raw = kcgeocode::hash_address(add1 = h5a1a, 
                                                              add2 = h5a1b, 
                                                              add3 = h5a2, 
                                                              city = h5a3, 
                                                              state = h5a4, 
                                                              zip = as.integer(h5a5), 
                                                              type = 'raw'))
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
        
        adds_distinct <- adds_distinct[!(geo_add1_raw == 'NULL' & geo_add2_raw == 'NULL')]
        
    
    ## Identify adds_already_clean & adds_to_clean ----
        db_hhsaw_prod <- create_db_connection(server = 'hhsaw', interactive = F, prod = T) # server 16
        address_check = kcgeocode::fetch_addresses(ads = adds_distinct, 
                                                   input_type = "raw", 
                                                   con = db_hhsaw_prod, 
                                                   geocode = FALSE)
        rads::sql_clean(address_check)
        
        adds_already_clean <- address_check[!is.na(geo_add1_clean)] # only keep coded addresses
        adds_already_clean[, "geo_po_box_clean" := NULL] # Want street address, not PO BOX
        
        adds_to_clean <- adds_distinct[!geo_hash_raw %in% adds_already_clean$geo_hash_raw] 
  
    ## Actual geocode using kcgeocode (SLOW!!!)----
        if(nrow(adds_to_clean) > 0){
          message("\U023F3 Geocoding ... be patient!")
          kcgeocode::submit_ads_for_cleaning(
            ads = setDF(copy(adds_to_clean)), 
            con = db_hhsaw_prod
          )}else{message('\U0001f642 There is nothing to geocode!')}
        
    ## Check geocoding status ----
        message("To check the geocoding status, update and uncomment the following line ... ")
        # kcgeocode::check_status('2023-07-18 16:29:07', type = 'timestamp', con = db_hhsaw_prod)
        
    ## Get newly cleaned/geocoded addresses ----
        if(nrow(adds_to_clean) > 0){
          adds_clean = setDT(kcgeocode::fetch_addresses(ads = adds_to_clean, 
                                                        input_type = "raw", 
                                                        con = db_hhsaw_prod, 
                                                        geocode = FALSE))
          sql_clean(adds_clean)
          adds_clean <- adds_clean[!is.na(geo_add1_clean)] # only keep coded addresses
          adds_clean[, "geo_po_box_clean" := NULL] # Want street address, not PO BOX
          
          message(paste0(rads::round2(100*nrow(adds_clean)/nrow(adds_to_clean), 1), 
                         "% of the addresses that were not previously geocoded (", 
                         format(nrow(adds_clean), big.mark = ','), 
                         " of ", 
                         format(nrow(adds_to_clean), big.mark = ','), 
                         ") were successfully geocoded"))
        } else {message("All addresses were previously geocoded.")}
        
    ## Append all clean addresses together ----
        if (nrow(adds_to_clean) > 0) {
          adds_final <- bind_rows(adds_already_clean, adds_clean)
        } else {
          adds_final <- adds_already_clean
        }
        
        adds_final <- unique(adds_final)
        
        rads::sql_clean(adds_final)
        
  
    ## Join clean addresses back to original data ----
        kcha_raw <- kcha_raw %>%
          map(~ .x %>% left_join(., 
                                 select(adds_final, geo_hash_raw:geo_hash_geocode),
                                 by = "geo_hash_raw") %>%
                select(-matches("h5a")))
      
      # Add in geo_blank
        kcha_raw <- kcha_raw %>%
        map(~ .x %>% mutate(geo_blank = ifelse(
          geo_hash_clean %in% c("45CA31C3315A5978F40438AAB46040D75E99C9B125C2FD01DB6E10AC80BEF906",
                                "8926262F06508A0E264BC13D340FD8FAB9291001FC06341D2E687BD9C3AF6104"),
          1L, 0L)))
    
    
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
          pivot_longer(cols = matches("h3[a-j,l-z]_|h3k[0-9]_"), # originally "h3([a-j,l-z]{1}|k[0-9]{1})"
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
    db_hhsaw_prod <- create_db_connection(server = 'hhsaw', interactive = F, prod = T)
    kcha_portfolio_codes <- unique(setDT(dbGetQuery(db_hhsaw_prod, "SELECT * FROM pha.ref_kcha_portfolio_codes")))
    
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
    
    
  # ADDITIONAL CLEAN UP AFTER RESHAPING ----
    ## Member numbers ----
    kcha_long <- kcha_long %>%
      map(~ .x %>%
            mutate(mbr_num = as.integer(str_remove(mbr_num, "p"))))
    
    ## Binary recodes ----
    kcha_long <- kcha_long %>%
      map(~ .x %>%
            mutate(across(any_of(c("r_white", "r_black", "r_aian", "r_asian", "r_nhpi", 
                                   "portability", "disability", "tb_rent_ceiling",
                                   "ph_rent_ceiling")) &
                            where(is.character),
                          ~ case_when(tolower(.) %in% c("y", "yes") ~ 1L,
                                      tolower(.) %in% c("n", "no", "n0") ~ 0L,
                                      is.na(.) ~ NA_integer_,
                                      TRUE ~ 99L)),
                   r_hisp = case_when(r_hisp == "2" ~ 0L,
                                      r_hisp == "1" ~ 1L,
                                      r_hisp == "0" ~ 0L,
                                      TRUE ~ as.integer(r_hisp)),
                   female = case_when(tolower(gender) %in% c("f", "female") ~ 1L,
                                      tolower(gender) %in% c("m", "male") ~ 0L,
                                      TRUE ~ NA_integer_)))
    
    
    ## Names ----
    # Set up suffixes to remove
    suffix <- c(" SR", " JR", "-JR", "JR", "JR I", "JR II", "JR III", " II", " III", " IV")
    
    kcha_long <- kcha_long %>%
      map(~ .x %>%
            mutate(across(any_of(c("hh_lname", "hh_fname", "hh_mname", "lname", "fname", "mname")), 
                          ~ toupper(.)),
                   across(any_of(c("hh_lname", "hh_fname", "hh_mname", "lname", "fname", "mname")), 
                          ~ str_replace_all(., "_|-", " ")),
                   across(any_of(c("hh_lname", "hh_fname", "hh_mname", "lname", "fname", "mname")), 
                          ~ str_replace_all(., "NULL|\\.|\"|\\\\|'|`|[0-9]|\\*", "")),
                   across(any_of(c("hh_lname", "hh_fname", "hh_mname", "lname", "fname", "mname")), 
                          ~ str_replace_all(., paste(suffix, "$", collapse="|", sep = ""), "")),
                   across(any_of(c("hh_lname", "hh_fname", "hh_mname", "lname", "fname", "mname")), 
                          ~ str_squish(.)),
                   across(any_of(c("hh_lname", "hh_fname", "hh_mname", "lname", "fname", "mname")), 
                          ~ ifelse(. == "", NA_character_, .))) %>%
            # Clean up where middle initial seems to be in first name field
            # NOTE: There are many rows with a middle initial in the fname field AND 
            # the mname field (and the initials are not always the same).
            # In those situations, keep existing middle initial and drop from fname if
            # the initial is the same
            mutate(f_init = str_sub(fname, -2, -1),
                   f_init_hh = str_sub(hh_fname, -2, -1),
                   mname = case_when(str_detect(f_init, "[:space:][A-Z]") & is.na(mname) ~ str_sub(fname, -1),
                                     TRUE ~ mname),
                   fname = case_when(str_detect(f_init, "[:space:][A-Z]") & str_sub(fname, -1) == mname ~ 
                                       str_sub(fname, 1, -3),
                                     TRUE ~ fname),
                   hh_mname = case_when(str_detect(f_init_hh, "[:space:][A-Z]") & is.na(hh_mname) ~ str_sub(hh_fname, -1),
                                         TRUE ~ hh_mname),
                   hh_fname = case_when(str_detect(f_init_hh, "[:space:][A-Z]") & str_sub(hh_fname, -1) == hh_mname ~ 
                                           str_sub(hh_fname, 1, -3),
                                         TRUE ~ hh_fname),
                   # when middle initial is not a letter, replace it with NA
                   across(any_of(c("hh_mname", "mname")), ~ ifelse(str_detect(., "[A-Z]", negate = T), NA, .)),
                   # Remove any first name unknown values
                   across(c("fname", "hh_fname"), ~ ifelse(. == "FNU", NA_character_, .)),
                   # Flag baby and institutional names for cleaning
                   drop_name = case_when(fname %in% c("UNBORN", "BABY", "MIRACLE", "CHILD") & 
                                           lname %in% c("UNBORN", "BABY", "CHILD") ~ 1L,
                                         lname == "YWCA" ~ 1L,
                                         lname == "PLH SITE" ~ 1L,
                                         lname == "& GIRLS CLUB" ~ 1L,
                                         lname == "ELDER" & fname == "PLACE" ~ 1L,
                                         lname == "ELDER PLACE" ~ 1L,
                                         str_detect(lname, "LIVE IN") ~ 1L,
                                         str_detect(fname, "LIVE IN") ~ 1L,
                                         TRUE ~ 0L),
                   drop_name_hh = case_when(hh_fname %in% c("UNBORN", "BABY", "MIRACLE", "CHILD") & 
                                              hh_lname %in% c("UNBORN", "BABY", "CHILD") ~ 1L,
                                            hh_lname == "YWCA" ~ 1L,
                                            hh_lname == "ELDER" & hh_fname == "PLACE" ~ 1L,
                                            hh_lname == "ELDER PLACE" ~ 1L,
                                         str_detect(hh_lname, "LIVE IN") ~ 1L,
                                         str_detect(hh_fname, "LIVE IN") ~ 1L,
                                         TRUE ~ 0L),
                   # Clean baby and other names
                   across(any_of(c("lname", "fname", "mname")), ~ ifelse(drop_name == 1, NA, .)),
                   fname = ifelse(fname %in% c("UNBORN", "BABY"), NA, fname),
                   across(any_of(c("hh_lname", "hh_fname", "hh_mname")), ~ ifelse(drop_name_hh == 1, NA, .))
                   ) %>%
            select(-f_init, -f_init_hh, -drop_name, -drop_name_hh) %>% 
            filter(!(lname == "DUFUS" & fname == "IAM"))
          )
    
    
    ## SSNs ----
    # Some SSNs are HUD/PHA-generated IDs so separate into new field
    kcha_long <- kcha_long %>%
      map(~ .x %>% 
            mutate(across(any_of(c("ssn", "hh_ssn")), ~ str_replace_all(., "-", "")), 
                   across(any_of(c("ssn", "hh_ssn")), 
                          ~ case_when(str_detect(., "XXX|A00-0|A000") ~ NA_character_, TRUE ~ .)),
                   across(any_of(c("ssn", "hh_ssn")), ~ ifelse(str_detect(., "[:alpha:]"), ., NA_character_),
                          .names = "{.col}_new")) %>%
            rename_with(., ~ str_replace(., "ssn_new", "pha_id"), .cols = matches("ssn_new")) %>%
            housing::validate_ssn(DTx = ., "ssn") %>% 
            housing::validate_ssn(DTx = ., "hh_ssn") %>%
            mutate(hh_ssn = ifelse(!is.na(hh_pha_id), NA, hh_ssn)) %>%
            mutate(ssn = ifelse(!is.na(pha_id), NA, ssn))
      ) 
  
  
  # ADD USEFUL VARIABLES ----
  last_run <- Sys.time()
  
  kcha_long <- kcha_long %>%
    bind_rows() %>%
    mutate(major_prog = ifelse(prog_type == "PH", "PH", "HCV"),
           # Make a hash of variables that are used for identity matching
           id_hash = as.character(toupper(openssl::sha256(paste(str_replace_na(ssn, ''),
                                                                str_replace_na(pha_id, ''),
                                                                str_replace_na(lname, ''),
                                                                str_replace_na(fname, ''),
                                                                str_replace_na(mname, ''),
                                                                str_replace_na(dob, ''),
                                                                str_replace_na(female, ''),
                                                                sep = "|")))),
           last_run = last_run)
  
 
  # QA FINAL DATA ----
    ## Row counts compared to last time ----
      # refreshed table will be total of rows in sha_long plus whatever exists right now in SQL
        rows_refresh <- nrow(kcha_long) + 
          as.integer(dbGetQuery(conn, glue_sql("SELECT count(*) FROM {`to_schema`}.{`to_table`}", .con = conn)))
      
      # previous rows from last refresh is stored in metadata table
        rows_previous <- as.integer(dbGetQuery(conn,
                                               glue_sql("SELECT qa_result FROM {`qa_schema`}.{`qa_table`}
                                                 WHERE qa_type = 'value' AND qa_item = 'row_count' AND
                                                 table_name = '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}'
                                                 ORDER BY qa_date desc",
                                                        .con = conn))[1,])
        
        if (is.na(rows_previous)) {
          qa_row_diff_result <- "PASS"
          qa_row_diff_note <- glue("There was no existing row data to compare to last time")
          message(paste("\U0001f642", qa_row_diff_note))
        } else if (!is.na(rows_previous) & rows_refresh >= rows_previous) {
          qa_row_diff_result <- "PASS"
          qa_row_diff_note <- glue("There are {format(rows_refresh - rows_previous, big.mark = ',')}", 
                                   " more rows in the latest stage table")
          message(paste("\U0001f642", qa_row_diff_note))
        } else if (!is.na(rows_previous) & rows_refresh < rows_previous) {
          qa_row_diff_result <- "FAIL"
          qa_row_diff_note <- glue("There were {format(rows_previous - rows_refresh, big.mark = ',')}", 
                                   " fewer rows in the latest stage table. See why this is.")
          warning(paste("\U00026A0", qa_row_diff_note))
        } 
      
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
    
    if (!inherits(cols_current, 'try-error')) { # condition is for when cols_current exists
      cols_current <- setdiff(names(cols_current), 
                              c('subsidy_id', 'row_n', 'h19g', 'eop_source', 'property_id')) # these are columns that only exist in 2015 or 2016, so it is okay if they are missing in future years
      
      if (length(setdiff(cols_current, names(kcha_long))) >= 1) {
        qa_names_result <- "WARNING"
        qa_names_note <- glue("The existing stage table in SQL has columns not found in the new data")
        warning(paste('\U00026A0', qa_names_note))
      } else if (length(setdiff(names(kcha_long), cols_current)) >= 1) {
        qa_names_result <- "FAIL"
        qa_names_note <- glue("The new stage table has columns not found in the current data in SQL")
        warning(paste('\U00026A0', qa_names_note))
      } else {
        qa_names_result <- "PASS"
        qa_names_note <- glue("The existing SQL stage table and new stage tables have matching columns")
        message(paste('\U0001f642', qa_names_note))
      }
    } else {
      qa_names_result <- "PASS"
      qa_names_note <- glue("There was no existing column data to compare to last time")
      message(qa_names_note)
    }
    
    DBI::dbExecute(conn,
                   glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`} 
                            (etl_batch_id, last_run, table_name, qa_type, qa_item, qa_result, qa_date, note) 
                            VALUES (NULL, {last_run}, '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}', 'result', 
                            'column_names', {qa_names_result}, {Sys.time()}, {qa_names_note})",
                            .con = conn))
    
    
  # ADD VALUES TO METADATA ----
  # Row counts (new rows + existing rows in SQL)
  DBI::dbExecute(conn,
                 glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`} 
                          (etl_batch_id, last_run, table_name, 
                            qa_type, qa_item, qa_result, qa_date, note) 
                          VALUES (NULL, {last_run}, '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                                  'value', 'row_count', {rows_refresh},
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
    
    message("As of 3/24/2023, this code only processes and loads data that is not already in stage.")
    message("For this reason, we are appending (not overwriting) the data in the lapply below.")
    
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
                     overwrite = F, append = T)
      } else {
        dbWriteTable(conn,
                     name = DBI::Id(schema = to_schema, table = to_table),
                     value = as.data.frame(kcha_long[start_row:end_row ,]),
                     overwrite = F, append = T)
      }
    })
    
}

# The end! ----