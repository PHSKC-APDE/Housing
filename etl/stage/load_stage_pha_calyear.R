#### CODE TO CREATE PRE-CALCULATED CALENDAR YEAR TABLE
# Alastair Matheson, PHSKC (APDE)
#
# 2021-07

### Run from main_pha_load script
# https://github.com/PHSKC-APDE/Housing/blob/main/claims_db/etl/db_loader/main_pha_load.R
# Assumes relevant libraries are already loaded

# Allocates each person into a single value for each category by calendar year
# Also adds a summary of person-time (days) for each each category

load_stage_pha_calyear <- function(conn = NULL,
                                   to_schema = "pha",
                                   to_table = "stage_calyear",
                                   demo_schema = "pha",
                                   demo_table = "final_demo",
                                   timevar_schema = "pha",
                                   timevar_table = "final_timevar") {
  
  # BRING IN DATA ----
  pha_demo <- dbGetQuery(conn, glue_sql("SELECT * FROM {`demo_schema`}.{`demo_table`}",
                                        .con = conn))
  pha_timevar <- dbGetQuery(conn, glue_sql("SELECT a.*, c.geo_tractce10 FROM
                                           (SELECT * FROM {`timevar_schema`}.{`timevar_table`}) a
                                           LEFT JOIN
                                           (SELECT DISTINCT geo_hash_clean, geo_hash_geocode FROM ref.address_clean) b
                                           ON a.geo_hash_clean = b.geo_hash_clean
                                           LEFT JOIN
                                           (SELECT DISTINCT geo_hash_geocode, geo_tractce10 FROM ref.address_geocode) c
                                           ON b.geo_hash_geocode = c.geo_hash_geocode",
                                           .con = conn))
  
  
  # RUN ALLOCATION FOR EACH CALENDAR YEAR ----
  # Allocate an individual to a PHA/program based on rules:
  # 1) Multiple PHAs = PHA group with most person-time for EACH PHA.
  # 2) Single PHA only = group with most person-time
  
  # Find the row with the most person-time in each agency and group of by_vars
  # (ties will be broken by whatever other ordering exists)
  allocate_calyear <- function(df,
                       time_start = NULL,
                       time_end = NULL,
                       by_vars = NULL) {
   
    
    ## Set up overlap between time period of interest and enrollment ----
    df <- df %>%
      mutate(
        overlap_amount = as.numeric(lubridate::intersect(
          #time_int,
          lubridate::interval(from_date, to_date),
          lubridate::interval(time_start, time_end)) / ddays(1) + 1)
      ) %>%
      # Remove any rows that don't overlap
      dplyr::filter(!is.na(overlap_amount))
     
    
    ## Find the row with the most person-time in each agency and group ----
    # (ties will be broken by whatever other ordering exists)
    pt <- df %>%
      group_by(agency, id_kc_pha, across(all_of(by_vars))) %>%
      summarise(pt = sum(overlap_amount))
    
    # Join back to a single df and sort so largest time is first in the group
    pop <- left_join(df, pt, by = c("agency", "id_kc_pha", by_vars)) %>%
      arrange(id_kc_pha, agency, desc(pt), desc(overlap_amount))
    
    # Take first row in group
    pop <- pop %>%
      group_by(agency, id_kc_pha) %>%
      slice(1) %>%
      ungroup()
    
    # Remove junk columns or columns with no meaning
    pop <- pop %>%
      select(id_kc_pha, agency, all_of(by_vars), pt)
    
    return(pop)
  }
  

  
  # Set up calendar years
  years <- seq(2012, 2020)
  
  # Set up each grouping variable
  categories <- c("hh_id_kc_pha", "disability", "major_prog", "subsidy_type", "prog_type",
                  "operator_type", "vouch_type_final", "property_id", "portfolio_final",
                  "geo_zip_clean", "geo_tractce10")
  
  
  allocated <- bind_rows(lapply(years, function(x) {
    
    message("Working on ", x)
    
    # Run over agency in general
    total <- allocate_calyear(df = pha_timevar, 
                              time_start = paste0(x, "-01-01"), 
                              time_end = paste0(x, "-12-31")) %>%
      rename(pt_total = pt)
    
    cats <- bind_rows(lapply(categories, function(y) {
      message("working on ", y)
      cat <- allocate_calyear(df = pha_timevar, 
                               time_start = paste0(x, "-01-01"), 
                               time_end = paste0(x, "-12-31"),
                               by_vars = y) %>%
        rename(group = y) %>%
        mutate(category = rlang::as_name(y),
               group = as.character(group))
    }))
    output <- left_join(total, cats, by = c("id_kc_pha", "agency")) %>% 
      mutate(year = x) %>%
      select(year, id_kc_pha, agency, pt_total, category, group, pt)
  }))
  
  
  # Reshape to get wide output and reorder
  # Also truncate the few (~145) rows with pt > 365/6
  allocated_wide <- allocated %>%
    mutate(pt = case_when(year %in% c(2012, 2016, 2020, 2024) & pt > 366 ~ 366,
                          pt > 365 ~ 365,
                          TRUE ~ pt)) %>%
    pivot_wider(names_from = category, 
                values_from = c(group, pt),
                names_glue = "{category}_{.value}") %>%
    rename_with(~ str_remove(., "_group"), .cols = contains("_group"))
  
  
  # ID AN ADMIT DATE FOR EACH YEAR ----
  # A person's first admit_date overall and for each PHA is captured in the pha_demo table
  # However, we may want to use a from_date as the admit_date if there has been a lengthy
  #   gap in their coverage. For example, if a person has coverage from 2012-2014 then 2016-2020,
  #   we would want their admit date to be 2016 for the second period.
  # For now, treat gaps of 1+ years as sufficient to trigger a new admit_date
  admit_date_setup <- pha_timevar %>%
    filter(from_date == period_start) %>%
    # Also filter gap_length = 0 where people have duplicate rows
    filter(gap_length != 0 | is.na(gap_length)) %>%
    distinct(id_kc_pha, gap_length, period_start) %>%
    mutate(use_new_date = case_when(is.na(gap_length) ~ 0L,
                                    gap_length >= 365 ~ 1L, 
                                    TRUE ~ 0L)) %>%
    distinct(id_kc_pha, period_start, use_new_date)
  
  pha_timevar <- left_join(pha_timevar, admit_date_setup, by = c("id_kc_pha", "period_start"))
  
  
  # Find a admit_date for each year
  admit_dates <- bind_rows(map(years, function(x) {
    message("Working on ", x)
    output <- pha_timevar %>%
      mutate(
        overlap_amount = as.numeric(lubridate::intersect(
          #time_int,
          lubridate::interval(from_date, to_date),
          lubridate::interval(as.Date(paste0(x, "-01-01")), as.Date(paste0(x, "-12-31")))) / ddays(1) + 1)
      ) %>%
      # Remove any rows that don't overlap
      dplyr::filter(!is.na(overlap_amount)) %>%
      left_join(., select(pha_demo, id_kc_pha, starts_with("admit_date")), by = "id_kc_pha")
    
    output <- output %>%
      mutate(admit_date_yr = case_when(use_new_date == 1 ~ period_start,
                                       year(admit_date_all) > x ~ period_start,
                                       !is.na(admit_date_all) ~ admit_date_all,
                                       agency == "KCHA" & !is.na(admit_date_kcha) ~ admit_date_kcha,
                                       agency == "SHA" & !is.na(admit_date_sha) ~ admit_date_sha,
                                       TRUE ~ period_start)) %>%
      distinct(id_kc_pha, agency, admit_date_yr) %>%
      group_by(id_kc_pha) %>%
      summarise(admit_date_yr = max(admit_date_yr, na.rm = T)) %>%
      ungroup() %>%
      mutate(year = x)
    
    output
  }))
  
  # Join back to other year table
  allocated_wide <- left_join(allocated_wide, admit_dates, by = c("id_kc_pha", "year"))
  
  
  # JOIN TO DEMO TABLE AND ADD CALCULATED FIELDS ----
  calyear <- setDT(left_join(allocated_wide, 
                             select(pha_demo, -last_run, -contains("_t")),
                             by = "id_kc_pha"))
    
  
  calyear[, age_yr := floor(interval(start = dob, end = paste0(year, "-12-31")) / years(1))]
  calyear[, adult := case_when(age_yr >= 18 ~ 1L, age_yr < 18 ~ 0L)]
  calyear[, senior := case_when(age_yr >= 62 ~ 1L, age_yr < 62 ~ 0L)]
  calyear[, agegrp := 
            case_when(age_yr < 18 ~ "<18",
                      data.table::between(age_yr, 18, 24.99, NAbounds = NA) ~ "18-24",
                      data.table::between(age_yr, 25, 44.99, NAbounds = NA) ~ "25-44",
                      data.table::between(age_yr, 45, 64.99, NAbounds = NA) ~ "45-64",
                      age_yr >= 65 ~ "65+",
                      is.na(age_yr) ~ NA_character_)]
  calyear[, agegrp_expanded := 
            case_when(age_yr < 10 ~ "<10",
                      data.table::between(age_yr, 10, 17.99, NAbounds = NA) ~ "10-17",
                      data.table::between(age_yr, 18, 24.99, NAbounds = NA) ~ "18-24",
                      data.table::between(age_yr, 25, 44.99, NAbounds = NA) ~ "25-44",
                      data.table::between(age_yr, 45, 64.99, NAbounds = NA) ~ "45-64",
                      data.table::between(age_yr, 65, 74.99, NAbounds = NA) ~ "65-74",
                      age_yr >= 75 ~ "75+",
                      is.na(age_yr) ~ NA_character_)]
  calyear[, time_housing_yr := round(interval(start = admit_date_yr, end = paste0(year, "-12-31")) / years(1), 1)]
  calyear[, time_housing := 
            case_when(time_housing_yr < 3 ~ "<3 years",
                      data.table::between(time_housing_yr, 3, 5.99, NAbounds = NA) ~ "3 to <6 years",
                      time_housing_yr >= 6 ~ "6+ years",
                      TRUE ~ "Unknown")]
  calyear[, last_run := Sys.time()]
  
  
  # WRITE DATA TO SQL SERVER ----
  ## Select and arrange columns
  cols_select <- c(
    # Core variables
    "year", "id_kc_pha", "agency", "pt_total", 
    "admit_date_all", "admit_date_kcha", "admit_date_sha", "time_housing_yr", "time_housing", 
    # Head of household variables
    "hh_id_kc_pha", "hh_id_kc_pha_pt",
    # Demog variables
    "dob", "age_yr", "agegrp", "agegrp_expanded", "adult", "senior",
    "gender_me", "gender_recent", "gender_female", "gender_male",
    "race_me", "race_eth_me", "race_recent", "race_eth_recent",
    "race_aian", "race_asian", "race_black", "race_latino", "race_nhpi",
    "race_white", "race_unk", "race_eth_unk",
    "disability", "disability_pt",
    # Program info
    "major_prog", "major_prog_pt", "subsidy_type", "subsidy_type_pt", 
    "prog_type", "prog_type_pt", "operator_type", "operator_type_pt", 
    "vouch_type_final", "vouch_type_final_pt", 
    # Address and portfolio info
    "geo_zip_clean", "geo_zip_clean_pt", 
    "property_id", "property_id_pt",
    "portfolio_final", "portfolio_final_pt", 
    "geo_tractce10", "geo_tractce10_pt",
    # Other info
    "last_run")
  
  calyear <- calyear[, ..cols_select]
  
  # Load to SQL
  # Split into smaller tables to avoid SQL connection issues
  start <- 1L
  max_rows <- 100000L
  cycles <- ceiling(nrow(calyear)/max_rows)
  
  lapply(seq(start, cycles), function(i) {
    start_row <- ifelse(i == 1, 1L, max_rows * (i-1) + 1)
    end_row <- min(nrow(calyear), max_rows * i)
    
    message("Loading cycle ", i, " of ", cycles)
    if (i == 1) {
      dbWriteTable(conn,
                   name = DBI::Id(schema = to_schema, table = to_table),
                   value = as.data.frame(calyear[start_row:end_row]),
                   overwrite = T, append = F)
    } else {
      dbWriteTable(conn,
                   name = DBI::Id(schema = to_schema, table = to_table),
                   value = as.data.frame(calyear[start_row:end_row]),
                   overwrite = F, append = T)
    }
  })
}
