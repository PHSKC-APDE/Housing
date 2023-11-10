###############################################################################
# Code to generate data for the housing/Medicaid dashboard
# Allocates each person into a single category for each calendar year
# Follows the logic set up in the allocate function of the housing package
# Used as denominator for chronic conditions and for summarizing enrollment.
#
# Also adds a summary of person-time (days) for each calendar year for each 
#    combination of variables. Used as a denominator for acute events.
#
# Alastair Matheson (PHSKC-APDE)
# 2018-01-24
# Tweaked by Danny Colombara (PHSKC-APE, 2023-11-09)
#
###############################################################################

# Set up global parameter and call in libraries #####
  options(max.print = 350, tibble.print_max = 30, scipen = 999)
  
  library(odbc) # Used to connect to SQL server
  library(openxlsx) # Used to import/export Excel files
  library(housing) # contains many useful functions for analyzing housing/Medicaid data
  library(lubridate) # Used to manipulate dates
  library(tidyverse) # Used to manipulate data
  library(glue)
  library(data.table) # Used to manipulate data
  
  maxyear = 2022


# Connect to the SQL servers #####
  db_hhsaw <- rads::validate_hhsaw_key() # connects to Azure 16 HHSAW
  
# BRING IN DATA ####
  ### Main merged data
    mcaid_mcare_pha_elig_demo <- setDT(dbGetQuery(db_hhsaw, "SELECT * FROM claims.final_mcaid_mcare_pha_elig_demo"))
    mcaid_mcare_pha_elig_timevar <- setDT(dbGetQuery(
      db_hhsaw, 
      "SELECT * FROM claims.final_mcaid_mcare_pha_elig_timevar
      WHERE mcaid = 1 OR pha = 1 OR (mcare = 1 AND geo_kc = 1)"))
  
  
  ### Fix up formats
    mcaid_mcare_pha_elig_demo[, c('dob', 'death_dt', 'start_housing') := lapply(.SD, as.Date), 
                              .SDcols = c('dob', 'death_dt', 'start_housing')]
    
    mcaid_mcare_pha_elig_timevar[, c('from_date', 'to_date') := lapply(.SD, as.Date), 
                              .SDcols = c('from_date', 'to_date')]
    
    mcare_vars <- c("part_a", "part_b", "part_c", "partial", "buy_in", "full_benefit", "full_criteria")
    mcaid_mcare_pha_elig_timevar[, (mcare_vars) := lapply(.SD, function(x) ifelse(mcaid == 0 & mcare == 0, 0L, x)), .SDcols = mcare_vars]
  
  
  ### Make enrollment field
    mcaid_mcare_pha_elig_timevar <- mcaid_mcare_pha_elig_timevar[, enroll_type := fcase(
      mcaid == 0 & mcare == 0 & pha == 1, "h",
      mcaid == 1 & mcare == 0 & pha == 1, "hmd",
      mcaid == 0 & mcare == 1 & pha == 1, "hme",
      mcaid == 1 & mcare == 0 & pha == 0, "md",
      mcaid == 0 & mcare == 1 & pha == 0, "me",
      mcaid == 1 & mcare == 1 & pha == 0, "mm",
      mcaid == 1 & mcare == 1 & pha == 1, "a"
    )]

# ALLOCATION of PERSON TIME TO SPECIFIC AGENCY/GROUP FOR EACH CALENDAR YEAR (for chronic disease denominator) ####
  # Used for chronic disease denominator and enrollment analyses
  # Set up calendar years
    years <- seq(2012, maxyear)

    allocated <- rbindlist(lapply(seq_along(years), function(x) {
      
      message(glue("Working on {years[x]}"))
      
      tempy <- allocate(df = mcaid_mcare_pha_elig_timevar, 
                       starttime = as.Date(paste0(years[x], "-01-01")), 
                       endtime = as.Date(paste0(years[x], "-12-31")), 
                       agency = pha_agency, 
                       enroll = enroll_type,
                       unit = KCMASTER_ID,
                       from_date = from_date, 
                       to_date = to_date)
      
      tempy[, year := years[x]]
      
      tempy[, c('last_run', 'geo_add1', 'geo_add2', 'geo_city', 'geo_state') := NULL]
      
      return(tempy)
    }))

  # CORRECT IMPOSSIBLE pt_tot VALUES
    # A handful of rows (currently ~600/200 IDs) have pt_tot >365/6
    # Some/most stem from duplicate rows in the PHA data where a person was recorded
    # at multiple addresses during the same time period.
    # For now, just truncate to 365/6
    # First check that it is only a handful of rows
    if (uniqueN(allocated$id_apde[allocated$pt_tot > 366]) > 200) {
      stop("More people than expected had pt_tot > 366 days")
    }
    
    allocated <- allocated %>%
      mutate(pt_tot = case_when(
        year %in% c(2012, 2016, 2020, 2024) & pt_tot > 366 ~ 366, 
        pt_tot >= 365 ~ 365, 
        TRUE ~ pt_tot))


# MAKE PT AND POP_EVER FIELDS (for acute event denominator) ####
  # Used as denominator for acute events

  # Want to keep a row for any combination of groups vars that appeared
    pt_rows <- rbindlist(lapply(seq_along(years), function(x) {
      
      message(glue("Working on {years[x]}"))
      
      output <- setDT(mcaid_mcare_pha_elig_timevar)
      
      output[, overlap_amount:= as.numeric(lubridate::intersect( # lubridate::intersect provide the overlap in seconds
        lubridate::interval(from_date, to_date),
        lubridate::interval(as.Date(paste0(years[x], "-01-01")), 
                            as.Date(paste0(years[x], "-12-31")))) / ddays(1) + 1)] # divide by ddays(1) to change from seconds to days
      
      # Remove any rows that don't overlap
      output <- output[!is.na(overlap_amount)]
      
      # Make summary data
      output <- output[, .(pt = sum(overlap_amount)),
                       by = .(KCMASTER_ID, mcaid, mcare, pha, mcaid_mcare_pha, enroll_type,
                              apde_dual, part_a, part_b, part_c, partial, buy_in, 
                              dual, tpl, bsp_group_cid, full_benefit, full_criteria, cov_type, 
                              mco_id, pha_agency, pha_subsidy, pha_voucher, 
                              pha_operator, pha_portfolio, geo_kc, geo_zip, geo_zip_centroid, 
                              geo_street_centroid, geo_county_code, geo_tract_code, 
                              geo_hra_code, geo_school_code)]
      
      output[, pop_ever := 1L]
      
      output[, year := years[x]]
      
      return(output)
    }))


  # MAKE FLAG TO INDICATE FULL CRITERIA FOR EACH YEAR
    # Definition: 11+ months coverage with full_criteria
    full_criteria <- pt_rows[, .(pt_tot = sum(pt)), by = .(KCMASTER_ID, year, full_criteria)]
    full_criteria[, full_criteria_12 := case_when(
      year %in% c(2012, 2016, 2020, 2024) & pt_tot >= 11/12 * 366 ~ 1L, 
      pt_tot >= 11/12 * 365 ~ 1L, 
      TRUE ~ 0L)]
    full_criteria <- full_criteria[, .(full_criteria_12 = max(full_criteria_12)), by = .(KCMASTER_ID, year)]
  
  # Join back to main data
    allocated <- allocated %>% left_join(., full_criteria, by = c("year", "KCMASTER_ID"))


# BRING INTO A SINGLE DATA FRAME () ####
  mcaid_mcare_pha_elig_calyear <- bind_rows(allocated, pt_rows)


# JOIN TO ELIG_DEMO AND ADD CALCULATED FIELDS ####
  mcaid_mcare_pha_elig_calyear <- left_join(mcaid_mcare_pha_elig_calyear, 
                                            select(mcaid_mcare_pha_elig_demo, -mcaid_mcare_pha, -apde_dual, -last_run), 
                                            by = "KCMASTER_ID")
  
  mcaid_mcare_pha_elig_calyear <- setDT(mcaid_mcare_pha_elig_calyear)
  mcaid_mcare_pha_elig_calyear[, age_yr := floor(interval(start = dob, end = paste0(year, "-12-31")) / years(1))]
  mcaid_mcare_pha_elig_calyear[, adult := fcase(
    age_yr >= 18, 1L,
    age_yr < 18, 0L
  )]
  
  mcaid_mcare_pha_elig_calyear[, senior := fcase(
    age_yr >= 62, 1L,
    age_yr < 62, 0L
  )]
  
  mcaid_mcare_pha_elig_calyear[, agegrp := fcase(
    age_yr < 18, "<18",
    data.table::between(age_yr, 18, 24.99, NAbounds = NA), "18-24",
    data.table::between(age_yr, 25, 44.99, NAbounds = NA), "25-44",
    data.table::between(age_yr, 45, 64.99, NAbounds = NA), "45-64",
    age_yr >= 65, "65+",
    is.na(age_yr), NA_character_
  )]
  
  mcaid_mcare_pha_elig_calyear[, agegrp_expanded := fcase(
    age_yr < 10, "<10",
    data.table::between(age_yr, 10, 17.99, NAbounds = NA), "10-17",
    data.table::between(age_yr, 18, 24.99, NAbounds = NA), "18-24",
    data.table::between(age_yr, 25, 44.99, NAbounds = NA), "25-44",
    data.table::between(age_yr, 45, 64.99, NAbounds = NA), "45-64",
    data.table::between(age_yr, 65, 74.99, NAbounds = NA), "65-74",
    age_yr >= 75, "75+",
    is.na(age_yr), NA_character_
  )]
  
  mcaid_mcare_pha_elig_calyear[, age_wc := fifelse(
    between(age_yr, 0, 6.99, NAbounds = NA), 
    "Children aged 0-6",
    NA_character_)]

  mcaid_mcare_pha_elig_calyear[, time_housing_yr := 
                                 rads::round2(interval(start = start_housing, end = paste0(year, "-12-31")) / years(1), 1)]
  
  mcaid_mcare_pha_elig_calyear[, time_housing := case_when( # keep case_when rather than fcase because want default 'Unknown' and unsure how to do that with fcase 
    is.na(pha_agency) | pha_agency == "Non-PHA" ~ "Non-PHA",
    time_housing_yr < 3 ~ "<3 years",
    data.table::between(time_housing_yr, 3, 5.99, NAbounds = NA) ~ "3 to <6 years",
    time_housing_yr >= 6 ~ "6+ years",
    TRUE ~ "Unknown")]
  
  mcaid_mcare_pha_elig_calyear[, last_run := Sys.time()]


#### WRITE DATA TO SQL SERVER ####
  table_config_stage <- yaml::read_yaml(paste0(here::here(), "/etl/stage/create_stage_mcaid_mcare_pha_elig_calyear.yaml"))
  source("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/add_index.R")


  # Ensure columns are in the correct order
    # First see which columns aren't in either source (should be pt_allocate in local that will be dropped)
      setdiff(names(mcaid_mcare_pha_elig_calyear), names(table_config_stage$vars))
      setdiff(names(table_config_stage$vars), names(mcaid_mcare_pha_elig_calyear))

  # restrict to columns only in YAML
      mcaid_mcare_pha_elig_calyear <- mcaid_mcare_pha_elig_calyear[, names(table_config_stage$vars), with = F]
      

# Load to SQL
    message("Loading elig_calyear data to SQL")
    chunk_loader(DTx = mcaid_mcare_pha_elig_calyear, # R data.frame/data.table
                 connx = db_hhsaw, # connection name
                 chunk.size = 10000, 
                 schemax = table_config_stage$schema, # schema name
                 tablex =  table_config_stage$table, # table name
                 overwritex = T, # overwrite?
                 appendx = F, # append?
                 field.typesx = unlist(table_config_stage$vars))  


#### QA TABLE AND MOVE TO FINAL ####
# Needs a separate SQL script, also part of main_mcaid_mcare_pha_load.R script






#### CLEAN UP ####
# Remove stage table
rm(housing_path, years)
rm(table_config_stage)
rm(mcaid_mcare_pha_elig_demo, mcaid_mcare_pha_elig_timevar)
rm(mcaid_mcare_pha_elig_calyear, allocated, pt_rows)
rm(full_criteria)
rm(cycles, max_rows, start)
