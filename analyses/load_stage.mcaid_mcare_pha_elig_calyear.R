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
# 2018-01-24, updated 202-01-13
#
#
###############################################################################

##### Set up global parameter and call in libraries #####
options(max.print = 350, tibble.print_max = 30, scipen = 999)

library(odbc) # Used to connect to SQL server
library(openxlsx) # Used to import/export Excel files
library(housing) # contains many useful functions for analyzing housing/Medicaid data
library(lubridate) # Used to manipulate dates
library(tidyverse) # Used to manipulate data
library(glue)
library(data.table) # Used to manipulate data
library(claims) # Used to aggregate data


##### Connect to the SQL servers #####
db_apde51 <- dbConnect(odbc(), "PH_APDEStore51")
db_claims51 <- dbConnect(odbc(), "PHClaims51")
db_extractstore51 <- dbConnect(odbc(), "PHExtractStore51")

housing_path <- "//phdata01/DROF_DATA/DOH DATA/Housing/Organized_data"


#### BRING IN DATA ####
### Main merged data
mcaid_mcare_pha_elig_demo <- dbGetQuery(db_apde51, "SELECT * FROM final.mcaid_mcare_pha_elig_demo")
mcaid_mcare_pha_elig_timevar <- dbGetQuery(
  db_apde51, 
  "SELECT * FROM final.mcaid_mcare_pha_elig_timevar
  WHERE mcaid = 1 OR pha = 1 OR (mcare = 1 AND geo_kc = 1)")


### Fix up formats
mcaid_mcare_pha_elig_demo <- mcaid_mcare_pha_elig_demo %>%
  mutate_at(vars(dob, death_dt, start_housing), list( ~ as.Date(.)))

mcaid_mcare_pha_elig_timevar <- mcaid_mcare_pha_elig_timevar %>%
  mutate_at(vars(from_date, to_date), list( ~ as.Date(.))) %>%
  mutate_at(vars(part_a, part_b, part_c, partial, buy_in, full_benefit, full_criteria),
            list(~ ifelse(mcaid == 0 & mcare == 0, 0L, .)))


### Make enroll field
mcaid_mcare_pha_elig_timevar <- mcaid_mcare_pha_elig_timevar %>%
  mutate(enroll_type = case_when(
    mcaid == 0 & mcare == 0 & pha == 1 ~ "h",
    mcaid == 1 & mcare == 0 & pha == 1 ~ "hmd",
    mcaid == 0 & mcare == 1 & pha == 1 ~ "hme",
    mcaid == 1 & mcare == 0 & pha == 0 ~ "md",
    mcaid == 0 & mcare == 1 & pha == 0 ~ "me",
    mcaid == 1 & mcare == 1 & pha == 0 ~ "mm",
    mcaid == 1 & mcare == 1 & pha == 1 ~ "a"
  ))


#### RUN ALLOCATION FOR EACH CALENDAR YEAR ####
# Used for chronic disease denominator and enrollment analyses

# Set up calendar years
years <- seq(2012, 2018)

allocated <- bind_rows(lapply(seq_along(years), function(x) {
  
  message(glue("Working on {years[x]}"))
  
  year <- allocate(df = mcaid_mcare_pha_elig_timevar, 
                   starttime = paste0(years[x], "-01-01"), 
                   endtime = paste0(years[x], "-12-31"), 
                   agency = pha_agency, enroll = enroll_type,
                   unit = id_apde,
                   from_date = from_date, to_date = to_date
                   # No grouping vars
  ) %>%
    mutate(year = years[x]) %>%
    dplyr::select(-last_run, -geo_add1, -geo_add2, -geo_city, -geo_state, 
                  -pt_allocate)
}))



#### MAKE PT AND POP_EVER FIELDS ####
# Used as denominator for acute events

# Want to keep a row for any combination of groups vars that appeared
pt_rows <- bind_rows(lapply(seq_along(years), function(x) {
  
  message(glue("Working on {years[x]}"))
  
  output <- setDT(mcaid_mcare_pha_elig_timevar)
  output[, overlap_amount:= as.numeric(lubridate::intersect(
    lubridate::interval(from_date, to_date),
    lubridate::interval(as.Date(paste0(years[x], "-01-01")), 
                        as.Date(paste0(years[x], "-12-31")))) / ddays(1) + 1)]
  # Remove any rows that don't overlap
  output <- output[!is.na(overlap_amount)]
  
  # Make summary data
  output <- output[, .(pt = sum(overlap_amount)),
                   by = .(id_apde, mcaid, mcare, pha, mcaid_mcare_pha, enroll_type,
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



#### MAKE FLAG TO INDICATE FULL CRITERIA FOR EACH YEAR
# Definition: 11+ months coverage with full_criteria
full_criteria <- pt_rows[, .(pt_tot = sum(pt)), by = .(id_apde, year, full_criteria)]
full_criteria[, full_criteria_12 := case_when(
  year %in% c(2012, 2016, 2020) & pt_tot >= 11/12 * 366 ~ 1L, 
  pt_tot >= 11/12 * 365 ~ 1L, 
  TRUE ~ 0L)]
full_criteria <- full_criteria[, .(full_criteria_12 = max(full_criteria_12)), by = .(id_apde, year)]

# Join back to main data
allocated <- allocated %>% left_join(., full_criteria, by = c("year", "id_apde"))


#### BRING INTO A SINGLE DATA FRAME ####
mcaid_mcare_pha_elig_calyear <- bind_rows(allocated, pt_rows)


#### JOIN TO ELIG_DEMO AND ADD CALCULATED FIELDS ####
mcaid_mcare_pha_elig_calyear <- left_join(mcaid_mcare_pha_elig_calyear, 
                                          select(mcaid_mcare_pha_elig_demo, -mcaid_mcare_pha, -apde_dual, -last_run), 
                                          by = "id_apde")

mcaid_mcare_pha_elig_calyear <- setDT(mcaid_mcare_pha_elig_calyear)
mcaid_mcare_pha_elig_calyear[, age_yr := floor(interval(start = dob, end = paste0(year, "-12-31")) / years(1))]
mcaid_mcare_pha_elig_calyear[, adult := case_when(age_yr >= 18 ~ 1L, age_yr < 18 ~ 0L)]
mcaid_mcare_pha_elig_calyear[, senior := case_when(age_yr >= 62 ~ 1L, age_yr < 62 ~ 0L)]
mcaid_mcare_pha_elig_calyear[, agegrp := case_when(
  age_yr < 18 ~ "<18",
  data.table::between(age_yr, 18, 24.99, NAbounds = NA) ~ "18-24",
  data.table::between(age_yr, 25, 44.99, NAbounds = NA) ~ "25-44",
  data.table::between(age_yr, 45, 64.99, NAbounds = NA) ~ "45-64",
  age_yr >= 65 ~ "65+",
  is.na(age_yr) ~ NA_character_)]
mcaid_mcare_pha_elig_calyear[, agegrp_expanded := case_when(
  age_yr < 10 ~ "<10",
  data.table::between(age_yr, 10, 17.99, NAbounds = NA) ~ "10-17",
  data.table::between(age_yr, 18, 24.99, NAbounds = NA) ~ "18-24",
  data.table::between(age_yr, 25, 44.99, NAbounds = NA) ~ "25-44",
  data.table::between(age_yr, 45, 64.99, NAbounds = NA) ~ "45-64",
  data.table::between(age_yr, 65, 74.99, NAbounds = NA) ~ "65-74",
  age_yr >= 75 ~ "75+",
  is.na(age_yr) ~ NA_character_)]
mcaid_mcare_pha_elig_calyear[, age_wc := case_when(
  data.table::between(age_yr, 0, 6.99, NAbounds = NA) ~ "Children aged 0-6", 
  TRUE ~ NA_character_)]
mcaid_mcare_pha_elig_calyear[, time_housing_yr := 
                               round(interval(start = start_housing, end = paste0(year, "-12-31")) / years(1), 1)]
mcaid_mcare_pha_elig_calyear[, time_housing := case_when(
  is.na(pha_agency) | pha_agency == "Non-PHA" ~ "Non-PHA",
  time_housing_yr < 3 ~ "<3 years",
  data.table::between(time_housing_yr, 3, 5.99, NAbounds = NA) ~ "3 to <6 years",
  time_housing_yr >= 6 ~ "6+ years",
  TRUE ~ "Unknown")]
mcaid_mcare_pha_elig_calyear[, last_run := Sys.time()]


#### WRITE DATA TO SQL SERVER ####
table_config_stage <- yaml::yaml.load(RCurl::getURL(
  "https://raw.githubusercontent.com/PHSKC-APDE/Housing/master/analyses/load.stage_mcaid_mcare_pha_elig_calyear.yaml"))
source("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/add_index.R")


# Ensure columns are in the correct order
# First see which columns aren't in either source
names(mcaid_mcare_pha_elig_calyear)[!names(mcaid_mcare_pha_elig_calyear) %in% names(table_config_stage$vars)]
names(table_config_stage$vars)[!names(table_config_stage$vars) %in% names(mcaid_mcare_pha_elig_calyear)]

# First approach restricts to columns only in YAML, second reorders but retains other columns
mcaid_mcare_pha_elig_calyear <- mcaid_mcare_pha_elig_calyear[, names(table_config_stage$vars), with = F]
# data.table::setcolorder(mcaid_mcare_pha_elig_calyear, names(table_config_stage$vars))

# Load to SQL
DBI::dbWriteTable(db_apde51,
                  name = DBI::Id(schema = "stage", table = "mcaid_mcare_pha_elig_calyear"),
                  value = mcaid_mcare_pha_elig_calyear,
                  append = F, overwrite = T,
                  field.types = unlist(table_config_stage$vars))


### Add index
add_index_f(db_apde51, table_config = table_config_stage, drop_index = T)


#### QA TABLE AND MOVE TO FINAL ####
# Move into new file?






#### CLEAN UP ####
# Remove stage table
rm(housing_path, years)
rm(table_config_stage)
rm(mcaid_mcare_pha_elig_demo, mcaid_mcare_pha_elig_timevar)
rm(mcaid_mcare_pha_elig_calyear, allocated, pt_rows)
