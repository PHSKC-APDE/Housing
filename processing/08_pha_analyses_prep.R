###############################################################################
# OVERVIEW:
# Code to create a cleaned person table from the combined 
# King County Housing Authority and Seattle Housing Authority data sets
# Aim is to have a single row per contiguous time in a house per person
#
# STEPS:
# 01 - Process raw KCHA data and load to SQL database
# 02 - Process raw SHA data and load to SQL database
# 03 - Bring in individual PHA datasets and combine into a single file
# 04 - Deduplicate data and tidy up via matching process
# 05 - Recode race and other demographics
# 06 - Clean up addresses
# 06a - Geocode addresses
# 07 - Consolidate data rows
# 08 - Add in final data elements and set up analyses ### (THIS CODE) ###
# 09 - Join with Medicaid eligibility data
# 10 - Set up joint housing/Medicaid analyses
#
# Alastair Matheson (PHSKC-APDE)
# alastair.matheson@kingcounty.gov
# 2016-08-13, split into separate files 2017-10
# 
###############################################################################


#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, scipen = 999)

library(housing) # contains many useful functions for cleaning
library(openxlsx) # Used to import/export Excel files
library(lubridate) # Used to manipulate dates
library(tidyverse) # Used to manipulate data
library(RJSONIO)
library(RCurl)

script <- RCurl::getURL("https://raw.githubusercontent.com/jmhernan/Housing/uw_test/processing/metadata/set_data_env.r")
eval(parse(text = script))

METADATA = RJSONIO::fromJSON(paste0(housing_source_dir,"metadata/metadata.json"))
set_data_envr(METADATA,"combined")

#### Bring in data ####
if (UW == TRUE) {
  print("skip data load")
} else {
pha_cleanadd_sort_dedup <- readRDS(file = paste0(
  housing_path, pha_cleanadd_sort_dedup_fn))
}


### Strip out some variables that no longer have meaning 
# (e.g., act_date, act_type, household size (this will need to be remade))
if(UW == TRUE) {
  pha_longitudinal <- pha_cleanadd_sort_dedup %>%
    select(
      # Original name and SSN variables (with minor clean up)
      ssn_new, ssn_c, lname_new, fname_new, mname_new, lnamesuf_new, dob,
      gender_new, citizen, disability, relcode,
      # New name, SSN, and demog variables (after matching)
      pid, ssn_id_m6, ssn_id_m6_junk, lname_new_m6, fname_new_m6, mname_new_m6,
      lnamesuf_new_m6, gender_new_m6, dob_m6, r_aian_new, r_asian_new, r_black_new,
      r_nhpi_new, r_white_new, r_hisp_new, race_new, age, adult, senior,
      # Old household info
      mbr_num, hh_ssn_new:hh_dob, hh_id, 
      # New household info
      hh_ssn_id_m6:hh_id_new,
      # MTW background info
      # list_date, list_zip, list_homeless, housing_act,
      # Program info
      agency_new, major_prog, prog_type, subsidy_type, operator_type, 
      vouch_type_final, agency_prog_concat,
      # Old address info
      unit_add:unit_zip,
      # New address info
      unit_add_new:unit_zip_new, unit_concat,
      # Property/portfolio info
      property_id, property_name, property_type, portfolio, portfolio_final,
      # Unit info
      unit_id, unit_type, unit_year, access_unit, access_req, access_rec, 
      bed_cnt,
      # Date info
      admit_date, startdate, enddate,
      # Port info
      port_in, port_out_kcha, port_out_sha, cost_pha,
      # Personal asset/income info
      asset_val, asset_inc, inc_fixed, inc_vary, inc, inc_excl, inc_adj,
      # Household asset/income info
      hh_asset_val, hh_asset_inc, hh_asset_impute, hh_asset_inc_final,
      hh_inc_fixed, hh_inc_vary, hh_inc, hh_inc_adj, 
      hh_inc_tot, hh_inc_deduct, hh_inc_tot_adj,
      # Rent info
      rent_type:tb_rent_ceiling,
      # Received various forms of assistance (SHA only)
      # dropping for now
      # Linking and ID variables
      incasset_id, subsidy_id, vouch_num, cert_id, increment, contains("source")
      )
} else {
  pha_longitudinal <- pha_cleanadd_sort_dedup %>%
  select(
    # Original name and SSN variables (with minor clean up)
    ssn_new, ssn_c, lname_new, fname_new, mname_new, lnamesuf_new, dob,
    gender_new, citizen, disability, relcode,
    # New name, SSN, and demog variables (after matching)
    pid, ssn_id_m6, ssn_id_m6_junk, lname_new_m6, fname_new_m6, mname_new_m6,
    lnamesuf_new_m6, gender_new_m6, dob_m6, r_aian_new, r_asian_new, r_black_new,
    r_nhpi_new, r_white_new, r_hisp_new, race_new, age, adult, senior,
    # Old household info
    mbr_num, hh_ssn_new:hh_dob, hh_id, 
    # New household info
    hh_ssn_id_m6:hh_id_new,
    # MTW background info
    list_date, list_zip, list_homeless, housing_act,
    # Program info
    agency_new, major_prog, prog_type, subsidy_type, operator_type, 
    vouch_type_final, agency_prog_concat,
    # Old address info
    unit_add:unit_zip,
    # New address info
    unit_add_new:unit_zip_new, unit_concat, check_esri, check_google, 
    check_opencage, geocode_source, zip_centroid, add_geocoded, zip_geocoded, 
    lon, lat,
    # Property/portfolio info
    property_id, property_name, property_type, portfolio, portfolio_final,
    # Unit info
    unit_id, unit_type, unit_year, access_unit, access_req, access_rec, 
    bed_cnt, move_in_date,
    # Date info
    admit_date, startdate, enddate,
    # Port info
    port_in, port_out_kcha, port_out_sha, cost_pha,
    # Personal asset/income info
    asset_val, asset_inc, inc_fixed, inc_vary, inc, inc_excl, inc_adj,
    # Household asset/income info
    hh_asset_val, hh_asset_inc, hh_asset_impute, hh_asset_inc_final,
    hh_inc_fixed, hh_inc_vary, hh_inc, hh_inc_adj, 
    hh_inc_tot, hh_inc_deduct, hh_inc_tot_adj,
    # Rent info
    rent_type:tb_rent_ceiling,
    # Received various forms of assistance (SHA only)
    # dropping for now
    # Linking and ID variables
    incasset_id, subsidy_id, vouch_num, cert_id, increment, contains("source")
    )
}

### Set up time in housing for each row and note when there was a gap in coverage
pha_longitudinal <- pha_longitudinal %>% 
  mutate(cov_time = interval(start = startdate, end = enddate) / ddays(1) + 1,
         gap = case_when(
           is.na(lag(pid, 1)) | pid != lag(pid, 1) ~ 0,
           startdate - lag(enddate, 1) > 62 ~ 1,
           TRUE ~ NA_real_
         )) %>%
  # Find the number of unique periods a person was in housing
  group_by(pid, gap) %>%
  mutate(period = row_number() * gap + 1) %>% 
  ungroup() %>%
  # Fill in missing data using previous non-missing value
  tidyr::fill(., period) %>%
  select(-gap)


### Flag date to use when calculating length of stay
pha_longitudinal <- los(pha_longitudinal)

# Set up length of stay using the end date
pha_longitudinal <- pha_longitudinal %>%
  mutate(
    time_housing = round(interval(start = start_housing, end = enddate) / years(1), 1),
    time_pha = round(interval(start = start_pha, end = enddate) / years(1), 1),
    time_prog = round(interval(start = start_prog, end = enddate) / years(1), 1)
  )

### Age
# Age at each date
pha_longitudinal <- pha_longitudinal %>%
  mutate(age12 = round(interval(start = dob_m6, end = ymd(20121231)) / years(1), 1),
         age13 = round(interval(start = dob_m6, end = ymd(20131231)) / years(1), 1),
         age14 = round(interval(start = dob_m6, end = ymd(20141231)) / years(1), 1),
         age15 = round(interval(start = dob_m6, end = ymd(20151231)) / years(1), 1),
         age16 = round(interval(start = dob_m6, end = ymd(20161231)) / years(1), 1),
         age17 = round(interval(start = dob_m6, end = ymd(20171231)) / years(1), 1)
  )

# Single age grouping
# Note: this is only valid for the person at the time of the start date
# Need to recalculate for each time period being analyzed
pha_longitudinal <- pha_longitudinal %>%
  mutate(agegrp = case_when(
    adult == 0 & !is.na(adult) ~ "Youth",
    adult == 1 & senior == 0 & !is.na(adult) ~ "Working age",
    adult == 1 & senior == 1 & !is.na(adult) ~ "Senior",
    TRUE ~ NA_character_
    ))

### Recode gender and disability
pha_longitudinal <- pha_longitudinal %>%
  mutate(gender2 = car::recode(gender_new_m6, "'1' = 'Female'; '2' = 'Male'; else = NA"),
         disability2 = car::recode(disability, "'1' = 'Disabled'; '0' = 'Not disabled'; else = NA"))


### ZIPs to restrict to KC and surrounds (helps make maps that drop far-flung ports)
zips <- read.csv(text = RCurl::getURL("https://raw.githubusercontent.com/PHSKC-APDE/reference-data/master/Spatial%20data/KC%20ZIPs.csv"), 
                 header = TRUE, stringsAsFactors = FALSE)
names(zips) <- tolower(names(zips))
zips <- zips %>% select(zipcode) %>% 
  mutate(kc_area = 1)

pha_longitudinal <- pha_longitudinal %>%
  mutate(unit_zip_new = as.numeric(unit_zip_new)) %>%
  left_join(., zips, by = c("unit_zip_new" = "zipcode"))
rm(zips)



#### Save point ####
# saveRDS(pha_longitudinal, file = paste0(housing_path, pha_longitudinal_fn))

write.csv(pha_longitudinal, file = paste0(hild_dir,"pha_longitudinal.csv"))
### Clean up remaining data frames
rm(pha_cleanadd_sort_dedup)
gc()
