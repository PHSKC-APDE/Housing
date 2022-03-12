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
# 09 - Join with Medicaid-Medicare eligibility & time varying data
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
library(data.table) # Used to manipulate data

script <- httr::content(httr::GET("https://raw.githubusercontent.com/PHSKC-APDE/Housing/main/processing/metadata/set_data_env.r"))
eval(parse(text = script))
METADATA = RJSONIO::fromJSON(paste0(housing_source_dir,"metadata/metadata.json"))
set_data_envr(METADATA,"combined")

if (sql == TRUE) {
  library(odbc) # Used to connect to SQL server
  db_apde51 <- dbConnect(odbc(), "PH_APDEStore51")
}

#### Bring in data ####
if (UW == TRUE) {
  print("skip data load")
} else {
#### Bring in data ####
pha_cleanadd_sort_dedup <- readRDS(file.path(housing_path, pha_cleanadd_sort_dedup_fn))
pha_cleanadd_sort_dedup <- setDT(pha_cleanadd_sort_dedup)
}


#### Strip out some variables that no longer have meaning ####
# (e.g., act_date, act_type, household size (this will need to be remade))
if(UW == TRUE) {
  cols_select <- c(
    # Original name and SSN variables (with minor clean up)
    "ssn_new", "ssn_c", "lname_new", "fname_new", "mname_new", "lnamesuf_new", 
    "dob", "gender_new", "citizen", "disability", "relcode",
    # New name, SSN, and demog variables (after matching)
    "pid", "ssn_id_m6", "ssn_id_m6_junk", "lname_new_m6", "fname_new_m6", 
    "mname_new_m6", "lnamesuf_new_m6", "gender_new_m6", 
    "dob_m6", "age", "adult", "senior",
    "r_aian_new", "r_asian_new", "r_black_new", "r_nhpi_new", 
    "r_white_new", "r_hisp_new", "race_new",
    # Old household info
    "mbr_num", "hh_ssn_new", "hh_ssn_c", "hh_lname", "hh_fname", "hh_mname",
    "hh_lnamesuf", "hh_dob", "hh_id", 
    # New household info
    "hh_ssn_id_m6", "hh_ssn_id_m6_junk", "hh_lname_m6", "hh_lnamesuf_m6", 
    "hh_fname_m6", "hh_mname_m6", "hh_dob_m6", "hh_id_new",
    # MTW background info
    # list_date, list_zip, list_homeless, housing_act,
    # Program info
    "agency_new", "major_prog", "prog_type", "subsidy_type", "operator_type", 
    "vouch_type_final", "agency_prog_concat",
    # Old address info
    "unit_add", "unit_apt", "unit_apt2", "unit_city", "unit_state", "unit_zip",
    # New address info
    "unit_add_new", "unit_apt_new", "unit_city_new", "unit_state_new", 
    "unit_zip_new", "unit_concat",
    # Property/portfolio info
    "property_id", "property_name", "property_type", "portfolio", "portfolio_final",
    # Unit info
    "unit_id", "unit_type", "unit_year", "access_unit", "access_req", "access_rec", 
    "bed_cnt",
    # Date info
    "admit_date", "startdate", "enddate",
    # Port info
    "port_in", "port_out_kcha", "port_out_sha", "cost_pha",
    # Personal asset/income info
    "asset_val", "asset_inc", "inc_fixed", "inc_vary", "inc", "inc_excl", "inc_adj",
    # Household asset/income info
    "hh_asset_val", "hh_asset_inc", "hh_asset_impute", "hh_asset_inc_final",
    "hh_inc_fixed", "hh_inc_vary", "hh_inc", "hh_inc_adj", 
    "hh_inc_tot", "hh_inc_deduct", "hh_inc_tot_adj",
    # Rent info
    "rent_type", "rent_tenant", "rent_mixfam", "ph_util_allow", "ph_rent_ceiling",
    "subs_type", "bdrm_voucher", "cost_month", "rent_owner", "tb_util_allow", 
    "rent_gross", "rent_subs", "rent_tenant_owner", 
    "rent_mixfam_owner", "tb_rent_ceiling",
    # Received various forms of assistance (SHA only)
    # dropping for now
    # Linking and ID variables
    "incasset_id", "subsidy_id", "vouch_num", "cert_id", "increment", 
    "sha_source", "kcha_source", "eop_source")
} else {
  cols_select <- c(
    # Original name and SSN variables (with minor clean up)
    "ssn_new", "ssn_c", "lname_new", "fname_new", "mname_new", "lnamesuf_new", 
    "dob", "gender_new", "citizen", "disability", "relcode",
    # New name, SSN, and demog variables (after matching)
    "pid", "ssn_id_m6", "ssn_id_m6_junk", "lname_new_m6", "fname_new_m6", 
    "mname_new_m6", "lnamesuf_new_m6", "gender_new_m6", 
    "dob_m6", "age", "adult", "senior",
    "r_aian_new", "r_asian_new", "r_black_new", "r_nhpi_new", 
    "r_white_new", "r_hisp_new", "race_new",
    # Old household info
    "mbr_num", "hh_ssn_new", "hh_ssn_c", "hh_lname", "hh_fname", "hh_mname",
    "hh_lnamesuf", "hh_dob", "hh_id", 
    # New household info
    "hh_ssn_id_m6", "hh_ssn_id_m6_junk", "hh_lname_m6", "hh_lnamesuf_m6", 
    "hh_fname_m6", "hh_mname_m6", "hh_dob_m6", "hh_id_new",
    # MTW background info
    "list_date", "list_zip", "list_homeless", "housing_act",
    # Program info
    "agency_new", "major_prog", "prog_type", "subsidy_type", "operator_type", 
    "vouch_type_final", "agency_prog_concat",
    # Old address info
    "unit_add", "unit_apt", "unit_apt2", "unit_city", 
    "unit_state", "unit_zip", "geo_hash_raw",
    # New address info
    "unit_add_new", "unit_apt_new", "unit_city_new", "unit_state_new", 
    "unit_zip_new", "geo_hash_clean", "geo_hash_geocode", "geo_blank",
    # Property/portfolio info
    "property_id", "property_name", "property_type", "portfolio", "portfolio_final",
    # Unit info
    "unit_id", "unit_type", "unit_year", "access_unit", "access_req", "access_rec", 
    "bed_cnt",
    # Date info
    "admit_date", "startdate", "enddate",
    # Port info
    "port_in", "port_out_kcha", "port_out_sha", "cost_pha",
    # Personal asset/income info
    "asset_val", "asset_inc", "inc_fixed", "inc_vary", "inc", "inc_excl", "inc_adj",
    # Household asset/income info
    "hh_asset_val", "hh_asset_inc", "hh_asset_impute", "hh_asset_inc_final",
    "hh_inc_fixed", "hh_inc_vary", "hh_inc", "hh_inc_adj", 
    "hh_inc_tot", "hh_inc_deduct", "hh_inc_tot_adj",
    # Rent info
    "rent_type", "rent_tenant", "rent_mixfam", "ph_util_allow", "ph_rent_ceiling",
    "subs_type", "bdrm_voucher", "cost_month", "rent_owner", "tb_util_allow", 
    "rent_gross", "rent_subs", "rent_tenant_owner", 
    "rent_mixfam_owner", "tb_rent_ceiling",
    # Received various forms of assistance (SHA only)
    # dropping for now
    # Linking and ID variables
    "incasset_id", "subsidy_id", "vouch_num", "cert_id", "increment", 
    "sha_source", "kcha_source", "eop_source")
}

pha_longitudinal <- pha_cleanadd_sort_dedup[, ..cols_select]


#### Set up time in housing for each row and note when there was a gap in coverage ####
pha_longitudinal <- pha_longitudinal[order(pid, startdate, enddate)]
pha_longitudinal[, ':=' (
  cov_time = interval(start = startdate, end = enddate) / ddays(1) + 1,
  gap = case_when(is.na(lag(pid, 1)) | pid != lag(pid, 1) ~ 0,
                  startdate - lag(enddate, 1) > 62 ~ 1,
                  TRUE ~ NA_real_))]
# Find the number of unique periods a person was in housing
pha_longitudinal[, period := rowid(pid, gap) * gap + 1]

# Fill in missing data using previous non-missing value
# Use new fill NA function in data.table (requires 1.12.3 or higher)
setnafill(pha_longitudinal, "locf", cols = "period")
pha_longitudinal[, gap := NULL]


#### Flag date to use when calculating length of stay ####
pha_longitudinal <- los(pha_longitudinal)

# Set up length of stay using the end date
pha_longitudinal[, ':=' (
  time_housing = round(interval(start = start_housing, end = enddate) / years(1), 1),
  time_pha = round(interval(start = start_pha, end = enddate) / years(1), 1),
  time_prog = round(interval(start = start_prog, end = enddate) / years(1), 1)
)]


#### Age ####
# Age at each date
pha_longitudinal[, ':=' (
  age12 = round(interval(start = dob_m6, end = ymd(20121231)) / years(1), 1),
  age13 = round(interval(start = dob_m6, end = ymd(20131231)) / years(1), 1),
  age14 = round(interval(start = dob_m6, end = ymd(20141231)) / years(1), 1),
  age15 = round(interval(start = dob_m6, end = ymd(20151231)) / years(1), 1),
  age16 = round(interval(start = dob_m6, end = ymd(20161231)) / years(1), 1),
  age17 = round(interval(start = dob_m6, end = ymd(20171231)) / years(1), 1),
  age18 = round(interval(start = dob_m6, end = ymd(20181231)) / years(1), 1),
  age19 = round(interval(start = dob_m6, end = ymd(20191231)) / years(1), 1),
  age20 = round(interval(start = dob_m6, end = ymd(20201231)) / years(1), 1)
)]


# Single age grouping
# Note: this is only valid for the person at the time of the start date
# Need to recalculate for each time period being analyzed
pha_longitudinal[, agegrp := case_when(
  adult == 0 & !is.na(adult) ~ "Youth",
  adult == 1 & senior == 0 & !is.na(adult) ~ "Working age",
  adult == 1 & senior == 1 & !is.na(adult) ~ "Senior",
  TRUE ~ NA_character_)]


#### Recode gender and disability ####
pha_longitudinal[, ':=' (
  gender2 = recode(gender_new_m6, "1" = "Female", "2" = "Male", .default = NA_character_),
  disability2 = recode(disability, "1" = "Disabled", "0" = "Not disabled", .default = NA_character_)
)]


#### ZIPs to restrict to KC and surrounds (helps make maps that drop far-flung ports) ####
zips <- read.csv(text = httr::content(httr::GET("https://raw.githubusercontent.com/PHSKC-APDE/reference-data/main/spatial_data/zip_hca.csv")), 
                 header = TRUE) %>% 
  select(zip) %>% mutate(zip = as.character(zip), kc_area = 1)
zips <- setDT(zips)


pha_longitudinal <- merge(pha_longitudinal, zips, 
                          by.x = c("unit_zip_new"), by.y = c("zip"),
                          all.x = TRUE, sort = F)

rm(zips)


### Reorder variables
pha_longitudinal <- pha_longitudinal[, 
                                     .(pid, ssn_new, ssn_c, lname_new, fname_new, mname_new, lnamesuf_new, 
                                       dob, gender_new, citizen, disability, relcode, ssn_id_m6, ssn_id_m6_junk, 
                                       lname_new_m6, fname_new_m6, mname_new_m6, lnamesuf_new_m6, gender_new_m6, 
                                       dob_m6, age, adult, senior, r_aian_new, r_asian_new, r_black_new, 
                                       r_nhpi_new, r_white_new, r_hisp_new, race_new, mbr_num, hh_ssn_new, 
                                       hh_ssn_c, hh_lname, hh_fname, hh_mname, hh_lnamesuf, hh_dob, hh_id, 
                                       hh_ssn_id_m6, hh_ssn_id_m6_junk, hh_lname_m6, hh_lnamesuf_m6, hh_fname_m6, 
                                       hh_mname_m6, hh_dob_m6, hh_id_new, list_date, list_zip, list_homeless, 
                                       housing_act, agency_new, major_prog, prog_type, subsidy_type, operator_type, 
                                       vouch_type_final, agency_prog_concat, 
                                       unit_add, unit_apt, unit_apt2, unit_city, unit_state, unit_zip, geo_hash_raw, 
                                       unit_add_new, unit_apt_new, unit_city_new, unit_state_new, 
                                       unit_zip_new, geo_hash_clean, geo_hash_geocode, geo_blank,
                                       property_id, property_name, property_type, 
                                       portfolio, portfolio_final, unit_id, unit_type, unit_year, 
                                       access_unit, access_req, access_rec, bed_cnt, admit_date, startdate, enddate, 
                                       port_in, port_out_kcha, port_out_sha, cost_pha, asset_val, asset_inc, 
                                       inc_fixed, inc_vary, inc, inc_excl, inc_adj, hh_asset_val, hh_asset_inc, 
                                       hh_asset_impute, hh_asset_inc_final, hh_inc_fixed, hh_inc_vary, hh_inc, 
                                       hh_inc_adj, hh_inc_tot, hh_inc_deduct, hh_inc_tot_adj, rent_type, rent_tenant, 
                                       rent_mixfam, ph_util_allow, ph_rent_ceiling, subs_type, bdrm_voucher, 
                                       cost_month, rent_owner, tb_util_allow, rent_gross, rent_subs, rent_tenant_owner, 
                                       rent_mixfam_owner, tb_rent_ceiling, incasset_id, subsidy_id, vouch_num, 
                                       cert_id, increment, sha_source, kcha_source, eop_source, cov_time, period, 
                                       start_housing, start_pha, start_prog, time_housing, time_pha, time_prog, 
                                       age12, age13, age14, age15, age16, age17, age18, age19, age20, 
                                       agegrp, gender2, disability2, kc_area)]


#### Save point ####
# saveRDS(pha_longitudinal, file = paste0(housing_path, pha_longitudinal_fn))


#### WRITE RESHAPED DATA TO SQL ####
if (sql == TRUE) {
  dbWriteTable(db_apde51, 
               name = DBI::Id(schema = "stage", table = "pha"), 
               value = as.data.frame(pha_longitudinal),
               field.types = c(dob = "date", dob_m6 = "date",
                               hh_dob = "date", hh_dob_m6 = "date",
                               admit_date = "date", startdate = "date",
                               enddate = "date"),
               overwrite = T)
}

if (UW == T) {
write.csv(pha_longitudinal, file = paste0(hild_dir,"pha_longitudinal.csv"))
}

### Clean up remaining data frames
rm(pha_cleanadd_sort_dedup)
gc()
