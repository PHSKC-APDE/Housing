###############################################################################
# OVERVIEW:
# Code to create a cleaned person table from the combined 
# King County Housing Authority and Seattle Housing Authority data sets
# Aim is to have a single row per contiguous time in a house per person
#
# STEPS:
# Process raw KCHA data and load to SQL database
# Process raw SHA data and load to SQL database
# Bring in individual PHA datasets and combine into a single file
# Deduplicate data and tidy up via matching process
# Recode race and other demographics
# Clean up addresses and geocode
# Consolidate data rows
# Add in final data elements and set up analyses
# Join with Medicaid eligibility data and set up analyses  ### (THIS CODE) ###
#
# Alastair Matheson (PHSKC-APDE)
# alastair.matheson@kingcounty.gov
# 2016-08-13, split into separate files 2017-10
# 
###############################################################################


##### Set up global parameter and call in libraries #####
options(max.print = 350, tibble.print_max = 30, scipen = 999)
housing_path <- "//phdata01/DROF_DATA/DOH DATA/Housing"

library(odbc) # Used to connect to SQL server
library(openxlsx) # Used to import/export Excel files
library(lubridate) # Used to manipulate dates
library(tidyverse) # Used to manipulate data
library(RecordLinkage) # used to make the linkage
library(phonics) # used to extract phonetic version of names


##### Connect to the servers #####
db.apde51 <- dbConnect(odbc(), "PH_APDEStore51")
db.claims51 <- dbConnect(odbc(), "PHClaims51")


##### Bring in data #####
### Housing
pha_longitudinal <- readRDS(file = paste0(housing_path, "/OrganizedData/pha_longitudinal.Rda"))


# Limit to one row per person and only variables used for merging (use most recent row of data)
# Filter if person's most recent enddate is <2012 since they can't match to Medicaid
pha_merge <- pha_longitudinal %>%
  filter(year(enddate) >= 2012) %>%
  distinct(ssn_id_m6, lname_new_m6, fname_new_m6, mname_new_m6, 
           dob_m6, gender_new_m6, enddate) %>%
  arrange(ssn_id_m6, lname_new_m6, fname_new_m6, mname_new_m6, 
          dob_m6, gender_new_m6, enddate) %>%
  group_by(ssn_id_m6, lname_new_m6, fname_new_m6, dob_m6) %>%
  slice(n()) %>%
  ungroup() %>%
  select(-(enddate)) %>%
  rename(ssn_new = ssn_id_m6, lname_new = lname_new_m6, 
         fname_new = fname_new_m6, mname_new = mname_new_m6, 
         dob = dob_m6, gender_new = gender_new_m6) %>%
  mutate(dob_y = year(dob), dob_m = month(dob), dob_d = day(dob),
         # Make a variable to match the Medicaid ID and order vars the same 
         mid = "") %>%
  select(mid, ssn_new:dob_d)


### Basic Medicaid eligibility table with link to names (~ 100-150 secs)
system.time(
  elig <- dbGetQuery(db.claims51,
                     "SELECT * FROM PHClaims.dbo.mcaid_elig_dual AS a
                     
                     LEFT JOIN
                     (SELECT DISTINCT b.*, c.LAST_NAME AS lname_m, 
                        c.FIRST_NAME AS fname_m, c.MIDDLE_NAME AS mname_m
                       FROM (
                         SELECT a.MEDICAID_RECIPIENT_ID AS mid, max(calmo) as maxdate
                         FROM (
                           SELECT MEDICAID_RECIPIENT_ID, 
                           cast(CLNDR_YEAR_MNTH as bigint) as calmo
                           from PHClaims.dbo.NewEligibility
                         ) AS a
                         group by a.MEDICAID_RECIPIENT_ID
                       ) AS b
                       
                       LEFT JOIN
                       (SELECT MEDICAID_RECIPIENT_ID, LAST_NAME, FIRST_NAME, 
                         MIDDLE_NAME, CLNDR_YEAR_MNTH
                         FROM PHClaims.dbo.NewEligibility
                       ) AS c
                       
                       ON b.mid = c.MEDICAID_RECIPIENT_ID AND 
                       b.maxdate = c.CLNDR_YEAR_MNTH) AS d
                     ON a.id = d.mid
                     ORDER BY id, from_date")
)


### Processed demographics (~6 secs)
system.time(elig_demog <- dbGetQuery(db.claims51,
                         "SELECT id, ssnnew, dobnew, gender_mx, 
                          race_mx, latino, maxlang
                         FROM PHClaims.dbo.mcaid_elig_demoever"))


#### Join data together ####
### First bring the Medicaid demographics together and fix formats
elig_join <- left_join(elig, elig_demog, by = "id") %>%
  mutate(
    gender_mx = as.numeric(car::recode(gender_mx, "'Female' = 1; 'Male' = 2; 'Multiple' = 3; else = NA")),
    dobnew = as.Date(str_sub(dobnew, 1, 10), format("%Y-%m-%d"))
    ) %>%
  select(-maxdate, -id)

# Rename matching variables to match housing data and restrict to these vars 
# (may expand to include race and address later)
elig_merge <- elig_join %>%
  rename(ssn_new = ssnnew, lname_new = lname_m, fname_new = fname_m, 
         mname_new = mname_m, gender_new = gender_mx, dob = dobnew) %>%
  select(mid, ssn_new, lname_new, fname_new, mname_new, gender_new, dob) %>%
  # Reduce to one row per person
  distinct() %>%
  mutate(dob_y = year(dob), dob_m = month(dob), dob_d = day(dob),
         # Remove missing name for more accurate match weight
         mname_new = ifelse(is.na(mname_new), "", mname_new))
  

##### Match 1 #####
# Block on SSN, match other vars
match1 <- compare.linkage(pha_merge, elig_merge, blockfld = c("ssn_new"),
                strcmp = c("mname_new", "gender_new", "dob_y", "dob_m", "dob_d"),
                phonetic = c("lname_new", "fname_new"), phonfun = soundex,
                exclude = c("dob", "mid"))

# Using EpiLink approach
match1_tmp <- epiWeights(match1)
classify1 <- epiClassify(match1_tmp, threshold.upper = 0.47)
summary(classify1)
pairs1 <- getPairs(classify1, single.rows = TRUE)

# Make record of pairs
pairs1 <- mutate(pairs1, pair = row_number())

# Looks like 0.47 is a good cutoff as long as DOBs aren't too far apart
# Need to decide which is correct version at some point, use Medicaid data as default for now
pairs1_full <- pairs1 %>%
  select(pair, ssn_new.1:dob.1, ssn_new.2:dob.2, mid.2, Weight) %>%
  filter(Weight >= 0.47 & (abs(dob.1-dob.2) <= 730 | is.na(abs(dob.1-dob.2))))


##### Match 2 #####
# Block on soundex last name, match other vars
# Restrict to PHA-generate IDs to avoid memory issues
pha_merge_id <- pha_merge %>%
  filter(str_detect(ssn_new, "[:alpha:]+"))


match2 <- compare.linkage(pha_merge_id, elig_merge, blockfld = c("lname_new"),
                          strcmp = c("mname_new", "gender_new", "dob_y", "dob_m", "dob_d"),
                          phonetic = c("fname_new"), phonfun = soundex,
                          exclude = c("dob", "ssn_new", "mid"))

# Using EpiLink approach
match2_tmp <- epiWeights(match2)
classify2 <- epiClassify(match2_tmp, threshold.upper = 0.85)
summary(classify2)
pairs2 <- getPairs(classify2, single.rows = TRUE)

# Make record of pairs
pairs2 <- mutate(pairs2, pair = row_number() + max(pairs1$pair))

# Looks like 0.85 is a good cutoff here, captures 1 twin pair still
# Allow for DOB date/month swaps but otherwise have stricter criteria for DOB differences
pairs2_full <- pairs2 %>%
  filter(Weight >= 0.85 & abs(dob.1 - dob.2) <= 30) %>% 
  select(pair, ssn_new.1:dob.1, ssn_new.2:dob.2, mid.2, Weight)

##### END OF MATCHING #####


#### Join matched pairs together and deduplicate ####
pairs_final <- bind_rows(pairs1_full, pairs2_full)
pairs_final <- pairs_final %>% distinct()

# Join back to Medicaid and PHA data (keep full data from all existing datasets)
pha_elig_merge <- pairs_final %>%
  # Get the Medicaid SSN back to numeric for joining with full Medicaid data
  mutate(ssn_new.2 = as.numeric(ssn_new.2)) %>%
  full_join(., elig_join, by = c("mid.2" = "mid")) %>%
  full_join(., pha_longitudinal, by = c("ssn_new.1" = "ssn_id_m6", "lname_new.1" = "lname_new_m6",
                                        "fname_new.1" = "fname_new_m6", "dob.1" = "dob_m6"))


# Rename core variables
# (m for Medicaid, h for housing where there is ambiguity)
pha_elig_merge <- pha_elig_merge %>%
  rename(startdate_m = from_date, enddate_m = to_date,
         startdate_h = startdate, enddate_h = enddate,
         mid = mid.2, dual_elig_m = dual, cov_time_m = cov_time_day,
         ssn_h = ssn_new.1, ssn_m = ssn_new.2,
         lname_h = lname_new.1, lnamesuf_h = lnamesuf_new_m6, 
         fname_h = fname_new.1, mname_h = mname_new_m6,
         dob_h = dob.1, dob_m = dobnew, agegrp_h = agegrp, adult_h = adult,
         senior_h = senior, age12_h = age12, age13_h = age13, age14_h = age14, 
         age15_h = age15, age16_h = age16, age17_h = age17,
         gender_h = gender_new_m6, gender2_h = gender2, gender_m = gender_mx,
         race_h = race_new, race_m = race_mx, 
         hisp_h = r_hisp_new, hisp_m = latino,
         disability_h = disability, disability2_h = disability2,
         citizen_h = citizen, hh_id_new_h = hh_id_new,
         hh_ssn_h = hh_ssn_id_m6, hh_lname_h = hh_lname_m6, 
         hh_lnamesuf_h = hh_lnamesuf_m6, hh_fname_h = hh_fname_m6,
         hh_mname_h = hh_mname_m6, hh_dob_h = hh_dob_m6,
         unit_add_h = unit_add_new, unit_apt_h = unit_apt_new,
         unit_apt2_h = unit_apt2_new, unit_city_h = unit_city_new,
         unit_state_h = unit_state_new, unit_zip_h = unit_zip_new,
         unit_concat_h = unit_concat)

# Additional renaming and select depending on whether or not geocoding was run
if ("X" %in% names(pha_elig_merge)) {
  pha_elig_merge <- pha_elig_merge %>%
    rename(x_h = X, y_h = Y, formatted_address_h = formatted_address) %>%
    select(
      # Name, SSN, and demog variables from housing data
      pid, ssn_h, lname_h, lnamesuf_h, fname_h, mname_h, dob_h, agegrp_h,
      age12_h:age17_h, adult_h, senior_h, gender_h, gender2_h, citizen_h, 
      disability_h, disability2_h, race_h, hisp_h, 
      # Household info from housing data
      mbr_num, hh_id_new_h, hh_ssn_h, hh_lname_h, hh_lnamesuf_h, hh_fname_h, 
      hh_mname_h, hh_dob_h, 
      # MTW background info
      list_date, list_zip, list_homeless, housing_act,
      # Housing program info
      agency_new, major_prog, prog_type, subsidy_type, operator_type, 
      vouch_type_final, agency_prog_concat,
      # Housing address info
      unit_add_h:unit_zip_h, unit_concat_h, kc_area,
      formatted_address_h, x_h, y_h,
      # Property/portfolio info
      property_id, property_name, property_type, portfolio, portfolio_final,
      # Housing unit info
      unit_id, unit_type, unit_year, access_unit, access_req, access_rec, 
      bed_cnt, move_in_date,
      # Port info
      port_in, port_out_kcha, port_out_sha, cost_pha,
      # Housing personal asset/income info
      asset_val, asset_inc, inc_fixed, inc_vary, inc, inc_excl, inc_adj,
      # Household asset/income info
      hh_asset_val, hh_asset_inc, hh_asset_impute, hh_asset_inc_final,
      hh_inc_fixed, hh_inc_vary, hh_inc, hh_inc_adj, 
      hh_inc_tot, hh_inc_deduct, hh_inc_tot_adj,
      # Rent info
      rent_type:tb_rent_ceiling,
      # Linking and ID variables
      incasset_id, subsidy_id, vouch_num, cert_id, increment, contains("source"),
      # Name, SSN, and demog variables from Medicaid data
      mid, ssn_m, lname_m, fname_m, mname_m, dob_m, gender_m, race_m, hisp_m, 
      dual_elig_m, cov_time_m,
      # Date info
      startdate_h, enddate_h, period:time_prog, startdate_m, enddate_m) 
} else {
  pha_elig_merge <- pha_elig_merge %>%
    select(
      # Name, SSN, and demog variables from housing data
      pid, ssn_h, lname_h, lnamesuf_h, fname_h, mname_h, dob_h, agegrp_h,
      age12_h:age17_h, adult_h, senior_h, gender_h, gender2_h, citizen_h, 
      disability_h, disability2_h, race_h, hisp_h, 
      # Household info from housing data
      mbr_num, hh_id_new_h, hh_ssn_h, hh_lname_h, hh_lnamesuf_h, hh_fname_h, 
      hh_mname_h, hh_dob_h, 
      # MTW background info
      list_date, list_zip, list_homeless, housing_act,
      # Housing program info
      agency_new, major_prog, prog_type, subsidy_type, operator_type, 
      vouch_type_final, agency_prog_concat,
      # Housing address info
      unit_add_h:unit_zip_h, unit_concat_h, kc_area,
      # Property/portfolio info
      property_id, property_name, property_type, portfolio, portfolio_final,
      # Housing unit info
      unit_id, unit_type, unit_year, access_unit, access_req, access_rec, 
      bed_cnt, move_in_date,
      # Port info
      port_in, port_out_kcha, port_out_sha, cost_pha,
      # Housing personal asset/income info
      asset_val, asset_inc, inc_fixed, inc_vary, inc, inc_excl, inc_adj,
      # Household asset/income info
      hh_asset_val, hh_asset_inc, hh_asset_impute, hh_asset_inc_final,
      hh_inc_fixed, hh_inc_vary, hh_inc, hh_inc_adj, 
      hh_inc_tot, hh_inc_deduct, hh_inc_tot_adj,
      # Rent info
      rent_type:tb_rent_ceiling,
      # Linking and ID variables
      incasset_id, subsidy_id, vouch_num, cert_id, increment, contains("source"),
      # Name, SSN, and demog variables from Medicaid data
      mid, ssn_m, lname_m, fname_m, mname_m, dob_m, gender_m, race_m, hisp_m, 
      dual_elig_m, cov_time_m,
      # Date info
      startdate_h, enddate_h, period:time_prog, startdate_m, enddate_m) 
}


pha_elig_merge <- pha_elig_merge %>%
  # Set up coverage times to look for overlap
  mutate(
    # Make sure dates are in the correct format
    startdate_h = as.Date(startdate_h, origin="1970-01-01", format = "%Y-%m-%d"),
    enddate_h = as.Date(enddate_h, origin="1970-01-01", format = "%Y-%m-%d"),
    startdate_m = as.Date(startdate_m, origin="1970-01-01", format = "%Y-%m-%d"),
    enddate_m = as.Date(enddate_m, origin="1970-01-01", format = "%Y-%m-%d")
  )


# Remove temporary data
rm(list = ls(pattern = "pairs"))
rm(list = ls(pattern = "pha_merge"))
rm(list = ls(pattern = "classify"))
rm(list = ls(pattern = "match"))
rm(elig_join)
rm(elig_merge)
gc()


#### Set up new variables ####
### Make combined demographics
# Logic: assume Medicaid is correct when there are conflicts 
# (housing data likely to overestimate multiple race for example), 
# use housing data to fill in missing Medicaid info.
pha_elig_merge <- pha_elig_merge %>%
  mutate(
    race_c = case_when(!is.na(race_m) ~ race_m,
                            !is.na(race_h) ~ race_h),
    hisp_c = case_when(!is.na(hisp_m) ~ hisp_m,
                       !is.na(hisp_h) ~ hisp_h),
    gender_c = case_when(!is.na(gender_m) ~ gender_m,
                       !is.na(gender_h) ~ gender_h))


# Make new unique ID to anonymize data
pha_elig_merge$pid2 <- group_indices(pha_elig_merge, mid, ssn_m, ssn_h, 
                                     lname_h, fname_h, dob_h)


#### Save point ####
saveRDS(pha_elig_merge, file = paste0(housing_path, "/OrganizedData/pha_elig_merge.Rda"))
#pha_elig_merge <- readRDS(file = paste0(housing_path, "/OrganizedData/pha_elig_merge.Rda"))


##### Calculate overlapping periods #####
### Set up intervals in each data set
# This is horribly slow and ugly code. Would like to make it more efficient.

temp <- pha_elig_merge %>%
  mutate(overlap_type = case_when(
    # First ID the non-matches
    is.na(startdate_h) | is.na(startdate_m) ~ 0,
    # Then figure out which overlapping date comes first
    # Exactly the same dates
    startdate_h == startdate_m & enddate_h == enddate_m ~ 1,
    # Housing before Medicaid (or exactly the same dates)
    startdate_h <= startdate_m & startdate_m <= enddate_h & 
      enddate_h <= enddate_m ~ 2,
    # Medicaid before housing
    startdate_m <= startdate_h & startdate_h <= enddate_m & 
      enddate_m <= enddate_h ~ 3,
    # Medicaid dates competely within housing dates or vice versa
    startdate_m >= startdate_h & enddate_m <= enddate_h ~ 4,
    startdate_h >= startdate_m & enddate_h <= enddate_m ~ 5,
    # Housing coverage only before Medicaid (or Medicaid only after housing)
    startdate_h < startdate_m & enddate_h < startdate_m ~ 6,
    # Housing coverage only after Medicaid (or Medicaid only before housing)
    startdate_h > enddate_m & enddate_h > enddate_m ~ 7,
    # Anyone rows that are left
    TRUE ~ 8),
    # Calculate overlapping dates
    startdate_o = as.Date(case_when(
      overlap_type %in% c(1, 2, 4) ~ startdate_m,
      overlap_type %in% c(3, 5) ~ startdate_h), origin = "1970-01-01"),
    enddate_o = as.Date(ifelse(overlap_type %in% c(1:5),
                               pmin(enddate_m, enddate_h),
                               NA), origin = "1970-01-01"),
    # Need to duplicate rows to separate out non-overlapping housing and Medicaid periods
    repnum = case_when(
      overlap_type %in% c(2:5) ~ 3,
      overlap_type %in% c(6:7) ~ 2,
      TRUE ~ 1)
    ) %>%
  select(pid2, startdate_h, enddate_h, startdate_m, enddate_m, 
         startdate_o, enddate_o, overlap_type, repnum) %>%
  arrange(pid2, startdate_h, startdate_m, startdate_o, 
          enddate_h, enddate_m, enddate_o)


### Expand out rows to separate out overlaps
temp_ext <- temp[rep(seq(nrow(temp)), temp$repnum), 1:ncol(temp)]


temp_ext <- temp_ext %>% 
  group_by(pid2, startdate_h, enddate_h, startdate_m, enddate_m) %>% 
  mutate(rownum_temp = row_number()) %>%
  ungroup() %>%
  arrange(pid2, startdate_h, enddate_h, startdate_m, enddate_m, startdate_o, 
          enddate_o, overlap_type, rownum_temp) %>%
  mutate(
    # Remove non-overlapping dates
    startdate_h = as.Date(ifelse((overlap_type == 6 & rownum_temp == 2) | 
                                   (overlap_type == 7 & rownum_temp == 1), 
                                 NA, startdate_h), origin = "1970-01-01"), 
    enddate_h = as.Date(ifelse((overlap_type == 6 & rownum_temp == 2) | 
                                 (overlap_type == 7 & rownum_temp == 1), 
                               NA, enddate_h), origin = "1970-01-01"),
    startdate_m = as.Date(ifelse((overlap_type == 6 & rownum_temp == 1) | 
                                   (overlap_type == 7 & rownum_temp == 2), 
                                 NA, startdate_m), origin = "1970-01-01"), 
    enddate_m = as.Date(ifelse((overlap_type == 6 & rownum_temp == 1) | 
                                 (overlap_type == 7 & rownum_temp == 2), 
                               NA, enddate_m), origin = "1970-01-01")) %>%
  distinct(pid2, startdate_h, enddate_h, startdate_m, enddate_m, startdate_o, 
           enddate_o, overlap_type, rownum_temp, .keep_all = TRUE) %>%
  # Remove first row if start dates are the same or housing is only one day
  filter(!(overlap_type %in% c(2:5) & rownum_temp == 1 & 
             (startdate_h == startdate_m | startdate_h == enddate_h))) %>%
  # Remove third row if enddates are the same
  filter(!(overlap_type %in% c(2:5) & rownum_temp == 3 & enddate_h == enddate_m))


### Calculate finalized date columns
temp_ext <- temp_ext %>%
  # Set up combined dates
  mutate(
    # Start with rows with only housing or Medicaid, or when both sets of dates are identical
    startdate_c = as.Date(
      case_when(
        (!is.na(startdate_h) & is.na(startdate_m)) | overlap_type == 1 ~ startdate_h,
        !is.na(startdate_m) & is.na(startdate_h) ~ startdate_m), origin = "1970-01-01"),
    enddate_c = as.Date(
      case_when(
        (!is.na(enddate_h) & is.na(enddate_m)) | overlap_type == 1 ~ enddate_h,
        !is.na(enddate_m) & is.na(enddate_h) ~ enddate_m), origin = "1970-01-01"),
    # Now look at overlapping rows and rows completely contained within the other data's dates
    startdate_c = as.Date(
      case_when(
        overlap_type %in% c(2, 4) & rownum_temp == 1 ~ startdate_h,
        overlap_type %in% c(3, 5) & rownum_temp == 1 ~ startdate_m,
        overlap_type %in% c(2:5) & rownum_temp == 2 ~ startdate_o,
        overlap_type %in% c(2:5) & rownum_temp == 3 ~ enddate_o + 1,
        TRUE ~ startdate_c), origin = "1970-01-01"),
    enddate_c = as.Date(
      case_when(
        overlap_type %in% c(2:5) & rownum_temp == 1 ~ lead(startdate_o, 1) - 1,
        overlap_type %in% c(2:5) & rownum_temp == 2 ~ enddate_o,
        overlap_type %in% c(2, 5) & rownum_temp == 3 ~ enddate_m,
        overlap_type %in% c(3, 4) & rownum_temp == 3 ~ enddate_h,
        TRUE ~ enddate_c), origin = "1970-01-01"),
    # Deal with the last line for each person if it's part of an overlap
    startdate_c = as.Date(ifelse((pid2 != lead(pid2, 1) | is.na(lead(pid2, 1))) &
                                  overlap_type %in% c(2:5) & 
                                   enddate_h != enddate_m, 
                                 lag(enddate_o, 1) + 1, 
                                 startdate_c), origin = "1970-01-01"),
    enddate_c = as.Date(ifelse((pid2 != lead(pid2, 1) | is.na(lead(pid2, 1))) &
                                overlap_type %in% c(2:5), 
                               pmax(enddate_h, enddate_m, na.rm = TRUE), 
                               enddate_c), origin = "1970-01-01")
    ) %>%
  arrange(pid2, startdate_c, enddate_c, startdate_h, startdate_m, 
          enddate_h, enddate_m, overlap_type) %>%
  mutate(
    # Identify which type of enrollment this row represents
    enroll_type = 
      case_when(
        (overlap_type == 2 & rownum_temp == 1) | 
          (overlap_type == 3 & rownum_temp == 3) |
          (overlap_type == 6 & rownum_temp == 1) | 
          (overlap_type == 7 & rownum_temp == 2) |
          (overlap_type == 4 & rownum_temp %in% c(1, 3)) |
          (overlap_type == 0 & is.na(startdate_m)) ~ "h",
        (overlap_type == 3 & rownum_temp == 1) | 
          (overlap_type == 2 & rownum_temp == 3) |
          (overlap_type == 6 & rownum_temp == 2) | 
          (overlap_type == 7 & rownum_temp == 1) | 
          (overlap_type == 5 & rownum_temp %in% c(1, 3)) |
          (overlap_type == 0 & is.na(startdate_h)) ~ "m",
        overlap_type == 1 | (overlap_type %in% c(2:5) & rownum_temp == 2) ~ "b",
        TRUE ~ "x"
      ),
    # Drop rows from enroll_type == h/m when they are fully covered by an enroll_type == b
    drop = 
      case_when(
        pid2 == lag(pid2, 1) & !is.na(lag(pid2, 1)) & 
          startdate_c == lag(startdate_c, 1) & !is.na(lag(startdate_c, 1)) &
          enddate_c >= lag(enddate_c, 1) & !is.na(lag(enddate_c, 1)) & 
          # Fix up quirk from housing data where two rows present for the same day
          !(lag(enroll_type, 1) != "m" & lag(enddate_h, 1) == lag(startdate_h, 1)) &
          enroll_type != "b" ~ 1,
        pid2 == lead(pid2, 1) & !is.na(lead(pid2, 1)) & 
          startdate_c == lead(startdate_c, 1) & !is.na(lead(startdate_c, 1)) &
          enddate_c <= lead(enddate_c, 1) & !is.na(lead(enddate_c, 1)) & 
          # Fix up quirk from housing data where two rows present for the same day
          !(lead(enroll_type, 1) != "m" & lead(enddate_h, 1) == lead(startdate_h, 1)) &
          enroll_type != "b" & lead(enroll_type, 1) == "b" ~ 1,
        # Fix up other oddities when the date range is only one day
        pid2 == lead(pid2, 1) & !is.na(lead(pid2, 1)) & 
          startdate_c == lag(startdate_c, 1) & !is.na(lag(startdate_c, 1)) &
          enddate_c <= lag(enddate_c, 1) & !is.na(lag(enddate_c, 1)) & 
          enroll_type == "m" & lag(enroll_type, 1) %in% c("b", "h") ~ 1,
        pid2 == lag(pid2, 1) & !is.na(lag(pid2, 1)) & 
          startdate_c == lag(startdate_c, 1) & !is.na(lag(startdate_c, 1)) &
          enddate_c >= lag(enddate_c, 1) & !is.na(lag(enddate_c, 1)) & 
          startdate_h == lag(startdate_h, 1) & enddate_h == lag(enddate_h, 1) &
          !is.na(startdate_h) & !is.na(lag(startdate_h, 1)) ~ 1,
        pid2 == lead(pid2, 1) & !is.na(lead(pid2, 1)) & 
          startdate_c == lead(startdate_c, 1) & !is.na(lead(startdate_c, 1)) &
          enddate_c >= lead(enddate_c, 1) & !is.na(lead(enddate_c, 1)) & 
          enroll_type == "m" & lead(enroll_type, 1) %in% c("b", "h") ~ 1,
        # Drop rows where the enddate_c < startdate_c due to 
        # both data sources' dates ending at the same time
        enddate_c < startdate_c ~ 1,
        TRUE ~ 0
      )
  ) %>%
  filter(drop == 0 | is.na(drop)) %>%
  # Truncate remaining overlapping end dates
  mutate(enddate_c = as.Date(
    ifelse(pid2 == lead(pid2, 1) &
             startdate_c < lead(startdate_c, 1) &
             enddate_c >= lead(enddate_c, 1),
           lead(startdate_c, 1) - 1,
           enddate_c),
    origin = "1970-01-01")
    ) %>%
  select(-drop, -repnum, -rownum_temp) %>%
  # With rows truncated, now additional rows with enroll_type == h/m that 
  # are fully covered by an enroll_type == b
  # Also catches single day rows that now have enddate < startdate
  mutate(
    drop = ifelse(
      pid2 == lag(pid2, 1) &
        startdate_c == lag(startdate_c, 1) &
        enddate_c == lag(enddate_c, 1) &
        enroll_type != "b",
      1, 0),
    drop = ifelse(
      pid2 == lead(pid2, 1) &
        startdate_c == lead(startdate_c, 1) &
        enddate_c <= lead(enddate_c, 1) &
        enroll_type != "b" &
        lead(enroll_type, 1) == "b",
      1, drop)
  ) %>%
  filter(drop == 0 | is.na(drop)) %>%
  select(-drop)

# Remove first temp file, keep the other for later code
rm(temp)
gc()

#### END RESHAPING ####

# For rows that are Medicaid-only, housing demographics are meaningless and vice versa
  # Merge back with original housing data and set Medicaid-only rows to missing
  # NB. Can't use start_date_m because then the housing data won't transition to 
    # rows that are housing only (the same problem applies to the Medicaid data), 
    # so use stripped down datasets in merge

# Choose variables to keep in the final data

# Jointlist = variables to keep across all rows
jointlist <- c("pid2", "mid", "ssn_m", "ssn_h", "lname_h", "fname_h", "mname_h",
               "dob_h", "dob_m", "race_c", "hisp_c", "gender_c")
houselist <- c(
  # ID, name, and demog variables
  "pid", "lnamesuf_h", "agegrp_h", "age12_h", "age13_h", "age14_h", 
  "age15_h", "age16_h", "age17_h", "adult_h", "senior_h", 
  "gender_h", "gender2_h", "citizen_h", "disability_h", "disability2_h", 
  "race_h", "hisp_h",
  # Household info
  "mbr_num", "hh_id_new_h", "hh_ssn_h", "hh_lname_h", "hh_lnamesuf_h", 
  "hh_fname_h", "hh_mname_h", "hh_dob_h",
  # MTW background info
  "list_date", "list_zip", "list_homeless", "housing_act",
  # Program info
  "agency_new",  "major_prog", "prog_type", "subsidy_type", "operator_type",
  "vouch_type_final", "agency_prog_concat",
  # Address info
  "unit_add_h", "unit_apt_h", "unit_apt2_h", "unit_city_h", "unit_state_h", 
  "unit_zip_h", "unit_concat_h", "kc_area",
  # Property/portfolio info
  "property_id", "property_name", "property_type", "portfolio", 
  "portfolio_final",
  # Unit info
  "unit_id", "unit_type", "unit_year", "access_unit", "access_req", 
  "access_rec", "bed_cnt", "move_in_date",
  # Dates
  "startdate_h", "enddate_h", "period", "start_housing", 
  "start_pha", "start_prog", "time_housing", "time_pha", "time_prog",
  # Port info
  "port_in", "port_out_kcha", "port_out_sha", "cost_pha",
  # Personal asset/income info
  "asset_val", "asset_inc", "inc_fixed", "inc_vary", "inc", "inc_excl", "inc_adj",
  # Household asset/income info
  "hh_asset_val", "hh_asset_inc", "hh_asset_impute", "hh_asset_inc_final", 
  "hh_inc_fixed", "hh_inc_vary", "hh_inc", "hh_inc_adj", "hh_inc_tot",
  "hh_inc_deduct", "hh_inc_tot_adj",
  # Rent info
    # Dropping for now as not used
  # Linking and ID variables
  "incasset_id", "subsidy_id", "vouch_num", "cert_id", "increment", 
  "sha_source", "kcha_source", "eop_source")

# Additional selection depending on whether or not geocoding was run
if ("x_h" %in% names(pha_elig_merge)) {
  houselist <- c(houselist, "formatted_address_h", "x_h", "y_h")
}

medlist <- c(
  # Name and demog variables
  "lname_m", "fname_m", "mname_m", "gender_m", "race_m", "hisp_m",
  # Coverage type
  "dual_elig_m", "cov_time_m",
  # Dates
  "startdate_m", "enddate_m")


# Just the PHA demographics and merging variables
pha_elig_merge_part1 <- pha_elig_merge %>%
  select(jointlist, houselist) %>%
  distinct()
temp_ext_h <- temp_ext %>% filter(enroll_type == "h")
merge1 <- left_join(temp_ext_h, pha_elig_merge_part1, 
                    by = c("pid2", "startdate_h", "enddate_h"))


# Just the Mediciad demographics and merging variables
pha_elig_merge_part2 <- pha_elig_merge %>%
  select(jointlist, medlist) %>%
  distinct()
temp_ext_m <- temp_ext %>% filter(enroll_type == "m")
merge2 <- left_join(temp_ext_m, pha_elig_merge_part2, by = c("pid2", "startdate_m", "enddate_m"))


# All variables
pha_elig_merge_part3 <- pha_elig_merge %>%
  select(jointlist, houselist, medlist) %>%
  distinct()
temp_ext_b <- temp_ext %>% filter(enroll_type == "b")
merge3 <- left_join(temp_ext_b, pha_elig_merge_part3, by = c("pid2", "startdate_h", "enddate_h", "startdate_m", "enddate_m"))


# Join into a single data frame
pha_elig_final <- bind_rows(merge1, merge2, merge3) %>%
  arrange(pid2, startdate_c, enddate_c)


### NB. This produces leads to 48 more rows than in the original temp_ext file
  # Seems to mostly be because of duplicate startdates and enddates in the 
  # pha_elig_merge_part2 files. Need to further investigate this issue

# Remove some temp data frames
rm(list = ls(pattern = "pha_elig_merge_part"))
rm(list = ls(pattern = "merge[0-9]"))
rm(list = ls(pattern = "list$"))
gc()


#### SET UP VARIABLES FOR ANALYSES ####
### Set up person-time each year
# First set up intervals for each year
i2012 <- interval(start = "2012-01-01", end = "2012-12-31")
i2013 <- interval(start = "2013-01-01", end = "2013-12-31")
i2014 <- interval(start = "2014-01-01", end = "2014-12-31")
i2015 <- interval(start = "2015-01-01", end = "2015-12-31")
i2016 <- interval(start = "2016-01-01", end = "2016-12-31")
i2017 <- interval(start = "2017-01-01", end = "2017-12-31")

# Person-time in housing, needs to be done separately to avoid errors
pt_temp_h <- pha_elig_final %>%
  distinct(pid2, startdate_h, enddate_h) %>%
  filter(!is.na(startdate_h)) %>%
  mutate(
    pt12_h = (lubridate::intersect(interval(start = startdate_h, end = enddate_h), i2012) / ddays(1)) + 1,
    pt13_h = (lubridate::intersect(interval(start = startdate_h, end = enddate_h), i2013) / ddays(1)) + 1,
    pt14_h = (lubridate::intersect(interval(start = startdate_h, end = enddate_h), i2014) / ddays(1)) + 1,
    pt15_h = (lubridate::intersect(interval(start = startdate_h, end = enddate_h), i2015) / ddays(1)) + 1,
    pt16_h = (lubridate::intersect(interval(start = startdate_h, end = enddate_h), i2016) / ddays(1)) + 1,
    pt17_h = (lubridate::intersect(interval(start = startdate_h, end = enddate_h), i2017) / ddays(1)) + 1
  )

pha_elig_final <- left_join(pha_elig_final, pt_temp_h, by = c("pid2", "startdate_h", "enddate_h"))
rm(pt_temp_h)

# Person-time in Medicaid, needs to be done separately to avoid errors
pt_temp_m <- pha_elig_final %>%
  filter(!is.na(startdate_m)) %>%
  distinct(pid2, startdate_m, enddate_m) %>%
  mutate(
    pt12_m = (lubridate::intersect(interval(start = startdate_m, end = enddate_m), i2012) / ddays(1)) + 1,
    pt13_m = (lubridate::intersect(interval(start = startdate_m, end = enddate_m), i2013) / ddays(1)) + 1,
    pt14_m = (lubridate::intersect(interval(start = startdate_m, end = enddate_m), i2014) / ddays(1)) + 1,
    pt15_m = (lubridate::intersect(interval(start = startdate_m, end = enddate_m), i2015) / ddays(1)) + 1,
    pt16_m = (lubridate::intersect(interval(start = startdate_m, end = enddate_m), i2016) / ddays(1)) + 1,
    pt17_m = (lubridate::intersect(interval(start = startdate_m, end = enddate_m), i2017) / ddays(1)) + 1
  )

pha_elig_final <- left_join(pha_elig_final, pt_temp_m, by = c("pid2", "startdate_m", "enddate_m"))
rm(pt_temp_m)

# Person-time in both, needs to be done separately to avoid errors
pt_temp_o <- pha_elig_final %>%
  filter(!is.na(startdate_o)) %>%
  distinct(pid2, startdate_o, enddate_o) %>%
  mutate(
    pt12_o = (lubridate::intersect(interval(start = startdate_o, end = enddate_o), i2012) / ddays(1)) + 1,
    pt13_o = (lubridate::intersect(interval(start = startdate_o, end = enddate_o), i2013) / ddays(1)) + 1,
    pt14_o = (lubridate::intersect(interval(start = startdate_o, end = enddate_o), i2014) / ddays(1)) + 1,
    pt15_o = (lubridate::intersect(interval(start = startdate_o, end = enddate_o), i2015) / ddays(1)) + 1,
    pt16_o = (lubridate::intersect(interval(start = startdate_o, end = enddate_o), i2016) / ddays(1)) + 1,
    pt17_o = (lubridate::intersect(interval(start = startdate_o, end = enddate_o), i2017) / ddays(1)) + 1
  )

pha_elig_final <- left_join(pha_elig_final, pt_temp_o, by = c("pid2", "startdate_o", "enddate_o"))
rm(pt_temp_o)

# Person-time specific to that interval, doesn't need to be done separately
pha_elig_final <- pha_elig_final %>%
  mutate(
    pt12 = (lubridate::intersect(interval(start = startdate_c, end = enddate_c), i2012) / ddays(1)) + 1,
    pt13 = (lubridate::intersect(interval(start = startdate_c, end = enddate_c), i2013) / ddays(1)) + 1,
    pt14 = (lubridate::intersect(interval(start = startdate_c, end = enddate_c), i2014) / ddays(1)) + 1,
    pt15 = (lubridate::intersect(interval(start = startdate_c, end = enddate_c), i2015) / ddays(1)) + 1,
    pt16 = (lubridate::intersect(interval(start = startdate_c, end = enddate_c), i2016) / ddays(1)) + 1,
    pt17 = (lubridate::intersect(interval(start = startdate_c, end = enddate_c), i2017) / ddays(1)) + 1
  )


### Fix up any NAs in the date fields
# Otherwise SQL load fails
pha_elig_final <- pha_elig_final %>%
  mutate_at(vars(startdate_h, enddate_h, startdate_m, enddate_m, startdate_o,
                 enddate_o, startdate_c, enddate_c, dob_h, dob_m, start_housing,
                 start_pha, start_prog, hh_dob_h),
            funs(if_else(. == "NA", NA, .)))



#### Save point ####
saveRDS(pha_elig_final, file = paste0(housing_path, "/OrganizedData/pha_elig_final.Rda"))


#### Write to SQL for joining with claims ####
dbRemoveTable(db.apde51, name = "housing_mcaid")
system.time(dbWriteTable(db.apde51, name = "housing_mcaid", 
             value = as.data.frame(pha_elig_final), overwrite = T,
             field.types = c(
               startdate_h = "date", enddate_h = "date", 
               startdate_m = "date", enddate_m = "date", 
               startdate_o = "date", enddate_o = "date", 
               startdate_c = "date", enddate_c = "date",
               dob_h = "date", dob_m = "date", hh_dob_h = "date",
               move_in_date = 'date', start_housing = "date", 
               start_pha = "date", start_prog = "date"))
)


#### Final clean up ####
rm(list = ls(pattern = "temp_ext"))
rm(list = ls(pattern = "i20"))
rm(pha_elig_merge)
rm(pha_longitudinal)
rm(list = c("elig", "elig_join", "elig_demog", "elig_merge"))
gc()

