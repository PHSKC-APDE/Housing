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

library(RODBC) # Used to connect to SQL server
library(openxlsx) # Used to import/export Excel files
library(stringr) # Used to manipulate string data
library(lubridate) # Used to manipulate dates
library(dplyr) # Used to manipulate data
library(RecordLinkage) # used to make the linkage
library(phonics) # used to extract phonetic version of names


##### Connect to the servers #####
db.apde51 <- odbcConnect("PH_APDEStore51")
db.claims51 <- odbcConnect("PHClaims51")


##### Bring in data #####
### Housing
pha_longitudinal <- readRDS(file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/pha_longitudinal.Rda")


# Limit to one row per person and only variables used for merging (use most recent row of data)
# Filter if person's most recent enddate is <2012 since they can't match to Medicaid
pha_merge <- pha_longitudinal %>%
  distinct(ssn_id_m6, lname_new_m6, fname_new_m6, mname_new_m6, dob_m6, gender_new_m6, enddate) %>%
  arrange(ssn_id_m6, lname_new_m6, fname_new_m6, mname_new_m6, dob_m6, gender_new_m6, enddate) %>%
  group_by(ssn_id_m6, lname_new_m6, fname_new_m6, dob_m6) %>%
  slice(n()) %>%
  ungroup() %>%
  filter(year(enddate) >= 2012) %>%
  select(-(enddate)) %>%
  rename(ssn_new = ssn_id_m6, lname_new = lname_new_m6, fname_new = fname_new_m6, mname_new = mname_new_m6, 
         dob = dob_m6, gender_new = gender_new_m6) %>%
  mutate(dob_y = year(dob), dob_m = month(dob), dob_d = day(dob),
         # Make a variable to match the Medicaid ID and order vars the same 
         mid = "") %>%
  select(mid, ssn_new:dob_d)


### Basic Medicaid eligibility table
ptm01 <- proc.time() # Times how long this query takes (~53 secs)
elig <-
  sqlQuery(db.claims51,
           "SELECT * FROM dbo.elig_overall",
           stringsAsFactors = FALSE
  )
proc.time() - ptm01


### Additional demographics for eligibility table (take most recent row per Medicaid ID/SSN combo)
# Short version that only pulls what will be used (takes ~320 secs)
ptm01 <- proc.time()
elig_demog <-
  sqlQuery(db.claims51,
           "SELECT x.MEDICAID_RECIPIENT_ID, x.SOCIAL_SECURITY_NMBR, FIRST_NAME, LAST_NAME, MIDDLE_NAME,
              GENDER, BIRTH_DATE, DUAL_ELIG, COVERAGE_TYPE_IND
            FROM PHClaims.dbo.NewEligibility AS x
            INNER JOIN (SELECT MEDICAID_RECIPIENT_ID, SOCIAL_SECURITY_NMBR, MAX(CLNDR_YEAR_MNTH) AS maxdate
              FROM PHClaims.dbo.NewEligibility
              GROUP BY MEDICAID_RECIPIENT_ID, SOCIAL_SECURITY_NMBR) AS y
            ON x.MEDICAID_RECIPIENT_ID = y.MEDICAID_RECIPIENT_ID AND
            x.SOCIAL_SECURITY_NMBR = y.SOCIAL_SECURITY_NMBR AND
            x.CLNDR_YEAR_MNTH = y.maxdate",
           stringsAsFactors = FALSE)
proc.time() - ptm01

# More detailed pull with more vars for future comparisons (takes ~240 secs)
# ptm01 <- proc.time()
# elig_demog <-
#   sqlQuery(db.claims51,
#            "SELECT x.MEDICAID_RECIPIENT_ID, x.SOCIAL_SECURITY_NMBR, FIRST_NAME, LAST_NAME, MIDDLE_NAME,
#               GENDER, RACE1, RACE2, RACE3, RACE4, HISPANIC_ORIGIN_NAME, BIRTH_DATE, DUAL_ELIG,
#               RSDNTL_ADRS_LINE_1, RSDNTL_ADRS_LINE_2, RSDNTL_CITY_NAME, RSDNTL_COUNTY_NAME, RSDNTL_POSTAL_CODE,
#               RSDNTL_STATE_CODE, COVERAGE_TYPE_IND
#             FROM PHClaims.dbo.NewEligibility AS x
#             INNER JOIN (SELECT MEDICAID_RECIPIENT_ID, SOCIAL_SECURITY_NMBR, MAX(CLNDR_YEAR_MNTH) AS maxdate
#               FROM PHClaims.dbo.NewEligibility
#               GROUP BY MEDICAID_RECIPIENT_ID, SOCIAL_SECURITY_NMBR) AS y
#             ON x.MEDICAID_RECIPIENT_ID = y.MEDICAID_RECIPIENT_ID AND
#             x.SOCIAL_SECURITY_NMBR = y.SOCIAL_SECURITY_NMBR AND
#             x.CLNDR_YEAR_MNTH = y.maxdate",
#            stringsAsFactors = FALSE)
# proc.time() - ptm01

# Get rid of duplicate rows (these arise because of people having multiple rows on the latest month due to multiple RACs)
elig_demog <- elig_demog %>% distinct()



##### Join data together #####
### First bring the Medicaid demographics together and fix formats
elig_join <- left_join(elig, elig_demog, by = c("MEDICAID_RECIPIENT_ID", "SOCIAL_SECURITY_NMBR")) %>%
  mutate(
    GENDER = as.numeric(car::recode(GENDER, "'Female' = 1; 'Male' = 2; 'Unknown' = NA; else = NA")),
    BIRTH_DATE = as.Date(str_sub(BIRTH_DATE, 1, 10), format("%Y-%m-%d"))
    )

#### Save point ####
#saveRDS(elig_join, file = "//dchs-shares01/DCHSDATA/DCHSPHClaimsData/Analyses/Alastair/elig_housing.Rda")
#elig_join <- readRDS(file = "//dchs-shares01/DCHSDATA/DCHSPHClaimsData/Analyses/Alastair/elig_housing.Rda")


# Limit to one row per person for merging with housing
elig_merge <- elig_join %>% distinct(MEDICAID_RECIPIENT_ID, SOCIAL_SECURITY_NMBR, .keep_all = TRUE)

# Rename matching variables to match housing data and restrict to these vars 
# (may expand to include race and address later)
elig_merge <- elig_merge %>%
  rename(mid = MEDICAID_RECIPIENT_ID, ssn_new = SOCIAL_SECURITY_NMBR, fname_new = FIRST_NAME, lname_new = LAST_NAME,
         mname_new = MIDDLE_NAME, gender_new = GENDER, dob = BIRTH_DATE) %>%
  select(mid, ssn_new, lname_new, fname_new, mname_new, gender_new, dob) %>%
  mutate(dob_y = year(dob), dob_m = month(dob), dob_d = day(dob),
         # Make ssn a character to match PHA data
         ssn_new = as.character(ssn_new),
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
classify1 <- epiClassify(match1_tmp, threshold.upper = 0.45)
summary(classify1)
pairs1 <- getPairs(classify1, single.rows = TRUE)

# Make record of pairs
pairs1 <- mutate(pairs1, pair = row_number())

# Looks like 0.45 is a good cutoff as long as DOBs aren't too far apart
# Need to decide which is correct version at some point, use Medicaid data as default for now
pairs1_full <- pairs1 %>%
  select(pair, ssn_new.1:dob.1, ssn_new.2:dob.2, mid.2, Weight) %>%
  filter(Weight >= 0.45 & abs(dob.1-dob.2) <= 730)


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
  filter(Weight >= 0.85 & 
           (abs(dob.1 - dob.2) <= 30 | 
              (dob_y.1 == dob_y.2 & dob_m.1 == dob_d.2 & dob_d.1 == dob_m.2 & dob_m.1 != dob_d.1 ))) %>% 
  select(pair, ssn_new.1:dob.1, ssn_new.2:dob.2, mid.2, Weight)



##### End of matching #####


#### Join matched pairs together and deduplicate ####
pairs_final <- bind_rows(pairs1_full, pairs2_full)
pairs_final <- pairs_final %>% distinct()

# Join back to Medicaid and PHA data (keep full data from all existing datasets)
pha_elig_merge <- pairs_final %>%
  # Get the Medicaid SSN back to numeric for joining with full Medicaid data
  mutate(ssn_new.2 = as.numeric(ssn_new.2)) %>%
  full_join(., elig_join, by = c("mid.2" = "MEDICAID_RECIPIENT_ID", "ssn_new.2" = "SOCIAL_SECURITY_NMBR")) %>%
  full_join(., pha_longitudinal, by = c("ssn_new.1" = "ssn_id_m6", "lname_new.1" = "lname_new_m6",
                                        "fname_new.1" = "fname_new_m6", "dob.1" = "dob_m6")) %>%
  # Rename variables to make them more obvious (m for Medicaid, h for housing)
  rename(startdate_m = startdate.x, enddate_m = enddate.x,
         startdate_h = startdate.y, enddate_h = enddate.y,
         mid = mid.2, dual_elig_m = DUAL_ELIG, cov_type_m = COVERAGE_TYPE_IND,
         ssn_h = ssn_new.1, ssn_m = ssn_new.2,
         lname_h = lname_new.1, lnamesuf_h = lnamesuf_new_m6, 
         fname_h = fname_new.1, mname_h = mname_new_m6,
         lname_m = LAST_NAME, fname_m = FIRST_NAME, mname_m = MIDDLE_NAME,
         dob_h = dob.1, dob_m = BIRTH_DATE, agegrp_h = agegrp, adult_h = adult,
         senior_h = senior, age12_h = age12, age13_h = age13, age14_h = age14, 
         age15_h = age15, age16_h = age16,
         gender_h = gender_new_m6, gender2_h = gender2, gender_m = GENDER,
         race_h = race2, disability_h = disability, disability2_h = disability2,
         citizen_h = citizen,
         hh_ssn_h = hh_ssn_id_m6, hh_lname_h = hh_lname_m6, 
         hh_lnamesuf_h = hh_lnamesuf_m6, hh_fname_h = hh_fname_m6,
         hh_mname_h = hh_mname_m6, hh_dob_h = hh_dob_m6,
         unit_add_h = unit_add_new, unit_apt_h = unit_apt_new,
         unit_apt2_h = unit_apt2_new, unit_city_h = unit_city_new,
         unit_state_h = unit_state_new, unit_zip_h = unit_zip_new,
         unit_concat_h = unit_concat, x_h = X, y_h = Y,
         formatted_address_h = formatted_address
         ) %>%
  # Restrict to columns that will be used in analyses
  select(pid, ssn_h, lname_h, lnamesuf_h, fname_h, mname_h, dob_h, agegrp_h,
         age12_h:age16_h,
         adult_h, senior_h, gender_h, gender2_h, citizen_h, disability_h, disability2_h,
         race_h, mbr_num, hhold_id_new, hh_ssn_h, hh_lname_h, hh_lnamesuf_h, hh_fname_h, 
         hh_mname_h, hh_dob_h, agency_new:property_id, agency_prog_concat, 
         portfolio_final, prog_final,
         unit_id, unit_add_h:unit_zip_h, unit_concat_h, formatted_address_h:y_h, 
         kc_area, property_name_new,
         mid, ssn_m, lname_m, fname_m, mname_m, dob_m, gender_m, dual_elig_m, cov_type_m,
         startdate_h, enddate_h, truncated, period:time_prog, port_in:port_out_sha,
         startdate_m, enddate_m) %>%
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


# Make new unique ID to anonymize data
pha_elig_merge$pid2 <- group_indices(pha_elig_merge, mid, ssn_m, ssn_h, lname_h, fname_h, dob_h)


### Save point
saveRDS(pha_elig_merge, file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/pha_elig_merge.Rda")
#pha_elig_merge <- readRDS(file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/pha_elig_merge.Rda")


##### Calculate overlapping periods #####
# Set up intervals in each data set


# This is horribly slow and ugly code. Would like to make it more efficient.

temp <- pha_elig_merge %>%
  mutate(
    overlap_type = 
      # First ID the non-matches
      ifelse(is.na(startdate_h) | is.na(startdate_m), 0,
             # Then figure out which overlapping date comes first
             ifelse(
               # Exactly the same dates
               startdate_h == startdate_m & enddate_h == enddate_m, 1,
               # Housing before Medicaid (or exactly the same dates)
               ifelse(startdate_h <= startdate_m & startdate_m <= enddate_h & enddate_h <= enddate_m, 2,
               # Medicaid before housing
               ifelse(startdate_m <= startdate_h & startdate_h <= enddate_m & enddate_m <= enddate_h, 3,
                      # Find when Medicaid dates are competely within housing dates or vice versa
                      ifelse(startdate_m >= startdate_h & enddate_m <= enddate_h, 4,
                             ifelse(startdate_h >= startdate_m & enddate_h <= enddate_m, 5,
                      # Find rows where person had housing coverage only before Medicaid (or Medicaid only after housing)
                      ifelse(startdate_h < startdate_m & enddate_h < startdate_m, 6,
                             # Housing coverage only after Medicaid (or Medicaid only before housing)
                             ifelse(startdate_h > enddate_m & enddate_h > enddate_m, 7,
                                    # Anyone rows that are left
                                    8)))))))),
    # Calculate overlapping dates
    startdate_o = as.Date(ifelse(overlap_type %in% c(1, 2, 4), startdate_m, 
                                 ifelse(overlap_type %in% c(3, 5), startdate_h,
                                        NA)), origin = "1970-01-01"),
    enddate_o = as.Date(ifelse(overlap_type %in% c(1:5), pmin(enddate_m, enddate_h), NA), origin = "1970-01-01"),
    # Need to duplicate rows to separate out non-overlapping housing and Medicaid periods
    repnum = ifelse(overlap_type %in% c(2:5), 3, ifelse(overlap_type %in% c(6:7), 2, 1))) %>%
  select(pid2, startdate_h, enddate_h, startdate_m, enddate_m, startdate_o, enddate_o, overlap_type, repnum) %>%
  arrange(pid2, startdate_h, startdate_m, startdate_o, enddate_h, enddate_m, enddate_o)


# Expand out rows to separate out overlaps
temp_ext <- temp[rep(seq(nrow(temp)), temp$repnum), 1:(ncol(temp) - 1)]


temp_ext <- temp_ext %>% 
  group_by(pid2, startdate_h, enddate_h, startdate_m, enddate_m) %>% 
  mutate(rownum_temp = row_number()) %>%
  ungroup() %>%
  arrange(pid2, startdate_h, enddate_h, startdate_m, enddate_m, startdate_o, enddate_o, overlap_type, rownum_temp) %>%
  mutate(
    # Remove non-overlapping dates
    startdate_h = as.Date(ifelse((overlap_type == 6 & rownum_temp == 2) | (overlap_type == 7 & rownum_temp == 1), 
                                 NA, startdate_h), origin = "1970-01-01"), 
    enddate_h = as.Date(ifelse((overlap_type == 6 & rownum_temp == 2) | (overlap_type == 7 & rownum_temp == 1), 
                               NA, enddate_h), origin = "1970-01-01"),
    startdate_m = as.Date(ifelse((overlap_type == 6 & rownum_temp == 1) | (overlap_type == 7 & rownum_temp == 2), 
                                 NA, startdate_m), origin = "1970-01-01"), 
    enddate_m = as.Date(ifelse((overlap_type == 6 & rownum_temp == 1) | (overlap_type == 7 & rownum_temp == 2), 
                               NA, enddate_m), origin = "1970-01-01")) %>%
  distinct(pid2, startdate_h, enddate_h, startdate_m, enddate_m, startdate_o, enddate_o, overlap_type, rownum_temp, .keep_all = TRUE) %>%
  # Remove first row if start dates are the same or housing is only one day
  filter(!(overlap_type %in% c(2:5) & rownum_temp == 1 & (startdate_h == startdate_m | startdate_h == enddate_h))) %>%
  # Remove third row if enddates are the same
  filter(!(overlap_type %in% c(2:5) & rownum_temp == 3 & enddate_h == enddate_m))

# Calculate finalized date columns
temp_ext <- temp_ext %>%
  # Set up combined dates
  mutate(
    # Start with rows with only housing or Medicaid, or when both sets of dates are identical
    startdate_c = as.Date(ifelse((!is.na(startdate_h) & is.na(startdate_m)) | overlap_type == 1, startdate_h,
                               ifelse(!is.na(startdate_m) & is.na(startdate_h), startdate_m,
                                      NA)), origin = "1970-01-01"),
    enddate_c = as.Date(ifelse((!is.na(enddate_h) & is.na(enddate_m)) | overlap_type == 1, enddate_h,
                             ifelse(!is.na(enddate_m) & is.na(enddate_h), enddate_m,
                                    NA)), origin = "1970-01-01"),
    # Now look at overlapping rows and rows completely contained within the other data's dates
    startdate_c = as.Date(ifelse(overlap_type %in% c(2, 4) & rownum_temp == 1, startdate_h, 
                                 ifelse(overlap_type %in% c(3, 5) & rownum_temp == 1, startdate_m,
                                        ifelse(overlap_type %in% c(2:5) & rownum_temp == 2, startdate_o,
                                               ifelse(overlap_type %in% c(2:5) & rownum_temp == 3, enddate_o + 1,
                                                      startdate_c)))), origin = "1970-01-01"),
    enddate_c = as.Date(ifelse(overlap_type %in% c(2:5) & rownum_temp == 1, lead(startdate_o, 1) - 1,
                               ifelse(overlap_type %in% c(2:5) & rownum_temp == 2, enddate_o,
                                      ifelse(overlap_type %in% c(2, 5) & rownum_temp == 3, enddate_m, 
                                             ifelse(overlap_type %in% c(3, 4) & rownum_temp == 3, enddate_h,
                                                    enddate_c)))), origin = "1970-01-01"),
    # Deal with the last line for each person if it's part of an overlap
    startdate_c = as.Date(ifelse((pid2 != lead(pid2, 1) | is.na(lead(pid2, 1))) &
                                  overlap_type %in% c(2:5) & enddate_h != enddate_m, lag(enddate_o, 1) + 1, startdate_c), origin = "1970-01-01"),
    enddate_c = as.Date(ifelse((pid2 != lead(pid2, 1) | is.na(lead(pid2, 1))) &
                                overlap_type %in% c(2:5), pmax(enddate_h, enddate_m, na.rm = TRUE), enddate_c), origin = "1970-01-01")
    ) %>%
  arrange(pid2, startdate_c, enddate_c, startdate_h, startdate_m, enddate_h, enddate_m, overlap_type) %>%
  mutate(
    # Identify which type of enrollment this row represents
    enroll_type = ifelse((overlap_type == 2 & rownum_temp == 1) | (overlap_type == 3 & rownum_temp == 3) |
                           (overlap_type == 6 & rownum_temp == 1) | (overlap_type == 7 & rownum_temp == 2) |
                           (overlap_type == 4 & rownum_temp %in% c(1, 3)) |
                           (overlap_type == 0 & is.na(startdate_m)), "h",
                         ifelse((overlap_type == 3 & rownum_temp == 1) | (overlap_type == 2 & rownum_temp == 3) |
                                  (overlap_type == 6 & rownum_temp == 2) | (overlap_type == 7 & rownum_temp == 1) | 
                                  (overlap_type == 5 & rownum_temp %in% c(1, 3)) |
                                  (overlap_type == 0 & is.na(startdate_h)), "m",
                                ifelse(overlap_type == 1 | (overlap_type %in% c(2:5) & rownum_temp == 2), "b", "x"))),
    # Drop rows from enroll_type == h/m when they are fully covered by an enroll_type == b
    drop = ifelse(pid2 == lag(pid2, 1) & !is.na(lag(pid2, 1)) & 
                    startdate_c == lag(startdate_c, 1) & !is.na(lag(startdate_c, 1)) &
                    enddate_c >= lag(enddate_c, 1) & !is.na(lag(enddate_c, 1)) & 
                    # Fix up quirk from housing data where two rows present for the same day
                    !(lag(enroll_type, 1) != "m" & lag(enddate_h, 1) == lag(startdate_h, 1)) &
                    enroll_type != "b", 1, 0),
    drop = ifelse(pid2 == lead(pid2, 1) & !is.na(lead(pid2, 1)) & 
                    startdate_c == lead(startdate_c, 1) & !is.na(lead(startdate_c, 1)) &
                    enddate_c <= lead(enddate_c, 1) & !is.na(lead(enddate_c, 1)) & 
                    # Fix up quirk from housing data where two rows present for the same day
                    !(lead(enroll_type, 1) != "m" & lead(enddate_h, 1) == lead(startdate_h, 1)) &
                    enroll_type != "b" & lead(enroll_type, 1) == "b", 1, drop),
    # Fix up other oddities when the date range is only one day
    drop = ifelse(pid2 == lead(pid2, 1) & !is.na(lead(pid2, 1)) & 
                    startdate_c == lag(startdate_c, 1) & !is.na(lag(startdate_c, 1)) &
                    enddate_c <= lag(enddate_c, 1) & !is.na(lag(enddate_c, 1)) & 
                    enroll_type == "m" & lag(enroll_type, 1) %in% c("b", "h"),
                  1, drop),
    drop = ifelse(pid2 == lag(pid2, 1) & !is.na(lag(pid2, 1)) & 
                    startdate_c == lag(startdate_c, 1) & !is.na(lag(startdate_c, 1)) &
                    enddate_c >= lag(enddate_c, 1) & !is.na(lag(enddate_c, 1)) & 
                    startdate_h == lag(startdate_h, 1) & enddate_h == lag(enddate_h, 1) &
                    !is.na(startdate_h) & !is.na(lag(startdate_h, 1)),
                  1, drop),
    drop = ifelse(pid2 == lead(pid2, 1) & !is.na(lead(pid2, 1)) & 
                    startdate_c == lead(startdate_c, 1) & !is.na(lead(startdate_c, 1)) &
                    enddate_c >= lead(enddate_c, 1) & !is.na(lead(enddate_c, 1)) & 
                    enroll_type == "m" & lead(enroll_type, 1) %in% c("b", "h"),
                  1, drop),
    # Drop rows where the enddate_c < startdate_c due to both data sources' dates ending at the same time
    drop = ifelse(enddate_c < startdate_c, 1, drop)
  ) %>%
  filter(!drop == 1) %>%
  # Truncate remaining overlapping end dates
  mutate(enddate_c = as.Date(ifelse(pid2 == lead(pid2, 1) & !is.na(lead(pid2, 1)) & 
                                    startdate_c < lead(startdate_c, 1) & !is.na(lead(startdate_c, 1)) &
                                    enddate_c >= lead(enddate_c, 1) & !is.na(lead(enddate_c, 1)),
                                  lead(startdate_c, 1) - 1, enddate_c), origin = "1970-01-01")) %>%
  select(-drop, -rownum_temp) %>%
  # With rows truncated, now additional rows with enroll_type == h/m that are fully covered by an enroll_type == b
  mutate(
    drop = ifelse(pid2 == lag(pid2, 1) & !is.na(lag(pid2, 1)) & 
                    startdate_c == lag(startdate_c, 1) & !is.na(lag(startdate_c, 1)) &
                    enddate_c == lag(enddate_c, 1) & !is.na(lag(enddate_c, 1)) & 
                    # Fix up quirk from housing data where two rows present for the same day
                    #!(lag(enroll_type, 1) != "m" & lag(enddate_h, 1) == lag(startdate_h, 1)) &
                    enroll_type != "b", 1, 0),
    drop = ifelse(pid2 == lead(pid2, 1) & !is.na(lead(pid2, 1)) & 
                    startdate_c == lead(startdate_c, 1) & !is.na(lead(startdate_c, 1)) &
                    enddate_c == lead(enddate_c, 1) & !is.na(lead(enddate_c, 1)) & 
                    # Fix up quirk from housing data where two rows present for the same day
                    #!(lead(enroll_type, 1) != "m" & lead(enddate_h, 1) == lead(startdate_h, 1)) &
                    enroll_type != "b" & lead(enroll_type, 1) == "b", 1, drop)
  ) %>%
  filter(!drop == 1) %>%
  select(-drop)


rm(temp)
gc()

#### END RESHAPING ####

# Note that for rows that are Medicaid-only, the housing demographics are meaningless and vice versa
# Merge back with original housing data and set Medicaid-only rows to missing
# NB. Can't use start_date_m because then the housing data won't transition to rows that are housing only
# (the same problem applies to the Medicaid data), so use stripped down datasets in merge

# Choose variables to keep in the final data
# Cutting out most of the geocoding and unit-specific variables as they are not currently being used
keeplist <- c("pid2", "mid", "ssn_m", "ssn_h", "lname_h", "fname_h", "dob_h", "dob_m")
houselist <- c("pid", "startdate_h", "enddate_h", "truncated", "period", "start_housing", 
               "start_pha", "start_prog", "time_housing", "time_pha", "time_prog",
               "mbr_num", "lnamesuf_h", "agegrp_h", "age12_h", "age13_h", "age14_h", 
               "age15_h", "age16_h", "adult_h", "senior_h", "gender_h", "gender2_h",
               "citizen_h", "disability_h", "disability2_h", "race_h", 
               "hhold_id_new", "hh_ssn_h", "hh_lname_h", "hh_lnamesuf_h", 
               "hh_fname_h", "hh_mname_h", "hh_dob_h",
               "agency_new", "major_prog", "prog_type", "prog_subtype", 
               "spec_purp_type", "agency_prog_concat", "prog_group", "prog_final",
               "portfolio_group", "portfolio_final", 
               "property_id", "property_name_new", "unit_id", "unit_add_h", 
               "unit_apt_h", "unit_apt2_h", "unit_city_h", "unit_state_h", "unit_zip_h", "unit_concat_h",
               "formatted_address_h", "x_h", "y_h", "kc_area",
               "port_in", "port_out_kcha", "port_out_sha")
medlist <- c("startdate_m", "enddate_m", "lname_m", "fname_m", "mname_m", 
             "gender_m", "dual_elig_m", "cov_type_m")


# Just the PHA demographics and merging variables
pha_elig_merge_part1 <- pha_elig_merge %>%
  select(keeplist, houselist) %>%
  distinct()
temp_ext_h <- temp_ext %>% filter(enroll_type == "h")
merge1 <- left_join(temp_ext_h, pha_elig_merge_part1, by = c("pid2", "startdate_h", "enddate_h"))


# Just the Mediciad demographics and merging variables
pha_elig_merge_part2 <- pha_elig_merge %>%
  select(keeplist, medlist) %>%
  distinct()
temp_ext_m <- temp_ext %>% filter(enroll_type == "m")
merge2 <- left_join(temp_ext_m, pha_elig_merge_part2, by = c("pid2", "startdate_m", "enddate_m"))


# All variables
pha_elig_merge_part3 <- pha_elig_merge %>%
  select(keeplist, houselist, medlist) %>%
  distinct()
temp_ext_b <- temp_ext %>% filter(enroll_type == "b")
merge3 <- left_join(temp_ext_b, pha_elig_merge_part3, by = c("pid2", "startdate_h", "enddate_h", "startdate_m", "enddate_m"))

pha_elig_final <- bind_rows(merge1, merge2, merge3) %>%
  arrange(pid2, startdate_c, enddate_c)


### NB. This produces leads to 7 more rows than in the original temp_ext file
# This seems to be because of duplicate startdates and enddates in the pha_elig_merge_part1 files
# Need to further investigate this issue

rm(list = ls(pattern = "pha_elig_merge_part"))
rm(list = ls(pattern = "merge[0-9]"))
rm(list = ls(pattern = "temp_ext"))
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

# Person-time in housing, needs to be done separately to avoid errors
pt_temp_h <- pha_elig_final %>%
  distinct(pid2, startdate_h, enddate_h) %>%
  filter(!is.na(startdate_h)) %>%
  mutate(
    pt12_h = (lubridate::intersect(interval(start = startdate_h, end = enddate_h), i2012) / ddays(1)) + 1,
    pt13_h = (lubridate::intersect(interval(start = startdate_h, end = enddate_h), i2013) / ddays(1)) + 1,
    pt14_h = (lubridate::intersect(interval(start = startdate_h, end = enddate_h), i2014) / ddays(1)) + 1,
    pt15_h = (lubridate::intersect(interval(start = startdate_h, end = enddate_h), i2015) / ddays(1)) + 1,
    pt16_h = (lubridate::intersect(interval(start = startdate_h, end = enddate_h), i2016) / ddays(1)) + 1
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
    pt16_m = (lubridate::intersect(interval(start = startdate_m, end = enddate_m), i2016) / ddays(1)) + 1
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
    pt16_o = (lubridate::intersect(interval(start = startdate_o, end = enddate_o), i2016) / ddays(1)) + 1
  )

pha_elig_final <- left_join(pha_elig_final, pt_temp_o, by = c("pid2", "startdate_o", "enddate_o"))
rm(pt_temp_o)


### Fix up any NAs in the date fields
# Otherwise SQL load fails
pha_elig_final <- pha_elig_final %>%
  mutate_at(vars(startdate_h, enddate_h, startdate_m, enddate_m, startdate_o,
                 enddate_o, startdate_c, enddate_c, dob_h, dob_m, start_housing,
                 start_pha, start_prog, hh_dob_h),
            funs(if_else(. == "NA", NA, .)))


#### Save point ####
saveRDS(pha_elig_final, file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/pha_elig_final.Rda")
rm(pha_elig_merge)
gc()

#### Write to SQL for joining with claims ####
ptm01 <- proc.time() # Times how long this query takes
sqlDrop(db.apde51, "dbo.housing_medicaid")
sqlSave(
  db.apde51,
  pha_elig_final,
  tablename = "dbo.housing_medicaid",
  varTypes = c(
    startdate_h = "Date", enddate_h = "Date", 
    startdate_m = "Date", enddate_m = "Date", 
    startdate_o = "Date", enddate_o = "Date", 
    startdate_c = "Date", enddate_c = "Date",
    dob_h = "Date", dob_m = "Date", 
    start_housing = "Date", start_pha = "Date", 
    start_prog = "Date", hh_dob_h = "Date"
    )
)
proc.time() - ptm01


