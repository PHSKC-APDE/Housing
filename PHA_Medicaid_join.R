###############################################################################
# Code to join Medicaid eligibility and claims data with 
# a combined public housing authority data set
#
# Alastair Matheson (PHSKC-APDE)
# alastair.matheson@kingcounty.gov
# 2016-06-14
###############################################################################


##### Set up global parameter and call in libraries #####
options(max.print = 350, tibble.print_max = 30, scipen = 999)

library(RODBC) # Used to connect to SQL server
library(openxlsx) # Used to import/export Excel files
library(car) # used to recode variables
library(stringr) # Used to manipulate string data
library(lubridate) # Used to manipulate dates
library(dplyr) # Used to manipulate data
library(data.table) # more data manipulation
library(dtplyr) # lets dplyr and data.table play nicely together
library(RecordLinkage) # used to make the linkage
library(phonics) # used to extract phonetic version of names


##### Connect to the servers #####
db.apde50 <- odbcConnect("PH_APDEStore50")
db.apde51 <- odbcConnect("PH_APDEStore51")
db.claims50 <- odbcConnect("PHClaims50")
db.claims51 <- odbcConnect("PHClaims51")


##### Bring in data #####
### Housing
pha_longitudinal <- readRDS(file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/pha_longitudinal.Rda")
# Fix date format
pha_longitudinal <- pha_longitudinal %>%
  mutate(dob_m6 = as.Date(dob_m6, origin = "1970-01-01"))

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
ptm01 <- proc.time() # Times how long this query takes (~240 secs) (could cut this down by selecting few columns)
elig_demog <-
  sqlQuery(db.claims51,
           "SELECT x.MEDICAID_RECIPIENT_ID, x.SOCIAL_SECURITY_NMBR, FIRST_NAME, LAST_NAME, MIDDLE_NAME,
              GENDER, RACE1, RACE2, RACE3, RACE4, HISPANIC_ORIGIN_NAME, BIRTH_DATE,
              RSDNTL_ADRS_LINE_1, RSDNTL_ADRS_LINE_2, RSDNTL_CITY_NAME, RSDNTL_COUNTY_NAME, RSDNTL_POSTAL_CODE,
              RSDNTL_STATE_CODE
            FROM PHClaims.dbo.NewEligibility AS x
            INNER JOIN (SELECT MEDICAID_RECIPIENT_ID, SOCIAL_SECURITY_NMBR, MAX(CAL_YEAR_MONTH) AS maxdate
              FROM PHClaims.dbo.NewEligibility
              GROUP BY MEDICAID_RECIPIENT_ID, SOCIAL_SECURITY_NMBR) AS y
            ON x.MEDICAID_RECIPIENT_ID = y.MEDICAID_RECIPIENT_ID AND
            x.SOCIAL_SECURITY_NMBR = y.SOCIAL_SECURITY_NMBR AND
            x.CAL_YEAR_MONTH = y.maxdate",
           stringsAsFactors = FALSE)
proc.time() - ptm01

# Get rid of duplicate rows (these arise because of people having multiple rows on the latest month due to multiple RACs)
elig_demog <- elig_demog %>% distinct()



##### Join data together #####
### First bring the Medicaid demographics together and fix formats
elig_join <- left_join(elig, elig_demog, by = c("MEDICAID_RECIPIENT_ID", "SOCIAL_SECURITY_NMBR")) %>%
  mutate(
    GENDER = as.numeric(car::recode(GENDER, "'Female' = 1; 'Male' = 2; 'Unknown' = NA; else = NA")),
    BIRTH_DATE = as.Date(str_sub(BIRTH_DATE, 1, 10), format("%Y-%m-%d"))
    )

# Remove rows with only a Medicaid ID and no other details
elig_join <- elig_join %>% filter(!(is.na(SOCIAL_SECURITY_NMBR) & is.na(LAST_NAME) & is.na(FIRST_NAME)))

# Limit to one row per person for merging with housing
elig_merge <- elig_join %>% distinct(MEDICAID_RECIPIENT_ID, SOCIAL_SECURITY_NMBR, .keep_all = TRUE)

# Rename matching variables to match housing data and restrict to these vars 
# (may expand to include race and address later)
elig_merge <- elig_merge %>%
  rename(mid = MEDICAID_RECIPIENT_ID, ssn_new = SOCIAL_SECURITY_NMBR, fname_new = FIRST_NAME, lname_new = LAST_NAME,
         mname_new = MIDDLE_NAME, gender_new = GENDER, dob = BIRTH_DATE) %>%
  select(mid, ssn_new, lname_new, fname_new, mname_new, dob, gender_new) %>%
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
  filter(Weight >= 0.85 & (abs(dob.1 - dob.2) <= 30 | (dob_y.1 == dob_y.2 & dob_m.1 == dob_d.2 & dob_d.1 == dob_m.2 & dob_m.1 != dob_d.1 ))) %>% 
  select(pair, ssn_new.1:dob.1, ssn_new.2:dob.2, mid.2, Weight)



##### End of matching #####


### Join matched pairs together and deduplicate
pairs_final <- bind_rows(pairs1_full, pairs2_full)
pairs_final <- pairs_final %>% distinct()

# Join back to Medicaid and PHA data (keep full data from all existing datasets)
pha_elig_merge <- pairs_final %>%
  # Get the Medicaid SSN back to numeric for joining with full Medicaid data
  mutate(ssn_new_m = as.numeric(ssn_new.2)) %>%
  full_join(., elig_join, by = c("mid.2" = "MEDICAID_RECIPIENT_ID", "ssn_new_m" = "SOCIAL_SECURITY_NMBR")) %>%
  full_join(., pha_longitudinal, by = c("ssn_new.1" = "ssn_id_m6", "lname_new.1" = "lname_new_m6",
                                        "fname_new.1" = "fname_new_m6", "dob.1" = "dob_m6")) %>%
  # Rename variables to make them more obvious (m for Medicaid, h for housing)
  rename(startdate_m = startdate.x, enddate_m = enddate.x,
         startdate_h = startdate.y, enddate_h = enddate.y) %>%
  # Set up coverage times to look for overlap
  mutate(
    # Makre sure dates are in the correct format
    startdate_h = as.Date(startdate_h, origin="1970-01-01", format = "%Y-%m-%d"),
    enddate_h = as.Date(enddate_h, origin="1970-01-01", format = "%Y-%m-%d"),
    startdate_m = as.Date(startdate_m, origin="1970-01-01", format = "%Y-%m-%d"),
    enddate_m = as.Date(enddate_m, origin="1970-01-01", format = "%Y-%m-%d"))
    # Make intervals and overlap (seems to yield odd results so do not use for now)
      #housing_time = interval(startdate_h, enddate_h),
      #medicaid_time = interval(startdate_m, enddate_m),
      #overlap = int_overlaps(housing_time, medicaid_time))


# Remove temporary data
rm(list = ls(pattern = "pairs"))
rm(list = ls(pattern = "pha_merge"))
rm(elig_join)
rm(elig_merge)
gc()


### Save point
#saveRDS(pha_elig_merge, file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/pha_elig_merge.Rda")
#pha_elig_merge <- readRDS(file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/pha_elig_merge.Rda")


##### Calculate overlapping periods #####
# Set up intervals in each data set
pha_elig_final <- pha_elig_merge %>%
  mutate(overlap_type = 
           # First ID the non-matches
           ifelse(is.na(startdate_h) | is.na(startdate_m), 0,
                  # Then figure out which date comes first
                  ifelse(startdate_m >= startdate_h & startdate_m < enddate_h, 1,
                               ifelse(startdate_h >= startdate_m & startdate_h < enddate_m, 2,
                                      NA)))) %>%
  filter(!is.na(overlap_type))  %>%
  mutate(startdate_o = as.Date(ifelse(overlap_type == 1, startdate_m, 
                                        ifelse(overlap_type == 2, startdate_h,
                                               NA)), origin = "1970-01-01"),
         enddate_o = as.Date(pmin(enddate_m, enddate_h), origin = "1970-01-01"),
         source = ifelse(overlap_type == 0 & !is.na(startdate_h), "h",
                         ifelse(overlap_type == 0 & !is.na(startdate_m), "m",
                                ifelse(overlap_type == 1 | overlap_type == 2, "b", "check")))
         ) %>%
  # Limit to variables used for analyses 
  # Using PHA demogs as default for now (apart from DOB, where Medicaid seems mildly more accurate, i.e. fewer 1/1 dates)
  select(mid.2, ssn_new_m, ssn_new.1, lname_new.1, fname_new.1, dob.1, dob.2, gender_new_m6, race2, disability, agency_new, 
         unit_add_new, unit_apt_new, unit_apt2_new, unit_city_new, unit_state_new, unit_zip_new,
         prog_type_new, proj_type_new, proj_name, property_id, 
         startdate_h, enddate_h, startdate_m, enddate_m, startdate_o, enddate_o, source) %>%
  # Rename make to make it easier to join with claims
  rename(MEDICAID_RECIPIENT_ID = mid.2, SOCIAL_SECURITY_NMBR = ssn_new_m, ssn_new_pha = ssn_new.1, dob_h = dob.1)


##### Write to SQL for joining with claims #####

### Save point
#saveRDS(pha_elig_final, file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/pha_elig_final.Rda")
#pha_elig_final <- readRDS(file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/pha_elig_final.Rda")

# NB. Do not currently have permissions to do this, save as csv instead
write.csv(pha_elig_final, "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/pha_medicaidelig_combined.csv")


# May need to delete table first
sqlDrop(db.apde50 , "dbo.pha_medicaid_combined")
sqlSave(db.apde50 , pha_elig_final, tablename = "dbo.pha_medicaid_combined",
        varTypes = c(
          startdate_h = "Date", enddate_h = "Date",
          startdate_m = "Date", enddate_m = "Date",
          startdate_o = "Date", enddate_o = "Date"
        ))



####### TESTING AREA ########
pha_elig_merge <- pairs_final %>%
  # Get the Medicaid SSN back to numeric for joining with full Medicaid data
  mutate(ssn_new_m = as.numeric(ssn_new.2)) %>%
  left_join(., elig_join, by = c("mid.2" = "MEDICAID_RECIPIENT_ID", "ssn_new_m" = "SOCIAL_SECURITY_NMBR")) %>%
  left_join(., pha_longitudinal, by = c("ssn_new.1" = "ssn_id_m6", "lname_new.1" = "lname_new_m6",
                                        "fname_new.1" = "fname_new_m6", "dob.1" = "dob_m6")) %>%
  # Rename variables to make them more obvious (m for Medicaid, h for housing)
  rename(startdate_m = startdate.x, enddate_m = enddate.x,
         startdate_h = startdate.y, enddate_h = enddate.y) %>%
  # Set up coverage times to look for overlap
  mutate(
    # Convert housing dates to POSIX
    startdate_h = as.POSIXct(startdate_h, tz = "America/Los_Angeles", origin="1970-01-01", format = "%Y-%m-%d"),
    enddate_h = as.POSIXct(enddate_h, tz = "America/Los_Angeles", origin="1970-01-01", format = "%Y-%m-%d"),
    startdate_m = as.POSIXct(startdate_m, tz = "America/Los_Angeles", origin="1970-01-01", format = "%Y-%m-%d"),
    enddate_m = as.POSIXct(enddate_m, tz = "America/Los_Angeles", origin="1970-01-01", format = "%Y-%m-%d"),
    # Make intervals and overlap (seems to yield odd results so do not use for now)
    housing_time = interval(startdate_h, enddate_h),
    medicaid_time = interval(startdate_m, enddate_m),
    overlap = int_overlaps(housing_time, medicaid_time))



temp <- pha_elig_merge %>%
  select(mid.2, ssn_new_m, dob.2, gender_new_m6, race2, disability, agency_new, prog_type_new, 
         startdate_h, enddate_h, startdate_m, enddate_m, housing_time, medicaid_time, overlap) %>%
  mutate(int_m_beg = int_start(medicaid_time),
         int_m_end = int_end(medicaid_time),
         beg_m_test = int_m_beg - startdate_h,
         end_m_test = int_m_end - enddate_h  
  )

summarise(temp, sum_start = sum(beg_m_test), sum_end = sum(end_m_test))


#######################



