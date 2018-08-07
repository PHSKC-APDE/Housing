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
# 08 - Add in final data elements and set up analyses
# 09 - Join with Medicaid eligibility data ### (THIS CODE) ###
# 10 - Set up joint housing/Medicaid analyses
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


### Basic Medicaid eligibility table with link to names (~ 60 secs)
system.time(
  mcaid <- dbGetQuery(db.claims51,
                     "SELECT DISTINCT b.mid AS id, c.LAST_NAME AS lname_m, 
                        c.FIRST_NAME AS fname_m, c.MIDDLE_NAME AS mname_m
                     FROM (
                       SELECT a.MEDICAID_RECIPIENT_ID AS mid, max(calmo) as maxdate
                       FROM (
                         SELECT MEDICAID_RECIPIENT_ID, 
                         cast(CLNDR_YEAR_MNTH as bigint) as calmo
                         from PHClaims.dbo.mcaid_elig_raw
                         ) AS a
                       GROUP BY a.MEDICAID_RECIPIENT_ID
                       ) AS b
                     
                     LEFT JOIN (
                       SELECT MEDICAID_RECIPIENT_ID, LAST_NAME, FIRST_NAME, 
                          MIDDLE_NAME, CLNDR_YEAR_MNTH
                       FROM PHClaims.dbo.mcaid_elig_raw
                       ) AS c
                     
                     ON b.mid = c.MEDICAID_RECIPIENT_ID AND 
                        b.maxdate = c.CLNDR_YEAR_MNTH
                     ORDER BY id, lname_m, fname_m, mname_m")
)


### Bring in data that combines new dates for change in elig status or address (~10 mins)
system.time(
  mcaid_dates <- dbGetQuery(db.claims51,
                           "SELECT 	j.id, MIN(j.from_date) AS from_date, MAX(j.to_date) AS to_date, j.dual, 
                           j.add1_new, j.city_new, j.state_new, j.zip_new, 
                           datediff(dd, min(j.from_date), max(j.to_date)) + 1 as cov_time_day
                           
                           FROM (
                             select i.id, i.dual, i.add1_new, i.city_new, i.state_new, i.zip_new, i.from_date, i.to_date, i.group_num2,
                             sum(case when i.group_num2 is null then 0 else 1 end) over
                             (partition by i.id, i.dual, i.add1_new, i.city_new, i.state_new, i.zip_new 
                               ORDER BY i.temp_row rows between unbounded preceding and current row) as group_num3
                             
                             from (
                               select h.id, h.dual, h.add1_new, h.city_new, h.state_new, h.zip_new, h.from_date, h.to_date,
                               case 
                               when h.from_date - lag(h.to_date) over (partition by h.id, h.dual, h.add1_new, h.city_new, h.state_new, h.zip_new 
                                                                       ORDER BY h.id, h.from_date) <= 1 then null
                               else row_number() OVER (partition by h.id, h.dual, h.add1_new, h.city_new, h.state_new, h.zip_new ORDER BY h.from_date)
                               end as group_num2,
                               row_number() over (partition by h.id, h.dual, h.add1_new, h.city_new, h.state_new, h.zip_new ORDER BY h.id, h.from_date, h.to_date) AS temp_row
                               
                               FROM (
                                 SELECT g.id, g.dual, g.add1_new, g.city_new, g.state_new, g.zip_new, g.from_date, g.to_date,
                                 min(g.from_date) OVER (partition by g.id, g.dual,g.add1_new, g.city_new, g.state_new, g.zip_new ORDER BY g.id, g.from_date, g.to_date desc
                                                        rows between unbounded preceding and current row) as 'min_from',
                                 
                                 max(g.to_date) OVER (partition by g.id, g.dual, g.add1_new, g.city_new, g.state_new, g.zip_new order by g.id, g.from_date, g.to_date desc
                                                      rows between unbounded preceding and current row) as 'max_to'
                                 
                                 FROM (
                                   select f.id, f.dual, f.add1_new, f.city_new, f.state_new, f.zip_new, f.group_num,
                                   case 
                                   when f.startdate >= f.fromdate then f.startdate
                                   when f.startdate < f.fromdate then f.fromdate
                                   else null
                                   end as from_date,
                                   case 
                                   when f.enddate <= f.todate then f.enddate
                                   when f.enddate > f.todate then f.todate
                                   else null
                                   end as to_date
                                   
                                   FROM (
                                     SELECT e.id, e.dual, e.add1_new, e.city_new, e.state_new, e.zip_new,
                                     min(calmonth) as startdate, dateadd(day, - 1, dateadd(month, 1, max(calmonth))) as enddate,
                                     e.group_num, e.fromdate, e.todate
                                     
                                     FROM (
                                       SELECT DISTINCT d.id, d.dual, d.add1_new, d.city_new, d.state_new, d.zip_new,
                                       d.calmonth, d.group_num, d.fromdate, d.todate
                                       
                                       FROM (
                                         SELECT DISTINCT c.id, c.dual, c.add1_new, c.city_new, c.state_new, c.zip_new,
                                         c.calmonth, c.fromdate, c.todate,
                                         datediff(month, 0, calmonth) - 
                                           row_number() over (partition by c.id, c.dual, c.add1_new,
                                                              c.city_new, c.state_new, c.zip_new ORDER BY calmonth) as group_num
                                         
                                         FROM (
                                           SELECT DISTINCT a.id, a.dual, CONVERT(DATETIME, CAST(a.CLNDR_YEAR_MNTH as varchar(200)) + '01', 112) AS calmonth,
                                           a.fromdate, a.todate, b.add1_new, b.city_new, b.state_new, IIF(b.zip_new IS NULL, a.zip, b.zip_new) AS zip_new
                                           
                                           from ( 
                                             SELECT MEDICAID_RECIPIENT_ID AS 'id', DUAL_ELIG AS 'dual', CLNDR_YEAR_MNTH,
                                             FROM_DATE AS 'fromdate', TO_DATE AS 'todate', 
                                             RSDNTL_ADRS_LINE_1 AS 'add1', RSDNTL_ADRS_LINE_2 AS 'add2',
                                             RSDNTL_CITY_NAME as 'city', 
                                             RSDNTL_STATE_CODE AS 'state', RSDNTL_POSTAL_CODE AS 'zip'
                                             FROM [PHClaims].[dbo].[mcaid_elig_raw]
                                           ) a
                                           LEFT JOIN (
                                             SELECT add1, add2, city, state, zip, add1_new, city_new, state_new, zip_new
                                             FROM [PHClaims].[dbo].[mcaid_elig_address_clean]
                                           ) b
                                           ON 
                                           (a.add1 = b.add1 OR (a.add1 IS NULL AND b.add1 IS NULL)) AND 
                                           (a.add2 = b.add2 OR (a.add2 IS NULL AND b.add2 IS NULL)) AND 
                                           (a.city = b.city OR (a.city IS NULL AND b.city IS NULL)) AND 
                                           (a.state = b.state OR (a.state IS NULL AND b.state IS NULL)) AND 
                                           (a.zip = b.zip OR (a.zip IS NULL AND b.zip IS NULL))
                                         ) c
                                       ) d
                                     ) e
                                     GROUP BY e.id, e.dual, e.add1_new, e.city_new, e.state_new, e.zip_new, e.group_num, e.fromdate, e.todate
                                   ) f
                                 ) g
                               ) h
                               where h.from_date >= h.min_from and h.to_date = h.max_to
                             ) i
                           ) j
                           group by j.id, j.dual, j.add1_new, j.city_new, j.state_new, j.zip_new, j.group_num3
                           order by j.id, from_date"           
  )
)



 ### Processed demographics (~6 secs)
system.time(mcaid_demog <- dbGetQuery(db.claims51,
                         "SELECT id, ssnnew, dobnew, gender_mx, 
                          race_mx, latino, maxlang
                         FROM PHClaims.dbo.mcaid_elig_demoever"))


#### Join data together ####
### First bring the Medicaid demographics and dates together and fix formats
mcaid_join <- left_join(mcaid_dates, mcaid, by = "id") %>%
  left_join(., mcaid_demog, by = "id") %>%
  mutate(
    gender_mx = as.numeric(car::recode(gender_mx, "'Female' = 1; 'Male' = 2; 'Multiple' = 3; else = NA")),
    dobnew = as.Date(str_sub(dobnew, 1, 10), format("%Y-%m-%d"))
    )

# Rename matching variables to match housing data and restrict to these vars 
# (may expand to include race and address later)
mcaid_merge <- mcaid_join %>%
  rename(mid = id, ssn_new = ssnnew, lname_new = lname_m, fname_new = fname_m, 
         mname_new = mname_m, gender_new = gender_mx, dob = dobnew) %>%
  # Get columns in the same order as pha_merge
  select(mid, ssn_new, lname_new, fname_new, mname_new, gender_new, dob) %>%
  # Reduce to one row per person
  distinct() %>%
  mutate(dob_y = year(dob), dob_m = month(dob), dob_d = day(dob),
         # Remove missing name for more accurate match weight
         mname_new = ifelse(is.na(mname_new), "", mname_new))
  

##### Match 1 #####
# Block on SSN, match other vars
match1 <- compare.linkage(pha_merge, mcaid_merge, blockfld = c("ssn_new"),
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

# Need to decide which is correct version at some point, use Medicaid data as default for now
pairs1_full <- pairs1 %>%
  filter(
    # Looks like 0.45 is a good cutoff when SSN and DOBs match exactly
    (Weight >= 0.45 & dob.1 == dob.2) |
      # Can use 0.49 when SSN and YOB match
      (Weight >= 0.49 & dob_y.1 == dob_y.2) |
      # When SSN, MOB, and DOB match but YOB is 1-2 years off
      (Weight <= 0.49 & dob_y.1 != dob_y.2 & 
         dob_m.1 == dob_m.2 & dob_d.1 == dob_d.2 &
         abs(dob.1 - dob.2) <= 731)
  ) %>%
  select(pair, ssn_new.1:dob_d.1, ssn_new.2:dob_d.2, mid.2, Weight)


##### Match 2 #####
# Block on soundex last name, match other vars
# Restrict to PHA-generate IDs to avoid memory issues
pha_merge_id <- pha_merge %>%
  filter(str_detect(ssn_new, "[:alpha:]+"))


match2 <- compare.linkage(pha_merge_id, mcaid_merge, blockfld = c("lname_new"),
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
pha_mcaid_merge <- pairs_final %>%
  # Get the Medicaid SSN back to numeric for joining with full Medicaid data
  mutate(ssn_new.2 = as.numeric(ssn_new.2)) %>%
  full_join(., mcaid_join, by = c("mid.2" = "id")) %>%
  full_join(., pha_longitudinal, by = c("ssn_new.1" = "ssn_id_m6", 
                                        "lname_new.1" = "lname_new_m6",
                                        "fname_new.1" = "fname_new_m6", 
                                        "dob.1" = "dob_m6"))


# Rename core variables
# (m for Medicaid, h for housing where there is ambiguity)
pha_mcaid_merge <- pha_mcaid_merge %>%
  rename(startdate_m = from_date, enddate_m = to_date,
         startdate_h = startdate, enddate_h = enddate,
         mid = mid.2, dual_elig_m = dual,
         ssn_h = ssn_new.1, ssn_m = ssn_new.2,
         lname_h = lname_new.1, lnamesuf_h = lnamesuf_new_m6, 
         fname_h = fname_new.1, mname_h = mname_new_m6,
         dob_h = dob.1, dob_m = dobnew, agegrp_h = agegrp, adult_h = adult,
         senior_h = senior, gender_h = gender_new_m6, 
         gender2_h = gender2, gender_m = gender_mx,
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
         unit_add_m = add1_new, unit_city_m = city_new,
         unit_state_m = state_new, unit_zip_m = zip_new,
         unit_concat_h = unit_concat, lon_h = lon, lat_h = lat, 
         add_geocoded_h = add_geocoded)

# Select useful columns
pha_mcaid_merge <- pha_mcaid_merge %>%
    select(
      # Name, SSN, and demog variables from housing data
      pid, ssn_h, lname_h, lnamesuf_h, fname_h, mname_h, dob_h, agegrp_h,
      adult_h, senior_h, gender_h, gender2_h, citizen_h, 
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
      add_geocoded_h, lon_h, lat_h,
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
      incasset_id, subsidy_id, vouch_num, cert_id, increment, sha_source,
      kcha_source, eop_source,
      # Name, SSN, and demog variables from Medicaid data
      mid, ssn_m, lname_m, fname_m, mname_m, dob_m, gender_m, race_m, hisp_m, 
      dual_elig_m,
      # Medicaid address info
      unit_add_m, unit_city_m, unit_state_m, unit_zip_m,
      # Date info
      startdate_h, enddate_h, period:time_prog, startdate_m, enddate_m)


pha_mcaid_merge <- pha_mcaid_merge %>%
  # Set up coverage times to look for overlap
  mutate(
    # Make sure dates are in the correct format
    startdate_h = as.Date(startdate_h, origin="1970-01-01", format = "%Y-%m-%d"),
    enddate_h = as.Date(enddate_h, origin="1970-01-01", format = "%Y-%m-%d"),
    startdate_m = as.Date(startdate_m, origin="1970-01-01", format = "%Y-%m-%d"),
    enddate_m = as.Date(enddate_m, origin="1970-01-01", format = "%Y-%m-%d")
  )



#### Set up new variables ####
### Make combined demographics
# Logic: assume Medicaid is correct when there are conflicts 
# (housing data likely to overestimate multiple race for example), 
# use housing data to fill in missing Medicaid info.
pha_mcaid_merge <- pha_mcaid_merge %>%
  mutate(
    ssn_c = case_when(!is.na(ssn_m) ~ as.character(ssn_m),
                      !is.na(ssn_h) ~ ssn_h),
    dob_c = as.Date(ifelse(is.na(dob_m), dob_h, dob_m), origin = "1970-01-01"),
    race_c = case_when(!is.na(race_m) ~ race_m,
                            !is.na(race_h) ~ race_h),
    hisp_c = case_when(!is.na(hisp_m) ~ hisp_m,
                       !is.na(hisp_h) ~ hisp_h),
    gender_c = case_when(!is.na(gender_m) ~ gender_m,
                       !is.na(gender_h) ~ gender_h)
    )


# Make new unique ID to anonymize data
pha_mcaid_merge$pid2 <- group_indices(pha_mcaid_merge, mid, ssn_c, lname_h, fname_h, dob_c)


#### Save point ####
saveRDS(pha_mcaid_merge, file = paste0(housing_path, "/OrganizedData/pha_mcaid_merge.Rda"))
#pha_mcaid_merge <- readRDS(file = paste0(housing_path, "/OrganizedData/pha_mcaid_merge.Rda"))

# Remove temporary data
rm(list = ls(pattern = "pairs"))
rm(list = ls(pattern = "pha_merge"))
rm(list = ls(pattern = "classify"))
rm(list = ls(pattern = "match"))
rm(mcaid)
rm(mcaid_dates)
rm(mcaid_demog)
rm(mcaid_join)
rm(mcaid_merge)
gc()


##### Calculate overlapping periods #####
### Set up intervals in each data set
# This is horribly slow and ugly code. Would like to make it more efficient.

temp <- pha_mcaid_merge %>%
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
        pid2 == lag(pid2, 1) & !is.na(lag(pid2, 1)) & 
          startdate_c == lag(startdate_c, 1) & !is.na(lag(startdate_c, 1)) &
          startdate_c == enddate_c & !is.na(startdate_c) & 
          enroll_type == "m" & lag(enroll_type, 1) %in% c("b", "h") ~ 1,
        pid2 == lag(pid2, 1) & !is.na(lag(pid2, 1)) & 
          startdate_c == lag(startdate_c, 1) & !is.na(lag(startdate_c, 1)) &
          startdate_c == enddate_c & !is.na(startdate_c) &
          startdate_h == lag(startdate_h, 1) & enddate_h == lag(enddate_h, 1) &
          !is.na(startdate_h) & !is.na(lag(startdate_h, 1)) &
          enroll_type != "b" ~ 1,
        pid2 == lead(pid2, 1) & !is.na(lead(pid2, 1)) & 
          startdate_c == lead(startdate_c, 1) & !is.na(lead(startdate_c, 1)) &
          startdate_c == enddate_c & !is.na(startdate_c) &
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
jointlist <- c("pid2", "mid", "ssn_c", "lname_h", "fname_h", "mname_h",
               "dob_c", "race_c", "hisp_c", "gender_c")

houselist <- c(
  # ID, name, and demog variables
  "pid", "ssn_h", "lnamesuf_h", "dob_h", "agegrp_h", "adult_h", "senior_h", 
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
  "unit_zip_h", "unit_concat_h", "kc_area", "lon_h", "lat_h", "add_geocoded_h",
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


medlist <- c(
  # Name and demog variables
  "ssn_m", "lname_m", "fname_m", "mname_m", "dob_m", 
  "gender_m", "race_m", "hisp_m",
  # Coverage type
  "dual_elig_m",
  # Address info
  "unit_add_m", "unit_city_m", "unit_state_m", "unit_zip_m",
  # Dates
  "startdate_m", "enddate_m")


# Just the PHA demographics and merging variables
pha_mcaid_merge_part1 <- pha_mcaid_merge %>%
  select(jointlist, houselist) %>%
  distinct()
temp_ext_h <- temp_ext %>% filter(enroll_type == "h")
merge1 <- left_join(temp_ext_h, pha_mcaid_merge_part1,
                    by = c("pid2", "startdate_h", "enddate_h"))


# Just the Mediciad demographics and merging variables
pha_mcaid_merge_part2 <- pha_mcaid_merge %>%
  select(jointlist, medlist) %>%
  distinct()
temp_ext_m <- temp_ext %>% filter(enroll_type == "m")
merge2 <- left_join(temp_ext_m, pha_mcaid_merge_part2, 
                    by = c("pid2", "startdate_m", "enddate_m"))
# Deal with duplicate dates
merge2 <- merge2 %>%
  arrange(pid2, startdate_c, enddate_c, startdate_h, enddate_h, 
          startdate_m, enddate_m, mid, ssn_c, lname_h, fname_h, mname_h, dob_c,
          race_c, hisp_c, gender_c, lname_m, fname_m, mname_m, unit_add_m,
          unit_zip_m) %>%
  group_by(pid2, startdate_c, enddate_c) %>%
  slice(1) %>%
  ungroup()


# All variables
pha_mcaid_merge_part3 <- pha_mcaid_merge %>%
  select(jointlist, houselist, medlist) %>%
  distinct()
temp_ext_b <- temp_ext %>% filter(enroll_type == "b")
merge3 <- left_join(temp_ext_b, pha_mcaid_merge_part3, 
                    by = c("pid2", "startdate_h", "enddate_h", 
                           "startdate_m", "enddate_m"))


# Remove joined frames after checking that rowcounts of <name>_part<n> 
# and merge<n> are the same (or almost the same)
rm(list = ls(pattern = "pha_mcaid_merge_"))
gc()

# Join into a single data frame
pha_mcaid_join <- bind_rows(merge1, merge2, merge3) %>%
  arrange(pid2, startdate_c, enddate_c)

# Make single ZIP var now that times are aligned
pha_mcaid_join <- pha_mcaid_join %>%
  mutate(zip_c = case_when(
    !is.na(unit_zip_h) ~ as.integer(unit_zip_h),
    !is.na(unit_zip_m) ~ as.integer(unit_zip_m)
    )
  )

### NB. This produces leads to 2 more rows than in the original temp_ext file
  # Seems to be because of duplicates in the pha_mcaid_merge_part1 file. 
  # Need to further investigate this issue

# Remove some temp data frames
rm(list = ls(pattern = "pha_mcaid_merge_"))
rm(list = ls(pattern = "merge[0-9]"))
rm(list = ls(pattern = "list$"))
rm(list = ls(pattern = "temp_ext"))
rm(pha_longitudinal)
rm(pha_mcaid_merge)
gc()


#### Save point ####
saveRDS(pha_mcaid_join, file = paste0(housing_path, 
                                      "/OrganizedData/pha_mcaid_join.Rda"))
