##
## Script name: jhu_waitlist_data_pull
##
## Purpose of script: Create analytic tables for Johns Hopkins researchers 
##        who are looking at the effect of housing on health
##
## Author: Alastair Matheson, Public Health - Seattle & King County
## Date Created: 2021-07-30
## Email: alastair.matheson@kingcounty.gov
##
## Notes:
##   
##

# SET OPTIONS AND BRING IN PACKAGES ----
options(scipen = 6, digits = 4, warning.length = 8170)

if (!require("pacman")) {install.packages("pacman")}
pacman::p_load(tidyverse, odbc, glue, data.table, lubridate, claims)

# Connect to HHSAW
db_hhsaw <- DBI::dbConnect(odbc::odbc(),
                           driver = "ODBC Driver 17 for SQL Server",
                           server = "tcp:kcitazrhpasqlprp16.azds.kingcounty.gov,1433",
                           database = "hhs_analytics_workspace",
                           uid = keyring::key_list("hhsaw")[["username"]],
                           pwd = keyring::key_get("hhsaw", keyring::key_list("hhsaw")[["username"]]),
                           Encrypt = "yes",
                           TrustServerCertificate = "yes",
                           Authentication = "ActiveDirectoryPassword")

# BRING IN DATA ----
## Waitlist data ----
# Also to Medicaid data to get id_mcaid
waitlist <- dbGetQuery(db_hhsaw,
                       "SELECT a.*, b.id_kc_pha, c.id_apde, e.id_mcaid, d.geo_tractce10 
                         FROM
                       (SELECT DISTINCT * FROM pha.stage_pha_waitlist) a
                       LEFT JOIN
                       (SELECT DISTINCT id_hash, id_kc_pha FROM pha.final_identities) b
                       ON a.id_hash = b.id_hash
                       LEFT JOIN 
                       (SELECT DISTINCT id_apde, id_kc_pha FROM claims.final_xwalk_apde_ids) c
                       ON b.id_kc_pha = c.id_kc_pha
                       LEFT JOIN
                       (SELECT DISTINCT geo_hash_geocode, geo_tractce10 FROM ref.address_geocode) d
                       ON a.geo_hash_geocode = d.geo_hash_geocode
                       LEFT JOIN
                       (SELECT DISTINCT id_apde, id_mcaid FROM claims.final_xwalk_apde_ids
                        WHERE id_mcaid IS NOT NULL) e
                        ON c.id_apde = e.id_apde")


### Spot check things ----
waitlist <- setDT(waitlist)

# See how many id_kc_pha values there are per id_apde (may indicate false positives)
waitlist[, id_pha_cnt := uniqueN(id_kc_pha), by = c("agency", "id_apde")]
waitlist %>% count(agency, id_pha_cnt)

#    agency id_pha_cnt     n
# 1:   KCHA          1 42166
# 2:   KCHA          2    50
# 3:    SHA          1 44392
# 4:    SHA          2    32

# Look into the duplicates
waitlist %>% filter(id_pha_cnt > 1) %>%
  select(agency, ssn, lname, fname, mname, dob, id_hash, id_kc_pha, id_apde) %>%
  arrange(agency, id_apde, id_kc_pha)
### About half appear to be name changes and half false positives
#   Exclude from analyses to be conservative (~0.1% for both agencies)

# See how many identifier permutations(id_hash) there are per ID
waitlist[, id_hash_cnt := uniqueN(id_hash), by = c("agency", "id_apde")]
waitlist %>% count(agency, id_hash_cnt)

waitlist %>% filter(id_hash_cnt == 3) %>%
  select(agency, ssn, lname, fname, mname, dob, id_hash, id_kc_pha, id_apde)


# Now look at when there are multiple IDs within an application 
# (also may indicate false positives)
# Need to remove id_mcaid and other variables that influence row counts
waitlist_chk <- copy(waitlist)
waitlist_chk <- waitlist_chk[id_pha_cnt == 1, .(agency, app_num, id_apde, id_mcaid)]
waitlist_chk[, id_app_cnt := uniqueN(id_apde), by = c("agency", "app_num")]
waitlist_chk[, id_mcaid_cnt := .N, by = c("agency", "app_num")]
waitlist_chk[, person_app_cnt := .N, by = c("agency", "app_num")]
waitlist_chk[, mismatch_flag := id_app_cnt != person_app_cnt]
waitlist_chk %>% count(person_app_cnt, id_app_cnt, id_mcaid_cnt)

waitlist_chk %>% filter(mismatch_flag)
waitlist_chk[person_app_cnt == 5 & id_app_cnt == 3]


waitlist_chk %>% 
  count(person_app_cnt, id_app_cnt, id_mcaid_cnt, mismatch_flag) %>%
  mutate(total = sum(n),
         mismatch_n = mismatch_flag * n,
         mismatch_sum = sum(mismatch_n)) %>%
  distinct(total, mismatch_sum) %>%
  mutate(match_sum = total - mismatch_sum)

#    total mismatch_sum match_sum
# 1: 86558          965     85593

waitlist_chk %>% group_by(agency, mismatch_flag) %>% summarise(cnt = n_distinct(app_num))

# agency mismatch_flag   cnt
# 1 KCHA   FALSE         18925
# 2 KCHA   TRUE             94
# 3 SHA    FALSE         21713
# 4 SHA    TRUE            110

### Some situations are because a person genuinely had two Medicaid IDs, but
#    others are because of false positives. Remove to be conservative (1.1% rows, 0.5% households).

### Limit columns ----
# Also restrict to one row per person per agency (there are duplicates)
waitlist_use <- waitlist %>%
  filter(id_pha_cnt == 1) %>%
  left_join(., distinct(waitlist_chk, app_num, mismatch_flag), by = "app_num") %>%
  filter(mismatch_flag == F) %>%
  select(agency, 
         # Household and head-of-household info
         app_num, hh_size, hh_dob, hh_gender, starts_with("hh_r"), 
         hh_education, hh_veteran, hh_disability, lang, lang_eng,
         # Income info
         hh_inc_tot, starts_with("inc_"),
         # Housing situation and location
         starts_with("live_"), current_housing, homeless, geo_tractce10, geo_add_mailing,
         # Individual info
         id_apde, id_mcaid, dob, gender, starts_with("r_"), disability, relcode,
         # Waitlist info
         waitlist, waitlist_2015) %>%
  distinct()


## PHA (non-time-varying) demographics ----
# Restrict to IDs also found in the waitlist data
pha_demo <- dbGetQuery(db_hhsaw,
                       "SELECT c.*, b.id_hash, b.ssn, b.lname, b.fname, b.mname, 
                       b.dob as dob_id, b.female, d.id_apde
                       FROM
                       (SELECT DISTINCT id_hash FROM pha.stage_pha_waitlist) a
                       LEFT JOIN
                       (SELECT DISTINCT * FROM pha.final_identities) b
                       ON a.id_hash = b.id_hash
                       INNER JOIN
                       (SELECT * FROM pha.final_demo) c
                       ON b.id_kc_pha = c.id_kc_pha 
                       LEFT JOIN 
                       (SELECT DISTINCT id_apde, id_kc_pha FROM claims.final_xwalk_apde_ids) d
                       ON b.id_kc_pha = d.id_kc_pha")

### Spot check things ----
pha_demo <- setDT(pha_demo)

# See how many id_kc_pha values there are per id_apde (may indicate false positives)
pha_demo[, id_pha_cnt := uniqueN(id_kc_pha), by = c("id_apde")]
pha_demo %>% count(id_pha_cnt)
#    id_pha_cnt     n
# 1:          1 24921
# 2:          2    16

# Look into the duplicates
pha_demo %>% filter(id_pha_cnt >1) %>%
  distinct(ssn, lname, fname, mname, dob_id, id_hash, id_kc_pha, id_apde) %>%
  arrange(id_apde, id_kc_pha)
### Most appear to be false positive matches, so exclude them from analyses (<0.1%)

### Apply filters ----
pha_demo_use <- pha_demo %>%
  filter(id_pha_cnt == 1) %>%
  select(id_kc_pha, id_apde, dob, admit_date:race_latino_t) %>%
  mutate(age_2017 = floor(interval(start = dob, end = "2017-12-31") / years(1))) %>%
  distinct()


## PHA (time-varying) demographics ----
# Restrict to IDs also found in the waitlist data
# Also remove people with only 1 day of coverage (usually data quality issue)
pha_timevar <- dbGetQuery(db_hhsaw,
                          "SELECT c.*, b.ssn, b.lname, b.fname, b.mname, 
                       b.dob as dob_id, b.female, d.id_apde, f.geo_tractce10 
                       FROM
                       (SELECT DISTINCT id_hash FROM pha.stage_pha_waitlist) a
                       LEFT JOIN
                       (SELECT DISTINCT * FROM pha.final_identities) b
                       ON a.id_hash = b.id_hash
                       INNER JOIN
                       (SELECT DISTINCT * FROM pha.final_timevar
                       WHERE cov_time > 1) c
                       ON b.id_kc_pha = c.id_kc_pha 
                       LEFT JOIN 
                       (SELECT DISTINCT id_apde, id_kc_pha FROM claims.final_xwalk_apde_ids) d
                       ON b.id_kc_pha = d.id_kc_pha
                       LEFT JOIN
                       (SELECT DISTINCT geo_hash_clean, geo_hash_geocode FROM ref.address_clean) e
                       ON c.geo_hash_clean = e.geo_hash_clean
                       LEFT JOIN
                       (SELECT DISTINCT geo_hash_geocode, geo_tractce10 FROM ref.address_geocode) f
                       ON e.geo_hash_geocode = f.geo_hash_geocode")

### Spot check things ----
pha_timevar <- setDT(pha_timevar)

# See how many id_kc_pha values there are per id_apde (may indicate false positives)
pha_timevar[, id_pha_cnt := uniqueN(id_kc_pha), by = c("id_apde")]
pha_timevar %>% count(id_pha_cnt)

#   id_pha_cnt     n
# 1:          1 55361
# 2:          2    22

# Look into the duplicates
pha_timevar %>% filter(id_pha_cnt >1) %>%
  distinct(ssn, lname, fname, mname, dob_id, id_hash, id_kc_pha, id_apde) %>%
  arrange(id_apde, id_kc_pha)
### Most appear to be false positive matches, so exclude them from analyses (<0.1%)

### Limit columns ----
pha_timevar_use <- pha_timevar %>%
  filter(id_pha_cnt == 1) %>%
  select(id_apde, id_hash, from_date, to_date, cov_time, gap, period,
         hh_id_long, disability,
         agency, major_prog, subsidy_type, prog_type, operator_type, vouch_type_final, 
         geo_tractce10) %>%
  distinct()


## Medicaid enrollment data ----
# Restrict to people on the waitlist (then restrict more later)
mcaid_enroll <- elig_timevar_collapse(conn = db_hhsaw,
                                      server = "hhsaw", source = "mcaid",
                                      dual = T, full_benefit = T, geo_add1 = T,
                                      geocode_vars = T,
                                      ids = unique(waitlist$id_mcaid[!is.na(waitlist$id_mcaid)]))

# We only want non-dual, full-benefit coverage to count
# Set up non-dual category to allow for multiplication
mcaid_enroll <- mcaid_enroll %>%
  mutate(non_dual = case_when(dual == 1 ~ 0L,
                              dual == 0 ~ 1L))



# APPLY INCLUSION/EXCLUSION CRITERIA ----
# Inclusion:
# - Continuously enrolled in Medicaid for at least 1 year prior to receipt of a tenant-based 
#   Housing Choice Voucher and at least 1 year following (2 years of continuous coverage) 
# OR
# - On the waitlist for a Housing Choice Voucher with 2 years of continuous Medicaid coverage
#   (so Jan 2016 to Dec 2018)
#
# Exclusion
# - Dually enrolled
# - >65 years old (assuming this applies to the end of the period so will right censor)
# - Receipt of public housing, project-based voucher, special purpose voucher holders (e.g., VSH, FUP, Mainstream)


## Remove people 65+ as of 2019-12-31 ----
# Track how many people are in the data
waitlist_cnt <- length(unique(waitlist_use$id_apde))

# Join with PHA data to get most accurate DOB possible
waitlist_dobs <- waitlist_use %>%
  distinct(id_apde, dob) %>%
  rename(dob_wl = dob) %>%
  left_join(., distinct(pha_demo_use, id_apde, dob) %>% rename(dob_58 = dob), by = "id_apde") %>%
  mutate(dob_use = case_when(!is.na(dob_58) ~ dob_58,
                             !is.na(dob_wl) ~ dob_wl,
                             TRUE ~ NA_Date_),
         dob_source_58 = case_when(!is.na(dob_58) ~ 1L,
                                   !is.na(dob_wl) ~ 0L)) %>%
  # Multiple DOBs shown for some in waitlist, take earliest date
  group_by(id_apde) %>%
  mutate(dob_use_min = min(dob_use, na.rm = T)) %>%
  ungroup() %>%
  distinct(id_apde, dob_use_min, dob_source_58)

# Pull out HoH DOB details
waitlist_dobs_hh <- waitlist_dobs %>%
  left_join(., distinct(waitlist_use, agency, app_num, id_apde, relcode), by = "id_apde") %>%
  filter(relcode %in% c("H", "Head of Household")) %>%
  rename(hh_dob_use_min = dob_use_min,
         hh_dob_source_58 = dob_source_58) %>%
  distinct(agency, app_num, hh_dob_use_min, hh_dob_source_58) %>%
  mutate(hh_age_2017 = floor(interval(start = hh_dob_use_min, end = "2017-12-31") / years(1)))

# Bring things together and add in additional variables
waitlist_dobs <- waitlist_dobs %>%
  mutate(age_2017 = floor(interval(start = dob_use_min, end = "2017-12-31") / years(1)),
         age_2019 = floor(interval(start = dob_use_min, end = "2019-12-31") / years(1)),
         age_65 = case_when(month(dob_use_min) == 2 & day(dob_use_min) == 29 ~ dob_use_min + days(1) + years(65),
                            TRUE ~ dob_use_min + years(65)),
         age_65_yr = year(age_65),
         age_drop = case_when(is.na(age_2019) ~ 9L,
                              age_2019 >= 65 ~ 1L,
                              TRUE ~ 0L))

# Show how many are going to be dropped for each reason
waitlist_dobs %>% count(age_drop)
# age_drop     n
# 0           65441
# 1           5040
# 9           5

waitlist_use <- inner_join(waitlist_use, 
                           filter(waitlist_dobs, age_drop == 0) %>% 
                             select(id_apde, age_2017, age_2019, age_65, age_65_yr, dob_source_58),
                           by = "id_apde") %>%
  left_join(., select(waitlist_dobs_hh, -hh_dob_use_min), by = c("agency", "app_num"))

waitlist_cnt_new <- length(unique(waitlist_use$id_apde))
message("There were ", waitlist_cnt - waitlist_cnt_new, " IDs removed due to age")




## Find people who moved from the waitlist into housing ----
received_voucher <- inner_join(select(waitlist_use, agency, app_num, id_apde, id_mcaid, age_65), 
                               pha_timevar_use, by = c("id_apde")) %>%
  rename(agency_application = agency.x, agency_housed = agency.y)
  

# Check for duplicates
received_voucher <- received_voucher %>%
  group_by(agency_application, app_num, id_apde, from_date) %>%
  mutate(row_cnt = n()) %>%
  ungroup()

received_voucher %>% count(row_cnt)
received_voucher %>% filter(row_cnt > 1) %>% as.data.frame()

### Look at dates people moved in ----
received_voucher %>%
  group_by(agency_housed, id_apde) %>%
  mutate(yr_move = year(min(from_date))) %>%
  distinct(agency_housed, id_apde, yr_move) %>%
  group_by(yr_move, agency_housed) %>%
  summarise(cnt = n())

# yr_move agency_housed  cnt
# 1     2001           SHA    7
# 2     2002           SHA    6
# 3     2003          KCHA   42
# 4     2003           SHA    6
# 5     2004          KCHA 1290
# 6     2004           SHA 1784
# 7     2005          KCHA  634
# 8     2005           SHA  312
# 9     2006          KCHA  390
# 10    2006           SHA  297
# 11    2007          KCHA  407
# 12    2007           SHA  418
# 13    2008          KCHA  322
# 14    2008           SHA  427
# 15    2009          KCHA  252
# 16    2009           SHA  480
# 17    2010          KCHA  252
# 18    2010           SHA  375
# 19    2011          KCHA  299
# 20    2011           SHA  514
# 21    2012          KCHA  348
# 22    2012           SHA  554
# 23    2013          KCHA  342
# 24    2013           SHA  597
# 25    2014          KCHA  366
# 26    2014           SHA  562
# 27    2015          KCHA  284
# 28    2015           SHA  540
# 29    2016          KCHA  458
# 30    2016           SHA  617
# 31    2017          KCHA  909
# 32    2017           SHA  832
# 33    2018          KCHA 1193
# 34    2018           SHA 1175
# 35    2019          KCHA 1111
# 36    2019           SHA 1402
# 37    2020          KCHA 1054
# 38    2020           SHA  663

### This seems odd but it looks like a lot of people who applied for the lottery were
#    already PHA recipients at some point in the past (e.g., some were in SHA and 
#    applied for the KCHA waitlist or vice versa).
# Flag people who were in housing prior to a 2017 move in but left censor at 2016
# Use 2017-02-01 because the SHA lottery was in Feb 2017 (KCHA was April 2017)

received_voucher <- received_voucher %>%
  mutate(prior_housing_sha = ifelse(agency_housed == "SHA" & from_date < "2017-02-01" &
                                      to_date >= "2015-01-31", 1L, 0L),
         prior_housing_kcha = ifelse(agency_housed == "KCHA" & from_date < "2017-04-01" &
                                       to_date >= "2015-03-31", 1L, 0L),
         post_lottery_entry = ifelse((agency_housed == "SHA" & from_date >= "2017-02-01") |
                                       (agency_housed == "KCHA" & from_date >= "2017-04-01"), 1L, 0L),
         post_lottery_housing = ifelse((agency_housed == "SHA" & to_date >= "2017-02-01") |
                                         (agency_housed == "KCHA" & to_date >= "2017-04-01"), 1L, 0L)) %>%
  group_by(id_apde) %>%
  # Don't take the max of post_lottery here because we want to be able filter on this
  mutate(prior_housing_sha = max(prior_housing_sha, na.rm = T),
         prior_housing_kcha = max(prior_housing_kcha, na.rm = T)) %>%
  ungroup()

received_voucher %>% distinct(id_apde, prior_housing_sha, prior_housing_kcha) %>%
  count(prior_housing_sha, prior_housing_kcha)

#   prior_housing_sha prior_housing_kcha     n
# 1                 0                  0  11606
# 2                 0                  1   2884
# 3                 1                  0   4512
# 4                 1                  1    145


### Flag first point after lottery ----
post_lottery_order <- received_voucher %>%
  filter(post_lottery_housing == 1) %>%
  distinct(id_apde, from_date, to_date) %>%
  arrange(id_apde, from_date, to_date) %>%
  group_by(id_apde) %>%
  mutate(post_lottery_order = row_number()) %>%
  ungroup()

received_voucher <- left_join(received_voucher, post_lottery_order, 
                              by = c("id_apde", "from_date", "to_date"))


### Flag people with HCV at first point after lottery ----
received_voucher <- received_voucher %>%
  mutate(post_general_voucher = case_when(
    post_lottery_order == 1 & prog_type %in% c("TBS8", "TENANT BASED") & 
      vouch_type_final == "GENERAL TENANT-BASED VOUCHER" ~ 1L, 
    post_lottery_order == 1 & (!prog_type %in% c("TBS8", "TENANT BASED") | 
      vouch_type_final != "GENERAL TENANT-BASED VOUCHER") ~ 0L))


### Flag people who ended up at a different PHA from the waitlist ----
received_mismatch <- received_voucher %>%
  filter(post_lottery_entry == 1) %>%
  mutate(agency_match = agency_application == agency_housed) %>%
  group_by(id_apde) %>%
  summarise(agency_mismatch = ifelse(max(agency_match) == 1, 0L, 1L)) %>%
  ungroup()

# Join back to main data
received_voucher <- left_join(received_voucher, received_mismatch, by = "id_apde")


### Set up table for joining with overall waitlist ----
# This will be used to flag people who were on the waitlist but already had housing at some point
# Make it here so we retain the information about prior housing etc.
received_housing <- received_voucher %>%
  group_by(id_apde) %>%
  mutate(across(c(post_lottery_entry, post_lottery_housing, post_general_voucher), ~ max(., na.rm = T))) %>%
  ungroup() %>%
  mutate(across(c(post_lottery_entry, post_lottery_housing, post_general_voucher), 
                ~ ifelse(is.infinite(.), NA_integer_, .)))


### Left censor at 2016-02-01 ----
received_voucher <- received_voucher %>% 
  filter(!to_date < "2016-02-01") %>%
  mutate(from_date = pmax(from_date, "2016-02-01"))


### Right censor at age 65 ----
received_voucher <- received_voucher %>%
  filter(!from_date >= age_65) %>%
  mutate(to_date = pmin(to_date, age_65))


### Estimate the earliest move in date after lotteries ----
# Complicated by people already being in housing
# Look for earliest from_date after 2017-02-01 for SHA and 2017-04-01 for KCHA
move_in_lottery <- received_voucher %>%
  filter(post_lottery_entry == 1) %>%
  group_by(id_apde) %>%
  mutate(move_in = min(from_date)) %>%
  ungroup() %>%
  filter(move_in == from_date) %>%
  rename(post_lottery_agency = agency_housed) %>%
  distinct(id_apde, move_in, post_lottery_agency)

move_in_all <- received_voucher %>%
  group_by(id_apde) %>%
  summarise(move_in_all = min(from_date))

# Join back to the data
received_voucher <- received_voucher %>%
  left_join(., move_in_lottery, by = "id_apde") %>%
  left_join(., move_in_all, by = "id_apde") %>%
  # If move_in is missing use max of 2017-02-01 or other move-in date and flag
  # Then set up 1 year later
  mutate(move_in_use_set = ifelse(is.na(move_in), 1L, 0L),
         move_in_use = as.Date(ifelse(is.na(move_in), 
                                      pmax(move_in_all, "2017-02-01"), 
                                      move_in),
                               origin = "1970-01-01"),
         move_in_use_plus_1 = 
           case_when(is.na(move_in_use) ~ NA_Date_,
                     month(move_in_use) == 2 & day(move_in_use) == 29 ~ 
                       move_in_use + days(1) + years(1) - days(1),
                     TRUE ~ move_in_use + years(1) - days(1)))

# See how many people are missing dates after the lottery
received_voucher %>% 
  filter(is.na(move_in)) %>% 
  filter(prog_type %in% c("TBS8", "TENANT BASED") & vouch_type_final == "GENERAL TENANT-BASED VOUCHER") %>%
  summarise(cnt = n_distinct(id_apde),
            cnt_hh = n_distinct(app_num))

#       cnt    cnt_hh
#   1   531    470


### Join to Medicaid data ----
received_voucher_mcaid <- received_voucher %>%
  # Keep only non-special HCV vouchers who entered after the lottery
  filter(post_lottery_entry == 1 & 
           prog_type %in% c("TBS8", "TENANT BASED") & 
           vouch_type_final == "GENERAL TENANT-BASED VOUCHER") %>%
  distinct(id_mcaid, id_apde, post_lottery_agency, move_in_use, move_in_use_plus_1) %>%
  inner_join(., mcaid_enroll, by = "id_mcaid")

# Find people with 11+ months coverage in the year up to lottery month
received_voucher_mcaid <- received_voucher_mcaid %>%
  mutate(pre_lottery = case_when(
    post_lottery_agency == "KCHA" ~ intersect(interval("2016-04-01", "2017-03-31"),
                                       interval(from_date, to_date)) / ddays(1) + 1,
    post_lottery_agency == "SHA" ~ intersect(interval("2016-02-01", "2017-01-31"),
                                       interval(from_date, to_date)) / ddays(1) + 1),
         pre_lottery_cnt = pre_lottery * non_dual * full_benefit) %>%
  group_by(id_apde) %>%
  mutate(pre_lottery = sum(pre_lottery, na.rm = T),
         pre_lottery_cnt = sum(pre_lottery_cnt, na.rm = T)) %>%
  ungroup() %>%
  # Using 365 even though 2016 was a leap year (since the KCHA interval doesn't include Feb 29)
  filter(pre_lottery_cnt >= 11/12 * 365 & !is.na(pre_lottery_cnt))

# Find people with 11+ months coverage after the first post-lottery entry to housing
received_voucher_mcaid <- received_voucher_mcaid %>%
  mutate(post_entry = intersect(interval(move_in_use, move_in_use_plus_1),
                                 interval(from_date, to_date)) / ddays(1) + 1,
         post_entry_cnt = post_entry * non_dual * full_benefit) %>%
  group_by(id_apde) %>%
  mutate(post_entry = sum(post_entry, na.rm = T),
         post_entry_cnt = sum(post_entry_cnt, na.rm = T)) %>%
  ungroup() %>%
  filter(post_entry_cnt >= 11/12 * 365 & !is.na(post_entry_cnt))
  


## Find people who did not move off the waitlist ----
# Not a simple anti-join since we want people who had housing up until 
# the start of the lottery but not after
no_voucher <- left_join(select(waitlist_use, agency, app_num, id_apde, id_mcaid, age_65), 
                        distinct(pha_timevar_use, agency, id_apde, from_date, to_date), 
                        by = c("id_apde")) %>%
  rename(agency_application = agency.x, agency_housed = agency.y)

### Find the latest date a person was in housing ----
# Remove all records for that person if they had housing post-lottery
no_voucher <- no_voucher %>%
  mutate(post_lottery_housing = case_when(is.na(to_date) ~ 0L,
                                          (agency_housed == "SHA" & to_date >= "2017-02-01") |
                                            (agency_housed == "KCHA" & to_date >= "2017-04-01") ~ 1L,
                                  TRUE ~ 0L)) %>%
  group_by(id_apde) %>%
  mutate(post_lottery_housing = max(post_lottery_housing, na.rm = T)) %>%
  ungroup() %>%
  filter(post_lottery_housing == 0)


### Find the last agency the person was at ----
# Used to determining cutoff date for coverage
# If the person never had housing, use the application agency
# If they applied to both agencies, use SHA since it has an earlier date
last_agency <- no_voucher %>%
  group_by(id_apde) %>%
  mutate(last_date = max(from_date),
         agency_cnt = n_distinct(agency_application)) %>%
  ungroup() %>%
  mutate(agency_last = case_when(is.na(last_date) & agency_cnt == 1 ~ agency_application,
                                 is.na(last_date) & agency_cnt > 1 ~ "SHA",
                                 last_date == from_date ~ agency_housed,
                                 TRUE ~ NA_character_)) %>%
  filter(!is.na(agency_last)) %>%
  distinct(id_apde, agency_last)

no_voucher <- left_join(no_voucher, last_agency, by = "id_apde")


### Join to Medicaid data ----
no_voucher_mcaid <- inner_join(distinct(no_voucher, id_mcaid, id_apde, agency_last), 
                               mcaid_enroll, by = "id_mcaid")

# Find people with 11+ months coverage in the year up to the lottery month
no_voucher_mcaid <- no_voucher_mcaid %>%
  mutate(pre_lottery = case_when(agency_last == "KCHA" ~ intersect(interval("2016-04-01", "2017-03-31"),
                                                                   interval(from_date, to_date)) / ddays(1) + 1,
                                 agency_last == "SHA" ~ intersect(interval("2016-02-01", "2017-01-31"),
                                                                  interval(from_date, to_date)) / ddays(1) + 1),
         pre_lottery_cnt = pre_lottery * non_dual * full_benefit) %>%
  group_by(id_apde) %>%
  mutate(pre_lottery = sum(pre_lottery, na.rm = T),
         pre_lottery_cnt = sum(pre_lottery_cnt, na.rm = T)) %>%
  ungroup() %>%
  filter(pre_lottery_cnt >= 11/12 * 365 & !is.na(pre_lottery_cnt))

# Find people with 11+ months coverage after lottery month
no_voucher_mcaid <- no_voucher_mcaid %>%
  mutate(post_entry = case_when(agency_last == "KCHA" ~ intersect(interval("2017-04-01", "2018-03-31"),
                                                                  interval(from_date, to_date)) / ddays(1) + 1,
                                agency_last == "SHA" ~ intersect(interval("2017-02-01", "2018-01-31"),
                                                                 interval(from_date, to_date)) / ddays(1) + 1),
         post_entry_cnt = post_entry * non_dual * full_benefit) %>%
  group_by(id_apde) %>%
  mutate(post_entry = sum(post_entry, na.rm = T),
         post_entry_cnt = sum(post_entry_cnt, na.rm = T)) %>%
  ungroup() %>%
  filter(post_entry_cnt >= 11/12 * 365 & !is.na(post_entry_cnt))


## Bring together list of eligible people ----
pha_mcaid <- bind_rows(distinct(received_voucher_mcaid, id_apde, id_mcaid),
                       distinct(no_voucher_mcaid, id_apde, id_mcaid)) %>%
  distinct()

# Load this to a temp SQL table for joining to mcaid data
try(dbRemoveTable(db_hhsaw, "##temp_ids", temporary = T))
dbWriteTable(db_hhsaw,
             "##temp_ids",
             pha_mcaid,
             overwrite = T)

# Add index to id and from_date for faster join
DBI::dbExecute(db_hhsaw, "CREATE NONCLUSTERED INDEX temp_ids_id ON ##temp_ids (id_mcaid)")



# MEDICAID TABLES ----
# Restrict to IDs also found in the waitlist data

## Medicaid demographics ----
mcaid_demog <- dbGetQuery(db_hhsaw,
                          "SELECT a.id_apde, b.*
                           FROM 
                        (SELECT id_apde, id_mcaid FROM ##temp_ids) a
                        INNER JOIN
                        (SELECT * FROM claims.final_mcaid_elig_demo) b
                        ON a.id_mcaid = b.id_mcaid")

## Medicaid header ----
mcaid_header <- dbGetQuery(db_hhsaw,
                           "SELECT a.id_apde, b.*
                           FROM 
                        (SELECT id_apde, id_mcaid FROM ##temp_ids) a
                        INNER JOIN
                        (SELECT * FROM claims.final_mcaid_claim_header
                        WHERE sud_dx_rda_any <> 1 AND first_service_date <= '2019-08-31') b
                        ON a.id_mcaid = b.id_mcaid")

# Remove columns that don't need to be transferred
mcaid_header <- mcaid_header %>%
  select(-id_mcaid, -ends_with("nyu"), -ed_avoid_ca, -ed_avoid_ca_nohosp, -sud_dx_rda_any)


## Medicaid claim ICDCM header ----
mcaid_icdcm_header <- dbGetQuery(db_hhsaw,
                                 "SELECT a.id_apde, b.*
                                 FROM 
                                 (SELECT id_apde, id_mcaid FROM ##temp_ids) a
                                 INNER JOIN
                                 (SELECT * FROM claims.final_mcaid_claim_icdcm_header
                                 WHERE first_service_date <= '2019-08-31') b
                                 ON a.id_mcaid = b.id_mcaid")

# Remove any claims that were dropped from the header table
mcaid_icdcm_header <- inner_join(distinct(mcaid_header, claim_header_id),
                                 mcaid_icdcm_header,
                                 by = "claim_header_id") %>%
  select(-id_mcaid)


## Medicaid claim line ----
mcaid_line <- dbGetQuery(db_hhsaw,
                           "SELECT a.id_apde, b.*
                           FROM 
                           (SELECT id_apde, id_mcaid FROM ##temp_ids) a
                           INNER JOIN
                           (SELECT * FROM claims.final_mcaid_claim_line
                           WHERE first_service_date <= '2019-08-31') b
                           ON a.id_mcaid = b.id_mcaid")

# Remove any claims that were dropped from the header table
mcaid_line <- inner_join(distinct(mcaid_header, claim_header_id),
                         mcaid_line,
                         by = "claim_header_id") %>%
  select(-id_mcaid)


## Medicaid pharmacy claims ----
mcaid_pharm <- dbGetQuery(db_hhsaw,
                         "SELECT a.id_apde, b.*
                           FROM 
                           (SELECT id_apde, id_mcaid FROM ##temp_ids) a
                           INNER JOIN
                           (SELECT * FROM claims.final_mcaid_claim_pharm) b
                         ON a.id_mcaid = b.id_mcaid")

# Remove any claims that were dropped from the header table
mcaid_pharm <- inner_join(distinct(mcaid_header, claim_header_id),
                          mcaid_pharm,
                         by = "claim_header_id") %>%
  select(-id_mcaid)


## Medicaid claim procedure codes ----
mcaid_procedure <- dbGetQuery(db_hhsaw,
                         "SELECT a.id_apde, b.*
                           FROM 
                           (SELECT id_apde, id_mcaid FROM ##temp_ids) a
                           INNER JOIN
                           (SELECT * FROM claims.final_mcaid_claim_procedure
                           WHERE first_service_date <= '2019-08-31') b
                         ON a.id_mcaid = b.id_mcaid")

# Remove any claims that were dropped from the header table
mcaid_procedure <- inner_join(distinct(mcaid_header, claim_header_id),
                              mcaid_procedure,
                              by = "claim_header_id") %>%
  select(-id_mcaid)


## Medicaid Chronic Condition Warehouse table ----
mcaid_ccw <- dbGetQuery(db_hhsaw,
                           "SELECT a.id_apde, b.from_date, b.to_date, b.ccw_code, b.ccw_desc 
                           FROM 
                           (SELECT id_apde, id_mcaid FROM ##temp_ids) a
                           INNER JOIN
                           (SELECT DISTINCT id_mcaid, from_date, to_date, ccw_code, ccw_desc
                           FROM claims.final_mcaid_claim_ccw
                           WHERE ccw_desc IN ('ccw_asthma', 'ccw_depression')) b
                          ON a.id_mcaid = b.id_mcaid")


# PREPARE FINAL OUTPUT ----
## Waitlist ----
# Those who met the inclusion criteria
waitlist_output <- inner_join(distinct(no_voucher_mcaid, id_apde),
                              waitlist_use,
                              by = "id_apde") %>%
  # Bring in any PHA demographics if they are available
  left_join(., 
            select(pha_demo_use, id_apde, starts_with("gender"), starts_with("race")) %>%
              distinct(), 
            by = "id_apde") %>%
  select(-id_mcaid, -age_65) %>%
  distinct()

# All waitlist (just to see how often people who are in housing are on the waitlist)
waitlist_all_output <- waitlist_use %>%
  select(-id_mcaid, -age_65) %>%
  distinct() %>%
  rename(agency_application = agency) %>%
  left_join(., distinct(received_housing, id_apde, prior_housing_sha, prior_housing_kcha, 
                        post_lottery_entry, post_lottery_housing, post_general_voucher, 
                        agency_mismatch),
            by = "id_apde") %>%
  left_join(., move_in_lottery, by = "id_apde") %>%
  left_join(., distinct(no_voucher, id_apde, post_lottery_housing), by = "id_apde") %>%
  mutate(post_lottery_housing = coalesce(post_lottery_housing.x, post_lottery_housing.y)) %>%
  select(-post_lottery_housing.x, -post_lottery_housing.y)


## PHA data  ----
pha_demo_output <- inner_join(distinct(waitlist_use, id_apde),
                              pha_demo_use,
                              by = "id_apde") %>% 
  select(id_apde, admit_date, age_2017, starts_with("gender"), starts_with("race_")) %>%
  distinct()

pha_timevar_output <- inner_join(distinct(waitlist_use, id_apde),
                                 pha_timevar_use,
                                 by = "id_apde") %>%
  select(-operator_type, -id_hash) %>%
  distinct() %>%
  left_join(., distinct(received_housing, id_apde, from_date, to_date, prior_housing_sha, 
                        prior_housing_kcha, post_lottery_entry, post_lottery_housing, 
                        post_lottery_order, post_general_voucher, agency_mismatch), 
            by = c("id_apde", "from_date", "to_date"))


## Medicaid tables ----
mcaid_demog_output <- mcaid_demog %>% 
  mutate(age_2017 = floor(interval(start = dob, end = "2017-12-31") / years(1))) %>%
  select(-id_mcaid, -dob)

mcaid_enroll_output <- left_join(mcaid_enroll,
                                 distinct(waitlist_use, id_apde, id_mcaid),
                                 by = "id_mcaid") %>%
  select(id_apde, from_date, to_date, dual, full_benefit, geo_tract_code)

# Other tables can be exported as is


# EXPORT DATA ----
tables_for_export <- list("waitlist_output" = waitlist_output,
                          "waitlist_all_output" = waitlist_all_output,
                          "pha_demo_output" = pha_demo_output,
                          "pha_timevar_output" = pha_timevar_output,
                          "mcaid_demog_output" = mcaid_demog_output,
                          "mcaid_enroll_output" = mcaid_enroll_output,
                          "mcaid_header" = mcaid_header,
                          "mcaid_icdcm_header" = mcaid_icdcm_header,
                          "mcaid_line" = mcaid_line,
                          "mcaid_pharm" = mcaid_pharm,
                          "mcaid_procedure" = mcaid_procedure,
                          "mcaid_ccw" = mcaid_ccw)


lapply(names(tables_for_export), function(x) {
  message("Working on ", x)
  write.csv(tables_for_export[[x]], 
            file = paste0("//dchs-shares01/DCHSDATA/DCHSPHClaimsData/Analyses/Alastair/jhu_waitlist_output/",
                          x, ".csv"),
            row.names = F)
})
