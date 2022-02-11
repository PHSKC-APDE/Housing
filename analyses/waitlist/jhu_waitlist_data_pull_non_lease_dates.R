## Script name: jhu_waitlist_data_pull_non_lease
##
## Purpose of script: Find people who received a voucher but didn't lease up
##
## Author: Alastair Matheson, Public Health - Seattle & King County
## Date Created: 2021-12-03
## Email: alastair.matheson@kingcounty.gov
##
## Notes:
##   
##

# SET OPTIONS AND BRING IN PACKAGES ----
options(scipen = 6, digits = 4, warning.length = 8170)

if (!require("pacman")) {install.packages("pacman")}
pacman::p_load(tidyverse, odbc, glue, data.table, janitor)

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
waitlist <- dbGetQuery(db_hhsaw,
                       "SELECT a.*, b.id_kc_pha, c.id_apde, d.geo_tractce10 
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
                       ON a.geo_hash_geocode = d.geo_hash_geocode")


waitlist_all_output <- read.csv("//dchs-shares01/DCHSDATA/DCHSPHClaimsData/Analyses/Alastair/jhu_waitlist_output/waitlist_all_output.csv")

## SHA ----
sha_waitlist_lease <- read.csv("//phdata01/DROF_DATA/DOH DATA/Housing/SHA/Original_data/Waitlist data/2017 HCV Waitlist with Flag for TBV Lease Up.csv") %>%
  mutate(DOB = as.Date(DOB, format = "%m/%d/%Y")) %>%
  mutate(across(contains("Name"), ~ toupper(.x))) %>%
  mutate(across(contains("SSN"), ~ str_pad(.x, 9, side = "left", pad = "0")))

sha_voucher_date <- readxl::read_xlsx("//phdata01/DROF_DATA/DOH DATA/Housing/SHA/Original_data/Waitlist data/Voucher_Issuance_date_2017_waitlist_received 2022-02-02.xlsx") %>%
  mutate(across(contains("NAME"), ~ toupper(.x))) %>%
  mutate(SSN = str_remove_all(SSN, "-"),
         voucher_date = as.Date(MIN_VOUCHER_ISSUE_DATE))


## KCHA ----
kcha_stage <- dbGetQuery(db_hhsaw, "SELECT DISTINCT hh_id, id_hash FROM pha.stage_kcha")

kcha_waitlist_lease <- read.csv("//phdata01/DROF_DATA/DOH DATA/Housing/KCHA/Original_data/2017 waitlist data/kcha_shopping success 2015-2021 final.csv") %>%
  janitor::clean_names() %>%
  mutate(across(contains("_date"), ~ as.Date(.x, format = "%m/%d/%Y")),
         across(c("voucherissueddate_n", "admissiondate_n", "effectivedate_n"), 
                ~ as.Date(.x, origin = "1960-01-01")))



# SHA ANALYSES ----
# Check everyone in the same household has the same status
sha_waitlist_lease %>% group_by(HeadOfHouseholdID) %>% 
  mutate(issue_cnt = n_distinct(Is.On.Issuance, na.rm = T), 
         lease_cnt = n_distinct(Is.On.Issuance, na.rm = T)) %>% 
  ungroup() %>% count(issue_cnt, lease_cnt)


# Check and see if everyone who ended up in the housing data was flagged that way in the 
sha_lease_chk <- sha_waitlist_lease %>%
  left_join(., distinct(waitlist_all_output, agency_application, app_num, id_apde, prior_housing_sha, 
                        prior_housing_kcha, post_lottery_entry, post_lottery_housing) %>%
              filter(agency_application == "SHA") %>%
              mutate(present = 1L),
            by = c("HeadOfHouseholdID" = "app_num"))

sha_lease_chk %>% count(Is.On.LeaseUp, present)

# Look at why people didn't match
sha_lease_chk %>% filter(is.na(present)) %>%
  select(HeadOfHouseholdID, Relationship, DOB) %>%
  mutate(age_2019 = floor(interval(start = DOB, end = "2019-12-31") / years(1))) %>%
  count(age_2019)

# 367/443 (82.8% over 65)
# age_2019	n
# 3	  5
# 4	  3
# 5	  2
# 6	  1
# 7	  3
# 8	  1
# 10	2
# 11	2
# 12	2
# 13	4
# 14	4
# 15	1
# 16	4
# 17	1
# 18	3
# 19	4
# 20	2
# 21	1
# 22	2
# 23	2
# 24	1
# 25	1
# 26	2
# 27	1
# 28	1
# 30	1
# 31	2
# 33	2
# 34	2
# 36	1
# 37	1
# 39	3
# 42	1
# 49	2
# 55	1
# 56	1
# 59	1
# 60	1
# 61	1
# 62	1
# 65	47
# 66	39
# 67	45
# 68	24
# 69	26
# 70	31
# 71	21
# 72	16
# 73	16
# 74	14
# 75	13
# 76	12
# 77	10
# 78	7
# 79	5
# 80	6
# 81	6
# 82	7
# 83	5
# 84	6
# 85	1
# 87	4
# 88	1
# 89	2
# 92	1
# 93	1
# 95	1


## Find people who had a voucher issued but didn't lease up ----
# Using inner join now to exclude non-matches
sha_waitlist_non_lease <- distinct(sha_waitlist_lease, HeadOfHouseholdID, Is.On.Waitlist, Is.On.Issuance, Is.On.LeaseUp) %>%
  inner_join(., distinct(waitlist_all_output, agency_application, app_num, id_apde, prior_housing_sha, 
                        prior_housing_kcha, post_lottery_entry, post_lottery_housing) %>%
              mutate(present = 1L),
            by = c("HeadOfHouseholdID" = "app_num"))


# See how many non-leased up people appear to actually have housing
sha_waitlist_non_lease %>% count(Is.On.LeaseUp, post_lottery_housing) %>%
  group_by(Is.On.LeaseUp) %>%
  mutate(Total = sum(n), pct = round(n / Total * 100, 1))

# Is.On.LeaseUp   post_lottery_housing     n Total   pct
# 1 FALSE                            0  5300  6648  79.7
# 2 FALSE                            1  1348  6648  20.3
# 3 TRUE                             0   107  2190   4.9
# 4 TRUE                             1  2083  2190  95.1

# See how many non-issued people appear to actually have housing
sha_waitlist_non_lease %>% count(Is.On.Issuance, post_lottery_housing) %>%
  group_by(Is.On.Issuance) %>%
  mutate(Total = sum(n), pct = round(n / Total * 100, 1))

# Is.On.Issuance   post_lottery_housing     n Total   pct
# 1 FALSE                             0  4828  5956  81.1
# 2 FALSE                             1  1128  5956  18.9
# 3 TRUE                              0   579  2882  20.1
# 4 TRUE                              1  2303  2882  79.9


## Find dates for people who were issued a voucher ----
# Also add waitlist data for id_apde
sha_lease_date <- sha_waitlist_lease %>%
  distinct(HeadOfHouseholdID, FirstName, MiddleInitial, LastName, SSN, HOH_SSN,
           Is.On.Waitlist, Is.On.Issuance, Is.On.LeaseUp, Leased.Up.Under.Standard.TBV) %>%
  left_join(., sha_voucher_date, 
            by = c("FirstName" = "FIRST_NAME", "LastName" = "LAST_NAME", "SSN")) %>%
  left_join(., distinct(waitlist_all_output, agency_application, app_num, id_apde, prior_housing_sha, 
                        prior_housing_kcha, post_lottery_entry, post_lottery_housing,
                        post_lottery_agency, move_in) %>%
              mutate(present = 1L),
            by = c("HeadOfHouseholdID" = "app_num")) %>%
  mutate(has_date = !is.na(MIN_VOUCHER_ISSUE_DATE),
         has_move_in_date = !is.na(move_in))
  

# See how many issued vouchers don't have a date
sha_lease_date %>% 
  distinct(HeadOfHouseholdID, Is.On.Issuance, has_date) %>%
  count(Is.On.Issuance, has_date)
#   Is.On.Issuance has_date    n
# 1          FALSE    FALSE 2967
# 2          FALSE     TRUE    9
# 3           TRUE    FALSE  874
# 4           TRUE     TRUE  864


# See how many lease ups don't have a date
sha_lease_date %>% 
  distinct(HeadOfHouseholdID, Is.On.LeaseUp, has_date) %>%
  count(Is.On.LeaseUp, has_date)
#   Is.On.LeaseUp has_date    n
# 1         FALSE    FALSE 3171
# 2         FALSE     TRUE  211
# 3          TRUE    FALSE  670
# 4          TRUE     TRUE  662

sha_lease_date %>% 
  distinct(HeadOfHouseholdID, Is.On.LeaseUp, has_date, has_move_in_date) %>%
  count(Is.On.LeaseUp, has_date, has_move_in_date)
#   Is.On.LeaseUp has_date has_move_in_date    n
# 1         FALSE    FALSE            FALSE 2758
# 2         FALSE    FALSE             TRUE  504
# 3         FALSE     TRUE            FALSE  173
# 4         FALSE     TRUE             TRUE   48
# 5          TRUE    FALSE            FALSE  117
# 6          TRUE    FALSE             TRUE  639
# 7          TRUE     TRUE            FALSE   97
# 8          TRUE     TRUE             TRUE  619


# Look into leases without dates
sha_lease_date %>% filter(Is.On.LeaseUp & has_date == F) %>% head()

# Look into people who were not flagged as leasing up but had voucher dates
sha_lease_date %>% filter(Is.On.LeaseUp == F & has_date) %>% head()

# Set up data for export
sha_lease_date_export <- sha_lease_date %>% select(id_apde, HeadOfHouseholdID, voucher_date)


# KCHA ANALYSES ----
kcha_waitlist_lease

chk <- kcha_waitlist_lease %>% 
  select(household_id, hoh_birthdate , voucher_number, 
         voucher_issued_date, voucher_effective_date, voucher_effective_date,
         voucher_end_date, lease_begin_date, finalization_result, effective_date, no_lease_up) %>%
  left_join(., mutate(kcha_stage, stage = 1L), by = c("household_id" = "hh_id")) %>%
  left_join(., filter(waitlist, agency == "KCHA") %>% distinct(id_hash, app_num), by = "id_hash") %>%
  left_join(., distinct(waitlist_all_output, agency_application, app_num, hh_size, hh_dob),
            by = "app_num")


# EXPORT DATA ----
write.csv(sha_waitlist_non_lease, "//dchs-shares01/DCHSDATA/DCHSPHClaimsData/Analyses/Alastair/jhu_waitlist_output/sha_waitlist_non_lease.csv",
          row.names = F)
write.csv(sha_lease_date_export, "//dchs-shares01/DCHSDATA/DCHSPHClaimsData/Analyses/Alastair/jhu_waitlist_output/sha_waitlist_lease_date.csv",
          row.names = F)
