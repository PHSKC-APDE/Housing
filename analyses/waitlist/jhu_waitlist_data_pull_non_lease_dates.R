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
pacman::p_load(tidyverse, odbc, glue, data.table, janitor, sqldf, lubridate)

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
  janitor::clean_names() %>%
  mutate(across(c(contains("date"), contains("dob")), ~ as.Date(.x, format = "%m/%d/%Y")),
         across(contains("name"), ~ toupper(.x)),
         across(contains("ssn"), ~ str_pad(.x, 9, side = "left", pad = "0")))

sha_voucher_date <- readxl::read_xlsx("//phdata01/DROF_DATA/DOH DATA/Housing/SHA/Original_data/Waitlist data/Voucher_Issuance_date_2017_waitlist_received 2022-03-31.xlsx", sheet = "Issuance and Lease Up") %>%
  janitor::clean_names() %>%
  mutate(across(contains("names"), ~ toupper(.x)),
         ssn = str_remove_all(ssn, "-"),
         across(c("termination_date", "placed_on_clock", "initial_shopping_term_end",
                  "new_move_in_date", "other_change_of_unit_date"), 
                ~ as.Date(as.numeric(.x), origin = "1899-12-30")),
         voucher_date = as.Date(effective_date))


## KCHA ----
kcha_stage <- dbGetQuery(db_hhsaw, "SELECT DISTINCT hh_id, id_hash FROM pha.stage_kcha")

kcha_waitlist_jhu <- read.csv("//phdata01/DROF_DATA/DOH DATA/Housing/KCHA/Original_data/2017 waitlist data/kcha_shopping success 2015-2021 final.csv")

kcha_waitlist_lease <- readxl::read_xlsx("//phdata01/DROF_DATA/DOH DATA/Housing/KCHA/Original_data/2017 waitlist data/Shopping success 2015-2021 final_received 2022-04-04.xlsx") %>%
  janitor::clean_names() %>%
  mutate(across(c(contains("_date"), "hoh_birthdate"), ~ as.Date(.x, format = "%m/%d/%Y")),
         across(c("voucherissueddate_n", "admissiondate_n", "effectivedate_n"), 
                ~ as.Date(.x, origin = "1960-01-01")),
         across(contains("_name"), ~ toupper(.x)),
         hoh_ssn = str_pad(str_remove_all(hoh_ssn, "-"), 9, pad = "0"))



# SHA ANALYSES ----
# Check everyone in the same household has the same status
sha_waitlist_lease %>% group_by(head_of_household_id) %>% 
  mutate(issue_cnt = n_distinct(is_on_issuance, na.rm = T), 
         lease_cnt = n_distinct(is_on_issuance, na.rm = T)) %>% 
  ungroup() %>% count(issue_cnt, lease_cnt)


# Check and see if everyone who ended up in the housing data was flagged that way in the 
sha_lease_chk <- sha_waitlist_lease %>%
  left_join(., distinct(waitlist_all_output, agency_application, app_num, id_apde, prior_housing_sha, 
                        prior_housing_kcha, post_lottery_entry, post_lottery_housing) %>%
              filter(agency_application == "SHA") %>%
              mutate(present = 1L),
            by = c("head_of_household_id" = "app_num"))

sha_lease_chk %>% count(is_on_lease_up, present)
# is_on_lease_up   present     n
# 1          FALSE       1 22139
# 2          FALSE      NA   370
# 3           TRUE       1  7861
# 4           TRUE      NA    73


# Look at why people didn't match
sha_lease_chk %>% filter(is.na(present)) %>%
  select(head_of_household_id, relationship, dob) %>%
  mutate(age_2019 = floor(interval(start = dob, end = "2019-12-31") / years(1))) %>%
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
sha_waitlist_non_lease <- distinct(sha_waitlist_lease, head_of_household_id, is_on_waitlist, is_on_issuance, is_on_lease_up) %>%
  inner_join(., distinct(waitlist_all_output, agency_application, app_num, id_apde, prior_housing_sha, 
                        prior_housing_kcha, post_lottery_entry, post_lottery_housing, post_general_voucher) %>%
              mutate(present = 1L,
                     post_general_voucher = case_when(is.infinite(post_general_voucher) ~ 0,
                                                      is.na(post_general_voucher) ~ 0,
                                                      TRUE ~ post_general_voucher)),
            by = c("head_of_household_id" = "app_num"))


# See how many non-leased up people appear to actually have housing
sha_waitlist_non_lease %>% count(is_on_lease_up, post_lottery_housing) %>%
  group_by(is_on_lease_up) %>%
  mutate(Total = sum(n), pct = round(n / Total * 100, 1))

# is_on_lease_up   post_lottery_housing     n Total   pct
# 1 FALSE                            0  5300  6648  79.7
# 2 FALSE                            1  1348  6648  20.3
# 3 TRUE                             0   107  2190   4.9
# 4 TRUE                             1  2083  2190  95.1

# See how many also had general vouchers
sha_waitlist_non_lease %>% count(is_on_lease_up, post_lottery_housing, post_general_voucher) %>%
  group_by(is_on_lease_up) %>%
  mutate(Total = sum(n), pct = round(n / Total * 100, 1))

# is_on_lease_up post_lottery_housing post_general_voucher     n Total   pct
# 1 FALSE                            0                    0  5300  6648  79.7
# 2 FALSE                            1                    0   956  6648  14.4
# 3 FALSE                            1                    1   392  6648   5.9
# 4 TRUE                             0                    0   107  2190   4.9
# 5 TRUE                             1                    0   553  2190  25.3
# 6 TRUE                             1                    1  1530  2190  69.9


# See how many non-issued people appear to actually have housing
sha_waitlist_non_lease %>% count(is_on_issuance, post_lottery_housing) %>%
  group_by(is_on_issuance) %>%
  mutate(Total = sum(n), pct = round(n / Total * 100, 1))

# is_on_issuance   post_lottery_housing     n Total   pct
# 1 FALSE                             0  4828  5956  81.1
# 2 FALSE                             1  1128  5956  18.9
# 3 TRUE                              0   579  2882  20.1
# 4 TRUE                              1  2303  2882  79.9


sha_waitlist_non_lease %>% count(is_on_issuance, post_lottery_housing, post_general_voucher) %>%
  group_by(is_on_issuance) %>%
  mutate(Total = sum(n), pct = round(n / Total * 100, 1))

#   is_on_issuance post_lottery_housing post_general_voucher     n Total   pct
# 1 FALSE                             0                    0  4828  5956  81.1
# 2 FALSE                             1                    0   785  5956  13.2
# 3 FALSE                             1                    1   343  5956   5.8
# 4 TRUE                              0                    0   579  2882  20.1
# 5 TRUE                              1                    0   724  2882  25.1
# 6 TRUE                              1                    1  1579  2882  54.8


## Find dates for people who were issued a voucher ----
# Also add waitlist data for id_apde
sha_lease_date <- sha_waitlist_lease %>%
  distinct(head_of_household_id, hoh_ssn, is_on_waitlist, is_on_issuance, is_on_lease_up, leased_up_under_standard_tbv) %>%
  left_join(., sha_voucher_date, by = c("hoh_ssn" = "ssn")) %>%
  left_join(., distinct(waitlist_all_output, agency_application, app_num, id_apde, prior_housing_sha, 
                        prior_housing_kcha, post_lottery_entry, post_lottery_housing, post_general_voucher,
                        post_lottery_agency, move_in) %>%
              mutate(present = 1L) %>%
              filter(agency_application == "SHA"),
            by = c("head_of_household_id" = "app_num")) %>%
  mutate(has_voucher_date = !is.na(effective_date),
         has_move_in_date = !is.na(move_in))
  

# See how many people in the voucher date list don't show up in the waitlist data
sha_waitlist_date_mismatch <- anti_join(sha_voucher_date, waitlist, by = c("ssn" = "hh_ssn"))


# See how many issued vouchers don't have a date
sha_lease_date %>% 
  distinct(head_of_household_id, is_on_issuance, has_voucher_date) %>%
  count(is_on_issuance, has_voucher_date)
#   is_on_issuance has_voucher_date    n
# 1          FALSE            FALSE 2969
# 2           TRUE            FALSE  114
# 3           TRUE             TRUE 1123


# See how many lease ups don't have a date
sha_lease_date %>% 
  distinct(head_of_household_id, is_on_lease_up, has_voucher_date) %>%
  count(is_on_lease_up, has_voucher_date)
#   is_on_lease_up has_voucher_date    n
# 1          FALSE            FALSE 3009
# 2          FALSE             TRUE  249
# 3           TRUE            FALSE   74
# 4           TRUE             TRUE  874

sha_lease_date %>% 
  distinct(head_of_household_id, is_on_lease_up, has_voucher_date, has_move_in_date) %>%
  count(is_on_lease_up, has_voucher_date, has_move_in_date)
#   is_on_lease_up has_voucher_date has_move_in_date    n
# 1          FALSE            FALSE            FALSE 2627
# 2          FALSE            FALSE             TRUE  463
# 3          FALSE             TRUE            FALSE  208
# 4          FALSE             TRUE             TRUE   51
# 5           TRUE            FALSE            FALSE   23
# 6           TRUE            FALSE             TRUE   58
# 7           TRUE             TRUE            FALSE  128
# 8           TRUE             TRUE             TRUE  825


sha_lease_date %>% 
  distinct(head_of_household_id, has_voucher_date, has_move_in_date) %>%
  count(has_voucher_date, has_move_in_date)
#   has_voucher_date has_move_in_date    n
# 1            FALSE            FALSE 2650
# 2            FALSE             TRUE  521
# 3             TRUE            FALSE  336
# 4             TRUE             TRUE  876


# Look into leases without dates
sha_lease_date %>% filter(is_on_lease_up & has_voucher_date == F) %>% head()


# Look into people who were not flagged as leasing up but had voucher dates
sha_lease_date %>% filter(is_on_lease_up == F & has_voucher_date) %>% head()
sha_lease_date %>% filter(is_on_lease_up == F & has_voucher_date) %>% count(status)
#             status   n
# 1  FAILED_LEASE_UP 535
# 2         SHOPPING  21
# 3 SUCCESS_LEASE_UP  48


# Look into people who had voucher dates but no general voucher move-in date
sha_lease_date %>% filter(has_voucher_date & has_move_in_date == F) %>% count(status)
#             status   n
# 1  FAILED_LEASE_UP 440
# 2         SHOPPING  19
# 3 SUCCESS_LEASE_UP 170
sha_lease_date %>% filter(has_voucher_date & has_move_in_date == F & status == "SUCCESS_LEASE_UP") %>% head()


# Look into people who had move in dates but no voucher date
sha_lease_date %>% filter(has_move_in_date == T & has_voucher_date == F) %>% 
  distinct(head_of_household_id, post_lottery_agency) %>% count(post_lottery_agency)

sha_lease_date %>% filter(has_move_in_date == T & has_voucher_date == F) %>% 
  distinct(head_of_household_id, post_general_voucher) %>% count(post_general_voucher)

sha_lease_date %>% 
  filter(has_move_in_date == T & has_voucher_date == F & post_lottery_agency == "SHA" & post_general_voucher == 1) %>% 
  count(year(move_in))

sha_lease_date %>% 
  filter(has_move_in_date == T & has_voucher_date == F & post_lottery_agency == "SHA" & post_general_voucher == 1) %>% 
  sample_n(5)


# Set up data for export
sha_lease_date_export <- sha_lease_date %>% 
  distinct(id_apde, head_of_household_id, voucher_date, has_voucher_date, has_move_in_date, status)



# KCHA ANALYSES ----
# Try to join lease up date data with existing waitlist and other data
# First use household ID to join
kcha_hh_join <- kcha_waitlist_lease %>% 
  select(household_id, hoh_birthdate, hoh_first_name, hoh_last_name, hoh_ssn,
         voucher_number, voucher_issued_date, voucher_effective_date, voucher_effective_date,
         voucher_end_date, lease_begin_date, finalization_result, effective_date, no_lease_up) %>%
  left_join(., mutate(kcha_stage, stage = 1L), by = c("household_id" = "hh_id")) %>%
  left_join(., filter(waitlist, agency == "KCHA") %>% 
              distinct(id_hash, fname, mname, lname, ssn, dob, id_apde) %>%
              mutate(waitlist = 1), 
            by = "id_hash") %>%
  left_join(., distinct(waitlist_all_output, agency_application, app_num, hh_size, hh_dob, id_apde) %>%
              filter(agency_application == "KCHA") %>%
              mutate(waitlist_out = 1),
            by = "id_apde")


# Then join on name or SSN match
kcha_pii_join <- sqldf("SELECT a.*, b.* FROM
             (SELECT household_id, hoh_birthdate, hoh_first_name, hoh_last_name, hoh_ssn,
               voucher_number, voucher_issued_date, voucher_effective_date, voucher_expiration_date,
               voucher_end_date, lease_begin_date, finalization_result, effective_date, no_lease_up
               FROM kcha_waitlist_lease) a
             LEFT JOIN
             (SELECT DISTINCT app_num, fname, mname, lname, ssn, dob, 1 AS waitlist
               FROM waitlist WHERE agency = 'KCHA') b
             ON (a.hoh_last_name = b.lname AND a.hoh_first_name = b.fname) OR
             (a.hoh_ssn = b.ssn AND a.hoh_ssn IS NOT NULL)")


# See how many rows were matched
kcha_hh_join %>% distinct(household_id, waitlist) %>% 
  filter(!is.na(waitlist)) %>%
  summarise(cnt = n())
# 1169

kcha_pii_join %>% distinct(household_id, waitlist) %>% 
  filter(!is.na(waitlist)) %>%
  summarise(cnt = n())
# 2071

# See how many overlapping and non-overlapping rows there were each approach
kcha_hh_join %>% distinct(household_id, waitlist) %>% 
  anti_join(., distinct(kcha_pii_join, household_id, waitlist), by = c("household_id", "waitlist")) %>%
  filter(!is.na(waitlist)) %>%
  summarise(cnt = n())
# 114

kcha_pii_join %>% distinct(household_id, waitlist) %>% 
  anti_join(., distinct(kcha_hh_join, household_id, waitlist), by = c("household_id", "waitlist")) %>%
  filter(!is.na(waitlist)) %>%
  summarise(cnt = n())
# 1016

kcha_hh_join %>% distinct(household_id, waitlist) %>% 
  inner_join(., distinct(kcha_pii_join, household_id, waitlist), by = c("household_id", "waitlist")) %>%
  filter(!is.na(waitlist)) %>%
  summarise(cnt = n())
1055


## Join together and prepare for exporting ----
kcha_dates <- bind_rows(kcha_hh_join %>%
                          filter(!is.na(waitlist)) %>%
                          select(id_apde, app_num, agency_application, household_id, 
                                 ends_with("_date"), no_lease_up) %>%
                          distinct() %>%
                          mutate(source = "hh"),
                        kcha_pii_join %>% 
                          distinct(household_id, waitlist) %>% 
                          anti_join(., distinct(kcha_hh_join, household_id, waitlist), by = c("household_id", "waitlist")) %>%
                          filter(!is.na(waitlist)) %>%
                          select(household_id) %>%
                          left_join(., select(kcha_pii_join, app_num, household_id, ends_with("_date"), no_lease_up) %>% 
                                      distinct(), 
                                    by = "household_id") %>%
                          left_join(., distinct(waitlist_all_output, agency_application, app_num, id_apde),
                                    by = "app_num") %>%
                          mutate(source = "pii")
                        ) %>%
  arrange(app_num, id_apde)

kcha_date_export <- kcha_dates %>%
  filter(!is.na(agency_application)) %>%
  select(app_num, id_apde, voucher_issued_date, voucher_effective_date, voucher_end_date,
         voucher_expiration_date, lease_begin_date, effective_date, no_lease_up) %>%
  distinct()


# EXPORT DATA ----
write.csv(sha_waitlist_non_lease, "//dchs-shares01/DCHSDATA/DCHSPHClaimsData/Analyses/Alastair/jhu_waitlist_output/sha_waitlist_non_lease.csv",
          row.names = F)
write.csv(sha_lease_date_export, "//dchs-shares01/DCHSDATA/DCHSPHClaimsData/Analyses/Alastair/jhu_waitlist_output/sha_waitlist_lease_date.csv",
          row.names = F)
write.csv(kcha_date_export, "//dchs-shares01/DCHSDATA/DCHSPHClaimsData/Analyses/Alastair/jhu_waitlist_output/kcha_waitlist_lease_date.csv",
          row.names = F)
