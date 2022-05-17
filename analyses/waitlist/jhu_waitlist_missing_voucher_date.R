## Script name: jhu_waitlist_missing_voucher_date
##
## Purpose of script: Add more details to the sample of missing voucher dates
##
## Author: Alastair Matheson, Public Health - Seattle & King County
## Date Created: 2022-04-29
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


waitlist_all_output <- read.csv("//dchs-shares01/DCHSDATA/DCHSPHClaimsData/Analyses/Alastair/jhu_waitlist_output/waitlist_all_output.csv") %>%
  mutate(agency_mismatch_new = case_when(is.na(post_lottery_agency) ~ NA_integer_,
                                         agency_application == post_lottery_agency ~ 0L,
                                         agency_application != post_lottery_agency ~ 1L))

ids <- dbGetQuery(db_hhsaw, "SELECT * FROM pha.final_identities")


## SHA ----
sha_sample <- read.csv("//phdata01/DROF_DATA/DOH DATA/Housing/SHA/Original_data/waitlist data/SHA_AppsToCheck_20220422.csv") %>%
  rename(app_num = HeadOfHouseholdID)

sha_stage <- dbGetQuery(db_hhsaw, "SELECT DISTINCT ssn, lname, fname, hh_id, hh_ssn, hh_lname, hh_fname, id_hash FROM pha.stage_sha")


# Join to waitlist to get id_apde and then join to IDs data to get SSN and other details
# Collapse to keep only rows with details
sha_sample_ids <- waitlist_all_output %>%
  filter(agency_application == "SHA") %>%
  distinct(app_num, id_apde) %>%
  right_join(., distinct(sha_sample, app_num), by = "app_num") %>%
  left_join(., select(waitlist, app_num, id_apde, id_kc_pha), by = c("app_num", "id_apde")) %>%
  left_join(., distinct(ids, id_kc_pha, id_hash), by = "id_kc_pha") %>%
  left_join(., sha_stage, by = "id_hash") %>%
  filter(!is.na(ssn) | !is.na(lname)) %>%
  select(-id_hash) %>%
  distinct()

# Join rows with details to initial sample
sha_sample_pii <- waitlist_all_output %>%
  filter(agency_application == "SHA") %>%
  distinct(app_num, id_apde, age_2019, agency_mismatch, agency_mismatch_new) %>%
  right_join(., distinct(sha_sample, app_num), by = "app_num") %>%
  left_join(., select(waitlist, app_num, ssn, lname, fname, id_apde, id_kc_pha), 
            by = c("app_num", "id_apde")) %>%
  rename(wl_ssn = ssn, wl_lname = lname, wl_fname = fname) %>%
  # Join to get hash that might line up with sha stage
  left_join(., sha_sample_ids, by = c("app_num", "id_kc_pha", "id_apde")) %>%
  distinct()


## KCHA ----
kcha_sample <- read.csv("//phdata01/DROF_DATA/DOH DATA/Housing/KCHA/Original_data/2017 waitlist data/KCHA_AppsToCheck_20220422.csv") %>%
  rename(app_num = Application.Number)

kcha_stage <- dbGetQuery(db_hhsaw, "SELECT DISTINCT hh_id, hh_ssn, hh_lname, hh_fname, id_hash FROM pha.stage_kcha")


# Join to waitlist to get id_apde and then join to IDs data to get SSN and other details
kcha_sample_pii <- waitlist_all_output %>%
  filter(agency_application == "KCHA") %>%
  distinct(app_num, id_apde, age_2019, agency_mismatch) %>%
  inner_join(., distinct(kcha_sample, app_num), by = "app_num") %>%
  left_join(., select(waitlist, app_num, ssn, lname, fname, id_apde, id_hash), 
            by = c("app_num", "id_apde")) %>%
  rename(wl_ssn = ssn, wl_lname = lname, wl_fname = fname) %>%
  left_join(., filter(kcha_stage, !is.na(hh_id)), by = "id_hash")
  

# EXPORT DATA ----
write.csv(sha_sample_pii, "//dchs-shares01/DCHSDATA/DCHSPHClaimsData/Analyses/Alastair/jhu_waitlist_output/sha_apps_to_check_details.csv",
          row.names = F)

write.csv(kcha_sample_pii, "//dchs-shares01/DCHSDATA/DCHSPHClaimsData/Analyses/Alastair/jhu_waitlist_output/kcha_apps_to_check_details.csv",
          row.names = F)

