#### Temp check on KCHA households

library(haven)
library(openxlsx)
library(dplyr)

housing_path <- "//phdata01/DROF_DATA/DOH DATA/Housing"

#### DATA IMPORT ####
# Bring in DASHH data
pha_longitudinal <- readRDS(file = paste0(housing_path, "/OrganizedData/pha_longitudinal.Rda"))

# Bring in extract from KCHA
kcha16 <- read_dta("//phdata01/DROF_DATA/DOH DATA/Housing/KCHA/Original data/2016 data extract/2016 Extract.dta")
kcha16certs <- read.xlsx("//phdata01/DROF_DATA/DOH DATA/Housing/KCHA/Original data/2016 data extract/Certification and Rent Type_received_2017-09-20.xlsx")
kcha16 <- kcha16 %>% mutate(source = "kcha", 
                            hhssn = str_replace_all(HOHSSN, "-", ""),
                            hhssn2 = as.numeric(hhssn),
                            mid_date_new = as.Date(mid_date_n, origin = "1960-01-01"))
kcha16certs <- kcha16certs %>% mutate(source = "kcha", 
                            hhssn = str_replace_all(HOHSSN, "-", ""),
                            hhssn2 = as.numeric(hhssn),
                            effectivedate_new = as.Date(effectivedate_n, origin = "1960-01-01"))


# Also pull in 2015 individuals
kcha15_ind <- read.xlsx("//phdata01/DROF_DATA/DOH DATA/Housing/KCHA/Original data/2015 Individuals_received_2017-10-09.xlsx")
kcha15_ind <- kcha15_ind %>% 
  mutate(dob = as.Date(mdy(dateofbirth), origin = "1960-01-01"), 
         ssn_new = as.numeric(ssn),
         source = "kcha")
# Bring over temp IDs that were stripped out of SSN field
kcha15_ind <- kcha15_ind %>% mutate(ssn_new = ifelse(is.na(ssn_new), ssn, ssn_new))


#### END OF DATA IMPORT ####

# Take filtered version of combined data (before row consolidation)
kcha_phskc <- pha_cleanadd_sort_bk %>% 
  filter(agency_new == "KCHA") %>% 
  distinct(hhold_id, hh_ssn_new, hh_ssn, hh_ssn_c, hh_lname, hh_fname, hh_dob, hh_ssn_new_m6, hh_lname_m6, hh_fname_m6, hh_dob_m6,
           unit_add, unit_apt, unit_city, unit_zip, unit_concat) %>%
  mutate(source = "phskc",
         hhold_id = as.character(hhold_id))


# Take filtered version of combined data (after row consolidation)
pha_temp <- pha_longitudinal %>%
  filter(!is.na(hh_ssn_new_m6) & hh_ssn_id_m6 == ssn_id_m6 & startdate <= as.Date("2016-12-31") & enddate >= as.Date("2016-01-01")) %>%
  select(hh_ssn_new_m6, hh_ssn_id_m6, hh_lname_m6, hh_fname_m6, hh_dob_m6, agency_new, major_prog, agency_prog_concat, 
         unit_concat, startdate, enddate, row, pid, hhold_id_new, port_in:port_out_sha) %>%
  mutate(source = "phskc")


pha_temp_ind <- pha_longitudinal %>%
  filter(!is.na(ssn_id_m6)) %>%
  select(pid, ssn_id_m6, lname_new, fname_new, mname_new, dob, lname_new_m6, fname_new_m6, mname_new_m6, dob_m6,
         agency_new, major_prog, agency_prog_concat, unit_concat, startdate, enddate, row, port_in:port_out_sha) %>%
  mutate(source = "phskc")

# Bring in record of dropped rows
drop_track <- readRDS(file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/drop_track.Rda")


#### Look at original/pre-consolidation data ####
kcha_match_left <- left_join(kcha16, kcha_phskc, by = c("householdid" = "hhold_id"))
kcha_match_left %>% group_by(source.x, source.y) %>% summarise(count = n())
temp_z <- kcha_match_left %>% filter(is.na(source.y)) %>% select(householdid:mid, housingauthorityname, vouchernumber, unitaddress, HOHSSN:HOHLastName)

kcha_phskc %>% filter(hh_ssn %in% c("574869731", "535960701", "531787221", "399544847"))
temp_a <- kcha_long %>% filter(hh_ssn == "535960701")
kcha %>% filter(h3n01 %in% c("399544847", "531718994", "535960701")) %>% select(householdid, h3n01)


#### Look at full post-consolidation data ####
kcha_match_ssn <- left_join(kcha16, pha_temp, by = c("hhssn2" = "hh_ssn_new_m6"))
kcha_match_ssn <- left_join(kcha16certs, pha_temp, by = c("hhssn2" = "hh_ssn_new_m6"))

kcha_match_ssn <- kcha_match_ssn %>%
  mutate(kcha_ever = ifelse(agency_new == "KCHA" & !is.na(agency_new), 1,
                            ifelse(agency_new == "SHA" & !is.na(agency_new), 0, NA))) %>%
  group_by(householdid) %>%
  mutate(kcha_ever = max(kcha_ever)) %>%
  ungroup()


## Compare action types and add years (doesn't really work)
cert_check <- left_join(kcha16certs, drop_track, by = c("hhssn2" = "ssn_new", "effectivedate_new" = "act_date")) #%>%
  filter(str_detect(agency_prog_concat, "KCHA"))
table(cert_check$hud500582atypeofaction, cert_check$act_type, useNA = 'always')
cert_check %>% filter(hud500582atypeofaction == 4 & act_type == 5) %>% head(.)
table(cert_check$rent_type, cert_check$add_yr, useNA = 'always')
cert_check %>% filter(rent_type == "WIN Rent" & add_yr == 3) %>% head(.)


### See why some SSNs are dropped
temp_a <- kcha_match_ssn %>% distinct(HOHSSN)
kcha_match_ssn_full <- left_join(kcha16, pha_temp, by = c("hhssn2" = "hh_ssn_new_m6")) %>% distinct(HOHSSN)
temp_b <- anti_join(kcha_match_ssn_full, temp_a) %>%
  left_join(kcha16) %>%
  select(householdid, HOHSSN, mid, housingauthorityname, vouchernumber, unitaddress, HOHBirthdate:HOHLastName) %>%
  mutate(hhssn = str_replace_all(HOHSSN, "-", ""), hhssn2 = as.numeric(hhssn))
# Find people who show up in KCHA 2016 data but are not matched
temp_c <- kcha_match_ssn %>% filter(is.na(kcha_ever)) %>% 
  select(householdid, mid, HOHSSN, HOHBirthdate, HOHLastName, HOHFirstName, 
         hh_ssn_id_m6, hh_dob_m6, hh_lname_m6, hh_fname_m6, agency_prog_concat, pid)

temp_d <- pha_longitudinal %>% filter(ssn_new %in% c(539676263, 535861473, 269132067, 537154689, 536151336, 574869731)) %>% 
  select(ssn_new, pid, row, lname_new_m6, fname_new_m6, dob_m6, agency_prog_concat, unit_concat, mbr_num, mbr_num_old,
         startdate, enddate, truncated, port_in:port_out_sha, hh_ssn_new_m6)




## Look at matched households to see where they sit
# Compare with distinct happening before and after grouping
kcha_match_ssn %>% distinct(HOHSSN, .keep_all = T) %>% group_by(source.x, source.y) %>% summarise(count = n())
kcha_match_ssn %>% distinct(pid, .keep_all = T) %>% group_by(source.x, source.y, agency_new, major_prog) %>% summarise(count = n())
kcha_match_ssn %>% distinct(pid, .keep_all = T) %>% group_by(source.x, source.y, kcha_ever) %>% summarise(count = n())
kcha_match_ssn %>% group_by(source.x, source.y, kcha_ever) %>% distinct(HOHSSN, .keep_all = T) %>% summarise(count = n())

kcha_match_ssn %>% group_by(source.x, source.y, agency_new, major_prog) %>% distinct(pid, .keep_all = T) %>% summarise(count = n())
kcha_match_ssn %>% group_by(source.x, source.y, kcha_ever, agency_new, major_prog, port_in, port_out_kcha, port_out_sha) %>% 
  summarise(count = n_distinct(householdid)) %>%
  mutate(total = sum(.$count))
# Switch order of distinct
kcha_match_ssn %>% 
  distinct(householdid, .keep_all = T) %>%
  group_by(source.x, source.y, kcha_ever, agency_new, major_prog, port_in, port_out_kcha, port_out_sha) %>% 
  summarise(count = n_distinct(householdid)) %>%
  mutate(total = sum(.$count))



#### Use DASHH data as starting point ####
kcha_match_dash <- left_join(pha_temp, kcha16, by = c("hh_ssn_new_m6" = "hhssn2")) %>%
  select(pid, hhold_id_new, hh_ssn_new_m6:enddate, port_in:port_out_sha, householdid:mod, unitaddress, mid_date_new, source.x, source.y)

kcha_match_dash <- kcha_match_dash %>%
  mutate(kcha_ever = ifelse(agency_new == "KCHA" & !is.na(agency_new), 1,
                            ifelse(agency_new == "SHA" & !is.na(agency_new), 0, NA))) %>%
  group_by(hhold_id_new) %>%
  mutate(kcha_ever = max(kcha_ever)) %>%
  ungroup()


### Pull out all KCHA households with move-in dates < KCHA file's MID
temp_a <- kcha_match_dash %>% filter(agency_new == "KCHA" & pid %in% c(1136, 1165, 1205, 38584, 98354, 81728, 21502, 49097, 133133, 149564))
temp_b <- pha_longitudinal %>% 
  filter(pid %in% c(1136, 1165, 1205, 38584, 98354, 81728, 21502, 49097, 133133, 149564)) %>% 
  select(pid, ssn_id_m6, lname_new_m6, fname_new_m6, dob_m6, agency_prog_concat, unit_concat, startdate, enddate)
temp_d <- drop_track %>% filter(pid %in% c(1136, 1165, 1205, 38584, 98354, 81728, 21502, 49097, 133133, 149564))

# Strip out SHA data
temp_a <- temp_a %>% filter(agency_new == "KCHA")
temp_b <- temp_b %>% filter(str_detect(agency_prog_concat, "KCHA"))
temp_d <- temp_d %>% filter(str_detect(agency_prog_concat, "KCHA"))

mismatch_mid_list <- list("Mismatches" = temp_a, "Longitudinal" = temp_b, "Drop_sheet" = temp_d)
write.xlsx(mismatch_mid_list, file = paste0("//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/KCHA cleaning/Mismatches from 2016 - early move ins_", 
                                    Sys.Date(), ".xlsx"))



### Find people who show up in DASHH data but not KCHA
temp_x <- kcha_match_dash %>% filter(agency_new == "KCHA" & is.na(source.y))
temp_y <- pha_longitudinal %>% 
  filter(pid %in% c(1084, 1294, 4098, 5904, 7443, 8288, 12957, 26039, 26707, 41067, 45329, 48900, 65557, 86106, 82539, 114638, 135622)) %>% 
  select(pid, hhold_id_new, ssn_id_m6, lname_new_m6, fname_new_m6, dob_m6, agency_prog_concat, unit_concat, startdate, enddate, port_in:port_out_sha)
temp_z <- drop_track %>% filter(pid %in% c(1084, 1294, 4098, 5904, 7443, 8288, 12957, 26039, 26707, 41067, 45329, 48900, 65557, 86106, 82539, 114638, 135622))



# Strip out SHA data
temp_x <- temp_x %>% filter(agency_new == "KCHA")
temp_y <- temp_y %>% filter(str_detect(agency_prog_concat, "KCHA"))
temp_z <- temp_z %>% filter(str_detect(agency_prog_concat, "KCHA"))

mismatch_overall_list <- list("Mismatches" = temp_x, "Longitudinal" = temp_y, "Drop_sheet" = temp_z)
write.xlsx(mismatch_overall_list, file = paste0("//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/KCHA cleaning/Mismatches from 2016 - all mismatches_", 
                                            Sys.Date(), ".xlsx"))



#### Compare numbers over 2015  ####
pha_longitudinal %>% filter(startdate < as.Date("2015-01-01") & enddate > as.Date("2015-12-31") & agency_new == "KCHA") %>%
  summarise(count_hh = n_distinct(hhold_id_new), count_ind = n_distinct(pid))


pha_longitudinal %>% filter(startdate <= as.Date("2015-12-31") & enddate >= as.Date("2015-01-01") & agency_new == "KCHA") %>%
  summarise(count_hh = n_distinct(hhold_id_new), count_ind = n_distinct(pid))


tempa <- pha_longitudinal %>% filter(startdate <= as.Date("2015-12-31") & enddate >= as.Date("2015-01-01") & agency_new == "KCHA") %>%
  arrange(hhold_id_new, cov_time) %>%
  group_by(hhold_id_new) %>%
  slice(n()) %>%
  ungroup()



#### Look at 2015 individuals ####
# Keep all DASH rows
kcha_match_dash <- left_join(pha_temp_ind, kcha15_ind, by = c("ssn_id_m6" = "ssn_new")) %>% 
  filter(startdate <= as.Date("2015-12-31") & enddate >= as.Date("2015-01-01"))

kcha_match_dash <- kcha_match_dash %>%
  mutate(kcha_ever = ifelse(agency_new == "KCHA" & !is.na(agency_new), 1,
                            ifelse(agency_new == "SHA" & !is.na(agency_new), 0, NA))) %>%
  group_by(pid) %>%
  mutate(kcha_ever = max(kcha_ever)) %>%
  ungroup()

kcha_match_dash %>% group_by(agency_new) %>% summarise(count = n_distinct(pid))
kcha_match_dash %>% group_by(source.x, source.y, agency_new) %>% summarise(count = n_distinct(pid))

# Keep all KCHA rows
kcha_match <- left_join(kcha15_ind, pha_temp_ind, by = c("ssn_new" = "ssn_id_m6")) %>% distinct(fullname, ssn, dob, .keep_all = T)
kcha_match %>% group_by(source.x, source.y) %>% summarise(count = n())

# Look at mismatches from DASH data
temp_1 <- kcha_match_dash %>% filter(is.na(source.y) & agency_new == "KCHA")

pids <- kcha_match_dash %>% filter(is.na(source.y) & agency_new == "KCHA") %>%
  distinct(pid) %>% sample_n(., 10)
temp_a <- left_join(pids, kcha_match_dash, by = "pid") %>%
  select(pid, ssn_id_m6, startdate, enddate, lname_new, fname_new, dob.x, unit_concat, agency_new, agency_prog_concat, source.x, source.y)
temp_d <- left_join(pids, drop_track, by = "pid")

# Look at mismatches from KCHA data
temp_2 <- kcha_match %>% filter(is.na(source.y))

pids2 <- kcha_match %>% filter(is.na(source.y)) %>% distinct(ssn) %>% sample_n(., 10)
temp_b <- left_join(pids2, kcha15_ind, by = "ssn")

temp_z <- pha_temp_ind %>% filter(str_detect(lname_new, "XXX") & str_detect(fname_new, "XXX")) %>% 
  select(pid, ssn_id_m6, startdate, enddate, lname_new, fname_new, dob, unit_concat, agency_new, agency_prog_concat)
temp_z <- kcha_long %>% filter(str_detect(toupper(lname), "XXXX") & str_detect(toupper(fname), "XXX")) %>% 
  select(ssn, lname, fname, mname, dob, unit_add)
temp_z <- kcha_long %>% filter(str_detect(ssn, "XXXX")) %>%  select(ssn, lname, fname, mname, dob, unit_add)



# Merge and export for review
write.xlsx(temp_1, file = paste0("//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/KCHA cleaning/Mismatches from 2015 - in DASHH not KCHA_", 
                                             Sys.Date(), ".xlsx"))

kcha_mismatch <- bind_rows(temp_a_bk, temp_a) %>% filter(is.na(source.y) & agency_new == "KCHA")
kcha_mismatch_drop <- bind_rows(temp_d_bk, temp_d) %>% filter(str_detect(agency_prog_concat, "KCHA") &
                                                                !pid %in% c(18949, 150130))
kcha_mismatch_list <- list("Mismatches" = kcha_mismatch, "Drop_sheet" = kcha_mismatch_drop)
write.xlsx(kcha_mismatch_list, file = paste0("//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/KCHA cleaning/Mismatches from 2015 - in DASHH not KCHA - sample_", 
                                            Sys.Date(), ".xlsx"))





#### Spare code to check things ####
temp_e <- kcha_match_ssn %>%
  filter(!is.na(pid)) %>%
  group_by(pid) %>%
  mutate(max_date = max(enddate)) %>%
  ungroup()

temp_f <- temp_e %>% distinct(pid, max_date)

temp_2 <- pha_longitudinal %>% filter(pid %in% c(123221, 82667, 128568, 30909, 14095, 123671)) %>% 
  select(ssn_new, pid, row, mbr_num, agency_prog_concat, unit_concat, port_in:port_out_sha, startdate, enddate, truncated)

temp_3 <- kcha16 %>% filter(hhssn2 %in% c(365605595, 531234770, 536372045, 545880229, 547691721, 569455026)) %>%
  select(householdid, HOHSSN, HH2016Status, mid, mod, mid_date_n, mod_date_n, lengthofstay, source)




kcha_match_ssn %>% filter(source.y == 'phskc' & agency_new == "SHA") %>% distinct(hhssn2, pid) %>% head(., n = 20)
kcha_match_ssn %>% filter(source.y == 'phskc' & agency_new == "SHA" & major_prog == "HCV" & port_out_kcha == 0 & kcha_ever == 0) %>% 
  distinct(hhssn2, pid) %>% head(., n = 20)

temp_b <- kcha_match_ssn %>% filter(source.y == 'phskc' & agency_new == "SHA") %>% 
  select(householdid, HH2016Status, mid, unitaddress, HOHSSN:HOHLastName, hh_ssn_id_m6:source.y) %>%
  distinct()
temp_b <- kcha_match_ssn %>% filter(pid == 23751) %>% 
  select(householdid, HH2016Status, mid, unitaddress, HOHSSN:HOHLastName, hh_ssn_id_m6:source.y) %>%
  distinct()


temp_b <- pha_cleanadd_sort_bk %>% filter(pid == 142388) %>% select(-(ssn_new:r_hisp), -(mbr_num:hhold_id_new))


temp_c <- drop_track %>% filter(pid %in% c(31574, 32098, 144804, 52315, 83756, 90031))


temp_1 <- drop_track %>% filter(pid %in% c(31574, 32098, 144804, 52315, 83756, 90031))
temp_1 <- drop_track %>% filter(pid %in% c(54588))
temp_1 <- drop_track %>% filter(pid %in% c(141169))
temp_1 <- drop_track %>% filter(pid %in% c(79126))
temp_1 <- drop_track %>% filter(pid %in% c(123671))


temp_2 <- pha_cleanadd_sort %>% filter(pid %in% c(31574, 32098, 144804, 52315, 83756, 90031)) %>% 
  select(ssn_new, pid, row, mbr_num, agency_prog_concat, unit_concat, act_date, act_type, max_date, port_in:port_out_sha, cost_pha, portability, 
         sha_source, drop)
temp_2 <- pha_cleanadd_sort %>% filter(pid %in% c(31574, 32098, 144804, 52315, 83756, 90031)) %>% 
  select(ssn_new, pid, row, mbr_num, agency_prog_concat, unit_concat, act_date, act_type, max_date, port_in:port_out_sha, cost_pha, portability, 
         sha_source, startdate, enddate, drop, add_yr)
temp_2 <- pha_longitudinal %>% filter(pid %in% c(31574, 32098, 144804, 52315, 83756, 90031)) %>% 
  select(ssn_new, pid, row, mbr_num, agency_prog_concat, unit_concat, port_in:port_out_sha, startdate, enddate, truncated)


ids <- pha_cleanadd_sort %>% filter(drop == 9) %>% distinct(pid) %>% sample_n(., 10)
temp_3 <- left_join(ids, pha_cleanadd_sort, by = "pid") %>%
  select(ssn_new, pid, row, agency_prog_concat, unit_concat, act_date, act_type, max_date, sha_source, port_in:port_out_sha, cost_pha, drop, drop2)

ids <- pha_cleanadd_sort %>% filter(truncated == 1) %>% distinct(pid) %>% sample_n(., 10)
temp_3 <- left_join(ids, pha_cleanadd_sort, by = "pid") %>%
  select(ssn_new, pid, row, agency_prog_concat, unit_concat, act_date, act_type, max_date, port_in:port_out_sha, cost_pha, portability, 
         sha_source, drop, startdate, enddate, add_yr, truncated)



filter(pid %in% c(2775, 48844, 97197, 25386, 135195))
filter(pid %in% c(134445, 65021, 118314, 141111, 10232)) # check these for ports with overlaps

95416

# People to track: 31574, 32098, 144804
ids <- data.frame("pid" = c(15237, 63630, 70079, 6492, 15641, 3624, 40402, 45462, 47447, 130096))
