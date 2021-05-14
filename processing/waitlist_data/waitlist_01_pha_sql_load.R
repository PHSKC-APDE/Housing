# OVERVIEW:
# Code to create a cleaned waitlist dataset from the SHA and KCHA data with health outcomes
#
# STEPS:
# 01 - Process raw KCHA and SHA data, combine and load to SQL database ### (THIS CODE) ###
# 02 - Join to Medicaid data
#
#
# Alastair Matheson (PHSKC-APDE)
# alastair.matheson@kingcounty.gov
# 2020-12

# BRING IN LIBRARIES AND SET CONNECTIONS ----
library(tidyverse) # Manipulate data
library(odbc) # Read to and write from SQL
library(configr) # Read in YAML files
library(glue) # Safely combine SQL code

db_apde51 <- dbConnect(odbc(), "PH_APDEStore51")

# BRING IN DATA ----
# KCHA
#kcha_waitlist <- read.csv("//phdata01/DROF_DATA/DOH DATA/Housing/KCHA/Original_data/2017 waitlist data/2017 HCV Waitlist - Applicants - JHU.csv")
kcha_waitlist_final <- read.csv("//phdata01/DROF_DATA/DOH DATA/Housing/KCHA/Original_data/2017 waitlist data/2017 HCV Waitlist - Final 3500 - JHU.csv")

# SHA
sha_waitlist <- read.csv("//phdata01/DROF_DATA/DOH DATA/Housing/SHA/Original_data/2017 HCV Waitlist with Flag for TBV Lease Up.csv")

# Bring in variable name mapping table
fields_waitlist <- read.csv(file.path(here::here(), "processing/waitlist_data/waitlist_field_name_mapping.csv"), 
                   header = TRUE, stringsAsFactors = FALSE)


# PROCESS KCHA DATA ----
# Rename fields
kcha_waitlist_final <- data.table::setnames(kcha_waitlist_final, fields_waitlist$common_name[match(names(kcha_waitlist_final), fields_waitlist$kcha_2017)])

### Drop fields that are not needed
kcha_waitlist_final <- kcha_waitlist_final %>%
  select(-starts_with("drop"))


## Set up fields for identity matching ----
# Most things look ok already
kcha_waitlist_final <- kcha_waitlist_final %>%
  mutate(ssn = str_replace_all(ssn, "-", ""),
         mname = ifelse(mname == "NULL", NA_character_, mname)) %>%
  mutate_at(vars(lname, fname, mname), list(~ trimws(toupper(.))))

# Run function to flag junk SSNs (more comprehensive than the existing ssn_missing flag)
kcha_waitlist_final <- housing::junk_ssn_num(kcha_waitlist_final, ssn)

kcha_waitlist_final <- kcha_waitlist_final %>% 
  mutate(ssn = ifelse(ssn_junk == 1, NA, ssn))


## Set up demographics ----
# The r_eth field contains summary race but it doesn't always line up with the individual
# race/ethniciity fields, so create new version
kcha_waitlist_final <- kcha_waitlist_final %>%
  mutate_at(vars(disability, r_hisp, single_parent, veteran),
            list(~case_when(. == "Y" ~ 1L,
                            . == "N" ~ 0L,
                            TRUE ~ NA_integer_))) %>%
  mutate(r_eth_new = case_when(r_hisp == 1 ~ "Hispanic",
                            r_multi > 1 ~ "Multiple race",
                            r_aian + r_asian + r_black + r_nhpi + r_white > 1 ~ "Multiple race",
                            r_aian == 1 ~ "AIAN only",
                            r_asian == 1 ~ "Asian only",
                            r_black == 1 ~ "Black only",
                            r_nhpi == 1 ~ "NHPI only",
                            r_white == 1 ~ "White only",
                            TRUE ~ NA_character_),
         dob = as.Date(dob, format = "%m/%d/%Y"),
         gender = case_when(gender == "F" ~ 1L,
                            gender == "M" ~ 2L,
                            TRUE ~ NA_integer_),
         lang_eng = case_when(lang == "ENG" ~ 1L, 
                              lang != "NULL" ~ 0L,
                              TRUE ~ NA_integer_)
         )


## Set up addresses ----
kcha_waitlist_final <- kcha_waitlist_final %>%
  mutate(unit_zip = as.character(unit_zip))


## Set up other concepts to examine ----
# Currently homeless?
# HH income


## QA ----
# Check new fields
lapply(c("r_hisp", "r_eth_new", "gender", "lang_eng", "ssn_junk"), function(x) {
  kcha_waitlist_final %>% group_by(!!sym(x)) %>% summarise(count = n())
})

# How does the calculated race compared to the pre-calculated field
kcha_waitlist_final %>% group_by(r_eth, r_eth_new) %>%
  summarise(count = n()) %>% ungroup() %>%
  arrange(r_eth, r_eth_new) %>% as.data.frame()

# See how many mailing addresses match up with the residential one (~83%)
kcha_waitlist_final %>%
  mutate(add_match = ifelse(unit_add == mail_add | mail_add == "NULL", 1L, 0L)) %>%
  distinct(app_num, add_match) %>%
  group_by(add_match) %>%
  summarise(count = n()) %>%
  ungroup() %>%
  mutate(tot = sum(count), pct = round(count/tot*100, 1))

# Find where there are multiple IDs
kcha_waitlist_final %>% summarise(count_hh = n_distinct(app_num), count_ind = n_distinct(app_mbr_num))
kcha_waitlist_final %>% group_by(hh_size) %>% summarise(count = n()) %>% ungroup() %>% mutate(hh_cnt = count / hh_size, hh_tot = sum(hh_cnt)) # hh_tot should match count_hh

kcha_waitlist_final %>% group_by(app_mbr_num) %>% summarise(cnt = n()) %>% ungroup() %>%
  filter(cnt > 1) %>% arrange(app_mbr_num) %>% head()


## Select final columns and add data source ----
kcha_final <- kcha_waitlist_final %>%
  select(app_num, app_mbr_num, mbr_num,
         # Identifiers
         lname, fname, mname, ssn,
         # Demographics
         dob, gender, r_aian, r_asian, r_black, r_nhpi, r_white, r_multi, r_hisp,
         r_eth_new, lang_eng, disability, veteran, single_parent, hh_size,
         # Address details
         unit_add, unit_apt, unit_city, unit_state, unit_zip
         # Other concepts
         # TBD
         ) %>%
  # Rename some fields to align with KCHA
  rename(r_eth = r_eth_new,
         geo_add1 = unit_add,
         geo_add2 = unit_apt,
         geo_city = unit_city,
         geo_state = unit_state,
         geo_zip = unit_zip) %>%
  mutate(data_source = "kcha_waitlist")



# PROCESS SHA DATA ----
# Rename fields
sha_waitlist <- data.table::setnames(sha_waitlist, fields_waitlist$common_name[match(names(sha_waitlist), fields_waitlist$sha_2017)])

## Set up fields for identity matching ----
# Most things look ok already
sha_waitlist <- sha_waitlist %>%
  mutate(mname = ifelse(mname == "", NA_character_, mname)) %>%
  mutate_at(vars(lname, fname, mname), list(~ trimws(toupper(.))))

# Run function to flag junk SSNs (more comprehensive than the existing ssn_missing flag)
sha_waitlist <- housing::junk_ssn_num(sha_waitlist, ssn)

sha_waitlist <- sha_waitlist %>% 
  mutate(ssn = ifelse(ssn_junk == 1, NA, ssn))


## Set up demographics ----
sha_waitlist <- sha_waitlist %>%
  mutate_at(vars(disability, r_aian, r_asian, r_black, r_nhpi, r_white, veteran, lang_eng,
                 issuance, lease_up, hcv_standard),
            list(~case_when(. == "TRUE" ~ 1L,
                            . == "FALSE" ~ 0L,
                            TRUE ~ NA_integer_))) %>%
  mutate(r_hisp = case_when(r_hisp == "Hispanic" ~ 1L,
                            r_hisp == "Non-Hispanic" ~ 0L,
                            TRUE ~ NA_integer_),
         r_eth = case_when(r_hisp == 1 ~ "Hispanic",
                           r_aian + r_asian + r_black + r_nhpi + r_white > 1 ~ "Multiple race",
                           r_aian == 1 ~ "AIAN only",
                           r_asian == 1 ~ "Asian only",
                           r_black == 1 ~ "Black only",
                           r_nhpi == 1 ~ "NHPI only",
                           r_white == 1 ~ "White only",
                           TRUE ~ NA_character_),
         dob = as.Date(dob, format = "%m/%d/%Y"),
         gender = case_when(gender == "Female" ~ 1L,
                            gender == "Male" ~ 2L,
                            TRUE ~ NA_integer_)
  )


## Set up addresses ----
sha_waitlist <- sha_waitlist %>%
  mutate_at(vars(starts_with("mail_")), list(~ trimws(toupper(.)))) %>%
  mutate_at(vars(starts_with("mail_")), list(~ ifelse(. == "", NA_character_, .))) %>%
  mutate(mail_apt = ifelse(is.na(mail_apt) & !is.na(mail_apt2), mail_apt2, mail_apt))


## Set up other concepts to examine ----
# Currently homeless?
# HH income


## QA ----
# Check new fields
lapply(c("r_hisp", "r_eth", "gender", "lang_eng", "ssn_junk"), function(x) {
  sha_waitlist %>% group_by(!!sym(x)) %>% summarise(count = n())
})

# Find where there are multiple IDs
sha_waitlist %>% summarise(count_hh = n_distinct(app_num), count_ind = n_distinct(app_mbr_num))
sha_waitlist %>% group_by(hh_size) %>% summarise(count = n()) %>% ungroup() %>% 
  mutate(hh_cnt = count / hh_size, hh_tot = sum(hh_cnt))  # hh_tot should match count_hh

sha_waitlist %>% group_by(app_mbr_num) %>% summarise(cnt = n()) %>% group_by(cnt) %>%
  summarise(count = n())
sha_waitlist %>% group_by(app_mbr_num) %>% mutate(cnt = n()) %>% ungroup() %>%
  filter(cnt > 1) %>% arrange(app_mbr_num, app_num) %>% head() %>% as.data.frame()



## Select final columns and add data source ----
sha_final <- sha_waitlist %>%
  select(app_num, app_mbr_num, 
         # Identifiers
         lname, fname, mname, ssn,
         # Demographics
         dob, gender, r_aian, r_asian,r_black, r_nhpi, r_white, r_hisp,
         r_eth, lang_eng, disability, veteran, education, hh_size,
         # Address details
         mail_add, mail_apt, mail_city, mail_state, mail_zip,
         # Other concepts
         # TBD
         # Housing outcome
         issuance, lease_up, increment, hcv_standard
  ) %>%
  # Rename some fields to align with KCHA
  rename(geo_add1 = mail_add,
         geo_add2 = mail_apt,
         geo_city = mail_city,
         geo_state = mail_state,
         geo_zip = mail_zip) %>%
  mutate(data_source = "sha_waitlist")


# BRING DATA TOGETHER ----
waitlist <- bind_rows(kcha_final, sha_final)

# Load to SQL
dbWriteTable(db_apde51,
             name = DBI::Id(schema = "stage", table = "pha_waitlist"),
             value = waitlist,
             overwrite = T)
