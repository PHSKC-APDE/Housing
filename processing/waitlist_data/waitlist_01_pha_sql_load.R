# OVERVIEW:
# Code to create a cleaned waitlist dataset from the SHA and KCHA data with health outcomes
#
# STEPS:
# 01 - Process raw KCHA and SHA data, combine and load to SQL database ### (THIS CODE) ###
# 02 - Join to existing housing data
# 03 - Join to 
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
  rename(r_eth_old = r_eth) %>%
  mutate(r_eth = case_when(r_hisp == 1 ~ "Hispanic",
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
lapply(c("r_hisp", "r_eth", "gender", "lang_eng", "ssn_junk"), function(x) {
  kcha_waitlist_final %>% group_by(!!sym(x)) %>% summarise(count = n())
})

# How does the calculated race compared to the pre-calculated field
kcha_waitlist_final %>% group_by(r_eth_old, r_eth) %>%
  summarise(count = n()) %>% ungroup() %>%
  arrange(r_eth_old, r_eth) %>% as.data.frame()

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
  # Rename some fields to align with KCHA
  rename(geo_add1_raw = unit_add,
         geo_add2_raw = unit_apt,
         geo_city_raw = unit_city,
         geo_state_raw = unit_state,
         geo_zip_raw = unit_zip) %>%
  mutate(data_source = "kcha_waitlist") %>%
  select(app_num, app_mbr_num, mbr_num,
         # Identifiers
         lname, fname, mname, ssn,
         # Demographics
         dob, gender, r_aian, r_asian, r_black, r_nhpi, r_white, r_multi, r_hisp,
         r_eth, lang_eng, disability, veteran, single_parent, hh_size,
         # Address details
         geo_add1_raw, geo_add2_raw, geo_city_raw, 
         geo_state_raw, geo_zip_raw
         # Other concepts
         # TBD
         )



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
  # Rename some fields to align with KCHA
  rename(geo_add1_raw = mail_add,
         geo_add2_raw = mail_apt,
         geo_city_raw = mail_city,
         geo_state_raw = mail_state,
         geo_zip_raw = mail_zip) %>%
  mutate(data_source = "sha_waitlist") %>%
  select(app_num, app_mbr_num, 
         # Identifiers
         lname, fname, mname, ssn,
         # Demographics
         dob, gender, r_aian, r_asian,r_black, r_nhpi, r_white, r_hisp,
         r_eth, lang_eng, disability, veteran, education, hh_size,
         # Address details
         geo_add1_raw, geo_add2_raw, geo_city_raw, 
         geo_state_raw, geo_zip_raw,
         # Other concepts
         # TBD
         # Housing outcome
         issuance, lease_up, increment, hcv_standard
  )


# BRING DATA TOGETHER ----
waitlist <- bind_rows(kcha_final, sha_final) %>%
  # Convert 5+4 ZIPs to be just 5 digist and add in geo_hash_raw
  mutate(geo_zip_raw = ifelse(str_detect(geo_zip_raw, "[0-9]{5}-[0-9]{4}"),
                              str_sub(geo_zip_raw, 1, 5), 
                              geo_zip_raw),
         geo_add3_raw = NA_character_,
         geo_hash_raw = as.character(toupper(openssl::sha256(paste(stringr::str_replace_na(geo_add1_raw, ''),
                                                      stringr::str_replace_na(geo_add2_raw, ''),
                                                      stringr::str_replace_na(geo_add3_raw, ''),
                                                      stringr::str_replace_na(geo_city_raw, ''), 
                                                      stringr::str_replace_na(geo_state_raw, ''), 
                                                      stringr::str_replace_na(geo_zip_raw, ''), 
                                                      sep = "|"))))) %>%
  select(app_num:geo_add2_raw, geo_add3_raw, geo_city_raw, geo_state_raw, geo_zip_raw, geo_hash_raw,
         education:hcv_standard)

## Clean addresses (first time section) ----
# Only need to load to SQL cleaning once, then can pull from ref table

# Pull out new addresses
adds_waitlist <- waitlist %>%
  select(starts_with("geo_")) %>%
  distinct()

# Compare to ref table
# Easiest to load to a temp table and join
db_hhsaw_prod <- dbConnect(odbc(), "hhsaw_prod", uid = keyring::key_list("hhsaw_dev")[["username"]])

dbRemoveTable(db_hhsaw_prod, "##waitlist_adds")
odbc::dbWriteTable(db_hhsaw_prod,
                  name = "##waitlist_adds",
                  value = adds_waitlist,
                  overwrite = T)


# Find addresses that need cleaning
adds_to_clean <- DBI::dbGetQuery(db_hhsaw_prod,
                                 "SELECT a.* FROM 
                                 (SELECT * FROM ##waitlist_adds) a
                                 LEFT JOIN
                                 (SELECT geo_hash_raw, 1 AS clean
                                 FROM ref.address_clean) b
                                 ON a.geo_hash_raw = b.geo_hash_raw
                                 WHERE b.clean IS NULL")


# Find addresses that are already clean
adds_already_clean <- DBI::dbGetQuery(db_hhsaw_prod,
                                 "SELECT b.* FROM 
                                 (SELECT geo_hash_raw FROM ##waitlist_adds) a
                                 INNER JOIN
                                 (SELECT * FROM ref.address_clean) b
                                 ON a.geo_hash_raw = b.geo_hash_raw")


if (nrow(adds_to_clean) > 0) {
  # Add new addresses that need cleaning into the Informatica table
  timestamp <- Sys.time()
  
  adds_to_clean <- adds_to_clean %>% mutate(geo_source = NA, timestamp = timestamp)
  
  DBI::dbWriteTable(db_hhsaw_prod, 
                    name = DBI::Id(schema = "ref", table = "informatica_address_input"),
                    value = adds_to_clean,
                    overwrite = F, append = T)
}


# WAIT FOR INFORMATICA JOB TO RUN
if (nrow(adds_to_clean) > 0) {
  db_hhsaw_prod <- dbConnect(odbc(), "hhsaw_prod", uid = keyring::key_list("hhsaw_dev")[["username"]])
  
  adds_clean <- dbGetQuery(db_hhsaw_prod,
                           glue::glue_sql("SELECT * FROM ref.informatica_address_output
                           WHERE convert(varchar, timestamp, 20) = {lubridate::with_tz(timestamp, 'utc')}",
                                          .con = db_hhsaw_prod))
  
  # Informatica seems to drop secondary designators when they start with #
  # Move over from old address
  adds_clean <- adds_clean %>%
    mutate(geo_add2_clean = ifelse(is.na(geo_add2_clean) & str_detect(geo_add1_raw, "^#"),
                                   geo_add1_raw, geo_add2_clean))
  
  # Tidy up some PO box messiness
  adds_clean <- adds_clean %>%
    mutate(geo_add1_clean = case_when((is.na(geo_add1_clean) | geo_add1_clean == "") & !is.na(geo_po_box_clean) ~ geo_po_box_clean,
                                      TRUE ~ geo_add1_clean),
           geo_add2_clean = case_when(
             geo_add1_clean == geo_po_box_clean ~ geo_add2_clean,
             (is.na(geo_add2_clean) | geo_add2_clean == "") & !is.na(geo_po_box_clean) & 
               !is.na(geo_add1_clean) ~ geo_po_box_clean,
             !is.na(geo_add2_clean) & !is.na(geo_po_box_clean) & 
               !is.na(geo_add1_clean) ~ paste(geo_add2_clean, geo_po_box_clean, sep = " "),
             TRUE ~ geo_add2_clean)
    )
  
  # Set up variables of interest
  adds_clean <- adds_clean %>%
    mutate(geo_geocode_skip = 0L,
           geo_hash_clean = toupper(openssl::sha256(paste(stringr::str_replace_na(geo_add1_clean, ''), 
                                                          stringr::str_replace_na(geo_add2_clean, ''), 
                                                          stringr::str_replace_na(geo_city_clean, ''), 
                                                          stringr::str_replace_na(geo_state_clean, ''), 
                                                          stringr::str_replace_na(geo_zip_clean, ''), 
                                                          sep = "|"))),
           geo_hash_geocode = toupper(openssl::sha256(paste(stringr::str_replace_na(geo_add1_clean, ''),  
                                                            stringr::str_replace_na(geo_city_clean, ''), 
                                                            stringr::str_replace_na(geo_state_clean, ''), 
                                                            stringr::str_replace_na(geo_zip_clean, ''), 
                                                            sep = "|"))),
           last_run = Sys.time()) %>%
    select(geo_add1_raw, geo_add2_raw, geo_add3_raw, geo_city_raw, 
           geo_state_raw, geo_zip_raw, geo_hash_raw,
           geo_add1_clean, geo_add2_clean, geo_city_clean, 
           geo_state_clean, geo_zip_clean, geo_hash_clean, geo_hash_geocode,
           geo_geocode_skip, last_run) %>%
    # Convert all blank fields to be NA
    mutate_if(is.character, list(~ ifelse(. == "", NA_character_, .)))
  
  
  dbWriteTable(db_hhsaw_prod, 
               name = DBI::Id(schema = "ref",  table = "address_clean"),
               adds_clean,
               overwrite = F, append = T)
  
  # Don't need to geocode at this point so skip that part
}


# Bring addresses together
if (nrow(adds_to_clean) > 0) {
  adds_final <- bind_rows(adds_already_clean, adds_clean)
} else {
  adds_final <- adds_already_clean
}



## Clean addresses (if ref table has already been updated) ----
# CODE TO COME



## Join cleaned addresses to waitlist data ----
waitlist <- waitlist %>%
  left_join(., select(adds_final, geo_hash_raw:geo_hash_geocode), 
            by = "geo_hash_raw") %>%
  select(app_num:hh_size, education, 
         geo_add1_clean:geo_hash_geocode, issuance:hcv_standard)



## Load to SQL ----
db_apde51 <- dbConnect(odbc(), "PH_APDEStore51")
dbWriteTable(db_apde51,
             name = DBI::Id(schema = "stage", table = "pha_waitlist"),
             value = waitlist,
             overwrite = T)
