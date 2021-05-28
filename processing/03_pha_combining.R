###############################################################################
# OVERVIEW:
# Code to create a cleaned person table from the combined 
# King County Housing Authority and Seattle Housing Authority data sets
# Aim is to have a single row per contiguous time in a house per person
#
# STEPS:
# 01 - Process raw KCHA data and load to SQL database
# 02 - Process raw SHA data and load to SQL database
# 03 - Bring in individual PHA datasets and combine into a single file ### (THIS CODE) ###
# 04 - Deduplicate data and tidy up via matching process
# 05 - Recode race and other demographics
# 06 - Clean up addresses
# 06a - Geocode addresses
# 07 - Consolidate data rows
# 08 - Add in final data elements and set up analyses
# 09 - Join with Medicaid-Medicare eligibility & time varying data
# 10 - Set up joint housing/Medicaid analyses
#
# Alastair Matheson (PHSKC-APDE)
# alastair.matheson@kingcounty.gov
# 2016-05-13, split into separate files 2017-10
# 
###############################################################################

#### Set up global parameter and call in libraries ####
library(housing) # contains many useful functions for cleaning
library(odbc) # Used to connect to SQL server
library(data.table) # Used to manipulate data
library(tidyverse) # Used to manipulate data


script <- httr::content(httr::GET("https://raw.githubusercontent.com/PHSKC-APDE/Housing/master/processing/metadata/set_data_env.r"))
eval(parse(text = script))

housing_source_dir <- file.path(here::here(), "processing")
METADATA = RJSONIO::fromJSON(file.path(housing_source_dir, "metadata/metadata.json"))
set_data_envr(METADATA,"combined")



#### Bring in data ####
if(sql == TRUE) {
  db_apde51 <- dbConnect(odbc(), "PH_APDEStore51")
  # This takes ~65 seconds
  system.time(sha <- DBI::dbReadTable(db_apde51, DBI::Id(schema = "stage", table = "pha_sha")))

  # This takes ~40 seconds
  system.time(kcha_long <- DBI::dbReadTable(db_apde51, DBI::Id(schema = "stage", table = "pha_kcha")))
}


#### Fix up variable formats ####
# Vars mtw_admit_date & move_in_date missing in UW files
# Switched to catch-all terms to accommodate differences in UW/PHSKC files
sha <- sha %>% mutate_at(vars(contains("date"), contains("dob")),
                         list(~ as.Date(., format = "%Y-%m-%d")))
sha <- sha %>% mutate_at(vars(contains("zip")), list(~ as.character(.)))

sha <- sha %>% mutate_at(vars(unit_year, bdrm_voucher), 
                         list(~ as.numeric(.)))


kcha_long <- kcha_long %>% mutate_at(vars(act_date, admit_date, dob, hh_dob),
                                     list(~ as.Date(., format = "%Y-%m-%d")))
kcha_long <- kcha_long %>% mutate_at(vars(contains("zip"), unit_type), list(~ as.character(.)))


  

#### Make variable to track where data came from ####
sha <- sha %>% 
  mutate(agency_new = "SHA",
         access_unit = as.numeric(access_unit),
         access_req = as.numeric(access_req))
kcha_long <- mutate(kcha_long, agency_new = "KCHA")


#### Append data ####
pha <- bind_rows(kcha_long, sha)


#### Clean up data ####
### Lots of variables have white space
pha <- pha %>%
  mutate_at(vars(relcode, unit_add, unit_apt, unit_apt2, 
                 unit_city, unit_state, contains("name"), 
                 prog_type, vouch_type, property_name, 
                 property_type, portfolio, cost_pha),
            list(~str_trim(.)))


### Fix up inconsistent capitalization in key variables
# Change names to be consistently upper case
pha <- pha %>% mutate_at(vars(contains("name"), contains("unit"), 
                              prog_type, vouch_type, property_name, 
                              property_type, portfolio, cost_pha), 
                         list(~ toupper(.)))


### Relative code
pha <- pha %>%
  mutate(relcode = ifelse(relcode == "NULL" | is.na(relcode) | relcode == "", 
                          "", relcode))

### Social security numbers
pha <- pha %>%
  # Some SSNs are HUD/PHA-generated IDs so make two fields (_c and _new)
  # (note that conversion of legitimate SSN to numeric strips out 
  # leading zeros and removes rows with characters)
  # Need to restore leading zeros
  # Remove dashes first
  mutate_at(vars(ssn, hh_ssn), list(~ str_replace_all(., "-", ""))) %>%
  mutate(ssn_c = ifelse(str_detect(ssn, "[:alpha:]"), ssn, NA),
         hh_ssn_c = ifelse(str_detect(hh_ssn, "[:alpha:]"), hh_ssn, NA)) %>%
  mutate_at(vars(ssn, hh_ssn), 
            list(new = ~ str_pad(round(as.numeric(.), digits = 0),
                                 width = 9, side = "left", pad = "0")))

# ID junk SSNs and IDs
pha <- junk_ssn_num(pha, ssn_new)
pha <- junk_ssn_char(pha, ssn_c)


### Billed agency (just fix up KCHA and other common ones)
pha <- pha %>%
  mutate(cost_pha = ifelse(cost_pha %in% c("WAOO2", "WA02 ", " WA02", "W002"),
                           "WA002", cost_pha),
         cost_pha = ifelse(cost_pha == " WA03", "WA003", cost_pha),
         cost_pha = ifelse(cost_pha == "NULL" | is.na(cost_pha), "", cost_pha))

### Dates
# Strip out dob components for matching
pha <- pha %>%
  mutate(dob_y = as.numeric(lubridate::year(dob)), 
         dob_mth = as.numeric(lubridate::month(dob)),
         dob_d = as.numeric(lubridate::day(dob)))


# Find most common DOB by SSN (doesn't work for SSN = NA or 0)
# Need to figure out how to ID most common or most recent last name for 
# SSNs like 0 or NA
pha <- setDT(pha)
pha[, dob_cnt := if_else(ssn_new_junk == 0 | ssn_c_junk == 0, .N, as.integer(0)),
    by = .(ssn_new, ssn_c, dob)]
pha <- setDF(pha)


### Names overall
# Strip out multiple spaces in the middle of last names, periods,
# inconsistent apostrophes, extraneous commas, and other general fixes
pha <- pha %>%
  mutate_at(vars(contains("name")), funs(str_replace(., "[:space:]{2,}|, ", " "))) %>%
  mutate_at(vars(contains("name")), funs(str_replace(., "`", "'"))) %>%
  mutate_at(vars(contains("name")), funs(str_replace(., "_", "-"))) %>%
  mutate_at(vars(contains("name")), funs(str_replace(., "NULL|\\.|\"|\\\\", ""))) %>%
  mutate_at(vars(contains("name")), funs(ifelse(is.na(.), "", .)))


### First name
# Clean up where middle initial seems to be in first name field
# NOTE: There are many rows with a middle initial in the fname field AND 
# the mname field (and the initials are not always the same)
# Need to talk with PHAs to determine best approach
pha <- pha %>% mutate(
  fname_new = ifelse(str_detect(str_sub(fname, -2, -1), "[:space:][A-Z]"), 
                     str_sub(fname, 1, -3), 
                     fname),
  mname_new = case_when(
    str_detect(str_sub(fname, -2, -1), "[:space:][A-Z]") & is.na(mname) ~ str_sub(fname, -1),
    str_detect(str_sub(fname, -2, -1), "[:space:][A-Z]") &
      !is.na(mname) ~ paste(str_sub(fname, -1), mname, sep = " "),
    mname == "" ~ NA_character_,
    TRUE ~ mname))

### Last name suffix
# Need to look for suffixes in first and last name if wanting to merge with SHA data
suffix <- c(" JR", " SR"," II", " IV", " III", " LLL", " 2ND", " 111", " JR.")

pha <- pha %>%
  # Suffixes in first names
  mutate(lnamesuf_new = 
           case_when(str_detect(fname_new, paste(suffix, collapse="|")) & is.na(lnamesuf) ~ 
                       str_trim(str_sub(fname_new, 
                                        str_locate(fname_new, paste(suffix, collapse="|"))[,1],
                                        str_locate(fname_new, paste(suffix, collapse="|"))[,2])),
                     str_detect(fname_new, paste(suffix, collapse="|")) & !is.na(lnamesuf) ~ 
                       paste(lnamesuf, str_trim(str_sub(fname_new, 
                                                        str_locate(fname_new, paste(suffix, collapse="|"))[,1],
                                                        str_locate(fname_new, paste(suffix, collapse="|"))[,2]))),
                     lnamesuf == "" ~ NA_character_,
                     TRUE ~ lnamesuf),
         fname_new = ifelse(str_detect(fname_new, paste(suffix, collapse="|")), 
                        str_trim(str_sub(fname_new, 
                                         1,
                                         str_locate(fname_new, paste(suffix, collapse="|"))[,1])), 
                        fname),
         # Suffixes in last names
         lnamesuf_new = 
           case_when(str_detect(lname, paste(suffix, collapse="|")) & is.na(lnamesuf) ~ 
                       str_trim(str_sub(lname, 
                                        str_locate(lname, paste(suffix, collapse="|"))[,1],
                                        str_locate(lname, paste(suffix, collapse="|"))[,2])),
                     str_detect(lname, paste(suffix, collapse="|")) & !is.na(lnamesuf) ~ 
                       paste(lnamesuf, str_trim(str_sub(lname, 
                                                        str_locate(lname, paste(suffix, collapse="|"))[,1],
                                                        str_locate(lname, paste(suffix, collapse="|"))[,2]))),
                     TRUE ~ lnamesuf_new),
         lname_new = ifelse(str_detect(lname, paste(suffix, collapse="|")), 
                        str_trim(str_sub(lname, 
                                         1,
                                         str_locate(lname, paste(suffix, collapse="|"))[,1])), 
                        lname),
         # Remove any punctuation
         lnamesuf_new = str_replace_all(lnamesuf_new, pattern = "[:punct:]|[:blank:]", replacement = "")
  )

#### Set up variables for matching ####
### Sort records
pha <- arrange(pha, hh_ssn_new, hh_ssn_c, ssn_new, ssn_c, act_date)

### Make trimmed version of names
pha <- pha %>%
  mutate(
    # Trim puncutation
    lname_trim = str_replace_all(lname_new, "[:punct:]|[:digit:]|[:blank:]|`", ""),
    fname_trim = str_replace_all(fname_new, pattern = "[:punct:]|[:digit:]|[:blank:]|`", replacement = ""),
    # Make soundex versions of names for matching/grouping
    lname_phon = RecordLinkage::soundex(lname_trim),
    fname_phon = RecordLinkage::soundex(fname_trim),
    # Make truncated first and last names for matching/grouping (alternative to soundex)
    lname3 = substr(lname_trim, 1, 3),
    fname3 = substr(fname_trim, 1, 3)
  )


### Count which first names appear most often (doesn't work for SSN = NA or 0)
# Need to figure out how to ID most common or most recent last name for SSNs like 0 or NA
pha <- setDT(pha)
pha[, fname_new_cnt := if_else(ssn_new_junk == 0 | ssn_c_junk == 0, .N, as.integer(0)),
                by = .(ssn_new, ssn_c, fname_new)]
pha <- setDF(pha)

### Count which non-blank middle names appear most often (doesn't work for SSN = NA or 0)
# Need to figure out how to ID most common or most recent last name for SSNs like 0 or NA
pha_middle <- setDT(pha)
pha_middle <- pha_middle[mname_new != "" & (ssn_new_junk == 0 | ssn_c_junk == 0),
                         .(mname_new_cnt = .N), by = .(ssn_new, ssn_c, mname_new)]
pha_middle <- setDF(pha_middle)

pha <- left_join(pha, pha_middle, by = c("ssn_new", "ssn_c", "mname_new"))
rm(pha_middle)

### Last name
# Find the most recent surname used (doesn't work for SSN = NA or 0)
# NB. Some most recent surnames are blank so use one before that
# Need to figure out how to ID most common or most recent last name for SSNs like 0 or NA
pha_last <- setDT(pha)
setorder(pha_last, ssn_new, ssn_c, -act_date, na.last = T)
pha_last <- pha_last[lname_new != "" & (ssn_new_junk == 0 | ssn_c_junk == 0),
                         .(lname_rec = lname_new[[1]]), by = .(ssn_new, ssn_c)]
pha_last <- setDF(pha_last)
pha_last <- pha_last %>% distinct(ssn_new, ssn_c, lname_rec)

pha <- left_join(pha, pha_last, by = c("ssn_new", "ssn_c"))
rm(pha_last)


### Count which non-blank last name suffixes appear most often (doesn't work for SSN = NA or 0)
# Need to figure out how to ID most common or most recent last name for SSNs like 0 or NA
pha_suffix <- pha %>%
  filter(lnamesuf_new != "" & (ssn_new_junk == 0 | ssn_c_junk == 0)) %>%
  group_by(ssn_new, ssn_c, lnamesuf_new) %>%
  summarise(lnamesuf_new_cnt = n()) %>%
  ungroup()
pha <- left_join(pha, pha_suffix, by = c("ssn_new", "ssn_c", "lnamesuf_new"))
rm(pha_suffix)


### Gender
# Count the number of genders recorded for an individual (doesn't work for SSN = NA or 0)
# Need to figure out how to ID most common or most recent last name for SSNs like 0 or NA
pha <- pha %>%
  mutate(gender_new = case_when(str_detect(str_trim(toupper(gender)), "^F$|^FEMALE$") ~ 1L,
                                str_detect(str_trim(toupper(gender)), "^M|^MALE$") ~ 2L,
                                TRUE ~ NA_integer_))

pha <- setDT(pha)
pha[, gender_new_cnt := if_else(ssn_new_junk == 0 | ssn_c_junk == 0, .N, as.integer(0)),
    by = .(ssn_new, ssn_c, gender_new)]
pha <- setDF(pha)

if (sql == TRUE) {
#### Save point ####
saveRDS(pha, file = file.path(housing_path, pha_fn))
}

#### Clean up ####
rm(kcha_long)
rm(sha)
rm(suffix)
gc()
