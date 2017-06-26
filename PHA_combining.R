###############################################################################
# Code to create a cleaned person table from the combined 
# King County Housing Authority and Seattle Housing Authority data sets
# Aim is to have a single row per contiguous time in a house per person
#
# Alastair Matheson (PHSKC-APDE)
# alastair.matheson@kingcounty.gov
# 2016-05-13
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
library(RecordLinkage) # used to clean up duplicates in the data
library(phonics) # used to extract phonetic version of names
library(reticulate) # used to pull in python-based address parser


##### Connect to the servers #####
db.apde51 <- odbcConnect("PH_APDEStore51")


##### Bring in data #####
ptm01 <- proc.time() # Times how long this query takes (~90 secs)
sha <-
  sqlQuery(db.apde51,
    "SELECT * FROM dbo.sha_combined_raw",
    stringsAsFactors = FALSE
  )
proc.time() - ptm01


ptm01 <- proc.time() # Times how long this query takes (~32 secs)
kcha_long <-
  sqlQuery(db.apde51,
           "SELECT * FROM dbo.kcha_reshaped",
           stringsAsFactors = FALSE
  )
proc.time() - ptm01


### Fix up variable formats
sha <- sha %>%
  mutate_at(vars(act_date, admit_date, dob),
            funs(as.Date(., format = "%Y-%m-%d"))) %>%
  mutate_at(vars(bdrm_voucher, rent_subs, disability),
            funs(as.numeric(car::recode(., "'Y' = 1; 'Yes' = 1; 'N' = 0; 'No ' = 0; 'N/A' = NA; 'SRO' = NA; 'NULL' = NA; else = NA"))))


kcha_long <- kcha_long %>%
  mutate_at(vars(act_date, admit_date, dob, hh_dob),
            funs(as.Date(., format = "%Y-%m-%d"))) %>%
  mutate_at(vars(ph_rent_ceiling, tb_rent_ceiling, disability),
            funs(as.numeric(car::recode(., "'Y' = 1; 'N' = 0; '0' = 0; 'N/A' = NA; 'NULL' = NA; else = NA"))))



### Make variable to track where data came from
sha <- mutate(sha, agency_new = "SHA")
kcha_long <- mutate(kcha_long, agency_new = "KCHA")


### Append data
pha <- bind_rows(kcha_long, sha)


# Keep just the variables used to make the longitudinal record and reorder them
pha <- select(pha, agency, agency_new, subsidy_id, hhold_id, incasset_id, cert_id, mbr_num, mbr_id, relcode, prog_type, 
              act_date, act_type, admit_date, reexam_date, correction, correction_date,
              lname, lnamesuf, fname, mname, dob, gender, disability, race, r_white:r_hisp, ssn, 
              property_id, unit_id, unit_add:unit_zip, hhold_size, hh_id, hh_lname, hh_lnamesuf, hh_fname, hh_mname, hh_ssn, hh_dob,
              subs_type, portability, cost_pha, proj_type, proj_name, purpose)


# Remove any duplicates
pha <- pha %>% distinct()


##### Clean up data #####
## Fix relative code
pha <- pha %>%
  mutate(relcode = str_trim(relcode),
         relcode = ifelse(relcode == "NULL" | is.na(relcode) | relcode == "", "", relcode))


### Reformat variables
pha <- pha %>%
  # Address fields have lots of white space
  mutate_at(vars(unit_add, unit_apt, unit_apt2, unit_city, unit_state), funs(str_trim(.))) %>%
  # Some SSNs are HUD/PHA-generated IDs so make two fields
  # (note that conversion of legitimate SSN to numeric strips out leading zeros and removes rows with characters)
  # Remove dashes first
  mutate_at(vars(ssn, hh_ssn), funs(str_replace_all(., "-", ""))) %>%
  mutate(ssn_c = ifelse(str_detect(ssn, "[:alpha:]"), ssn, ""),
         hh_ssn_c = ifelse(str_detect(hh_ssn, "[:alpha:]"), hh_ssn, "")) %>%
  mutate_at(vars(ssn, hh_ssn), funs(new = round(as.numeric(.), digits = 0)))


### Billed agency (just fix up KCHA and other common ones)
pha <- pha %>%
  mutate(cost_pha = ifelse(cost_pha %in% c("WAOO2", "WA02 ", " WA02"), "WA002", cost_pha),
         cost_pha = ifelse(cost_pha == " WA03", "WA003", cost_pha))

### Dates
# Strip out dob components for matching
pha <- pha %>%
  mutate(dob_y = as.numeric(year(dob)),
         dob_mth = as.numeric(month(dob)),
         dob_d = as.numeric(day(dob)))


# Find most common DOB by SSN (doesn't work for SSN = NA or 0)
# Need to figure out how to ID most common or most recent last name for SSNs like 0 or NA
pha <- pha %>%
  group_by(ssn_new, ssn_c, dob) %>%
  mutate(dob_cnt = ifelse((ssn_new > 1000000 & ssn_new < 900000000 & ssn_new != 99999999 & ssn_new != 123456789 & 
                             ssn_new != 111111111 & ssn_new != 333333333 & ssn_new != 555555555 & ssn_new != 888888888 & 
                             str_sub(as.character(ssn_new), -4, -1) != "0000" & 
                             str_detect(str_sub(as.character(ssn_new), 1, 3), paste(c("000", "666"), collapse="|")) == FALSE) | 
                            (ssn_c != "" & ssn_c != "XXX-XX-XXXX" & ssn_c != "XXXXXXXXX" & ssn_c != "A00-00-0000"),
                          n(),
                          NA)) %>%
  ungroup()


### Names overall
# Change names to be consistently upper case
pha <- pha %>%
  mutate_at(vars(ends_with("name"), ends_with("suf")), funs(toupper))

# Strip out any white space around names
pha <- pha %>%
  mutate_at(vars(ends_with("name"), ends_with("suf")), funs(str_trim(.)))

# Strip out multiple spaces in the middle of last names, fix inconsistent apostrophes,
#   remove extraneous commas, and other general fixes
pha <- pha %>%
  mutate_at(vars(ends_with("name"), ends_with("suf")), funs(str_replace(., "[:space:]{2,}", " "))) %>%
  mutate_at(vars(ends_with("name"), ends_with("suf")), funs(str_replace(., "`", "'"))) %>%
  mutate_at(vars(ends_with("name"), ends_with("suf")), funs(str_replace(., ", ", " "))) %>%
  mutate_at(vars(ends_with("name"), ends_with("suf")), funs(str_replace(., "_", "-")))


### First name
# Clean up where middle initial seems to be in first name field
# NOTE: There are many rows with a middle initial in the fname field AND the mname field
# (and the initials are not always the same)
# Need to talk with pha to determine best approach

pha <- pha %>%
  mutate(fname_new = ifelse((str_detect(str_sub(fname, -2, -1), "[:space:][A-Z]") == TRUE | 
                               str_detect(str_sub(fname, -3, -1), "[:space:][A-Z]\\.") == TRUE),
                            str_sub(fname, 1, -3), fname),
         mname_new = ifelse((str_detect(str_sub(fname, -2, -1), "[:space:][A-Z]") == TRUE |
                               str_detect(str_sub(fname, -3, -1), "[:space:][A-Z]\\.") == TRUE) &
                              is.na(mname),
                            str_sub(fname, -1), 
                            ifelse((str_detect(str_sub(fname, -2, -1), "[:space:][A-Z]") == TRUE |
                                      str_detect(str_sub(fname, -3, -1), "[:space:][A-Z]\\.") == TRUE) &
                                     !is.na(mname),
                                   paste(str_sub(fname, -1), mname, sep = " "),
                                   mname)))


### Last name suffix
# Need to look for suffixes in last name if wanting to merge with SHA data
pha <- pha %>%
  # Suffixes in last names
  mutate(lnamesuf_new = ifelse(str_detect(str_sub(lname, -3, -1), paste(c(" JR", " SR"," II", " IV"),
                                                                        collapse="|")) == TRUE,
                               str_sub(lname, -2, -1), ""),
         lname_new = ifelse(str_detect(str_sub(lname, -3, -1), paste(c(" JR", " SR", " II", " IV"),
                                                                     collapse="|")) == TRUE,
                            str_sub(lname, 1, -4), lname),
         lnamesuf_new = ifelse(str_detect(str_sub(lname, -4, -1), paste(c(" III", " LLL", " JR.", " SR.", " 2ND",
                                                                          " II\"", " II\\.", " IV\\."),
                                                                        collapse = "|")) == TRUE,
                               str_sub(lname, -3, -1), lnamesuf_new),
         lname_new = ifelse(str_detect(str_sub(lname, -4, -1), paste(c(" III", " LLL", " JR.", " SR.", " 2ND",
                                                                       " II\"", " II\\.", " IV\\."),
                                                                     collapse = "|")) == TRUE,
                            str_sub(lname, 1, -5), lname_new),
         
         # Suffixes in first names
         lnamesuf_new = ifelse(str_detect(str_sub(fname_new, -3, -1), paste(c(" JR", " SR", " II", " IV"),
                                                                            collapse="|")) == TRUE,
                               str_sub(fname_new, -2, -1), lnamesuf_new),
         fname_new = ifelse(str_detect(str_sub(fname_new, -3, -1), paste(c(" JR", " SR", " II", " IV"),
                                                                         collapse="|")) == TRUE,
                            str_sub(fname_new, 1, -4), fname_new),
         lnamesuf_new = ifelse(str_detect(str_sub(fname_new, -4, -1), paste(c(" JR.", " III", " SR."),
                                                                            collapse="|")) == TRUE,
                               str_sub(fname_new, -3, -1), lnamesuf_new),
         fname_new = ifelse(str_detect(str_sub(fname_new, -4, -1), paste(c(" JR\\.", " III", " SR\\."),
                                                                         collapse="|")) == TRUE,
                            str_sub(fname_new, 1, -5), fname_new),
         
         # Remove any punctuation
         lnamesuf_new = str_replace_all(lnamesuf_new, pattern = "[:punct:]|[:blank:]", replacement = "")
  )



#### Set up variables for matching ####
### Sort records
pha <- arrange(pha, hh_ssn, ssn, act_date)

### Make trimmed version of names
pha <- pha %>%
  mutate(
    # Trim puncutation
    lname_trim = str_replace_all(lname_new, pattern = "[:punct:]|[:digit:]|[:blank:]|`", replacement = ""),
    fname_trim = str_replace_all(fname_new, pattern = "[:punct:]|[:digit:]|[:blank:]|`", replacement = ""),
    # Make soundex versions of names for matching/grouping
    lname_phon = soundex(lname_trim),
    fname_phon = soundex(fname_trim),
    # Make truncated first and last names for matching/grouping (alternative to soundex)
    lname3 = substr(lname_trim, 1, 3),
    fname3 = substr(fname_trim, 1, 3)
  )


### Count which first names appear most often (doesn't work for SSN = NA or 0)
# Need to figure out how to ID most common or most recent last name for SSNs like 0 or NA
pha <- pha %>%
  group_by(ssn_new, ssn_c, fname_new) %>%
  mutate(fname_new_cnt = ifelse((ssn_new > 1000000 & ssn_new < 900000000 & ssn_new != 99999999 & ssn_new != 123456789 & 
                                   ssn_new != 111111111 & ssn_new != 333333333 & ssn_new != 555555555 & ssn_new != 888888888 & 
                                   str_sub(as.character(ssn_new), -4, -1) != "0000" & 
                                   str_detect(str_sub(as.character(ssn_new), 1, 3), paste(c("000", "666"), collapse="|")) == FALSE) | 
                                  (ssn_c != "" & ssn_c != "XXX-XX-XXXX" & ssn_c != "XXXXXXXXX" & ssn_c != "A00-00-0000"),
                                n(),
                                NA)) %>%
  ungroup()


### Count which non-blank middle names appear most often (doesn't work for SSN = NA or 0)
# Need to figure out how to ID most common or most recent last name for SSNs like 0 or NA
pha_middle <- pha %>%
  filter(mname_new != "" & 
           ((ssn_new > 1000000 & ssn_new < 900000000 & ssn_new != 99999999 & ssn_new != 123456789 & 
               ssn_new != 111111111 & ssn_new != 333333333 & ssn_new != 555555555 & ssn_new != 888888888 & 
               str_sub(as.character(ssn_new), -4, -1) != "0000" & 
               str_detect(str_sub(as.character(ssn_new), 1, 3), paste(c("000", "666"), collapse="|")) == FALSE) |
              (ssn_c != "" & ssn_c != "XXX-XX-XXXX" & ssn_c != "XXXXXXXXX" & ssn_c != "A00-00-0000"))) %>%
  group_by(ssn_new, ssn_c, mname_new) %>%
  summarise(mname_new_cnt = n()) %>%
  ungroup()

pha <- left_join(pha, pha_middle, by = c("ssn_new", "ssn_c", "mname_new"))
rm(pha_middle)


### Last name
# Find the most recent surname used (doesn't work for SSN = NA or 0)
# Need to figure out how to ID most common or most recent last name for SSNs like 0 or NA
pha <- pha %>%
  arrange(ssn_new, ssn_c, desc(act_date)) %>%
  group_by(ssn_new, ssn_c) %>%
  mutate(lname_rec = ifelse((ssn_new > 1000000 & ssn_new < 900000000 & ssn_new != 99999999 & ssn_new != 123456789 & 
                               ssn_new != 111111111 & ssn_new != 333333333 & ssn_new != 555555555 & ssn_new != 888888888 & 
                               str_sub(as.character(ssn_new), -4, -1) != "0000" & 
                               str_detect(str_sub(as.character(ssn_new), 1, 3), paste(c("000", "666"), collapse="|")) == FALSE) | 
                              (ssn_c != "" & ssn_c != "XXX-XX-XXXX" & ssn_c != "XXXXXXXXX" & ssn_c != "A00-00-0000"),
                            first(lname_new),
                            NA)) %>%
  ungroup()


### Count which non-blank last name suffixes appear most often (doesn't work for SSN = NA or 0)
# Need to figure out how to ID most common or most recent last name for SSNs like 0 or NA
pha_suffix <- pha %>%
  filter(lnamesuf_new != "" & 
           ((ssn_new > 1000000 & ssn_new < 900000000 & ssn_new != 99999999 & ssn_new != 123456789 & 
               ssn_new != 111111111 & ssn_new != 333333333 & ssn_new != 555555555 & ssn_new != 888888888 & 
               str_sub(as.character(ssn_new), -4, -1) != "0000" & 
               str_detect(str_sub(as.character(ssn_new), 1, 3), paste(c("000", "666"), collapse="|")) == FALSE) |
              (ssn_c != "" & ssn_c != "XXX-XX-XXXX" & ssn_c != "XXXXXXXXX" & ssn_c != "A00-00-0000"))) %>%
  group_by(ssn_new, ssn_c, lnamesuf_new) %>%
  summarise(lnamesuf_new_cnt = n()) %>%
  ungroup()

pha <- left_join(pha, pha_suffix, by = c("ssn_new", "ssn_c", "lnamesuf_new"))
rm(pha_suffix)


### Gender
# Count the number of genders recorded for an individual (doesn't work for SSN = NA or 0)
# Need to figure out how to ID most common or most recent last name for SSNs like 0 or NA
pha <- pha %>%
  mutate(gender_new = as.numeric(car::recode(gender, c("'F' = 1; 'female' = 1; 'Female' = 1; 
                                                       'M' = 2; 'male  ' = 2; 'Male  ' = 2; 
                                                       'NULL' = NA; else = NA")))) %>%
  group_by(ssn_new, ssn_c, gender_new) %>%
  mutate(gender_new_cnt = ifelse((ssn_new > 1000000 & ssn_new < 900000000 & ssn_new != 99999999 & ssn_new != 123456789 & 
                                    ssn_new != 111111111 & ssn_new != 333333333 & ssn_new != 555555555 & ssn_new != 888888888 & 
                                    str_sub(as.character(ssn_new), -4, -1) != "0000" & 
                                    str_detect(str_sub(as.character(ssn_new), 1, 3), paste(c("000", "666"), collapse="|")) == FALSE) | 
                                   (ssn_c != "" & ssn_c != "XXX-XX-XXXX" & ssn_c != "XXXXXXXXX" & ssn_c != "A00-00-0000"),
                                 n(),
                                 NA)) %>%
  ungroup()



### Remove duplicate records in preparation for matching
pha_dedup <- pha %>%
  select(ssn_new, ssn_c, lname_new, fname_new:lnamesuf_new, lname_trim:fname_phon, lname_rec, fname_new_cnt, mname_new_cnt, lnamesuf_new_cnt,
         dob, dob_y:dob_cnt, gender_new, gender_new_cnt) %>%
  distinct(ssn_new, ssn_c, lname_new, lnamesuf_new, fname_new, mname_new, lname_rec, fname_new_cnt, mname_new_cnt,
           lnamesuf_new_cnt, dob, dob_y, dob_mth, dob_d, dob_cnt, gender_new, gender_new_cnt,
           .keep_all = TRUE)


##### Matching protocol #####
# 01) Block on SSN, soundex lname, and DOB year; match fname, mname, lname suffix, gender, and other DOB elements
# 02) Repeat match 01 but block on HUD ID instead of SSN
# 03) Repeat match 01 with relaxed last name match to capture people with spelling variations
# 04) Repeat match 02 with relaxed last name match to capture people with spelling variations
# 05) Block on soundex lname, soundex fname, and DOB; match SSN, mname, gender, and lname suffix
# 06) Block on soundex lname, soundex fname, and DOB; match combined SSN/HUD ID, mname, gender, and lname suffix

##### Match #01 - block on SSN, soundex lname, and DOB year; match fname, mname, lname suffix, gender, and other DOB elements #####
match1 <- compare.dedup(pha_dedup, blockfld = c("ssn_new", "lname_trim", "dob_y"),
                        strcmp = c("mname_new", "dob_mth", "dob_d", "gender_new", "lnamesuf_new"),
                        phonetic = c("lname_trim", "fname_trim"), phonfun = soundex,
                        exclude = c("ssn_c", "lname_new", "lname_phon", "fname_new", "fname_phon", 
                                    "dob", "lname_rec", "fname_new_cnt", "mname_new_cnt", 
                                    "lnamesuf_new_cnt", "dob_cnt", "gender_new_cnt"))

# Using EpiLink approach
match1_tmp <- epiWeights(match1)
classify1 <- epiClassify(match1_tmp, threshold.upper = 0.49)
summary(classify1)
pairs1 <- getPairs(classify1, single.rows = FALSE)


# Fix formattings
pairs1 <- pairs1 %>%
  mutate(
    # Add ID to each pair
    pair = rep(seq(from = 1, to = nrow(.)/3), each = 3),
    dob = as.Date(dob, origin = "1970-01-01")
  ) %>%
  # Fix up formatting by removing factors
  mutate_at(vars(id, ssn_new, dob_y, dob_mth, dob_d, fname_new_cnt, mname_new_cnt,
                 lnamesuf_new_cnt, gender_new, dob_cnt, gender_new_cnt, Weight), funs(as.numeric(as.character(.)))
  ) %>%
  mutate_at(vars(ssn_c, lname_new, fname_new, mname_new, lnamesuf_new, lname_rec, lname_trim, lname_phon,
                 fname_trim, fname_phon), funs(as.character(.))
  ) %>%
  filter(!(id == "" & ssn_new == "" & Weight == "")) %>%
  # Propogate weight to both rows in a pair
  group_by(pair) %>%
  mutate(Weight = last(Weight)) %>%
  ungroup()


# Clean data based on matches and set up matches for relevant rows
pairs1_full <- pairs1 %>%
  filter(ssn_new > 1000000 & ssn_new < 900000000 & ssn_new != 99999999 & ssn_new != 123456789 & ssn_new != 111111111 & 
           ssn_new != 333333333 & ssn_new != 555555555 & ssn_new != 888888888 & str_sub(as.character(ssn_new), -4, -1) != "0000" & 
           str_detect(str_sub(as.character(ssn_new), 1, 3), paste(c("000", "666"), collapse="|")) == FALSE) %>%
  group_by(pair) %>%
  mutate(
    # Use most recent name (this field should be the same throughout)
    lname_new_m1 = lname_rec,
    # Take most common first name (for ties, the first ocurrance is used)
    fname_new_m1 = fname_new[which.max(fname_new_cnt)],
    # Take most common middle name (character(0) accounts for groups with no middle name)
    # (for ties, the first ocurrance is used)
    mname_new_m1 = ifelse(identical(mname_new[which.max(mname_new_cnt)], character(0)),
                          "",
                          mname_new[which.max(mname_new_cnt)]),
    # Take most common last name suffix (character(0) accounts for groups with no suffix)
    # (for ties, the first ocurrance is used)
    lnamesuf_new_m1 = ifelse(identical(lnamesuf_new[which.max(lnamesuf_new_cnt)], character(0)),
                             "",
                             lnamesuf_new[which.max(lnamesuf_new_cnt)]),
    # Take most common gender (character(0) accounts for groups with missing genders)
    # (for ties, the first ocurrance is used)
    gender_new_m1 = ifelse(identical(gender_new[which.max(gender_new_cnt)], character(0)),
                           "",
                           gender_new[which.max(gender_new_cnt)]),
    # Take most common DOB (character(0) accounts for groups with missing DOBs)
    # (for ties, the first ocurrance is used)
    dob_m1 = as.Date(ifelse(identical(dob[which.max(dob_cnt)], character(0)),
                            "",
                            dob[which.max(dob_cnt)]), origin = "1970-01-01"),
    # Keep track of most common variables for future matches
    # If identical, the first ocurrance is used
    lname_rec_m1 = lname_rec,
    fname_new_cnt_m1 = fname_new_cnt[which.max(fname_new_cnt)],
    mname_new_cnt_m1 = ifelse(identical(mname_new_cnt[which.max(mname_new_cnt)], character(0)),
                              NA,
                              mname_new_cnt[which.max(mname_new_cnt)]),
    lnamesuf_new_cnt_m1 = ifelse(identical(lnamesuf_new_cnt[which.max(lnamesuf_new_cnt)], character(0)),
                                 NA,
                                 lnamesuf_new_cnt[which.max(lnamesuf_new_cnt)]),
    gender_new_cnt_m1 = ifelse(identical(gender_new_cnt[which.max(gender_new_cnt)], character(0)),
                               NA,
                               gender_new_cnt[which.max(gender_new_cnt)]),
    dob_cnt_m1 = ifelse(identical(dob_cnt[which.max(dob_cnt)], character(0)),
                        NA,
                        dob_cnt[which.max(dob_cnt)]),
    # Keep track of abbreviated variables for future matches
    lname_trim_m1 = lname_trim,
    lname_phon_m1 = lname_phon,
    fname_trim_m1 = fname_trim[which.max(fname_new_cnt)],
    fname_phon_m1 = fname_phon[which.max(fname_new_cnt)]
  ) %>%
  ungroup() %>%
  select(ssn_new:lnamesuf_new, gender_new, dob, lname_new_m1:fname_phon_m1) %>%
  distinct(ssn_new, ssn_c, lname_new, fname_new, mname_new, dob, gender_new, .keep_all = TRUE)


# Make cleaner data for next deduplication process
pha_complete <- left_join(pha_dedup, pairs1_full, by = c("ssn_new", "ssn_c", "lname_new", "fname_new", "mname_new", 
                                                           "lnamesuf_new", "dob", "gender_new")) %>%
  mutate(lname_new_m1 = ifelse(is.na(lname_new_m1), lname_new, lname_new_m1),
         fname_new_m1 = ifelse(is.na(fname_new_m1), fname_new, fname_new_m1),
         mname_new_m1 = ifelse(is.na(mname_new_m1), mname_new, mname_new_m1),
         lnamesuf_new_m1 = ifelse(is.na(lnamesuf_new_m1), lnamesuf_new, lnamesuf_new_m1),
         dob_m1 = as.Date(ifelse(is.na(dob_m1), dob, dob_m1), origin = "1970-01-01"),
         dob_y_m1 = as.numeric(year(dob_m1)),
         dob_mth_m1 = as.numeric(month(dob_m1)),
         dob_d_m1 = as.numeric(day(dob_m1)),
         gender_new_m1 = ifelse(is.na(gender_new_m1), gender_new, gender_new_m1),
         lname_rec_m1 = ifelse(is.na(lname_rec_m1), lname_rec, lname_rec_m1),
         fname_new_cnt_m1 = ifelse(is.na(fname_new_cnt_m1), fname_new_cnt, fname_new_cnt_m1),
         mname_new_cnt_m1 = ifelse(is.na(mname_new_cnt_m1), mname_new_cnt, mname_new_cnt_m1),
         lnamesuf_new_cnt_m1 = ifelse(is.na(lnamesuf_new_cnt_m1), lnamesuf_new_cnt, lnamesuf_new_cnt_m1),
         dob_cnt_m1 = ifelse(is.na(dob_cnt_m1), dob_cnt, dob_cnt_m1),
         gender_new_cnt_m1 = ifelse(is.na(gender_new_cnt_m1), gender_new_cnt, gender_new_cnt_m1),
         lname_trim_m1 = ifelse(is.na(lname_trim_m1), lname_trim, lname_trim_m1),
         lname_phon_m1 = ifelse(is.na(lname_phon_m1), lname_phon, lname_phon_m1),
         fname_trim_m1 = ifelse(is.na(fname_trim_m1), fname_trim, fname_trim_m1),
         fname_phon_m1 = ifelse(is.na(fname_phon_m1), fname_phon, fname_phon_m1)
  )

pha_new <- pha_complete %>% 
  select(ssn_new, ssn_c, lname_new_m1:dob_m1, dob_y_m1:dob_d_m1, lname_rec_m1:fname_phon_m1) %>%
  distinct(ssn_new, ssn_c, lname_new_m1, fname_new_m1, mname_new_m1, lnamesuf_new_m1, dob_m1, gender_new_m1, .keep_all = TRUE)


##### Match #02 - Repeat match 01 but block on HUD ID instead of SSN #####
match2 <- compare.dedup(pha_new, blockfld = c("ssn_c", "lname_trim_m1", "dob_y_m1"),
                        strcmp = c("mname_new_m1", "dob_mth_m1", "dob_d_m1", "gender_new_m1", "lnamesuf_new_m1"),
                        phonetic = c("lname_trim_m1", "fname_trim_m1"), phonfun = soundex,
                        exclude = c("ssn_new", "lname_new_m1", "lname_phon_m1", "fname_new_m1", "fname_phon_m1", 
                                    "dob_m1", "lname_rec_m1", "fname_new_cnt_m1", "mname_new_cnt_m1", 
                                    "lnamesuf_new_cnt_m1", "dob_cnt_m1", "gender_new_cnt_m1"))

# Using EpiLink approach
match2_tmp <- epiWeights(match2)
classify2 <- epiClassify(match2_tmp, threshold.upper = 0.49)
summary(classify2)
pairs2 <- getPairs(classify2, single.rows = FALSE)


pairs2 <- pairs2 %>%
  mutate(
    # Add ID to each pair
    pair = rep(seq(from = 1, to = nrow(.)/3), each = 3),
    dob_m1 = as.Date(dob_m1, origin = "1970-01-01")
  ) %>%
  # Fix up formatting by removing factors
  mutate_at(vars(id, ssn_new, dob_y_m1, dob_mth_m1, dob_d_m1, fname_new_cnt_m1, mname_new_cnt_m1,
                 lnamesuf_new_cnt_m1, gender_new_m1, dob_cnt_m1, gender_new_cnt_m1, Weight), 
            funs(as.numeric(as.character(.)))
  ) %>%
  mutate_at(vars(ssn_c, lname_new_m1, fname_new_m1, mname_new_m1, lname_rec_m1, lname_trim_m1, lname_phon_m1,
                 lnamesuf_new_m1, fname_trim_m1, fname_phon_m1), funs(as.character(.))
  ) %>%
  # Remove blank row
  filter(!(id == "" & Weight == "")) %>%
  # Propogate weight to both rows in a pair
  group_by(pair) %>%
  mutate(Weight = last(Weight)) %>%
  ungroup()


# Clean data based on matches and set up matches for relevant rows
pairs2_full <- pairs2 %>%
  filter(ssn_c != "" & ssn_c != "XXX-XX-XXXX" & ssn_c != "XXXXXXXXX" & ssn_c != "A00-00-0000") %>%
  group_by(pair) %>%
  mutate(
    # See match 1 above for details on this block of code
    lname_new_m2 = lname_rec_m1,
    fname_new_m2 = fname_new_m1[which.max(fname_new_cnt_m1)],
    mname_new_m2 = ifelse(identical(mname_new_m1[which.max(mname_new_cnt_m1)], character(0)),
                          "",
                          mname_new_m1[which.max(mname_new_cnt_m1)]),
    lnamesuf_new_m2 = ifelse(identical(lnamesuf_new_m1[which.max(lnamesuf_new_cnt_m1)], character(0)),
                             "",
                             lnamesuf_new_m1[which.max(lnamesuf_new_cnt_m1)]),
    gender_new_m2 = ifelse(identical(gender_new_m1[which.max(gender_new_cnt_m1)], character(0)),
                           "",
                           gender_new_m1[which.max(gender_new_cnt_m1)]),
    dob_m2 = as.Date(ifelse(identical(dob_m1[which.max(dob_cnt_m1)], character(0)),
                            "",
                            dob_m1[which.max(dob_cnt_m1)]), origin = "1970-01-01"),
    lname_rec_m2 = lname_rec_m1,
    fname_new_cnt_m2 = fname_new_cnt_m1[which.max(fname_new_cnt_m1)],
    mname_new_cnt_m2 = ifelse(identical(mname_new_cnt_m1[which.max(mname_new_cnt_m1)], character(0)),
                              NA,
                              mname_new_cnt_m1[which.max(mname_new_cnt_m1)]),
    lnamesuf_new_cnt_m2 = ifelse(identical(lnamesuf_new_cnt_m1[which.max(lnamesuf_new_cnt_m1)], character(0)),
                                 NA,
                                 lnamesuf_new_cnt_m1[which.max(lnamesuf_new_cnt_m1)]),
    gender_new_cnt_m2 = ifelse(identical(gender_new_cnt_m1[which.max(gender_new_cnt_m1)], character(0)),
                               NA,
                               gender_new_cnt_m1[which.max(gender_new_cnt_m1)]),
    dob_cnt_m2 = ifelse(identical(dob_cnt_m1[which.max(dob_cnt_m1)], character(0)),
                        NA,
                        dob_cnt_m1[which.max(dob_cnt_m1)]),
    lname_trim_m2 = lname_trim_m1,
    lname_phon_m2 = lname_phon_m1,
    fname_trim_m2 = fname_trim_m1[which.max(fname_new_cnt_m1)],
    fname_phon_m2 = fname_phon_m1[which.max(fname_new_cnt_m1)]
  ) %>%
  ungroup() %>%
  select(ssn_new:lnamesuf_new_m1, gender_new_m1, dob_m1, lname_new_m2:fname_phon_m2) %>%
  distinct(ssn_new, ssn_c, lname_new_m1, fname_new_m1, mname_new_m1, dob_m1, gender_new_m1, .keep_all = TRUE)


# Add to full dedup set and make cleaner data for next deduplication process
pha_complete2 <- left_join(pha_complete, pairs2_full, by = c("ssn_new", "ssn_c","lname_new_m1", "fname_new_m1",
                                                             "mname_new_m1", "lnamesuf_new_m1",
                                                             "dob_m1", "gender_new_m1")) %>%
  mutate(lname_new_m2 = ifelse(is.na(lname_new_m2), lname_new_m1, lname_new_m2),
         fname_new_m2 = ifelse(is.na(fname_new_m2), fname_new_m1, fname_new_m2),
         mname_new_m2 = ifelse(is.na(mname_new_m2), mname_new_m1, mname_new_m2),
         lnamesuf_new_m2 = ifelse(is.na(lnamesuf_new_m2), lnamesuf_new_m1, lnamesuf_new_m2),
         dob_m2 = as.Date(ifelse(is.na(dob_m2), dob_m1, dob_m2), origin = "1970-01-01"),
         dob_y_m2 = as.numeric(year(dob_m2)),
         dob_mth_m2 = as.numeric(month(dob_m2)),
         dob_d_m2 = as.numeric(day(dob_m2)),
         gender_new_m2 = ifelse(is.na(gender_new_m2), gender_new_m1, gender_new_m2),
         lname_rec_m2 = ifelse(is.na(lname_rec_m2), lname_rec_m1, lname_rec_m2),
         fname_new_cnt_m2 = ifelse(is.na(fname_new_cnt_m2), fname_new_cnt_m1, fname_new_cnt_m2),
         mname_new_cnt_m2 = ifelse(is.na(mname_new_cnt_m2), mname_new_cnt_m1, mname_new_cnt_m2),
         lnamesuf_new_cnt_m2 = ifelse(is.na(lnamesuf_new_cnt_m2), lnamesuf_new_cnt_m1, lnamesuf_new_cnt_m2),
         dob_cnt_m2 = ifelse(is.na(dob_cnt_m2), dob_cnt_m1, dob_cnt_m2),
         gender_new_cnt_m2 = ifelse(is.na(gender_new_cnt_m2), gender_new_cnt_m1, gender_new_cnt_m2),
         lname_trim_m2 = ifelse(is.na(lname_trim_m2), lname_trim_m1, lname_trim_m2),
         lname_phon_m2 = ifelse(is.na(lname_phon_m2), lname_phon_m1, lname_phon_m2),
         fname_trim_m2 = ifelse(is.na(fname_trim_m2), fname_trim_m1, fname_trim_m2),
         fname_phon_m2 = ifelse(is.na(fname_phon_m2), fname_phon_m1, fname_phon_m2)
  )

pha_new2 <- pha_complete2 %>%
  select(ssn_new, ssn_c, lname_new_m2:dob_m2, dob_y_m2:dob_d_m2, lname_rec_m2:fname_phon_m2) %>%
  distinct(ssn_new, ssn_c, lname_new_m2, fname_new_m2, mname_new_m2, lnamesuf_new_m2, dob_m2, gender_new_m2, .keep_all = TRUE)



##### Match #03 - repeat match 01 with relaxed last name match to capture people with spelling variations #####
# (i.e., block on SSN, and DOB year; match soundex lname, fname, mname, and other DOB elements) #
match3 <- compare.dedup(pha_new2, blockfld = c("ssn_new", "dob_y_m2"),
                        strcmp = c("mname_new_m2", "dob_mth_m2", "dob_d_m2", "gender_new_m2", "lnamesuf_new_m2"),
                        phonetic = c("lname_trim_m2", "fname_trim_m2"), phonfun = soundex,
                        exclude = c("ssn_c", "lname_new_m2", "lname_phon_m2", "fname_new_m2", "fname_phon_m2", 
                                    "dob_m2", "lname_rec_m2", "fname_new_cnt_m2", "mname_new_cnt_m2", 
                                    "lnamesuf_new_cnt_m2", "dob_cnt_m2", "gender_new_cnt_m2"))


# Using EpiLink approach
match3_tmp <- epiWeights(match3)
classify3 <- epiClassify(match3_tmp, threshold.upper = 0.38)
summary(classify3)
pairs3 <- getPairs(classify3, single.rows = FALSE)


# Fix formattings
pairs3 <- pairs3 %>%
  mutate(
    # Add ID to each pair
    pair = rep(seq(from = 1, to = nrow(.)/3), each = 3),
    dob_m2 = as.Date(dob_m2, origin = "1970-01-01")
  ) %>%
  # Fix up formatting by removing factors
  mutate_at(vars(id, ssn_new, dob_y_m2, dob_mth_m2, dob_d_m2, fname_new_cnt_m2, mname_new_cnt_m2,
                 lnamesuf_new_cnt_m2, gender_new_m2, dob_cnt_m2, gender_new_cnt_m2, Weight), 
            funs(as.numeric(as.character(.)))
  ) %>%
  mutate_at(vars(ssn_c, lname_new_m2, fname_new_m2, mname_new_m2, lname_rec_m2, lname_trim_m2, lname_phon_m2,
                 lnamesuf_new_m2, fname_trim_m2, fname_phon_m2), funs(as.character(.))
  ) %>%
  filter(!(id == "" & ssn_new == "" & Weight == "")) %>%
  # Propogate weight to both rows in a pair
  group_by(pair) %>%
  mutate(Weight = last(Weight)) %>%
  ungroup()


# Clean data based on matches and set up matches for relevant rows
pairs3_full <- pairs3 %>%
  filter(ssn_new > 1000000 & ssn_new < 900000000 & ssn_new != 99999999 & ssn_new != 123456789 & ssn_new != 111111111 & 
           ssn_new != 333333333 & ssn_new != 555555555 & ssn_new != 888888888 & str_sub(as.character(ssn_new), -4, -1) != "0000" & 
           str_detect(str_sub(as.character(ssn_new), 1, 3), paste(c("000", "666"), collapse="|")) == FALSE &
           Weight > 0.38) %>%
  group_by(pair) %>%
  mutate(
    # See match 1 above for details on this block of code
    lname_new_m3 = lname_rec_m2,
    fname_new_m3 = fname_new_m2[which.max(fname_new_cnt_m2)],
    mname_new_m3 = ifelse(identical(mname_new_m2[which.max(mname_new_cnt_m2)], character(0)),
                          "",
                          mname_new_m2[which.max(mname_new_cnt_m2)]),
    lnamesuf_new_m3 = ifelse(identical(lnamesuf_new_m2[which.max(lnamesuf_new_cnt_m2)], character(0)),
                             "",
                             lnamesuf_new_m2[which.max(lnamesuf_new_cnt_m2)]),
    gender_new_m3 = ifelse(identical(gender_new_m2[which.max(gender_new_cnt_m2)], character(0)),
                           "",
                           gender_new_m2[which.max(gender_new_cnt_m2)]),
    dob_m3 = as.Date(ifelse(identical(dob_m2[which.max(dob_cnt_m2)], character(0)),
                            "",
                            dob_m2[which.max(dob_cnt_m2)]), origin = "1970-01-01"),
    lname_rec_m3 = lname_rec_m2,
    fname_new_cnt_m3 = fname_new_cnt_m2[which.max(fname_new_cnt_m2)],
    mname_new_cnt_m3 = ifelse(identical(mname_new_cnt_m2[which.max(mname_new_cnt_m2)], character(0)),
                              NA,
                              mname_new_cnt_m2[which.max(mname_new_cnt_m2)]),
    lnamesuf_new_cnt_m3 = ifelse(identical(lnamesuf_new_cnt_m2[which.max(lnamesuf_new_cnt_m2)], character(0)),
                                 NA,
                                 lnamesuf_new_cnt_m2[which.max(lnamesuf_new_cnt_m2)]),
    gender_new_cnt_m3 = ifelse(identical(gender_new_cnt_m2[which.max(gender_new_cnt_m2)], character(0)),
                               NA,
                               gender_new_cnt_m2[which.max(gender_new_cnt_m2)]),
    dob_cnt_m3 = ifelse(identical(dob_cnt_m2[which.max(dob_cnt_m2)], character(0)),
                        NA,
                        dob_cnt_m2[which.max(dob_cnt_m2)]),
    lname_trim_m3 = lname_trim_m2,
    lname_phon_m3 = lname_phon_m2,
    fname_trim_m3 = fname_trim_m2[which.max(fname_new_cnt_m2)],
    fname_phon_m3 = fname_phon_m2[which.max(fname_new_cnt_m2)]
  ) %>%
  ungroup() %>%
  select(ssn_new:lnamesuf_new_m2, gender_new_m2, dob_m2, lname_new_m3:fname_phon_m3) %>%
  distinct(ssn_new, ssn_c, lname_new_m2, fname_new_m2, mname_new_m2, dob_m2, gender_new_m2, .keep_all = TRUE)


# Add to full dedup set and make cleaner data for next deduplication process
pha_complete3 <- left_join(pha_complete2, pairs3_full, by = c("ssn_new", "ssn_c", "lname_new_m2", "fname_new_m2",
                                                               "mname_new_m2", "lnamesuf_new_m2",
                                                               "dob_m2", "gender_new_m2")) %>%
  mutate(lname_new_m3 = ifelse(is.na(lname_new_m3), lname_new_m2, lname_new_m3),
         fname_new_m3 = ifelse(is.na(fname_new_m3), fname_new_m2, fname_new_m3),
         mname_new_m3 = ifelse(is.na(mname_new_m3), mname_new_m2, mname_new_m3),
         lnamesuf_new_m3 = ifelse(is.na(lnamesuf_new_m3), lnamesuf_new_m2, lnamesuf_new_m3),
         dob_m3 = as.Date(ifelse(is.na(dob_m3), dob_m2, dob_m3), origin = "1970-01-01"),
         dob_y_m3 = as.numeric(year(dob_m3)),
         dob_mth_m3 = as.numeric(month(dob_m3)),
         dob_d_m3 = as.numeric(day(dob_m3)),
         gender_new_m3 = ifelse(is.na(gender_new_m3), gender_new_m2, gender_new_m3),
         lname_rec_m3 = ifelse(is.na(lname_rec_m3), lname_rec_m2, lname_rec_m3),
         fname_new_cnt_m3 = ifelse(is.na(fname_new_cnt_m3), fname_new_cnt_m2, fname_new_cnt_m3),
         mname_new_cnt_m3 = ifelse(is.na(mname_new_cnt_m3), mname_new_cnt_m2, mname_new_cnt_m3),
         lnamesuf_new_cnt_m3 = ifelse(is.na(lnamesuf_new_cnt_m3), lnamesuf_new_cnt_m2, lnamesuf_new_cnt_m3),
         dob_cnt_m3 = ifelse(is.na(dob_cnt_m3), dob_cnt_m2, dob_cnt_m3),
         gender_new_cnt_m3 = ifelse(is.na(gender_new_cnt_m3), gender_new_cnt_m2, gender_new_cnt_m3),
         lname_trim_m3 = ifelse(is.na(lname_trim_m3), lname_trim_m2, lname_trim_m3),
         lname_phon_m3 = ifelse(is.na(lname_phon_m3), lname_phon_m2, lname_phon_m3),
         fname_trim_m3 = ifelse(is.na(fname_trim_m3), fname_trim_m2, fname_trim_m3),
         fname_phon_m3 = ifelse(is.na(fname_phon_m3), fname_phon_m2, fname_phon_m3)
  )

pha_new3 <- pha_complete3 %>%
  select(ssn_new, ssn_c, lname_new_m3:dob_m3, dob_y_m3:dob_d_m3, lname_rec_m3:fname_phon_m3) %>%
  distinct(ssn_new, ssn_c, lname_new_m3, fname_new_m3, mname_new_m3, lnamesuf_new_m3, dob_m3, gender_new_m3, .keep_all = TRUE)



##### Match #04 - repeat match 02 with relaxed last name match to capture people with spelling variations #####
# (i.e., block on HUD ID, and DOB year; match soundex lname, fname, mname, and other DOB elements) #
match4 <- compare.dedup(pha_new3, blockfld = c("ssn_c", "dob_y_m3"),
                        strcmp = c("mname_new_m3", "dob_mth_m3", "dob_d_m3", "gender_new_m3", "lnamesuf_new_m3"),
                        phonetic = c("lname_trim_m3", "fname_trim_m3"), phonfun = soundex,
                        exclude = c("ssn_new", "lname_new_m3", "lname_phon_m3", "fname_new_m3", "fname_phon_m3", 
                                    "dob_m3", "lname_rec_m3", "fname_new_cnt_m3", "mname_new_cnt_m3", 
                                    "lnamesuf_new_cnt_m3", "dob_cnt_m3", "gender_new_cnt_m3"))


# Using EpiLink approach
match4_tmp <- epiWeights(match4)
classify4 <- epiClassify(match4_tmp, threshold.upper = 0.4)
summary(classify4)
pairs4 <- getPairs(classify4, single.rows = FALSE)


# Fix formattings
pairs4 <- pairs4 %>%
  mutate(
    # Add ID to each pair
    pair = rep(seq(from = 1, to = nrow(.)/3), each = 3),
    dob_m3 = as.Date(dob_m3, origin = "1970-01-01")
  ) %>%
  # Fix up formatting by removing factors
  mutate_at(vars(id, ssn_new, dob_y_m3, dob_mth_m3, dob_d_m3, fname_new_cnt_m3, mname_new_cnt_m3,
                 lnamesuf_new_cnt_m3, gender_new_m3, dob_cnt_m3, gender_new_cnt_m3, Weight), 
            funs(as.numeric(as.character(.)))
  ) %>%
  mutate_at(vars(ssn_c, lname_new_m3, fname_new_m3, mname_new_m3, lname_rec_m3, lname_trim_m3, lname_phon_m3,
                 lnamesuf_new_m3, fname_trim_m3, fname_phon_m3), funs(as.character(.))
  ) %>%
  filter(!(id == "" & ssn_new == "" & Weight == "")) %>%
  # Propogate weight to both rows in a pair
  group_by(pair) %>%
  mutate(Weight = last(Weight)) %>%
  ungroup()


# Clean data based on matches and set up matches for relevant rows
pairs4_full <- pairs4 %>%
  filter(ssn_c != "" & ssn_c != "XXX-XX-XXXX" & ssn_c != "XXXXXXXXX" & ssn_c != "A00-00-0000" & Weight > 0.4) %>%
  group_by(pair) %>%
  mutate(
    # See match 1 above for details on this block of code
    lname_new_m4 = lname_rec_m3,
    fname_new_m4 = fname_new_m3[which.max(fname_new_cnt_m3)],
    mname_new_m4 = ifelse(identical(mname_new_m3[which.max(mname_new_cnt_m3)], character(0)),
                          "",
                          mname_new_m3[which.max(mname_new_cnt_m3)]),
    lnamesuf_new_m4 = ifelse(identical(lnamesuf_new_m3[which.max(lnamesuf_new_cnt_m3)], character(0)),
                             "",
                             lnamesuf_new_m3[which.max(lnamesuf_new_cnt_m3)]),
    gender_new_m4 = ifelse(identical(gender_new_m3[which.max(gender_new_cnt_m3)], character(0)),
                           "",
                           gender_new_m3[which.max(gender_new_cnt_m3)]),
    dob_m4 = as.Date(ifelse(identical(dob_m3[which.max(dob_cnt_m3)], character(0)),
                            "",
                            dob_m3[which.max(dob_cnt_m3)]), origin = "1970-01-01"),
    lname_rec_m4 = lname_rec_m3,
    fname_new_cnt_m4 = fname_new_cnt_m3[which.max(fname_new_cnt_m3)],
    mname_new_cnt_m4 = ifelse(identical(mname_new_cnt_m3[which.max(mname_new_cnt_m3)], character(0)),
                              NA,
                              mname_new_cnt_m3[which.max(mname_new_cnt_m3)]),
    lnamesuf_new_cnt_m4 = ifelse(identical(lnamesuf_new_cnt_m3[which.max(lnamesuf_new_cnt_m3)], character(0)),
                                 NA,
                                 lnamesuf_new_cnt_m3[which.max(lnamesuf_new_cnt_m3)]),
    gender_new_cnt_m4 = ifelse(identical(gender_new_cnt_m3[which.max(gender_new_cnt_m3)], character(0)),
                               NA,
                               gender_new_cnt_m3[which.max(gender_new_cnt_m3)]),
    dob_cnt_m4 = ifelse(identical(dob_cnt_m3[which.max(dob_cnt_m3)], character(0)),
                        NA,
                        dob_cnt_m3[which.max(dob_cnt_m3)]),
    lname_trim_m4 = lname_trim_m3,
    lname_phon_m4 = lname_phon_m3,
    fname_trim_m4 = fname_trim_m3[which.max(fname_new_cnt_m3)],
    fname_phon_m4 = fname_phon_m3[which.max(fname_new_cnt_m3)]
  ) %>%
  ungroup() %>%
  select(ssn_new:lnamesuf_new_m3, gender_new_m3, dob_m3, lname_new_m4:fname_phon_m4) %>%
  distinct(ssn_new, ssn_c, lname_new_m3, fname_new_m3, mname_new_m3, dob_m3, gender_new_m3, .keep_all = TRUE)


# Add to full dedup set and make cleaner data for next deduplication process
pha_complete4 <- left_join(pha_complete3, pairs4_full, by = c("ssn_new", "ssn_c", "lname_new_m3", "fname_new_m3",
                                                              "mname_new_m3", "lnamesuf_new_m3",
                                                              "dob_m3", "gender_new_m3")) %>%
  mutate(lname_new_m4 = ifelse(is.na(lname_new_m4), lname_new_m3, lname_new_m4),
         fname_new_m4 = ifelse(is.na(fname_new_m4), fname_new_m3, fname_new_m4),
         mname_new_m4 = ifelse(is.na(mname_new_m4), mname_new_m3, mname_new_m4),
         lnamesuf_new_m4 = ifelse(is.na(lnamesuf_new_m4), lnamesuf_new_m3, lnamesuf_new_m4),
         dob_m4 = as.Date(ifelse(is.na(dob_m4), dob_m3, dob_m4), origin = "1970-01-01"),
         dob_y_m4 = as.numeric(year(dob_m4)),
         dob_mth_m4 = as.numeric(month(dob_m4)),
         dob_d_m4 = as.numeric(day(dob_m4)),
         gender_new_m4 = ifelse(is.na(gender_new_m4), gender_new_m3, gender_new_m4),
         lname_rec_m4 = ifelse(is.na(lname_rec_m4), lname_rec_m3, lname_rec_m4),
         fname_new_cnt_m4 = ifelse(is.na(fname_new_cnt_m4), fname_new_cnt_m3, fname_new_cnt_m4),
         mname_new_cnt_m4 = ifelse(is.na(mname_new_cnt_m4), mname_new_cnt_m3, mname_new_cnt_m4),
         lnamesuf_new_cnt_m4 = ifelse(is.na(lnamesuf_new_cnt_m4), lnamesuf_new_cnt_m3, lnamesuf_new_cnt_m4),
         dob_cnt_m4 = ifelse(is.na(dob_cnt_m4), dob_cnt_m3, dob_cnt_m4),
         gender_new_cnt_m4 = ifelse(is.na(gender_new_cnt_m4), gender_new_cnt_m3, gender_new_cnt_m4),
         lname_trim_m4 = ifelse(is.na(lname_trim_m4), lname_trim_m3, lname_trim_m4),
         lname_phon_m4 = ifelse(is.na(lname_phon_m4), lname_phon_m3, lname_phon_m4),
         fname_trim_m4 = ifelse(is.na(fname_trim_m4), fname_trim_m3, fname_trim_m4),
         fname_phon_m4 = ifelse(is.na(fname_phon_m4), fname_phon_m3, fname_phon_m4)
  )

pha_new4 <- pha_complete4 %>%
  select(ssn_new, ssn_c, lname_new_m4:dob_m4, dob_y_m4:dob_d_m4, lname_rec_m4:fname_phon_m4) %>%
  distinct(ssn_new, ssn_c, lname_new_m4, fname_new_m4, mname_new_m4, lnamesuf_new_m4, dob_m4, gender_new_m4, .keep_all = TRUE)


##### Match #05 - block on soundex lname, soundex fname, and DOB; match SSN, mname, gender, and lname suffix #####
### Need to first identify which is the 'correct' SSN
# For non-junk SSNs (i.e., 9 digits that do not repeat/use consecutive numbers), take most common
# For junk SSNs, assume none are correct
# NB. This approach will contain errors because one person's mistyped SSNs will be included in another person's SSN count
# However, this error rate should be small and dwarfed by the count of the correct social
# Other errors exist because not all junk SSNs are caught here
pha_ssn <- pha %>%
  filter(ssn_new > 1000000 & ssn_new < 900000000 & ssn_new != 99999999 & ssn_new != 123456789 & ssn_new != 111111111 & 
           ssn_new != 333333333 & ssn_new != 555555555 & ssn_new != 888888888 & str_sub(as.character(ssn_new), -4, -1) != "0000" & 
           str_detect(str_sub(as.character(ssn_new), 1, 3), paste(c("000", "666"), collapse="|")) == FALSE) %>%
  group_by(ssn_new) %>%
  summarise(ssn_new_cnt = n()) %>%
  ungroup()

pha_new4 <- left_join(pha_new4, pha_ssn, by = c("ssn_new"))
rm(pha_ssn) 


match5 <- compare.dedup(pha_new4, blockfld = c("lname_trim_m4", "fname_trim_m4","dob_m4"),
                        strcmp = c("ssn_new","mname_new_m4", "gender_new_m4", "lnamesuf_new_m4"),
                        phonetic = c("lname_trim_m4", "fname_trim_m4"), phonfun = soundex,
                        exclude = c("ssn_c", "lname_new_m4", "lname_phon_m4", "fname_new_m4", 
                                    "fname_phon_m4", "dob_y_m4", "dob_mth_m4", "dob_d_m4", "lname_rec_m4", 
                                    "fname_new_cnt_m4", "mname_new_cnt_m4", "lnamesuf_new_cnt_m4", 
                                    "dob_cnt_m4", "gender_new_cnt_m4", "ssn_new_cnt"))

# Using EpiLink approach
match5_tmp <- epiWeights(match5)
classify5 <- epiClassify(match5_tmp, threshold.upper = 0.5)
summary(classify5)
pairs5 <- getPairs(classify5, single.rows = FALSE)


# Fix formattings
pairs5 <- pairs5 %>%
  mutate(
    # Add ID to each pair
    pair = rep(seq(from = 1, to = nrow(.)/3), each = 3),
    dob_m4 = as.Date(dob_m4, origin = "1970-01-01")
  ) %>%
  # Fix up formatting by removing factors
  mutate_at(vars(id, ssn_new, dob_y_m4, dob_mth_m4, dob_d_m4, fname_new_cnt_m4, mname_new_cnt_m4,
                 lnamesuf_new_cnt_m4, gender_new_m4, dob_cnt_m4, gender_new_cnt_m4, Weight), 
            funs(as.numeric(as.character(.)))
  ) %>%
  mutate_at(vars(ssn_c, lname_new_m4, fname_new_m4, mname_new_m4, lname_rec_m4, lname_trim_m4, lname_phon_m4,
                 lnamesuf_new_m4, fname_trim_m4, fname_phon_m4), funs(as.character(.))
  ) %>%
  filter(!(id == "" & Weight == "")) %>%
  # Propogate weight to both rows in a pair and find pairs where at least one SSN is non-junk
  group_by(pair) %>%
  mutate(Weight = last(Weight),
         ssn_reg = ifelse((first(ssn_new) > 1000000 & first(ssn_new) < 900000000 & first(ssn_new) != 99999999 & first(ssn_new) != 123456789 & 
                             first(ssn_new) != 111111111 & first(ssn_new) != 333333333 & first(ssn_new) != 555555555 & first(ssn_new) != 888888888 &
                             str_sub(as.character(first(ssn_new)), -4, -1) != "0000" & 
                             str_detect(str_sub(as.character(first(ssn_new)), 1, 3), paste(c("000", "666"), collapse="|")) == FALSE) |
                            (last(ssn_new) > 1000000 & last(ssn_new) < 900000000 & last(ssn_new) != 99999999 & last(ssn_new) != 123456789 & 
                               last(ssn_new) != 111111111 & last(ssn_new) != 333333333 & last(ssn_new) != 555555555 & last(ssn_new) != 888888888 &
                               str_sub(as.character(last(ssn_new)), -4, -1) != "0000" & 
                               str_detect(str_sub(as.character(last(ssn_new)), 1, 3), paste(c("000", "666"), collapse="|")) == FALSE), 1, 0)
         ) %>%
  ungroup()


### Need a method to identify twins


# Clean data based on matches and set up matches for relevant rows
pairs5_full <- pairs5 %>%
  filter((Weight >= 0.2 & !(dob_mth_m4 == 1 & dob_d_m4 == 1) & ssn_reg == 1) | 
           (Weight >= 0.8218 & (dob_mth_m4 == 1 & dob_d_m4 == 1) & ssn_reg == 1)  
  ) %>%
  group_by(pair) %>%
  mutate(
    # See match 1 above for details on this block of code (exceptions noted below)
    ssn_new_m5 = ifelse(is.na(first(ssn_new_cnt)), last(ssn_new), ifelse(is.na(last(ssn_new_cnt)), first(ssn_new),
                                                                         ssn_new[which.max(ssn_new_cnt)])),
    # Can no longer assume lname_rec is the same on both rows because of junk SSNs.
    # Now look for non-missing rows and decide what to do when both rows are non-missing
    # Currently taking the lname associated with the most common fname, taking the first row when ties occur
    lname_new_m5 = ifelse(is.na(first(lname_rec_m4)), last(lname_rec_m4), 
                          ifelse(is.na(last(lname_rec_m4)), first(lname_rec_m4),
                                 lname_rec_m4[which.max(fname_new_cnt_m4)])),
    # Now need to rule out missing counts for other name variables
    fname_new_m5 = ifelse(is.na(first(fname_new_cnt_m4)), last(fname_new_m4), ifelse(is.na(last(fname_new_cnt_m4)),
                                                                                     first(fname_new_m4),
                                                                                     fname_new_m4[which.max(fname_new_cnt_m4)])),
    mname_new_m5 = ifelse(is.na(first(mname_new_cnt_m4)), last(mname_new_m4), 
                          ifelse(is.na(last(mname_new_cnt_m4)),
                                 first(mname_new_m4), 
                                 ifelse(identical(mname_new_m4[which.max(mname_new_cnt_m4)], character(0)), "",
                                        mname_new_m4[which.max(mname_new_cnt_m4)]))),
    lnamesuf_new_m5 = ifelse(is.na(first(lnamesuf_new_cnt_m4)), last(lnamesuf_new_m4), 
                             ifelse(is.na(last(lnamesuf_new_cnt_m4)),
                                    first(lnamesuf_new_m4), 
                                    ifelse(identical(lnamesuf_new_m4[which.max(lnamesuf_new_cnt_m4)], character(0)), "",
                                           lnamesuf_new_m4[which.max(lnamesuf_new_cnt_m4)]))),
    gender_new_m5 = ifelse(is.na(first(gender_new_cnt_m4)), last(gender_new_m4), 
                           ifelse(is.na(last(gender_new_cnt_m4)),
                                  first(gender_new_m4), 
                                  ifelse(identical(gender_new_m4[which.max(gender_new_cnt_m4)], character(0)), "",
                                         gender_new_m4[which.max(gender_new_cnt_m4)]))),
    dob_m5 = as.Date(ifelse(identical(dob_m4[which.max(dob_cnt_m4)], character(0)),
                            "",
                            dob_m4[which.max(dob_cnt_m4)]), origin = "1970-01-01"),
    # Reset lname_rec to match current lname using the logic above
    lname_rec_m5 = lname_new_m5,
    fname_new_cnt_m5 = ifelse(is.na(first(fname_new_cnt_m4)), last(fname_new_cnt_m4), ifelse(is.na(last(fname_new_cnt_m4)),
                                                                                             first(fname_new_cnt_m4),
                                                                                             fname_new_cnt_m4[which.max(fname_new_cnt_m4)])),
    mname_new_cnt_m5 = ifelse(is.na(first(mname_new_cnt_m4)), last(mname_new_cnt_m4), 
                              ifelse(is.na(last(mname_new_cnt_m4)),
                                     first(mname_new_cnt_m4), 
                                     ifelse(identical(mname_new_cnt_m4[which.max(mname_new_cnt_m4)], character(0)),
                                            NA,
                                            mname_new_cnt_m4[which.max(mname_new_cnt_m4)]))),
    lnamesuf_new_cnt_m5 = ifelse(is.na(first(lnamesuf_new_cnt_m4)), last(lnamesuf_new_cnt_m4), 
                                 ifelse(is.na(last(lnamesuf_new_cnt_m4)),
                                        first(lnamesuf_new_cnt_m4), 
                                        ifelse(identical(lnamesuf_new_cnt_m4[which.max(lnamesuf_new_cnt_m4)], character(0)),
                                               NA,
                                               lnamesuf_new_cnt_m4[which.max(lnamesuf_new_cnt_m4)]))),
    gender_new_cnt_m5 = ifelse(is.na(first(gender_new_cnt_m4)), last(gender_new_cnt_m4), 
                               ifelse(is.na(last(gender_new_cnt_m4)),
                                      first(gender_new_cnt_m4), 
                                      ifelse(identical(gender_new_cnt_m4[which.max(gender_new_cnt_m4)], character(0)),
                                             NA,
                                             gender_new_cnt_m4[which.max(gender_new_cnt_m4)]))),
    dob_cnt_m5 = ifelse(identical(dob_cnt_m4[which.max(dob_cnt_m4)], character(0)),
                                NA,
                                dob_cnt_m4[which.max(dob_cnt_m4)]),
    # Easier to recreate the trim and phonetic variables than apply the logic above
    lname_trim_m5 = str_replace_all(lname_new_m5, pattern = "[:punct:]|[:digit:]|[:blank:]|`", replacement = ""),
    fname_trim_m5 = str_replace_all(fname_new_m5, pattern = "[:punct:]|[:digit:]|[:blank:]|`", replacement = ""),
    # Make soundex versions of names for matching/grouping
    lname_phon_m5 = soundex(lname_trim_m5),
    fname_phon_m5 = soundex(fname_trim_m5)
  ) %>%
  ungroup() %>%
  select(ssn_new, ssn_new_m5, ssn_c, lname_new_m4:dob_m4, lname_new_m5:fname_phon_m5) %>%
  distinct(ssn_new, ssn_c, lname_new_m4, fname_new_m4, mname_new_m4, dob_m4, gender_new_m4, .keep_all = TRUE)


# Add to full dedup set and make cleaner data for next deduplication process
pha_complete5 <- left_join(pha_complete4, pairs5_full, by = c("ssn_new", "ssn_c","lname_new_m4", "fname_new_m4",
                                                                "mname_new_m4", "lnamesuf_new_m4",
                                                                "dob_m4", "gender_new_m4")) %>%
  mutate(ssn_new_m5 = ifelse(is.na(ssn_new_m5), ssn_new, ssn_new_m5),
         lname_new_m5 = ifelse(is.na(lname_new_m5), lname_new_m4, lname_new_m5),
         fname_new_m5 = ifelse(is.na(fname_new_m5), fname_new_m4, fname_new_m5),
         mname_new_m5 = ifelse(is.na(mname_new_m5), mname_new_m4, mname_new_m5),
         lnamesuf_new_m5 = ifelse(is.na(lnamesuf_new_m5), lnamesuf_new_m4, lnamesuf_new_m5),
         dob_m5 = as.Date(ifelse(is.na(dob_m5), dob_m4, dob_m5), origin = "1970-01-01"),
         dob_y_m5 = as.numeric(year(dob_m5)),
         dob_mth_m5 = as.numeric(month(dob_m5)),
         dob_d_m5 = as.numeric(day(dob_m5)),
         gender_new_m5 = ifelse(is.na(gender_new_m5), gender_new_m4, gender_new_m5),
         lname_rec_m5 = ifelse(is.na(lname_rec_m5), lname_rec_m4, lname_rec_m5),
         fname_new_cnt_m5 = ifelse(is.na(fname_new_cnt_m5), fname_new_cnt_m4, fname_new_cnt_m5),
         mname_new_cnt_m5 = ifelse(is.na(mname_new_cnt_m5), mname_new_cnt_m4, mname_new_cnt_m5),
         lnamesuf_new_cnt_m5 = ifelse(is.na(lnamesuf_new_cnt_m5), lnamesuf_new_cnt_m4, lnamesuf_new_cnt_m5),
         dob_cnt_m5 = ifelse(is.na(dob_cnt_m5), dob_cnt_m4, dob_cnt_m5),
         gender_new_cnt_m5 = ifelse(is.na(gender_new_cnt_m5), gender_new_cnt_m4, gender_new_cnt_m5),
         lname_trim_m5 = ifelse(is.na(lname_trim_m5), lname_trim_m4, lname_trim_m5),
         lname_phon_m5 = ifelse(is.na(lname_phon_m5), lname_phon_m4, lname_phon_m5),
         fname_trim_m5 = ifelse(is.na(fname_trim_m5), fname_trim_m4, fname_trim_m5),
         fname_phon_m5 = ifelse(is.na(fname_phon_m5), fname_phon_m4, fname_phon_m5)
  )

pha_new5 <- pha_complete5 %>%
  select(ssn_new_m5, ssn_c, lname_new_m5:dob_m5, dob_y_m5:dob_d_m5, lname_rec_m5:fname_phon_m5) %>%
  distinct(ssn_new_m5, ssn_c, lname_new_m5, fname_new_m5, mname_new_m5, lnamesuf_new_m5, dob_m5, gender_new_m5, .keep_all = TRUE)




##### Match #06 - block on soundex lname, soundex fname, and DOB; match combined SSN/HUD ID, mname, gender, and lname suffix #####
# Next make combined SSN/HUD ID variable (there are no overlaps between the two initially)
pha_complete5 <- mutate(pha_complete5, ssn_id = ifelse(!is.na(ssn_new_m5) & ssn_new_m5 > 1000000 & ssn_new_m5 < 900000000 & ssn_new_m5 != 99999999 & 
                                                         ssn_new_m5 != 123456789 & ssn_new_m5 != 111111111 & ssn_new_m5 != 333333333 & 
                                                         ssn_new_m5 != 555555555 & ssn_new_m5 != 888888888 & str_sub(as.character(ssn_new_m5), -4, -1) != "0000" &
                                                         str_detect(str_sub(as.character(ssn_new_m5), 1, 3), paste(c("000", "666"), collapse="|")) == FALSE,
                                                       ssn_new_m5,
                                                       ifelse(ssn_c != "" & ssn_c != "XXX-XX-XXXX" & ssn_c != "XXXXXXXXX" & ssn_c != "A00-00-0000",
                                                              ssn_c,
                                                              NA)))

pha_new5 <- mutate(pha_new5, ssn_id = ifelse(!is.na(ssn_new_m5) & ssn_new_m5 > 1000000 & ssn_new_m5 < 900000000 & ssn_new_m5 != 99999999 & 
                                                         ssn_new_m5 != 123456789 & ssn_new_m5 != 111111111 & ssn_new_m5 != 333333333 & 
                                                         ssn_new_m5 != 555555555 & ssn_new_m5 != 888888888 & str_sub(as.character(ssn_new_m5), -4, -1) != "0000" &
                                                         str_detect(str_sub(as.character(ssn_new_m5), 1, 3), paste(c("000", "666"), collapse="|")) == FALSE,
                                                       ssn_new_m5,
                                                       ifelse(ssn_c != "" & ssn_c != "XXX-XX-XXXX" & ssn_c != "XXXXXXXXX" & ssn_c != "A00-00-0000",
                                                              ssn_c,
                                                              NA)))

### Identify which is the 'correct' SSN/ID
# Prioritize numeric SSNs over IDs
# Then take most common
pha_ssn <- pha_complete5 %>%
  filter(!is.na(ssn_id)) %>%
  group_by(ssn_id) %>%
  summarise(ssn_id_cnt = n()) %>%
  ungroup()

pha_new5 <- left_join(pha_new5, pha_ssn, by = c("ssn_id"))
rm(pha_ssn) 



match6 <- compare.dedup(pha_new5, blockfld = c("lname_trim_m5", "fname_trim_m5","dob_m5"),
                        strcmp = c("ssn_id","mname_new_m5", "gender_new_m5", "lnamesuf_new_m5"),
                        phonetic = c("lname_trim_m5", "fname_trim_m5"), phonfun = soundex,
                        exclude = c("ssn_new_m5", "ssn_c", "ssn_id_cnt", "lname_new_m5", "lname_phon_m5", "fname_new_m5", 
                                    "fname_phon_m5", "dob_y_m5", "dob_mth_m5", "dob_d_m5", "lname_rec_m5", 
                                    "fname_new_cnt_m5", "mname_new_cnt_m5", "lnamesuf_new_cnt_m5", 
                                    "dob_cnt_m5", "gender_new_cnt_m5"))


# Using EpiLink approach
match6_tmp <- epiWeights(match6)
classify6 <- epiClassify(match6_tmp, threshold.upper = 0.5)
summary(classify6)
pairs6 <- getPairs(classify6, single.rows = FALSE)


# Fix formattings
pairs6 <- pairs6 %>%
  mutate(
    # Add ID to each pair
    pair = rep(seq(from = 1, to = nrow(.)/3), each = 3),
    dob_m5 = as.Date(dob_m5, origin = "1970-01-01")
  ) %>%
  # Fix up formatting by removing factors
  mutate_at(vars(id, ssn_new_m5, ssn_id_cnt, dob_y_m5, dob_mth_m5, dob_d_m5, fname_new_cnt_m5, mname_new_cnt_m5,
                 lnamesuf_new_cnt_m5, gender_new_m5, dob_cnt_m5, gender_new_cnt_m5, Weight), 
            funs(as.numeric(as.character(.)))
  ) %>%
  mutate_at(vars(ssn_id, ssn_c, lname_new_m5, fname_new_m5, mname_new_m5, lname_rec_m5, lname_trim_m5, lname_phon_m5,
                 lnamesuf_new_m5, fname_trim_m5, fname_phon_m5), funs(as.character(.))
  ) %>%
  filter(!(id == "" & Weight == "")) %>%
  # Propogate weight to both rows in a pair and find pairs where at least one SSN is non-junk
  group_by(pair) %>%
  mutate(Weight = last(Weight)) %>%
  ungroup()


# Clean data based on matches and set up matches for relevant rows
pairs6_full <- pairs6 %>%
  filter((Weight >= 0.2 & !(dob_mth_m5 == 1 & dob_d_m5 == 1)) | 
           (Weight >= 0.8211 & (dob_mth_m5 == 1 & dob_d_m5 == 1))  
  ) %>%
  group_by(pair) %>%
  mutate(
    # See match 1 above for details on this block of code (exceptions noted below)
    ssn_id_m6 = ifelse(is.na(first(ssn_id_cnt)), last(ssn_id), ifelse(is.na(last(ssn_id_cnt)), first(ssn_id),
                                                                       ssn_id[which.max(ssn_id_cnt)])),
    # Can no longer assume lname_rec is the same on both rows because of junk SSNs.
    # Now look for non-missing rows and decide what to do when both rows are non-missing
    # Currently taking the lname associated with the most common fname, taking the first row when ties occur
    lname_new_m6 = ifelse(is.na(first(lname_rec_m5)), last(lname_rec_m5), 
                          ifelse(is.na(last(lname_rec_m5)), first(lname_rec_m5),
                                 lname_rec_m5[which.max(fname_new_cnt_m5)])),
    # Now need to rule out missing counts for other name variables
    fname_new_m6 = ifelse(is.na(first(fname_new_cnt_m5)), last(fname_new_m5), ifelse(is.na(last(fname_new_cnt_m5)),
                                                                                     first(fname_new_m5),
                                                                                     fname_new_m5[which.max(fname_new_cnt_m5)])),
    mname_new_m6 = ifelse(is.na(first(mname_new_cnt_m5)), last(mname_new_m5), 
                          ifelse(is.na(last(mname_new_cnt_m5)),
                                 first(mname_new_m5), 
                                 ifelse(identical(mname_new_m5[which.max(mname_new_cnt_m5)], character(0)), "",
                                        mname_new_m5[which.max(mname_new_cnt_m5)]))),
    lnamesuf_new_m6 = ifelse(is.na(first(lnamesuf_new_cnt_m5)), last(lnamesuf_new_m5), 
                             ifelse(is.na(last(lnamesuf_new_cnt_m5)),
                                    first(lnamesuf_new_m5), 
                                    ifelse(identical(lnamesuf_new_m5[which.max(lnamesuf_new_cnt_m5)], character(0)), "",
                                           lnamesuf_new_m5[which.max(lnamesuf_new_cnt_m5)]))),
    gender_new_m6 = ifelse(is.na(first(gender_new_cnt_m5)), last(gender_new_m5), 
                           ifelse(is.na(last(gender_new_cnt_m5)),
                                  first(gender_new_m5), 
                                  ifelse(identical(gender_new_m5[which.max(gender_new_cnt_m5)], character(0)), "",
                                         gender_new_m5[which.max(gender_new_cnt_m5)]))),
    dob_m6 = as.Date(ifelse(is.na(first(dob_cnt_m5)), last(dob_m5), 
                            ifelse(is.na(last(dob_cnt_m5)),
                                   first(dob_m5), 
                                   ifelse(identical(dob_m5[which.max(dob_cnt_m5)], character(0)), "",
                                          dob_m5[which.max(dob_cnt_m5)]))), origin = "1970-01-01"),
    # Reset lname_rec to match current lname using the logic above
    lname_rec_m6 = lname_new_m6,
    fname_new_cnt_m6 = ifelse(is.na(first(fname_new_cnt_m5)), last(fname_new_cnt_m5), ifelse(is.na(last(fname_new_cnt_m5)),
                                                                                             first(fname_new_cnt_m5),
                                                                                             fname_new_cnt_m5[which.max(fname_new_cnt_m5)])),
    mname_new_cnt_m6 = ifelse(is.na(first(mname_new_cnt_m5)), last(mname_new_cnt_m5), 
                              ifelse(is.na(last(mname_new_cnt_m5)),
                                     first(mname_new_cnt_m5), 
                                     ifelse(identical(mname_new_cnt_m5[which.max(mname_new_cnt_m5)], character(0)),
                                            NA,
                                            mname_new_cnt_m5[which.max(mname_new_cnt_m5)]))),
    lnamesuf_new_cnt_m6 = ifelse(is.na(first(lnamesuf_new_cnt_m5)), last(lnamesuf_new_cnt_m5), 
                                 ifelse(is.na(last(lnamesuf_new_cnt_m5)),
                                        first(lnamesuf_new_cnt_m5), 
                                        ifelse(identical(lnamesuf_new_cnt_m5[which.max(lnamesuf_new_cnt_m5)], character(0)),
                                               NA,
                                               lnamesuf_new_cnt_m5[which.max(lnamesuf_new_cnt_m5)]))),
    gender_new_cnt_m6 = ifelse(is.na(first(gender_new_cnt_m5)), last(gender_new_cnt_m5), 
                               ifelse(is.na(last(gender_new_cnt_m5)),
                                      first(gender_new_cnt_m5), 
                                      ifelse(identical(gender_new_cnt_m5[which.max(gender_new_cnt_m5)], character(0)),
                                             NA,
                                             gender_new_cnt_m5[which.max(gender_new_cnt_m5)]))),
    dob_cnt_m6 = ifelse(is.na(first(dob_cnt_m5)), last(dob_cnt_m5), 
                                ifelse(is.na(last(dob_cnt_m5)),
                                       first(dob_cnt_m5), 
                                       ifelse(identical(dob_cnt_m5[which.max(dob_cnt_m5)], character(0)), "",
                                              dob_cnt_m5[which.max(dob_cnt_m5)]))),
    # Easier to recreate the trim and phonetic variables than apply the logic above
    lname_trim_m6 = str_replace_all(lname_new_m6, pattern = "[:punct:]|[:digit:]|[:blank:]|`", replacement = ""),
    fname_trim_m6 = str_replace_all(fname_new_m6, pattern = "[:punct:]|[:digit:]|[:blank:]|`", replacement = ""),
    # Make soundex versions of names for matching/grouping
    lname_phon_m6 = soundex(lname_trim_m6),
    fname_phon_m6 = soundex(fname_trim_m6)
  ) %>%
  ungroup() %>%
  select(ssn_new_m5, ssn_c, ssn_id, ssn_id_m6, lname_new_m5:dob_m5, lname_new_m6:fname_phon_m6) %>%
  distinct(ssn_new_m5, ssn_c, ssn_id, lname_new_m5, fname_new_m5, mname_new_m5, dob_m5, gender_new_m5, .keep_all = TRUE)


# Add to full dedup set and make cleaner data for next deduplication process
pha_complete6 <- left_join(pha_complete5, pairs6_full, by = c("ssn_new_m5", "ssn_c", "ssn_id", "lname_new_m5", "fname_new_m5",
                                                              "mname_new_m5", "lnamesuf_new_m5", "dob_m5", "gender_new_m5")) %>%
  mutate(ssn_id_m6 = ifelse(is.na(ssn_id_m6), ssn_id, ssn_id_m6),
         lname_new_m6 = ifelse(is.na(lname_new_m6), lname_new_m5, lname_new_m6),
         fname_new_m6 = ifelse(is.na(fname_new_m6), fname_new_m5, fname_new_m6),
         mname_new_m6 = ifelse(is.na(mname_new_m6), mname_new_m5, mname_new_m6),
         lnamesuf_new_m6 = ifelse(is.na(lnamesuf_new_m6), lnamesuf_new_m5, lnamesuf_new_m6),
         dob_m6 = as.Date(ifelse(is.na(dob_m6), dob_m5, dob_m6), origin = "1970-01-01"),
         dob_y_m6 = as.numeric(year(dob_m6)),
         dob_mth_m6 = as.numeric(month(dob_m6)),
         dob_d_m6 = as.numeric(day(dob_m6)),
         gender_new_m6 = ifelse(is.na(gender_new_m6), gender_new_m5, gender_new_m6),
         lname_rec_m6 = ifelse(is.na(lname_rec_m6), lname_rec_m5, lname_rec_m6),
         fname_new_cnt_m6 = ifelse(is.na(fname_new_cnt_m6), fname_new_cnt_m5, fname_new_cnt_m6),
         mname_new_cnt_m6 = ifelse(is.na(mname_new_cnt_m6), mname_new_cnt_m5, mname_new_cnt_m6),
         lnamesuf_new_cnt_m6 = ifelse(is.na(lnamesuf_new_cnt_m6), lnamesuf_new_cnt_m5, lnamesuf_new_cnt_m6),
         dob_cnt_m6 = ifelse(is.na(dob_cnt_m6), dob_cnt_m5, dob_cnt_m6),
         gender_new_cnt_m6 = ifelse(is.na(gender_new_cnt_m6), gender_new_cnt_m5, gender_new_cnt_m6),
         lname_trim_m6 = ifelse(is.na(lname_trim_m6), lname_trim_m5, lname_trim_m6),
         lname_phon_m6 = ifelse(is.na(lname_phon_m6), lname_phon_m5, lname_phon_m6),
         fname_trim_m6 = ifelse(is.na(fname_trim_m6), fname_trim_m5, fname_trim_m6),
         fname_phon_m6 = ifelse(is.na(fname_phon_m6), fname_phon_m5, fname_phon_m6)
  )

pha_new6 <- pha_complete6 %>%
  select(ssn_id_m6, lname_new_m6:dob_m6, dob_y_m6:dob_d_m6, lname_rec_m6:fname_phon_m6) %>%
  distinct(ssn_id_m6, lname_new_m6, fname_new_m6, mname_new_m6, lnamesuf_new_m6, dob_m6, gender_new_m6, .keep_all = TRUE)



##### MERGE FINAL DEDUPLICATED DATA BACK TO ORIGINAL #####
pha_clean <- pha_complete6 %>%
  select(ssn_new:lnamesuf_new, lname_rec:gender_new_cnt, ssn_new_m5, ssn_id_m6:dob_d_m6) %>%
  right_join(., pha, by = c("ssn_new", "ssn_c", "lname_new", "lnamesuf_new", "fname_new", "mname_new", 
                             "lname_rec", "fname_new_cnt", "mname_new_cnt", "lnamesuf_new_cnt", "dob",
                             "dob_y", "dob_mth", "dob_d", "dob_cnt", "gender_new", "gender_new_cnt"))


### Carry over updated names etc. to head-of-household details
# Ideally, this will eventually happen in parallel with each cleanup step above for use in later deduplication

# Also need to set up mbr_num for SHA HCV data (doing here for now, move upstream later)
pha_clean <- pha_clean %>%
  mutate(mbr_num = ifelse(is.na(mbr_num) & agency_new == "SHA" & prog_type == "HCV" &
                            (ssn_new == hh_ssn_new | ssn_c == hh_ssn_c) &
                            lname_new == hh_lname & fname_new == hh_fname,
                          1, mbr_num))

# Group on concatenated subsidy_id, hhold_id, and cert_id for KCHA and concatenated incasset_id and hh_id for SHA
pha_clean <- pha_clean %>%
  mutate(
    # Make new household ID
    hhold_id_new = ifelse(agency_new == "KCHA", paste0(subsidy_id, hhold_id, cert_id),
                          ifelse(agency_new == "SHA", paste0(incasset_id, hh_id),
                                 "CHECK ROW"))
  )

# Set up cleaned names
pha_clean <- pha_clean %>%
  mutate(
    hh_ssn_new_m6 = ifelse(mbr_num == 1 & !is.na(ssn_new_m5), ssn_new_m5, hh_ssn_new),
    hh_ssn_c_m6 = ifelse(mbr_num == 1 & ssn_c != "", ssn_c, hh_ssn_c),
    hh_ssn_id_m6 = ifelse(mbr_num == 1 & ssn_id_m6 != "", ssn_id_m6, NA),
    hh_lname_m6 = ifelse(mbr_num == 1 & lname_new_m6 != "", lname_new_m6, hh_lname),
    hh_lnamesuf_m6 = ifelse(mbr_num == 1 & lnamesuf_new_m6 != "", lnamesuf_new_m6, hh_lnamesuf),
    hh_fname_m6 = ifelse(mbr_num == 1 & fname_new_m6 != "", fname_new_m6, hh_fname),
    hh_mname_m6 = ifelse(mbr_num == 1 & mname_new_m6 != "", mname_new_m6, hh_mname),
    hh_dob_m6 = as.Date(ifelse(mbr_num == 1 & !is.na(dob_m6), dob_m6, hh_dob), origin = "1970-01-01")
  )

# Transfer to other household members (note that the head-of-household can change even within a hhold_id so need to group by date)
pha_clean <- pha_clean %>%
  arrange(hhold_id_new, act_date, mbr_num) %>%
  group_by(hhold_id_new, act_date) %>%
  mutate(
    hh_ssn_new_m6 = first(hh_ssn_new_m6),
    hh_ssn_c_m6 = first(hh_ssn_c_m6),
    hh_ssn_id_m6 = first(hh_ssn_id_m6),
    hh_lname_m6 = first(hh_lname_m6),
    hh_lnamesuf_m6 = first(hh_lnamesuf_m6),
    hh_fname_m6 = first(hh_fname_m6),
    hh_mname_m6 = first(hh_mname_m6),
    hh_dob_m6 = first(hh_dob_m6)
    ) %>%
  ungroup()



# Trim extraneous variables
pha_clean <- pha_clean %>%
  select(ssn_new:lnamesuf_new, dob, gender_new, disability, relcode, ssn_id_m6:dob_m6, hh_id, hh_ssn_c, hh_ssn, hh_ssn_new, hh_lname:hh_mname, hh_dob, 
         hh_ssn_new_m6, hh_ssn_c_m6, hh_ssn_id_m6, hh_lname_m6:hh_dob_m6, agency, agency_new, cost_pha, proj_name, proj_type, purpose,
         hhold_id_new, subsidy_id:correction_date, race:r_hisp, property_id:unit_zip)


# Remove data frames and values made along the way
#rm(list = ls()[!ls() %in% c("pha", "pha.bk", "pha_clean", "kcha_long", "sha", "db.apde")])
rm(list = ls(pattern = "pha_new[1-5]*$"))
rm(list = ls(pattern = "pha_complete[1-5]*$"))
rm(list = ls(pattern = "pairs"))
rm(list = ls(pattern = "classify"))
rm(list = ls(pattern = "match"))
rm(list = ls(pattern = "pha_dedup"))
gc()

# Filter out test names
pha_clean <- pha_clean %>% filter(!(lname_new_m6 == "DUFUS" & fname_new_m6 == "IAM"))

# Remove any remaining duplicates
pha_clean <- pha_clean %>% distinct()


### Save point
#saveRDS(pha_clean, file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/pha_combined.Rda")
#pha_clean <- readRDS(file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/pha_combined.Rda")

##### END MATCHING/DEDUPLICATION SECTION #####


##### Check for people in both KCHA and SHA data sets
# WARNING: running this operation on a grouped data frame is very slow and memory intensive
#pha_clean <- pha_clean %>%
#  group_by(ssn_id, lname_new_m5, fname_new_m5) %>%
#  mutate(pha_cnt = n_distinct(agency_new)) %>%
#  ungroup()


##### RECODE RACE AND OTHER VARIABLES #####
#### Race ####
# Recode race variables and make numeric
# Note: Because of typos and other errors, this process will overestimate the number of people with multiple races
pha_clean <- pha_clean %>%
  mutate_at(vars(r_white:r_nhpi), funs(new = car::recode(., "'Y' = 1; 'N' = 0; 'NULL' = NA; else = NA", 
                                                         as.numeric.result = TRUE, as.factor.result = FALSE
  ))) %>%
  # Make r_hisp new for now, need to check recode eventually
  mutate(r_hisp_new = ifelse(r_hisp == 2 & !is.na(r_hisp), 0, r_hisp),
         # Propogate collapsed race code from SHA HCV data
         r_white_new = ifelse(race == 1 & !is.na(race), 1, r_white_new),
         r_black_new = ifelse(race == 2 & !is.na(race), 1, r_black_new),
         r_aian_new = ifelse(race == 3 & !is.na(race), 1, r_aian_new),
         r_asian_new = ifelse(race == 4 & !is.na(race), 1, r_asian_new),
         r_nhpi_new = ifelse(race == 5 & !is.na(race), 1, r_nhpi_new)
         )


# Identify individuals with contradictory race values and set to Y
pha_clean <- pha_clean %>%
  group_by(ssn_id_m6, lname_new_m6, fname_new_m6, dob_m6) %>%
  mutate_at(vars(r_white_new:r_hisp_new), funs(tot = sum(., na.rm = TRUE))) %>%
  ungroup() %>%
  mutate_at(vars(r_white_new_tot:r_hisp_new_tot), funs(replace(., which(. > 0), 1))) %>%
  mutate(r_white_new = ifelse(r_white_new_tot == 1, 1, 0),
         r_black_new = ifelse(r_black_new_tot == 1, 1, 0),
         r_aian_new = ifelse(r_aian_new_tot == 1, 1, 0),
         r_asian_new = ifelse(r_asian_new_tot == 1, 1, 0),
         r_nhpi_new = ifelse(r_nhpi_new_tot == 1, 1, 0),
         r_hisp_new = ifelse(r_hisp_new_tot == 1, 1, 0),
         # Find people with multiple races
         r_multi_new = rowSums(cbind(r_white_new_tot, r_black_new_tot, r_aian_new_tot, r_asian_new_tot,
                                     r_nhpi_new_tot), na.rm = TRUE),
         r_multi_new = ifelse(r_multi_new > 1, 1, 0)) %>%
  # make new variable to look at people with one race only
  mutate_at(vars(r_white_new:r_nhpi_new), funs(alone = ifelse(r_multi_new == 1, 0, .))) %>%
  # make single race variable
  mutate(race2 = ifelse(r_white_new_alone == 1, "White only",
                        ifelse(r_black_new_alone == 1, "Black only",
                               ifelse(r_aian_new_alone == 1, "AIAN only",
                                      ifelse(r_asian_new_alone == 1, "Asian only",
                                             ifelse(r_nhpi_new_alone == 1, "NHPI only",
                                                    ifelse(r_multi_new == 1, "Multiple race",
                                                           NA)))))))





##### Addresses #####
### Import Python address parser
addparser <- import("usaddress")


### Clean addresses
pha_cleanadd <- mutate_at(pha_clean, vars(unit_add, unit_apt, unit_apt2, unit_city, unit_state), funs(toupper(.)))
pha_cleanadd <- arrange(pha_cleanadd, ssn_id_m6, lname_new_m6, fname_new_m6, act_date)

# Remove written NAs and make actually missing
pha_cleanadd <- pha_cleanadd %>%
  mutate_at(vars(unit_add, unit_apt, unit_apt2, unit_city, unit_state),
            funs(ifelse(is.na(.) | . == "NULL", "", .)))


### Specific addresses
# Some addresses have specific issues than cannot be addressed via rules
# However, these specific addresses should not be shared publically
adds_specific <- read.xlsx("//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/PHA_specific_addresses_fix - DO NOT SHARE FILE.xlsx",
                           na.strings = "")
adds_specific <- mutate_all(adds_specific, funs(ifelse(is.na(.), "", .)))
# For some reason there seem to be duplicates in the address data, possible created when cleaning up missing in the line above
adds_specific <- adds_specific %>% distinct()
pha_cleanadd <- left_join(pha_cleanadd, adds_specific, by = c("unit_add", "unit_apt", "unit_apt2", "unit_city", "unit_state", "unit_zip"))


# Bring over addresses not matched (could use overidden == 0 too, could also collapse to a mutate_at statement)
pha_cleanadd <- pha_cleanadd %>%
  mutate(
    unit_add_new = ifelse(is.na(unit_add_new), unit_add, unit_add_new),
    unit_apt_new = ifelse(is.na(unit_apt_new), unit_apt, unit_apt_new),
    unit_apt2_new = ifelse(is.na(unit_apt2_new), unit_apt2, unit_apt2_new),
    unit_city_new = ifelse(is.na(unit_city_new), unit_city, unit_city_new),
    unit_state_new = ifelse(is.na(unit_state_new), unit_state, unit_state_new),
    unit_zip_new = ifelse(is.na(unit_zip_new), unit_zip, unit_zip_new)
  )


# Get rid of extra spacing in addresses and some punctuation
pha_cleanadd <- pha_cleanadd %>%
  mutate(unit_add_new = str_replace_all(unit_add_new, "\\.|,", ""),
         unit_add_new = str_replace_all(unit_add_new, "[:space:]+", " "),
         unit_apt_new = str_replace_all(unit_apt_new, ",", "")
  )

# Clean up road name in wrong field
pha_cleanadd <- pha_cleanadd %>%
  mutate(
    unit_add_new = if_else(str_detect(unit_apt_new, "^(MEM[:space:]DR|RD[:space:]SE|PKWY[:space:]SW|WAY[:space:]NE)[:space:]+") == TRUE,
                           paste(unit_add_new, str_sub(unit_apt_new, 1, 
                                                       str_locate(unit_apt_new, 
                                                                  "^(MEM[:space:]DR|RD[:space:]SE|PKWY[:space:]SW|WAY[:space:]NE)[:space:]+")[, 2] - 1),
                                 sep = " "),
                           unit_add_new),
    unit_apt_new = if_else(str_detect(unit_apt_new, "^(MEM[:space:]DR|RD[:space:]SE|PKWY[:space:]SW|WAY[:space:]NE)[:space:]+") == TRUE,
                           str_sub(unit_apt_new, 
                                   str_locate(unit_apt_new, "^(MEM[:space:]DR|RD[:space:]SE|PKWY[:space:]SW|WAY[:space:]NE)[:space:]+")[, 2],
                                   str_length(unit_apt_new)),
                           unit_apt_new),
    unit_add_new = if_else(str_detect(unit_apt_new, "^(RD|PKWY|WAY|NE|SE)[:space:]+") == TRUE,
                           paste(unit_add_new, str_sub(unit_apt_new, 1, 
                                                       str_locate(unit_apt_new, 
                                                                  "^(RD|PKWY|WAY|NE|SE)[:space:]+")[, 2] - 1),
                                 sep = " "),
                           unit_add_new),
    unit_apt_new = if_else(str_detect(unit_apt_new, "^(RD|PKWY|WAY|NE|SE)[:space:]+") == TRUE,
                           str_sub(unit_apt_new, 
                                   str_locate(unit_apt_new, "^(RD|PKWY|WAY|NE|SE)[:space:]+")[, 2],
                                   str_length(unit_apt_new)),
                           unit_apt_new)
  )


### Figure out when apartments are in wrong field
# Set up list of secondary designators
secondary <- c("#", "\\$", "APT", "APPT", "APARTMENT", "APRT", "ATPT","BOX", "BLDG", "BLD", "BLG", "BUILDING", "DUPLEX", "FL ", 
               "FLOOR", "HOUSE", "LOT", "LOWER", "LOWR", "LWR", "REAR", "RM", "ROOM", "SLIP", "STE", "SUITE", "SPACE", "SPC", "STUDIO",
               "TRAILER", "TRAILOR", "TLR", "TRL", "TRLR", "UNIT", "UPPER", "\\$")
secondary_init <- c("^#", "^\\$", "^APT", "^APPT","^APARTMENT", "^APRT", "^ATPT", "^BOX", "^BLDG", "^BLD", "^BLG", "^BUILDING", "^DUPLEX", "^FL ", 
                    "^FLOOR", "^HOUSE", "^LOT", "^LOWER", "^LOWR", "^LWR", "^REAR", "^RM", "^ROOM", "^SLIP", "^STE", "^SUITE", "^SPACE", "^SPC", 
                    "^STUDIO", "^TRAILER", "^TRAILOR", "^TLR", "^TRL", "^TRLR", "^UNIT", "^UPPER", "^\\$")

# Clean up apartments in wrong field
pha_cleanadd <- pha_cleanadd %>%
  mutate(
    # Remove straight duplicates of apt numbers in address and apt fields
    unit_add_new = if_else(unit_apt_new != "" &
                             str_sub(unit_add_new, str_length(unit_add_new) - str_length(unit_apt_new) + 1, str_length(unit_add_new)) ==
                             str_sub(unit_apt_new, 1, str_length(unit_apt_new)),
                           str_sub(unit_add_new, 1, str_length(unit_add_new) - str_length(unit_apt_new)),
                           unit_add_new),
    # Remove duplicates that are a little more complicated
    unit_add_new = if_else(str_sub(unit_apt_new, str_locate(unit_apt_new, paste0(paste(secondary, collapse = "|"), "[:space:]*"))[, 2] + 1, str_length(unit_apt_new)) ==
                             str_sub(unit_add_new, str_length(unit_add_new) - (str_length(unit_apt_new) - 
                                                                                 (str_locate(unit_apt_new, paste0(paste(secondary, collapse = "|"), "[:space:]*"))[, 2] + 1)),
                                     str_length(unit_add_new)) &
                             !str_sub(unit_add_new, str_length(unit_add_new) - 1, str_length(unit_add_new)) %in% c("LA", "N", "NE", "NW", "S", "SE", "SW"),
                           str_sub(unit_add_new, 1, str_length(unit_add_new) - (str_length(unit_apt_new) - 
                                                                                  str_locate(unit_apt_new, paste0(paste(secondary, collapse = "|"), "[:space:]*"))[, 2])),
                           unit_add_new),
    # ID apartment numbers that need to move into the appropriate column (1, 2)
    # Also include addresses that end in a number as many seem to be apartments (3, 4)
    unit_apt_move = if_else(unit_apt_new == "" & str_detect(unit_add_new, paste0("[:space:]*", paste(secondary, collapse = "|"))) == TRUE,
                            1, if_else(
                              unit_apt_new != "" & str_detect(unit_add_new, paste0("[:space:]*", paste(secondary, collapse = "|"))) == TRUE,
                              2, if_else(
                                unit_apt_new == "" & str_detect(unit_add_new, "[:space:]+[:alnum:]*[-]*[:digit:]+$") == TRUE &
                                  str_detect(unit_add_new, "PO BOX|PMB") == FALSE & str_detect(unit_add_new, "HWY 99$") == FALSE,
                                3, if_else(
                                  unit_apt_new != "" & str_detect(unit_add_new, "[:space:]+[:alnum:]*[-]*[:digit:]+$") == TRUE &
                                    str_detect(unit_add_new, "PO BOX|PMB") == FALSE & str_detect(unit_add_new, "HWY 99$") == FALSE,
                                  4, 0
                                )))),
    # Move apartment numbers to unit_apt_new if that field currently blank
    unit_apt_new = if_else(unit_apt_move == 1,
                           str_sub(unit_add_new, str_locate(unit_add_new, paste0("[:space:]*", paste(secondary, collapse = "|")))[, 1], 
                                   str_length(unit_add_new)),
                           unit_apt_new),
    unit_apt_new = if_else(unit_apt_move == 3,
                           str_sub(unit_add_new, str_locate(unit_add_new, "[:space:]+[:alnum:]*[-]*[:digit:]+$")[, 1], str_length(unit_add_new)),
                           unit_apt_new),
    # Merge apt data from unit_add_new with unit_apt_new if the latter is currently not blank
    unit_apt_new = if_else(unit_apt_move == 2,
                           paste(str_sub(unit_add_new, str_locate(unit_add_new, paste0("[:space:]*", paste(secondary, collapse = "|")))[, 1], 
                                         str_length(unit_add_new)),
                                 unit_apt_new, sep = " "),
                           unit_apt_new),
    unit_apt_new = if_else(unit_apt_move == 4 & str_detect(unit_apt, "#") == FALSE,
                           paste(str_sub(unit_add_new, str_locate(unit_add_new, "[:space:]+[:alnum:]*[-]*[:digit:]+$")[, 1], 
                                         str_length(unit_add_new)), unit_apt_new, sep = " "),
                           if_else(unit_apt_move == 4 & str_detect(unit_apt, "#") == TRUE,
                                   paste(str_sub(unit_add_new, str_locate(unit_add_new, "[:space:]+[:alnum:]*[-]*[:digit:]+$")[, 1], 
                                                 str_length(unit_add_new)), 
                                         str_sub(unit_apt_new, str_locate(unit_apt_new, "[:digit:]")[, 1], str_length(unit_apt_new)),
                                         sep = " "),
                                   unit_apt_new)),
    # Remove apt data from the address field (this needs to happen after the above code)
    unit_add_new = if_else(unit_apt_move %in% c(1, 2),
                           str_sub(unit_add_new, 1, str_locate(unit_add_new, paste0("[:space:]*", paste(secondary, collapse = "|")))[, 1] - 1),
                           unit_add_new),
    unit_add_new = if_else(unit_apt_move %in% c(3, 4),
                           str_sub(unit_add_new, 1, str_locate(unit_add_new, "[:space:]+[:alnum:]*[-]*[:digit:]+$")[, 1] - 1),
                           unit_add_new),
    # Now pull over any straggler apartments ending in a single letter or letter prefix
    unit_apt_move = if_else(str_detect(unit_add_new, "[:space:]+[A-D|F-M|O-R|T-V|X-Z][-]*[:space:]{0,1}$") == TRUE, 5, unit_apt_move),
    unit_apt_new = if_else(unit_apt_move == 5 & str_detect(unit_apt_new, "#") == FALSE,
                           paste0(str_sub(unit_add_new, 
                                          str_locate(unit_add_new, "[:space:]+[A-D|F-M|O-R|T-V|X-Z][-]*$")[, 1] + 1,
                                          str_length(unit_add_new)), 
                                  unit_apt_new),
                           unit_apt_new),
    unit_apt_new = if_else(unit_apt_move == 5 & str_detect(unit_apt_new, "#") == TRUE,
                           paste0(str_sub(unit_add_new, 
                                          str_locate(unit_add_new, "[:space:]+[A-D|F-M|O-R|T-V|X-Z][-]*[:space:]{0,1}$")[, 1] + 1,
                                          str_length(unit_add_new)), 
                                  str_sub(unit_apt_new, str_locate(unit_apt_new, "#")[, 1] + 1, str_length(unit_apt_new))),
                           unit_apt_new),
    # Remove apt data from the address field (this needs to happen after the above code)
    unit_add_new = if_else(unit_apt_move == 5,
                           str_sub(unit_add_new, 1, str_locate(unit_add_new, "[:space:]+[A-D|F-M|O-R|T-V|X-Z][-]*[:space:]{0,1}$")[, 1] - 1),
                           unit_add_new)
  ) %>%
  # Remove any whitespace generated in the process
  mutate_at(vars(unit_add_new, unit_apt_new), funs(str_trim(.))) %>%
  mutate_at(vars(unit_add_new, unit_apt_new), funs(str_replace_all(., "[:space:]+", " "))) %>%
  mutate(
    # Add in hyphens between apt numbers
    unit_apt_new = if_else(str_detect(unit_apt_new, "[:alnum:]+[:space:]+[:digit:]+$") == TRUE & 
                             str_detect(unit_apt_new, paste0("(", paste(secondary, collapse = "|"), ")")) == FALSE,
                           str_replace_all(unit_apt_new, "[:space:]", "-"), unit_apt_new),
    # Move the # to the start if in between apartment components
    unit_apt_new = if_else((str_detect(unit_apt_new, "[A-Z][:digit:]*[:space:]*#[:space:]*[:digit:]+") == TRUE &
                              str_detect(unit_apt_new, paste0("(", paste(secondary, collapse = "|"), ")")) == FALSE) | 
                             (str_detect(unit_apt_new, "[:digit:]+[:space:]*#[:space:]*[:digit:]+") == TRUE & 
                                str_detect(unit_apt_new, paste0("(", paste(secondary, collapse = "|"), ")")) == FALSE),
                           str_replace(unit_apt_new, "[:space:]*#[:space:]*", "-"), unit_apt_new),
    # Replace # to make a match more likely and more likely the parser adds a unit designator
    unit_apt_new = if_else(str_detect(unit_apt_new, paste0("(", paste(secondary, collapse = "|"), ")")) == FALSE, 
                           str_replace_all(unit_apt_new, "[#|\\$][:space:]*[-]*", "APT "),
                           str_replace(unit_apt_new, "[#|\\$]", "")),
    # Add in an APT prefix if there is no other unit designator
    unit_apt_new = if_else(str_detect(unit_apt_new, "^[:digit:]+$") == TRUE | str_detect(unit_apt_new, "^[:alnum:]$") == TRUE |
                             (str_detect(unit_apt_new, "^[:alnum:]+[-]*[:digit:]+$") == TRUE & 
                                str_detect(unit_apt_new, paste0("(", paste(secondary, collapse = "|"), ")")) == FALSE),
                           paste0("APT ", unit_apt_new), unit_apt_new),
    # Remove spaces between hyphens
    unit_apt_new = str_replace(unit_apt_new, "[:space:]-[:space:]", "-")
  )


# With apartments gone from unit_add_new, get rid of remaining extraneous punctuation
pha_cleanadd <- pha_cleanadd %>%
  mutate(unit_add_new = str_replace_all(unit_add_new, "[-]+", " "))


# Clean up street addresses
pha_cleanadd <- pha_cleanadd %>%
  mutate(
    # standardize street names
    unit_add_new = str_replace_all(unit_add_new, "[:space:]AVENUE", " AVE"),
    unit_add_new = str_replace_all(unit_add_new, "[:space:]BOULEVARD", " BLVD"),
    unit_add_new = str_replace_all(unit_add_new, "[:space:]CIRCLE", " CIR"),
    unit_add_new = str_replace_all(unit_add_new, "[:space:]COURT", " CT"),
    unit_add_new = str_replace_all(unit_add_new, "[:space:]DRIVE", " DR"),
    unit_add_new = str_replace_all(unit_add_new, "[:space:]HIGHWAY", " HWY"),
    unit_add_new = str_replace_all(unit_add_new, "[:space:]LANE", " LN"),
    unit_add_new = str_replace_all(unit_add_new, "[:space:]NORTH", " N"),
    unit_add_new = str_replace_all(unit_add_new, "[:space:]NORTH EAST", " NE"),
    unit_add_new = str_replace_all(unit_add_new, "[:space:]NORTH WEST", " NW"),
    unit_add_new = str_replace_all(unit_add_new, "[:space:]PARKWAY", " PKWY"),
    unit_add_new = str_replace_all(unit_add_new, "[:space:]PLACE", " PL"),
    unit_add_new = str_replace_all(unit_add_new, "[:space:]ROAD", " RD"),
    unit_add_new = str_replace_all(unit_add_new, "[:space:]SO[:space:]|[:space:]SO$|[:space:]SOUTH", " S"),
    unit_add_new = str_replace_all(unit_add_new, "[:space:]SOUTH EAST", " SE"),
    unit_add_new = str_replace_all(unit_add_new, "[:space:]SOUTH WEST", " SW"),
    unit_add_new = str_replace_all(unit_add_new, "[:space:]STREET", " ST"),
    unit_add_new = str_replace_all(unit_add_new, "[:space:]STST", " ST"),
    unit_add_new = str_replace_all(unit_add_new, "[:space:]WEST", " W")
  )


# Make concatenated version of address fields
pha_cleanadd <- pha_cleanadd %>%
  mutate(unit_concat = paste(unit_add_new, unit_apt_new, unit_city_new, unit_state_new, unit_zip, sep = ","))


### Set up distinct addresses for parsing (remove confidential addresses and those associate with portable)
# adds <- pha_cleanadd %>%
#   filter(unit_add_new != "0 PORTABLE" & str_detect(unit_add_new, "CONF") == FALSE) %>%
#   distinct(unit_add_new, unit_apt_new, unit_apt2_new, unit_city_new, unit_state_new, unit_zip, unit_concat) %>%
#   arrange(unit_add_new, unit_apt_new, unit_apt2_new, unit_city_new, unit_state_new, unit_zip, unit_concat)


#### Parse addresses (will be useful for geocoding) ####
# NOT CURRENTLY DOING THIS
# Make empty list to add data to
# addlist = list()
# # Loop over addresses
# for (i in 1:nrow(adds)) {
#   add <- addparser$tag(adds$unit_concat[i])[1]
#   addlist[[i]] <- toString(add[[1]])
# }



#### Consolidate address rows ####
#### Consolidate agency rows ####
### Order by date, agency, then program type
# Note: need to sort differently depending on SSN (assumes that most junk SSNs represent multiple people
# whereas a normal SSN represents one person regardless of name differences)
# Note2: currently not making the assumption above, instead grouping the same for all
# Which makes this code somewhat redundant (but it still works)
pha_cleanadd_junkssn <- pha_cleanadd %>%
  # Note that all of these numerical errors were likely picked up in deduplication #6 and so ssn_id_m6 is now NA already
  filter(is.na(ssn_id_m6) |
           as.numeric(ssn_id_m6, na.rm = TRUE) < 1000000 | as.numeric(ssn_id_m6, na.rm = TRUE) >= 900000000 | as.numeric(ssn_id_m6, na.rm = TRUE) == 99999999 | 
           as.numeric(ssn_id_m6, na.rm = TRUE) == 123456789 |  as.numeric(ssn_id_m6, na.rm = TRUE) == 111111111 | as.numeric(ssn_id_m6, na.rm = TRUE) == 333333333 | 
           as.numeric(ssn_id_m6, na.rm = TRUE) == 555555555 | as.numeric(ssn_id_m6, na.rm = TRUE) == 888888888 | 
           str_sub(ssn_id_m6, -4, -1) == "0000" | str_detect(str_sub(ssn_id_m6, 1, 3), paste(c("000", "666"), collapse="|")) == TRUE | 
           ssn_id_m6 == "" | ssn_id_m6 == "XXX-XX-XXXX" | ssn_id_m6 == "XXXXXXXXX" | ssn_id_m6 == "A00-00-0000") %>%
  arrange(ssn_id_m6, lname_new_m6, fname_new_m6, dob_m6, act_date, agency_new, prog_type) %>%
  mutate(junkssn = 1)
# Set up a unique ID for each person
pha_cleanadd_junkssn$pid <- group_indices(pha_cleanadd_junkssn, ssn_id_m6, lname_new_m6, fname_new_m6, dob_m6)

pha_cleanadd_normssn <- pha_cleanadd %>%
  filter(!is.na(ssn_id_m6) & ssn_id_m6 != "" & 
           as.numeric(ssn_id_m6, na.rm = TRUE) >= 1000000 & as.numeric(ssn_id_m6, na.rm = TRUE) < 900000000 & as.numeric(ssn_id_m6, na.rm = TRUE) != 99999999 &
           as.numeric(ssn_id_m6, na.rm = TRUE) != 123456789 & as.numeric(ssn_id_m6, na.rm = TRUE) != 111111111 & as.numeric(ssn_id_m6, na.rm = TRUE) != 333333333 &
           as.numeric(ssn_id_m6, na.rm = TRUE) != 555555555 & as.numeric(ssn_id_m6, na.rm = TRUE) != 888888888 & 
           str_sub(ssn_id_m6, -4, -1) != "0000" & str_detect(str_sub(ssn_id_m6, 1, 3), paste(c("000", "666"), collapse="|")) == FALSE | 
           (str_detect(ssn_id_m6, "[:alpha:]") & ssn_id_m6 != "XXX-XX-XXXX" & ssn_id_m6 != "XXXXXXXXX" & ssn_id_m6 != "A00-00-0000"))  %>%
  arrange(ssn_id_m6, lname_new_m6, fname_new_m6, dob_m6, act_date, agency_new, prog_type) %>%
  mutate(junkssn = 0)
# Set up a unique ID and adjust for the junk SSN unique IDs
pha_cleanadd_normssn$pid <- group_indices(pha_cleanadd_normssn, ssn_id_m6, lname_new_m6, fname_new_m6, dob_m6) + max(pha_cleanadd_junkssn$pid)


# Join back with differential sorting and remove temp dfs
pha_cleanadd_sort <- bind_rows(pha_cleanadd_junkssn, pha_cleanadd_normssn)
rm(pha_cleanadd_junkssn)
rm(pha_cleanadd_normssn)


### Save point
#saveRDS(pha_cleanadd_sort, file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/pha_cleanadd_sort.Rda")
#pha_cleanadd_sort <- readRDS(file = //phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/pha_cleanadd_sort.Rda")


### Look at dropping all rows where address data is missing (maybe if act_type == 10 or 16)
pha_cleanadd_sort <- pha_cleanadd_sort %>%
  filter(!(unit_concat %in% c(",,,,NA", ",,,,0") & act_type %in% c(10, 16)))


### Find when a person is in both KCHA and SHA data due to port ins/outs
# Seems to be that when SHA is missing address data and the action code == Port-Out Update (Not Submitted To MTCS),
# the person is in another housing authority. Check to make sure they're in KCHA data too and delete.
# Find other instances when person is in both agencies on the same date and delete the row for the agency
# not filling in the form (i.e., usually the one being billed for their port out)

repeat {
  dfsize <-  nrow(pha_cleanadd_sort)
  pha_cleanadd_sort <- pha_cleanadd_sort %>%
    mutate(drop = if_else(
      (pid == lead(pid, 1) & act_date == lead(act_date, 1) & agency_new == "KCHA" & lead(agency_new, 1) == "SHA" & 
         (unit_concat == lead(unit_concat, 1) | unit_concat == ",,,,0") & 
         lead(cost_pha, 1) %in% c("WA002", "NULL") & !is.na(lead(cost_pha, 1))) | 
        (pid == lag(pid, 1) & act_date == lag(act_date, 1) & agency_new == "SHA" & 
           lag(agency_new, 1) %in% c("KCHA", "SWHA") & 
           (unit_concat == lag(unit_concat, 1) | unit_concat == ",,,,NA") &
           lag(cost_pha, 1) == "WA001" & !is.na(lag(cost_pha, 1))),
      1, 0)) %>%
    filter(drop == 0)
  
  dfsize2 <-  nrow(pha_cleanadd_sort)
  if (dfsize2 == dfsize) {
    break
  }
}


# Get rid of blank addresses when there is an address for the same date (currently no rows)
repeat {
  dfsize <-  nrow(pha_cleanadd_sort)
  pha_cleanadd_sort <- pha_cleanadd_sort %>%
    mutate(drop = if_else(
      (pid == lead(pid, 1) & act_date == lead(act_date, 1) & agency_new == "KCHA" & lead(agency_new, 1) == "SHA" & 
         (unit_concat == lead(unit_concat, 1) | unit_concat == ",,,,0") & 
         lead(cost_pha, 1) == "WA002" & !is.na(lead(cost_pha, 1))) | 
        (pid == lag(pid, 1) & act_date == lag(act_date, 1) & agency_new == "SHA" & 
           lag(agency_new, 1) %in% c("KCHA", "SWHA") & 
           (unit_concat == lag(unit_concat, 1) | unit_concat == ",,,,NA") &
           lag(cost_pha, 1) == "WA001" & !is.na(lag(cost_pha, 1))),
      1, 0)) %>%
    filter(drop == 0)
  
  dfsize2 <-  nrow(pha_cleanadd_sort)
  if (dfsize2 == dfsize) {
    break
  }
}



### Fix up program and project type
pha_cleanadd_sort <- pha_cleanadd_sort %>%
  mutate(
    # Tidy up project name spellings
    proj_type_new = toupper(proj_type),
    proj_type_new = ifelse(str_detect(proj_type_new, "PROJECT"), "PB", ifelse(
      str_detect(proj_type_new, "TENANT[-]*[:space:]*BASED"), "TB", ifelse(
        str_detect(proj_type_new, "PORT"), "PORT", ifelse(
          str_detect(proj_type_new, "PAYMENT BUI"), "PREPAY BLDG", ifelse(
            str_detect(proj_type_new, "SPECIAL"), "SPEC", ifelse(
              str_detect(proj_type_new, "REGULAR"), "REGULAR",
              proj_type_new)))))),
    
    # Clean up KCHA programs
    prog_type_new = ifelse(prog_type == "P", "PH",
                                ifelse(prog_type == "PR", "PBS8",
                                       ifelse(prog_type == "T", "TBS8",
                                              prog_type))),
    # Now clean up SHA projects
    prog_type_new = ifelse(prog_type == "HCV" & proj_type_new == "PB" & !is.na(proj_type_new), "PBS8", ifelse(
      prog_type == "HCV" & proj_type_new == "TB" & !is.na(proj_type_new), "TBS8", ifelse(
        prog_type == "HCV" & proj_type_new == "PORT" & !is.na(proj_type_new), "PORT",
        prog_type_new
      )))
)


### Remove annual reexaminations/intermediate visits if address is the same
# Want to avoid capturing the first or last row for a person at a given address
pha_cleanadd_sort <- pha_cleanadd_sort %>%
  arrange(pid, agency_new, prog_type_new, proj_type_new, act_date) %>%
  mutate(drop = if_else(
    pid == lag(pid, 1) & pid == lead(pid, 1) & !is.na(lag(pid, 1)) & !is.na(lead(pid, 1)) &
      # Checking for prog_type and agency_new matches
      prog_type_new == lag(prog_type_new, 1) & prog_type_new == lead(prog_type_new, 1) &
         (proj_type_new == lag(proj_type_new, 1) & proj_type_new == lead(proj_type_new, 1) |
            is.na(proj_type_new) & is.na(lag(proj_type_new, 1)) & is.na(lead(proj_type_new, 1))) &
      agency_new == lag(agency_new, 1) & agency_new == lead(agency_new, 1) &
      unit_concat == lag(unit_concat, 1) & unit_concat == lead(unit_concat, 1),
    1, 0)) %>%
  filter(drop == 0 | is.na(drop))

pha_cleanadd_sort <- pha_cleanadd_sort %>%
  mutate(age = round(interval(start = dob_m6, end = act_date), 1),
         age2 = age / years(1))

### Set up the number of years between reexaminations
# 3-year gaps if head of household, co-head, or spouse are senior (62+ yrs) or disabled
# Otherwise 1-year gaps for SHA and 2-year gaps for KCHA
# Apply largest gap to entire household

# Due to missing ages/action dates, need to make separate data frame and merge back
age_temp <- pha_cleanadd_sort %>%
  distinct(pid, dob_m6, act_date, mbr_num) %>%
  filter(!(is.na(dob_m6) | is.na(act_date))) %>%
  mutate(age = round(interval(start = dob_m6, end = act_date) / years(1), 1),
         # Fix up wonky birthdates and recalculate age
         # Negative ages mostly due to incorrect century
         dob_m6 = as.Date(ifelse(age < -10, format(dob_m6, "19%y-%m-%d"), format(dob_m6)), origin = "1970-01-01"),
         # Over 100 years and < 116 years treated as genuine if head of household
         # Over 100 years unlikely to be genuine if not head of household (some are typos, most century errors)
         dob_m6 = as.Date(ifelse((age > 115 & age < 117 & mbr_num == 1) | (age > 100 & age < 117 & (mbr_num > 1 | is.na(mbr_num))) |
                                   (age >= 1000 & format(dob_m6, "%y") < 18), format(dob_m6, "20%y-%m-%d"), 
                                 ifelse(age >= 117 & age < 1000 | (age >= 1000 & format(dob_m6, "%y") > 17),  format(dob_m6, "19%y-%m-%d"),
                                        format(dob_m6))), origin = "1970-01-01"),
         age = round(interval(start = dob_m6, end = act_date) / years(1), 1),
         senior = ifelse(age >= 62, 1, 0)
  )

# Join back to main data   
pha_cleanadd_sort <- pha_cleanadd_sort %>%
  left_join(., age_temp, by = c("pid", "act_date", "mbr_num")) %>%
  # Keep new DOBs if changed
  mutate(dob_m6 = ifelse(is.na(dob_m6.y), dob_m6.x, dob_m6.y),
         add_yr_temp = ifelse(((disability == 1 & !is.na(disability)) | (senior == 1 & !is.na(senior))) &
                                relcode %in% c("H", "K", "S"), 3,
                              ifelse(agency_new == "KCHA", 2,
                                     ifelse(agency_new == "SHA", 1, NA)))
         ) %>%
  # Now apply to entire household
  group_by(hhold_id_new) %>%
  mutate(add_yr = max(add_yr_temp)) %>%
  ungroup() %>%
  select(-(add_yr_temp), -(dob_m6.x), -(dob_m6.y))

# Remove temporary data
rm(age_temp)


### Create start and end dates for a person at that address/progam/agency
# Remove missing dates
pha_cleanadd_sort <- pha_cleanadd_sort %>%
  filter(!is.na(act_date))

pha_cleanadd_sort <- pha_cleanadd_sort %>%
  arrange(pid, agency_new, prog_type_new, proj_type_new, act_date) %>%
  # First row for a person = act_date (admit_dates stretch back too far for port ins)
  # Other rows where that is the person's first row at that address = act_date
  mutate(startdate = as.Date(ifelse(pid != lag(pid, 1) |
                                      # account for first row
                                      is.na(lag(pid, 1)),
                                    act_date,
    # Treat start of different program or agency as new situation (separate from project type for easier coding)
    ifelse(agency_new != lag(agency_new, 1) | prog_type_new != lag(prog_type_new, 1),
           act_date,
           # Now look at project type
           ifelse((proj_type_new != lag(proj_type_new, 1) | (is.na(proj_type_new) & !is.na(lag(proj_type_new, 1))) |
             (!is.na(proj_type_new) & is.na(lag(proj_type_new,1)))) & !(is.na(proj_type_new) & is.na(lag(proj_type_new, 1))),
             act_date,
             # Treat a new address as a new situation
             ifelse(unit_concat != lag(unit_concat, 1), act_date,
                    NA)))),
    origin = "1970-01-01"),
    # Last row for a person = exit date or today's date or act_date + 1-3 years (depending on agency, age, and disability)
    # Other rows where that is the person's last row at that address = act_date at next address - 1 day
    # Unless act_date is the same as startdate (e.g., because of different programs), then act_date 
    enddate = as.Date(ifelse(act_type == 5 | act_type == 6, act_date,
                             ifelse(
                               pid != lead(pid, 1) |
                                 # Account for last row
                                 is.na(lead(pid, 1 )),
                               pmin(today(), act_date + dyears(add_yr), na.rm = TRUE),
                               # Treat start of different program or agency as new situation (separate from project type for easier coding)
                               ifelse(agency_new != lead(agency_new, 1) | prog_type_new != lead(prog_type_new, 1),
                                      pmin(today(), act_date + dyears(add_yr), na.rm = TRUE),
                                      # Now look at project type
                                      ifelse((proj_type_new != lead(proj_type_new, 1) | (is.na(proj_type_new) & !is.na(lead(proj_type_new, 1))) |
                                        (!is.na(proj_type_new) & is.na(lead(proj_type_new, 1)))) & !(is.na(proj_type_new) & is.na(lead(proj_type_new, 1))),
                                        pmin(today(), act_date + dyears(add_yr), na.rm = TRUE),
                                        # Treat a new address as simple date change (unless date is missing on the next row)
                                        ifelse(unit_concat != lead(unit_concat, 1) & act_date != lead(act_date, 1),
                                               lead(act_date, 1) - 1,
                                               ifelse(unit_concat != lead(unit_concat, 1) & act_date == lead(act_date, 1),
                                                      lead(act_date, 1),
                                                      NA)))))),
                      origin = "1970-01-01")
  )



### Collapse rows to have a single line per person per address per time
pha_cleanadd_sort <- pha_cleanadd_sort %>%
  # Remove rows at an address that are neither the start or end of a time there
  filter(!(is.na(startdate) & is.na(enddate))) %>%
  # Bring start and end dates onto a single line per address/program
  mutate(enddate = as.Date(ifelse(is.na(enddate), lead(enddate, 1), enddate), origin = "1970-01-01")) %>%
  filter(!(is.na(startdate)) & !(is.na(enddate)))


### Deal with overlapping program/agency dates
# assume that most recent program/agency is the one to count
pha_cleanadd_sort <- pha_cleanadd_sort %>%
  arrange(pid, startdate, enddate, agency_new) %>%
  mutate(enddate = as.Date(ifelse(pid == lead(pid, 1) & !is.na(lead(pid, 1)) & enddate >= lead(startdate, 1), 
                                  lead(startdate, 1) - 1, enddate), origin = "1970-01-01"))


### Save point
#saveRDS(pha_cleanadd_sort, file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/pha_longitudinal.Rda")
#pha_longitudinal <- readRDS(file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/pha_longitudinal.Rda")