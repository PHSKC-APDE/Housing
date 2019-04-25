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
# 04 - Deduplicate data and tidy up via matching process ### (THIS CODE) ###
# 05 - Recode race and other demographics
# 06 - Clean up addresses
# 06a - Geocode addresses
# 07 - Consolidate data rows
# 08 - Add in final data elements and set up analyses
# 09 - Join with Medicaid eligibility data
# 10 - Set up joint housing/Medicaid analyses
#
#
# Alastair Matheson (PHSKC-APDE)
# alastair.matheson@kingcounty.gov
# 2016-05-13, split into separate files 2017-10
# 
###############################################################################


#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, scipen = 999)

library(housing) # contains many useful functions for cleaning
library(tidyverse) # Used to manipulate data
library(data.table) # Used to manipulate data
library(lubridate) # used to manipulate dates
library(RecordLinkage) # used to clean up duplicates in the data
library(phonics) # used to extract phonetic version of names
library(RJSONIO)
library(RCurl)

source(file = paste0(getwd(), "/processing/metadata/set_data_env.r"))
METADATA <- RJSONIO::fromJSON(paste0(getwd(), "/processing/metadata/metadata.json"))
set_data_envr(METADATA,"combined")

#### Bring in data ####
# Assumes pha_combining.R has been run at some point
pha <- readRDS(file = paste0(housing_path, pha_fn))

### Remove duplicate records in preparation for matching
pha_dedup <- pha %>%
  select(ssn_new, ssn_c, ssn_new_junk, ssn_c_junk, lname_new, fname_new:lnamesuf_new, 
         lname_trim:fname_phon, lname_rec, fname_new_cnt:lnamesuf_new_cnt,
         dob, dob_y:dob_cnt, gender_new, gender_new_cnt) %>%
  distinct(ssn_new, ssn_c, ssn_new_junk, ssn_c_junk, lname_new, lnamesuf_new, 
           fname_new, mname_new, lname_rec, fname_new_cnt, mname_new_cnt,
           lnamesuf_new_cnt, dob, dob_y, dob_mth, dob_d, dob_cnt, gender_new, gender_new_cnt,
           .keep_all = TRUE)


##### Matching protocol #####
# 01) Block on SSN, soundex lname, and DOB year; match fname, mname, lname suffix, gender, and other DOB elements
# 02) Repeat match 01 but block on HUD ID instead of SSN
# 03) Repeat match 01 with relaxed last name match to capture people with spelling variations
# 04) Repeat match 02 with relaxed last name match to capture people with spelling variations
# 05) Block on soundex lname, soundex fname, and DOB; match SSN, mname, gender, and lname suffix
# 06) Block on soundex lname, soundex fname, and DOB; match combined SSN/HUD ID, mname, gender, and lname suffix

#### Match #01 - block on SSN, soundex lname, and DOB year; match fname, mname, lname suffix, gender, and other DOB elements ####
match1 <- compare.dedup(
  pha_dedup, blockfld = c("ssn_new", "lname_trim", "dob_y"),
  strcmp = c("mname_new", "dob_mth", "dob_d", "gender_new", "lnamesuf_new"),
  phonetic = c("lname_trim", "fname_trim"), phonfun = soundex,
  exclude = c("ssn_c", "ssn_new_junk", "ssn_c_junk", "lname_new", "lname_phon", 
              "fname_new", "fname_phon", "dob", "lname_rec", "fname_new_cnt", 
              "mname_new_cnt", "lnamesuf_new_cnt", "dob_cnt", "gender_new_cnt"))

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
  mutate_at(vars(id, ssn_new, ssn_new_junk, ssn_c_junk, dob_y, dob_mth, dob_d, 
                 fname_new_cnt, mname_new_cnt, lnamesuf_new_cnt, gender_new, 
                 gender_new_cnt, dob_cnt, Weight), 
            funs(as.numeric(as.character(.)))
  ) %>%
  mutate_at(vars(ssn_c, lname_new, fname_new, mname_new, lnamesuf_new, lname_rec, 
                 lname_trim, lname_phon, fname_trim, fname_phon), 
            funs(as.character(.))
  ) %>%
  filter(!(id == "" & ssn_new == "" & Weight == "")) %>%
  # Propogate weight to both rows in a pair
  group_by(pair) %>%
  mutate(Weight = last(Weight)) %>%
  ungroup()


# Clean data based on matches and set up matches for relevant rows
pairs1_full <- setDT(pairs1)
pairs1_full <- pairs1_full[ssn_new_junk == 0 | ssn_c_junk == 0]
pairs1_full <- pairs1_full[, ':=' (
              # Use most recent name (this field should be the same throughout)
              lname_new_m1 = lname_rec,
              # Take most common first name (for ties, the first ocurrance is used)
              fname_new_m1 = fname_new[which.max(fname_new_cnt)],
              # Take most common middle name (character(0) accounts for groups with no middle name)
              # (for ties, the first ocurrance is used)
              mname_new_m1 = ifelse(identical(mname_new[which.max(mname_new_cnt)], character(0)),
                                    "", mname_new[which.max(mname_new_cnt)]),
              # Take most common last name suffix (character(0) accounts for groups with no suffix)
              # (for ties, the first ocurrance is used)
              lnamesuf_new_m1 = ifelse(identical(lnamesuf_new[which.max(lnamesuf_new_cnt)], character(0)),
                                       "", lnamesuf_new[which.max(lnamesuf_new_cnt)]),
              # Take most common gender (character(0) accounts for groups with missing genders)
              # (for ties, the first ocurrance is used)
              gender_new_m1 = ifelse(identical(gender_new[which.max(gender_new_cnt)], character(0)),
                                     "", gender_new[which.max(gender_new_cnt)]),
              # Take most common DOB (character(0) accounts for groups with missing DOBs)
              # (for ties, the first ocurrance is used)
              dob_m1 = as.Date(ifelse(identical(dob[which.max(dob_cnt)], character(0)),
                                      "", dob[which.max(dob_cnt)]), origin = "1970-01-01"),
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
              # Keep track of other variables for future matches
              lname_trim_m1 = lname_trim,
              lname_phon_m1 = lname_phon,
              fname_trim_m1 = fname_trim[which.max(fname_new_cnt)],
              fname_phon_m1 = fname_phon[which.max(fname_new_cnt)],
              ssn_new_junk_m1 = ssn_new_junk,
              ssn_c_junk_m1 = ssn_c_junk), 
            by = "pair"]
pairs1_full <- pairs1_full[, list(ssn_new, ssn_c, ssn_new_junk, ssn_c_junk,
                                  lname_new, fname_new, mname_new, lnamesuf_new,
                                  gender_new, dob, lname_new_m1, fname_new_m1, mname_new_m1, 
                                  lnamesuf_new_m1, gender_new_m1, dob_m1, lname_rec_m1, 
                                  fname_new_cnt_m1, mname_new_cnt_m1, lnamesuf_new_cnt_m1, 
                                  gender_new_cnt_m1, dob_cnt_m1, lname_trim_m1, 
                                  lname_phon_m1, fname_trim_m1, fname_phon_m1, 
                                  ssn_new_junk_m1, ssn_c_junk_m1)]
pairs1_full <- unique(pairs1_full, 
                                  by = c("ssn_new", "ssn_c", "lname_new", 
                                         "fname_new", "mname_new", "dob", 
                                         "gender_new"))
pairs1_full <- setDF(pairs1_full)

# Make cleaner data for next deduplication process
pha_complete <- left_join(pha_dedup, pairs1_full, 
                          by = c("ssn_new", "ssn_c", "ssn_new_junk", "ssn_c_junk",
                                 "lname_new", "fname_new", "mname_new", 
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
         fname_phon_m1 = ifelse(is.na(fname_phon_m1), fname_phon, fname_phon_m1),
         ssn_new_junk_m1 = ifelse(is.na(ssn_new_junk_m1), ssn_new_junk, ssn_new_junk_m1),
         ssn_c_junk_m1 = ifelse(is.na(ssn_c_junk_m1), ssn_c_junk, ssn_c_junk_m1)
  )

pha_new <- pha_complete %>% 
  select(ssn_new, ssn_c, lname_new_m1:dob_m1, dob_y_m1:dob_d_m1, lname_rec_m1:ssn_c_junk_m1) %>%
  distinct(ssn_new, ssn_c, lname_new_m1, fname_new_m1, mname_new_m1, 
           lnamesuf_new_m1, dob_m1, gender_new_m1, .keep_all = TRUE)


#### Match #02 - Repeat match 01 but block on HUD ID instead of SSN ####
match2 <- compare.dedup(
  pha_new, blockfld = c("ssn_c", "lname_trim_m1", "dob_y_m1"),
  strcmp = c("mname_new_m1", "dob_mth_m1", "dob_d_m1", "gender_new_m1", "lnamesuf_new_m1"),
  phonetic = c("lname_trim_m1", "fname_trim_m1"), phonfun = soundex,
  exclude = c("ssn_new", "ssn_new_junk_m1", "ssn_c_junk_m1", "lname_new_m1", 
              "lname_phon_m1", "fname_new_m1", "fname_phon_m1", "dob_m1", 
              "lname_rec_m1", "fname_new_cnt_m1", "mname_new_cnt_m1", 
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
  mutate_at(vars(id, ssn_new, ssn_new_junk_m1, ssn_c_junk_m1, dob_y_m1, dob_mth_m1, 
                 dob_d_m1, fname_new_cnt_m1, mname_new_cnt_m1, lnamesuf_new_cnt_m1, 
                 gender_new_m1, dob_cnt_m1, gender_new_cnt_m1, Weight), 
            funs(as.numeric(as.character(.)))
  ) %>%
  mutate_at(vars(ssn_c, lname_new_m1, fname_new_m1, mname_new_m1, lname_rec_m1, 
                 lname_trim_m1, lname_phon_m1, lnamesuf_new_m1, fname_trim_m1, 
                 fname_phon_m1), funs(as.character(.))
  ) %>%
  # Remove blank row
  filter(!(id == "" & Weight == "")) %>%
  # Propogate weight to both rows in a pair
  group_by(pair) %>%
  mutate(Weight = last(Weight)) %>%
  ungroup()


# Clean data based on matches and set up matches for relevant rows
pairs2_full <- setDT(pairs2)
pairs2_full <- pairs2_full[ssn_c != "" & ssn_c_junk_m1 == 0]
pairs2_full[, ':=' 
            (lname_new_m2 = lname_rec_m1,
              fname_new_m2 = fname_new_m1[which.max(fname_new_cnt_m1)],
              mname_new_m2 = ifelse(identical(mname_new_m1[which.max(mname_new_cnt_m1)], character(0)),
                                    "", mname_new_m1[which.max(mname_new_cnt_m1)]),
              lnamesuf_new_m2 = ifelse(identical(lnamesuf_new_m1[which.max(lnamesuf_new_cnt_m1)], character(0)),
                                       "", lnamesuf_new_m1[which.max(lnamesuf_new_cnt_m1)]),
              gender_new_m2 = ifelse(identical(gender_new_m1[which.max(gender_new_cnt_m1)], character(0)),
                                     "", gender_new_m1[which.max(gender_new_cnt_m1)]),
              dob_m2 = as.Date(ifelse(identical(dob_m1[which.max(dob_cnt_m1)], character(0)),
                                      "", dob_m1[which.max(dob_cnt_m1)]), origin = "1970-01-01"),
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
              fname_phon_m2 = fname_phon_m1[which.max(fname_new_cnt_m1)],
              ssn_new_junk_m2 = ssn_new_junk_m1,
              ssn_c_junk_m2 = ssn_c_junk_m1),
            by = "pair"]
pairs2_full <- pairs2_full[, list(ssn_new, ssn_c, lname_new_m1, fname_new_m1, 
                                  mname_new_m1, lnamesuf_new_m1, gender_new_m1, 
                                  dob_m1, ssn_new_junk_m1, ssn_c_junk_m1, 
                                  lname_new_m2, fname_new_m2, mname_new_m2, 
                                  lnamesuf_new_m2, gender_new_m2, dob_m2, 
                                  lname_rec_m2, fname_new_cnt_m2, mname_new_cnt_m2, 
                                  lnamesuf_new_cnt_m2, gender_new_cnt_m2, 
                                  dob_cnt_m2, lname_trim_m2, lname_phon_m2, 
                                  fname_trim_m2, fname_phon_m2, ssn_new_junk_m2, 
                                  ssn_c_junk_m2)]
pairs2_full <- unique(pairs2_full,
                      by = c("ssn_new", "ssn_c", "lname_new_m1", "fname_new_m1", 
                             "mname_new_m1", "dob_m1", "gender_new_m1"))
pairs2_full <- setDF(pairs2_full)

# Add to full dedup set and make cleaner data for next deduplication process
pha_complete2 <- left_join(pha_complete, pairs2_full, 
                           by = c("ssn_new", "ssn_c", "ssn_new_junk_m1", "ssn_c_junk_m1",
                                  "lname_new_m1", "fname_new_m1", "mname_new_m1", 
                                  "lnamesuf_new_m1", "dob_m1", "gender_new_m1")) %>%
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
         fname_phon_m2 = ifelse(is.na(fname_phon_m2), fname_phon_m1, fname_phon_m2),
         ssn_new_junk_m2 = ifelse(is.na(ssn_new_junk_m2), ssn_new_junk_m1, ssn_new_junk_m2),
         ssn_c_junk_m2 = ifelse(is.na(ssn_c_junk_m2), ssn_c_junk_m1, ssn_c_junk_m2)
  )

pha_new2 <- pha_complete2 %>%
  select(ssn_new, ssn_c, lname_new_m2:dob_m2, dob_y_m2:dob_d_m2, lname_rec_m2:ssn_c_junk_m2) %>%
  distinct(ssn_new, ssn_c, lname_new_m2, fname_new_m2, mname_new_m2, lnamesuf_new_m2, dob_m2, gender_new_m2, .keep_all = TRUE)



#### Match #03 - repeat match 01 with relaxed last name match to capture people with spelling variations ####
# (i.e., block on SSN, and DOB year; match soundex lname, fname, mname, and other DOB elements) #
match3 <- compare.dedup(
  pha_new2, blockfld = c("ssn_new", "dob_y_m2"),
  strcmp = c("mname_new_m2", "dob_mth_m2", "dob_d_m2", "gender_new_m2", "lnamesuf_new_m2"),
  phonetic = c("lname_trim_m2", "fname_trim_m2"), phonfun = soundex,
  exclude = c("ssn_c", "ssn_new_junk_m2", "ssn_c_junk_m2", "lname_new_m2", 
              "lname_phon_m2", "fname_new_m2", "fname_phon_m2", "dob_m2", 
              "lname_rec_m2", "fname_new_cnt_m2", "mname_new_cnt_m2", 
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
  mutate_at(vars(id, ssn_new, ssn_new_junk_m2, ssn_c_junk_m2, dob_y_m2, dob_mth_m2, 
                 dob_d_m2, fname_new_cnt_m2, mname_new_cnt_m2, lnamesuf_new_cnt_m2, 
                 gender_new_m2, dob_cnt_m2, gender_new_cnt_m2, Weight), 
            funs(as.numeric(as.character(.)))
  ) %>%
  mutate_at(vars(ssn_c, lname_new_m2, fname_new_m2, mname_new_m2, lname_rec_m2, 
                 lname_trim_m2, lname_phon_m2, lnamesuf_new_m2, fname_trim_m2, 
                 fname_phon_m2), funs(as.character(.))
  ) %>%
  filter(!(id == "" & ssn_new == "" & Weight == "")) %>%
  # Propogate weight to both rows in a pair
  group_by(pair) %>%
  mutate(Weight = last(Weight)) %>%
  ungroup()


# Clean data based on matches and set up matches for relevant rows
pairs3_full <- setDT(pairs3)
pairs3_full <- pairs3_full[ssn_new_junk_m2 == 0 & Weight > 0.38]
pairs3_full[, ':=' 
            (lname_new_m3 = lname_rec_m2,
              fname_new_m3 = fname_new_m2[which.max(fname_new_cnt_m2)],
              mname_new_m3 = ifelse(identical(mname_new_m2[which.max(mname_new_cnt_m2)], character(0)),
                                    "", mname_new_m2[which.max(mname_new_cnt_m2)]),
              lnamesuf_new_m3 = ifelse(identical(lnamesuf_new_m2[which.max(lnamesuf_new_cnt_m2)], character(0)),
                                       "", lnamesuf_new_m2[which.max(lnamesuf_new_cnt_m2)]),
              gender_new_m3 = ifelse(identical(gender_new_m2[which.max(gender_new_cnt_m2)], character(0)),
                                     "", gender_new_m2[which.max(gender_new_cnt_m2)]),
              dob_m3 = as.Date(ifelse(identical(dob_m2[which.max(dob_cnt_m2)], character(0)),
                                      "", dob_m2[which.max(dob_cnt_m2)]), origin = "1970-01-01"),
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
              fname_phon_m3 = fname_phon_m2[which.max(fname_new_cnt_m2)],
              ssn_new_junk_m3 = ssn_new_junk_m2,
              ssn_c_junk_m3 = ssn_c_junk_m2),
            by = "pair"]
pairs3_full <- pairs3_full[, list(ssn_new, ssn_c, lname_new_m2, fname_new_m2, 
                                  mname_new_m2, lnamesuf_new_m2, gender_new_m2, 
                                  dob_m2, ssn_new_junk_m2, ssn_c_junk_m2, 
                                  lname_new_m3, fname_new_m3, mname_new_m3, 
                                  lnamesuf_new_m3, gender_new_m3, dob_m3, 
                                  lname_rec_m3, fname_new_cnt_m3, mname_new_cnt_m3, 
                                  lnamesuf_new_cnt_m3, gender_new_cnt_m3, 
                                  dob_cnt_m3, lname_trim_m3, lname_phon_m3, 
                                  fname_trim_m3, fname_phon_m3, ssn_new_junk_m3, 
                                  ssn_c_junk_m3)]
pairs3_full <- unique(pairs3_full,
                      by = c("ssn_new", "ssn_c", "lname_new_m2", "fname_new_m2", 
                             "mname_new_m2", "dob_m2", "gender_new_m2"))
pairs3_full <- setDF(pairs3_full)


# Add to full dedup set and make cleaner data for next deduplication process
pha_complete3 <- left_join(pha_complete2, pairs3_full, 
                           by = c("ssn_new", "ssn_c", "ssn_new_junk_m2", "ssn_c_junk_m2",
                                  "lname_new_m2", "fname_new_m2", "mname_new_m2", 
                                  "lnamesuf_new_m2", "dob_m2", "gender_new_m2")) %>%
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
         fname_phon_m3 = ifelse(is.na(fname_phon_m3), fname_phon_m2, fname_phon_m3),
         ssn_new_junk_m3 = ifelse(is.na(ssn_new_junk_m3), ssn_new_junk_m2, ssn_new_junk_m3),
         ssn_c_junk_m3 = ifelse(is.na(ssn_c_junk_m3), ssn_c_junk_m2, ssn_c_junk_m3)
  )

pha_new3 <- pha_complete3 %>%
  select(ssn_new, ssn_c, ssn_new_junk_m3, ssn_c_junk_m3, lname_new_m3:dob_m3, 
         dob_y_m3:dob_d_m3, lname_rec_m3:fname_phon_m3) %>%
  distinct(ssn_new, ssn_c, lname_new_m3, fname_new_m3, mname_new_m3, 
           lnamesuf_new_m3, dob_m3, gender_new_m3, .keep_all = TRUE)



#### Match #04 - repeat match 02 with relaxed last name match to capture people with spelling variations ####
# (i.e., block on HUD ID, and DOB year; match soundex lname, fname, mname, and other DOB elements) #
match4 <- compare.dedup(
  pha_new3, blockfld = c("ssn_c", "dob_y_m3"),
  strcmp = c("mname_new_m3", "dob_mth_m3", "dob_d_m3", "gender_new_m3", "lnamesuf_new_m3"),
  phonetic = c("lname_trim_m3", "fname_trim_m3"), phonfun = soundex,
  exclude = c("ssn_new", "ssn_new_junk_m3", "ssn_c_junk_m3", "lname_new_m3", 
              "lname_phon_m3", "fname_new_m3", "fname_phon_m3", "dob_m3", 
              "lname_rec_m3", "fname_new_cnt_m3", "mname_new_cnt_m3", 
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
  mutate_at(vars(id, ssn_new, ssn_new_junk_m3, ssn_c_junk_m3, dob_y_m3, dob_mth_m3, 
                 dob_d_m3, fname_new_cnt_m3, mname_new_cnt_m3, lnamesuf_new_cnt_m3, 
                 gender_new_m3, dob_cnt_m3, gender_new_cnt_m3, Weight), 
            funs(as.numeric(as.character(.)))
  ) %>%
  mutate_at(vars(ssn_c, lname_new_m3, fname_new_m3, mname_new_m3, lname_rec_m3, 
                 lname_trim_m3, lname_phon_m3, lnamesuf_new_m3, fname_trim_m3, fname_phon_m3), 
            funs(as.character(.))
  ) %>%
  filter(!(id == "" & ssn_new == "" & Weight == "")) %>%
  # Propogate weight to both rows in a pair
  group_by(pair) %>%
  mutate(Weight = last(Weight)) %>%
  ungroup()


# Clean data based on matches and set up matches for relevant rows
pairs4_full <- pairs4 %>%
  filter(ssn_c != "" & ssn_c_junk_m3 == 0 & Weight > 0.4) %>%
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
    fname_phon_m4 = fname_phon_m3[which.max(fname_new_cnt_m3)],
    ssn_new_junk_m4 = ssn_new_junk_m3,
    ssn_c_junk_m4 = ssn_c_junk_m3
  ) %>%
  ungroup() %>%
  select(ssn_new:lnamesuf_new_m3, ssn_new_junk_m3, ssn_c_junk_m3, gender_new_m3, 
         dob_m3, lname_new_m4:ssn_c_junk_m4) %>%
  distinct(ssn_new, ssn_c, lname_new_m3, fname_new_m3, mname_new_m3, dob_m3, 
           gender_new_m3, .keep_all = TRUE)


# Add to full dedup set and make cleaner data for next deduplication process
pha_complete4 <- left_join(pha_complete3, pairs4_full, 
                           by = c("ssn_new", "ssn_c", "ssn_new_junk_m3", "ssn_c_junk_m3",
                                  "lname_new_m3", "fname_new_m3", "mname_new_m3", 
                                  "lnamesuf_new_m3", "dob_m3", "gender_new_m3")) %>%
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
         fname_phon_m4 = ifelse(is.na(fname_phon_m4), fname_phon_m3, fname_phon_m4),
         ssn_new_junk_m4 = ifelse(is.na(ssn_new_junk_m4), ssn_new_junk_m3, ssn_new_junk_m4),
         ssn_c_junk_m4 = ifelse(is.na(ssn_c_junk_m4), ssn_c_junk_m3, ssn_c_junk_m4)
  )

pha_new4 <- pha_complete4 %>%
  select(ssn_new, ssn_c, lname_new_m4:dob_m4, dob_y_m4:dob_d_m4, lname_rec_m4:ssn_c_junk_m4) %>%
  distinct(ssn_new, ssn_c, lname_new_m4, fname_new_m4, mname_new_m4, lnamesuf_new_m4, 
           dob_m4, gender_new_m4, .keep_all = TRUE)


#### Match #05 - block on soundex lname, soundex fname, and DOB; match SSN, mname, gender, and lname suffix ####
### Need to first identify which is the 'correct' SSN
# For non-junk SSNs (i.e., 9 digits that do not repeat/use consecutive numbers), take most common
# For junk SSNs, assume none are correct
# NB. This approach will contain errors because one person's mistyped SSNs will be included in another person's SSN count
# However, this error rate should be small and dwarfed by the count of the correct social
# Other errors exist because not all junk SSNs are caught here
pha_ssn <- pha %>%
  filter(ssn_new_junk == 0) %>%
  group_by(ssn_new) %>%
  summarise(ssn_new_cnt = n()) %>%
  ungroup()

pha_new4 <- left_join(pha_new4, pha_ssn, by = c("ssn_new"))
rm(pha_ssn) 


match5 <- compare.dedup(
  pha_new4, blockfld = c("lname_trim_m4", "fname_trim_m4","dob_m4"),
  strcmp = c("ssn_new","mname_new_m4", "gender_new_m4", "lnamesuf_new_m4"),
  phonetic = c("lname_trim_m4", "fname_trim_m4"), phonfun = soundex,
  exclude = c("ssn_c", "ssn_new_junk_m4", "ssn_c_junk_m4", "lname_new_m4", 
              "lname_phon_m4", "fname_new_m4", "fname_phon_m4", "dob_y_m4", 
              "dob_mth_m4", "dob_d_m4", "lname_rec_m4", "fname_new_cnt_m4", 
              "mname_new_cnt_m4", "lnamesuf_new_cnt_m4", "dob_cnt_m4", 
              "gender_new_cnt_m4", "ssn_new_cnt"))

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
  mutate_at(vars(id, ssn_new, ssn_new_junk_m4, ssn_c_junk_m4, dob_y_m4, dob_mth_m4, 
                 dob_d_m4, fname_new_cnt_m4, mname_new_cnt_m4, lnamesuf_new_cnt_m4, 
                 gender_new_m4, dob_cnt_m4, gender_new_cnt_m4, Weight), 
            funs(as.numeric(as.character(.)))
  ) %>%
  mutate_at(vars(ssn_c, lname_new_m4, fname_new_m4, mname_new_m4, lname_rec_m4, 
                 lname_trim_m4, lname_phon_m4, lnamesuf_new_m4, fname_trim_m4, fname_phon_m4), 
            funs(as.character(.))
  ) %>%
  filter(!(id == "" & Weight == "")) %>%
  # Propogate weight to both rows in a pair and find pairs where at least one SSN is non-junk
  group_by(pair) %>%
  mutate(Weight = last(Weight),
         ssn_reg = ifelse(first(ssn_new_junk_m4) == 0 | last(ssn_new_junk_m4) == 0, 
                          1, 0)) %>%
  ungroup()


### Need a method to identify twins


# Clean data based on matches and set up matches for relevant rows
pairs5_full <- setDT(pairs5)
pairs5_full <- pairs5_full[(Weight >= 0.2 & !(dob_mth_m4 == 1 & dob_d_m4 == 1) & ssn_reg == 1) | 
                             (Weight >= 0.8218 & (dob_mth_m4 == 1 & dob_d_m4 == 1) & ssn_reg == 1)]
pairs5_full[, ':=' 
            (# See match 1 above for details on this block of code (exceptions noted below)
              ssn_new_m5 = case_when(
                is.na(first(ssn_new_cnt)) ~ last(ssn_new),
                is.na(last(ssn_new_cnt)) ~  first(ssn_new),
                TRUE ~ ssn_new[which.max(ssn_new_cnt)]),
              # Can no longer assume lname_rec is the same on both rows because of junk SSNs
              # Now look for non-missing rows and decide what to do when both rows are non-missing
              # Currently taking the lname associated with the most common fname, taking the first row when ties occur
              lname_new_m5 = case_when(
                is.na(first(lname_rec_m4)) ~ last(lname_rec_m4),
                is.na(last(lname_rec_m4)) ~ first(lname_rec_m4),
                TRUE ~ lname_rec_m4[which.max(fname_new_cnt_m4)]),
              # Now need to rule out missing counts for other name variables
              fname_new_m5 = case_when(
                is.na(first(fname_new_cnt_m4)) ~ last(fname_new_m4),
                is.na(last(fname_new_cnt_m4)) ~ first(fname_new_m4),
                TRUE ~ fname_new_m4[which.max(fname_new_cnt_m4)]),
              mname_new_m5 = case_when(
                is.na(first(mname_new_cnt_m4)) ~ last(mname_new_m4),
                is.na(last(mname_new_cnt_m4)) ~ first(mname_new_m4),
                identical(mname_new_m4[which.max(mname_new_cnt_m4)], 
                          character(0)) ~ "",
                TRUE ~ mname_new_m4[which.max(mname_new_cnt_m4)]),
              lnamesuf_new_m5 = case_when(
                is.na(first(lnamesuf_new_cnt_m4)) ~ last(lnamesuf_new_m4),
                is.na(last(lnamesuf_new_cnt_m4)) ~ first(lnamesuf_new_m4),
                identical(lnamesuf_new_m4[which.max(lnamesuf_new_cnt_m4)], 
                          character(0)) ~ "",
                TRUE ~ lnamesuf_new_m4[which.max(lnamesuf_new_cnt_m4)]),
              gender_new_m5 = case_when(
                is.na(first(gender_new_cnt_m4)) ~ last(gender_new_m4),
                is.na(last(gender_new_cnt_m4)) ~ first(gender_new_m4),
                identical(gender_new_m4[which.max(gender_new_cnt_m4)], 
                          character(0)) ~ NA_real_,
                TRUE ~ gender_new_m4[which.max(gender_new_cnt_m4)]),
              dob_m5 = as.Date(ifelse(identical(dob_m4[which.max(dob_cnt_m4)], character(0)),
                                      "",
                                      dob_m4[which.max(dob_cnt_m4)]), origin = "1970-01-01")
            ), by = "pair"]
pairs5_full[, ':=' 
            (# Reset lname_rec to match current lname using the logic above
              lname_rec_m5 = lname_new_m5,
              fname_new_cnt_m5 = case_when(
                is.na(first(fname_new_cnt_m4)) ~ last(fname_new_cnt_m4),
                is.na(last(fname_new_cnt_m4)) ~ first(fname_new_cnt_m4),
                TRUE ~ fname_new_cnt_m4[which.max(fname_new_cnt_m4)]),
              mname_new_cnt_m5 = case_when(
                is.na(first(mname_new_cnt_m4)) ~ last(mname_new_cnt_m4),
                is.na(last(mname_new_cnt_m4)) ~ first(mname_new_cnt_m4),
                identical(mname_new_cnt_m4[which.max(mname_new_cnt_m4)], 
                          character(0)) ~ NA_real_,
                TRUE ~ mname_new_cnt_m4[which.max(mname_new_cnt_m4)]),
              lnamesuf_new_cnt_m5 = case_when(
                is.na(first(lnamesuf_new_cnt_m4)) ~ last(lnamesuf_new_cnt_m4),
                is.na(last(lnamesuf_new_cnt_m4)) ~ first(lnamesuf_new_cnt_m4),
                identical(lnamesuf_new_cnt_m4[which.max(lnamesuf_new_cnt_m4)], 
                          character(0)) ~ NA_real_,
                TRUE ~ lnamesuf_new_cnt_m4[which.max(lnamesuf_new_cnt_m4)]),
              gender_new_cnt_m5 = case_when(
                is.na(first(gender_new_cnt_m4)) ~ last(gender_new_cnt_m4),
                is.na(last(gender_new_cnt_m4)) ~ first(gender_new_cnt_m4),
                identical(gender_new_cnt_m4[which.max(gender_new_cnt_m4)], 
                          character(0)) ~ NA_real_,
                TRUE ~ gender_new_cnt_m4[which.max(gender_new_cnt_m4)]),
              dob_cnt_m5 = ifelse(identical(dob_cnt_m4[which.max(dob_cnt_m4)], character(0)),
                                  NA_real_,
                                  dob_cnt_m4[which.max(dob_cnt_m4)]),
              # Easier to recreate the trim and phonetic variables than apply the logic above
              lname_trim_m5 = str_replace_all(lname_new_m5, 
                                              pattern = "[:punct:]|[:digit:]|[:blank:]|`", 
                                              replacement = ""),
              fname_trim_m5 = str_replace_all(fname_new_m5, 
                                              pattern = "[:punct:]|[:digit:]|[:blank:]|`", 
                                              replacement = "")
            ), by = "pair"]
pairs5_full[, ':='
               (# Make soundex versions of names for matching/grouping
                 lname_phon_m5 = soundex(lname_trim_m5),
                 fname_phon_m5 = soundex(fname_trim_m5),
                 # Cannot assume junk SSN flags apply, need to reset
                 ssn_new_junk_m5 = case_when(
                   is.na(first(ssn_new_cnt)) ~ last(ssn_new_junk_m4),
                   is.na(last(ssn_new_cnt)) ~ first(ssn_new_junk_m4),
                   TRUE ~ ssn_new_junk_m4[which.max(ssn_new_cnt)]),
                 ssn_c_junk_m5 = ssn_c_junk_m4
                 ), by = "pair"]
pairs5_full <- pairs5_full[, list(ssn_new, ssn_new_junk_m4, ssn_c_junk_m4, 
                                  ssn_new_m5, ssn_c, lname_new_m4, fname_new_m4, 
                                  mname_new_m4, lnamesuf_new_m4, gender_new_m4, 
                                  dob_m4, lname_new_m5, fname_new_m5, mname_new_m5, 
                                  lnamesuf_new_m5, gender_new_m5, dob_m5, 
                                  lname_rec_m5, fname_new_cnt_m5, mname_new_cnt_m5, 
                                  lnamesuf_new_cnt_m5, gender_new_cnt_m5, dob_cnt_m5, 
                                  lname_trim_m5, fname_trim_m5, lname_phon_m5, 
                                  fname_phon_m5, ssn_new_junk_m5, ssn_c_junk_m5)]
pairs5_full <- unique(pairs5_full,
                      by = c("ssn_new", "ssn_c", "lname_new_m4", "fname_new_m4", 
                             "mname_new_m4", "dob_m4", "gender_new_m4"))
pairs5_full <- setDF(pairs5_full)


# Add to full dedup set and make cleaner data for next deduplication process
pha_complete5 <- left_join(pha_complete4, pairs5_full, 
                           by = c("ssn_new", "ssn_c", "ssn_new_junk_m4", "ssn_c_junk_m4",
                                  "lname_new_m4", "fname_new_m4", "mname_new_m4", 
                                  "lnamesuf_new_m4", "dob_m4", "gender_new_m4")) %>%
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
         fname_phon_m5 = ifelse(is.na(fname_phon_m5), fname_phon_m4, fname_phon_m5),
         ssn_new_junk_m5 = ifelse(is.na(ssn_new_junk_m5), ssn_new_junk_m4, ssn_new_junk_m5),
         ssn_c_junk_m5 = ifelse(is.na(ssn_c_junk_m5), ssn_c_junk_m4, ssn_c_junk_m5)
  )

pha_new5 <- pha_complete5 %>%
  select(ssn_new_m5, ssn_c, lname_new_m5:dob_m5, dob_y_m5:dob_d_m5, lname_rec_m5:ssn_c_junk_m5) %>%
  distinct(ssn_new_m5, ssn_c, lname_new_m5, fname_new_m5, mname_new_m5, 
           lnamesuf_new_m5, dob_m5, gender_new_m5, .keep_all = TRUE)



#### Match #06 - block on soundex lname, soundex fname, and DOB; match combined SSN/HUD ID, mname, gender, and lname suffix ####
# Next make combined SSN/HUD ID variable (there are no overlaps between the two initially)
pha_complete5 <- pha_complete5 %>% mutate(
  ssn_id = ifelse(ssn_new_junk_m5 == 0, ssn_new_m5,
                  ifelse(ssn_c != "" & ssn_c_junk_m5 == 0, ssn_c,
                         NA)))

pha_new5 <- pha_new5 %>% mutate(
  ssn_id = ifelse(ssn_new_junk_m5 == 0, ssn_new_m5,
                  ifelse(ssn_c != "" & ssn_c_junk_m5 == 0, ssn_c,
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



match6 <- compare.dedup(
  pha_new5, blockfld = c("lname_trim_m5", "fname_trim_m5","dob_m5"),
  strcmp = c("ssn_id","mname_new_m5", "gender_new_m5", "lnamesuf_new_m5"),
  phonetic = c("lname_trim_m5", "fname_trim_m5"), phonfun = soundex,
  exclude = c("ssn_new_m5", "ssn_c", "ssn_id_cnt", "ssn_new_junk_m5", "ssn_c_junk_m5", 
              "lname_new_m5", "lname_phon_m5", "fname_new_m5", "fname_phon_m5", 
              "dob_y_m5", "dob_mth_m5", "dob_d_m5", "lname_rec_m5", 
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
  mutate_at(vars(id, ssn_new_m5, ssn_id_cnt, ssn_new_junk_m5, ssn_c_junk_m5, 
                 dob_y_m5, dob_mth_m5, dob_d_m5, fname_new_cnt_m5, mname_new_cnt_m5,
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
# Keep as nested ifelse because it carried through middle initials better than case_when
pairs6_full <- pairs6 %>%
  filter((Weight >= 0.2 & !(dob_mth_m5 == 1 & dob_d_m5 == 1)) | 
           (Weight >= 0.8211 & (dob_mth_m5 == 1 & dob_d_m5 == 1))  
  ) %>%
  group_by(pair) %>%
  mutate(
    # See match 1 above for details on this block of code (exceptions noted below)
    ssn_id_m6 = ifelse(is.na(first(ssn_id_cnt)), last(ssn_id), 
                       ifelse(is.na(last(ssn_id_cnt)), first(ssn_id),
                              ssn_id[which.max(ssn_id_cnt)])),
    # Can no longer assume lname_rec is the same on both rows because of junk SSNs.
    # Now look for non-missing rows and decide what to do when both rows are non-missing
    # Currently taking the lname associated with the most common fname, taking the first row when ties occur
    lname_new_m6 = ifelse(is.na(first(lname_rec_m5)), last(lname_rec_m5), 
                          ifelse(is.na(last(lname_rec_m5)), first(lname_rec_m5),
                                 lname_rec_m5[which.max(fname_new_cnt_m5)])),
    # Now need to rule out missing counts for other name variables
    fname_new_m6 = ifelse(is.na(first(fname_new_cnt_m5)), last(fname_new_m5), 
                          ifelse(is.na(last(fname_new_cnt_m5)), first(fname_new_m5),
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
    fname_new_cnt_m6 = ifelse(is.na(first(fname_new_cnt_m5)), last(fname_new_cnt_m5), 
                              ifelse(is.na(last(fname_new_cnt_m5)), 
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
    # Don't make new junk SSN/ID flags, recreate from ssn_id_m6 later
  ) %>%
  ungroup() %>%
  select(ssn_new_m5, ssn_c, ssn_id, ssn_id_m6, ssn_new_junk_m5, ssn_c_junk_m5,
         lname_new_m5:dob_m5, lname_new_m6:fname_phon_m6) %>%
  distinct(ssn_new_m5, ssn_c, ssn_id, lname_new_m5, fname_new_m5, mname_new_m5, 
           dob_m5, gender_new_m5, .keep_all = TRUE)

# Add to full dedup set and make cleaner data for next deduplication process
pha_complete6 <- left_join(pha_complete5, pairs6_full, 
                           by = c("ssn_new_m5", "ssn_c", "ssn_id", "ssn_new_junk_m5", 
                                  "ssn_c_junk_m5", "lname_new_m5", "fname_new_m5",
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



#### MERGE FINAL DEDUPLICATED DATA BACK TO ORIGINAL ####
pha_clean <- pha_complete6 %>%
  select(ssn_new:lnamesuf_new, lname_rec:gender_new_cnt, ssn_new_m5, ssn_id_m6:dob_d_m6) %>%
  right_join(., pha, by = c("ssn_new", "ssn_c", "ssn_new_junk", "ssn_c_junk",
                            "lname_new", "lnamesuf_new", "fname_new", "mname_new", 
                             "lname_rec", "fname_new_cnt", "mname_new_cnt", "lnamesuf_new_cnt", "dob",
                             "dob_y", "dob_mth", "dob_d", "dob_cnt", "gender_new", "gender_new_cnt"))


#### Trim extraneous variables ####
# Try to limit to only those that are needed for cleanup or analyses
# Adding in more variables increased the chance of duplicate rows per action date
if(UW == TRUE) {
pha_clean <- pha_clean %>%
  select(
    # Person demographics
    ssn_new:lnamesuf_new, dob, gender_new, citizen, disability, relcode, 
    ssn_id_m6:dob_m6, race, r_white:r_hisp, mbr_num,
    # don't keep mbr_id for now as there are some individuals with multiple IDs on the same date
    # Head of household demographics
    hh_ssn, hh_ssn_new, hh_ssn_c, hh_lname:hh_mname, hh_lnamesuf, hh_dob,
    # Household details
    hh_size, hh_id, 
    # Previous experience
    # list_date, list_zip, list_homeless, housing_act,
    # Action and subsidy details
    act_type, act_date:admit_date, reexam_date, subsidy_id, vouch_num, cert_id,
    increment, increment_old,# repayment, repay_amount,
    # Program details
    agency, agency_new, portability, cost_pha, major_prog, prog_type, 
    vouch_type,# noncit_subsidy, sroyn,
    # Unit details
    property_id, property_name, property_type, portfolio, unit_id, unit_add:unit_zip,
    unit_type, unit_year, access_unit, access_req, access_rec, bed_cnt, 
    # moving_in, move_in_date,
    # Rent details
    rent_type:bdrm_voucher, cost_month, rent_owner:tb_rent_ceiling,
    # Asset and income details
    incasset_id, hh_asset_val, hh_asset_inc, hh_asset_impute, hh_asset_inc_final,
    asset_val, asset_inc,
    inc_fixed, inc_vary, inc, inc_excl, inc_adj_fixed, inc_adj_vary, inc_adj,
    hh_inc_fixed, hh_inc_adj_fixed, hh_inc, 
    hh_inc_vary, hh_inc_adj_vary, 
    hh_inc_tot, hh_inc_adj, hh_inc_deduct, hh_inc_tot_adj,
    # FSS and MTW details + assistance
    # ss, fss_cat, mtw_self_suff, mtw_admit_date,
    # assist_tanf:assist_eitc, child_care_srvc,
    # reimbursed_med, med_dis_thresh, med_dis_allowance,
    # Process details
    sha_source, kcha_source, eop_source
  )
} else {
pha_clean <- pha_clean %>%
  select(
    # Person demographics
    ssn_new:lnamesuf_new, dob, gender_new, citizen, disability, relcode, 
    ssn_id_m6:dob_m6, race, r_white:r_hisp, mbr_num,
    # don't keep mbr_id for now as there are some individuals with multiple IDs on the same date
    # Head of household demographics
    hh_ssn, hh_ssn_new, hh_ssn_c, hh_lname:hh_mname, hh_lnamesuf, hh_dob,
    # Household details
    hh_size, hh_id, 
    # Previous experience
    list_date, list_zip, list_homeless, housing_act,
    # Action and subsidy details
    act_type, act_date:admit_date, reexam_date, subsidy_id, vouch_num, cert_id,
    increment, increment_old, repayment, repay_amount,
    # Program details
    agency, agency_new, portability, cost_pha, major_prog, prog_type, 
    vouch_type, noncit_subsidy, sroyn,
    # Unit details
    property_id, property_name, property_type, portfolio, unit_id, unit_add:unit_zip,
    unit_type, unit_year, access_unit, access_req, access_rec, bed_cnt, 
    moving_in, move_in_date,
    # Rent details
    rent_type:bdrm_voucher, cost_month, rent_owner:tb_rent_ceiling,
    # Asset and income details
    incasset_id, hh_asset_val, hh_asset_inc, hh_asset_impute, hh_asset_inc_final,
    asset_val, asset_inc,
    inc_fixed, inc_vary, inc, inc_excl, inc_adj_fixed, inc_adj_vary, inc_adj,
    hh_inc_fixed, hh_inc_adj_fixed, hh_inc, 
    hh_inc_vary, hh_inc_adj_vary, 
    hh_inc_tot, hh_inc_adj, hh_inc_deduct, hh_inc_tot_adj,
    # FSS and MTW details + assistance
    fss, fss_cat, mtw_self_suff, mtw_admit_date,
    assist_tanf:assist_eitc, child_care_srvc,
    reimbursed_med, med_dis_thresh, med_dis_allowance,
    # Process details
    sha_source, kcha_source, eop_source
  )
}
# With some variables stripped out, can reduce the number of rows
pha_clean <- pha_clean %>% distinct()


#### Carry over updated names etc. to head-of-household details ####
# Ideally, this will eventually happen in parallel with each cleanup step above for use in later deduplication
# Set up cleaned head of household names
# Can only use ssn_id not ssn_new and ssn_c because the latter two differ across 
# rows for the same HoH
pha_clean <- pha_clean %>%
  mutate(
    hh_ssn_id_m6 = ifelse(mbr_num == 1 & ssn_id_m6 != "", ssn_id_m6, ""),
    hh_lname_m6 = ifelse(mbr_num == 1 & lname_new_m6 != "", lname_new_m6, hh_lname),
    hh_lnamesuf_m6 = ifelse(mbr_num == 1 & lnamesuf_new_m6 != "", lnamesuf_new_m6, hh_lnamesuf),
    hh_fname_m6 = ifelse(mbr_num == 1 & fname_new_m6 != "", fname_new_m6, hh_fname),
    hh_mname_m6 = ifelse(mbr_num == 1 & mname_new_m6 != "", mname_new_m6, hh_mname),
    hh_dob_m6 = as.Date(ifelse(mbr_num == 1 & !is.na(dob_m6), dob_m6, hh_dob), origin = "1970-01-01")
  )


# Set up another temporary household ID based on slightly modified original HoH characteristics
# Need to use date and addresses too because the cleanup didn't catch everything
# Result is some inconsistency in HoH name over time but the overall numbers should work
pha_clean$hh_id_temp <- group_indices(pha_clean, major_prog, hh_ssn_new, hh_ssn_c, 
                                         hh_lname, hh_lnamesuf, hh_fname, hh_dob, 
                                         unit_add, unit_apt, unit_apt2, unit_city, 
                                         act_date, act_type)

# Limit to just the cleaned up HH variables
pha_clean_hh <- pha_clean %>%
  distinct(hh_id_temp, 
           hh_ssn_id_m6, hh_lname_m6, hh_lnamesuf_m6, 
           hh_fname_m6, hh_mname_m6, hh_dob_m6, mbr_num) %>%
  filter(mbr_num == 1) %>%
  select(-mbr_num)

# Merge back to the main data and clean up duplicated varnames
pha_clean <- left_join(pha_clean, pha_clean_hh, by = c("hh_id_temp")) %>%
  rename(hh_ssn_id_m6 = hh_ssn_id_m6.y, hh_lname_m6 = hh_lname_m6.y, 
         hh_lnamesuf_m6 = hh_lnamesuf_m6.y, hh_fname_m6 = hh_fname_m6.y,
         hh_mname_m6 = hh_mname_m6.y, hh_dob_m6 = hh_dob_m6.y) %>%
  select(-ends_with(".x"))


### Make a new household ID based on head of household characteristics
pha_clean$hh_id_new <- group_indices(pha_clean, hh_ssn_id_m6, hh_lname_m6, hh_fname_m6, hh_dob_m6)

### Make new flags for junk SSNs
pha_clean <- junk_ssn_all(pha_clean, ssn_id_m6)
pha_clean <- junk_ssn_all(pha_clean, hh_ssn_id_m6)

### Clean up column order
pha_clean <- pha_clean %>% 
  select(ssn_new:ssn_id_m6, ssn_id_m6_junk, lname_new_m6:hh_ssn_id_m6, 
         hh_ssn_id_m6_junk, hh_lname_m6:hh_id_new, -hh_id_temp)


### Filter out test names
pha_clean <- pha_clean %>% filter(!(lname_new_m6 == "DUFUS" & fname_new_m6 == "IAM"))


### Make IDs and reorder
pha_clean$pid <- group_indices(pha_clean, ssn_id_m6, lname_new_m6, 
                               fname_new_m6, dob_m6)
pha_clean <- pha_clean %>% select(pid, ssn_new:hh_id_new)


#### Save point ####
saveRDS(pha_clean, file = file.path(housing_path, pha_clean_fn))

# Remove data frames and values made along the way
rm(list = ls(pattern = "pha_new[1-6]*$"))
rm(list = ls(pattern = "pha_complete[1-6]*$"))
rm(list = ls(pattern = "pairs[1-6]"))
rm(list = ls(pattern = "classify"))
rm(list = ls(pattern = "match"))
rm(list = ls(pattern = "pha_dedup"))
rm(pha_clean_hh)
rm(pha)
gc()
