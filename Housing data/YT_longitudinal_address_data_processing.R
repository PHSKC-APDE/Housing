###############################################################################
# Code to create a cleaned person table from the Seattle Housing Authority data
# Aim is to have a single row per contiguous time in a house per person
#
# Alastair Matheson (PHSKC-APDE)
# 2016-09-19
###############################################################################


##### Set up global parameter and call in libraries #####
options(max.print = 700, scipen = 0)

library(RODBC) # Used to connect to SQL server
library(openxlsx) # Used to import/export Excel files
library(car) # used to recode variables
library(stringr) # Used to manipulate string data
library(lubridate) # Used to manipulate dates
library(dplyr) # Used to manipulate data
library(RecordLinkage) # used to clean up duplicates in the data
library(phonics) # used to extract phonetic version of names


##### Bring in merged SHA data #####
# Import YT data
yt <- read.csv(file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/LIPH_YT_2004_2016.csv")

# Make a copy of the dataset to avoid having to reread it
yt.bk <- yt


##### Clean up data #####
# Strip out extraneous variables
yt <- select(yt, IncAsset_ID, Property.ID:Unit.Zip.Code...5a, Member.number...3a:Member.relation.code...3h,
             Race.white.indicator...3k1:Yesler.Terrace.Indicator)

# Rename variables
yt <- rename(yt, incasset_id = IncAsset_ID, property_id = Property.ID, unit_id = Unit.ID, 
             act_type = Action.Type...2a, act_date = Effective.Date.of.Action...2b, admit_date = Date.of.Admission...2h,
             eexam_date = Projected.Effective.Date.of.Next.Re.Exam...2i, hofh_ssn = Head.of.House.SSN,
             hofh_lname = Last.Name...3b..Head., hofh_lnamesuf = Last.Name.Head.Suffix,
             hofh_fname = First.Name...3c..Head., hofh_mname = Middle.Initial...3d..Head.,
             hhold_num = Number.of.Household.Members...3t, unit_add = Unit.Address.Number.and.Street....5a,
             unit_cty = Unit.City...5a, unit_state = Unit.State...5a, unit_zip = Unit.Zip.Code...5a,
             mbr_num = Member.number...3a, lname = Last.name...3b, lnamesuf = Last.Name.Suffix,
             fname = First.name...3c, mname = Middle.initial...3d, dob = Date.of.birth...3e,
             gender = Sex...3g, relcode = Member.relation.code...3h, r_white = Race.white.indicator...3k1,
             r_black = Race.black.african.american.indicator...3k2, r_aian = Race.american.indian.alaska.native.indicator...3k3,
             r_asian = Race.asian.indicator...3k4, r_nhpi = Race.native.hawaiin.other.pacific.islander.indicator...3k5,
             r_hisp = Ethnicity...3m, ssn = Family.Member.SSN...3n, hofh_exssn = Former.Head.of.House.SSN...3w,
             table = Original.Table, year = Year.of.Action, yt = Yesler.Terrace.Indicator)


# Sort records
yt <- arrange(yt, hofh_ssn, ssn, act_date)


### SSN - part 1
# Clean up SSNs
yt$ssnnew <- as.numeric(as.character(str_replace_all(yt$ssn, "-", "")))
yt$hofh_ssnnew <- as.numeric(as.character(str_replace_all(yt$hofh_ssn, "-", "")))


### Dates
# Format dates and strip out dob components for matching
yt <- yt %>%
  mutate_at(vars(act_date, admit_date, eexam_date), funs(as.Date(., format = "%m/%d/%Y"))) %>%
  mutate(dob = as.Date(dob, format = "%m/%d/%Y"),
         dob_y = as.numeric(year(dob)),
         dob_mth = as.numeric(month(dob)),
         dob_d = as.numeric(day(dob)))


# Find most common DOB by SSN (doesn't work for SSN = NA or 0)
# Need to figure out how to ID most common or most recent last name for SSNs like 0 or NA
yt <- yt %>%
  group_by(ssnnew, dob) %>%
  mutate(dob_cnt = ifelse(ssnnew > 10000000 & ssnnew != 111111111 & 
                            ssnnew != 123456789 & ssnnew != 999999999,
                          n(),
                          NA)) %>%
  ungroup()


### Names
# Change name case to be consistently upper case
yt <- yt %>%
  mutate_at(vars(ends_with("name")), toupper)

# Strip out any white space around names (mutate_at did not work)
yt <- yt %>%
  mutate(lname = trimws(lname, which = c("both")),
         fname = trimws(fname, which = c("both")),
         mname = trimws(mname, which = c("both")),
         hofh_lname = trimws(hofh_lname, which = c("both")),
         hofh_fname = trimws(hofh_fname, which = c("both")),
         hofh_mname = trimws(hofh_mname, which = c("both")))

# Fix format of suffix
yt <- mutate(yt, lnamesuf = as.character(lnamesuf))


### First name
# Clean up where middle initial seems to be in first name field
yt <- yt %>%
  mutate(fname_new = ifelse(str_detect(str_sub(fname, -2, -1), "[:space:][A-Z]") == TRUE,
                            str_sub(fname, 1, -3), fname),
         mname_new = ifelse(str_detect(str_sub(fname, -2, -1), "[:space:][A-Z]") == TRUE,
                            str_sub(fname, -1), mname))


### Strip out multiple spaces in the middle of last names and fix inconsistent apostrophes
yt <- yt %>%
  mutate(lname_new = str_replace(lname, "[:space:]{2,}", " "),
         fname_new = str_replace(fname_new, "[:space:]{2,}", " "),
         lname_new = str_replace(lname_new, "`", "'"),
         fname_new = str_replace(fname_new, "`", "'"))



### Pull out suffixes into the appropriate column
# NB. Order of recoding important or else the suffix is lost
yt <- yt %>%
  mutate(lnamesuf_new = ifelse(str_detect(str_sub(lname_new, -3, -1), paste(c(" JR", " SR"," II", " IV"),
                                                                            collapse="|")) == TRUE,
                               str_sub(lname_new, -2, -1), lnamesuf),
         lname_new = ifelse(str_detect(str_sub(lname_new, -3, -1), paste(c(" JR", " SR", " II", " IV"),
                                                                         collapse="|")) == TRUE,
                                str_sub(lname_new, 1, -4), lname_new),
         # NB. There is one row with a typo of LLL instead of III
         lnamesuf_new = ifelse(str_detect(str_sub(lname_new, -4, -1), paste(c(" III", " LLL", " JR."),
                                                                            collapse = "|")) == TRUE,
                               str_sub(lname_new, -3, -1), lnamesuf_new),
         lname_new = ifelse(str_detect(str_sub(lname_new, -4, -1), paste(c(" III", " LLL", " JR."),
                                                                         collapse = "|")) == TRUE,
                                str_sub(lname_new, 1, -5), lname_new),

         
         lnamesuf_new = ifelse(str_detect(str_sub(fname_new, -3, -1), paste(c(" JR", " SR", " II", " IV"),
                                                                            collapse="|")) == TRUE,
                               str_sub(fname_new, -2, -1), lnamesuf_new),
         fname_new = ifelse(str_detect(str_sub(fname_new, -3, -1), paste(c(" JR", " SR", " II", " IV"),
                                                                         collapse="|")) == TRUE,
                            str_sub(fname_new, 1, -4), fname_new),
         lnamesuf_new = ifelse(str_detect(str_sub(fname_new, -4, -1), " III") == TRUE,
                               str_sub(fname_new, -3, -1), lnamesuf_new),
         fname_new = ifelse(str_detect(str_sub(fname_new, -4, -1), " III") == TRUE,
                            str_sub(fname_new, 1, -5), fname_new),
         # Remove any punctuation
         lnamesuf_new = str_replace_all(lnamesuf_new, pattern = "[:punct:]|[:blank:]", replacement = "")
         )


### Set up names for matching
yt <- yt %>%
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
yt <- yt %>%
  group_by(ssnnew, fname_new) %>%
  mutate(fname_new_cnt = ifelse(ssnnew > 10000000 & ssnnew != 111111111 & 
                                  ssnnew != 123456789 & ssnnew != 999999999,
                                n(),
                                NA)) %>%
  ungroup()


### Count which non-blank middle names appear most often (doesn't work for SSN = NA or 0)
# Need to figure out how to ID most common or most recent last name for SSNs like 0 or NA
yt_middle <- yt %>%
  filter(mname_new != "" & ssnnew > 10000000 & ssnnew != 111111111 & 
           ssnnew != 123456789 & ssnnew != 999999999) %>%
  group_by(ssnnew, mname_new) %>%
  summarise(mname_new_cnt = n()) %>%
  ungroup()
  
yt <- left_join(yt, yt_middle, by = c("ssnnew", "mname_new"))
rm(yt_middle)


### Last name
# Find the most recent surname used (doesn't work for SSN = NA or 0)
# Need to figure out how to ID most common or most recent last name for SSNs like 0 or NA
yt <- yt %>%
  arrange(ssnnew, desc(act_date)) %>%
  group_by(ssnnew) %>%
  mutate(lname_rec = ifelse(ssnnew > 10000000 & ssnnew != 111111111 & 
                              ssnnew != 123456789 & ssnnew != 999999999,
                            first(lname_new),
                            NA)) %>%
  ungroup()


### Count which non-blank last name suffixes appear most often (doesn't work for SSN = NA or 0)
# Need to figure out how to ID most common or most recent last name for SSNs like 0 or NA
yt_suffix <- yt %>%
  filter(lnamesuf_new != "" & ssnnew > 10000000 & ssnnew != 111111111 & 
           ssnnew != 123456789 & ssnnew != 999999999) %>%
  group_by(ssnnew, lnamesuf_new) %>%
  summarise(lnamesuf_new_cnt = n()) %>%
  ungroup()

yt <- left_join(yt, yt_suffix, by = c("ssnnew", "lnamesuf_new"))
rm(yt_suffix)


### Gender
# Count the number of genders recorded for an individual (doesn't work for SSN = NA or 0)
# Need to figure out how to ID most common or most recent last name for SSNs like 0 or NA
yt <- yt %>%
  mutate(gender_new = as.numeric(car::recode(gender, c("'F' = 1; 'M' = 2; 'NULL' = NA; else = NA")))) %>%
  group_by(ssnnew, gender_new) %>%
  mutate(gender_new_cnt = ifelse(ssnnew > 10000000 & ssnnew != 111111111 & 
                                   ssnnew != 123456789 & ssnnew != 999999999,
                                 n(),
                                 NA)) %>%
  ungroup()


### Remove duplicate records in preparation for matching
yt_dedup <- yt %>%
  select(ssnnew, fname_new:fname_phon, lname_rec, fname_new_cnt, mname_new_cnt, lnamesuf_new_cnt,
         dob, dob_y, dob_mth, dob_d, dob_cnt, gender_new, gender_new_cnt) %>%
  distinct(ssnnew, lname_new, lnamesuf_new, fname_new, mname_new, lname_rec, fname_new_cnt, mname_new_cnt,
           lnamesuf_new_cnt, dob, dob_y, dob_mth, dob_d, dob_cnt, gender_new, gender_new_cnt,
                     .keep_all = TRUE)



##### Matching protocol #####
# 01) Block on SSN, soundex lname, and DOB year; match fname, mname, lname suffix, gender, and other DOB elements
# 02) Repeat match 01 with relaxed last name match to capture people with spelling variations
# 03) Block on soundex lname, soundex fname, and DOB; match SSN, mname, gender, and lname suffix
# 04) More to come

##### Match #01 - block on SSN, soundex lname, and DOB year; match fname, mname, lname suffix, gender, and other DOB elements #####
match1 <- compare.dedup(yt_dedup, blockfld = c("ssnnew", "lname_trim", "dob_y"),
                        strcmp = c("mname_new", "dob_mth", "dob_d", "gender_new", "lnamesuf_new"),
                        phonetic = c("lname_trim", "fname_trim"), phonfun = soundex,
                        exclude = c("lname_new", "lname_phon", "fname_new", "fname_phon", 
                                    "dob", "lname_rec", "fname_new_cnt", "mname_new_cnt", 
                                    "lnamesuf_new_cnt", "dob_cnt", "gender_new_cnt"))

# Using EpiLink approach
match1_tmp <- epiWeights(match1)
classify1 <- epiClassify(match1_tmp, threshold.upper = 0.59)
summary(classify1)
pairs1 <- getPairs(classify1, single.rows = FALSE)

# Using EM weights approach
#match1_tmp <- emWeights(match1, cutoff = 0.8)
#pairs1 <- getPairs(match1_tmp, single.rows = FALSE)



# Fix formattings
pairs1 <- pairs1 %>%
  mutate(
    # Add ID to each pair
    pair = rep(seq(from = 1, to = nrow(.)/3), each = 3),
    dob = as.Date(dob, origin = "1970-01-01")
    ) %>%
  # Fix up formatting by removing factors
  mutate_at(vars(id, ssnnew, dob_y, dob_mth, dob_d, fname_new_cnt, mname_new_cnt,
                 lnamesuf_new_cnt, gender_new, dob_cnt, gender_new_cnt, Weight), funs(as.numeric(as.character(.)))
            ) %>%
  mutate_at(vars(lname_new, fname_new, mname_new, lnamesuf_new, lname_rec, lname_trim, lname_phon,
                 fname_trim, fname_phon), funs(as.character(.))
            ) %>%
  filter(!(id == "" & ssnnew == "" & Weight == ""))

# Take a look at results
pairs1 %>% filter(row_number() > 50) %>% head()


# Clean data based on matches and set up matches for relevant rows
pairs1_full <- pairs1 %>%
  filter(ssnnew > 10000000 & ssnnew != 111111111 & 
           ssnnew != 123456789 & ssnnew != 999999999) %>%
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
  select(ssnnew:lnamesuf_new, gender_new, dob, lname_new_m1:fname_phon_m1) %>%
  distinct(ssnnew, lname_new, fname_new, mname_new, dob, gender_new, .keep_all = TRUE)


# Make cleaner data for next deduplication process
yt_complete <- left_join(yt_dedup, pairs1_full, by = c("ssnnew", "lname_new", "fname_new", "mname_new", 
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
  
yt_new <- yt_complete %>% 
  select(ssnnew, lname_new_m1:dob_m1, dob_y_m1:dob_d_m1, lname_rec_m1:fname_phon_m1) %>%
  distinct(ssnnew, lname_new_m1, fname_new_m1, mname_new_m1, lnamesuf_new_m1, dob_m1, gender_new_m1, .keep_all = TRUE)




##### Match #02 - repeat match 01 with relaxed last name match to capture people with spelling variations #####
# (i.e., block on SSN, and DOB year; match soundex lname, fname, mname, and other DOB elements) #
match2 <- compare.dedup(yt_new, blockfld = c("ssnnew", "lname_trim_m1", "dob_y_m1"),
                        strcmp = c("mname_new_m1", "dob_mth_m1", "dob_d_m1", "gender_new_m1", "lnamesuf_new_m1"),
                        phonetic = c("lname_trim_m1", "fname_trim_m1"), phonfun = soundex,
                        exclude = c("lname_new_m1", "lname_phon_m1", "fname_new_m1", "fname_phon_m1", 
                                    "dob_m1", "lname_rec_m1", "fname_new_cnt_m1", "mname_new_cnt_m1", 
                                    "lnamesuf_new_cnt_m1", "dob_cnt_m1", "gender_new_cnt_m1"))


# Using EpiLink approach
match2_tmp2 <- epiWeights(match2)
classify2 <- epiClassify(match2_tmp2, threshold.upper = 0.59)
summary(classify2)
pairs2 <- getPairs(classify2, single.rows = FALSE)


# Fix formattings
pairs2 <- pairs2 %>%
  mutate(
    # Add ID to each pair
    pair = rep(seq(from = 1, to = nrow(.)/3), each = 3),
    dob_m1 = as.Date(dob_m1, origin = "1970-01-01")
  ) %>%
  # Fix up formatting by removing factors
  mutate_at(vars(id, ssnnew, dob_y_m1, dob_mth_m1, dob_d_m1, fname_new_cnt_m1, mname_new_cnt_m1,
                 lnamesuf_new_cnt_m1, gender_new_m1, dob_cnt_m1, gender_new_cnt_m1, Weight), 
            funs(as.numeric(as.character(.)))
  ) %>%
  mutate_at(vars(lname_new_m1, fname_new_m1, mname_new_m1, lname_rec_m1, lname_trim_m1, lname_phon_m1,
                 lnamesuf_new_m1, fname_trim_m1, fname_phon_m1), funs(as.character(.))
  ) %>%
  filter(!(id == "" & ssnnew == "" & Weight == ""))


# Take a look at results
pairs2 %>% filter(row_number() > 50) %>% head()

# Clean data based on matches and set up matches for relevant rows
pairs2_full <- pairs2 %>%
  filter(ssnnew > 10000000 & ssnnew != 111111111 & 
           ssnnew != 123456789 & ssnnew != 999999999) %>%
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
  select(ssnnew:lnamesuf_new_m1, gender_new_m1, dob_m1, lname_new_m2:fname_phon_m2) %>%
  distinct(ssnnew, lname_new_m1, fname_new_m1, mname_new_m1, dob_m1, gender_new_m1, .keep_all = TRUE)


# Add to full dedup set and make cleaner data for next deduplication process
yt_complete2 <- left_join(yt_complete, pairs2_full, by = c("ssnnew", "lname_new_m1", "fname_new_m1",
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
    
yt_new2 <- yt_complete2 %>%
  select(ssnnew, lname_new_m2:dob_m2, dob_y_m2:dob_d_m2, lname_rec_m2:fname_phon_m2) %>%
  distinct(ssnnew, lname_new_m2, fname_new_m2, mname_new_m2, lnamesuf_new_m2, dob_m2, gender_new_m2, .keep_all = TRUE)



##### Match #03 - block on soundex lname, soundex fname, and DOB; match SSN, mname, gender, and lname suffix #####
### Need to first identify which is the 'correct' SSN
# For non-junk SSNs (i.e., 9 digits that do not repeat/use consecutive numbers), take most common
# For junk SSNs, assume none are correct
# NB. This approach will contain errors because one person's mistyped SSNs will be included in another person's SSN count
# However, this error rate should be small and dwarfed by the count of the correct social
# Other errors exist because not all junk SSNs are caught here (e.g., some are 999991234 etc.)
yt_ssn <- yt %>%
  filter(ssnnew > 10000000 & ssnnew != 111111111 & 
           ssnnew != 123456789 & ssnnew != 999999999) %>%
  group_by(ssnnew) %>%
  summarise(ssnnew_cnt = n()) %>%
  ungroup()

yt_new2 <- left_join(yt_new2, yt_ssn, by = c("ssnnew"))
rm(yt_ssn) 



match3 <- compare.dedup(yt_new2, blockfld = c("lname_trim_m2", "fname_trim_m2","dob_m2"),
                        strcmp = c("ssnnew","mname_new_m2", "gender_new_m2", "lnamesuf_new_m2"),
                        phonetic = c("lname_trim_m2", "fname_trim_m2"), phonfun = soundex,
                        exclude = c("lname_new_m2", "lname_phon_m2", "fname_new_m2", "fname_phon_m2", 
                                    "dob_y_m2", "dob_mth_m2", "dob_d_m2", "lname_rec_m2", 
                                    "fname_new_cnt_m2", "mname_new_cnt_m2", "lnamesuf_new_cnt_m2", 
                                    "dob_cnt_m2", "gender_new_cnt_m2", "ssnnew_cnt"))

# Using EpiLink approach
match3_tmp3 <- epiWeights(match3)
classify3 <- epiClassify(match3_tmp3, threshold.upper = 0.6)
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
  mutate_at(vars(id, ssnnew, dob_y_m2, dob_mth_m2, dob_d_m2, fname_new_cnt_m2, mname_new_cnt_m2,
                 lnamesuf_new_cnt_m2, gender_new_m2, dob_cnt_m2, gender_new_cnt_m2, Weight), 
            funs(as.numeric(as.character(.)))
  ) %>%
  mutate_at(vars(lname_new_m2, fname_new_m2, mname_new_m2, lname_rec_m2, lname_trim_m2, lname_phon_m2,
                 lnamesuf_new_m2, fname_trim_m2, fname_phon_m2), funs(as.character(.))
  ) %>%
  filter(!(id == "" & ssnnew == "" & Weight == "")) %>%
  # Propogate weight to both rows in a pair
  group_by(pair) %>%
  mutate(Weight = last(Weight)) %>%
  ungroup()


# Take a look at results
pairs3 %>% filter(row_number() > 50) %>% select(ssnnew, lname_new_m2:mname_new_m2, dob_m2, fname_new_cnt_m2, ssnnew_cnt, Weight, pair) %>% head()
pairs3 %>% filter(Weight <= 0.6) %>% select(ssnnew, lname_new_m2:mname_new_m2, dob_m2, fname_new_cnt_m2, ssnnew_cnt, Weight, pair) %>% head(., n =20)


# Clean data based on matches and set up matches for relevant rows
pairs3_full <- pairs3 %>%
  filter(Weight >= 0.6) %>%
  group_by(pair) %>%
  mutate(
    # See match 1 above for details on this block of code (exceptions noted below)
    ssnnew_m3 = ifelse(is.na(first(ssnnew_cnt)), last(ssnnew), ifelse(is.na(last(ssnnew_cnt)), first(ssnnew),
                                                                      ssnnew[which.max(ssnnew_cnt)])),
    # Can no longer assume lname_rec is the same on both rows because of junk SSNs.
    # Now look for non-missing rows and decide what to do when both rows are non-missing
    # Currently taking the lname associated with the most common fname, taking the first row when ties occur
    lname_new_m3 = ifelse(is.na(first(lname_rec_m2)), last(lname_rec_m2), 
                          ifelse(is.na(last(lname_rec_m2)), first(lname_rec_m2),
                                 lname_rec_m2[which.max(fname_new_cnt_m2)])),
    # Now need to rule out missing counts for other name variables
    fname_new_m3 = ifelse(is.na(first(fname_new_cnt_m2)), last(fname_new_m2), ifelse(is.na(last(fname_new_cnt_m2)),
                                                                                     first(fname_new_m2),
                                                                                     fname_new_m2[which.max(fname_new_cnt_m2)])),
    mname_new_m3 = ifelse(is.na(first(mname_new_cnt_m2)), last(mname_new_m2), 
                          ifelse(is.na(last(mname_new_cnt_m2)),
                                 first(mname_new_m2), 
                                 ifelse(identical(mname_new_m2[which.max(mname_new_cnt_m2)], character(0)), "",
                                                                                            mname_new_m2[which.max(mname_new_cnt_m2)]))),
    lnamesuf_new_m3 = ifelse(is.na(first(lnamesuf_new_cnt_m2)), last(lnamesuf_new_m2), 
                          ifelse(is.na(last(lnamesuf_new_cnt_m2)),
                                 first(lnamesuf_new_m2), 
                                 ifelse(identical(lnamesuf_new_m2[which.max(lnamesuf_new_cnt_m2)], character(0)), "",
                                        lnamesuf_new_m2[which.max(lnamesuf_new_cnt_m2)]))),
    gender_new_m3 = ifelse(is.na(first(gender_new_cnt_m2)), last(gender_new_m2), 
                             ifelse(is.na(last(gender_new_cnt_m2)),
                                    first(gender_new_m2), 
                                    ifelse(identical(gender_new_m2[which.max(gender_new_cnt_m2)], character(0)), "",
                                           gender_new_m2[which.max(gender_new_cnt_m2)]))),
    dob_m3 = as.Date(ifelse(identical(dob_m2[which.max(dob_cnt_m2)], character(0)),
                            "",
                            dob_m2[which.max(dob_cnt_m2)]), origin = "1970-01-01"),
    # Reset lname_rec to match current lname using the logic above
    lname_rec_m3 = lname_new_m3,
    fname_new_cnt_m3 = ifelse(is.na(first(fname_new_cnt_m2)), last(fname_new_cnt_m2), ifelse(is.na(last(fname_new_cnt_m2)),
                                                                                             first(fname_new_cnt_m2),
                                                                                             fname_new_cnt_m2[which.max(fname_new_cnt_m2)])),
    mname_new_cnt_m3 = ifelse(is.na(first(mname_new_cnt_m2)), last(mname_new_cnt_m2), 
                          ifelse(is.na(last(mname_new_cnt_m2)),
                                 first(mname_new_cnt_m2), 
                                 ifelse(identical(mname_new_cnt_m2[which.max(mname_new_cnt_m2)], character(0)),
                                        NA,
                                        mname_new_cnt_m2[which.max(mname_new_cnt_m2)]))),
    lnamesuf_new_cnt_m3 = ifelse(is.na(first(lnamesuf_new_cnt_m2)), last(lnamesuf_new_cnt_m2), 
                             ifelse(is.na(last(lnamesuf_new_cnt_m2)),
                                    first(lnamesuf_new_cnt_m2), 
                                    ifelse(identical(lnamesuf_new_cnt_m2[which.max(lnamesuf_new_cnt_m2)], character(0)),
                                           NA,
                                           lnamesuf_new_cnt_m2[which.max(lnamesuf_new_cnt_m2)]))),
    gender_new_cnt_m3 = ifelse(is.na(first(gender_new_cnt_m2)), last(gender_new_cnt_m2), 
                           ifelse(is.na(last(gender_new_cnt_m2)),
                                  first(gender_new_cnt_m2), 
                                  ifelse(identical(gender_new_cnt_m2[which.max(gender_new_cnt_m2)], character(0)),
                                         NA,
                                         gender_new_cnt_m2[which.max(gender_new_cnt_m2)]))),
    dob_cnt_m3 = as.Date(ifelse(identical(dob_cnt_m2[which.max(dob_cnt_m2)], character(0)),
                            NA,
                            dob_cnt_m2[which.max(dob_cnt_m2)]), origin = "1970-01-01"),
    # Easier to recreate the trim and phonetic variables than apply the logic above
    lname_trim_m3 = str_replace_all(lname_new_m3, pattern = "[:punct:]|[:digit:]|[:blank:]|`", replacement = ""),
    fname_trim_m3 = str_replace_all(fname_new_m3, pattern = "[:punct:]|[:digit:]|[:blank:]|`", replacement = ""),
    # Make soundex versions of names for matching/grouping
    lname_phon_m3 = soundex(lname_trim_m3),
    fname_phon_m3 = soundex(fname_trim_m3)
  ) %>%
  ungroup() %>%
  select(ssnnew, ssnnew_m3, lname_new_m2:dob_m2, lname_new_m3:fname_phon_m3) %>%
  distinct(ssnnew, lname_new_m2, fname_new_m2, mname_new_m2, dob_m2, gender_new_m2, .keep_all = TRUE)


# Add to full dedup set and make cleaner data for next deduplication process
yt_complete3 <- left_join(yt_complete2, pairs3_full, by = c("ssnnew", "lname_new_m2", "fname_new_m2",
                                                           "mname_new_m2", "lnamesuf_new_m2",
                                                           "dob_m2", "gender_new_m2")) %>%
  mutate(ssnnew_m3 = ifelse(is.na(ssnnew_m3), ssnnew, ssnnew_m3),
         lname_new_m3 = ifelse(is.na(lname_new_m3), lname_new_m2, lname_new_m3),
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
    
yt_new3 <- yt_complete3 %>%
  select(ssnnew_m3, lname_new_m3:dob_m3, dob_y_m3:dob_d_m3, lname_rec_m3:fname_phon_m3) %>%
  distinct(ssnnew_m3, lname_new_m3, fname_new_m3, mname_new_m3, lnamesuf_new_m3, dob_m3, gender_new_m3, .keep_all = TRUE)





##### MERGE FINAL DEDUPLICATED DATA BACK TO ORIGINAL #####
yt_clean <- yt_complete3 %>%
  select(ssnnew:lnamesuf_new, lname_rec:gender_new_cnt, ssnnew_m3:dob_d_m3) %>%
  right_join(., yt, by = c("ssnnew", "lname_new", "lnamesuf_new", "fname_new", "mname_new", 
                                               "lname_rec", "fname_new_cnt", "mname_new_cnt", "lnamesuf_new_cnt", "dob",
                                               "dob_y", "dob_mth", "dob_d", "dob_cnt", "gender_new", "gender_new_cnt")) %>%
  # Trim extraneous variables
  select(ssnnew:lnamesuf_new, dob, gender_new, ssnnew_m3:dob_m3, incasset_id:eexam_date, hhold_num:mbr_num,
         r_white:r_hisp, table:yt)



##### END MATCHING/DEDUPLICATION SECTION #####


##### RECODE RACE AND OTHER VARIABLES #####
### Race
# Recode race variables and make numeric
yt_clean <- yt_clean %>%
  mutate_at(vars(r_white:r_hisp), funs(new = car::recode(., "'Y' = 1; 'N' = 0; 'NULL' = NA; else = NA"))) %>%
  mutate_at(vars(r_white_new:r_hisp_new), funs(as.numeric(.))) # as numeric stopped working when placed in row above


# Identify individuals with contradictory race values and set to Y
yt_clean <- yt_clean %>%
  group_by(ssnnew_m3, lname_new_m3, fname_new_m3) %>%
  mutate_at(vars(r_white_new:r_hisp_new), funs(tot = sum(.))) %>%
  ungroup %>%
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
  mutate_at(vars(r_white_new:r_nhpi_new), funs(alone = ifelse(r_multi_new == 1, 0, .)))


##### Consolidate address rows #####
### Make all addresses upper case
yt_cleanadd <- mutate(yt_clean, unit_add_new = toupper(unit_add))

yt_cleanadd <- arrange(yt_cleanadd, ssnnew_m3, lname_new_m3, fname_new_m3, act_date)

### Remove annual reexaminations if address is the same
# Want to avoid capturing the first or last row for a person at a given address
yt_cleanadd <- yt_cleanadd %>%
  mutate(drop = ifelse((ssnnew_m3 == lead(ssnnew_m3, 1) | (is.na(ssnnew_m3) & is.na(lead(ssnnew_m3, 1)))) & 
                         (ssnnew_m3 == lag(ssnnew_m3, 1) | (is.na(ssnnew_m3) & is.na(lag(ssnnew_m3, 1)))) & 
                         lname_new_m3 == lag(lname_new_m3, 1) &  lname_new_m3 == lead(lname_new_m3, 1) & 
                         fname_new_m3 == lag(fname_new_m3, 1) & fname_new_m3 == lead(fname_new_m3, 1) &
                         act_type %in% c(2, 3, 14) & 
                         unit_add_new == lag(unit_add_new, 1) & unit_add_new == lead(unit_add_new, 1), 
                       1, 0)) %>%
  filter(drop == 0)

### Create start and end dates for a person at that address
yt_cleanadd <- yt_cleanadd %>%
  # First row for a person = least recent of act_date or admit_date
  # Other rows where that is the person's first row at that address = act_date
  mutate(startdate = as.Date(ifelse((ssnnew_m3 != lag(ssnnew_m3, 1) | (is.na(ssnnew_m3) & is.na(lag(ssnnew_m3, 1)))) &
                            (lname_new_m3 != lag(lname_new_m3, 1) | fname_new_m3 != lag(fname_new_m3, 1)) | 
                              # account for first row
                              (!is.na(ssnnew_m3) & !is.na(lname_new_m3) & is.na(lag(ssnnew_m3, 1)) & is.na(lag(lname_new_m3, 1))),
                            pmin(act_date, admit_date),
                            ifelse(unit_add_new != lag(unit_add_new, 1), act_date,
                                   NA)),
                            origin = "1970-01-01"),
         # Last row for a person = today's date or act_date + 3 years
         # Other rows where that is the person's last row at that address = date at next address - 1 day
         enddate = as.Date(ifelse(act_type == 6, act_date,
                                  ifelse(ssnnew_m3 != lead(ssnnew_m3, 1) | 
                                                             (((is.na(ssnnew_m3) & is.na(lead(ssnnew_m3, 1))) | ssnnew_m3 == lead(ssnnew_m3, 1)) & 
                                                             (lname_new_m3 != lead(lname_new_m3, 1) | fname_new_m3 != lead(fname_new_m3, 1))) | 
                                           # account for last row
                                           (!is.na(lname_new_m3) & is.na(lead(lname_new_m3, 1))),
                                    pmin(today(), act_date + dyears(3)),
                                    ifelse(unit_add_new != lead(unit_add_new, 1),
                                         lead(act_date, 1) - 1, NA))),
                           origin = "1970-01-01")
         )



### Collapse rows to have a single line per address
yt_cleanadd <- yt_cleanadd %>%
  # Remove rows at an address that are neither the start or end of a time there
  filter(!(is.na(startdate) & is.na(enddate))) %>%
  mutate(enddate = as.Date(ifelse(is.na(enddate), lead(enddate, 1), enddate), origin = "1970-01-01")) %>%
  filter(!(is.na(startdate)) & !(is.na(enddate)))




