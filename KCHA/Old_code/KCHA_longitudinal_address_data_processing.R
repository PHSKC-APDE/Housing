###############################################################################
# Code to create a cleaned person table from the King County Housing Authority data
# Aim is to have a single row per contiguous time in a house per person
#
# Alastair Matheson (PHSKC-APDE)
# alastair.matheson@kingcounty.gov
# 2016-04-07
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
library(tidyr) # used to reorganize and reshape data
library(reshape2) # used to reorganize and reshape data
library(RecordLinkage) # used to clean up duplicates in the data
library(phonics) # used to extract phonetic version of names
library(reticulate) # used to pull in python-based address parser



##### Connect to the servers #####
db.apde51 <- odbcConnect("PH_APDEStore51")

##### Bring in raw, joined KCHA data #####
ptm01 <- proc.time()
kcha <- sqlQuery(
  db.apde51,
  "SELECT *
  FROM dbo.kcha_combined_raw",
  stringsAsFactors = FALSE)
proc.time() - ptm01

# Make a copy of the dataset to avoid having to reread it
kcha.bk <- kcha


### Strip out some variables for now
# Remove panel 19 on income, 20 on public housing info, and some of 21 on project info
#kcha <- select(kcha, -(starts_with("h19")), -(starts_with("h20")), -(h21a), -(h21b), -(h21i), -(h21j), -(h21k), -(h21m), -(h21n), -(h21q))
kcha <- select(kcha, -(starts_with("h19")))

# Need to strip duplicates again now that some variables have been removed
kcha <- kcha %>% distinct()

##### Reshape data #####
# The data initially has household members in wide format
# Need to reshape to give one hhold member per row but retain head of hhold info

# Make HH vars first
kcha <- kcha %>%
  mutate(
    hh_lname = h3b01,
    hh_fname = h3c01,
    hh_mname = h3d01,
    hh_ssn = h3n01,
    hh_dob = h3e01,
    hhold_id_temp = row_number()
  )


# Rename some variable names to have consistent suffix
names <- as.data.frame(names(kcha), stringsAsFactors = FALSE)
colnames(names) <- c("varname")
names <- names %>%
  mutate(
    varname = ifelse(str_detect(varname, "h3k[:digit:]*a"), str_replace(varname, "h3k", "h3k1"), varname),
    varname = ifelse(str_detect(varname, "h3k[:digit:]*b"), str_replace(varname, "h3k", "h3k2"), varname),
    varname = ifelse(str_detect(varname, "h3k[:digit:]*c"), str_replace(varname, "h3k", "h3k3"), varname),
    varname = ifelse(str_detect(varname, "h3k[:digit:]*d"), str_replace(varname, "h3k", "h3k4"), varname),
    varname = ifelse(str_detect(varname, "h3k[:digit:]*e"), str_replace(varname, "h3k", "h3k5"), varname),
    # Trim the final letter
    varname = ifelse(str_detect(varname, "h3k"), str_sub(varname, 1, -2), varname)
  )
colnames(kcha) <- names[,1]


# Using an apply process over reshape due to memory issues
# Make function to save space
reshape_func <- function(x) {
  print(x)
  sublong[[x]] <- kcha %>%
    select_('h1a:h2h',
            h3a = paste0("h3a", x), # not reliable so make own member number
            h3b = paste0("h3b", x),
            h3c = paste0("h3c", x),
            h3d = paste0("h3d", x),
            h3e = paste0("h3e", x),
            h3g = paste0("h3g", x),
            h3h = paste0("h3h", x),
            h3i = paste0("h3i", x),
            h3j = paste0("h3j", x),
            h3k1 = paste0("h3k1", x),
            h3k2 = paste0("h3k2", x),
            h3k3 = paste0("h3k3", x),
            h3k4 = paste0("h3k4", x),
            h3k5 = paste0("h3k5", x),
            h3m = paste0("h3m", x),
            h3n = paste0("h3n", x),
            'program_type',
            'spec_vouch',
            'h5a1a:hhold_id_temp') %>%
    mutate(mbr_num = x)
}


# Make empty list and run reshaping function
sublong <- list()
members <- c("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12")
kcha_long <- lapply(members, reshape_func)
kcha_long <- bind_rows(kcha_long)

# Get rid of white space (focus on variables used to filter first to save memory)
kcha_long <- kcha_long %>%
  mutate_at(vars(h3a, h3b, h3c, h3n, starts_with("h5a")), funs(str_trim(.)))

# Get rid of empty rows (note: some rows have a SSN but no name or address, others have an address but no name or other details)
kcha_long <- kcha_long %>%
  filter(!((is.na(h3a) |  h3a == 0) & h3b == "" & h3c == "" & h3n == ""))

# Add number of household members
kcha_long <- kcha_long %>%
  group_by(hhold_id_temp) %>%
  mutate(hhold_size = n()) %>%
  ungroup() %>%
  select(-(hhold_id_temp))

# Make mbr_num and unit_zip a number
kcha_long <- mutate_at(kcha_long, vars(mbr_num, h5a5), funs(as.numeric(.)))


# Convert dates to appropriate format
kcha_long <- mutate_at(kcha_long, vars(h2b, h2h, h3e, hh_dob),
                       funs(as.Date(., origin = "1970-01-01")))



##### Rename variables #####
# Bring in variable name mapping table
fields <- read.xlsx("//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/Field name mapping.xlsx")

kcha_long <- setnames(kcha_long, fields$PHSKC[match(names(kcha_long), fields$KCHA_modified)])


##### OPTIONAL: WRITE RESHAPED DATA TO SQL #####
sqlDrop(db.apde51, "dbo.kcha_reshaped")
sqlSave(
  db.apde51,
  kcha_long,
  tablename = "dbo.kcha_reshaped",
  varTypes = c(
    act_date = "Date",
    admit_date = "Date",
    dob = "Date",
    hh_dob = "Date"
   )
 )




##### Remove temporary files #####
rm(list = ls(pattern = "kcha_new_"))
rm(list = ls(pattern = "kcha_old_"))
rm(names)
rm(members)
rm(sublong)
gc()




##### Clean up data #####
### Reformat variables
kcha_long <- kcha_long %>%
  # Dates come as integers with dropped leading zeros
  mutate_at(vars(act_date, admit_date, dob, hh_dob),
            funs(as.Date(ifelse(nchar(as.character(.)) == 7, paste0("0", as.character(.)),
                                as.character(.)), "%m%d%Y"))) %>%
  # Address fields have lots of white space
  mutate_at(vars(unit_add, unit_apt, unit_apt2, unit_city), funs(str_trim(.))) %>%
  # SSNs are characters (note that this strips out leading zeros)
  mutate_at(vars(ssn, hh_ssn), funs(new = round(as.numeric(.), digits = 0)))


### Dates
# Strip out dob components for matching
kcha_long <- kcha_long %>%
  mutate(dob_y = as.numeric(year(dob)),
         dob_mth = as.numeric(month(dob)),
         dob_d = as.numeric(day(dob)))


# Find most common DOB by SSN (doesn't work for SSN = NA or 0)
# Need to figure out how to ID most common or most recent last name for SSNs like 0 or NA
kcha_long <- kcha_long %>%
  group_by(ssn_new, dob) %>%
  mutate(dob_cnt = ifelse(ssn_new >= 1000000 & ssn_new != 111111111 & 
                            ssn_new != 123456789 & ssn_new != 999999999,
                          n(),
                          NA)) %>%
  ungroup()


### Names overall
# Change names to be consistently upper case
kcha <- kcha %>%
  mutate_at(vars(ends_with("name")), funs(toupper))

# Strip out any white space around names
kcha <- kcha %>%
  mutate_at(vars(ends_with("name")), funs(str_trim(.)))

# Strip out multiple spaces in the middle of last names, fix inconsistent apostrophes,
#   remove extraneous commas, and other general fixes
kcha <- kcha %>%
  mutate_at(vars(ends_with("name")), funs(str_replace(., "[:space:]{2,}", " "))) %>%
  mutate_at(vars(ends_with("name")), funs(str_replace(., "`", "'"))) %>%
  mutate_at(vars(ends_with("name")), funs(str_replace(., ", ", " "))) %>%
  mutate_at(vars(ends_with("name")), funs(str_replace(., "_", "-")))



### First name
# Clean up where middle initial seems to be in first name field
# NOTE: There are many rows with a middle initial in the fname field AND the mname field
# (and the initials are not always the same)
# Need to talk with KCHA to determine best approach

kcha <- kcha %>%
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
kcha <- kcha %>%
  # Suffixes in last names
  mutate(lnamesuf_new = ifelse(str_detect(str_sub(lname, -3, -1), paste(c(" JR", " SR"," II", " IV"),
                                                                            collapse="|")) == TRUE,
                               str_sub(lname, -2, -1), ""),
         lname_new = ifelse(str_detect(str_sub(lname, -3, -1), paste(c(" JR", " SR", " II", " IV"),
                                                                         collapse="|")) == TRUE,
                            str_sub(lname, 1, -4), lname),
         lnamesuf_new = ifelse(str_detect(str_sub(lname, -4, -1), paste(c(" III", " JR.", " SR.", " 2ND",
                                                                          " II\"", " II.", " IV."),
                                                                            collapse = "|")) == TRUE,
                               str_sub(lname, -3, -1), lnamesuf_new),
         lname_new = ifelse(str_detect(str_sub(lname, -4, -1), paste(c(" III", " JR.", " SR.", " 2ND",
                                                                       " II\"", " II.", " IV."),
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
         fname_new = ifelse(str_detect(str_sub(fname_new, -4, -1), paste(c(" JR.", " III", " SR."),
                                                                         collapse="|")) == TRUE,
                            str_sub(fname_new, 1, -5), fname_new),
         
         # Remove any punctuation
         lnamesuf_new = str_replace_all(lnamesuf_new, pattern = "[:punct:]|[:blank:]", replacement = "")
  )





#### Set up variables for matching ####
### Sort records
kcha <- arrange(kcha, hh_ssn, ssn, act_date)

### Make trimmed version of names
kcha <- kcha %>%
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
kcha <- kcha %>%
  group_by(ssn_new, fname_new) %>%
  mutate(fname_new_cnt = ifelse(ssn_new >= 1000000 & ssn_new != 111111111 & 
                                  ssn_new != 123456789 & ssn_new != 999999999,
                                n(),
                                NA)) %>%
  ungroup()


### Count which non-blank middle names appear most often (doesn't work for SSN = NA or 0)
# Need to figure out how to ID most common or most recent last name for SSNs like 0 or NA
kcha_middle <- kcha %>%
  filter(mname_new != "" & ssn_new >= 1000000 & ssn_new != 111111111 & 
           ssn_new != 123456789 & ssn_new != 999999999) %>%
  group_by(ssn_new, mname_new) %>%
  summarise(mname_new_cnt = n()) %>%
  ungroup()

kcha <- left_join(kcha, kcha_middle, by = c("ssn_new", "mname_new"))
rm(kcha_middle)


### Last name
# Find the most recent surname used (doesn't work for SSN = NA or 0)
# Need to figure out how to ID most common or most recent last name for SSNs like 0 or NA
kcha <- kcha %>%
  arrange(ssn_new, desc(act_date)) %>%
  group_by(ssn_new) %>%
  mutate(lname_rec = ifelse(ssn_new >= 1000000 & ssn_new != 111111111 & 
                              ssn_new != 123456789 & ssn_new != 999999999,
                            first(lname_new),
                            NA)) %>%
  ungroup()


### Count which non-blank last name suffixes appear most often (doesn't work for SSN = NA or 0)
# Need to figure out how to ID most common or most recent last name for SSNs like 0 or NA
kcha_suffix <- kcha %>%
  filter(lnamesuf_new != "" & ssn_new >= 1000000 & ssn_new != 111111111 & 
           ssn_new != 123456789 & ssn_new != 999999999) %>%
  group_by(ssn_new, lnamesuf_new) %>%
  summarise(lnamesuf_new_cnt = n()) %>%
  ungroup()

kcha <- left_join(kcha, kcha_suffix, by = c("ssn_new", "lnamesuf_new"))
rm(kcha_suffix)


### Gender
# Count the number of genders recorded for an individual (doesn't work for SSN = NA or 0)
# Need to figure out how to ID most common or most recent last name for SSNs like 0 or NA
kcha <- kcha %>%
  mutate(gender_new = as.numeric(car::recode(gender, c("'F' = 1; 'M' = 2; 'NULL' = NA; else = NA")))) %>%
  group_by(ssn_new, gender_new) %>%
  mutate(gender_new_cnt = ifelse(ssn_new >= 1000000 & ssn_new != 111111111 & 
                                   ssn_new != 123456789 & ssn_new != 999999999,
                                 n(),
                                 NA)) %>%
  ungroup()


### Remove duplicate records in preparation for matching
kcha_dedup <- kcha %>%
  select(ssn_new, lname_new, fname_new:lnamesuf_new, lname_trim:fname_phon, lname_rec, fname_new_cnt, mname_new_cnt, lnamesuf_new_cnt,
         dob, dob_y:dob_cnt, gender_new, gender_new_cnt) %>%
  distinct(ssn_new, lname_new, lnamesuf_new, fname_new, mname_new, lname_rec, fname_new_cnt, mname_new_cnt,
           lnamesuf_new_cnt, dob, dob_y, dob_mth, dob_d, dob_cnt, gender_new, gender_new_cnt,
                     .keep_all = TRUE)





##### Matching protocol #####
# 01) Block on SSN, soundex lname, and DOB year; match fname, mname, lname suffix, gender, and other DOB elements
# 02) Repeat match 01 with relaxed last name match to capture people with spelling variations
# 03) Block on soundex lname, soundex fname, and DOB; match SSN, mname, gender, and lname suffix
# 04) More to come

##### Match #01 - block on SSN, soundex lname, and DOB year; match fname, mname, lname suffix, gender, and other DOB elements #####
match1 <- compare.dedup(kcha_dedup, blockfld = c("ssn_new", "lname_trim", "dob_y"),
                        strcmp = c("mname_new", "dob_mth", "dob_d", "gender_new", "lnamesuf_new"),
                        phonetic = c("lname_trim", "fname_trim"), phonfun = soundex,
                        exclude = c("lname_new", "lname_phon", "fname_new", "fname_phon", 
                                    "dob", "lname_rec", "fname_new_cnt", "mname_new_cnt", 
                                    "lnamesuf_new_cnt", "dob_cnt", "gender_new_cnt"))

# Using EpiLink approach
match1_tmp <- epiWeights(match1)
classify1 <- epiClassify(match1_tmp, threshold.upper = 0.49)
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
  mutate_at(vars(id, ssn_new, dob_y, dob_mth, dob_d, fname_new_cnt, mname_new_cnt,
                 lnamesuf_new_cnt, gender_new, dob_cnt, gender_new_cnt, Weight), funs(as.numeric(as.character(.)))
            ) %>%
  mutate_at(vars(lname_new, fname_new, mname_new, lnamesuf_new, lname_rec, lname_trim, lname_phon,
                 fname_trim, fname_phon), funs(as.character(.))
            ) %>%
  filter(!(id == "" & ssn_new == "" & Weight == "")) %>%
  # Propogate weight to both rows in a pair
  group_by(pair) %>%
  mutate(Weight = last(Weight)) %>%
  ungroup()

# Take a look at results
pairs1 %>% filter(row_number() > 50) %>% head()


# Clean data based on matches and set up matches for relevant rows
pairs1_full <- pairs1 %>%
  filter(ssn_new >= 1000000 & ssn_new != 111111111 & 
           ssn_new != 123456789 & ssn_new != 999999999) %>%
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
  distinct(ssn_new, lname_new, fname_new, mname_new, dob, gender_new, .keep_all = TRUE)


# Make cleaner data for next deduplication process
kcha_complete <- left_join(kcha_dedup, pairs1_full, by = c("ssn_new", "lname_new", "fname_new", "mname_new", 
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
  
kcha_new <- kcha_complete %>% 
  select(ssn_new, lname_new_m1:dob_m1, dob_y_m1:dob_d_m1, lname_rec_m1:fname_phon_m1) %>%
  distinct(ssn_new, lname_new_m1, fname_new_m1, mname_new_m1, lnamesuf_new_m1, dob_m1, gender_new_m1, .keep_all = TRUE)




##### Match #02 - repeat match 01 with relaxed last name match to capture people with spelling variations #####
# (i.e., block on SSN, and DOB year; match soundex lname, fname, mname, and other DOB elements) #
match2 <- compare.dedup(kcha_new, blockfld = c("ssn_new", "lname_trim_m1", "dob_y_m1"),
                        strcmp = c("mname_new_m1", "dob_mth_m1", "dob_d_m1", "gender_new_m1", "lnamesuf_new_m1"),
                        phonetic = c("lname_trim_m1", "fname_trim_m1"), phonfun = soundex,
                        exclude = c("lname_new_m1", "lname_phon_m1", "fname_new_m1", "fname_phon_m1", 
                                    "dob_m1", "lname_rec_m1", "fname_new_cnt_m1", "mname_new_cnt_m1", 
                                    "lnamesuf_new_cnt_m1", "dob_cnt_m1", "gender_new_cnt_m1"))


# Using EpiLink approach
match2_tmp2 <- epiWeights(match2)
classify2 <- epiClassify(match2_tmp2, threshold.upper = 0.47)
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
  mutate_at(vars(id, ssn_new, dob_y_m1, dob_mth_m1, dob_d_m1, fname_new_cnt_m1, mname_new_cnt_m1,
                 lnamesuf_new_cnt_m1, gender_new_m1, dob_cnt_m1, gender_new_cnt_m1, Weight), 
            funs(as.numeric(as.character(.)))
  ) %>%
  mutate_at(vars(lname_new_m1, fname_new_m1, mname_new_m1, lname_rec_m1, lname_trim_m1, lname_phon_m1,
                 lnamesuf_new_m1, fname_trim_m1, fname_phon_m1), funs(as.character(.))
  ) %>%
  filter(!(id == "" & ssn_new == "" & Weight == "")) %>%
  # Propogate weight to both rows in a pair
  group_by(pair) %>%
  mutate(Weight = last(Weight)) %>%
  ungroup()



# Take a look at results
pairs2 %>% filter(row_number() > 50) %>% head()

# Clean data based on matches and set up matches for relevant rows
pairs2_full <- pairs2 %>%
  filter(ssn_new >= 1000000 & ssn_new != 111111111 & 
           ssn_new != 123456789 & ssn_new != 999999999) %>%
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
  distinct(ssn_new, lname_new_m1, fname_new_m1, mname_new_m1, dob_m1, gender_new_m1, .keep_all = TRUE)


# Add to full dedup set and make cleaner data for next deduplication process
kcha_complete2 <- left_join(kcha_complete, pairs2_full, by = c("ssn_new", "lname_new_m1", "fname_new_m1",
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
    
kcha_new2 <- kcha_complete2 %>%
  select(ssn_new, lname_new_m2:dob_m2, dob_y_m2:dob_d_m2, lname_rec_m2:fname_phon_m2) %>%
  distinct(ssn_new, lname_new_m2, fname_new_m2, mname_new_m2, lnamesuf_new_m2, dob_m2, gender_new_m2, .keep_all = TRUE)



##### Match #03 - block on soundex lname, soundex fname, and DOB; match SSN, mname, gender, and lname suffix #####
### Need to first identify which is the 'correct' SSN
# For non-junk SSNs (i.e., 9 digits that do not repeat/use consecutive numbers), take most common
# For junk SSNs, assume none are correct
# NB. This approach will contain errors because one person's mistyped SSNs will be included in another person's SSN count
# However, this error rate should be small and dwarfed by the count of the correct social
# Other errors exist because not all junk SSNs are caught here (e.g., some are 999991234 etc.)
kcha_ssn <- kcha %>%
  filter(ssn_new >= 1000000 & ssn_new != 111111111 & 
           ssn_new != 123456789 & ssn_new != 999999999) %>%
  group_by(ssn_new) %>%
  summarise(ssn_new_cnt = n()) %>%
  ungroup()

kcha_new2 <- left_join(kcha_new2, kcha_ssn, by = c("ssn_new"))
rm(kcha_ssn) 



match3 <- compare.dedup(kcha_new2, blockfld = c("lname_trim_m2", "fname_trim_m2","dob_m2"),
                        strcmp = c("ssn_new","mname_new_m2", "gender_new_m2", "lnamesuf_new_m2"),
                        phonetic = c("lname_trim_m2", "fname_trim_m2"), phonfun = soundex,
                        exclude = c("lname_new_m2", "lname_phon_m2", "fname_new_m2", "fname_phon_m2", 
                                    "dob_y_m2", "dob_mth_m2", "dob_d_m2", "lname_rec_m2", 
                                    "fname_new_cnt_m2", "mname_new_cnt_m2", "lnamesuf_new_cnt_m2", 
                                    "dob_cnt_m2", "gender_new_cnt_m2", "ssn_new_cnt"))

# Using EpiLink approach
match3_tmp3 <- epiWeights(match3)
classify3 <- epiClassify(match3_tmp3, threshold.upper = 0.5)
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
  mutate_at(vars(lname_new_m2, fname_new_m2, mname_new_m2, lname_rec_m2, lname_trim_m2, lname_phon_m2,
                 lnamesuf_new_m2, fname_trim_m2, fname_phon_m2), funs(as.character(.))
  ) %>%
  filter(!(id == "" & ssn_new == "" & Weight == "")) %>%
  # Propogate weight to both rows in a pair and find pairs where at least one SSN is non-junk
  group_by(pair) %>%
  mutate(Weight = last(Weight),
         ssn_reg = ifelse((first(ssn_new) >= 1000000 & first(ssn_new) != 111111111 & 
                            first(ssn_new) != 123456789 & first(ssn_new) != 999999999) |
                            (last(ssn_new) >= 1000000 & last(ssn_new) != 111111111 & 
                            last(ssn_new) != 123456789 & last(ssn_new) != 999999999), 1, 0)) %>%
  ungroup()


# Take a look at results
pairs3 %>% filter(row_number() > 50) %>% select(ssn_new, lname_new_m2:mname_new_m2, dob_m2, fname_new_cnt_m2, ssn_new_cnt, Weight, pair) %>% head(., n = 20)
pairs3 %>% filter(Weight <= 0.6) %>% select(ssn_new, lname_new_m2:mname_new_m2, dob_m2, fname_new_cnt_m2, ssn_new_cnt, Weight, pair) %>% head(., n =20)


# Clean data based on matches and set up matches for relevant rows
pairs3_full <- pairs3 %>%
  filter((Weight >= 0.2 & !(dob_mth_m2 == 1 & dob_d_m2 == 1) & ssn_reg == 1) | 
           (Weight >= 0.8218 & (dob_mth_m2 == 1 & dob_d_m2 == 1) & ssn_reg == 1)  
           ) %>%
  group_by(pair) %>%
  mutate(
    # See match 1 above for details on this block of code (exceptions noted below)
    ssn_new_m3 = ifelse(is.na(first(ssn_new_cnt)), last(ssn_new), ifelse(is.na(last(ssn_new_cnt)), first(ssn_new),
                                                                      ssn_new[which.max(ssn_new_cnt)])),
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
  select(ssn_new, ssn_new_m3, lname_new_m2:dob_m2, lname_new_m3:fname_phon_m3) %>%
  distinct(ssn_new, lname_new_m2, fname_new_m2, mname_new_m2, dob_m2, gender_new_m2, .keep_all = TRUE)


# Add to full dedup set and make cleaner data for next deduplication process
kcha_complete3 <- left_join(kcha_complete2, pairs3_full, by = c("ssn_new", "lname_new_m2", "fname_new_m2",
                                                           "mname_new_m2", "lnamesuf_new_m2",
                                                           "dob_m2", "gender_new_m2")) %>%
  mutate(ssn_new_m3 = ifelse(is.na(ssn_new_m3), ssn_new, ssn_new_m3),
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
    
kcha_new3 <- kcha_complete3 %>%
  select(ssn_new_m3, lname_new_m3:dob_m3, dob_y_m3:dob_d_m3, lname_rec_m3:fname_phon_m3) %>%
  distinct(ssn_new_m3, lname_new_m3, fname_new_m3, mname_new_m3, lnamesuf_new_m3, dob_m3, gender_new_m3, .keep_all = TRUE)





##### MERGE FINAL DEDUPLICATED DATA BACK TO ORIGINAL #####
kcha_clean <- kcha_complete3 %>%
  select(ssn_new:lnamesuf_new, lname_rec:gender_new_cnt, ssn_new_m3:dob_d_m3) %>%
  right_join(., kcha, by = c("ssn_new", "lname_new", "lnamesuf_new", "fname_new", "mname_new", 
                                               "lname_rec", "fname_new_cnt", "mname_new_cnt", "lnamesuf_new_cnt", "dob",
                                               "dob_y", "dob_mth", "dob_d", "dob_cnt", "gender_new", "gender_new_cnt")) %>%
  # Trim extraneous variables
  select(ssn_new:lnamesuf_new, dob, gender_new, ssn_new_m3:dob_m3, authority:mbr_num, relcode:hh_ssn_new)


# Remove data frames and values made along the way
rm(list = ls()[!ls() %in% c("kcha", "kcha.bk", "kcha_clean", "db.apde")])
gc()

# Filter out test names
kcha_clean <- kcha_clean %>% filter(!(lname_new_m3 == "DUFUS" & fname_new_m3 == "IAM"))


##### END MATCHING/DEDUPLICATION SECTION #####


##### RECODE RACE AND OTHER VARIABLES #####
#### Race ####
# Recode race variables and make numeric
# Note: Because of typos and other errors, this process will overestimate the number of people with multiple races
kcha_clean <- kcha_clean %>%
  mutate_at(vars(r_white:r_hisp), funs(new = car::recode(., "'Y' = 1; 'N' = 0; 'NULL' = NA; else = NA", 
                                                         as.numeric.result = TRUE, as.factor.result = FALSE
                                                         )))


# Identify individuals with contradictory race values and set to Y
kcha_clean <- kcha_clean %>%
  group_by(ssn_new_m3, lname_new_m3, fname_new_m3) %>%
  mutate_at(vars(r_white_new:r_hisp_new), funs(tot = sum(.))) %>%
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



#### Addresses ####
### Import Python address parser
addparser <- import("usaddress")


### Clean addresses
# Make all addresses upper case and sort
kcha_cleanadd <- mutate_at(kcha_clean, vars(unit_add, unit_apt, unit_apt2, unit_city, unit_state), funs(toupper(.)))
kcha_cleanadd <- arrange(kcha_cleanadd, ssn_new_m3, lname_new_m3, fname_new_m3, act_date)


# Specific addresses
# Some addresses have specific issues than cannot be addressed via rules
# However, these specific addresses should not be shared publically
adds_specific <- read.xlsx("//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/PHA_specific_addresses_fix - DO NOT SHARE FILE.xlsx",
                           na.strings = "")
adds_specific <- mutate_all(adds_specific, funs(ifelse(is.na(.), "", .)))
# For some reason there seem to be duplicates in the address data, possible created when cleaning up missing in the line above
adds_specific <- adds_specific %>% distinct()
kcha_cleanadd <- left_join(kcha_cleanadd, adds_specific, by = c("unit_add", "unit_apt", "unit_apt2", "unit_city", "unit_state", "unit_zip"))


# Bring over addresses not matched (could use overidden == 0 too, could also collapse to a mutate_at statement)
kcha_cleanadd <- kcha_cleanadd %>%
  mutate(
    unit_add_new = ifelse(is.na(unit_add_new), unit_add, unit_add_new),
    unit_apt_new = ifelse(is.na(unit_apt_new), unit_apt, unit_apt_new),
    unit_apt2_new = ifelse(is.na(unit_apt2_new), unit_apt2, unit_apt2_new),
    unit_city_new = ifelse(is.na(unit_city_new), unit_city, unit_city_new),
    unit_state_new = ifelse(is.na(unit_state_new), unit_state, unit_state_new),
    unit_zip_new = ifelse(is.na(unit_zip_new), unit_zip, unit_zip_new)
  )



# Get rid of extra spacing in addresses and some punctuation
kcha_cleanadd <- kcha_cleanadd %>%
  mutate(unit_add_new = str_replace_all(unit_add_new, "\\.|,", ""),
         unit_add_new = str_replace_all(unit_add_new, "[:space:]+", " "),
         unit_apt_new = str_replace_all(unit_apt_new, ",", "")
         )

# Clean up road name in wrong field
kcha_cleanadd <- kcha_cleanadd %>%
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


# Clean up apartments in wrong field
kcha_cleanadd <- kcha_cleanadd %>%
  mutate(
    # Remove straight duplicates of apt numbers in address and apt fields
    unit_add_new = if_else(unit_apt_new != "" &
                             str_sub(unit_add_new, str_length(unit_add_new) - str_length(unit_apt_new) + 1, str_length(unit_add_new)) ==
                             str_sub(unit_apt_new, 1, str_length(unit_apt_new)),
                           str_sub(unit_add_new, 1, str_length(unit_add_new) - str_length(unit_apt_new)),
                           unit_add_new),
    # Remove duplicates that are a little more complicated
    unit_add_new = if_else(str_sub(unit_apt_new, str_locate(unit_apt_new, "(APT|UNIT|#|BLDG)*[:space:]*")[, 2] + 1, str_length(unit_apt_new)) ==
                             str_sub(unit_add_new, str_length(unit_add_new) - (str_length(unit_apt_new) - 
                                                                                 (str_locate(unit_apt_new, "(APT|UNIT|#|BLDG)*[:space:]*")[, 2] + 1)),
                                     str_length(unit_add_new)) &
                             !str_sub(unit_add_new, str_length(unit_add_new) - 1, str_length(unit_add_new)) %in% c("LA", "N", "NE", "NW", "S", "SE", "SW"),
                           str_sub(unit_add_new, 1, str_length(unit_add_new) - (str_length(unit_apt_new) - 
                                                                                  str_locate(unit_apt_new, "(APT|UNIT|#|BLDG)*[:space:]*")[, 2])),
                                   unit_add_new),
    # ID apartment numbers that need to move into the appropriate column (1, 2)
    # Also include addresses that end in a number as many seem to be apartments (3, 4)
    unit_apt_move = if_else(unit_apt_new == "" & str_detect(unit_add_new, "APT|#|\\$") == TRUE,
                           1, if_else(
                             unit_apt_new != "" & str_detect(unit_add_new, "APT|#|\\$") == TRUE,
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
                          str_sub(unit_add_new, str_locate(unit_add_new, "APT|#|\\$")[, 1], str_length(unit_add_new)),
                          unit_apt_new),
    unit_apt_new = if_else(unit_apt_move == 3,
                          str_sub(unit_add_new, str_locate(unit_add_new, "[:space:]+[:alnum:]*[-]*[:digit:]+$")[, 1], str_length(unit_add_new)),
                          unit_apt_new),
    # Merge apt data from unit_add_new with unit_apt_new if the latter is currently not blank
    unit_apt_new = if_else(unit_apt_move == 2,
                          paste(str_sub(unit_add_new, str_locate(unit_add_new, "APT|#")[, 1], str_length(unit_add_new)),
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
                          str_sub(unit_add_new, 1, str_locate(unit_add_new, "APT|#|\\$")[, 1] - 1),
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
                            str_detect(unit_apt_new, "(APT|BLDG|UNIT)") == FALSE,
                          str_replace_all(unit_apt_new, "[:space:]", "-"), unit_apt_new),
    # Move the # to the start if in between apartment components
    unit_apt_new = if_else((str_detect(unit_apt_new, "[A-Z][:digit:]*[:space:]*#[:space:]*[:digit:]+") == TRUE &
                                         str_detect(unit_apt_new, "(APT|BLDG|UNIT)") == FALSE) | 
                             (str_detect(unit_apt_new, "[:digit:]+[:space:]*#[:space:]*[:digit:]+") == TRUE & 
                                str_detect(unit_apt_new, "(APT|UNIT)") == FALSE),
                           str_replace(unit_apt_new, "[:space:]*#[:space:]*", "-"), unit_apt_new),
    # Replace # to make a match more likely and more likely the parser adds a unit designator
    unit_apt_new = if_else(str_detect(unit_apt_new, "(APT|BLDG|UNIT)") == FALSE, 
                           str_replace_all(unit_apt_new, "[#|\\$][:space:]*[-]*", "APT "),
                           str_replace(unit_apt_new, "[#|\\$]", "")),
    # Add in an APT prefix if there is no other unit designator
    unit_apt_new = if_else(str_detect(unit_apt_new, "^[:digit:]+$") == TRUE | str_detect(unit_apt_new, "^[:alnum:]$") == TRUE |
                            (str_detect(unit_apt_new, "^[:alnum:]+[-]*[:digit:]+$") == TRUE & str_detect(unit_apt_new, "(APT|BLDG|#|UNIT)") == FALSE),
                          paste0("APT ", unit_apt_new), unit_apt_new),
    # Remove spaces between hyphens
    unit_apt_new = str_replace(unit_apt_new, "[:space:]-[:space:]", "-")
  )


# With apartments gone from unit_add_new, get rid of remaining extraneous punctuation
kcha_cleanadd <- kcha_cleanadd %>%
  mutate(unit_add_new = str_replace_all(unit_add_new, "[-]+", " "))


# Clean up street addresses
kcha_cleanadd <- kcha_cleanadd %>%
  mutate(
    # standardize street names
    unit_add_new = str_replace_all(unit_add_new, "[:space:]AVENUE", " AVE"),
    unit_add_new = str_replace_all(unit_add_new, "[:space:]BOULEVARD", " BLVD"),
    unit_add_new = str_replace_all(unit_add_new, "[:space:]CIRCLE", " CIR"),
    unit_add_new = str_replace_all(unit_add_new, "[:space:]COURT", " CT"),
    unit_add_new = str_replace_all(unit_add_new, "[:space:]DRIVE", " DR"),
    unit_add_new = str_replace_all(unit_add_new, "[:space:]HIGHWAY", " HWY"),
    unit_add_new = str_replace_all(unit_add_new, "[:space:]LANE", " LN"),
    unit_add_new = str_replace_all(unit_add_new, "[:space:]PARKWAY", " PKWY"),
    unit_add_new = str_replace_all(unit_add_new, "[:space:]PLACE", " PL"),
    unit_add_new = str_replace_all(unit_add_new, "[:space:]ROAD", " RD"),
    unit_add_new = str_replace_all(unit_add_new, "[:space:]SO[:space:]|[:space:]SO$", " S"),
    unit_add_new = str_replace_all(unit_add_new, "[:space:]STREET", " ST")
  )


# Make concatenated version of address fields
kcha_cleanadd <- kcha_cleanadd %>%
  mutate(unit_concat = paste(unit_add_new, unit_apt_new, unit_city_new, unit_state_new, unit_zip, sep = ","))


### Set up distinct addresses for parsing (remove confidential addresses and those associate with portable)
adds <- kcha_cleanadd %>%
  filter(unit_add_new != "0 PORTABLE" & str_detect(unit_add_new, "CONF") == FALSE) %>%
  distinct(unit_add_new, unit_apt_new, unit_apt2_new, unit_city_new, unit_state_new, unit_zip, unit_concat) %>%
  arrange(unit_add_new, unit_apt_new, unit_apt2_new, unit_city_new, unit_state_new, unit_zip, unit_concat)


### Parse addresses (will be useful for geocoding)
# Make empty list to add data to
addlist = list()
# Loop over addresses
for (i in 1:nrow(adds)) {
  add <- addparser$tag(adds$unit_concat[i])[1]
  addlist[[i]] <- toString(add[[1]])
}

adds_parsed <- as.data.frame(reduce(addlist, c))
adds_parsed <- rename(adds_parsed, unit_parsed = `reduce(addlist, c)`)

adds_parsed <- cbind(adds, adds_parsed)

# Separate out parsed data
addresses <- adds_parsed %>%
  mutate(address_num = str_sub(unit_parsed, str_locate(unit_parsed, "AddressNumber', '")[,2] + 1,
                               str_locate(unit_parsed, "AddressNumber', '[\\w|\\s]+'\\),")[,2] - 3),
         street_name_pre_dir = str_sub(unit_parsed, str_locate(unit_parsed, "StreetNamePreDirectional', '")[,2] + 1,
                               str_locate(unit_parsed, "StreetNamePreDirectional', '[\\w|\\s]+'\\),")[,2] - 3),
         street_name = str_sub(unit_parsed, str_locate(unit_parsed, "StreetName', '")[,2] + 1,
                               str_locate(unit_parsed, "StreetName', '[\\w|\\s]+'\\),")[,2] - 3),
         street_type = str_sub(unit_parsed, str_locate(unit_parsed, "StreetNamePostType', '")[,2] + 1,
                               str_locate(unit_parsed, "StreetNamePostType', '[\\w|\\s]+'\\),")[,2] - 3),
         street_name_post_dir = str_sub(unit_parsed, str_locate(unit_parsed, "StreetNamePostDirectional', '")[,2] + 1,
                               str_locate(unit_parsed, "StreetNamePostDirectional', '[\\w|\\s]+'\\),")[,2] - 3),
         occupancy_type = str_sub(unit_parsed, str_locate(unit_parsed, "OccupancyType', '")[,2] + 1,
                               str_locate(unit_parsed, "OccupancyType', '[\\w|\\s]+'\\),")[,2] - 3),
         occupancy_id = str_sub(unit_parsed, str_locate(unit_parsed, "OccupancyIdentifier', '")[,2] + 1,
                               str_locate(unit_parsed, "OccupancyIdentifier', '[\\w|\\s]+'\\),")[,2] - 3),
         place_name = str_sub(unit_parsed, str_locate(unit_parsed, "PlaceName', '")[,2] + 1,
                               str_locate(unit_parsed, "PlaceName', '[\\w|\\s]+'\\),")[,2] - 3),
         state = str_sub(unit_parsed, str_locate(unit_parsed, "StateName', '")[,2] + 1,
                               str_locate(unit_parsed, "StateName', '[\\w|\\s]+'\\),")[,2] - 3),
         zip = str_sub(unit_parsed, str_locate(unit_parsed, "ZipCode', '")[,2] + 1,
                         str_locate(unit_parsed, "ZipCode', '[\\w|\\s]+'\\),")[,2] - 3)
         )
         





#### Consolidate address rows ####
### Order by program type and date
# Note: need to sort differently depending on SSN (assumes that most junk SSNs represent multiple people
# whereas a normal SSN represents one person regardless of name differences)
kcha_cleanadd_junkssn <- kcha_cleanadd %>%
  filter(ssn_new_m3 < 100000 | ssn_new_m3 == 999999999 | ssn_new_m3 == 123456789 | ssn_new_m3 == 111111111 | is.na(ssn_new_m3)) %>%
  arrange(ssn_new_m3, lname_new_m3, fname_new_m3, program_type, act_date) %>%
  mutate(ssn_type = 1)
# Set up a unique ID for each person
kcha_cleanadd_junkssn$pid <- group_indices(kcha_cleanadd_junkssn, ssn_new_m3, lnamesuf_new, fname_new_m3)

kcha_cleanadd_normssn <- kcha_cleanadd %>%
  filter(!(ssn_new_m3 < 100000 | ssn_new_m3 == 999999999 | ssn_new_m3 == 123456789 | ssn_new_m3 == 111111111 | is.na(ssn_new_m3))) %>%
  arrange(ssn_new_m3, program_type, act_date) %>%
  mutate(ssn_type = 2)
# Set up a unique ID and adjust for the junk SSN unique IDs
kcha_cleanadd_normssn$pid <- group_indices(kcha_cleanadd_normssn, ssn_new_m3) + max(kcha_cleanadd_junkssn$pid)


# Join back with differential sorting
kcha_cleanadd_sort <- bind_rows(kcha_cleanadd_junkssn, kcha_cleanadd_normssn)


### Remove annual reexaminations/intermediate visits if address is the same
# Want to avoid capturing the first or last row for a person at a given address
kcha_cleanadd_sort <- kcha_cleanadd_sort %>%
  mutate(drop = if_else(
    (
      (ssn_type == 1 &
       lname_new_m3 == lag(lname_new_m3, 1) &  lname_new_m3 == lead(lname_new_m3, 1) & 
       fname_new_m3 == lag(fname_new_m3, 1) & fname_new_m3 == lead(fname_new_m3, 1)) |
        ssn_type == 2
      ) &
      (ssn_new_m3 == lag(ssn_new_m3, 1) | (is.na(ssn_new_m3) & is.na(lag(ssn_new_m3, 1)))) & 
      program_type == lag(program_type, 1) & program_type == lead(program_type, 1) &
      # Think about if action type is important or whether to just focus on addresses
      # act_type %in% c(2, 3, 14) & 
      unit_concat == lag(unit_concat, 1) & unit_concat == lead(unit_concat, 1), 
    1, 0)) %>%
  filter(drop == 0)


### Create start and end dates for a person at that address
kcha_cleanadd_sort <- kcha_cleanadd_sort %>%
  # First row for a person = least recent of act_date or admit_date
  # Other rows where that is the person's first row at that address = act_date
  mutate(startdate = as.Date(ifelse(
    (
      (ssn_type == 1 & ((ssn_new_m3 == lag(ssn_new_m3, 1) | (is.na(ssn_new_m3) & is.na(lag(ssn_new_m3, 1)))) & 
                          (lname_new_m3 != lag(lname_new_m3, 1) | fname_new_m3 != lag(fname_new_m3, 1)))) |
         (ssn_type == 1 & ssn_new_m3 != lag(ssn_new_m3, 1)) |
        (ssn_type == 2 & ssn_new_m3 != lag(ssn_new_m3, 1))
         ) |
      # account for first row
      (!is.na(ssn_new_m3) & !is.na(lname_new_m3) & is.na(lag(ssn_new_m3, 1)) & is.na(lag(lname_new_m3, 1))),
    pmin(act_date, admit_date, na.rm = TRUE),
    # treat start of different program as above
    ifelse(program_type != lag(program_type, 1),
           pmin(act_date, admit_date, na.rm = TRUE),
           # look within a program
           ifelse(unit_concat != lag(unit_concat, 1), act_date,
                  NA))),
    origin = "1970-01-01"),
    # Last row for a person = exit date or today's date or act_date + 3 years
    # Other rows where that is the person's last row at that address = act_date at next address - 1 day
    enddate = as.Date(ifelse(act_type == 6, act_date,
                             ifelse(
                               (
                                 (ssn_type == 1 & ((ssn_new_m3 == lead(ssn_new_m3, 1) | (is.na(ssn_new_m3) & is.na(lead(ssn_new_m3, 1)))) & 
                                                     (lname_new_m3 != lead(lname_new_m3, 1) | fname_new_m3 != lead(fname_new_m3, 1)))) |
                                   (ssn_type == 1 & ssn_new_m3 != lead(ssn_new_m3, 1)) |
                                   (ssn_type == 2 & ssn_new_m3 != lead(ssn_new_m3, 1))
                                 ) |
                                 # account for last row
                                 (!is.na(lname_new_m3) & is.na(lead(lname_new_m3, 1))),
                               pmin(today(), act_date + dyears(3), na.rm = TRUE),
                               # treat end of program as above
                               ifelse(program_type != lead(program_type, 1),
                                      pmin(today(), act_date + dyears(3), na.rm = TRUE),
                                      # look within a program
                                      ifelse(unit_concat != lead(unit_concat, 1) & act_date != lead(act_date, 1),
                                             lead(act_date, 1) - 1,
                                             ifelse(unit_concat != lead(unit_concat, 1) & act_date == lead(act_date, 1),
                                                    lead(act_date, 1),
                                                    NA))))),
                           origin = "1970-01-01")
         )



### Collapse rows to have a single line per person per address per time
kcha_cleanadd_sort <- kcha_cleanadd_sort %>%
  # Remove rows at an address that are neither the start or end of a time there
  filter(!(is.na(startdate) & is.na(enddate))) %>%
  mutate(enddate = as.Date(ifelse(is.na(enddate), lead(enddate, 1), enddate), origin = "1970-01-01")) %>%
  filter(!(is.na(startdate)) & !(is.na(enddate)))



### Once code is settled, remove interim data frames
# rm(pairs1, pairs1_full, pairs2, pairs2_full, pairs3, pairs3_full, yt_new, yt_new2, yt_new3, yt_complete, yt_complete2)

