# Excess Mortality among PHA enrollees, 2017-2021
# Script for matching/blocking, drawn from:
# https://github.com/PHSKC-APDE/hud_hears/blob/3136d6c5c6d5c48ee4bba50bf583c26a90d42d3b/data_processing/load_stage_xwalk_ids.R#L100


# 1. run functions ----

## 1.1 prep names function ----
# Function from Danny
# https://github.com/PHSKC-APDE/pers_dvc/blob/main/R/covid/COVID_Medicaid_Medicare_match/wdrs_claims_linkage.R#L37-
prep.names <- function(dt){
  # Remove any spaces in the name
  # Will lead to squishing names together but some better matches
  dt[, fname := gsub(" ", "", fname)]
  dt[, lname := gsub(" ", "", lname)]
  dt <- unique(dt)
  
  # Last names
  dt[, lname := gsub("[0-9]", "", lname)]  # remove any numbers that may be present in last name
  dt[, lname := gsub("'", "", lname)]  # remove apostrophes from last name, e.g., O'BRIEN >> OBRIEN
  dt[, lname := gsub("\\.", "", lname)]  # remove periods from last name, e.g., JONES JR. >> JONES JR
  dt[, lname := gsub(" SR$", "", lname)]  # remove all " SR" suffixes from last name
  dt[, lname := gsub(" JR$", "", lname)]  # remove all " JR" suffixes from last name
  dt[, lname := gsub("-JR$", "", lname)]  # remove all "-JR" suffixes from last name
  dt[, lname := gsub("JR$", "", lname)]  # remove all "JR" suffixes from last name
  dt[, lname := gsub("JR I$", "", lname)]  # remove all "JR I" suffixes from last name
  dt[, lname := gsub("JR II$", "", lname)]  # remove all "JR II" suffixes from last name
  dt[, lname := gsub("JR III$", "", lname)]  # remove all "JR III" suffixes from last name
  dt[, lname := gsub(" II$", "", lname)]  # remove all " II" suffixes from last name
  dt[, lname := gsub(" III$", "", lname)]  # remove all " III" suffixes from last name
  dt[, lname := gsub(" IV$", "", lname)]  # remove all " IV" suffixes from last name
  #dt[, lname := gsub("-", "", lname)]  # remove all hyphens from last name !! made matching worse, commenting out
  
  # First names
  dt[, fname := gsub("[0-9]", "", fname)]  # remove any numbers that may be present in first names
  dt[, fname := gsub("\\.", "", fname)]  # remove periods from first name, e.g., JONES JR. >> JONES JR
  dt[fname!="JR", fname := gsub("JR$", "", fname)]  # remove all "JR" suffixes from first name, but keep if it is the full first name
  dt[, fname := gsub(" JR$", "", fname)]  # remove all " JR" suffixes from first name
  dt[, fname := gsub("-JR$", "", fname)]  # remove all "-JR" suffixes from first name
  dt[, fname := gsub("JR I$", "", fname)]  # remove all "JR I" suffixes from first name
  dt[, fname := gsub("JR II$", "", fname)]  # remove all "JR II" suffixes from first name
  dt[, fname := gsub("JR III$", "", fname)]  # remove all "JR III" suffixes from first name
  dt[, fname := gsub("-", "", fname)]  # remove all hyphens from first names because use is inconsistent. Found that replacing with a space was worse
  
  if ("mname" %in% names(dt)) {
    # Middle initials
    dt[, mname := gsub(" $", "", mname)] # remove blank spaces at end of name
    dt[, mname := gsub("^ ", "", mname)] # remove blank spaces at the beginning of name
    
    dt[, mname := gsub("[0-9]", "", mname)]  # remove any numbers that may be present in middle initial
    dt[!(grep("[A-Z]", mname)), mname := NA] # when middle initial is not a letter, replace it with NA
  }
  
  return(dt)
}

## 1.2 validate ssn function ----
# function from Danny
# https://github.com/PHSKC-APDE/Housing/blob/main/R/validate_ssn.R
# tidies and standardizes a designated SSN column when 
# possible and sets the value equal to NA when the value is clearly not an SSN. 
validate_ssn <- function(DTx, ssnx = NULL){
  setDT(DTx)
  # ensure nine digits long ----
  DTx[, paste0(ssnx) := as.character(get(ssnx))]
  DTx[, paste0(ssnx) := gsub('\\D+','', get(ssnx))] # only keep numbers 
  DTx[nchar(get(ssnx)) > 9, paste0(ssnx) := NA] # drop if SSN is > 9 digits
  DTx[nchar(get(ssnx)) < 7, paste0(ssnx) := NA] # drop if SSN is < 7 digits (b/c can only have two preceding zeros)
  DTx[nchar(get(ssnx)) < 9, paste0(ssnx) := gsub(" ", "0", sprintf("%9i", as.numeric(get(ssnx))))] # add preceding zeros to make 9 digits
  
  # drop illogical values with regular expressions ---- 
  # https://www.ssa.gov/employer/randomizationfaqs.html
  DTx[!grepl("[0-8]{1}[0-9]{2}[0-9]{2}[0-9]{4}", get(ssnx)), paste0(ssnx) := NA] # generalized acceptable pattern
  DTx[grepl("^000|^666|^9[0-9][0-9]", get(ssnx)), paste0(ssnx) := NA ] # drop if first three digits are forbidden
  DTx[grepl("^[0-9]{3}00", get(ssnx)), paste0(ssnx) := NA ] # drop if middle two digits are forbidden
  DTx[grepl("^[0-9]{5}0000", get(ssnx)), paste0(ssnx) := NA ] # drop if last four digits are forbidden 
  
  # drop specific known false SSN ----
  DTx[get(ssnx) == c("078051120"), paste0(ssnx) := NA] # Woolworth wallet SSN
  DTx[get(ssnx) == c("219099999"), paste0(ssnx) := NA] # SS Administration advertisement SSN
  DTx[get(ssnx) == c("457555462"), paste0(ssnx) := NA] # Lifelock CEO Todd Davis (at least 13 cases of identity theft)
  DTx[get(ssnx) == c("123456789"), paste0(ssnx) := NA] # garbage filler
  DTx[get(ssnx) %in% c("010010101", "011111111", "011223333", "111111111", 
                       "111119999", "111223333", "112234455", "123121234", 
                       "123123123", "123456789", "222111212", "222332222", 
                       "333333333", "444444444", "555112222", "555115555", 
                       "555555555", "555555566", "699999999", "888888888", 
                       "898888899", "898989898"), 
      paste0(ssnx) := NA] # additional unacceptable SSN
  return(DTx)
  
}




# 2. prep data for matching ----

# 2.1 prep pha data ----
# select unique PHA IDs by agency
pha_id <- final_timevar %>% 
  dplyr::select(KCMASTER_ID, agency) %>% 
  unique()

# create pha dataframe for matching 
pha <- final_identities %>% 
  # keep only those in public housing between 2017-2021
  filter(KCMASTER_ID %in% pha_id$KCMASTER_ID) %>%
  # keep only those observed for at least one month
  filter(KCMASTER_ID %in% obs_by_month$KCMASTER_ID)

# data cleaning
pha <- pha %>%
  rename(
    # To use "mname" as the matching variable for middle initial
    # (as formatted in kc_deaths), first rename for manipulation
    middle_name = mname
  ) %>%
  mutate(
    # create linking variable for middle initial
    mname = substr(middle_name,1,1),
    # count number of missing identifiers
    n_miss = rowSums(is.na(.))
  ) %>%
  group_by(KCMASTER_ID) %>%
  # arrange where the most complete case within a given PHA ID is at the top
  arrange(n_miss) %>%
  mutate(
    # create grouped row numbers
    obs_num = row_number(),
    # count number of different values within groups of PHA IDs
    flag_dob = n_distinct(dob),
    flag_ssn = n_distinct(ssn),
    flag_fname = n_distinct(fname),
    flag_lname = n_distinct(lname)
    #flag_mname = n_distinct(mname)
  ) %>%
  ungroup() %>%
  # drop middle name, in favor of using middle initial for linking 
  dplyr::select(-middle_name) %>%
  # prep pha names
  as.data.table() %>%
  ## drop debugging variables that make rows non-unique
  dplyr::select(-c(n_miss, obs_num)) %>%
  ## run prep.names function
  prep.names() %>% 
  ## keep unique row
  unique() %>%
  # convert to dataframe
  as.data.frame() %>% 
  # format identifiers for blocking
  mutate(
    date_of_birth_day = lubridate::day(dob),
    date_of_birth_month = lubridate::month(dob),
    date_of_birth_year = lubridate::year(dob),
    ssn4 = str_sub(ssn,-4,-1),
    fname_c1 = str_sub(fname,1,1),
    # ID based on row number
    rowid = row_number()
  )

# run validate ssn function
pha <- validate_ssn(pha, "ssn")


# 2.2 prep deaths data ----

# run validate ssn function
kc_deaths <- validate_ssn(kc_deaths, "ssn")

# additional deaths data cleaning
## prep names
kc_deaths <- prep.names(kc_deaths) %>%  
  # other data cleaning
  as.data.frame() %>% 
  # format identifiers for blocking & linking
  mutate(
    # five category age var, collapse <18 and 18-24
    age5 = ifelse(age6 == "<18" | age6 == "18-24", "<25", age6),
    # create blocking keys
    ssn4 = str_sub(ssn,-4,-1),
    fname_c1 = str_sub(fname,1,1),
    # ID based on row number
    rowid = nrow(pha) + row_number()
  )



# 2.3 create single data frame ----
# identify id variables ----
id_vars <- c(
  "rowid","date_of_birth_day","date_of_birth_month","date_of_birth_year",
  "dob","ssn","ssn4","lname","fname","fname_c1"
)

# subset for matching
m_pha <- pha %>% 
  dplyr::select(all_of(id_vars),"KCMASTER_ID") %>%
  mutate(source = "pha")

m_deaths <- kc_deaths %>% 
  dplyr::select(all_of(id_vars)) %>% 
  mutate(source = "deaths")

# create single data frame for linkage via deduplication
pha_deaths <- bind_rows(
  m_pha,
  m_deaths
) %>%
  # Add phonics and set up a rowid for self-joining later
  mutate(
    lname_phon = RecordLinkage::soundex(lname),
    fname_phon = RecordLinkage::soundex(fname),
    rowid = row_number()
  )

## 2.4 assess missingness ----
#library(naniar)
## public housing data
#m_pha %>% select("ssn","fname","lname","dob") %>% vis_miss()
## deaths data
#m_deaths %>% select("ssn","fname","lname","dob") %>% vis_miss()

# 3. Block 0 (deterministic) ----
# perfect matches 
# matching on ssn, dob, lname and fname
pairs0 <- m_pha %>%
  inner_join(
    m_deaths, 
    by = c(id_vars[-1])
  ) %>% 
  # remove duplicates
  group_by(KCMASTER_ID) %>%
  slice(1) %>%
  ungroup()
# n=3104

# 4. Block 1 ----
## 4.1 set up ----
# remove matches
pha_deaths <- pha_deaths %>% filter(! rowid %in% pairs0$rowid.x & ! rowid %in% pairs0$rowid.y)

## 4.2 perform linkage ----
match1 <- RecordLinkage::compare.dedup(
  pha_deaths, 
  blockfld = "ssn", 
  strcmp = c("lname", "fname", "date_of_birth_day", "date_of_birth_month", "date_of_birth_year"), 
  exclude = c("KCMASTER_ID","source", "rowid")
)

# add epi link weights
match1 <- epiWeights(match1)
# summary(match1)
# hist(match1$Wdata)

# get pairs
pairs1 <- getPairs(match1, single.rows = TRUE) %>%
  # pha <-> deaths links only
  filter(source.1 != source.2) %>%
  # row ids as integer (instead of character)
  mutate(
    rowid.1 = as.integer(rowid.1),
    rowid.2 = as.integer(rowid.2)
  ) %>% setDT()

# remove duplicates by selecting highest weight
## >1 PHA record on a death record
pairs1[, count1 := 1:.N, rowid.1]
## pairs1[count1 > 1] 
## none
pairs1 <- pairs1[count1 == 1] # note: not necessary since all count1==1
## >1 death record on a PHA record
pairs1[, count2 := 1:.N, rowid.2]
## pairs1[count2 > 1] #n=174 
pairs1 <- pairs1[count2 == 1] #389

# summary(pairs1$Weight)
# hist(pairs1$Weight)
# max = .999
# min = .360

## 4.3 determine appropriate threshold ----
# view bottom 15 by weight
tail(pairs1, 15) %>% 
  select(
    id1, rowid.1, source.1, dob.1, lname.1, fname.1, 
    rowid.2, source.2, dob.2, lname.2, fname.2, KCMASTER_ID.1, 
    Weight
  )

# names transposed, dob matches
pairs1 %>% filter(
  lname.1 == fname.2 & lname.2 == fname.1) %>% 
  select(id1, rowid.1, source.1, dob.1, lname.1, fname.1, rowid.2, source.2, dob.2, lname.2, fname.2, Weight)
# n = 2
# weights = 0.71-0.868

# year of birth is different, day of birth matches, month of birth matches, names match
pairs1 %>% filter(
  date_of_birth_year.1 != date_of_birth_year.2 &
    date_of_birth_day.1 == date_of_birth_day.2 & date_of_birth_month.1 == date_of_birth_month.2 &
    lname.1 == lname.2 & fname.1 == fname.2) %>% 
  select(id1, rowid.1, source.1, dob.1, lname.1, fname.1, rowid.2, source.2, dob.2, lname.2, fname.2, KCMASTER_ID.1, Weight) 
# n = 21
# weights = 0.868-0.859

# year of birth is different, no name check
pairs1 %>% filter(
  date_of_birth_year.1 != date_of_birth_year.2 & 
    date_of_birth_day.1 == date_of_birth_day.2 & date_of_birth_month.1 == date_of_birth_month.2) %>% 
  select(id1, rowid.1, source.1, dob.1, lname.1, fname.1, rowid.2, source.2, dob.2, lname.2, fname.2, KCMASTER_ID.1, Weight)
# matches section with name check included. use this one

# transposed month and day of birth, no name check
pairs1 %>% filter(
  date_of_birth_day.1 == date_of_birth_month.2 & date_of_birth_day.2 == date_of_birth_month.1 & 
    #lname.1 == lname.2 & fname.1 == fname.2 &
    date_of_birth_year.1 == date_of_birth_year.2 & dob.1 != dob.2) %>% 
  select(id1, rowid.1, source.1, dob.1, lname.1, fname.1, rowid.2, source.2, dob.2, lname.2, fname.2, KCMASTER_ID.1, Weight)


# DOB matches, first name matches, last name does not
pairs1 %>% filter(
  dob.1 == dob.2 & fname.1 == fname.2 & 
    lname.1 != lname.2) %>% 
  select(id1, rowid.1, source.1, dob.1, lname.1, fname.1, rowid.2, source.2, dob.2, lname.2, fname.2, KCMASTER_ID.1, Weight)

# DOB matches, first and last names do not
pairs1 %>% filter(
  dob.1 == dob.2 & fname.1 != fname.2 & 
    lname.1 != lname.2) %>% 
  select(id1, rowid.1, source.1, dob.1, lname.1, fname.1, rowid.2, source.2, dob.2, lname.2, fname.2, KCMASTER_ID.1, Weight) %>%
  tail(15)

## 4.4 final filtering ----
#' threshold of 0.7 determined based on visual inspection of weights under 
#' different scenarios of administrative errors and confidence in SSN being a 
#' unique identifier
pairs1 <- pairs1 %>% filter(Weight >= 0.70) # n=379

# 5. Block 2 ----
## 5.1 set up ----
# remove matches
pha_deaths <- pha_deaths %>% filter(! rowid %in% pairs1$rowid.1 & ! rowid %in% pairs1$rowid.2)

## 5.2 perform linkage ----
match2 <- RecordLinkage::compare.dedup(
  pha_deaths, 
  blockfld = c("dob"),
  strcmp = c("ssn","lname","fname"), 
  exclude = c("KCMASTER_ID","source", "rowid")
)

# add epi link weights
match2 <- epiWeights(match2)
# summary(match2)
# hist(match2$Wdata)

# get pairs
pairs2 <- getPairs(match2, single.rows = TRUE) %>%
  # pha <-> deaths links only
  filter(source.1 != source.2) %>%
  # row ids as integer (instead of character)
  mutate(
    rowid.1 = as.integer(rowid.1),
    rowid.2 = as.integer(rowid.2)
  ) %>% setDT()

# remove duplicates by selecting highest weight
## >1 PHA record on a death record
pairs2[, count1 := 1:.N, rowid.1]
## pairs2[count1 > 1] #n=67349
pairs2 <- pairs2[count1 == 1] 
## >1 death record on a PHA record
pairs2[, count2 := 1:.N, rowid.2]
## pairs2[count2 > 1] #n=31994 
pairs2 <- pairs2[count2 == 1] #19724

# summary(pairs2$Weight)
# hist(pairs2$Weight)
# max = 0.2555  
# min = 0.9952 

## 5.3 determine appropriate threshold ----
# visual inspection of plausible weight thresholds
pairs2 %>% filter(Weight>0.81) %>% 
  select(
    id1, rowid.1, source.1, ssn.1, lname.1, fname.1, 
    rowid.2, source.2, ssn.2, lname.2, fname.2, KCMASTER_ID.1, 
    Weight
  )

# last 4 ssn matches, first name matches, last name differs
pairs2 %>% filter(
  ssn4.1 == ssn4.2 & fname.1 == fname.2 & lname.1 != lname.2) %>% 
  select(id1, rowid.1, source.1, dob.1, lname.1, fname.1, ssn.1, rowid.2, source.2, dob.2, lname.2, fname.2, ssn.2, Weight)

# last 4 ssn matches, last name matches, first name differs
pairs2 %>% filter(
  ssn4.1 == ssn4.2 & fname.1 != fname.2 & lname.1 == lname.2) %>% 
  select(id1, rowid.1, source.1, dob.1, lname.1, fname.1, ssn.1, rowid.2, source.2, dob.2, lname.2, fname.2, ssn.2, Weight)

# first and last names match, SSN is na
pairs2 %>% filter(
  fname.1 == fname.2 & lname.1 == lname.2 & (is.na(ssn.1) | is.na(ssn.2))) %>% 
  select(id1, rowid.1, source.1, dob.1, lname.1, fname.1, ssn.1, rowid.2, source.2, dob.2, lname.2, fname.2, ssn.2, Weight)

# first and last names match, SSN differs
pairs2 %>% filter(
  fname.1 == fname.2 & lname.1 == lname.2 & ssn.1 != ssn.2) %>% 
  select(id1, rowid.1, source.1, dob.1, lname.1, fname.1, ssn.1, rowid.2, source.2, dob.2, lname.2, fname.2, ssn.2, Weight)

## 5.4 final filtering ----
pairs2 <- pairs2 %>% 
  filter(
    # names transposed
    (lname.1 == fname.2 & lname.2 == fname.1) |
      # last 4 ssn matches, first name matches, last name differs
      (ssn4.1 == ssn4.2 & fname.1 == fname.2 & lname.1 != lname.2) |
      # last 4 ssn matches, last name matches, first name differs
      (ssn4.1 == ssn4.2 & fname.1 != fname.2 & lname.1 == lname.2) |
      # first and last names match, SSN is na on one or more source
      (fname.1 == fname.2 & lname.1 == lname.2 & (is.na(ssn.1) | is.na(ssn.2))) |
      # first and last names match, SSN differs
      (fname.1 == fname.2 & lname.1 == lname.2 & ssn.1 != ssn.2) |
      # all other plausible matches
      Weight > 0.81
  )
# n=490

# 6. Block 3 ----
## 6.1 set up ----
# remove matches
pha_deaths <- pha_deaths %>% filter(! rowid %in% pairs2$rowid.1 & ! rowid %in% pairs2$rowid.2)

## 6.2 perform linkage ----
match3 <- RecordLinkage::compare.dedup(
  pha_deaths, 
  blockfld = c("dob","lname_phon","fname_phon"),
  strcmp = c("lname","fname"), 
  exclude = c("KCMASTER_ID","source", "rowid")
)

# add epi link weights
match3 <- epiWeights(match3)
# summary(match3)
# hist(match3$Wdata)

# get pairs
pairs3 <- getPairs(match3, single.rows = TRUE) %>%
  # pha <-> deaths links only
  filter(source.1 != source.2) %>%
  # row ids as integer (instead of character)
  mutate(
    rowid.1 = as.integer(rowid.1),
    rowid.2 = as.integer(rowid.2)
  ) %>% setDT()

# remove duplicates by selecting highest weight
## >1 PHA record on a death record
pairs3[, count1 := 1:.N, rowid.1]
## pairs3[count1 > 1] #n=0
pairs3 <- pairs3[count1 == 1] 
## >1 death record on a PHA record
pairs3[, count2 := 1:.N, rowid.2]
## pairs3[count2 > 1] #n=5 
pairs3 <- pairs3[count2 == 1] #29

# summary(pairs3$Weight)
# hist(pairs3$Weight)
# max = 0.699  
# min = 0.745 

## 6.3 determine appropriate threshold ----
# visual inspection of plausible weight thresholds
pairs3 %>% 
  select(
    id1, rowid.1, source.1, ssn.1, dob.1, lname.1, fname.1, 
    rowid.2, source.2, ssn.2, dob.2, lname.2, fname.2, KCMASTER_ID.1, 
    Weight
  )


## 6.4 final filtering ----
#' threshold of 0.73 determined based on visual inspection of 29 potential matches
pairs3 <- pairs3 %>%  filter(Weight > 0.73)
# n=27

# 7. Final matched data ----
block0 <- pairs0 %>% 
  left_join(pha, by = c("rowid.x" = "rowid", "KCMASTER_ID")) %>%
  left_join(kc_deaths, by = c("rowid.y" = "rowid"))

## Block 1
block1 <- pairs1 %>% 
  left_join(pha, by = c("rowid.1" = "rowid", "KCMASTER_ID.1" = "KCMASTER_ID")) %>%
  select(-KCMASTER_ID.2) %>%
  rename(KCMASTER_ID = KCMASTER_ID.1) %>%
  left_join(kc_deaths, by = c("rowid.2" = "rowid"))
## Block 2
block2 <- pairs2 %>% 
  left_join(pha, by = c("rowid.1" = "rowid", "KCMASTER_ID.1" = "KCMASTER_ID")) %>%
  select(-KCMASTER_ID.2) %>%
  rename(KCMASTER_ID = KCMASTER_ID.1) %>%
  left_join(kc_deaths, by = c("rowid.2" = "rowid"))
## Block 3
block3 <- pairs3 %>% 
  left_join(pha, by = c("rowid.1" = "rowid", "KCMASTER_ID.1" = "KCMASTER_ID")) %>%
  select(-KCMASTER_ID.2) %>%
  rename(KCMASTER_ID = KCMASTER_ID.1) %>%
  left_join(kc_deaths, by = c("rowid.2" = "rowid"))

# bind rows
match <- bind_rows(block0, block1, block2, block3,.id = "match_type") %>% 
  mutate(match_type = as.factor(as.numeric(match_type)-1))
# note: match_type == 0: deterministic, match_type %in% c(1,2,3): probabilistic

