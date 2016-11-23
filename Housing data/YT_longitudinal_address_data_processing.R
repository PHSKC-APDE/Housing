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


##### Bring in merged SHA data #####
# Import YT data
yt <- read.csv(file = "//phdata01/DROF_DATA/DOH DATA/Housing/LIPH_YT_2004_2016.csv")

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


### SSN
# Clean up SSNs
yt$ssnnew <- as.numeric(as.character(str_replace_all(yt$ssn, "-", "")))
yt$hofh_ssnnew <- as.numeric(as.character(str_replace_all(yt$hofh_ssn, "-", "")))

# Find most common SSN by last + first name (and DOB)
yt <- yt %>%
  arrange(lname, fname, ssnnew) %>%
  group_by(lname, fname, ssnnew) %>%
  mutate(ssnnew_cnt = n()) %>%
  ungroup() %>%
  arrange(lname, fname, dob, ssnnew) %>%
  group_by(lname, fname, dob, ssnnew) %>%
  mutate(ssnnewdob_cnt = n()) %>%
  ungroup()


### Dates
# Format and strip out dob components for matching
yt <- yt %>%
  mutate_at(vars(act_date, admit_date, eexam_date), funs(as.Date(., format = "%m/%d/%Y"))) %>%
  mutate(dob = as.Date(dob, format = "%m/%d/%Y"),
         dob_y = as.numeric(year(dob)),
         dob_m = as.numeric(month(dob)),
         dob_d = as.numeric(day(dob)))


# Find most common DOB by SSN
yt <- yt %>%
  group_by(ssnnew, dob) %>%
  mutate(dob_cnt = n()) %>%
  ungroup()
  

### Names
# Change name case to be consistently upper case
yt <- yt %>%
  mutate_at(vars(ends_with("name")), toupper)
  
# Strip out any white space in names (mutate_at did not work)
yt <- yt %>%
  mutate(lname = trimws(lname, which = c("both")),
         fname = trimws(fname, which = c("both")),
         mname = trimws(mname, which = c("both")),
         hofh_lname = trimws(hofh_lname, which = c("both")),
         hofh_fname = trimws(hofh_fname, which = c("both")),
         hofh_mname = trimws(hofh_mname, which = c("both")))


# Make truncated first name for matching
yt <- mutate(yt, fname3 = substr(fname, 1, 3))
  

### Last name
# Find the most recent surname used (doesn't work for SSN = NA or 0)
yt <- yt %>%
  arrange(ssnnew, desc(act_date)) %>%
  group_by(ssnnew) %>%
  mutate(lname.rec = first(lname)) %>%
  ungroup()

### First name
# Clean up where middle initial seems to be in first name field
yt <- yt %>%
  mutate(fname_new = ifelse(str_detect(str_sub(fname, -2, -1), "[:space:][A-Z]") == TRUE,
                            str_sub(fname, 1, -3), fname),
         mname_new = ifelse(str_detect(str_sub(fname, -2, -1), "[:space:][A-Z]") == TRUE,
                            str_sub(fname, -1), mname))


# Count which first name appears most often
yt <- yt %>%
  group_by(ssnnew, fname_new) %>%
  mutate(fname_new_cnt = n()) %>%
  ungroup()



# Need to figure out how to ID most common or most recent last name for SSNs like 0 or NA


# Remove duplicates
yt.match1 <- distinct(yt, ssnnew, lname, fname_new, mname_new, dob_y, dob_m, dob_d, 
                      ssnnew_cnt, ssnnewdob_cnt, lname.rec, fname_new_cnt, dob_cnt)

# Divide up 

yt.match2 <- yt.match1


match1 <- RLBigDataDedup(yt.match1,
                         strcmp = c("ssnnew", "lname", "fname_new", "mname_new", "dob_y", "dob_m", "dob_d"),
                         exclude = c("lname.rec", "fname_new_cnt", "dob_cnt", "ssnnew_cnt", "ssnnewdob_cnt"))


########## TESTING AREA ################
### Looking to use probabilistic matching to tidy up names and SSNs
library(RecordLinkage)



match1 <- RLBigDataDedup(yt.match1,
                         strcmp = c("ssnnew", "lname", "fname_new", "mname_new", "dob_y", "dob_m", "dob_d"),
                         exclude = c("lname.rec", "fname_new_cnt", "dob_cnt", "ssnnew_cnt", "ssnnewdob_cnt"))

yt.match2 <- yt.match1


### Try to improve on just using last name and first 3 letters of first name
yt.test <- filter(yt, row_number() < 10001) %>%
  distinct(ssnnew, lname, fname_new, mname_new, dob_y, dob_m, dob_d, ssnnew_cnt, ssnnewdob_cnt, lname.rec, fname_new_cnt, dob_cnt)
yt.test2 <- yt.test


### Using a single data frame
temp <- compare.dedup(yt.test, strcmp = c("ssnnew", "lname", "fname_new", "mname_new", "dob_y", "dob_m", "dob_d"),
                      exclude = c("lname.rec", "fname_new_cnt", "dob_cnt", "ssnnew_cnt", "ssnnewdob_cnt"))
temp2 <- emWeights(temp)

temppairs <- getPairs(temp2, single.rows = TRUE)

# Try big data version
temp.big <- RLBigDataDedup(yt.test, strcmp = c("ssnnew", "lname", "fname_new", "mname_new", "dob_y", "dob_m", "dob_d"),
                           exclude = c("lname.rec", "fname_new_cnt", "dob_cnt", "ssnnew_cnt", "ssnnewdob_cnt"))
temp.big2 <- emWeights(temp.big)
temppairs.big <- getPairs(temp.big2, single.rows = TRUE, min.weight = 0)




### Using two duplicated data frames
temp <- compare.linkage(yt.test, yt.test2, 
                        strcmp = c("ssnnew", "lname", "fname_new", "mname_new", "dob_y", "dob_m", "dob_d"),
                        exclude = c("lname.rec", "fname_new_cnt", "dob_cnt"))
temp2 <- emWeights(temp)
temppairs <- getPairs(temp2,  single.rows = TRUE, min.weight = 0)




### Look at all pairs to guage where the cutoff should be

# Clean up pairs by removing duplicates and fixing dates
temppairs <- temppairs %>%
  filter(id1 != id2) %>%
  mutate(dob.1 = as.Date(paste(dob_y.1, dob_m.1, dob_d.1, sep = "-"), origin = "1970-01-01"),
         dob.2 = as.Date(paste(dob_y.2, dob_m.2, dob_d.2, sep = "-"), origin = "1970-01-01")) %>%
  select(id1:mname_new.1, dob.1, lname.rec.1:mname_new.2, dob.2, lname.rec.2:Weight)

temppairs <- temppairs.big %>%
  filter(id.1 != id.2) %>%
  mutate(dob.1 = as.Date(paste(dob_y.1, dob_m.1, dob_d.1, sep = "-"), origin = "1970-01-01"),
         dob.2 = as.Date(paste(dob_y.2, dob_m.2, dob_d.2, sep = "-"), origin = "1970-01-01")) %>%
  select(id.1:mname_new.1, dob.1, lname.rec.1:mname_new.2, dob.2, lname.rec.2:Weight)




# Arrange in various ways to look at the data
temppairs <- arrange(temppairs, desc(Weight), ssnnew.1, lname.1, fname_new.1, mname_new.1, dob.1)
temppairs <- arrange(temppairs, Weight, ssnnew.1, lname.1, fname_new.1, mname_new.1, dob.1)
temppairs <- arrange(temppairs, ssnnew.1, lname.1, fname_new.1, mname_new.1, dob.1, Weight)

head(temppairs)
head(temppairs[temppairs$Weight > 4, ])

# Look at lowest Weighted matching SSN apart from 0/temporary/missing
temppairs %>%
  filter(ssnnew.1 == ssnnew.2 & ssnnew.1 > 10000000 & ssnnew.1 != 111111111 &
           ssnnew.1 != 123456789) %>%
  select(-(lname.rec.1), -(fname_new_cnt.1), -(dob_cnt.1), -(lname.rec.2), -(fname_new_cnt.2), -(dob_cnt.2)) %>%
  arrange(Weight, ssnnew.1, lname.1, fname_new.1, mname_new.1, dob.1) %>%
  head(n = 20)

# Look at lowest Weighted plausible matching SSN when 0/temporary/missing
temppairs %>%
  filter(ssnnew.1 == ssnnew.2 & (ssnnew.1 < 10000000 | ssnnew.1 == 111111111 |
           ssnnew.1 == 123456789) & Weight >= 00 & fname_new.1 != "UNBORN" & fname_new.2 != "UNBORN"&
           abs(dob.1 - dob.2) < 12) %>%
  select(id.1:dob.1, id.2:dob.2, Weight) %>%
  arrange(Weight, ssnnew.1, lname.1, fname_new.1, mname_new.1, dob.1) %>%
  head(n = 20)

temppairs %>%
  filter(ssnnew.1 == 0 & Weight >= 0 & fname_new.1 != "UNBORN" & fname_new.2 != "UNBORN"&
           abs(dob.1 - dob.2) < 12) %>%
  select(-(fname_new_cnt.1), -(lname.rec.2), -(fname_new_cnt.2), -(dob_cnt.2), -(is_match)) %>%
  arrange(Weight, ssnnew.1, lname.1, fname_new.1, mname_new.1, dob.1) %>%
  head(n = 20)


# Look at lowest plausible matching pairs where SSNs do not match
temppairs %>%
  filter(ssnnew.1 != ssnnew.2 & Weight > 10 &
           abs(dob.1 - dob.2) < 12) %>%
  select(-(lname.rec.1), -(fname_new_cnt.1), -(dob_cnt.1), -(lname.rec.2), -(fname_new_cnt.2), -(dob_cnt.2)) %>%
  arrange(Weight, ssnnew.1, lname.1, fname_new.1, mname_new.1, dob.1) %>%
  head(n = 20)

temppairs %>%
  filter(ssnnew.1 != ssnnew.2 & Weight > 16) %>%
  select(-(lname.rec.1), -(fname_new_cnt.1), -(dob_cnt.1), -(lname.rec.2), -(fname_new_cnt.2), -(dob_cnt.2)) %>%
  arrange(Weight, ssnnew.1, lname.1, fname_new.1, mname_new.1, dob.1) %>%
  head(n = 20)



# Extract final matched pairs
finalpairs <- getPairs(temp2, min.weight = 5, single.rows = TRUE)
# Clean up pairs by removing duplicates
finalpairs <- finalpairs %>%
  filter(id1 != id2)


# Look at various cutoffs
finalpairs <- arrange(finalpairs, lname.1, fname_new.1, Weight)
finalpairs <- arrange(finalpairs, Weight, lname.1, fname_new.1)

head(finalpairs)
finalpairs


### Prioritize matches
finalpairs <- finalpairs %>%
  filter(((
    # Use a lower weight when the SSN matches but is missing or a dummy SSN
    ssnnew.1 == ssnnew.2 &
      (ssnnew.1 < 10000000 | ssnnew.1 == 111111111 |
         ssnnew.1 == 123456789) & Weight >= 10
  ) |
    # Use a more stringent (higher) weight when the SSN matches and is a real SSN
    (
      ssnnew.1 == ssnnew.2 & ssnnew.1 > 10000000 & ssnnew.1 != 111111111 &
        ssnnew.1 != 123456789 & Weight > 10
    ) |
    # Use the highest weight when the SSN does not match but other variables do
    (ssnnew.1 != ssnnew.2 & Weight > 16)
  ) &
    # Remove non-real names that might accidentally match and intergenerational matches
    fname_new.1 != "UNBORN" & fname_new.2 != "UNBORN" &
    abs(dob_y.1 - dob_y.2) < 12) %>%
  mutate(
    ssn.chk = ifelse(ssnnew.1 >= ssnnew.2 & ssnnew.1 < 999999999, 1, 0),
    lname.chk = ifelse(lname.1 == lname.rec.1, 1, 0),
    fname.chk = ifelse(fname_new_cnt.1 >= fname_new_cnt.2, 1, 0),
    mname.chk = ifelse(!is.na(mname_new.1), 1, 0),
    dob.chk = ifelse(dob_cnt.1 >= dob_cnt.2, 1, 0),
    keep = rowSums(
      cbind(ssn.chk, lname.chk, fname.chk, mname.chk, dob.chk),
      na.rm = TRUE
    ),
    keep2 = ifelse(ssnnew.1 >= ssnnew.2 & ssnnew.1 < 999999999 &
                     lname.1 == lname.rec.1 &
                     fname_new_cnt.1 >= fname_new_cnt.2 &
                     dob_cnt.1 >= dob_cnt.2, 1, 0)
  )



# Merge back with original and keep most promising option
finalpairs1 <- finalpairs %>%
  rename(ssnnew.use = ssnnew.1, lname.use = lname.1, fname.use = fname_new.1, mname.use = mname_new.1) %>%
  mutate(dob.use = as.Date(paste(dob_y.1, dob_m.1, dob_d.1, sep = "-"))) %>%
  select(ssnnew.use:mname.use, dob.use, ssnnew.2:dob_d.2, Weight, keep)

yt.test.merge <- left_join(yt.test, finalpairs1, by = c("ssnnew" = "ssnnew.2", "lname" = "lname.2",
                                                        "fname_new" = "fname_new.2", "mname_new" = "mname_new.2",
                                                        "dob_y" = "dob_y.2", "dob_m" = "dob_m.2",
                                                        "dob_d" = "dob_d.2"))


######### END TESTING AREA ###############








### Gender
# Count the number of genders recorded for an individual
yt <- yt %>%
  mutate(gendernew = car::recode(gender, c("'F' = 1; 'M' = 2; 'NULL' = NA; else = NA"))) %>%
  group_by(ssnnew, lname, fname3) %>%
  mutate(gender_tot = n_distinct(gender, na.rm = TRUE)) %>%
  ungroup() %>%
  # Replace multiple genders as 3
  mutate(gendernew = ifelse(gender_tot > 1, 3, gendernew))


### Race
# Recode race variables and make numeric
yt <- yt %>%
  mutate_at(vars(r_white:r_hisp), funs(as.numeric(new = car::recode(., "'Y' = 1; 'N' = 0; 'NULL' = NA; else = NA")))) %>%
  mutate_at(vars(r_white_new:r_hisp_new), funs(. - 1)) # recode goes wonky so need to correct


# Identify individuals with contradictory race values and set to Y
yt <- yt %>%
  group_by(ssnnew, lname, fname3) %>%
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
         r_multi = rowSums(cbind(r_white_new_tot, r_black_new_tot, r_aian_new_tot, r_asian_new_tot,
                                 r_nhpi_new_tot), na.rm = TRUE),
         r_multi = ifelse(r_multi_new > 1, 1, 0)) %>%
  # make new variable to look at people with one race only
  mutate_at(vars(r_white_new:r_nhpi_new), funs(alone = ifelse(r_multi_new == 1, 0, .)))


##### Consolidate address rows #####
yt <- arrange(yt, hofh_ssnnew, hofh_lname, ssnnew, lname, fname3, act_date)

# Remove annual reexaminations if address is the same
yt <- yt %>%
  group_by(ssnnew, lname, fname3)
  mutate(drop = ifelse(act_type ))
