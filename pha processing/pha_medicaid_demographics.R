

library(pastecs) # Used for summary statistics

# Header, libraries etc

#pha_elig_final <- readRDS(file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/pha_elig_final.Rda")

##### Look at demographics of overlapping PHA and Medicaid #####


### Set up person-time each year
# First set up intervals for each year
i2012 <- interval(start = "2012-01-01", end = "2012-12-31")
i2013 <- interval(start = "2013-01-01", end = "2013-12-31")
i2014 <- interval(start = "2014-01-01", end = "2014-12-31")
i2015 <- interval(start = "2015-01-01", end = "2015-12-31")
i2016 <- interval(start = "2016-01-01", end = "2016-12-31")

# Person-time in housing, needs to be done separately to avoid errors
pt_temp_h <- pha_elig_final %>%
  distinct(ssn_new_pha, lname_new.1, fname_new.1, dob_h, startdate_h, enddate_h) %>%
  filter(!is.na(startdate_h)) %>%
  mutate(
    pt12_h = (lubridate::intersect(interval(start = startdate_h, end = enddate_h), i2012) / ddays(1)) + 1,
    pt13_h = (lubridate::intersect(interval(start = startdate_h, end = enddate_h), i2013) / ddays(1)) + 1,
    pt14_h = (lubridate::intersect(interval(start = startdate_h, end = enddate_h), i2014) / ddays(1)) + 1,
    pt15_h = (lubridate::intersect(interval(start = startdate_h, end = enddate_h), i2015) / ddays(1)) + 1,
    pt16_h = (lubridate::intersect(interval(start = startdate_h, end = enddate_h), i2016) / ddays(1)) + 1
  )

pha_elig_final <- left_join(pha_elig_final, pt_temp_h, by = c("ssn_new_pha", "lname_new.1", "fname_new.1", "dob_h", "startdate_h", "enddate_h"))
rm(pt_temp_h)

# Person-time in Medicaid, needs to be done separately to avoid errors
pt_temp_m <- pha_elig_final %>%
  filter(!is.na(startdate_m)) %>%
  distinct(ssn_new_pha, lname_new.1, fname_new.1, dob_h, startdate_m, enddate_m) %>%
  mutate(
    pt12_m = (lubridate::intersect(interval(start = startdate_m, end = enddate_m), i2012) / ddays(1)) + 1,
    pt13_m = (lubridate::intersect(interval(start = startdate_m, end = enddate_m), i2013) / ddays(1)) + 1,
    pt14_m = (lubridate::intersect(interval(start = startdate_m, end = enddate_m), i2014) / ddays(1)) + 1,
    pt15_m = (lubridate::intersect(interval(start = startdate_m, end = enddate_m), i2015) / ddays(1)) + 1,
    pt16_m = (lubridate::intersect(interval(start = startdate_m, end = enddate_m), i2016) / ddays(1)) + 1
  )

pha_elig_final <- left_join(pha_elig_final, pt_temp_m, by = c("ssn_new_pha", "lname_new.1", "fname_new.1", "dob_h", "startdate_m", "enddate_m"))
rm(pt_temp_m)

# Person-time in both, needs to be done separately to avoid errors
pt_temp_o <- pha_elig_final %>%
  filter(!is.na(startdate_o)) %>%
  distinct(ssn_new_pha, lname_new.1, fname_new.1, dob_h, startdate_o, enddate_o) %>%
  mutate(
    pt12_o = (lubridate::intersect(interval(start = startdate_o, end = enddate_o), i2012) / ddays(1)) + 1,
    pt13_o = (lubridate::intersect(interval(start = startdate_o, end = enddate_o), i2013) / ddays(1)) + 1,
    pt14_o = (lubridate::intersect(interval(start = startdate_o, end = enddate_o), i2014) / ddays(1)) + 1,
    pt15_o = (lubridate::intersect(interval(start = startdate_o, end = enddate_o), i2015) / ddays(1)) + 1,
    pt16_o = (lubridate::intersect(interval(start = startdate_o, end = enddate_o), i2016) / ddays(1)) + 1
  )

pha_elig_final <- left_join(pha_elig_final, pt_temp_o, by = c("ssn_new_pha", "lname_new.1", "fname_new.1", "dob_h", "startdate_o", "enddate_o"))
rm(pt_temp_o)

# Quick count of people enrolled at any point in 2015
temp <- pha_elig_final %>% 
  filter(pt15_h > 0) %>% 
  group_by(ssn_new_pha, lname_new.1, fname_new.1, dob_h) %>% 
  summarise(pt15_h = sum(pt15_h, na.rm = T), pt15_o = sum(pt15_o, na.rm = T)) %>% 
  ungroup() 
temp %>% 
  mutate(
    pt15_h_ever = ifelse(is.na(pt15_h), 0, ifelse(pt15_h > 0, 1, 0)), 
    pt15_o_ever = ifelse(is.na(pt15_o), 0, ifelse(pt15_o > 0, 1, 0))) %>% 
  summarise(pha = sum(pt15_h_ever, na.rm = T), overlap = sum(pt15_o_ever, na.rm = T))


### Set up a particular year (2015)
# Enrolled in housing as of December 31, 2015
pha_elig_demogs <- pha_elig_final %>%
  mutate(
    h2015 = ifelse(startdate_h <= as.Date("2015-12-31", origin = "1970-01-01") & 
                     enddate_h >= as.Date("2015-12-31", origin = "1970-01-01"),
                   1, 0),
    m2015 = ifelse(startdate_m <= as.Date("2015-12-31", origin = "1970-01-01") & 
                     enddate_m >= as.Date("2015-12-31", origin = "1970-01-01"),
                   1, 0),
    o2015 = ifelse(startdate_o <= as.Date("2015-12-31", origin = "1970-01-01") & 
                     enddate_o >= as.Date("2015-12-31", origin = "1970-01-01"),
                   1, 0)
  )

# Enrolled in Medicaid as of December 31, 2015
elig <- elig %>%
  mutate(
    m2015 = ifelse(startdate <= as.Date("2015-12-31", origin = "1970-01-01") & 
                     enddate >= as.Date("2015-12-31", origin = "1970-01-01"),
                   1, 0)
  )


# Age at each date
pha_elig_demogs <- pha_elig_demogs %>%
  mutate(age15 = round(interval(start = dob.2, end = ymd(20151231)) / years(1), 1)
  )



### Count per year
# Housing
pha_elig_demogs %>% filter(h2015 == 1) %>% distinct(ssn_new_pha, lname_new.1, fname_new.1, dob_h) %>% summarise(count = n())
# Mediciad
elig %>% filter(m2015 == 1) %>% distinct(MEDICAID_RECIPIENT_ID, SOCIAL_SECURITY_NMBR) %>% summarise(count = n())
pha_elig_demogs %>% filter(m2015 == 1) %>% distinct(ssn_new_pha, lname_new.1, fname_new.1, dob_h) %>% summarise(count = n())
# Overlapping
pha_elig_demogs %>% filter(o2015 == 1) %>% distinct(ssn_new_pha, lname_new.1, fname_new.1, dob_h) %>% summarise(count = n())


### Output to Excel for use in Tableau
temp <- pha_elig_demogs %>%
  filter(h2015 == 1) %>%
  distinct(ssn_new_pha, lname_new.1, fname_new.1, dob_h, .keep_all = TRUE)

write.xlsx(temp, file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/pha_elig_demogs.xlsx")



### Gender per year
pha_elig_demogs %>%
  filter(h2015 == 1) %>%
  distinct(ssn_new_pha, lname_new.1, fname_new.1, dob_h, .keep_all = TRUE) %>%
  group_by(agency_new, prog_type_new, o2015) %>%
  summarise(female = sum(gender_new_m6[gender_new_m6 == 1], na.rm = TRUE),
            male = sum(gender_new_m6[gender_new_m6 == 2] - 1, na.rm = TRUE),
            unknown = sum(is.na(gender_new_m6)),
            total = n()) %>%
  mutate(femaleper = female / total, maleper = male / total, unkper = unknown / total)


temp <- pha_elig_demogs %>%
  filter(h2015 == 1) %>%
  distinct(ssn_new_pha, lname_new.1, fname_new.1, dob_h, .keep_all = TRUE)
stat.desc(temp$age15[temp$agency_new == "KCHA" & temp$prog_type_new == "PBS8" & temp$o2015 == 0], basic = F)
stat.desc(temp$age15[temp$agency_new == "KCHA" & temp$prog_type_new == "PBS8" & temp$o2015 == 1], basic = F)
stat.desc(temp$age15[temp$agency_new == "KCHA" & temp$prog_type_new == "PH" & temp$o2015 == 0], basic = F)
stat.desc(temp$age15[temp$agency_new == "KCHA" & temp$prog_type_new == "PH" & temp$o2015 == 1], basic = F)
stat.desc(temp$age15[temp$agency_new == "KCHA" & temp$prog_type_new == "PORT" & temp$o2015 == 0], basic = F)
stat.desc(temp$age15[temp$agency_new == "KCHA" & temp$prog_type_new == "PORT" & temp$o2015 == 1], basic = F)
stat.desc(temp$age15[temp$agency_new == "KCHA" & temp$prog_type_new == "TBS8" & temp$o2015 == 0], basic = F)
stat.desc(temp$age15[temp$agency_new == "KCHA" & temp$prog_type_new == "TBS8" & temp$o2015 == 1], basic = F)

stat.desc(temp$age15[temp$agency_new == "SHA" & temp$prog_type_new == "HCV" & temp$o2015 == 0], basic = F)
stat.desc(temp$age15[temp$agency_new == "SHA" & temp$prog_type_new == "HCV" & temp$o2015 == 1], basic = F)
stat.desc(temp$age15[temp$agency_new == "SHA" & temp$prog_type_new == "PBS8" & temp$o2015 == 0], basic = F)
stat.desc(temp$age15[temp$agency_new == "SHA" & temp$prog_type_new == "PBS8" & temp$o2015 == 1], basic = F)
stat.desc(temp$age15[temp$agency_new == "SHA" & temp$prog_type_new == "PH" & temp$o2015 == 0], basic = F)
stat.desc(temp$age15[temp$agency_new == "SHA" & temp$prog_type_new == "PH" & temp$o2015 == 1], basic = F)
stat.desc(temp$age15[temp$agency_new == "SHA" & temp$prog_type_new == "PORT" & temp$o2015 == 0], basic = F)
stat.desc(temp$age15[temp$agency_new == "SHA" & temp$prog_type_new == "PORT" & temp$o2015 == 1], basic = F)
stat.desc(temp$age15[temp$agency_new == "SHA" & temp$prog_type_new == "TBS8" & temp$o2015 == 0], basic = F)
stat.desc(temp$age15[temp$agency_new == "SHA" & temp$prog_type_new == "TBS8" & temp$o2015 == 1], basic = F)





