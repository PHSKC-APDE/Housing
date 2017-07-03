###############################################################################
# Code to join examine demographics of Yesler Terrace residents
#
# Alastair Matheson (PHSKC-APDE)
# alastair.matheson@kingcounty.gov
# 2017-06-30
#
# NOTE THAT THIS CODE IS A WORK IN PROGRESS
#
###############################################################################


##### Set up global parameter and call in libraries #####
options(max.print = 350, tibble.print_max = 30, scipen = 999)

library(RODBC) # Used to connect to SQL server
library(lubridate) # Used to manipulate dates
library(dplyr) # Used to manipulate data
library(pastecs) # Used for summary statistics

##### Connect to the servers #####
db.apde50 <- odbcConnect("PH_APDEStore50")
db.apde51 <- odbcConnect("PH_APDEStore51")
db.claims50 <- odbcConnect("PHClaims50")
db.claims51 <- odbcConnect("PHClaims51")


##### Bring in combined PHA data #####
pha_elig_final <- readRDS(file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/pha_elig_final.Rda")


##### Set up key variables #####
# Yesler Terrace and scattered sites indicators
yt_demogs <- pha_elig_final %>%
  # Only pull people who were in the housing data
  filter(source %in% c("h", "b")) %>%
  mutate(
    yt = ifelse((property_id %in% c("1", "591", "738", "743") & !is.na(property_id)) |
                  str_detect(unit_add_new, "1105 E F") | str_detect(unit_add_new, "1305 E F") | 
                  str_detect(unit_add_new, "820[:space:]*[E]*[:space:]*YESLER"), 1, 0),
    yt_old = ifelse(property_id %in% c("1") & !is.na(property_id), 1, 0),
    yt_new = ifelse((property_id %in% c("591", "738", "743") & !is.na(property_id)) |
                      str_detect(unit_add_new, "1105 E F") | str_detect(unit_add_new, "1305 E F") | 
                      str_detect(unit_add_new, "820[:space:]*[E]*[:space:]*YESLER"), 1, 0),
    ss = ifelse(property_id %in% c("50", "51", "52", "53", "54", "55", "56", "57", "A42", "A43", 
                                   "I43", "L42", "P42", "P43") & !is.na(property_id), 1, 0)
  )

# Find people who were ever at YT or SS
yt_demogs <- yt_demogs %>%
  group_by(MEDICAID_RECIPIENT_ID, SOCIAL_SECURITY_NMBR, ssn_new_pha, lname_new.1, fname_new.1, dob_h) %>%
  mutate_at(vars(yt, ss), funs(ever = sum(., na.rm = TRUE))) %>%
  ungroup() %>%
  mutate_at(vars(yt_ever, ss_ever), funs(replace(., which(. > 0), 1)))


# Set up presence/absence in housing, Medicaid, and both at Dec 31 each year
yt_demogs <- yt_demogs %>%
  mutate(
    # Enrolled in housing
    dec12_h = ifelse(startdate_h <= as.Date("2012-12-31", origin = "1970-01-01") & enddate_h >= as.Date("2012-12-31", origin = "1970-01-01"), 1, 0),
    dec13_h = ifelse(startdate_h <= as.Date("2013-12-31", origin = "1970-01-01") & enddate_h >= as.Date("2013-12-31", origin = "1970-01-01"), 1, 0),
    dec14_h = ifelse(startdate_h <= as.Date("2014-12-31", origin = "1970-01-01") & enddate_h >= as.Date("2014-12-31", origin = "1970-01-01"), 1, 0),
    dec15_h = ifelse(startdate_h <= as.Date("2015-12-31", origin = "1970-01-01") & enddate_h >= as.Date("2015-12-31", origin = "1970-01-01"), 1, 0),
    dec16_h = ifelse(startdate_h <= as.Date("2016-12-31", origin = "1970-01-01") & enddate_h >= as.Date("2016-12-31", origin = "1970-01-01"), 1, 0),
    # Enrolled in Medicaid (will be NA if never enrolled in Medicaid)
    dec12_m = ifelse(startdate_m <= as.Date("2012-12-31", origin = "1970-01-01") & enddate_m >= as.Date("2012-12-31", origin = "1970-01-01"), 1, 0),
    dec13_m = ifelse(startdate_m <= as.Date("2013-12-31", origin = "1970-01-01") & enddate_m >= as.Date("2013-12-31", origin = "1970-01-01"), 1, 0),
    dec14_m = ifelse(startdate_m <= as.Date("2014-12-31", origin = "1970-01-01") & enddate_m >= as.Date("2014-12-31", origin = "1970-01-01"), 1, 0),
    dec15_m = ifelse(startdate_m <= as.Date("2015-12-31", origin = "1970-01-01") & enddate_m >= as.Date("2015-12-31", origin = "1970-01-01"), 1, 0),
    dec16_m = ifelse(startdate_m <= as.Date("2016-12-31", origin = "1970-01-01") & enddate_m >= as.Date("2016-12-31", origin = "1970-01-01"), 1, 0),
    # Enrolled in both
    dec12_o = ifelse(startdate_o <= as.Date("2012-12-31", origin = "1970-01-01") & enddate_o >= as.Date("2012-12-31", origin = "1970-01-01"), 1, 0),
    dec13_o = ifelse(startdate_o <= as.Date("2013-12-31", origin = "1970-01-01") & enddate_o >= as.Date("2013-12-31", origin = "1970-01-01"), 1, 0),
    dec14_o = ifelse(startdate_o <= as.Date("2014-12-31", origin = "1970-01-01") & enddate_o >= as.Date("2014-12-31", origin = "1970-01-01"), 1, 0),
    dec15_o = ifelse(startdate_o <= as.Date("2015-12-31", origin = "1970-01-01") & enddate_o >= as.Date("2015-12-31", origin = "1970-01-01"), 1, 0),
    dec16_o = ifelse(startdate_o <= as.Date("2016-12-31", origin = "1970-01-01") & enddate_o >= as.Date("2016-12-31", origin = "1970-01-01"), 1, 0)
  )


# Set up person-time each year
# First set up intervals for each year
i2012 <- interval(start = "2012-01-01", end = "2012-12-31")
i2013 <- interval(start = "2013-01-01", end = "2013-12-31")
i2014 <- interval(start = "2014-01-01", end = "2014-12-31")
i2015 <- interval(start = "2015-01-01", end = "2015-12-31")
i2016 <- interval(start = "2016-01-01", end = "2016-12-31")

# Person-time in housing
yt_demogs <- yt_demogs %>%
  mutate(
    pt12_h = (lubridate::intersect(interval(start = startdate_h, end = enddate_h), i2012) / ddays(1)) + 1,
    pt13_h = (lubridate::intersect(interval(start = startdate_h, end = enddate_h), i2013) / ddays(1)) + 1,
    pt14_h = (lubridate::intersect(interval(start = startdate_h, end = enddate_h), i2014) / ddays(1)) + 1,
    pt15_h = (lubridate::intersect(interval(start = startdate_h, end = enddate_h), i2015) / ddays(1)) + 1,
    pt16_h = (lubridate::intersect(interval(start = startdate_h, end = enddate_h), i2016) / ddays(1)) + 1
    )

# Person-time in Medicaid, needs to be done separately to avoid errors
pt_temp_m <- yt_demogs %>%
  filter(!is.na(startdate_m)) %>%
  distinct(ssn_new_pha, lname_new.1, fname_new.1, dob_h, startdate_m, enddate_m) %>%
  mutate(
    pt12_m = (lubridate::intersect(interval(start = startdate_m, end = enddate_m), i2012) / ddays(1)) + 1,
    pt13_m = (lubridate::intersect(interval(start = startdate_m, end = enddate_m), i2013) / ddays(1)) + 1,
    pt14_m = (lubridate::intersect(interval(start = startdate_m, end = enddate_m), i2014) / ddays(1)) + 1,
    pt15_m = (lubridate::intersect(interval(start = startdate_m, end = enddate_m), i2015) / ddays(1)) + 1,
    pt16_m = (lubridate::intersect(interval(start = startdate_m, end = enddate_m), i2016) / ddays(1)) + 1
  )

yt_demogs <- left_join(yt_demogs, pt_temp_m, by = c("ssn_new_pha", "lname_new.1", "fname_new.1", "dob_h", "startdate_m", "enddate_m"))
rm(pt_temp_m)

# Person-time in both, needs to be done separately to avoid errors
pt_temp_o <- yt_demogs %>%
  filter(!is.na(startdate_o)) %>%
  distinct(ssn_new_pha, lname_new.1, fname_new.1, dob_h, startdate_o, enddate_o) %>%
  mutate(
    pt12_o = (lubridate::intersect(interval(start = startdate_o, end = enddate_o), i2012) / ddays(1)) + 1,
    pt13_o = (lubridate::intersect(interval(start = startdate_o, end = enddate_o), i2013) / ddays(1)) + 1,
    pt14_o = (lubridate::intersect(interval(start = startdate_o, end = enddate_o), i2014) / ddays(1)) + 1,
    pt15_o = (lubridate::intersect(interval(start = startdate_o, end = enddate_o), i2015) / ddays(1)) + 1,
    pt16_o = (lubridate::intersect(interval(start = startdate_o, end = enddate_o), i2016) / ddays(1)) + 1
  )

yt_demogs <- left_join(yt_demogs, pt_temp_o, by = c("ssn_new_pha", "lname_new.1", "fname_new.1", "dob_h", "startdate_o", "enddate_o"))
rm(pt_temp_o)


# Age at each date
yt_demogs <- yt_demogs %>%
  mutate(age12 = round(interval(start = dob_h, end = ymd(20121231)) / years(1), 1),
         age13 = round(interval(start = dob_h, end = ymd(20131231)) / years(1), 1),
         age14 = round(interval(start = dob_h, end = ymd(20141231)) / years(1), 1),
         age15 = round(interval(start = dob_h, end = ymd(20151231)) / years(1), 1),
         age16 = round(interval(start = dob_h, end = ymd(20161231)) / years(1), 1)
          )



### Output to Excel for use in Tableau
temp <- yt_demogs
# Make unique ID to anonymize data
temp$pid <- group_indices(temp, MEDICAID_RECIPIENT_ID, SOCIAL_SECURITY_NMBR, ssn_new_pha, lname_new.1, fname_new.1, dob_h)
# Strip out identifying variables
temp <- temp %>% select(pid, gender_new_m6, race2, disability, agency_new, prog_type_new:property_id, startdate_h:age16)

temp <- temp %>%
  select(pid:agency_new, yt:age16) %>%
  distinct()

write.xlsx(temp, file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/Summaries/yt_elig_demogs.xlsx")

### Save point
#saveRDS(yt_demogs, file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/SHA cleaning/yt_demogs.Rds")
#yt_demogs <- readRDS("//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/SHA cleaning/yt_demogs.Rds")



##### Look at demographics in 2012–2016 #####

### Count per year
yt_demogs %>% filter(dec12_h == 1 & agency_new == "SHA") %>% distinct(ssn_new_pha, lname_new.1, fname_new.1, dob_h, .keep_all = TRUE) %>%
  group_by(yt, ss) %>% summarise(count = n())
yt_demogs %>% filter(dec13_h == 1 & agency_new == "SHA") %>% distinct(ssn_new_pha, lname_new.1, fname_new.1, dob_h, .keep_all = TRUE) %>%
  group_by(yt, ss) %>% summarise(count = n())
yt_demogs %>% filter(dec14_h == 1 & agency_new == "SHA") %>% distinct(ssn_new_pha, lname_new.1, fname_new.1, dob_h, .keep_all = TRUE) %>%
  group_by(yt, ss) %>% summarise(count = n())
yt_demogs %>% filter(dec15_h == 1 & agency_new == "SHA") %>% distinct(ssn_new_pha, lname_new.1, fname_new.1, dob_h, .keep_all = TRUE) %>%
  group_by(yt, ss, race2) %>% summarise(count = n())
yt_demogs %>% filter(dec16_h == 1 & agency_new == "SHA") %>% distinct(ssn_new_pha, lname_new.1, fname_new.1, dob_h, .keep_all = TRUE) %>%
  group_by(yt, ss) %>% summarise(count = n())


### Look at gender each year
yt_demogs %>%
  filter(dec12_h == 1 & agency_new == "SHA") %>%
  distinct(ssn_new_pha, lname_new.1, fname_new.1, dob_h, .keep_all = TRUE) %>%
  group_by(yt) %>%
  summarise(female = sum(gender_new_m6[gender_new_m6 == 1], na.rm = TRUE),
            # Need to subtract 1 to avoid doubling the count since male == 2
            male = sum(gender_new_m6[gender_new_m6 == 2] - 1, na.rm = TRUE),
            unknown = sum(is.na(gender_new_m6)),
            total = n()) %>%
  mutate(femaleper = female / total, maleper = male / total, unkper = unknown / total)

yt_demogs %>%
  filter(dec15_h == 1 & agency_new == "SHA") %>%
  distinct(ssn_new_pha, lname_new.1, fname_new.1, dob_h, .keep_all = TRUE) %>%
  group_by(yt) %>%
  summarise(female = sum(gender_new_m6[gender_new_m6 == 1], na.rm = TRUE),
            # Need to subtract 1 to avoid doubling the count since male == 2
            male = sum(gender_new_m6[gender_new_m6 == 2] - 1, na.rm = TRUE),
            unknown = sum(is.na(gender_new_m6)),
            total = n()) %>%
  mutate(femaleper = female / total, maleper = male / total, unkper = unknown / total)



### Look at age each year
temp <- yt_demogs %>%
  filter(agency_new == "SHA") %>%
  distinct(ssn_new_pha, lname_new.1, fname_new.1, dob_h, startdate_h, enddate_h, .keep_all = TRUE)

stat.desc(temp$age12[temp$dec12_h == 1 & temp$yt == 1], basic = F)
stat.desc(temp$age12[temp$dec12_h == 1 & temp$yt == 0], basic = F)

stat.desc(temp$age13[temp$dec13_h == 1 & temp$yt == 1], basic = F)
stat.desc(temp$age13[temp$dec13_h == 1 & temp$yt == 0], basic = F)

stat.desc(temp$age14[temp$dec14_h == 1 & temp$yt == 1], basic = F)
stat.desc(temp$age14[temp$dec14_h == 1 & temp$yt == 0], basic = F)

stat.desc(temp$age15[temp$dec15_h == 1 & temp$yt == 1], basic = F)
stat.desc(temp$age15[temp$dec15_h == 1 & temp$yt == 0], basic = F)

stat.desc(temp$age16[temp$dec16_h == 1 & temp$yt == 1], basic = F)
stat.desc(temp$age16[temp$dec16_h == 1 & temp$yt == 0], basic = F)


### Look at race each year
yt_demogs %>% filter(dec12_h == 1) %>%
  group_by(yt, race2) %>%
  summarise(count = n())

yt_demogs %>% filter(dec13_h == 1) %>%
  group_by(yt, race2) %>%
  summarise(count = n())

yt_demogs %>% filter(dec14_h == 1) %>%
  group_by(yt, race2) %>%
  summarise(count = n())

yt_demogs %>% filter(dec15_h == 1) %>%
  group_by(yt, race2) %>%
  summarise(count = n())

yt_demogs %>% filter(dec16_h == 1) %>%
  group_by(yt, race2) %>%
  summarise(count = n())

