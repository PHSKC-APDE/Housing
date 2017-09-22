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
library(stringr) # Used to manipulate string data
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
yt_elig_final <- pha_elig_final %>%
  # Strip out identifiers
  #select(pid, MEDICAID_RECIPIENT_ID, dob_h, gender_new_m6:enroll_type) %>%
  mutate(
    yt = ifelse((property_id %in% c("001", "591", "738", "743") & !is.na(property_id)) |
                  ((str_detect(unit_add_new, "1105 E F") | str_detect(unit_add_new, "1305 E F") | 
                      str_detect(unit_add_new, "820[:space:]*[E]*[:space:]*YESLER")) & !is.na(unit_add_new)), 1, 0),
    yt_old = ifelse(property_id %in% c("1") & !is.na(property_id), 1, 0),
    yt_new = ifelse((property_id %in% c("591", "738", "743") & !is.na(property_id)) |
                      (!is.na(unit_add_new) & (str_detect(unit_add_new, "1105 E F") | 
                                                 str_detect(unit_add_new, "1305 E F") | 
                                                 str_detect(unit_add_new, "820[:space:]*[E]*[:space:]*YESLER"))),
                    1, 0),
    ss = ifelse(property_id %in% c("050", "051", "052", "053", "054", "055", "056", "057", "A42", "A43", 
                                   "I43", "L42", "P42", "P43") & !is.na(property_id), 1, 0)
  )

# Find people who were ever at YT or SS
yt_elig_final <- yt_elig_final %>%
  group_by(pid) %>%
  mutate_at(vars(yt, ss), funs(ever = sum(., na.rm = TRUE))) %>%
  ungroup() %>%
  mutate_at(vars(yt_ever, ss_ever), funs(replace(., which(. > 0), 1)))


# Set up presence/absence in housing, Medicaid, and both at Jun 31 each year
yt_elig_final <- yt_elig_final %>%
  mutate(
    # Enrolled in housing
    jun12_h = ifelse(startdate_c <= as.Date("2012-06-30", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2012-06-30", origin = "1970-01-01") & enroll_type %in% c("b", "h"), 1, 0),
    jun13_h = ifelse(startdate_c <= as.Date("2013-06-30", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2013-06-30", origin = "1970-01-01") & enroll_type %in% c("b", "h"), 1, 0),
    jun14_h = ifelse(startdate_c <= as.Date("2014-06-30", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2014-06-30", origin = "1970-01-01") & enroll_type %in% c("b", "h"), 1, 0),
    jun15_h = ifelse(startdate_c <= as.Date("2015-06-30", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2015-06-30", origin = "1970-01-01") & enroll_type %in% c("b", "h"), 1, 0),
    jun16_h = ifelse(startdate_c <= as.Date("2016-06-30", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2016-06-30", origin = "1970-01-01") & enroll_type %in% c("b", "h"), 1, 0),
    # Enrolled in Medicaid (will be NA if never enrolled in Medicaid)
    jun12_m = ifelse(startdate_c <= as.Date("2012-06-30", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2012-06-30", origin = "1970-01-01") & enroll_type %in% c("b", "m"), 1, 0),
    jun13_m = ifelse(startdate_c <= as.Date("2013-06-30", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2013-06-30", origin = "1970-01-01") & enroll_type %in% c("b", "m"), 1, 0),
    jun14_m = ifelse(startdate_c <= as.Date("2014-06-30", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2014-06-30", origin = "1970-01-01") & enroll_type %in% c("b", "m"), 1, 0),
    jun15_m = ifelse(startdate_c <= as.Date("2015-06-30", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2015-06-30", origin = "1970-01-01") & enroll_type %in% c("b", "m"), 1, 0),
    jun16_m = ifelse(startdate_c <= as.Date("2016-06-30", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2016-06-30", origin = "1970-01-01") & enroll_type %in% c("b", "m"), 1, 0)
  )


# Set up person-time each year
# First set up intervals for each year
i2012 <- interval(start = "2012-01-01", end = "2012-12-31")
i2013 <- interval(start = "2013-01-01", end = "2013-12-31")
i2014 <- interval(start = "2014-01-01", end = "2014-12-31")
i2015 <- interval(start = "2015-01-01", end = "2015-12-31")
i2016 <- interval(start = "2016-01-01", end = "2016-12-31")

# Person-time in housing
pt_temp_h <- yt_elig_final %>%
  filter(enroll_type == "h" | enroll_type == "b") %>%
  distinct(pid, startdate_c, enddate_c) %>%
  mutate(
    pt12_h = (lubridate::intersect(interval(start = startdate_c, end = enddate_c), i2012) / ddays(1)) + 1,
    pt13_h = (lubridate::intersect(interval(start = startdate_c, end = enddate_c), i2013) / ddays(1)) + 1,
    pt14_h = (lubridate::intersect(interval(start = startdate_c, end = enddate_c), i2014) / ddays(1)) + 1,
    pt15_h = (lubridate::intersect(interval(start = startdate_c, end = enddate_c), i2015) / ddays(1)) + 1,
    pt16_h = (lubridate::intersect(interval(start = startdate_c, end = enddate_c), i2016) / ddays(1)) + 1
  )

yt_elig_final <- left_join(yt_elig_final, pt_temp_h, by = c("pid", "startdate_c", "enddate_c"))
rm(pt_temp_h)


# Person-time in Medicaid, needs to be done separately to avoid errors
pt_temp_m <- yt_elig_final %>%
  filter(enroll_type == "m" | enroll_type == "b") %>%
  distinct(pid, startdate_c, enddate_c) %>%
  mutate(
    pt12_m = (lubridate::intersect(interval(start = startdate_c, end = enddate_c), i2012) / ddays(1)) + 1,
    pt13_m = (lubridate::intersect(interval(start = startdate_c, end = enddate_c), i2013) / ddays(1)) + 1,
    pt14_m = (lubridate::intersect(interval(start = startdate_c, end = enddate_c), i2014) / ddays(1)) + 1,
    pt15_m = (lubridate::intersect(interval(start = startdate_c, end = enddate_c), i2015) / ddays(1)) + 1,
    pt16_m = (lubridate::intersect(interval(start = startdate_c, end = enddate_c), i2016) / ddays(1)) + 1
  )

yt_elig_final <- left_join(yt_elig_final, pt_temp_m, by = c("pid", "startdate_c", "enddate_c"))
rm(pt_temp_m)

# Person-time in both, needs to be done separately to avoid errors
pt_temp_o <- yt_elig_final %>%
  filter(enroll_type == "b") %>%
  distinct(pid, startdate_c, enddate_c) %>%
  mutate(
    pt12_o = (lubridate::intersect(interval(start = startdate_c, end = enddate_c), i2012) / ddays(1)) + 1,
    pt13_o = (lubridate::intersect(interval(start = startdate_c, end = enddate_c), i2013) / ddays(1)) + 1,
    pt14_o = (lubridate::intersect(interval(start = startdate_c, end = enddate_c), i2014) / ddays(1)) + 1,
    pt15_o = (lubridate::intersect(interval(start = startdate_c, end = enddate_c), i2015) / ddays(1)) + 1,
    pt16_o = (lubridate::intersect(interval(start = startdate_c, end = enddate_c), i2016) / ddays(1)) + 1
  )

yt_elig_final <- left_join(yt_elig_final, pt_temp_o, by = c("pid", "startdate_c", "enddate_c"))
rm(pt_temp_o)


# Age at each date
yt_elig_final <- yt_elig_final %>%
  mutate(age12 = round(interval(start = dob_h, end = ymd(20120630)) / years(1), 1),
         age13 = round(interval(start = dob_h, end = ymd(20130630)) / years(1), 1),
         age14 = round(interval(start = dob_h, end = ymd(20140630)) / years(1), 1),
         age15 = round(interval(start = dob_h, end = ymd(20150630)) / years(1), 1),
         age16 = round(interval(start = dob_h, end = ymd(20160630)) / years(1), 1)
          )



### Output to Excel for use in Tableau
temp <- yt_elig_final
# Strip out identifying variables
temp <- temp %>% select(pid, gender_new_m6, race2, adult, senior, disability, DUAL_ELIG, COVERAGE_TYPE_IND, agency_new, 
                        startdate_h:enddate_o, startdate_c:enroll_type, yt:age16) %>%
  arrange(pid, startdate_c, enddate_c)


write.xlsx(temp, file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/Summaries/yt_elig_demogs.xlsx")

### Save point
#saveRDS(yt_elig_final, file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/SHA cleaning/yt_elig_final.Rds")
#yt_elig_final <- readRDS("//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/SHA cleaning/yt_elig_final.Rds")



##### Look at demographics in 2012–2016 #####

### Count per year
yt_elig_final %>% filter(jun12_h == 1 & agency_new == "SHA") %>% distinct(pid, .keep_all = TRUE) %>%
  group_by(yt, ss) %>% summarise(count = n())
yt_elig_final %>% filter(jun13_h == 1 & agency_new == "SHA") %>% distinct(pid, .keep_all = TRUE) %>%
  group_by(yt, ss) %>% summarise(count = n())
yt_elig_final %>% filter(jun14_h == 1 & agency_new == "SHA") %>% distinct(pid, .keep_all = TRUE) %>%
  group_by(yt, ss) %>% summarise(count = n())
yt_elig_final %>% filter(jun15_h == 1 & agency_new == "SHA") %>% distinct(pid, .keep_all = TRUE) %>%
  group_by(yt, ss, race2) %>% summarise(count = n())
yt_elig_final %>% filter(jun16_h == 1 & agency_new == "SHA") %>% distinct(pid, .keep_all = TRUE) %>%
  group_by(yt, ss) %>% summarise(count = n())


### Look at gender each year
yt_elig_final %>%
  filter(jun12_h == 1 & agency_new == "SHA") %>%
  distinct(pid, .keep_all = TRUE) %>%
  group_by(yt) %>%
  summarise(female = sum(gender_new_m6[gender_new_m6 == 1], na.rm = TRUE),
            # Need to subtract 1 to avoid doubling the count since male == 2
            male = sum(gender_new_m6[gender_new_m6 == 2] - 1, na.rm = TRUE),
            unknown = sum(is.na(gender_new_m6)),
            total = n()) %>%
  mutate(femaleper = female / total, maleper = male / total, unkper = unknown / total)

yt_elig_final %>%
  filter(jun15_h == 1 & agency_new == "SHA") %>%
  distinct(pid, .keep_all = TRUE) %>%
  group_by(yt) %>%
  summarise(female = sum(gender_new_m6[gender_new_m6 == 1], na.rm = TRUE),
            # Need to subtract 1 to avoid doubling the count since male == 2
            male = sum(gender_new_m6[gender_new_m6 == 2] - 1, na.rm = TRUE),
            unknown = sum(is.na(gender_new_m6)),
            total = n()) %>%
  mutate(femaleper = female / total, maleper = male / total, unkper = unknown / total)



### Look at age each year
temp <- yt_elig_final %>%
  filter(agency_new == "SHA") %>%
  distinct(pid, startdate_h, enddate_h, .keep_all = TRUE)

stat.desc(temp$age12[temp$jun12_h == 1 & temp$yt == 1], basic = F)
stat.desc(temp$age12[temp$jun12_h == 1 & temp$yt == 0], basic = F)

stat.desc(temp$age13[temp$jun13_h == 1 & temp$yt == 1], basic = F)
stat.desc(temp$age13[temp$jun13_h == 1 & temp$yt == 0], basic = F)

stat.desc(temp$age14[temp$jun14_h == 1 & temp$yt == 1], basic = F)
stat.desc(temp$age14[temp$jun14_h == 1 & temp$yt == 0], basic = F)

stat.desc(temp$age15[temp$jun15_h == 1 & temp$yt == 1], basic = F)
stat.desc(temp$age15[temp$jun15_h == 1 & temp$yt == 0], basic = F)

stat.desc(temp$age16[temp$jun16_h == 1 & temp$yt == 1], basic = F)
stat.desc(temp$age16[temp$jun16_h == 1 & temp$yt == 0], basic = F)


### Look at race each year
yt_elig_final %>% filter(jun12_h == 1) %>%
  distinct(pid, .keep_all = TRUE) %>%
  group_by(yt, race2) %>%
  summarise(count = n())

yt_elig_final %>% filter(jun13_h == 1) %>%
  distinct(pid, .keep_all = TRUE) %>%
  group_by(yt, race2) %>%
  summarise(count = n())

yt_elig_final %>% filter(jun14_h == 1) %>%
  distinct(pid, .keep_all = TRUE) %>%
  group_by(yt, race2) %>%
  summarise(count = n())

yt_elig_final %>% filter(jun15_h == 1) %>%
  distinct(pid, .keep_all = TRUE) %>%
  group_by(yt, race2) %>%
  summarise(count = n())

yt_elig_final %>% filter(jun16_h == 1) %>%
  distinct(pid, .keep_all = TRUE) %>%
  group_by(yt, race2) %>%
  summarise(count = n())

