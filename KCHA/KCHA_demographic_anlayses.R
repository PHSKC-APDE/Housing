
# Turn scientific notation off and other settings
options(max.print = 700, scipen = 100, digits = 5)

library(lubridate) # Used to manipulate dates
library(dplyr) # Used to manipulate data
library(pastecs) # Used for summary statistics


### Look at demographicss in 2013, 2014, 2015, 2016
# Set up presence/absence at June 1 each year
kcha_demogs <- kcha_cleanadd_sort %>%
  mutate(june12 = ifelse(startdate <= as.Date("2012-06-01", origin = "1970-01-01") & 
                           enddate >= as.Date("2012-06-01", origin = "1970-01-01"),
                         1, 0),
         june13 = ifelse(startdate <= as.Date("2013-06-01", origin = "1970-01-01") & 
                           enddate >= as.Date("2013-06-01", origin = "1970-01-01"),
                         1, 0),
         june14 = ifelse(startdate <= as.Date("2014-06-01", origin = "1970-01-01") & 
                           enddate >= as.Date("2014-06-01", origin = "1970-01-01"),
                         1, 0),
         june15 = ifelse(startdate <= as.Date("2015-06-01", origin = "1970-01-01") & 
                           enddate >= as.Date("2015-06-01", origin = "1970-01-01"),
                         1, 0),
         june16 = ifelse(startdate <= as.Date("2016-06-01", origin = "1970-01-01") & 
                           enddate >= as.Date("2016-06-01", origin = "1970-01-01"),
                         1, 0)
         )

# Check people are not appearing twice at a given time
# Need to account for multiple programs
kcha_demogs %>% group_by(pid) %>%
  summarise(
    cntprog = n_distinct(program_type),
    cnt12 = sum(june12),
    cnt13 = sum(june13),
    cnt14 = sum(june14),
    cnt15 = sum(june15),
    cnt16 = sum(june16)
      ) %>%
  ungroup() %>%
  filter((cnt12 > 1 | cnt13 > 1 | cnt14 > 1 | cnt15 > 1 | cnt16 > 1) & cntprog == 1)


### Set up other variables
# Age at each date
kcha_demogs <- kcha_demogs %>%
  mutate(age12 = round(interval(start = dob_m3, end = ymd(20120601)) / years(1), 1),
         age13 = round(interval(start = dob_m3, end = ymd(20130601)) / years(1), 1),
         age14 = round(interval(start = dob_m3, end = ymd(20140601)) / years(1), 1),
         age15 = round(interval(start = dob_m3, end = ymd(20150601)) / years(1), 1),
         age16 = round(interval(start = dob_m3, end = ymd(20160601)) / years(1), 1)
          )

### Count per year
length(unique(kcha_demogs$pid[kcha_demogs$june12 == 1]))
length(unique(kcha_demogs$pid[kcha_demogs$june13 == 1]))
length(unique(kcha_demogs$pid[kcha_demogs$june14 == 1]))
length(unique(kcha_demogs$pid[kcha_demogs$june15 == 1]))
length(unique(kcha_demogs$pid[kcha_demogs$june16 == 1]))


### Look at gender each year
kcha_demogs %>%
  filter(june12 == 1) %>%
  distinct(pid, .keep_all = TRUE) %>%
  summarise(female = sum(gender_new[gender_new == 1], na.rm = TRUE),
            male = sum(gender_new[gender_new == 2] - 1, na.rm = TRUE),
            unknown = sum(is.na(gender_new)),
            total = n()) %>%
  mutate(femaleper = female / total, maleper = male / total)

kcha_demogs %>%
  filter(june13 == 1) %>%
  distinct(pid, .keep_all = TRUE) %>%
  summarise(female = sum(gender_new[gender_new == 1], na.rm = TRUE),
            male = sum(gender_new[gender_new == 2] - 1, na.rm = TRUE),
            unknown = sum(is.na(gender_new)),
            total = n()) %>%
  mutate(femaleper = female / total, maleper = male / total)

kcha_demogs %>%
  filter(june14 == 1) %>%
  distinct(pid, .keep_all = TRUE) %>%
  summarise(female = sum(gender_new[gender_new == 1], na.rm = TRUE),
            male = sum(gender_new[gender_new == 2] - 1, na.rm = TRUE),
            unknown = sum(is.na(gender_new)),
            total = n()) %>%
  mutate(femaleper = female / total, maleper = male / total)

kcha_demogs %>%
  filter(june15 == 1) %>%
  distinct(pid, .keep_all = TRUE) %>%
  summarise(female = sum(gender_new[gender_new == 1], na.rm = TRUE),
            male = sum(gender_new[gender_new == 2] - 1, na.rm = TRUE),
            unknown = sum(is.na(gender_new)),
            total = n()) %>%
  mutate(femaleper = female / total, maleper = male / total)

kcha_demogs %>%
  filter(june16 == 1) %>%
  distinct(pid, .keep_all = TRUE) %>%
  summarise(female = sum(gender_new[gender_new == 1], na.rm = TRUE),
            male = sum(gender_new[gender_new == 2] - 1, na.rm = TRUE),
            unknown = sum(is.na(gender_new)),
            total = n()) %>%
  mutate(femaleper = female / total, maleper = male / total)


### Look at age each year
temp <- kcha_demogs %>%
  filter(june12 == 1) %>%
  distinct(pid, .keep_all = TRUE)
stat.desc(temp$age12, basic = F)

temp <- kcha_demogs %>%
  filter(june13 == 1) %>%
  distinct(pid, .keep_all = TRUE)
stat.desc(temp$age13, basic = F)

temp <- kcha_demogs %>%
  filter(june14 == 1) %>%
  distinct(pid, .keep_all = TRUE)
stat.desc(temp$age14, basic = F)

temp <- kcha_demogs %>%
  filter(june15 == 1) %>%
  distinct(pid, .keep_all = TRUE)
stat.desc(temp$age15, basic = F)

temp <- kcha_demogs %>%
  filter(june16 == 1) %>%
  distinct(pid, .keep_all = TRUE)
stat.desc(temp$age16, basic = F)



### Look at race each year
kcha_demogs %>%
  filter(june12 == 1) %>%
  distinct(pid, .keep_all = TRUE) %>%
  summarise(aian = sum(r_aian_new_alone, na.rm = TRUE),
            asian = sum(r_asian_new_alone, na.rm = TRUE),
            black = sum(r_black_new_alone, na.rm = TRUE),
            multi = sum(r_multi_new, na.rm = TRUE),
            nhpi = sum(r_nhpi_new_alone, na.rm = TRUE),
            white = sum(r_white_new_alone, na.rm = TRUE)
            )

kcha_demogs %>%
  filter(june13 == 1) %>%
  distinct(pid, .keep_all = TRUE) %>%
  summarise(aian = sum(r_aian_new_alone, na.rm = TRUE),
            asian = sum(r_asian_new_alone, na.rm = TRUE),
            black = sum(r_black_new_alone, na.rm = TRUE),
            multi = sum(r_multi_new, na.rm = TRUE),
            nhpi = sum(r_nhpi_new_alone, na.rm = TRUE),
            white = sum(r_white_new_alone, na.rm = TRUE)
  )

kcha_demogs %>%
  filter(june14 == 1) %>%
  distinct(pid, .keep_all = TRUE) %>%
  summarise(aian = sum(r_aian_new_alone, na.rm = TRUE),
            asian = sum(r_asian_new_alone, na.rm = TRUE),
            black = sum(r_black_new_alone, na.rm = TRUE),
            multi = sum(r_multi_new, na.rm = TRUE),
            nhpi = sum(r_nhpi_new_alone, na.rm = TRUE),
            white = sum(r_white_new_alone, na.rm = TRUE)
  )

kcha_demogs %>%
  filter(june15 == 1) %>%
  distinct(pid, .keep_all = TRUE) %>%
  summarise(aian = sum(r_aian_new_alone, na.rm = TRUE),
            asian = sum(r_asian_new_alone, na.rm = TRUE),
            black = sum(r_black_new_alone, na.rm = TRUE),
            multi = sum(r_multi_new, na.rm = TRUE),
            nhpi = sum(r_nhpi_new_alone, na.rm = TRUE),
            white = sum(r_white_new_alone, na.rm = TRUE)
  )

kcha_demogs %>%
  filter(june16 == 1) %>%
  distinct(pid, .keep_all = TRUE) %>%
  summarise(aian = sum(r_aian_new_alone, na.rm = TRUE),
            asian = sum(r_asian_new_alone, na.rm = TRUE),
            black = sum(r_black_new_alone, na.rm = TRUE),
            multi = sum(r_multi_new, na.rm = TRUE),
            nhpi = sum(r_nhpi_new_alone, na.rm = TRUE),
            white = sum(r_white_new_alone, na.rm = TRUE)
  )


