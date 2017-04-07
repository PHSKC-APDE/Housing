
# Turn scientific notation off and other settings
options(max.print = 700, scipen = 100, digits = 5)

library(lubridate) # Used to manipulate dates
library(dplyr) # Used to manipulate data


### Look at demographicss in 2013, 2014, 2015, 2016
# Set up presence/absence at June 1 each year
yt_demogs <- yt_cleanadd %>%
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
# Currently one non-null person who seemed to have this issue, needs further review
yt_demogs %>% group_by(ssnnew_m3, lname_new_m3, fname_new_m3) %>%
  summarise(
    cnt12 = sum(june12),
    cnt13 = sum(june13),
    cnt14 = sum(june14),
    cnt15 = sum(june15),
    cnt16 = sum(june16)
      ) %>%
  ungroup() %>%
  filter(cnt12 > 1 | cnt13 > 1 | cnt14 > 1 | cnt15 > 1 | cnt16 > 1)


### Set up other variables
# Age at each date
yt_demogs <- yt_demogs %>%
  mutate(age12 = round(interval(start = dob_m3, end = ymd(20120601)) / years(1), 1),
         age13 = round(interval(start = dob_m3, end = ymd(20130601)) / years(1), 1),
         age14 = round(interval(start = dob_m3, end = ymd(20140601)) / years(1), 1),
         age15 = round(interval(start = dob_m3, end = ymd(20150601)) / years(1), 1),
         age16 = round(interval(start = dob_m3, end = ymd(20160601)) / years(1), 1)
          )

### Count per year
table(yt_demogs$june12, yt_demogs$yt, useNA = 'always')
table(yt_demogs$june13, yt_demogs$yt, useNA = 'always')
table(yt_demogs$june14, yt_demogs$yt, useNA = 'always')
table(yt_demogs$june15, yt_demogs$yt, useNA = 'always')
table(yt_demogs$june16, yt_demogs$yt, useNA = 'always')


### Look at gender each year
table(yt_demogs$gender_new[yt_demogs$june12 == 1], yt_demogs$yt[yt_demogs$june12 == 1], useNA = 'always')
prop.table(table(yt_demogs$gender_new[yt_demogs$june12 == 1], yt_demogs$yt[yt_demogs$june12 == 1], useNA = 'always'), 2)

table(yt_demogs$gender_new[yt_demogs$june13 == 1], yt_demogs$yt[yt_demogs$june13 == 1], useNA = 'always')
prop.table(table(yt_demogs$gender_new[yt_demogs$june13 == 1], yt_demogs$yt[yt_demogs$june13 == 1], useNA = 'always'), 2)

table(yt_demogs$gender_new[yt_demogs$june14 == 1], yt_demogs$yt[yt_demogs$june14 == 1], useNA = 'always')
prop.table(table(yt_demogs$gender_new[yt_demogs$june14 == 1], yt_demogs$yt[yt_demogs$june14 == 1], useNA = 'always'), 2)

table(yt_demogs$gender_new[yt_demogs$june15 == 1], yt_demogs$yt[yt_demogs$june15 == 1], useNA = 'always')
prop.table(table(yt_demogs$gender_new[yt_demogs$june15 == 1], yt_demogs$yt[yt_demogs$june15 == 1], useNA = 'always'), 2)

table(yt_demogs$gender_new[yt_demogs$june16 == 1], yt_demogs$yt[yt_demogs$june16 == 1], useNA = 'always')
prop.table(table(yt_demogs$gender_new[yt_demogs$june16 == 1], yt_demogs$yt[yt_demogs$june16 == 1], useNA = 'always'), 2)


### Look at age each year
stat.desc(yt_demogs$age12[yt_demogs$june12 == 1 & yt_demogs$yt == 1], basic = F)
stat.desc(yt_demogs$age12[yt_demogs$june12 == 1 & yt_demogs$yt == 0], basic = F)

stat.desc(yt_demogs$age13[yt_demogs$june13 == 1 & yt_demogs$yt == 1], basic = F)
stat.desc(yt_demogs$age13[yt_demogs$june13 == 1 & yt_demogs$yt == 0], basic = F)

stat.desc(yt_demogs$age14[yt_demogs$june14 == 1 & yt_demogs$yt == 1], basic = F)
stat.desc(yt_demogs$age14[yt_demogs$june14 == 1 & yt_demogs$yt == 0], basic = F)

stat.desc(yt_demogs$age15[yt_demogs$june15 == 1 & yt_demogs$yt == 1], basic = F)
stat.desc(yt_demogs$age15[yt_demogs$june15 == 1 & yt_demogs$yt == 0], basic = F)

stat.desc(yt_demogs$age16[yt_demogs$june16 == 1 & yt_demogs$yt == 1], basic = F)
stat.desc(yt_demogs$age16[yt_demogs$june16 == 1 & yt_demogs$yt == 0], basic = F)


### Look at race each year
yt_demogs %>% filter(june12 == 1) %>%
  group_by(yt) %>%
  summarise(aian = sum(r_aian_new_alone, na.rm = TRUE),
            asian = sum(r_asian_new_alone, na.rm = TRUE),
            black = sum(r_black_new_alone, na.rm = TRUE),
            multi = sum(r_multi_new, na.rm = TRUE),
            nhpi = sum(r_nhpi_new_alone, na.rm = TRUE),
            white = sum(r_white_new_alone, na.rm = TRUE)
            )

yt_demogs %>% filter(june13 == 1) %>%
  group_by(yt) %>%
  summarise(aian = sum(r_aian_new_alone, na.rm = TRUE),
            asian = sum(r_asian_new_alone, na.rm = TRUE),
            black = sum(r_black_new_alone, na.rm = TRUE),
            multi = sum(r_multi_new, na.rm = TRUE),
            nhpi = sum(r_nhpi_new_alone, na.rm = TRUE),
            white = sum(r_white_new_alone, na.rm = TRUE)
  )

yt_demogs %>% filter(june14 == 1) %>%
  group_by(yt) %>%
  summarise(aian = sum(r_aian_new_alone, na.rm = TRUE),
            asian = sum(r_asian_new_alone, na.rm = TRUE),
            black = sum(r_black_new_alone, na.rm = TRUE),
            multi = sum(r_multi_new, na.rm = TRUE),
            nhpi = sum(r_nhpi_new_alone, na.rm = TRUE),
            white = sum(r_white_new_alone, na.rm = TRUE)
  )

yt_demogs %>% filter(june15 == 1) %>%
  group_by(yt) %>%
  summarise(aian = sum(r_aian_new_alone, na.rm = TRUE),
            asian = sum(r_asian_new_alone, na.rm = TRUE),
            black = sum(r_black_new_alone, na.rm = TRUE),
            multi = sum(r_multi_new, na.rm = TRUE),
            nhpi = sum(r_nhpi_new_alone, na.rm = TRUE),
            white = sum(r_white_new_alone, na.rm = TRUE)
  )

yt_demogs %>% filter(june16 == 1) %>%
  group_by(yt) %>%
  summarise(aian = sum(r_aian_new_alone, na.rm = TRUE),
            asian = sum(r_asian_new_alone, na.rm = TRUE),
            black = sum(r_black_new_alone, na.rm = TRUE),
            multi = sum(r_multi_new, na.rm = TRUE),
            nhpi = sum(r_nhpi_new_alone, na.rm = TRUE),
            white = sum(r_white_new_alone, na.rm = TRUE)
  )


