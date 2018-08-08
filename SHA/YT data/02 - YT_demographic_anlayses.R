###############################################################################
# OVERVIEW:
# Code to examine Yesler Terrace and Scattered sites data (housing and health)
#
# STEPS:
# 01 - Set up YT parameters in combined PHA/Medicaid data
# 02 - Conduct demographic analyses and produce visualizations ### (THIS CODE) ###
# 03 - Bring in health conditions and join to demographic data
# 04 - Conduct health condition analyses (multiple files)
#
# Alastair Matheson (PHSKC-APDE)
# alastair.matheson@kingcounty.gov
# 2017-06-30
#
###############################################################################


#### Set up global parameter and call in libraries ####
# Turn scientific notation off and other settings
options(max.print = 700, scipen = 100, digits = 5)

library(housing) # contains many useful functions for analyses
library(openxlsx) # Used to import/export Excel files
library(lubridate) # Used to manipulate dates
library(tidyverse) # Used to manipulate data
library(pastecs) # Used for summary statistics
library(ggmap) # Used to incorporate Google maps data
library(rgdal) # Used to convert coordinates between ESRI and Google output

housing_path <- "//phdata01/DROF_DATA/DOH DATA/Housing"


#### Bring in combined PHA/Medicaid data with some demographics already run ####
yt_mcaid_final <- readRDS(file = paste0(housing_path, 
                                       "/OrganizedData/SHA cleaning/yt_mcaid_final.Rds"))

# Retain only people living at YT or SS
yt_ss <- yt_mcaid_final %>% filter(yt == 1 | ss == 1)


#### Analyses in this code ####
# 1) General demographics of people in YT and SS at any point each year
# 2) Counts and demographics of people in YT at the end of each year
# X) Movement patterns in the data (Sankey and maps)


### Movements within data
# Use simplified system for describing movement

# First letter of start_type describes previous address,
# Second letter of start_type describes current address

# First letter of end_type describes current address,
# Second letter of end_type describes next address

#   K = KCHA
#   N = YT address (new unit)
#   O = non-YT, non-scattered site SHA unit
#   S = SHA scattered site
#   U = unknown (i.e., new into SHA system, mostly people who only had Medicaid but not PHA coverage)
#   Y = YT address (old unit)



#### 1) Demographics of people in YT/SS at any point in the year, 2012–2017 ####

### Annual counts
# Write function to look over all demogs of interest
yt_demogs_f <- function(df, group = quos(yt), unit = pid2, period = "year") {

  unit2 <- enquo(unit)
  
  result <- popcount(df, group_var = group, yearmin = 2012, yearmax = 2017,
                     period = period, unit = !!unit2) %>%
    mutate(year = year(date)) %>%
    group_by(year, yt) %>%
    mutate(total = sum(pop, na.rm = T)) %>%
    ungroup %>%
    mutate(percent = round(pop / total*100, 1)) %>%
    select(year, yt, !!!group, pop, pt_days, total, percent, unit, period)
    
  return(result)
}

overall <- yt_demogs_f(df = yt_ss, group = quos(yt), unit = pid2)

hholds <- yt_demogs_f(df = yt_ss, group = quos(yt), unit = hh_id_new_h)
overall <- yt_demogs_f(df = yt_ss, group = quos(yt), unit = pid2)
enroll <- yt_demogs_f(df = yt_ss, group = quos(yt, enroll_type), unit = pid2)
gender <- yt_demogs_f(df = yt_ss, group = quos(yt, gender_c), unit = pid2)
age <- yt_demogs_f(df = yt_ss, group = quos(yt, agegrp_h), unit = pid2)
race <- yt_demogs_f(df = yt_ss, group = quos(yt, race_c), unit = pid2)
los <- yt_demogs_f(df = yt_ss, group = quos(yt, time_housing), unit = pid2)


popcount(yt_ss, group_var = quos(yt), period = "date")
popcount2(yt_ss, group_var = quos(yt), period = "date")

### Look at age median and mean each year
temp <- yt_ss %>% select(pid2, yt, matches("age1[0-9]?$"), matches("pt1[0-9]?_h")) %>% 
  distinct(pid2, .keep_all = TRUE)

stat.desc(temp$age12[temp$pt12_h > 30 & temp$yt == 1], basic = F)
stat.desc(temp$age12[temp$pt12_h > 30 & temp$yt == 0], basic = F)

stat.desc(temp$age13[temp$pt13_h > 30 & temp$yt == 1], basic = F)
stat.desc(temp$age13[temp$pt13_h > 30 & temp$yt == 0], basic = F)

stat.desc(temp$age14[temp$pt14_h > 30 & temp$yt == 1], basic = F)
stat.desc(temp$age14[temp$pt14_h > 30 & temp$yt == 0], basic = F)

stat.desc(temp$age15[temp$pt15_h > 30 & temp$yt == 1], basic = F)
stat.desc(temp$age15[temp$pt15_h > 30 & temp$yt == 0], basic = F)

stat.desc(temp$age16[temp$pt16_h > 30 & temp$yt == 1], basic = F)
stat.desc(temp$age16[temp$pt16_h > 30 & temp$yt == 0], basic = F)

stat.desc(temp$age17[temp$pt17_h > 30 & temp$yt == 1], basic = F)
stat.desc(temp$age17[temp$pt17_h > 30 & temp$yt == 0], basic = F)

rm(temp)


### Summarize for Tableau
# Monthly
race_count_hh_yt <- f_phacount(yt_elig_final, group_var = c("yt", "race2", "enroll_type"), period = "month", agency = "sha", unit = hh_id_new_h)
agegrp_count_hh_yt <- f_phacount(yt_elig_final, group_var = c("yt", "agegrp", "enroll_type"), period = "month", agency = "sha", unit = hh_id_new_h)
disability_count_hh_yt <- f_phacount(yt_elig_final, group_var = c("yt", "disability2", "enroll_type"), period = "month", agency = "sha", unit = hh_id_new_h)
gender_count_hh_yt <- f_phacount(yt_elig_final, group_var = c("yt", "gender2", "enroll_type"), period = "month", agency = "sha", unit = hh_id_new_h)

race_count_ind_yt <- f_phacount(yt_elig_final, group_var = c("yt", "race2", "enroll_type"), period = "month", agency = "sha", unit = pid2)
agegrp_count_ind_yt <- f_phacount(yt_elig_final, group_var = c("yt", "agegrp", "enroll_type"), period = "month", agency = "sha", unit = pid2)
disability_count_ind_yt <- f_phacount(yt_elig_final, group_var = c("yt", "disability2", "enroll_type"), period = "month", agency = "sha", unit = pid2)
gender_count_ind_yt <- f_phacount(yt_elig_final, group_var = c("yt", "gender2", "enroll_type"), period = "month", agency = "sha", unit = pid2)

# Yearly
race_count_yr_hh_yt <- f_phacount(yt_elig_final, group_var = c("yt", "race2", "enroll_type"), period = "year", agency = "sha", unit = hh_id_new_h)
agegrp_count_yr_hh_yt <- f_phacount(yt_elig_final, group_var = c("yt", "agegrp", "enroll_type"), period = "year", agency = "sha", unit = hh_id_new_h)
disability_count_yr_hh_yt <- f_phacount(yt_elig_final, group_var = c("yt", "disability2", "enroll_type"), period = "year", agency = "sha", unit = hh_id_new_h)
gender_count_yr_hh_yt <- f_phacount(yt_elig_final, group_var = c("yt", "gender2", "enroll_type"), period = "year", agency = "sha", unit = hh_id_new_h)

race_count_yr_ind_yt <- f_phacount(yt_elig_final, group_var = c("yt", "race2", "enroll_type"), period = "year", agency = "sha", unit = pid2)
agegrp_count_yr_ind_yt <- f_phacount(yt_elig_final, group_var = c("yt", "agegrp", "enroll_type"), period = "year", agency = "sha", unit = pid2)
disability_count_yr_ind_yt <- f_phacount(yt_elig_final, group_var = c("yt", "disability2", "enroll_type"), period = "year", agency = "sha", unit = pid2)
gender_count_yr_ind_yt <- f_phacount(yt_elig_final, group_var = c("yt", "gender2", "enroll_type"), period = "year", agency = "sha", unit = pid2)

# Combine files
race_count <- bind_rows(race_count_hh_yt, race_count_ind_yt, race_count_yr_hh_yt, race_count_yr_ind_yt) %>% mutate(category = "Race", group = race2)
agegrp_count <- bind_rows(agegrp_count_hh_yt, agegrp_count_ind_yt, agegrp_count_yr_hh_yt, agegrp_count_yr_ind_yt) %>% mutate(category = "Age group", group = agegrp)
disability_count <- bind_rows(disability_count_hh_yt, disability_count_ind_yt, disability_count_yr_hh_yt, disability_count_yr_ind_yt) %>% 
  mutate(category = "Disability", group = disability2)
gender_count <- bind_rows(gender_count_hh_yt, gender_count_ind_yt, gender_count_yr_hh_yt, gender_count_yr_ind_yt) %>% mutate(category = "Gender", group = gender2)

yt_count <- bind_rows(race_count, agegrp_count, disability_count, gender_count) %>%
  mutate(medicaid = car::recode(enroll_type, "'b' = 'Medicaid'; 'h' = 'No Medicaid'"),
         unit = car::recode(unit, "'hh_id_new_h' = 'Households'; 'pid2' = 'Individuals'"),
         date_yr = ifelse(period == "year", year(date), NA))

write.xlsx(yt_count, file = paste0("//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/Summaries/YT enrollment count_", Sys.Date(), ".xlsx"))


rm(list = ls(pattern = "^race"))
rm(list = ls(pattern = "^age"))
rm(list = ls(pattern = "^disability"))
rm(list = ls(pattern = "^gender"))
gc()


#### Look at drop off by year ####

# Pull out person time for each year (eventually fold into function below)
yt12 <- yt_elig_final %>%
  filter((yt == 1 | ss == 1) & !is.na(pt12) & enroll_type == "b") %>%
  select(pid2, startdate_c, enddate_c, pt12, yt, enroll_type) %>%
  # Sum up time in that year
  group_by(pid2, yt) %>% mutate(pt_t = sum(pt12)) %>% ungroup() %>%
  select(-startdate_c, -enddate_c, -pt12) %>% distinct()

yt13 <- yt_elig_final %>%
  filter((yt == 1 | ss == 1) & !is.na(pt13) & enroll_type == "b") %>%
  select(pid2, startdate_c, enddate_c, pt13, yt, enroll_type) %>%
  # Sum up time in that year
  group_by(pid2, yt) %>% mutate(pt_t = sum(pt13)) %>% ungroup() %>%
  select(-startdate_c, -enddate_c, -pt13) %>% distinct()

yt14 <- yt_elig_final %>%
  filter((yt == 1 | ss == 1) & !is.na(pt14) & enroll_type == "b" & dual_elig_m == "N") %>%
  select(pid2, startdate_c, enddate_c, pt14, yt, enroll_type) %>%
  # Sum up time in that year
  group_by(pid2, yt) %>% mutate(pt_t = sum(pt14)) %>% ungroup() %>%
  select(-startdate_c, -enddate_c, -pt14) %>% distinct()

yt15 <- yt_elig_final %>%
  filter((yt == 1 | ss == 1) & !is.na(pt15) & enroll_type == "b") %>%
  select(pid2, startdate_c, enddate_c, pt15, yt, enroll_type) %>%
  # Sum up time in that year
  group_by(pid2, yt) %>% mutate(pt_t = sum(pt15)) %>% ungroup() %>%
  select(-startdate_c, -enddate_c, -pt15) %>% distinct()

yt16 <- yt_elig_final %>%
  filter((yt == 1 | ss == 1) & !is.na(pt16) & enroll_type == "b" & dual_elig_m == "N") %>%
  select(pid2, startdate_c, enddate_c, pt16, yt, enroll_type) %>%
  # Sum up time in that year
  group_by(pid2, yt) %>% mutate(pt_t = sum(pt16)) %>% ungroup() %>%
  select(-startdate_c, -enddate_c, -pt16) %>% distinct()



# Make function to show the proportion of people still enrolled after x days
# Group results by YT (yt = 1) or SS (yt = 0)
surv_f <- function(df, x) {
  df %>%
    group_by(yt) %>%
    mutate(num = ifelse(pt_t >= x, 1, 0)) %>%
    summarise(prop = sum(num)/n())
}

# Set up number of days in a year
yr_leap <- data.frame(days = rep(1:366, each = 2))
yr <- data.frame(days = rep(1:365, each = 2))

# Apply function to all years
surv12 <- cbind(yr_leap, bind_rows(lapply(1:366, surv_f, df = yt12)), year = 2012)
surv13 <- cbind(yr, bind_rows(lapply(1:365, surv_f, df = yt13)), year = 2013)
surv14 <- cbind(yr, bind_rows(lapply(1:365, surv_f, df = yt14)), year = 2014)
surv15 <- cbind(yr, bind_rows(lapply(1:365, surv_f, df = yt15)), year = 2015)
surv16 <- cbind(yr_leap, bind_rows(lapply(1:366, surv_f, df = yt16)), year = 2016)

# Merge into single df
surv <- bind_rows(surv12, surv13, surv14, surv15, surv16) %>%
  mutate(year = as.factor(year),
         yt = factor(yt, levels = c(0, 1), labels = c("SS", "YT")))

# Plot survival curve
ggplot(surv, aes(x = days, y = prop)) +
  geom_line(aes(color = year, group = year)) +
  scale_colour_brewer(type = "qual") +
  geom_hline(yintercept = 0.9, linetype = "dashed", color = "#767F8B") +
  geom_vline(xintercept = 300, linetype = "dashed", color = "#767F8B") +
  facet_wrap( ~ yt, ncol = 1)


### Compare demographics by different cutoff groups
## 2012
# Make cutoffs
yt12 <- yt12 %>%
  mutate(gt300 = ifelse(pt_t >= 300, 1, 0),
         gt330 = ifelse(pt_t >= 330, 1, 0))
yt12_demog <- yt_elig_final %>%
  filter((yt == 1 | ss == 1) & !is.na(pt12) & enroll_type == "b") %>%
  select(pid2, yt, race_c, hisp_c, gender_c, age12_h, start_housing) %>%
  distinct() %>%
  right_join(., yt12, by = c("pid2", "yt"))

# Counts
yt12_demog %>% 
  group_by(yt, gt300) %>%
  summarise(count = n_distinct(pid2))
yt12_demog %>% 
  group_by(yt, gt330) %>%
  summarise(count = n_distinct(pid2))

# Age
stat.desc(yt12_demog$age12_h[yt12_demog$yt == 1 & yt12_demog$gt300 == 0], basic = F)
stat.desc(yt12_demog$age12_h[yt12_demog$yt == 1 & yt12_demog$gt300 == 1], basic = F)
stat.desc(yt12_demog$age12_h[yt12_demog$yt == 0 & yt12_demog$gt300 == 0], basic = F)
stat.desc(yt12_demog$age12_h[yt12_demog$yt == 0 & yt12_demog$gt300 == 1], basic = F)

stat.desc(yt12_demog$age12_h[yt12_demog$yt == 1 & yt12_demog$gt330 == 0], basic = F)
stat.desc(yt12_demog$age12_h[yt12_demog$yt == 1 & yt12_demog$gt330 == 1], basic = F)
stat.desc(yt12_demog$age12_h[yt12_demog$yt == 0 & yt12_demog$gt330 == 0], basic = F)
stat.desc(yt12_demog$age12_h[yt12_demog$yt == 0 & yt12_demog$gt330 == 1], basic = F)

# Gender
yt12_demog %>% 
  group_by(yt, gt300) %>%
  summarise(female = 2 - mean(gender_c, na.rm = T))
yt12_demog %>% 
  group_by(yt, gt330) %>%
  summarise(female = 2 - mean(gender_c, na.rm = T))

# Race
temp <- yt12_demog %>% 
  group_by(yt, gt300, race_c) %>%
  summarise(count = n_distinct(pid2)) %>%
  mutate(total = sum(count, na.rm = T),
         pct = count / total)
temp <- yt12_demog %>% 
  group_by(yt, gt330, race_c) %>%
  summarise(count = n_distinct(pid2)) %>%
  mutate(total = sum(count, na.rm = T),
         pct = count / total)
rm(temp)


## 2014
# Make cutoffs
yt14 <- yt14 %>%
  mutate(gt300 = ifelse(pt_t >= 300, 1, 0),
         gt330 = ifelse(pt_t >= 330, 1, 0))
yt14_demog <- yt_elig_final %>%
  filter((yt == 1 | ss == 1) & !is.na(pt14) & enroll_type == "b") %>%
  select(pid2, yt, race_c, hisp_c, gender_c, age14_h, start_housing) %>%
  distinct() %>%
  right_join(., yt14, by = c("pid2", "yt"))

# Counts
yt14_demog %>% 
  group_by(yt, gt300) %>%
  summarise(count = n_distinct(pid2))
yt14_demog %>% 
  group_by(yt, gt330) %>%
  summarise(count = n_distinct(pid2))

# Age
stat.desc(yt14_demog$age14_h[yt14_demog$yt == 1 & yt14_demog$gt300 == 0], basic = F)
stat.desc(yt14_demog$age14_h[yt14_demog$yt == 1 & yt14_demog$gt300 == 1], basic = F)
stat.desc(yt14_demog$age14_h[yt14_demog$yt == 0 & yt14_demog$gt300 == 0], basic = F)
stat.desc(yt14_demog$age14_h[yt14_demog$yt == 0 & yt14_demog$gt300 == 1], basic = F)

stat.desc(yt14_demog$age14_h[yt14_demog$yt == 1 & yt14_demog$gt330 == 0], basic = F)
stat.desc(yt14_demog$age14_h[yt14_demog$yt == 1 & yt14_demog$gt330 == 1], basic = F)
stat.desc(yt14_demog$age14_h[yt14_demog$yt == 0 & yt14_demog$gt330 == 0], basic = F)
stat.desc(yt14_demog$age14_h[yt14_demog$yt == 0 & yt14_demog$gt330 == 1], basic = F)

# Gender
yt14_demog %>% 
  group_by(yt, gt300) %>%
  summarise(female = 2 - mean(gender_c, na.rm = T))
yt14_demog %>% 
  group_by(yt, gt330) %>%
  summarise(female = 2 - mean(gender_c, na.rm = T))

# Race
temp <- yt14_demog %>% 
  group_by(yt, gt300, race_c) %>%
  summarise(count = n_distinct(pid2)) %>%
  mutate(total = sum(count, na.rm = T),
         pct = count / total)
temp <- yt14_demog %>% 
  group_by(yt, gt330, race_c) %>%
  summarise(count = n_distinct(pid2)) %>%
  mutate(total = sum(count, na.rm = T),
         pct = count / total)
rm(temp)


## 2016
# Make cutoffs
yt16 <- yt16 %>%
  mutate(gt300 = ifelse(pt_t >= 300, 1, 0),
         gt330 = ifelse(pt_t >= 330, 1, 0))
yt16_demog <- yt_elig_final %>%
  filter((yt == 1 | ss == 1) & !is.na(pt16) & enroll_type == "b") %>%
  select(pid2, yt, race_c, hisp_c, gender_c, age16_h, start_housing) %>%
  distinct() %>%
  right_join(., yt16, by = c("pid2", "yt"))

# Counts
yt16_demog %>% 
  group_by(yt, gt300) %>%
  summarise(count = n_distinct(pid2))
yt16_demog %>% 
  group_by(yt, gt330) %>%
  summarise(count = n_distinct(pid2))

# Age
stat.desc(yt16_demog$age16_h[yt16_demog$yt == 1 & yt16_demog$gt300 == 0], basic = F)
stat.desc(yt16_demog$age16_h[yt16_demog$yt == 1 & yt16_demog$gt300 == 1], basic = F)
stat.desc(yt16_demog$age16_h[yt16_demog$yt == 0 & yt16_demog$gt300 == 0], basic = F)
stat.desc(yt16_demog$age16_h[yt16_demog$yt == 0 & yt16_demog$gt300 == 1], basic = F)

stat.desc(yt16_demog$age16_h[yt16_demog$yt == 1 & yt16_demog$gt330 == 0], basic = F)
stat.desc(yt16_demog$age16_h[yt16_demog$yt == 1 & yt16_demog$gt330 == 1], basic = F)
stat.desc(yt16_demog$age16_h[yt16_demog$yt == 0 & yt16_demog$gt330 == 0], basic = F)
stat.desc(yt16_demog$age16_h[yt16_demog$yt == 0 & yt16_demog$gt330 == 1], basic = F)

# Gender
yt16_demog %>% 
  group_by(yt, gt300) %>%
  summarise(female = 2 - mean(gender_c, na.rm = T))
yt16_demog %>% 
  group_by(yt, gt330) %>%
  summarise(female = 2 - mean(gender_c, na.rm = T))

# Race
temp <- yt16_demog %>% 
  group_by(yt, gt300, race_c) %>%
  summarise(count = n_distinct(pid2)) %>%
  mutate(total = sum(count, na.rm = T),
         pct = count / total)
temp <- yt16_demog %>% 
  group_by(yt, gt330, race_c) %>%
  summarise(count = n_distinct(pid2)) %>%
  mutate(total = sum(count, na.rm = T),
         pct = count / total)
rm(temp)







#### Mapping ####
### Look at location to make geocoding looks ok
yt_mapdata <- yt_elig_final %>% filter(dec12_h == 1 & yt == 1 & !is.na(X) & !is.na(Y)) %>% mutate(northing = X, easting = Y)
yt_mapdata2 <- yt_mapdata %>% filter(str_detect(unit_concat, "110 8TH")) %>% select(unit_concat, X, Y) %>% mutate(northing = X, easting = Y)
# Convert spatial coordinates to one that can go on Google maps (they are stored in 1983 HARN State Plane WA N FIPS 4601)
coordinates(yt_mapdata) <- ~ easting + northing
proj4string(yt_mapdata) <- CRS("+proj=lcc +lat_1=47.5 +lat_2=49.73333333333333 +lat_0=47 +lon_0=-120.8333333333333 +x_0=500000.0000000001 +y_0=0 +ellps=GRS80 +to_meter=0.3048006096012192 +no_defs")
yt_mapdata <- spTransform(yt_mapdata, CRS("+init=epsg:4326"))

mean(yt_mapdata$easting, na.rm = T)
mean(yt_mapdata$northing, na.rm = T)

# Add basemap
yt_map <- get_map(location = c(lon = mean(yt_mapdata$easting, na.rm = T), lat = mean(yt_mapdata$northing, na.rm = T)), zoom = 16, crop = T)

ggmap(yt_map) + geom_point(data = as.data.frame(coordinates(yt_mapdata)), aes(x = easting, y = northing))


