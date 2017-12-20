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


#### Set up global parameter and call in libraries ####
# Turn scientific notation off and other settings
options(max.print = 700, scipen = 100, digits = 5)

library(housing) # contains many useful functions for analyses
library(openxlsx) # Used to import/export Excel files
library(stringr) # Used to manipulate string data
library(lubridate) # Used to manipulate dates
library(dplyr) # Used to manipulate data
library(pastecs) # Used for summary statistics
library(ggplot2) # Used to make plots
library(ggmap) # Used to incorporate Google maps data
library(rgdal) # Used to convert coordinates between ESRI and Google output

housing_path <- "//phdata01/DROF_DATA/DOH DATA/Housing"


#### Bring in combined PHA/Medicaid data with some demographics already run ####
pha_elig_final <- readRDS(file = paste0(housing_path, "/OrganizedData/pha_elig_final.Rda"))


#### Set up key variables ####
### Yesler Terrace and scattered sites indicators
yt_elig_final <- yesler(pha_elig_final)

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


yt_elig_final <- yt_elig_final %>%
  arrange(pid2, startdate_c, enddate_c) %>%
  mutate(
    # First ID the place for that row
    place = ifelse(is.na(agency_new), "U", 
                   ifelse(agency_new == "KCHA" & !is.na(agency_new), "K",
                          ifelse(agency_new == "SHA" & !is.na(agency_new) & yt == 0 & ss == 0, "O",
                                 ifelse(agency_new == "SHA" & !is.na(agency_new) & yt_old == 1, "Y",
                                        ifelse(agency_new == "SHA" & !is.na(agency_new) & yt_new == 1, "N",
                                               ifelse(agency_new == "SHA" & !is.na(agency_new) & yt == 0 & ss == 1, "S", NA)))))),
    start_type = NA,
    start_type = ifelse(pid2 != lag(pid2, 1) | is.na(lag(pid2, 1)), paste0("U", place),
                        ifelse(pid2 == lag(pid2, 1) & !is.na(lag(pid2, 1)), paste0(lag(place, 1), place), start_type)),
    end_type = NA,
    end_type = ifelse((pid2 != lead(pid2, 1) | is.na(lead(pid2, 1))) & enddate_c < as.Date("2017-09-15"), paste0(place, "U"),
                      ifelse(pid2 == lead(pid2, 1) & !is.na(lead(pid2, 1)), paste0(place, lead(place, 1)), end_type))
  )




#### Output to Excel for use in Tableau ####
# temp <- yt_elig_final
# # Strip out identifying variables
# temp <- temp %>% select(pid, gender_new_m6, race2, adult, senior, disability, DUAL_ELIG, COVERAGE_TYPE_IND, agency_new, 
#                         startdate_h:enddate_o, startdate_c:enroll_type, yt:age16) %>%
#   arrange(pid, startdate_c, enddate_c)
# 
# 
# write.xlsx(temp, file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/Summaries/yt_elig_demogs.xlsx")

### Save point
#saveRDS(yt_elig_final, file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/SHA cleaning/yt_elig_final.Rds")
#yt_elig_final <- readRDS("//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/SHA cleaning/yt_elig_final.Rds")



#### Look at demographics in 2012–2016 ####
### Annual counts
counts(yt_elig_final, agency = "sha", group_var = c("agency_new", "yt"), period = "year", unit = hhold_id_new) %>%
  filter(yt == 1) %>% group_by(date) %>% mutate(total = sum(.$count)) %>%
  ungroup()

counts(yt_elig_final, agency = "sha", group_var = c("agency_new", "yt"), period = "year", unit = pid2) %>%
  filter(yt == 1) %>% group_by(date) %>% mutate(total = sum(.$count)) %>%
  ungroup()
counts(yt_elig_final, agency = "sha", group_var = c("agency_new", "yt", "gender2_h"), period = "year", unit = pid2) %>%
  filter(yt == 1) %>% group_by(date) %>% mutate(total = sum(.$count)) %>%
  ungroup() %>%
  mutate(percent = count / total) %>% select(gender2_h, date, period, count, total, percent)
counts(yt_elig_final, agency = "sha", group_var = c("agency_new", "yt", "agegrp_h"), period = "year", unit = pid2) %>%
  filter(yt == 1) %>% group_by(date) %>% mutate(total = sum(.$count)) %>%
  ungroup() %>%
  mutate(percent = count / total) %>% select(agegrp_h, date, period, count, total, percent)
counts(yt_elig_final, agency = "sha", group_var = c("agency_new", "yt", "race_h"), period = "year", unit = pid2) %>%
  filter(yt == 1) %>% group_by(date) %>% mutate(total = sum(.$count)) %>%
  ungroup() %>%
  mutate(percent = count / total) %>% select(race_h, date, period, count, total, percent)
counts(yt_elig_final, agency = "sha", group_var = c("agency_new", "yt", "disability_h"), period = "year", unit = pid2) %>%
  filter(yt == 1) %>% group_by(date) %>% mutate(total = sum(.$count)) %>%
  ungroup() %>%
  mutate(percent = count / total) %>% select(disability_h, date, period, count, total, percent)
counts(yt_elig_final, agency = "sha", group_var = c("agency_new", "yt", "time_prog"), period = "year", unit = pid2) %>%
  filter(yt == 1) %>% group_by(date) %>% mutate(total = sum(.$count)) %>%
  ungroup() %>%
  mutate(percent = count / total) %>% select(time_prog, date, period, count, total, percent)
counts(yt_elig_final, agency = "sha", group_var = c("agency_new", "yt", "enroll_type"), period = "year", unit = pid2) %>%
  filter(yt == 1) %>% group_by(date) %>% mutate(total = sum(.$count)) %>%
  ungroup() %>%
  mutate(percent = count / total) %>% select(enroll_type, date, period, count, total, percent)



### Count for a given date
counts(yt_elig_final, agency = "sha", group_var = c("agency_new", "yt"), period = "date", unit = hhold_id_new) %>%
  filter(yt == 1) %>%
  group_by(date) %>%
  mutate(total = sum(.$count)) %>%
  ungroup()
counts(yt_elig_final, agency = "sha", group_var = c("agency_new", "yt"), period = "date", unit = pid2) %>%
  filter(yt == 1) %>%
  group_by(date) %>%
  mutate(total = sum(.$count)) %>%
  ungroup()


### Look at gender each year
counts(yt_elig_final, agency = "sha", group_var = c("agency_new", "yt", "gender2_h"), period = "date", unit = hhold_id_new) %>%
  filter(yt == 1) %>%
  group_by(date) %>%
  mutate(total = sum(.$count)) %>%
  ungroup() %>%
  mutate(percent = count / total) %>%
  select(gender2_h, date, period, count, total, percent)
counts(yt_elig_final, agency = "sha", group_var = c("agency_new", "yt", "gender2_h"), period = "date", unit = pid2) %>%
  filter(yt == 1) %>%
  group_by(date) %>%
  mutate(total = sum(.$count)) %>%
  ungroup() %>%
  mutate(percent = count / total) %>%
  select(gender2_h, date, period, count, total, percent)


### Look at age groups each year
counts(yt_elig_final, agency = "sha", group_var = c("agency_new", "yt", "agegrp_h"), period = "date", unit = hhold_id_new) %>%
  filter(yt == 1) %>%
  group_by(date) %>%
  mutate(total = sum(.$count)) %>%
  mutate(percent = count / total) %>%
  select(agegrp_h, date, period, count, total, percent)
counts(yt_elig_final, agency = "sha", group_var = c("agency_new", "yt", "agegrp_h"), period = "date", unit = pid2) %>%
  filter(yt == 1) %>%
  group_by(date) %>%
  mutate(total = sum(.$count)) %>%
  ungroup() %>%
  mutate(percent = count / total) %>%
  select(agegrp_h, date, period, count, total, percent)

### Look at age median and mean each year
temp <- yt_elig_final %>%
  filter(agency_new == "SHA") %>%
  distinct(pid2, .keep_all = TRUE)

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

rm(temp)


### Look at race each year
f_phacount(yt_elig_final, agency = "sha", group_var = c("agency_new", "yt", "race2"), period = "date", unit = hhold_id_new) %>%
  filter(yt == 1) %>%
  group_by(date) %>%
  mutate(total = sum(.$count)) %>%
  ungroup() %>%
  mutate(percent = count / total) %>%
  select(race2, date, period, count, total, percent)
f_phacount(yt_elig_final, agency = "sha", group_var = c("agency_new", "yt", "race2"), period = "date", unit = pid2) %>%
  filter(yt == 1) %>%
  group_by(date) %>%
  mutate(total = sum(.$count)) %>%
  ungroup() %>%
  mutate(percent = count / total) %>%
  select(race2, date, period, count, total, percent)



### Summarize for Tableau
# Monthly
race_count_hh_yt <- f_phacount(yt_elig_final, group_var = c("yt", "race2", "enroll_type"), period = "month", agency = "sha", unit = hhold_id_new)
agegrp_count_hh_yt <- f_phacount(yt_elig_final, group_var = c("yt", "agegrp", "enroll_type"), period = "month", agency = "sha", unit = hhold_id_new)
disability_count_hh_yt <- f_phacount(yt_elig_final, group_var = c("yt", "disability2", "enroll_type"), period = "month", agency = "sha", unit = hhold_id_new)
gender_count_hh_yt <- f_phacount(yt_elig_final, group_var = c("yt", "gender2", "enroll_type"), period = "month", agency = "sha", unit = hhold_id_new)

race_count_ind_yt <- f_phacount(yt_elig_final, group_var = c("yt", "race2", "enroll_type"), period = "month", agency = "sha", unit = pid2)
agegrp_count_ind_yt <- f_phacount(yt_elig_final, group_var = c("yt", "agegrp", "enroll_type"), period = "month", agency = "sha", unit = pid2)
disability_count_ind_yt <- f_phacount(yt_elig_final, group_var = c("yt", "disability2", "enroll_type"), period = "month", agency = "sha", unit = pid2)
gender_count_ind_yt <- f_phacount(yt_elig_final, group_var = c("yt", "gender2", "enroll_type"), period = "month", agency = "sha", unit = pid2)

# Yearly
race_count_yr_hh_yt <- f_phacount(yt_elig_final, group_var = c("yt", "race2", "enroll_type"), period = "year", agency = "sha", unit = hhold_id_new)
agegrp_count_yr_hh_yt <- f_phacount(yt_elig_final, group_var = c("yt", "agegrp", "enroll_type"), period = "year", agency = "sha", unit = hhold_id_new)
disability_count_yr_hh_yt <- f_phacount(yt_elig_final, group_var = c("yt", "disability2", "enroll_type"), period = "year", agency = "sha", unit = hhold_id_new)
gender_count_yr_hh_yt <- f_phacount(yt_elig_final, group_var = c("yt", "gender2", "enroll_type"), period = "year", agency = "sha", unit = hhold_id_new)

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
         unit = car::recode(unit, "'hhold_id_new' = 'Households'; 'pid2' = 'Individuals'"),
         date_yr = ifelse(period == "year", year(date), NA))

write.xlsx(yt_count, file = paste0("//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/Summaries/YT enrollment count_", Sys.Date(), ".xlsx"))


rm(list = ls(pattern = "^race"))
rm(list = ls(pattern = "^age"))
rm(list = ls(pattern = "^disability"))
rm(list = ls(pattern = "^gender"))
gc()


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
  

