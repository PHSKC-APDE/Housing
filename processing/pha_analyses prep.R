###############################################################################
# OVERVIEW:
# Code to create a cleaned person table from the combined
# King County Housing Authority and Seattle Housing Authority data sets
# Aim is to have a single row per contiguous time in a house per person
#
# STEPS:
# Process raw KCHA data and load to SQL database
# Process raw SHA data and load to SQL database
# Bring in individual PHA datasets and combine into a single file
# Deduplicate data and tidy up via matching process
# Recode race and other demographics
# Clean up addresses and geocode
# Consolidate data rows
# Add in final data elements and set up analyses ### (THIS CODE) ###
# Join with Medicaid eligibility data and set up analyses
#
# Alastair Matheson (PHSKC-APDE)
# alastair.matheson@kingcounty.gov
# 2016-08-13, split into separate files 2017-10
#
###############################################################################

rm(list=ls()) #reset
options(max.print = 900, tibble.print_max = 50, scipen = 999, width = 100)
gc()
#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, scipen = 999)
housing_path <- "//phdata01/DROF_DATA/DOH DATA/Housing"

library(colorout)
library(housing)
library(openxlsx) # Used to import/export Excel files
library(stringr) # Used to manipulate string data
library(lubridate) # Used to manipulate dates
library(tidyverse)

#### Bring in data ####
# pha_cleanadd_sort_dedup <- readRDS(file = "~/data/Housing/OrganizedData/pha_cleanadd_sort_dedup.Rdata")
load(file = "~/data/Housing/OrganizedData/pha_cleanadd_sort_dedup.Rdata")



### Strip out some variables that no longer have meaning
# (e.g., act_date, act_type, income data, household size (this will need to be remade))
pha_longitudinal <- pha_cleanadd_sort_dedup %>%
  select(-(hhold_inc_fixed:hhold_inc_vary), -(act_type:correction_date), -(reexam_date:agency),
         -(portability:cost_pha), -inc_fixed, -(sha_source),
         -(r_white_new_tot:r_hisp_new_tot), -add_num, -drop, -next_hh_act, -add_yr)

glimpse(pha_longitudinal)
### Fix date format
pha_longitudinal <- pha_longitudinal %>% mutate(dob_m6 = as.Date(dob_m6, origin = "1970-01-01"))

### Fix up remaining program categories (will be moved earlier eventually)
# Bring in program name mapping table and add new variables
#program_map <- read.xlsx("//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/Program name mapping.xlsx")
# No longer needed. Still some KCHA PBS8 properties that could have their portfolio updated based on property name
# but numbers are small


### Set up time in housing for each row and note when there was a gap in coverage
pha_longitudinal <- pha_longitudinal %>%
  mutate(cov_time = interval(start = startdate, end = enddate) / ddays(1) + 1,
         gap = ifelse(is.na(lag(pid, 1)) | pid != lag(pid, 1), 0,
                      ifelse(startdate - lag(enddate, 1) > 62, 1, NA))) %>%
  # Find the number of unique periods a person was in housing
  group_by(pid, gap) %>%
  mutate(period = row_number() * gap + 1) %>%
  ungroup() %>%
  # Fill in missing data
  tidyr::fill(., period) %>%
  select(-gap)


### Flag date to use when calculating length of stay
pha_longitudinal <- los(pha_longitudinal)

# Set up length of stay using the end date
pha_longitudinal <- pha_longitudinal %>%
  mutate(
    time_housing = round(interval(start = start_housing, end = enddate) / years(1), 1),
    time_pha = round(interval(start = start_pha, end = enddate) / years(1), 1),
    time_prog = round(interval(start = start_prog, end = enddate) / years(1), 1)
  )

### Age
# Age at each date
pha_longitudinal <- pha_longitudinal %>%
  mutate(age12 = round(interval(start = dob_m6, end = ymd(20121231)) / years(1), 1),
         age13 = round(interval(start = dob_m6, end = ymd(20131231)) / years(1), 1),
         age14 = round(interval(start = dob_m6, end = ymd(20141231)) / years(1), 1),
         age15 = round(interval(start = dob_m6, end = ymd(20151231)) / years(1), 1),
         age16 = round(interval(start = dob_m6, end = ymd(20161231)) / years(1), 1)
  )

# Single age grouping
# Note: this is only valid for the person at the time of the start date
# Need to recalculate for each time period being analyzed
pha_longitudinal <- pha_longitudinal %>%
  mutate(agegrp = ifelse(adult == 0 & !is.na(adult), "Youth",
                         ifelse(adult == 1 & senior == 0 & !is.na(adult), "Working age",
                                ifelse(adult == 1 & senior == 1 & !is.na(adult), "Senior", NA))))

### Recode gender and disability
pha_longitudinal <- pha_longitudinal %>%
  mutate(gender2 = car::recode(gender_new_m6, "'1' = 'Female'; '2' = 'Male'; else = NA"),
         disability2 = car::recode(disability, "'1' = 'Disabled'; '0' = 'Not disabled'; else = NA"))


### ZIPs to restrict to KC
# zips <- read.xlsx("//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/ZIP filter for KC.xlsx")

# ==========================================================================
# Zip file not provided, alternative method below
# ==========================================================================
zips <- c(98126,98133,98136,98134,98138,98144,98146,98148,98155,98154,98158,98164,98166,98168,98177,98178,98190,98188,98198,98195,98199,98224,98251,98288,98354,98001,98003,98002,98005,98004,98007,98006,98009,98008,98011,98010,98014,98019,98022,98024,98023,98025,98028,98027,98030,98029,98032,98031,98034,98033,98038,98040,98039,98042,98045,98047,98051,98050,98053,98052,98055,98057,98056,98059,98058,98068,98065,98070,98072,98075,98074,98077,98083,98092,98101,98103,98102,98105,98104,98107,98106,98109,98108,98112,98115,98114,98117,98116,98119,98118,98122,98121,98125)

zip2 <- c(98001,98001,98001,98002,98003,98003,98004,98004,98004,98004,98004,98005,98006,98007,98008,98009,98010,98011,98013,98013,98014,98015,98019,98022,98023,98023,98024,98025,98027,98028,98028,98029,98030,98031,98032,98033,98034,98035,98038,98039,98040,98041,98042,98042,98045,98047,98047,98050,98051,98052,98053,98054,98055,98056,98056,98057,98058,98059,98059,98062,98063,98063,98064,98065,98068,98068,98070,98071,98072,98073,98074,98074,98075,98075,98083,98092,98093,98093,98101,98102,98103,98104,98105,98106,98107,98108,98108,98109,98111,98112,98114,98115,98116,98117,98118,98119,98121,98122,98124,98125,98126,98131,98132,98133,98134,98136,98138,98144,98145,98146,98148,98148,98148,98148,98148,98154,98155,98155,98155,98155,98155,98158,98158,98160,98161,98164,98166,98166,98166,98168,98168,98168,98168,98171,98174,98177,98177,98178,98178,98188,98188,98188,98198,98198,98198,98198,98199,98224,98288) %>% unique()

# ==========================================================================
# Zip codes cross king county boundaires... need to find a better way to do this
# Begin fix
# install sf to pull in King County shapefile to spatially locate points within
# King County - Need to put in County Shapefile into S3
# ==========================================================================

    ### install help: https://stackoverflow.com/questions/42287164/install-udunits2-package-for-r3-3
    library(sf)
    library(tidycensus)
    library(tidyverse)
    options(tigris_use_chache = TRUE) # to cache shapefiles

      racevars <- c(White = "B03002_003",
                  Black = "B03002_004",
                  Asian = "B03002_005",
                Hispanic = "B03002_012")

      king <- get_acs(geography = "county", variables = racevars, survey = "acs5", state = "WA", county = "King County", geometry = TRUE, summary_var = "B03002_001")

pha_longitudinal_kc <- pha_longitudinal %>%
  mutate(unit_zip_new = as.numeric(unit_zip_new)) %>%
  filter(unit_zip_new %in% zips)
  # left_join(., zips, by = c("unit_zip_new" = "zip"))
rm(zips)


#### Save point ####
save(pha_longitudinal_kc, file = "~/data/Housing/OrganizedData/pha_longitudinal_kc.Rdata")
save(pha_longitudinal, file = "~/data/Housing/OrganizedData/pha_longitudinal.Rdata")

write.csv(pha_longitudinal_kc, "~/data/Housing/OrganizedData/pha_longitudinal_kc.csv")
write.csv(pha_longitudinal, "~/data/Housing/OrganizedData/pha_longitudinal.csv")


### Clean up remaining data frames
rm(pha_cleanadd_sort_dedup)
gc()
