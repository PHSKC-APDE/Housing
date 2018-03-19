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
library(colorout)
library(housing)
library(rgdal)
library(openxlsx) # Used to import/export Excel files
library(stringr) # Used to manipulate string data
library(lubridate) # Used to manipulate dates
library(tidyverse)

#### Bring in data ####
load(file = "~/data/Housing/OrganizedData/pha_cleanadd_sort_dedup.RData")

### Strip out some variables that no longer have meaning
# (e.g., act_date, act_type, income data, household size (this will need to be remade))
pha_longitudinal <- pha_cleanadd_sort_dedup %>%
  select(-(hhold_inc_fixed:hhold_inc_vary), -(act_type:correction_date), -(reexam_date:agency),
         -(portability:cost_pha), -inc_fixed, -(sha_source),
         -(r_white_new_tot:r_hisp_new_tot), -add_num, -drop, -next_hh_act, -add_yr)

glimpse(pha_longitudinal)
### Fix date format
pha_longitudinal <- pha_longitudinal %>% 
                        mutate(dob_m6 = as.Date(dob_m6, origin = "1970-01-01"))

### Fix up remaining program categories (will be moved earlier eventually)
# Bring in program name mapping table and add new variables
#program_map <- read.xlsx("//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/Program name mapping.xlsx")
# No longer needed. Still some KCHA PBS8 properties that could have their portfolio updated based on property name
# but numbers are small


### Set up time in housing for each row and note when there was a gap in coverage
pha_longitudinal <- pha_longitudinal %>%
                        mutate(cov_time = interval(
                                            start = startdate, 
                                            end = enddate) / ddays(1) + 1,
                               gap = ifelse(is.na(lag(pid, 1)) | 
                                            pid != lag(pid, 1), 0,
                                            ifelse(startdate - lag(enddate, 1)
                                                   > 62, 1, NA))
                                            ) %>%
                                # Find the number of unique periods a person 
                                # was in housing
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
  mutate(age12 = round(interval(start = dob_m6,
                                end = ymd(20121231)) / years(1), 1),
         age13 = round(interval(start = dob_m6, 
                       end = ymd(20131231)) / years(1), 1),
         age14 = round(interval(start = dob_m6, 
                       end = ymd(20141231)) / years(1), 1),
         age15 = round(interval(start = dob_m6, 
                       end = ymd(20151231)) / years(1), 1),
         age16 = round(interval(start = dob_m6, 
                      end = ymd(20161231)) / years(1), 1))

# Single age grouping
# Note: this is only valid for the person at the time of the start date
# Need to recalculate for each time period being analyzed
pha_longitudinal <- pha_longitudinal %>%
  mutate(agegrp = ifelse(adult == 0 & !is.na(adult), "Youth",
                         ifelse(adult == 1 & senior == 0 & !is.na(adult), "Working age",
                                ifelse(adult == 1 & senior == 1 & !is.na(adult), "Senior", NA))))

### Recode gender and disability
pha_longitudinal <- pha_longitudinal %>%
  mutate(gender2 = car::recode(gender_new_m6, 
                               "'1' = 'Female'; '2' = 'Male'; else = NA"),
         disability2 = car::recode(disability, 
                    "'1' = 'Disabled'; '0' = 'Not disabled'; else = NA"))

### Subset cases in King County
  # Note: the following zipcodes are from the US census and overlapping King County.
  # Some of the zipcodes fall outside of King County proper, but I'm choosing to use
  # this method as spatial subsets also leave some points outside, meaning a
  # somewhat unreliable geocoding process.

  zip <- c(98001,98002,98003,98004,98005,98006,98007,98008,98010,98011,98014,
           98019,98022,98023,98024,98027,98028,98029,98030,98031,98032,98033,
           98034,98038,98039,98040,98042,98045,98047,98050,98051,98052,98053,
           98055,98056,98057,98058,98059,98065,98068,98070,98072,98074,98075,
           98077,98092,98101,98102,98103,98104,98105,98106,98107,98108,98109,
           98112,98115,98116,98117,98118,98119,98121,98224,98122,98125,98126,
           98133,98134,98136,98144,98146,98148,98154,98288,98155,98158,98164,
           98166,98168,98174,98177,98178,98188,98195,98198,98199)

  pha_longitudinal_kc <- pha_longitudinal %>%
    filter(unit_zip_new %in% zip) #%>% filter(!is.na(X))

#### Save point ####
save(pha_longitudinal_kc, 
     file = "~/data/Housing/OrganizedData/pha_longitudinal_kc.RData")

save(pha_longitudinal, 
    file = "~/data/Housing/OrganizedData/pha_longitudinal.RData")

write.csv(pha_longitudinal_kc, 
          "~/data/Housing/OrganizedData/pha_longitudinal_kc.csv")

write.csv(pha_longitudinal, "~/data/Housing/OrganizedData/pha_longitudinal.csv")


### Clean up remaining data frames
rm(pha_cleanadd_sort_dedup)
gc()
