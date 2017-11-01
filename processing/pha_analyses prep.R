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


#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, scipen = 999)
housing_path <- "//phdata01/DROF_DATA/DOH DATA/Housing"

library(openxlsx) # Used to import/export Excel files
library(stringr) # Used to manipulate string data
library(lubridate) # Used to manipulate dates
library(dplyr) # Used to manipulate data
library(tidyr) # More data manipulation


#### Bring in data ####
pha_cleanadd_sort_dedup <- readRDS(file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/pha_cleanadd_sort_dedup.Rda")



### Strip out some variables that no longer have meaning 
# (e.g., act_date, act_type, income data, household size (this will need to be remade))
pha_longitudinal <- pha_cleanadd_sort_dedup %>%
  select(-(hhold_inc_fixed:hhold_inc_vary), -(act_type:correction_date), -(reexam_date:agency), 
         -(portability:cost_pha), -(incasset_id:inc_fixed), 
         -(sha_source), -(r_white_new_tot:r_hisp_new_tot), -drop, -add_yr)

### Fix date format
pha_longitudinal <- pha_longitudinal %>% mutate(dob_m6 = as.Date(dob_m6, origin = "1970-01-01"))

### Fix up remaining program categories (will be moved earlier eventually)
# Bring in program name mapping table and add new variables
program_map <- read.xlsx("//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/Program name mapping.xlsx")
# Replace NAs with blanks so the join works correctly
program_map <- program_map %>% mutate(spec_purp_type = ifelse(is.na(spec_purp_type), "", spec_purp_type))

pha_longitudinal <- left_join(pha_longitudinal, program_map, by = c("agency_new", "major_prog", "prog_type", "prog_subtype",
                                                                    "spec_purp_type", "portfolio"))

pha_longitudinal <- pha_longitudinal %>%
  mutate(
    property_name_new = ifelse(agency_new == "SHA" & !is.na(property_name) & 
                                 str_detect(property_name, "HIGH"), "HIGH POINT",
                               ifelse(agency_new == "SHA" & !is.na(property_name) & 
                                        str_detect(property_name, "HOLLY"), "NEW HOLLY",
                                      ifelse(agency_new == "SHA" & !is.na(property_name) & 
                                               str_detect(property_name, "VISTA"), "RAINIER VISTA", property_name)))
  )

# Make final portfolio and program groups
pha_longitudinal <- pha_longitudinal %>%
  mutate(
    portfolio_final = ifelse(agency_new == "SHA" & major_prog == "PH", toupper(portfolio_group),
                             ifelse(agency_new == "KCHA", portfolio_new, NA)),
    prog_final = ifelse(agency_new == "SHA" & major_prog == "HCV", toupper(prog_group),
                        ifelse(agency_new == "SHA" & major_prog == "PH", "PH",
                               ifelse(agency_new == "KCHA", prog_type, NA)))
    
  )

rm(program_map)

### Set up time in housing for each row and note when there was a gap in coverage
pha_longitudinal <- pha_longitudinal %>% 
  mutate(
    cov_time = interval(start = startdate, end = enddate) / ddays(1) + 1,
    gap = ifelse(is.na(lag(pid, 1)) | pid != lag(pid, 1), 0,
                 ifelse(startdate - lag(enddate, 1) > 62, 1, NA))) %>%
  # Find the number of unique periods a person was in housing
  group_by(pid, gap) %>%
  mutate(period = row_number()*gap + 1) %>% 
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
zips <- read.xlsx("//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/ZIP filter for KC.xlsx")
pha_longitudinal <- pha_longitudinal %>%
  mutate(unit_zip_new = as.numeric(unit_zip_new)) %>%
  left_join(., zips, by = c("unit_zip_new" = "zip"))
rm(zips)



#### Save point ####
saveRDS(pha_longitudinal, file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/pha_longitudinal.Rda")


### Clean up remaining data frames
rm(pha_cleanadd_sort_dedup)
gc()
