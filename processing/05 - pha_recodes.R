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
# Recode race and other demographics ### (THIS CODE) ###
# Clean up addresses and geocode
# Consolidate data rows
# Add in final data elements and set up analyses
# Join with Medicaid eligibility data and set up analyses
#
# Alastair Matheson (PHSKC-APDE)
# alastair.matheson@kingcounty.gov
# 2016-05-13, split into separate files 2017-10
# 
###############################################################################


#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, scipen = 999)
housing_path <- "//phdata01/DROF_DATA/DOH DATA/Housing"

library(tidyverse) # Used to manipulate data


#### Bring in data ####
pha_clean <- readRDS(file = paste0(housing_path, "/OrganizedData/pha_matched.Rda"))


#### Race ####
# Recode race variables and make numeric
# Note: Because of typos and other errors, this process will overestimate the number of people with multiple races
pha_recoded <- pha_clean %>%
  mutate_at(vars(r_white:r_nhpi), 
            funs(new = car::recode(., "'Y' = 1; 'N' = 0; 'NULL' = NA; else = NA", 
                                   as.numeric.result = TRUE, as.factor.result = FALSE
                                   ))
            ) %>%
  # Make r_hisp new for now, need to check recode eventually
  mutate(r_hisp_new = ifelse(r_hisp == 2 & !is.na(r_hisp), 0, r_hisp),
         # Propogate collapsed race code from SHA HCV data
         r_white_new = ifelse(race == 1 & !is.na(race), 1, r_white_new),
         r_black_new = ifelse(race == 2 & !is.na(race), 1, r_black_new),
         r_aian_new = ifelse(race == 3 & !is.na(race), 1, r_aian_new),
         r_asian_new = ifelse(race == 4 & !is.na(race), 1, r_asian_new),
         r_nhpi_new = ifelse(race == 5 & !is.na(race), 1, r_nhpi_new)
         )


# Identify individuals with contradictory race values and set to Y
pha_recoded <- pha_recoded %>%
  group_by(ssn_id_m6, lname_new_m6, fname_new_m6, dob_m6) %>%
  mutate_at(vars(r_white_new:r_hisp_new), funs(tot = sum(., na.rm = TRUE))) %>%
  ungroup() %>%
  mutate_at(vars(r_white_new_tot:r_hisp_new_tot), funs(replace(., which(. > 0), 1))) %>%
  mutate(r_white_new = ifelse(r_white_new_tot == 1, 1, 0),
         r_black_new = ifelse(r_black_new_tot == 1, 1, 0),
         r_aian_new = ifelse(r_aian_new_tot == 1, 1, 0),
         r_asian_new = ifelse(r_asian_new_tot == 1, 1, 0),
         r_nhpi_new = ifelse(r_nhpi_new_tot == 1, 1, 0),
         r_hisp_new = ifelse(r_hisp_new_tot == 1, 1, 0),
         # Find people with multiple races
         r_multi_new = rowSums(cbind(r_white_new_tot, r_black_new_tot, r_aian_new_tot, r_asian_new_tot,
                                     r_nhpi_new_tot), na.rm = TRUE),
         r_multi_new = ifelse(r_multi_new > 1, 1, 0)) %>%
  # make new variable to look at people with one race only
  mutate_at(vars(r_white_new:r_nhpi_new), funs(alone = ifelse(r_multi_new == 1, 0, .))) %>%
  # make single race variable
  mutate(race2 = case_when(
    r_white_new_alone == 1 ~ "White only",
    r_black_new_alone == 1 ~ "Black only",
    r_aian_new_alone == 1 ~ "AIAN only",
    r_asian_new_alone == 1 ~ "Asian only",
    r_nhpi_new_alone == 1 ~ "NHPI only",
    r_multi_new == 1 ~ "Multiple race"
  ))


#### Add other recodes later ####

#### Save point ####
saveRDS(pha_recoded, file = paste0(housing_path, "/OrganizedData/pha_recoded.Rda"))

#### Clean up ####
rm(pha_clean)
gc()

