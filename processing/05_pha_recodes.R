###############################################################################
# OVERVIEW:
# Code to create a cleaned person table from the combined 
# King County Housing Authority and Seattle Housing Authority data sets
# Aim is to have a single row per contiguous time in a house per person
#
# STEPS:
# 01 - Process raw KCHA data and load to SQL database
# 02 - Process raw SHA data and load to SQL database
# 03 - Bring in individual PHA datasets and combine into a single file
# 04 - Deduplicate data and tidy up via matching process
# 05 - Recode race and other demographics ### (THIS CODE) ###
# 06 - Clean up addresses
# 06a - Geocode addresses
# 07 - Consolidate data rows
# 08 - Add in final data elements and set up analyses
# 09 - Join with Medicaid-Medicare eligibility & time varying data
# 10 - Set up joint housing/Medicaid analyses
#
# Alastair Matheson (PHSKC-APDE)
# alastair.matheson@kingcounty.gov
# 2016-05-13, split into separate files 2017-10
# 
###############################################################################


#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, scipen = 999)

library(tidyverse) # Used to manipulate data

script <- httr::content(httr::GET("https://raw.githubusercontent.com/PHSKC-APDE/Housing/main/processing/metadata/set_data_env.r"))
eval(parse(text = script))

housing_source_dir <- file.path(here::here(), "processing")
METADATA = RJSONIO::fromJSON(file.path(housing_source_dir, "metadata/metadata.json"))
set_data_envr(METADATA,"combined")

#### Bring in data ####
if (UW == TRUE) {
  "skip load of pha_clean"
} else {
pha_clean <- readRDS(file = file.path(housing_path, pha_clean_fn))
}

#### Race ####
# Recode race variables and make numeric
# Note: Because of typos and other errors, this process will overestimate 
# the number of people with multiple races
pha_recoded <- pha_clean %>%
    mutate_at(vars(r_white:r_nhpi),
              list(new = ~ as.numeric(case_when(. %in% c("Y", "1") ~ 1,
                                                . %in% c("N", "0") ~ 0,
                                                TRUE ~  NA_real_)))) %>%
  # Make r_hisp new for now, need to check recode eventually
  mutate(r_hisp_new = ifelse(r_hisp == 2 & !is.na(r_hisp), 0, r_hisp),
         # Propogate collapsed race code from SHA HCV data
         r_white_new = ifelse(race == 1 & !is.na(race), 1, r_white_new),
         r_black_new = ifelse(race == 2 & !is.na(race), 1, r_black_new),
         r_aian_new = ifelse(race == 3 & !is.na(race), 1, r_aian_new),
         r_asian_new = ifelse(race == 4 & !is.na(race), 1, r_asian_new),
         r_nhpi_new = ifelse(race == 5 & !is.na(race), 1, r_nhpi_new))


# Identify individuals with contradictory race values and set to Y
if (UW == TRUE) {
  pha_recoded <- pha_recoded %>%
    group_by(pid) %>%
    mutate_at(vars(r_white_new:r_hisp_new), funs(tot = sum(., na.rm = TRUE))) %>%
    ungroup() %>%
    mutate_at(vars(r_white_new_tot:r_hisp_new_tot), 
              funs(replace(., which(. > 0), 1))) %>%
    mutate(r_white_new = ifelse(r_white_new_tot == 1, 1, 0),
           r_black_new = ifelse(r_black_new_tot == 1, 1, 0),
           r_aian_new = ifelse(r_aian_new_tot == 1, 1, 0),
           r_asian_new = ifelse(r_asian_new_tot == 1, 1, 0),
           r_nhpi_new = ifelse(r_nhpi_new_tot == 1, 1, 0),
           r_hisp_new = ifelse(r_hisp_new_tot == 1, 1, 0),
           # Find people with multiple races
           r_multi_new = rowSums(cbind(r_white_new_tot, r_black_new_tot, 
                                       r_aian_new_tot, r_asian_new_tot,
                                       r_nhpi_new_tot), na.rm = TRUE),
           r_multi_new = ifelse(r_multi_new > 1, 1, 0)) %>%
    # make new variable to look at people with one race only
    mutate_at(vars(r_white_new:r_nhpi_new), 
              funs(alone = ifelse(r_multi_new == 1, 0, .))) %>%
    # make single race variable
    mutate(race_new = case_when(
      r_white_new_alone == 1 ~ "White only",
      r_black_new_alone == 1 ~ "Black only",
      r_aian_new_alone == 1 ~ "AIAN only",
      r_asian_new_alone == 1 ~ "Asian only",
      r_nhpi_new_alone == 1 ~ "NHPI only",
      r_multi_new == 1 ~ "Multiple race",
      TRUE ~ ""
    ))
} else {
pha_recoded <- pha_recoded %>%
  group_by(pid) %>%
  mutate_at(vars(r_white_new:r_hisp_new), list(tot = ~ sum(., na.rm = TRUE))) %>%
  ungroup() %>%
  mutate_at(vars(r_white_new_tot:r_hisp_new_tot), 
            list(~ replace(., which(. > 0), 1))) %>%
  mutate(r_white_new = ifelse(r_white_new_tot == 1, 1, 0),
         r_black_new = ifelse(r_black_new_tot == 1, 1, 0),
         r_aian_new = ifelse(r_aian_new_tot == 1, 1, 0),
         r_asian_new = ifelse(r_asian_new_tot == 1, 1, 0),
         r_nhpi_new = ifelse(r_nhpi_new_tot == 1, 1, 0),
         r_hisp_new = ifelse(r_hisp_new_tot == 1, 1, 0),
         # Find people with multiple races
         r_multi_new = rowSums(cbind(r_white_new_tot, r_black_new_tot, 
                                     r_aian_new_tot, r_asian_new_tot,
                                     r_nhpi_new_tot), na.rm = TRUE),
         r_multi_new = ifelse(r_multi_new > 1, 1, 0)) %>%
  # make new variable to look at people with one race only
  mutate_at(vars(r_white_new:r_nhpi_new), 
            list(alone = ~ ifelse(r_multi_new == 1, 0, .))) %>%
  # make single race variable
  mutate(race_new = case_when(r_white_new_alone == 1 ~ "White only",
                              r_black_new_alone == 1 ~ "Black only",
                              r_aian_new_alone == 1 ~ "AIAN only",
                              r_asian_new_alone == 1 ~ "Asian only",
                              r_nhpi_new_alone == 1 ~ "NHPI only",
                              r_multi_new == 1 ~ "Multiple race",
                              TRUE ~ "")) %>%
  # Drop earlier race variables
  select(-r_white, -r_black, -r_aian, -r_asian, -r_nhpi, 
         -race, -contains("_new_tot"), -contains("_alone"), -r_multi_new)
}

### Fill in missing gender information (won't work if all are missing, also
# will not fill in any initial NAs)
pha_recoded <- setDT(pha_recoded)
pha_recoded[, gender_new_m6 := fill(.SD), by = "pid", .SDcols = "gender_new_m6"]
pha_recoded[, gender_new_m6 := fill(.SD, .direction = "up"), by = "pid", .SDcols = "gender_new_m6"]
pha_recoded <- setDF(pha_recoded)



#### Add other recodes later ####
#### Save point ####
if (UW == TRUE) {
  "skip save point"
  rm(pha_clean)
  gc()
} else {
saveRDS(pha_recoded, file = file.path(housing_path, pha_recoded_fn))


#### Clean up ####
rm(pha_clean)
gc()
}
