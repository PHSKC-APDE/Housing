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
library(dplyr) # Used to manipulate data

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
    place = case_when(
      is.na(agency_new) ~ "U",
      agency_new == "KCHA" & !is.na(agency_new) ~ "K",
      agency_new == "SHA" & !is.na(agency_new) & yt == 0 & ss == 0 ~ "O",
      agency_new == "SHA" & !is.na(agency_new) & yt_old == 1 ~ "Y",
      agency_new == "SHA" & !is.na(agency_new) & yt_new == 1 ~ "N",
      agency_new == "SHA" & !is.na(agency_new) & yt == 0 & ss == 1 ~ "S"
    ),
    start_type = case_when(
      pid2 != lag(pid2, 1) | is.na(lag(pid2, 1)) ~ paste0("U", place),
      pid2 == lag(pid2, 1) & !is.na(lag(pid2, 1)) ~ paste0(lag(place, 1), place)
      ),
    end_type = case_when(
      pid2 != lead(pid2, 1) | is.na(lead(pid2, 1)) ~ paste0(place, "U"),
      pid2 == lead(pid2, 1) & !is.na(lead(pid2, 1)) ~ paste0(place, lead(place, 1))
    )
  )


### Age
# Make groups of ages
yt_elig_final <- yt_elig_final %>%
  mutate_at(
    vars(age12, age13, age14, age15, age16, age17),
    funs(grp = case_when(
      . < 18 ~ "<18",
      between(., 18, 24.99) ~ "18–24",
      between(., 25, 44.99) ~ "25–44",
      between(., 45, 61.99) ~ "45–61",
      between(., 62, 64.99) ~ "62–64",
      . >= 65 ~ "65+",
      is.na(.) ~ "Unknown"
    )
    )
  )


### Time in housing
# Make groups of time in housing
yt_elig_final <- yt_elig_final %>%
  mutate_at(
    vars(length12, length13, length14, length15, length16),
    funs(grp = case_when(
      . < 3 ~ "<3 years",
      between(., 3, 5.99) ~ "3–<6 years",
      . >= 6 ~ "6+ years",
      is.na(.) ~ "Unknown"
    )
    )
  )


### Household income
# Add in latest income for each calendar year



### Save point
saveRDS(yt_elig_final, file = paste0(housing_path, "/OrganizedData/SHA cleaning/yt_elig_final.Rds"))
#yt_elig_final <- readRDS(file = paste0(housing_path, "/OrganizedData/SHA cleaning/yt_elig_final.Rds"))



#### Output to Excel for use in Tableau ####
# temp <- yt_elig_final
# # Strip out identifying variables
# temp <- temp %>% select(pid, gender_new_m6, race2, adult, senior, disability, DUAL_ELIG, COVERAGE_TYPE_IND, agency_new, 
#                         startdate_h:enddate_o, startdate_c:enroll_type, yt:age16) %>%
#   arrange(pid, startdate_c, enddate_c)
# 
# 
# write.xlsx(temp, file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/Summaries/yt_elig_demogs.xlsx")
