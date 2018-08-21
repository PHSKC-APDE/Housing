###############################################################################
# OVERVIEW:
# Code to examine Yesler Terrace and Scattered sites data (housing and health)
#
# STEPS:
# 01 - Set up YT parameters in combined PHA/Medicaid data ### (THIS CODE) ###
# 02 - Conduct demographic analyses and produce visualizations
# 03 - Analyze movement patterns and geographic elements (optional)
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
library(tidyverse) # Used to manipulate data

housing_path <- "//phdata01/DROF_DATA/DOH DATA/Housing"

#### Bring in combined PHA/Medicaid data with some demographics already run ####
pha_mcaid_final <- readRDS(file = paste0(housing_path, 
                                         "/OrganizedData/pha_mcaid_final.Rda"))


#### Set up key variables ####
### Yesler Terrace and scattered sites indicators
yt_mcaid_final <- yt_flag(pha_mcaid_final, unit = pid2)

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


yt_mcaid_final <- yt_mcaid_final %>%
  arrange(pid2, startdate_c, enddate_c) %>%
  mutate(
    # First ID the place for that row
    place = case_when(
      is.na(agency_new) | agency_new == "Non-PHA" ~ "U",
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
yt_mcaid_final <- yt_mcaid_final %>%
  mutate_at(
    vars(age12, age13, age14, age15, age16, age17),
    funs(grp = case_when(
      . < 18 ~ "<18",
      between(., 18, 24.99) ~ "18-24",
      between(., 25, 44.99) ~ "25-44",
      between(., 45, 61.99) ~ "45-61",
      between(., 62, 64.99) ~ "62-64",
      . >= 65 ~ "65+",
      is.na(.) ~ "Unknown"
    )
    )
  )


### Time in housing
# Make groups of time in housing
yt_mcaid_final <- yt_mcaid_final %>%
  mutate_at(
    vars(length12, length13, length14, length15, length16, length17),
    funs(grp = case_when(
      . < 3 ~ "<3 years",
      between(., 3, 5.99) ~ "3-<6 years",
      . >= 6 ~ "6+ years",
      is.na(.) ~ "Unknown"
    )
    )
  )


### Household income
# Add in latest income for each calendar year
# Kludgy workaround for now, look upstream to better track annual income
# Also slow function, look to optimize
hh_inc_f <- function(df, year) {
  pt <- rlang::sym(paste0("pt", quo_name(year)))
  hh_inc_yr <- rlang::sym(paste0("hh_inc_", quo_name(year)))

  df_inc <- df %>%
    filter((!!pt) > 0) %>%
    arrange(pid2, desc(startdate_c)) %>%
    group_by(pid2) %>%
    mutate((!!hh_inc_yr) := first(hh_inc)) %>%
    ungroup() %>%
    arrange(pid2, startdate_c) %>%
    select(pid2, startdate_c, (!!hh_inc_yr))
  
  df <- left_join(df, df_inc, by = c("pid2", "startdate_c"))
  
  return(df)
}

yt_mcaid_final <- hh_inc_f(yt_mcaid_final, 12)
yt_mcaid_final <- hh_inc_f(yt_mcaid_final, 13)
yt_mcaid_final <- hh_inc_f(yt_mcaid_final, 14)
yt_mcaid_final <- hh_inc_f(yt_mcaid_final, 15)
yt_mcaid_final <- hh_inc_f(yt_mcaid_final, 16)
yt_mcaid_final <- hh_inc_f(yt_mcaid_final, 17)


### Set up income per capita
yt_mcaid_final <- yt_mcaid_final %>%
  group_by(hh_id_new_h, startdate_h) %>%
  mutate(hh_size_new = n_distinct(pid2)) %>%
  ungroup() %>%
  mutate_at(vars(starts_with("hh_inc_1")), funs(cap = . / hh_size_new))



### Save point
saveRDS(yt_mcaid_final, file = paste0(housing_path, 
                                      "/OrganizedData/SHA cleaning/yt_mcaid_final.Rds"))

#### Write to SQL for joining with claims ####
dbRemoveTable(db.apde51, name = "housing_mcaid_yt")
system.time(dbWriteTable(db.apde51, name = "housing_mcaid_yt", 
                         value = as.data.frame(yt_mcaid_final), overwrite = T,
                         field.types = c(
                           startdate_h = "date", enddate_h = "date", 
                           startdate_m = "date", enddate_m = "date", 
                           startdate_o = "date", enddate_o = "date", 
                           startdate_c = "date", enddate_c = "date",
                           dob_h = "date", dob_m = "date", dob_c = "date",
                           hh_dob_h = "date",
                           move_in_date = 'date', start_housing = "date", 
                           start_pha = "date", start_prog = "date"))
)


rm(pha_mcaid_final)
rm(hh_inc_f)
gc()
