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
library(odbc) # Connect to SQL

housing_path <- "//phdata01/DROF_DATA/DOH DATA/Housing"
db_apde51 <- dbConnect(odbc(), "PH_APDEStore51")

#### Bring in combined PHA/Medicaid data with some demographics already run ####
# Currently using stage schema but eventually switch to final
pha_mcaid_final <- dbGetQuery(
  db_apde51,
  "SELECT pid2, hh_id_new_h, startdate_c, enddate_c, enroll_type, dual_elig_m, 
    full_benefit_m, id_mcaid, 
    dob_c, race_c, hisp_c, ethn_c, gender_c, lang_m, 
    agency_new, major_prog, prog_type, subsidy_type, operator_type, vouch_type_final, 
    unit_add_h, property_id, property_name, portfolio_final, zip_c, 
    start_housing, start_pha, hh_inc, 
    age12, age13, age14, age15, age16, age17, age18,
    length12, length13, length14, length15, length16, length17, length18, 
    pt12, pt13, pt14, pt15, pt16, pt17, pt18 
  FROM stage.mcaid_pha")

# Bring in timevar table to find people at YT or SS addresses
pha_timevar <- dbGetQuery(
  db_apde51,
  "SELECT id_apde, from_date, to_date, 
  pha_subsidy, pha_voucher, pha_operator, pha_portfolio, geo_add1, geo_city
  FROM final.mcaid_mcare_pha_elig_timevar
  WHERE pha = 1")

# Bring in property IDs etc
pha_property <- dbGetQuery(
  db_apde51,
  "SELECT DISTINCT unit_add_new, unit_city_new, property_id, property_name
  FROM stage.pha
  WHERE property_id IS NOT NULL OR property_name IS NOT NULL"
)

# Can use pre-calculated calendar year table and join to YT/SS IDs
pha_claims_calyear <- dbGetQuery(
  db_apde51,
  "SELECT year, id_apde, age_yr, gender_me, race_me, race_eth_me, 
  enroll_type, full_criteria, 
  pha_agency, pha_subsidy, pha_voucher, pha_operator, pha_portfolio, 
  geo_zip, pt
  FROM stage.mcaid_mcare_pha_elig_calyear
  WHERE pha = 1 AND pop_ever = 1")


#### JOIN DATA ####



#### Set up key variables ####



### Yesler Terrace and scattered sites indicators
yt_mcaid_final <- yt_flag(pha_mcaid_final, unit = pid2, prop_id = property_id,
                          prop_name = property_name, address = unit_add_h)

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
    vars(age12, age13, age14, age15, age16, age17, age18),
    list(grp = ~ case_when(
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
    vars(length12, length13, length14, length15, length16, length17, length18),
    funs(grp = case_when(
      . < 3 ~ "<3 years",
      between(., 3, 5.99) ~ "3-<6 years",
      . >= 6 ~ "6+ years",
      is.na(.) ~ "Unknown")
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
yt_mcaid_final <- hh_inc_f(yt_mcaid_final, 18)


### Set up income per capita
yt_mcaid_final <- yt_mcaid_final %>%
  group_by(hh_id_new_h, startdate_c) %>%
  mutate(hh_size_new = n_distinct(pid2)) %>%
  ungroup() %>%
  mutate_at(vars(starts_with("hh_inc_1")), list(cap = ~ . / hh_size_new))



### Save point
#### Write to SQL for joining with claims ####
dbRemoveTable(db_apde51, name = DBI::Id(schema = "stage", table = "mcaid_pha_yt"))
system.time(dbWriteTable(db_apde51, name = DBI::Id(schema = "stage", table = "mcaid_pha_yt"), 
                         value = as.data.frame(yt_mcaid_final), overwrite = T,
                         field.types = c(
                           startdate_c = "date", enddate_c = "date",
                           dob_c = "date", start_housing = "date", start_pha = "date"))
)


rm(pha_mcaid_final)
rm(hh_inc_f)
gc()
