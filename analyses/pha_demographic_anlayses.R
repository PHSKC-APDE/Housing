###############################################################################
# Code to join examine demographics of public housing authority residents
#
# Alastair Matheson (PHSKC-APDE)
# alastair.matheson@kingcounty.gov
# 2016-12-31, updated 2017-10
#
# NOTE THAT THIS CODE IS A WORK IN PROGRESS
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


housing_path <- "//phdata01/DROF_DATA/DOH DATA/Housing"

#### Bring in data ####
pha_elig_final <- readRDS(file = paste0(housing_path, "/OrganizedData/pha_elig_final.Rda"))
# Or just bring in PHA data without Medicaid matching
# pha_longitudinal <- readRDS(file = paste0(housing_path, "/OrganizedData/pha_longitudinal.Rda"))


#### Set up variables for analysis ####

# Set up presence/absence in housing, Medicaid, and both at December 31 each year
pha_elig_demogs <- pha_elig_demogs %>%
  mutate(
    # Enrolled in housing
    dec12_h = ifelse(startdate_c <= as.Date("2012-12-31", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2012-12-31", origin = "1970-01-01") & enroll_type %in% c("b", "h"), 1, 0),
    dec13_h = ifelse(startdate_c <= as.Date("2013-12-31", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2013-12-31", origin = "1970-01-01") & enroll_type %in% c("b", "h"), 1, 0),
    dec14_h = ifelse(startdate_c <= as.Date("2014-12-31", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2014-12-31", origin = "1970-01-01") & enroll_type %in% c("b", "h"), 1, 0),
    dec15_h = ifelse(startdate_c <= as.Date("2015-12-31", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2015-12-31", origin = "1970-01-01") & enroll_type %in% c("b", "h"), 1, 0),
    dec16_h = ifelse(startdate_c <= as.Date("2016-12-31", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2016-12-31", origin = "1970-01-01") & enroll_type %in% c("b", "h"), 1, 0),
    # Enrolled in Medicaid (will be NA if never enrolled in Medicaid)
    dec12_m = ifelse(startdate_c <= as.Date("2012-12-31", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2012-12-31", origin = "1970-01-01") & enroll_type %in% c("b", "m"), 1, 0),
    dec13_m = ifelse(startdate_c <= as.Date("2013-12-31", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2013-12-31", origin = "1970-01-01") & enroll_type %in% c("b", "m"), 1, 0),
    dec14_m = ifelse(startdate_c <= as.Date("2014-12-31", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2014-12-31", origin = "1970-01-01") & enroll_type %in% c("b", "m"), 1, 0),
    dec15_m = ifelse(startdate_c <= as.Date("2015-12-31", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2015-12-31", origin = "1970-01-01") & enroll_type %in% c("b", "m"), 1, 0),
    dec16_m = ifelse(startdate_c <= as.Date("2016-12-31", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2016-12-31", origin = "1970-01-01") & enroll_type %in% c("b", "m"), 1, 0)
  )



# Set up presence/absence in housing for the entire year (to allow comparisons with KCHA numbers)
pha_elig_demogs <- pha_elig_demogs %>%
  mutate(
    # Enrolled in housing at all in that year
    any12_h = ifelse(startdate_c <= as.Date("2012-12-31", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2012-01-01", origin = "1970-01-01") & enroll_type %in% c("b", "h"), 1, 0),
    any13_h = ifelse(startdate_c <= as.Date("2013-12-31", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2013-01-01", origin = "1970-01-01") & enroll_type %in% c("b", "h"), 1, 0),
    any14_h = ifelse(startdate_c <= as.Date("2014-12-31", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2014-01-01", origin = "1970-01-01") & enroll_type %in% c("b", "h"), 1, 0),
    any15_h = ifelse(startdate_c <= as.Date("2015-12-31", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2015-01-01", origin = "1970-01-01") & enroll_type %in% c("b", "h"), 1, 0),
    any16_h = ifelse(startdate_c <= as.Date("2016-12-31", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2016-01-01", origin = "1970-01-01") & enroll_type %in% c("b", "h"), 1, 0)
  )


# Set up person-time each year
# First set up intervals for each year
i2012 <- interval(start = "2012-01-01", end = "2012-12-31")
i2013 <- interval(start = "2013-01-01", end = "2013-12-31")
i2014 <- interval(start = "2014-01-01", end = "2014-12-31")
i2015 <- interval(start = "2015-01-01", end = "2015-12-31")
i2016 <- interval(start = "2016-01-01", end = "2016-12-31")




#### Save point ####
#saveRDS(pha_elig_demogs, file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/pha_elig_demogs.Rda")
#pha_elig_demogs <- readRDS(file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/pha_elig_demogs.Rda")



#### Optional: Output to Excel for use in Tableau ####
# # Warning: makes huge file and takes a long time
# temp <- pha_elig_demogs %>%
#   # Strip out identifying variables
#   select(pid, gender_new_m6, race_h, disability, adult, senior, agency_new, major_prog:property_id, unit_zip_h, startdate_h:age16)
# 
# write.xlsx(temp, file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/Summaries/pha_elig_demogs_2017-08-09.xlsx")

#### End of optional section ####






### Make files for Tableau export
# Note: Counts are highers for each PHA if run separately
# This is because the first row is taken for a given period, which forces selection of just one PHA when the person may have switched during that period

### Overall
# Dual eligibility
dual_agency_kcha <- counts(pha_elig_final, group_var = c("agency_new", "major_prog", "enroll_type", "dual_elig_m"), 
                        period = "month", agency = "kcha", unit = pid2, filter = quo(port_out_kcha == 0))
dual_race_kcha <- counts(pha_elig_final, group_var = c("agency_new", "race_h", "enroll_type", "dual_elig_m"), 
                         period = "month", agency = "kcha", unit = pid2, filter = quo(port_out_kcha == 0))
dual_age_kcha <- counts(pha_elig_final, group_var = c("agency_new", "agegrp", "enroll_type", "dual_elig_m"), 
                        period = "month", agency = "kcha", unit = pid2, filter = quo(port_out_kcha == 0))
dual_disability_kcha <- counts(pha_elig_final, group_var = c("agency_new", "disability2_h", "enroll_type", "dual_elig_m"), 
                        period = "month", agency = "kcha", unit = pid2, filter = quo(port_out_kcha == 0))
dual_gender_kcha <- counts(pha_elig_final, group_var = c("agency_new", "gender2_h", "enroll_type", "dual_elig_m"), 
                           period = "month", agency = "kcha", unit = pid2, filter = quo(port_out_kcha == 0))


dual_agency_sha <- counts(pha_elig_final, group_var = c("agency_new", "major_prog", "enroll_type", "dual_elig_m"), 
                           period = "month", agency = "sha", unit = pid2, filter = quo(port_out_sha == 0))
dual_race_sha <- counts(pha_elig_final, group_var = c("agency_new", "race_h", "enroll_type", "dual_elig_m"), 
                        period = "month", agency = "sha", unit = pid2, filter = quo(port_out_sha == 0))
dual_age_sha <- counts(pha_elig_final, group_var = c("agency_new", "agegrp", "enroll_type", "dual_elig_m"), 
                        period = "month", agency = "sha", unit = pid2, filter = quo(port_out_sha == 0))
dual_disability_sha <- counts(pha_elig_final, group_var = c("agency_new", "disability2_h", "enroll_type", "dual_elig_m"), 
                       period = "month", agency = "sha", unit = pid2, filter = quo(port_out_sha == 0))
dual_gender_sha <- counts(pha_elig_final, group_var = c("agency_new", "gender2_h", "enroll_type", "dual_elig_m"), 
                           period = "month", agency = "sha", unit = pid2, filter = quo(port_out_sha == 0))


dual_agency <- bind_rows(dual_agency_kcha, dual_agency_sha) %>% mutate(category = "Agency", group = major_prog)
dual_race <- bind_rows(dual_race_kcha, dual_race_sha) %>% mutate(category = "Race", group = race_h)
dual_agegrp <- bind_rows(dual_age_kcha, dual_age_sha) %>% mutate(category = "Age group", group = agegrp)
dual_disability <- bind_rows(dual_disability_kcha, dual_disability_sha) %>% mutate(category = "Disability", group = disability2_h)
dual_gender <- bind_rows(dual_gender_kcha, dual_gender_sha) %>% mutate(category = "Gender", group = gender2_h)

dual_pha <- bind_rows(dual_agency, dual_race, dual_agegrp, dual_disability, dual_gender) %>%
  mutate(medicaid = car::recode(enroll_type, "'b' = 'Medicaid'; 'h' = 'No Medicaid'"),
         unit = car::recode(unit, "'pid2' = 'Individuals'"),
         source = "dual")

write.xlsx(dual_pha, file = paste0("//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/Summaries/PHA enrollment count_", 
                                    Sys.Date(), "_dual elig.xlsx"))
write.table(dual_pha, file = paste0("//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/Summaries/PHA enrollment count_", 
                                    Sys.Date(), "_dual elig.csv"))


rm(list = ls(pattern = "^dual_[:alnum:]_kcha"))
rm(list = ls(pattern = "^dual_[:alnum:]_sha"))
rm(dual_agency)
rm(dual_race)
rm(dual_agegrp)
rm(dual_disability)
rm(dual_gender)
gc()


### KCHA
# Monthly
agency_count_hh_kcha <- counts(pha_elig_final, group_var = c("agency_new", "major_prog", "enroll_type"), period = "month", agency = "kcha", unit = hhold_id_new, filter = quo(port_out_kcha == 0))
program_count_hh_kcha <- counts(pha_elig_final, group_var = c("agency_new", "prog_final", "enroll_type"), period = "month", agency = "kcha", unit = hhold_id_new,  filter = quo(port_out_kcha == 0))
portfolio_count_hh_kcha <- counts(pha_elig_final, group_var = c("agency_new", "portfolio_final", "enroll_type"), period = "month", agency = "kcha", unit = hhold_id_new, filter = quo(port_out_kcha == 0))
race_count_hh_kcha <- counts(pha_elig_final, group_var = c("agency_new", "race_h", "enroll_type"), period = "month", agency = "kcha", unit = hhold_id_new, filter = quo(port_out_kcha == 0))
agegrp_count_hh_kcha <- counts(pha_elig_final, group_var = c("agency_new", "agegrp", "enroll_type"), period = "month", agency = "kcha", unit = hhold_id_new, filter = quo(port_out_kcha == 0))
disability_count_hh_kcha <- counts(pha_elig_final, group_var = c("agency_new", "disability2_h", "enroll_type"), period = "month", agency = "kcha", unit = hhold_id_new, filter = quo(port_out_kcha == 0))
gender_count_hh_kcha <- counts(pha_elig_final, group_var = c("agency_new", "gender2_h", "enroll_type"), period = "month", agency = "kcha", unit = hhold_id_new, filter = quo(port_out_kcha == 0))
zip_count_hh_kcha <- counts(pha_elig_final, group_var = c("agency_new", "unit_zip_h", "kc_area", "enroll_type"), period = "month", agency = "kcha", unit = hhold_id_new, filter = quo(port_out_kcha == 0))
los_count_hh_kcha <- counts(pha_elig_final, group_var = c("agency_new", "time_pha", "enroll_type"), period = "month", agency = "kcha", unit = hhold_id_new, filter = quo(port_out_kcha == 0))

agency_count_ind_kcha <- counts(pha_elig_final, group_var = c("agency_new", "major_prog", "enroll_type"), period = "month", agency = "kcha", unit = pid2, filter = quo(port_out_kcha == 0))
program_count_ind_kcha <- counts(pha_elig_final, group_var = c("agency_new", "prog_final", "enroll_type"), period = "month", agency = "kcha", unit = pid2, filter = quo(port_out_kcha == 0))
portfolio_count_ind_kcha <- counts(pha_elig_final, group_var = c("agency_new", "portfolio_final", "enroll_type"), period = "month", agency = "kcha", unit = pid2, filter = quo(port_out_kcha == 0))
race_count_ind_kcha <- counts(pha_elig_final, group_var = c("agency_new", "race_h", "enroll_type"), period = "month", agency = "kcha", unit = pid2, filter = quo(port_out_kcha == 0))
agegrp_count_ind_kcha <- counts(pha_elig_final, group_var = c("agency_new", "agegrp", "enroll_type"), period = "month", agency = "kcha", unit = pid2, filter = quo(port_out_kcha == 0))
disability_count_ind_kcha <- counts(pha_elig_final, group_var = c("agency_new", "disability2_h", "enroll_type"), period = "month", agency = "kcha", unit = pid2, filter = quo(port_out_kcha == 0))
gender_count_ind_kcha <- counts(pha_elig_final, group_var = c("agency_new", "gender2_h", "enroll_type"), period = "month", agency = "kcha", unit = pid2, filter = quo(port_out_kcha == 0))
zip_count_ind_kcha <- counts(pha_elig_final, group_var = c("agency_new", "unit_zip_h", "kc_area", "enroll_type"), period = "month", agency = "kcha", unit = pid2, filter = quo(port_out_kcha == 0))
los_count_ind_kcha <- counts(pha_elig_final, group_var = c("agency_new", "time_pha", "enroll_type"), period = "month", agency = "kcha", unit = pid2, filter = quo(port_out_kcha == 0))


# Yearly
agency_count_yr_hh_kcha <- counts(pha_elig_final, group_var = c("agency_new", "major_prog", "enroll_type"), period = "year", agency = "kcha", unit = hhold_id_new, filter = quo(port_out_kcha == 0))
program_count_yr_hh_kcha <- counts(pha_elig_final, group_var = c("agency_new", "prog_final", "enroll_type"), period = "year", agency = "kcha", unit = hhold_id_new, filter = quo(port_out_kcha == 0))
portfolio_count_yr_hh_kcha <- counts(pha_elig_final, group_var = c("agency_new", "portfolio_final", "enroll_type"), period = "year", agency = "kcha", unit = hhold_id_new, filter = quo(port_out_kcha == 0))
race_count_yr_hh_kcha <- counts(pha_elig_final, group_var = c("agency_new", "race_h", "enroll_type"), period = "year", agency = "kcha", unit = hhold_id_new, filter = quo(port_out_kcha == 0))
agegrp_count_yr_hh_kcha <- counts(pha_elig_final, group_var = c("agency_new", "agegrp", "enroll_type"), period = "year", agency = "kcha", unit = hhold_id_new, filter = quo(port_out_kcha == 0))
disability_count_yr_hh_kcha <- counts(pha_elig_final, group_var = c("agency_new", "disability2_h", "enroll_type"), period = "year", agency = "kcha", unit = hhold_id_new, filter = quo(port_out_kcha == 0))
gender_count_yr_hh_kcha <- counts(pha_elig_final, group_var = c("agency_new", "gender2_h", "enroll_type"), period = "year", agency = "kcha", unit = hhold_id_new, filter = quo(port_out_kcha == 0))
zip_count_yr_hh_kcha <- counts(pha_elig_final, group_var = c("agency_new", "unit_zip_h", "kc_area", "enroll_type"), period = "year", agency = "kcha", unit = hhold_id_new, filter = quo(port_out_kcha == 0))
los_count_yr_hh_kcha <- counts(pha_elig_final, group_var = c("agency_new", "time_pha", "enroll_type"), period = "year", agency = "kcha", unit = hhold_id_new, filter = quo(port_out_kcha == 0))

agency_count_yr_ind_kcha <- counts(pha_elig_final, group_var = c("agency_new", "major_prog", "enroll_type"), period = "year", agency = "kcha", unit = pid2, filter = quo(port_out_kcha == 0))
program_count_yr_ind_kcha <- counts(pha_elig_final, group_var = c("agency_new", "prog_final", "enroll_type"), period = "year", agency = "kcha", unit = pid2, filter = quo(port_out_kcha == 0))
portfolio_count_yr_ind_kcha <- counts(pha_elig_final, group_var = c("agency_new", "portfolio_final", "enroll_type"), period = "year", agency = "kcha", unit = pid2, filter = quo(port_out_kcha == 0))
race_count_yr_ind_kcha <- counts(pha_elig_final, group_var = c("agency_new", "race_h", "enroll_type"), period = "year", agency = "kcha", unit = pid2, filter = quo(port_out_kcha == 0))
agegrp_count_yr_ind_kcha <- counts2(pha_elig_final, group_var = c("agency_new", "agegrp", "enroll_type"), period = "year", agency = "kcha", unit = pid2, filter = quo(port_out_kcha == 0))
disability_count_yr_ind_kcha <- counts(pha_elig_final, group_var = c("agency_new", "disability2_h", "enroll_type"), period = "year", agency = "kcha", unit = pid2, filter = quo(port_out_kcha == 0))
gender_count_yr_ind_kcha <- counts(pha_elig_final, group_var = c("agency_new", "gender2_h", "enroll_type"), period = "year", agency = "kcha", unit = pid2, filter = quo(port_out_kcha == 0))
zip_count_yr_ind_kcha <- counts(pha_elig_final, group_var = c("agency_new", "unit_zip_h", "kc_area", "enroll_type"), period = "year", agency = "kcha", unit = pid2, filter = quo(port_out_kcha == 0))
los_count_yr_ind_kcha <- counts2(pha_elig_final, group_var = c("agency_new", "time_pha", "enroll_type"), period = "year", agency = "kcha", unit = pid2, filter = quo(port_out_kcha == 0))

### SHA
agency_count_hh_sha <- counts(pha_elig_final, group_var = c("agency_new", "major_prog", "enroll_type"), period = "month", agency = "sha", unit = hhold_id_new, filter = quo(port_out_sha == 0))
program_count_hh_sha <- counts(pha_elig_final, group_var = c("agency_new", "prog_final", "enroll_type"), period = "month", agency = "sha", unit = hhold_id_new, filter = quo(port_out_sha == 0))
portfolio_count_hh_sha <- counts(pha_elig_final, group_var = c("agency_new", "portfolio_final", "enroll_type"), period = "month", agency = "sha", unit = hhold_id_new, filter = quo(port_out_sha == 0))
race_count_hh_sha <- counts(pha_elig_final, group_var = c("agency_new", "race_h", "enroll_type"), period = "month", agency = "sha", unit = hhold_id_new, filter = quo(port_out_sha == 0))
agegrp_count_hh_sha <- counts2(pha_elig_final, group_var = c("agency_new", "agegrp", "enroll_type"), period = "month", agency = "sha", unit = hhold_id_new, filter = quo(port_out_sha == 0))
disability_count_hh_sha <- counts(pha_elig_final, group_var = c("agency_new", "disability2_h", "enroll_type"), period = "month", agency = "sha", unit = hhold_id_new, filter = quo(port_out_sha == 0))
gender_count_hh_sha <- counts(pha_elig_final, group_var = c("agency_new", "gender2_h", "enroll_type"), period = "month", agency = "sha", unit = hhold_id_new, filter = quo(port_out_sha == 0))
zip_count_hh_sha <- counts(pha_elig_final, group_var = c("agency_new", "unit_zip_h", "kc_area", "enroll_type"), period = "month", agency = "sha", unit = hhold_id_new, filter = quo(port_out_sha == 0))
los_count_hh_sha <- counts2(pha_elig_final, group_var = c("agency_new", "time_pha", "enroll_type"), period = "month", agency = "sha", unit = hhold_id_new, filter = quo(port_out_sha == 0))

agency_count_ind_sha <- counts(pha_elig_final, group_var = c("agency_new", "major_prog", "enroll_type"), period = "month", agency = "sha", unit = pid2, filter = quo(port_out_sha == 0))
program_count_ind_sha <- counts(pha_elig_final, group_var = c("agency_new", "prog_final", "enroll_type"), period = "month", agency = "sha", unit = pid2, filter = quo(port_out_sha == 0))
portfolio_count_ind_sha <- counts(pha_elig_final, group_var = c("agency_new", "portfolio_final", "enroll_type"), period = "month", agency = "sha", unit = pid2, filter = quo(port_out_sha == 0))
race_count_ind_sha <- counts(pha_elig_final, group_var = c("agency_new", "race_h", "enroll_type"), period = "month", agency = "sha", unit = pid2, filter = quo(port_out_sha == 0))
agegrp_count_ind_sha <- counts2(pha_elig_final, group_var = c("agency_new", "agegrp", "enroll_type"), period = "month", agency = "sha", unit = pid2, filter = quo(port_out_sha == 0))
disability_count_ind_sha <- counts(pha_elig_final, group_var = c("agency_new", "disability2_h", "enroll_type"), period = "month", agency = "sha", unit = pid2, filter = quo(port_out_sha == 0))
gender_count_ind_sha <- counts(pha_elig_final, group_var = c("agency_new", "gender2_h", "enroll_type"), period = "month", agency = "sha", unit = pid2, filter = quo(port_out_sha == 0))
zip_count_ind_sha <- counts(pha_elig_final, group_var = c("agency_new", "unit_zip_h", "kc_area", "enroll_type"), period = "month", agency = "sha", unit = pid2, filter = quo(port_out_sha == 0))
los_count_ind_sha <- counts2(pha_elig_final, group_var = c("agency_new", "time_pha", "enroll_type"), period = "month", agency = "sha", unit = pid2, filter = quo(port_out_sha == 0))

# Yearly
agency_count_yr_hh_sha <- counts(pha_elig_final, group_var = c("agency_new", "major_prog", "enroll_type"), period = "year", agency = "sha", unit = hhold_id_new, filter = quo(port_out_sha == 0))
program_count_yr_hh_sha <- counts(pha_elig_final, group_var = c("agency_new", "prog_final", "enroll_type"), period = "year", agency = "sha", unit = hhold_id_new, filter = quo(port_out_sha == 0))
portfolio_count_yr_hh_sha <- counts(pha_elig_final, group_var = c("agency_new", "portfolio_final", "enroll_type"), period = "year", agency = "sha", unit = hhold_id_new, filter = quo(port_out_sha == 0))
race_count_yr_hh_sha <- counts(pha_elig_final, group_var = c("agency_new", "race_h", "enroll_type"), period = "year", agency = "sha", unit = hhold_id_new, filter = quo(port_out_sha == 0))
agegrp_count_yr_hh_sha <- counts2(pha_elig_final, group_var = c("agency_new", "agegrp", "enroll_type"), period = "year", agency = "sha", unit = hhold_id_new, filter = quo(port_out_sha == 0))
disability_count_yr_hh_sha <- counts(pha_elig_final, group_var = c("agency_new", "disability2_h", "enroll_type"), period = "year", agency = "sha", unit = hhold_id_new, filter = quo(port_out_sha == 0))
gender_count_yr_hh_sha <- counts(pha_elig_final, group_var = c("agency_new", "gender2_h", "enroll_type"), period = "year", agency = "sha", unit = hhold_id_new, filter = quo(port_out_sha == 0))
zip_count_yr_hh_sha <- counts(pha_elig_final, group_var = c("agency_new", "unit_zip_h", "kc_area", "enroll_type"), period = "year", agency = "sha", unit = hhold_id_new, filter = quo(port_out_sha == 0))
los_count_yr_hh_sha <- counts2(pha_elig_final, group_var = c("agency_new", "time_pha", "enroll_type"), period = "year", agency = "sha", unit = hhold_id_new, filter = quo(port_out_sha == 0))

agency_count_yr_ind_sha <- counts(pha_elig_final, group_var = c("agency_new", "major_prog", "enroll_type"), period = "year", agency = "sha", unit = pid2, filter = quo(port_out_sha == 0))
program_count_yr_ind_sha <- counts(pha_elig_final, group_var = c("agency_new", "prog_final", "enroll_type"), period = "year", agency = "sha", unit = pid2, filter = quo(port_out_sha == 0))
portfolio_count_yr_ind_sha <- counts(pha_elig_final, group_var = c("agency_new", "portfolio_final", "enroll_type"), period = "year", agency = "sha", unit = pid2, filter = quo(port_out_sha == 0))
race_count_yr_ind_sha <- counts(pha_elig_final, group_var = c("agency_new", "race_h", "enroll_type"), period = "year", agency = "sha", unit = pid2, filter = quo(port_out_sha == 0))
agegrp_count_yr_ind_sha <- counts2(pha_elig_final, group_var = c("agency_new", "agegrp", "enroll_type"), period = "year", agency = "sha", unit = pid2, filter = quo(port_out_sha == 0))
disability_count_yr_ind_sha <- counts(pha_elig_final, group_var = c("agency_new", "disability2_h", "enroll_type"), period = "year", agency = "sha", unit = pid2, filter = quo(port_out_sha == 0))
gender_count_yr_ind_sha <- counts(pha_elig_final, group_var = c("agency_new", "gender2_h", "enroll_type"), period = "year", agency = "sha", unit = pid2, filter = quo(port_out_sha == 0))
zip_count_yr_ind_sha <- counts(pha_elig_final, group_var = c("agency_new", "unit_zip_h", "kc_area", "enroll_type"), period = "year", agency = "sha", unit = pid2, filter = quo(port_out_sha == 0))
los_count_yr_ind_sha <- counts2(pha_elig_final, group_var = c("agency_new", "time_pha", "enroll_type"), period = "year", agency = "sha", unit = pid2, filter = quo(port_out_sha == 0))


# Combine files
agency_count <- bind_rows(agency_count_hh_kcha, agency_count_ind_kcha, agency_count_yr_hh_kcha, agency_count_yr_ind_kcha,
                          agency_count_hh_sha, agency_count_ind_sha, agency_count_yr_hh_sha, agency_count_yr_ind_sha) %>%
  mutate(category = "Agency", group = major_prog)
program_count <- bind_rows(program_count_hh_kcha, program_count_ind_kcha, program_count_yr_hh_kcha, program_count_yr_ind_kcha,
                           program_count_hh_sha, program_count_ind_sha, program_count_yr_hh_sha, program_count_yr_ind_sha) %>%
  mutate(category = "Program", group = prog_final)
portfolio_count <- bind_rows(portfolio_count_hh_kcha, portfolio_count_ind_kcha, portfolio_count_yr_hh_kcha, portfolio_count_yr_ind_kcha,
                             portfolio_count_hh_sha, portfolio_count_ind_sha, portfolio_count_yr_hh_sha, portfolio_count_yr_ind_sha) %>% 
  mutate(category = "Portfolio", group = portfolio_final)
race_count <- bind_rows(race_count_hh_kcha, race_count_ind_kcha, race_count_yr_hh_kcha, race_count_yr_ind_kcha,
                        race_count_hh_sha, race_count_ind_sha, race_count_yr_hh_sha, race_count_yr_ind_sha) %>% 
  mutate(category = "Race", group = race_h)
agegrp_count <- bind_rows(agegrp_count_hh_kcha, agegrp_count_ind_kcha, agegrp_count_yr_hh_kcha, agegrp_count_yr_ind_kcha,
                          agegrp_count_hh_sha, agegrp_count_ind_sha, agegrp_count_yr_hh_sha, agegrp_count_yr_ind_sha) %>% 
  mutate(category = "Age group", group = agegrp)
disability_count <- bind_rows(disability_count_hh_kcha, disability_count_ind_kcha, disability_count_yr_hh_kcha, disability_count_yr_ind_kcha,
                              disability_count_hh_sha, disability_count_ind_sha, disability_count_yr_hh_sha, disability_count_yr_ind_sha) %>% 
  mutate(category = "Disability", group = disability2_h)
gender_count <- bind_rows(gender_count_hh_kcha, gender_count_ind_kcha, gender_count_yr_hh_kcha, gender_count_yr_ind_kcha,
                          gender_count_hh_sha, gender_count_ind_sha, gender_count_yr_hh_sha, gender_count_yr_ind_sha) %>% 
  mutate(category = "Gender", group = gender2_h)
zip_count <- bind_rows(zip_count_hh_kcha, zip_count_ind_kcha, zip_count_yr_hh_kcha, zip_count_yr_ind_kcha,
                       zip_count_hh_sha, zip_count_ind_sha, zip_count_yr_hh_sha, zip_count_yr_ind_sha) %>% 
  mutate(category = "Zip", group = as.character(unit_zip_h))
los_count <- bind_rows(los_count_hh_kcha, los_count_ind_kcha, los_count_yr_hh_kcha, los_count_yr_ind_kcha,
                       los_count_hh_sha, los_count_ind_sha, los_count_yr_hh_sha, los_count_yr_ind_sha) %>% 
  mutate(category = "Length of stay", group = time_pha)


pha_count <- bind_rows(agency_count, program_count, portfolio_count, race_count, agegrp_count, disability_count, gender_count, zip_count, los_count) %>%
  mutate(medicaid = car::recode(enroll_type, "'b' = 'Medicaid'; 'h' = 'No Medicaid'"),
         unit = car::recode(unit, "'hhold_id_new' = 'Households'; 'pid2' = 'Individuals'"),
         date_yr = ifelse(period == "year", year(date), NA))

write.xlsx(pha_count, file = paste0("//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/Summaries/PHA enrollment count_", 
                                    Sys.Date(), ".xlsx"), sheetName = "enrollment")


rm(list = ls(pattern = "^agency"))
rm(list = ls(pattern = "^program"))
rm(list = ls(pattern = "^portfolio"))
rm(list = ls(pattern = "^race"))
rm(list = ls(pattern = "^age"))
rm(list = ls(pattern = "^disability"))
rm(list = ls(pattern = "^gender"))
rm(list = ls(pattern = "^zip"))
rm(list = ls(pattern = "^los"))
gc()


#### ANALYSES OF NON-MATCHED PHA DATA ####
# Quick checks of enrollment counts without having to link with the Medicaid data
### Date cut offs (both PHAs)
counts(pha_longitudinal, group_var = c("agency_new", "port_in"), period = "date", date = "-06-30", agency = "kcha", unit = hhold_id_new)
counts(pha_longitudinal, group_var = c("agency_new", "port_in"), period = "date", date = "-06-30", agency = "kcha", unit = pid)
counts(pha_longitudinal, group_var = c("agency_new", "port_in"), period = "date", date = "-12-31", agency = "kcha", unit = hhold_id_new)
counts(pha_longitudinal, group_var = c("agency_new", "port_in"), period = "date", date = "-12-31", agency = "kcha", unit = pid)

counts(pha_longitudinal, group_var = c("agency_new", "port_in"), period = "date", date = "-06-30", agency = "sha", unit = hhold_id_new)
counts(pha_longitudinal, group_var = c("agency_new", "port_in"), period = "date", date = "-06-30", agency = "sha", unit = pid)
counts(pha_longitudinal, group_var = c("agency_new", "port_in"), period = "date", date = "-12-31", agency = "sha", unit = hhold_id_new)
counts(pha_longitudinal, group_var = c("agency_new", "port_in"), period = "date", date = "-12-31", agency = "sha", unit = pid)


### KCHA
# Monthly
agency_count_hh_kcha <- counts(pha_longitudinal, group_var = c("agency_new", "port_in"), period = "month", agency = "kcha", unit = hhold_id_new)
agency_count_ind_kcha <- counts(pha_longitudinal, group_var = c("agency_new", "port_in"), period = "month", agency = "kcha", unit = pid)

# Yearly
agency_count_yr_hh_kcha <- counts(pha_longitudinal, group_var = c("agency_new", "port_in"), period = "year", agency = "kcha", unit = hhold_id_new)
agency_count_yr_ind_kcha <- counts(pha_longitudinal, group_var = c("agency_new", "port_in"), period = "year", agency = "kcha", unit = pid)


### SHA
# Monthly
agency_count_hh_sha <- counts(pha_longitudinal, group_var = c("agency_new", "port_in"), period = "month", agency = "sha", unit = hhold_id_new)
agency_count_ind_sha <- counts(pha_longitudinal, group_var = c("agency_new", "port_in"), period = "month", agency = "sha", unit = pid)

# Yearly
agency_count_yr_hh_sha <- counts(pha_longitudinal, group_var = c("agency_new", "port_in"), period = "year", agency = "sha", unit = hhold_id_new)
agency_count_yr_ind_sha <- counts(pha_longitudinal, group_var = c("agency_new", "port_in"), period = "year", agency = "sha", unit = pid)




# Combine files
agency_count <- bind_rows(agency_count_hh_kcha, agency_count_ind_kcha, agency_count_yr_hh_kcha, agency_count_yr_ind_kcha,
                          agency_count_hh_sha, agency_count_ind_sha, agency_count_yr_hh_sha, agency_count_yr_ind_sha) %>%
  mutate(category = "Agency", group = major_prog)


pha_count <- bind_rows(agency_count) %>%
  mutate(unit = car::recode(unit, "'hhold_id_new' = 'Households'; 'pid' = 'Individuals'"),
         date_yr = ifelse(period == "year", year(date), NA))

write.xlsx(pha_count, file = paste0("//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/Summaries/PHA enrollment count - non-matched_", 
                                    Sys.Date(), ".xlsx"))

rm(list = ls(pattern = "^agency"))




#### Counts overall (older code) ####

# In housing as at Dec 31
pha_elig_demogs %>% filter(dec12_h == 1) %>% summarise(count = n_distinct(hhold_id_new))
pha_elig_demogs %>% filter(dec13_h == 1) %>% summarise(count = n_distinct(hhold_id_new))
pha_elig_demogs %>% filter(dec14_h == 1) %>% summarise(count = n_distinct(hhold_id_new))
pha_elig_demogs %>% filter(dec15_h == 1) %>% summarise(count = n_distinct(hhold_id_new))
pha_elig_demogs %>% filter(dec16_h == 1) %>% summarise(count = n_distinct(hhold_id_new))

pha_elig_demogs %>% filter(dec12_h == 1) %>% summarise(count = n_distinct(pid))
pha_elig_demogs %>% filter(dec13_h == 1) %>% summarise(count = n_distinct(pid))
pha_elig_demogs %>% filter(dec14_h == 1) %>% summarise(count = n_distinct(pid))
pha_elig_demogs %>% filter(dec15_h == 1) %>% summarise(count = n_distinct(pid))
pha_elig_demogs %>% filter(dec16_h == 1) %>% summarise(count = n_distinct(pid))

# Ever in housing that year
pha_elig_demogs %>% filter(any12_h == 1) %>% summarise(count = n_distinct(hhold_id_new))
pha_elig_demogs %>% filter(any13_h == 1) %>% summarise(count = n_distinct(hhold_id_new))
pha_elig_demogs %>% filter(any14_h == 1) %>% summarise(count = n_distinct(hhold_id_new))
pha_elig_demogs %>% filter(any15_h == 1) %>% summarise(count = n_distinct(hhold_id_new))
pha_elig_demogs %>% filter(any16_h == 1) %>% summarise(count = n_distinct(hhold_id_new))

pha_elig_demogs %>% filter(any12_h == 1) %>% summarise(count = n_distinct(pid))
pha_elig_demogs %>% filter(any13_h == 1) %>% summarise(count = n_distinct(pid))
pha_elig_demogs %>% filter(any14_h == 1) %>% summarise(count = n_distinct(pid))
pha_elig_demogs %>% filter(any15_h == 1) %>% summarise(count = n_distinct(pid))
pha_elig_demogs %>% filter(any16_h == 1) %>% summarise(count = n_distinct(pid))


#### Counts of households ####
# NB. Can't look at Mediciad numbers because different members within a household have different enrollment in Medicaid
# In housing as at Dec 31
pha_elig_demogs %>% filter(dec12_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(hhold_id_new))
pha_elig_demogs %>% filter(dec13_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(hhold_id_new))
pha_elig_demogs %>% filter(dec14_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(hhold_id_new))
pha_elig_demogs %>% filter(dec15_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(hhold_id_new))
pha_elig_demogs %>% filter(dec16_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(hhold_id_new))

# Ever in housing that year
pha_elig_demogs %>% filter(any12_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(hhold_id_new))
pha_elig_demogs %>% filter(any13_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(hhold_id_new))
pha_elig_demogs %>% filter(any14_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(hhold_id_new))
pha_elig_demogs %>% filter(any15_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(hhold_id_new))
pha_elig_demogs %>% filter(any16_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(hhold_id_new))


#### Counts of people ####
# In housing as at Dec 31
pha_elig_demogs %>% filter(dec12_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(pid))
pha_elig_demogs %>% filter(dec13_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(pid))
pha_elig_demogs %>% filter(dec14_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(pid))
pha_elig_demogs %>% filter(dec15_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(pid))
pha_elig_demogs %>% filter(dec16_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(pid))

# Ever in housing that year
pha_elig_demogs %>% filter(any12_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(pid))
pha_elig_demogs %>% filter(any13_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(pid))
pha_elig_demogs %>% filter(any14_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(pid))
pha_elig_demogs %>% filter(any15_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(pid))
pha_elig_demogs %>% filter(any16_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(pid))



### Enrolled in housing by program
pha_elig_demogs %>% filter(dec12_h == 1) %>% distinct(pid, .keep_all = TRUE) %>% group_by(agency_new, major_prog, prog_type_new, spec_purp_type, portfolio_new) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count))
pha_elig_demogs %>% filter(dec13_h == 1) %>% distinct(pid, .keep_all = TRUE) %>% group_by(agency_new, major_prog, prog_type_new, spec_purp_type, portfolio_new) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count))
pha_elig_demogs %>% filter(dec14_h == 1) %>% distinct(pid, .keep_all = TRUE) %>% group_by(agency_new, major_prog, prog_type_new, spec_purp_type, portfolio_new) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count))
pha_elig_demogs %>% filter(dec15_h == 1) %>% distinct(pid, .keep_all = TRUE) %>% group_by(agency_new, major_prog, prog_type_new, spec_purp_type, portfolio_new) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count))
pha_elig_demogs %>% filter(dec16_h == 1) %>% distinct(pid, .keep_all = TRUE) %>% group_by(agency_new, major_prog, prog_type_new, spec_purp_type, portfolio_new) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count))



### Enrolled in Medicaid
pha_elig_demogs %>% filter(dec12_m == 1) %>% distinct(pid) %>% summarise(count = n())
pha_elig_demogs %>% filter(dec13_m == 1) %>% distinct(pid) %>% summarise(count = n())
pha_elig_demogs %>% filter(dec14_m == 1) %>% distinct(pid) %>% summarise(count = n())
pha_elig_demogs %>% filter(dec15_m == 1) %>% distinct(pid) %>% summarise(count = n())
pha_elig_demogs %>% filter(dec16_m == 1) %>% distinct(pid) %>% summarise(count = n())


### Enrolled in both housing and Medicaid by housing
pha_elig_demogs %>% filter(dec12_h == 1 & dec12_m == 1) %>% distinct(pid, .keep_all = TRUE) %>% group_by(agency_new, major_prog, prog_type_new, spec_purp_type) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count))
pha_elig_demogs %>% filter(dec13_h == 1 & dec13_m == 1) %>% distinct(pid, .keep_all = TRUE) %>% group_by(agency_new, major_prog, prog_type_new, spec_purp_type) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count))
pha_elig_demogs %>% filter(dec14_h == 1 & dec14_m == 1) %>% distinct(pid, .keep_all = TRUE) %>% group_by(agency_new, major_prog, prog_type_new, spec_purp_type) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count))
pha_elig_demogs %>% filter(dec15_h == 1 & dec15_m == 1) %>% distinct(pid, .keep_all = TRUE) %>% group_by(agency_new, major_prog, prog_type_new, spec_purp_type) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count))
pha_elig_demogs %>% filter(dec16_h == 1 & dec16_m == 1) %>% distinct(pid, .keep_all = TRUE) %>% group_by(agency_new, major_prog, prog_type_new, spec_purp_type) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count))



#### SHA ONLY ####
### As at Dec 31
# Households
temp12_sha <- pha_elig_demogs %>% filter(dec12_h == 1 & agency_new == "SHA") %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2012)
temp13_sha <- pha_elig_demogs %>% filter(dec13_h == 1 & agency_new == "SHA") %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2013)
temp14_sha <- pha_elig_demogs %>% filter(dec14_h == 1 & agency_new == "SHA") %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2014)
temp15_sha <- pha_elig_demogs %>% filter(dec15_h == 1 & agency_new == "SHA") %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2015)
temp16_sha <- pha_elig_demogs %>% filter(dec16_h == 1 & agency_new == "SHA") %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2016)

sha_count_hh <- bind_rows(temp12_sha, temp13_sha, temp14_sha, temp15_sha, temp16_sha)
sha_count_hh <- mutate(sha_count_hh, count_type = "Households")

# Individuals
temp12_sha <- pha_elig_demogs %>% filter(dec12_h == 1 & agency_new == "SHA") %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2012)
temp13_sha <- pha_elig_demogs %>% filter(dec13_h == 1 & agency_new == "SHA") %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2013)
temp14_sha <- pha_elig_demogs %>% filter(dec14_h == 1 & agency_new == "SHA") %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2014)
temp15_sha <- pha_elig_demogs %>% filter(dec15_h == 1 & agency_new == "SHA") %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2015)
temp16_sha <- pha_elig_demogs %>% filter(dec16_h == 1 & agency_new == "SHA") %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2016)

sha_count_ind <- bind_rows(temp12_sha, temp13_sha, temp14_sha, temp15_sha, temp16_sha)
sha_count_ind <- mutate(sha_count_ind, count_type = "Individuals")

sha_count <- bind_rows(sha_count_hh, sha_count_ind) %>%
  select(count_type, year, major_prog:total)
write.xlsx(sha_count, file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/Summaries/SHA enrollment count_2017-08-09.xlsx")



#### KCHA ONLY ####
### As at Dec 31
# Households
temp12_kcha <- pha_elig_demogs %>% filter(dec12_h == 1 & agency_new == "KCHA" & mbr_num == 1) %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2012)
temp13_kcha <- pha_elig_demogs %>% filter(dec13_h == 1 & agency_new == "KCHA" & mbr_num == 1) %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2013)
temp14_kcha <- pha_elig_demogs %>% filter(dec14_h == 1 & agency_new == "KCHA" & mbr_num == 1) %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2014)
temp15_kcha <- pha_elig_demogs %>% filter(dec15_h == 1 & agency_new == "KCHA" & mbr_num == 1) %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2015)
temp16_kcha <- pha_elig_demogs %>% filter(dec16_h == 1 & agency_new == "KCHA" & mbr_num == 1) %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2016)

kcha_count_hh <- bind_rows(temp12_kcha, temp13_kcha, temp14_kcha, temp15_kcha, temp16_kcha)
kcha_count_hh <- mutate(kcha_count_hh, count_type = "Households")

# Individuals
temp12_kcha <- pha_elig_demogs %>% filter(dec12_h == 1 & agency_new == "KCHA") %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2012)
temp13_kcha <- pha_elig_demogs %>% filter(dec13_h == 1 & agency_new == "KCHA") %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2013)
temp14_kcha <- pha_elig_demogs %>% filter(dec14_h == 1 & agency_new == "KCHA") %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2014)
temp15_kcha <- pha_elig_demogs %>% filter(dec15_h == 1 & agency_new == "KCHA") %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2015)
temp16_kcha <- pha_elig_demogs %>% filter(dec16_h == 1 & agency_new == "KCHA") %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2016)

kcha_count_ind <- bind_rows(temp12_kcha, temp13_kcha, temp14_kcha, temp15_kcha, temp16_kcha)
kcha_count_ind <- mutate(kcha_count_ind, count_type = "Individuals")

kcha_count_dec <- bind_rows(kcha_count_hh, kcha_count_ind) %>%
  select(count_type, year, major_prog:total)
# write.xlsx(kcha_count_dec, file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/Summaries/kcha enrollment count_2017-08-09.xlsx",
#            sheetName = "dec_30")


### Any point during the year
# Households
temp12_kcha <- pha_elig_demogs %>% filter(any12_h == 1 & agency_new == "KCHA" & mbr_num == 1) %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2012)
temp13_kcha <- pha_elig_demogs %>% filter(any13_h == 1 & agency_new == "KCHA" & mbr_num == 1) %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2013)
temp14_kcha <- pha_elig_demogs %>% filter(any14_h == 1 & agency_new == "KCHA" & mbr_num == 1) %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2014)
temp15_kcha <- pha_elig_demogs %>% filter(any15_h == 1 & agency_new == "KCHA" & mbr_num == 1) %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2015)
temp16_kcha <- pha_elig_demogs %>% filter(any16_h == 1 & agency_new == "KCHA" & mbr_num == 1) %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2016)

kcha_count_fy_hh <- bind_rows(temp12_kcha, temp13_kcha, temp14_kcha, temp15_kcha, temp16_kcha)
kcha_count_fy_hh <- mutate(kcha_count_fy_hh, count_type = "Households")

# Individuals
temp12_kcha <- pha_elig_demogs %>% filter(any12_h == 1 & agency_new == "KCHA") %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2012)
temp13_kcha <- pha_elig_demogs %>% filter(any13_h == 1 & agency_new == "KCHA") %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2013)
temp14_kcha <- pha_elig_demogs %>% filter(any14_h == 1 & agency_new == "KCHA") %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2014)
temp15_kcha <- pha_elig_demogs %>% filter(any15_h == 1 & agency_new == "KCHA") %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2015)
temp16_kcha <- pha_elig_demogs %>% filter(any16_h == 1 & agency_new == "KCHA") %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2016)

kcha_count_fy_ind <- bind_rows(temp12_kcha, temp13_kcha, temp14_kcha, temp15_kcha, temp16_kcha)
kcha_count_fy_ind <- mutate(kcha_count_fy_ind, count_type = "Individuals")

kcha_count_fy <- bind_rows(kcha_count_fy_hh, kcha_count_fy_ind) %>%
  select(count_type, year, major_prog:total)


# Write combined file (Dec 31 and full year data)
kcha_count_list <- list("dec_31" = kcha_count_dec, "Full_year" = kcha_count_fy)
write.xlsx(kcha_count_list, 
           file = paste0("//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/Summaries/KCHA enrollment count_", Sys.Date(), ".xlsx"))



#### KCHA WITH VARIOUS PORT IN/OUT COMBINATIONS ####
### As at Dec 31
# Households
temp12_kcha <- pha_elig_demogs %>% filter(dec12_h == 1 & (agency_new == "KCHA" | port_out_kcha == 1) & mbr_num == 1) %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(port_in, port_out_kcha, agency_new, prog_final, portfolio_final) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2012)
temp13_kcha <- pha_elig_demogs %>% filter(dec13_h == 1 & (agency_new == "KCHA" | port_out_kcha == 1) & mbr_num == 1) %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(port_in, port_out_kcha, agency_new, prog_final, portfolio_final) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2013)
temp14_kcha <- pha_elig_demogs %>% filter(dec14_h == 1 & (agency_new == "KCHA" | port_out_kcha == 1) & mbr_num == 1) %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(port_in, port_out_kcha, agency_new, prog_final, portfolio_final) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2014)
temp15_kcha <- pha_elig_demogs %>% filter(dec15_h == 1 & (agency_new == "KCHA" | port_out_kcha == 1) & mbr_num == 1) %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(port_in, port_out_kcha, agency_new, prog_final, portfolio_final) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2015)
temp16_kcha <- pha_elig_demogs %>% filter(dec16_h == 1 & (agency_new == "KCHA" | port_out_kcha == 1) & mbr_num == 1) %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(port_in, port_out_kcha, agency_new, prog_final, portfolio_final) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2016)

kcha_count_hh <- bind_rows(temp12_kcha, temp13_kcha, temp14_kcha, temp15_kcha, temp16_kcha)
kcha_count_hh <- mutate(kcha_count_hh, count_type = "Households")

# Individuals
temp12_kcha <- pha_elig_demogs %>% filter(dec12_h == 1 & (agency_new == "KCHA" | port_out_kcha == 1)) %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(port_in, port_out_kcha, agency_new, prog_final, portfolio_final) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2012)
temp13_kcha <- pha_elig_demogs %>% filter(dec13_h == 1 & (agency_new == "KCHA" | port_out_kcha == 1)) %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(port_in, port_out_kcha, agency_new, prog_final, portfolio_final) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2013)
temp14_kcha <- pha_elig_demogs %>% filter(dec14_h == 1 & (agency_new == "KCHA" | port_out_kcha == 1)) %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(port_in, port_out_kcha, agency_new, prog_final, portfolio_final) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2014)
temp15_kcha <- pha_elig_demogs %>% filter(dec15_h == 1 & (agency_new == "KCHA" | port_out_kcha == 1)) %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(port_in, port_out_kcha, agency_new, prog_final, portfolio_final) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2015)
temp16_kcha <- pha_elig_demogs %>% filter(dec16_h == 1 & (agency_new == "KCHA" | port_out_kcha == 1)) %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(port_in, port_out_kcha, agency_new, prog_final, portfolio_final) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2016)

kcha_count_ind <- bind_rows(temp12_kcha, temp13_kcha, temp14_kcha, temp15_kcha, temp16_kcha)
kcha_count_ind <- mutate(kcha_count_ind, count_type = "Individuals")

kcha_count_dec <- bind_rows(kcha_count_hh, kcha_count_ind) %>%
  select(count_type, year, port_in, port_out_kcha, agency_new, prog_final, portfolio_final, count, total)


### Any point during the year
# Households
temp12_kcha <- pha_elig_demogs %>% filter(any12_h == 1 & (agency_new == "KCHA" | port_out_kcha == 1) & mbr_num == 1) %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(port_in, port_out_kcha, agency_new, prog_final, portfolio_final) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2012)
temp13_kcha <- pha_elig_demogs %>% filter(any13_h == 1 & (agency_new == "KCHA" | port_out_kcha == 1) & mbr_num == 1) %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(port_in, port_out_kcha, agency_new, prog_final, portfolio_final) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2013)
temp14_kcha <- pha_elig_demogs %>% filter(any14_h == 1 & (agency_new == "KCHA" | port_out_kcha == 1) & mbr_num == 1) %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(port_in, port_out_kcha, agency_new, prog_final, portfolio_final) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2014)
temp15_kcha <- pha_elig_demogs %>% filter(any15_h == 1 & (agency_new == "KCHA" | port_out_kcha == 1) & mbr_num == 1) %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(port_in, port_out_kcha, agency_new, prog_final, portfolio_final) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2015)
temp16_kcha <- pha_elig_demogs %>% filter(any16_h == 1 & (agency_new == "KCHA" | port_out_kcha == 1) & mbr_num == 1) %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(port_in, port_out_kcha, agency_new, prog_final, portfolio_final) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2016)

kcha_count_fy_hh <- bind_rows(temp12_kcha, temp13_kcha, temp14_kcha, temp15_kcha, temp16_kcha)
kcha_count_fy_hh <- mutate(kcha_count_fy_hh, count_type = "Households")

# Individuals
temp12_kcha <- pha_elig_demogs %>% filter(any12_h == 1 & (agency_new == "KCHA" | port_out_kcha == 1)) %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(port_in, port_out_kcha, agency_new, prog_final, portfolio_final) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2012)
temp13_kcha <- pha_elig_demogs %>% filter(any13_h == 1 & (agency_new == "KCHA" | port_out_kcha == 1)) %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(port_in, port_out_kcha, agency_new, prog_final, portfolio_final) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2013)
temp14_kcha <- pha_elig_demogs %>% filter(any14_h == 1 & (agency_new == "KCHA" | port_out_kcha == 1)) %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(port_in, port_out_kcha, agency_new, prog_final, portfolio_final) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2014)
temp15_kcha <- pha_elig_demogs %>% filter(any15_h == 1 & (agency_new == "KCHA" | port_out_kcha == 1)) %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(port_in, port_out_kcha, agency_new, prog_final, portfolio_final) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2015)
temp16_kcha <- pha_elig_demogs %>% filter(any16_h == 1 & (agency_new == "KCHA" | port_out_kcha == 1)) %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(port_in, port_out_kcha, agency_new, prog_final, portfolio_final) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2016)

kcha_count_fy_ind <- bind_rows(temp12_kcha, temp13_kcha, temp14_kcha, temp15_kcha, temp16_kcha)
kcha_count_fy_ind <- mutate(kcha_count_fy_ind, count_type = "Individuals")

kcha_count_fy <- bind_rows(kcha_count_fy_hh, kcha_count_fy_ind) %>%
  select(count_type, year, port_in, port_out_kcha, agency_new, prog_final, portfolio_final, count, total)


# Write combined file (Dec 31 and full year data)
kcha_count_list <- list("dec_31" = kcha_count_dec, "Full_year" = kcha_count_fy)
write.xlsx(kcha_count_list, 
           file = paste0("//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/Summaries/KCHA enrollment count_ports_", Sys.Date(), ".xlsx"))

rm(list = ls(pattern = "temp1[2-7]+_kcha"))
rm(list = ls(pattern = "kcha_count"))
gc()



#### Demographics (older code) ####



# Look at gender each year in housing
pha_elig_demogs %>%
  filter(dec12_h == 1) %>%
  distinct(pid, .keep_all = TRUE) %>%
  group_by(agency_new, prog_type_new, prog_subtype) %>%
  summarise(female = sum(gender_new_m6[gender_new_m6 == 1], na.rm = TRUE),
            male = sum(gender_new_m6[gender_new_m6 == 2] - 1, na.rm = TRUE),
            unknown = sum(is.na(gender_new_m6)),
            total = n()) %>%
  mutate(femaleper = female / total, maleper = male / total, unkper = unknown / total)

pha_elig_demogs %>%
  filter(dec13_h == 1) %>%
  distinct(pid, .keep_all = TRUE) %>%
  group_by(agency_new) %>%
  summarise(female = sum(gender_new_m6[gender_new_m6 == 1], na.rm = TRUE),
            male = sum(gender_new_m6[gender_new_m6 == 2] - 1, na.rm = TRUE),
            unknown = sum(is.na(gender_new_m6)),
            total = n()) %>%
  mutate(femaleper = female / total, maleper = male / total, unkper = unknown / total)

pha_elig_demogs %>%
  filter(dec14_h == 1) %>%
  distinct(pid, .keep_all = TRUE) %>%
  group_by(agency_new) %>%
  summarise(female = sum(gender_new_m6[gender_new_m6 == 1], na.rm = TRUE),
            male = sum(gender_new_m6[gender_new_m6 == 2] - 1, na.rm = TRUE),
            unknown = sum(is.na(gender_new_m6)),
            total = n()) %>%
  mutate(femaleper = female / total, maleper = male / total, unkper = unknown / total)

pha_demogs %>%
  filter(dec15_h == 1) %>%
  distinct(pid, .keep_all = TRUE) %>%
  group_by(agency_new) %>%
  summarise(female = sum(gender_new_m6[gender_new_m6 == 1], na.rm = TRUE),
            male = sum(gender_new_m6[gender_new_m6 == 2] - 1, na.rm = TRUE),
            unknown = sum(is.na(gender_new_m6)),
            total = n()) %>%
  mutate(femaleper = female / total, maleper = male / total, unkper = unknown / total)

pha_elig_demogs %>%
  filter(dec16_h == 1) %>%
  distinct(pid, .keep_all = TRUE) %>%
  group_by(agency_new) %>%
  summarise(female = sum(gender_new_m6[gender_new_m6 == 1], na.rm = TRUE),
            male = sum(gender_new_m6[gender_new_m6 == 2] - 1, na.rm = TRUE),
            unknown = sum(is.na(gender_new_m6)),
            total = n()) %>%
  mutate(femaleper = female / total, maleper = male / total, unkper = unknown / total)


### Look at age each year
temp <- pha_demogs %>%
  filter(dec12 == 1) %>%
  distinct(pid, .keep_all = TRUE)
stat.desc(temp$age12[temp$agency_new == "KCHA"], basic = F)
stat.desc(temp$age12[temp$agency_new == "SHA"], basic = F)

temp <- pha_demogs %>%
  filter(dec13 == 1) %>%
  distinct(pid, .keep_all = TRUE)
stat.desc(temp$age13[temp$agency_new == "KCHA"], basic = F)
stat.desc(temp$age13[temp$agency_new == "SHA"], basic = F)

temp <- pha_demogs %>%
  filter(dec14 == 1) %>%
  distinct(pid, .keep_all = TRUE)
stat.desc(temp$age14[temp$agency_new == "KCHA"], basic = F)
stat.desc(temp$age14[temp$agency_new == "SHA"], basic = F)

temp <- pha_demogs %>%
  filter(dec15 == 1) %>%
  distinct(pid, .keep_all = TRUE)
stat.desc(temp$age15[temp$agency_new == "KCHA"], basic = F)
stat.desc(temp$age15[temp$agency_new == "SHA"], basic = F)

temp <- pha_demogs %>%
  filter(dec16 == 1) %>%
  distinct(pid, .keep_all = TRUE)
stat.desc(temp$age16[temp$agency_new == "KCHA"], basic = F)
stat.desc(temp$age16[temp$agency_new == "SHA"], basic = F)



#### Look at moves between agencies ####
pha_demogs <- pha_demogs %>%
  mutate(
    move_pha = ifelse((pid == lag(pid, 1) | is.na(lag(pid, 1))) & agency_new != lag(agency_new, 1), 1, 0),
    move_to_sha = ifelse((pid == lag(pid, 1) | is.na(lag(pid, 1))) & agency_new != lag(agency_new, 1) &
                           agency_new == "SHA", 1, 0),
    move_to_kcha = ifelse((pid == lag(pid, 1) | is.na(lag(pid, 1))) & agency_new != lag(agency_new, 1) &
                            agency_new == "KCHA", 1, 0),
    move_to_yt = ifelse((pid == lag(pid, 1) | is.na(lag(pid, 1))) & property_id == "1" & !is.na(property_id) &
                          (lag(property_id, 1) != "1" | is.na(lag(property_id, 1))),
                        1, 0),
    move_from_yt = ifelse((pid == lead(pid, 1) | is.na(lead(pid, 1))) & property_id == "1" & !is.na(property_id) &
                            (lead(property_id, 1) != "1" | is.na(lead(property_id, 1))),
                          1, 0)
             )

table(pha_demogs$move_to_kcha[pha_demogs$startdate <= as.Date("2012-01-01", origin = "1970-01-01") & 
                                pha_demogs$startdate >= as.Date("2012-12-31", origin = "1970-01-01")], useNA = 'always')
table(pha_demogs$move_to_sha[pha_demogs$startdate <= as.Date("2012-01-01", origin = "1970-01-01") & 
                                pha_demogs$startdate >= as.Date("2012-12-31", origin = "1970-01-01")], useNA = 'always')

table(pha_demogs$move_to_kcha[pha_demogs$startdate <= as.Date("2013-01-01", origin = "1970-01-01") & 
                                pha_demogs$startdate >= as.Date("2013-12-31", origin = "1970-01-01")], useNA = 'always')
table(pha_demogs$move_to_sha[pha_demogs$startdate <= as.Date("2013-01-01", origin = "1970-01-01") & 
                               pha_demogs$startdate >= as.Date("2013-12-31", origin = "1970-01-01")], useNA = 'always')


