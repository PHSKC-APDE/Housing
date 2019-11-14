###############################################################################
# Code to generate data for the housing/Medicaid dashboard
#
# Alastair Matheson (PHSKC-APDE)
# 2018-01-24
#
#
###############################################################################

##### Set up global parameter and call in libraries #####
options(max.print = 350, tibble.print_max = 30, scipen = 999)

library(odbc) # Used to connect to SQL server
library(openxlsx) # Used to import/export Excel files
library(housing) # contains many useful functions for analyzing housing/Medicaid data
library(lubridate) # Used to manipulate dates
library(tidyverse) # Used to manipulate data
library(data.table) # Used to manipulate data
library(medicaid) # Used to aggregate data


##### Connect to the SQL servers #####
db_apde51 <- dbConnect(odbc(), "PH_APDEStore51")
db_claims51 <- dbConnect(odbc(), "PHClaims51")
db_extractstore51 <- dbConnect(odbc(), "PHExtractStore51")
db_extractstore50 <- dbConnect(odbc(), "PHExtractStore50")

housing_path <- "//phdata01/DROF_DATA/DOH DATA/Housing/Organized_data"


#### BRING IN DATA ####
### Code for mapping field values
demo_codes <- read.csv(text = RCurl::getURL("https://raw.githubusercontent.com/PHSKC-APDE/Housing/pha_2018_data/processing/housing_mcaid%20demo%20codes.csv"), 
                   header = TRUE, stringsAsFactors = FALSE)

### Main merged data
mcaid_mcare_pha_elig_demo <- dbGetQuery(db_apde51, "SELECT * FROM final.mcaid_mcare_pha_elig_demo")
mcaid_mcare_pha_elig_timevar <- dbGetQuery(
  db_apde51, 
  "SELECT * FROM final.mcaid_mcare_pha_elig_timevar
  WHERE mcaid = 1 OR pha = 1 OR (mcare = 1 AND geo_kc = 1)")


### Fix up formats
mcaid_mcare_pha_elig_demo <- mcaid_mcare_pha_elig_demo %>%
  mutate_at(vars(dob, death_dt, start_housing), list( ~ as.Date(.)))

mcaid_mcare_pha_elig_timevar <- mcaid_mcare_pha_elig_timevar %>%
  mutate_at(vars(from_date, to_date), list( ~ as.Date(.)))


### Make enroll field
mcaid_mcare_pha_elig_timevar <- mcaid_mcare_pha_elig_timevar %>%
  mutate(enroll_type = case_when(
    mcaid == 0 & mcare == 0 & pha == 1 ~ "h",
    mcaid == 1 & mcare == 0 & pha == 1 ~ "hmd",
    mcaid == 0 & mcare == 1 & pha == 1 ~ "hme",
    mcaid == 1 & mcare == 0 & pha == 0 ~ "md",
    mcaid == 0 & mcare == 1 & pha == 0 ~ "me",
    mcaid == 1 & mcare == 1 & pha == 0 ~ "mm",
    mcaid == 1 & mcare == 1 & pha == 1 ~ "a"
  ))


#### RUN ALLOCATION FOR EACH CALENDAR YEAR ####
# Set up calendar years
years <- seq(2012, 2016)

allocated <- bind_rows(lapply(seq_along(years), function(x) {
  year <- allocate(df = mcaid_mcare_pha_elig_timevar, 
                   starttime = paste0(years[x], "-01-01"), 
                   endtime = paste0(years[x], "-12-31"), 
                   agency = pha_agency, enroll = enroll_type,
                   unit = id_apde,
                   from_date = from_date, to_date = to_date,
                   # Grouping vars
                   apde_dual, full_benefit, pha_subsidy, pha_voucher, 
                   pha_operator, pha_portfolio, geo_zip) %>%
    mutate(year = years[x]) %>%
    select(year, id_apde:pha, enroll_type, apde_dual, geo_kc, part_a:pha_portfolio, 
           geo_zip:geo_school_code, pt, pt_tot)
}))


#### MAKE PT AND POP_EVER FIELDS ####
# Want to keep a row for any combination of groups vars that appeared
pt_rows <- bind_rows(lapply(seq_along(years), function(x) {
  
  output <- mcaid_mcare_pha_elig_timevar %>%
    mutate(
      overlap_amount = as.numeric(lubridate::intersect(
        #time_int,
        lubridate::interval(from_date, to_date),
        lubridate::interval(as.Date(paste0(years[x], "-01-01")), 
                            as.Date(paste0(years[x], "-12-31")))) / ddays(1) + 1)
    ) %>%
    # Remove any rows that don't overlap
    filter(!is.na(overlap_amount)) %>%
    group_by(id_apde, enroll_type, pha_agency, 
             apde_dual, full_benefit, pha_subsidy, pha_voucher, 
             pha_operator, pha_portfolio, geo_zip) %>%
    summarise(pop_ever = n_distinct(id_apde),
              pt_days = sum(overlap_amount)) %>%
    ungroup() %>%
    mutate(year = years[x])

  
  return(output)
}))




#### JOIN TO ELIG_DEMO AND ADD CALCULATED FIELDS ####
mcaid_mcare_pha_elig_calyear <- left_join(pt_rows, 
                  allocated, by = c("year", "id_apde", "enroll_type", "pha_agency", 
                                    "apde_dual", "full_benefit", "pha_subsidy", 
                                    "pha_voucher", "pha_operator", "pha_portfolio", "geo_zip")) %>%
  left_join(., select(mcaid_mcare_pha_elig_demo, -apde_dual, -last_run), 
            by = "id_apde")


mcaid_mcare_pha_elig_calyear <- mcaid_mcare_pha_elig_calyear %>%
  mutate(age_yr = floor(interval(start = dob, end = paste0(year, "-12-31")) / years(1)),
         adult = case_when(age_yr >= 18 ~ 1L, age_yr < 18 ~ 0L),
         senior = case_when(age_yr >= 62 ~ 1L, age_yr < 62 ~ 0L),
         agegrp = case_when(
           age_yr < 18 ~ "<18",
           between(age_yr, 18, 24.99) ~ "18-24",
           between(age_yr, 25, 44.99) ~ "24-44",
           between(age_yr, 45, 61.99) ~ "45-61",
           between(age_yr, 62, 64.99) ~ "62-64",
           age_yr >= 65 ~ "65+",
           is.na(age_yr) ~ NA_character_),
         age_wc = case_when(between(age_yr, 0, 6.99) ~ "Children aged 0-6", 
                            TRUE ~ NA_character_),
         time_housing_yr = 
           round(interval(start = start_housing, end = paste0(year, "-12-31")) / years(1), 1),
         time_housing = case_when(
           is.na(pha_agency) ~ "Non-PHA",
           time_housing_yr < 3 ~ "<3 years",
           between(time_housing_yr, 3, 5.99) ~ "3 to <6 years",
           time_housing_yr >= 6 ~ "6+ years",
           TRUE ~ "Unknown"),
         last_run = Sys.time()
  )


### Rearrange fields
mcaid_mcare_pha_elig_calyear <- mcaid_mcare_pha_elig_calyear %>%
  select(year, id_apde, mcaid, mcare, pha, mcaid_mcare_pha, enroll_type, 
         pha_agency, apde_dual, part_a, part_b, part_c, partial, buy_in, tpl, 
         bsp_group_name, cov_type, mco_id, full_benefit,
         dob, death_dt, age_yr, adult, senior, agegrp, age_wc,
         gender_me:gender_male, race_me:race_unk, 
         start_housing, time_housing_yr, time_housing, 
         pha_subsidy, pha_voucher, pha_operator, pha_portfolio, geo_kc, geo_zip, 
         geo_zip_centroid, geo_street_centroid, geo_county_code, geo_tract_code,
         geo_hra_code, geo_school_code, pt_days, pt_tot, pop, pop_ever,
         last_run)


#### TEMP UNTIL ELIG TABLES ARE UPDATED ####
# Just go through to 2016 with joint data
mcaid_mcare_pha_elig_calyear <- mcaid_mcare_pha_elig_calyear %>% filter(year < 2017)


#### Find out why so many are missing addresses ####
mcaid_mcare_pha_elig_timevar %>% filter(pha == 1) %>% summarise(count = n()) # 620391
mcaid_mcare_pha_elig_timevar %>% filter(pha == 1 & is.na(geo_add1)) %>% summarise(count = n()) # 5292


#### WRITE DATA TO SQL SERVER ####
table_config_stage <- yaml::yaml.load_file("C:/Users/mathesal/OneDrive - King County/github/Housing/analyses/load.stage_mcaid_mcare_pha_elig_calyear.yaml")
source("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/add_index.R")


### Temp: remove index
dbGetQuery(db_apde51, 'DROP INDEX "idx_ccs_stage_mcaid_mcare_pha_elig_calyear" ON stage.mcaid_mcare_pha_elig_calyear')


DBI::dbWriteTable(db_apde51,
                  name = DBI::Id(schema = "stage", table = "mcaid_mcare_pha_elig_calyear"),
                  value = mcaid_mcare_pha_elig_calyear,
                  append = F, overwrite = T)


#### QA TABLE AND MOVE TO FINAL ####
# Move into new file?



#### ADD INDEX TO SQL ####
# For now, adding it to stage


add_index_f(db_apde51, table_config = table_config_stage, drop_index = T)


#### CLEAN UP ####
# Remove stage table
