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
db.apde51 <- dbConnect(odbc(), "PH_APDEStore51")
db.claims51 <- dbConnect(odbc(), "PHClaims51")

housing_path <- "//phdata01/DROF_DATA/DOH DATA/Housing"


#### FUNCTIONS ####
# Relabel function
relabel_f <- function(df) {
  
  # Turn this into a loop/apply at some point
  
  if ("agency" %in% names(df)) {
    df$agency <- demo_codes$agency_new[match(df$agency, demo_codes$code)]
  }
  if ("enroll_type" %in% names(df)) {
    df$enroll_type <- demo_codes$enroll_type[match(df$enroll_type, demo_codes$code)]
  }
  if ("dual" %in% names(df)) {
    df$dual <- demo_codes$dual_elig_m[match(df$dual, demo_codes$code)]
  }
  if ("age_group" %in% names(df)) {
    df$age_group <- demo_codes$agegrp[match(df$age_group, demo_codes$code)]
  }
  if ("gender" %in% names(df)) {
    df$gender <- demo_codes$gender_c[match(df$gender, demo_codes$code)]
  }
  if ("ethn" %in% names(df)) {
    df$ethn <- demo_codes$race_c[match(df$ethn, demo_codes$code)]
  }
  if ("voucher" %in% names(df)) {
    df$voucher <- demo_codes$voucher_type_final[match(df$voucher, demo_codes$code)]
  }
  if ("subsidy" %in% names(df)) {
    df$subsidy <- demo_codes$subsidy_type[match(df$subsidy, demo_codes$code)]
  }
  if ("operator" %in% names(df)) {
    df$operator <- demo_codes$operator_type[match(df$operator, demo_codes$code)]
  }
  if ("portfolio" %in% names(df)) {
    df$portfolio <- demo_codes$portfolio_final[match(df$portfolio, demo_codes$code)]
  }
  if ("length" %in% names(df)) {
    df$length <- demo_codes$length_grp[match(df$length, demo_codes$code)]
  }
  
  return(df)
}

### Population ###
# Function to assign a person to each group by year
# (used for allocating people with chronic conditions to groups)
chronic_pop_f <- function(df, year = 12, wc = F) {
  
  agex_quo <- rlang::sym(paste0("age", quo_name(year), "_num"))
  lengthx_quo <- rlang::sym(paste0("length", quo_name(year), "_num"))
  ptx_quo <- rlang::sym(paste0("pt", quo_name(year)))
  
  year_full <- as.numeric(paste0(20, year))
  
  
  if (wc == T) {
    df <- df %>%
      mutate(
        age_temp = floor(interval(start = dob_c, 
                                  end = as.Date(paste0(year_full, "-12-31"),
                                                origin = "1970-01-01"))
                         / years(1))
      ) %>%
      filter(!is.na(age_temp) & age_temp >= 0 & age_temp <= 6) %>%
      mutate(!!agex_quo := 8)
  }
  
  # Make new generic vars so data table works
  # Currently only using ptx, expand to others if more code converted
  df <- df %>% 
    mutate(ptx = !!ptx_quo,
           agex = !!agex_quo,
           lengthx = !!lengthx_quo)
  
  
  # Count a person if they were in that group at any point in the period
  # Also count person time accrued in each group (in days)
  pt <- df %>%
    filter(!is.na(ptx)) %>%
    group_by(agency_num, enroll_type_num, agex, gender_num, 
             ethn_num, dual_elig_num, voucher_num, subsidy_num, 
             operator_num, portfolio_num, lengthx, zip_c) %>%
    summarise(pt_days = sum(ptx)) %>%
    ungroup()
  
  print(sum(pt$pt_days))
  
  
  # Allocate an individual to a PHA/program based on rules:
  # 1) Medicaid only and PHA only = Medicaid row with most time
  #   (rationale is we can look at the health data for Medicaid portion at least)
  # 2) Medicaid only and PHA/Medicaid = PHA group with most person-time where
  #    person was enrolled in both housing and Medicaid
  # 3) Multiple PHAs = PHA group with most person-time for EACH PHA where
  #    person was enrolled in both housing and Medicaid
  # 4) PHA only = group with most person-time (for one or more PHAs)
  # Note that this only allocates individuals, not person-time, which should
  # be allocated to each group in which it is accrued
  
  # Join back to a single df
  df_pop <- left_join(df, pt, by = c("agency_num", "enroll_type_num", "agex", "gender_num", 
                                       "ethn_num", "dual_elig_num", "voucher_num", "subsidy_num", 
                                       "operator_num", "portfolio_num", "lengthx", "zip_c")) %>%
    arrange(pid2, agency_num, enroll_type_num, desc(pt_days))
  
  # Find the row with the most person-time in each group
  # (ties will be broken by whatever other ordering exists)
  pop <- setDT(df_pop)
  pop <- pop[!is.na(ptx)]
  # pop <- df_pop[ptx > 0 & !is.na(ptx)]
  pop <- pop[pop[, .I[1], by = .(pid2, agency_num, enroll_type_num)]$V1]
  # pop <- pop[order(pid2, agency_num, enroll_type_num, -ptx)]
  # pop[pop[, .I[1], by = .(pid2, agency_num, enroll_type_num)]$V1]
  
  # Number of agencies, should only be one row per possibility below
  pop <- pop %>%
    mutate(agency_count = case_when(
      agency_num == 1 & enroll_type_num == 1 ~ 0,
      agency_num == 2 & enroll_type_num == 1 ~ 0,
      agency_num == 1 & enroll_type_num == 3 ~ 1,
      agency_num == 2 & enroll_type_num == 3 ~ 2,
      agency_num == 0 ~ 4
    ))
  
  # Switch to data table for faster grouped operation then back to DF
  pop <- setDT(pop)
  pop[, agency_sum := sum(agency_count, na.rm = T), by = pid2]
  pop <- as.data.frame(pop)
  
  # Filter so only rows meeting the rules above are kept
  pop <- pop %>%
    filter((agency_sum == 4 & agency_count == 4) | 
             (agency_sum == 5 & agency_count == 1) |
             (agency_sum == 6 & agency_count == 2) |
             (agency_sum == 7 & agency_count == 1) | 
             (agency_sum == 7 & agency_count == 2) |
             (agency_sum == 1 & agency_count == 1) |
             (agency_sum == 2 & agency_count == 2) |
             agency_sum == 3 |
             agency_sum == 0) %>%
    select(pid2, mid, agency_num, enroll_type_num, dual_elig_num, agex, 
           gender_num, ethn_num, voucher_num, subsidy_num, operator_num, 
           portfolio_num, lengthx, zip_c) %>%
    rename(age_group = agex, length = lengthx) %>%
    mutate(year = as.numeric(paste0("20", year)))
  
  
  if (wc == T) {
    pop <- pop %>% mutate(wc_flag = 1)
  } else {
    pop <- pop %>% mutate(wc_flag = 0)
  }
  
  return(pop)
}


### Function to summarize acute events across all demographics
# Possibly add ability to differentiate by injury intention (e.g., unintentional)
eventcount_acute_f <- function(df, event = NULL, number = TRUE, 
                               person = FALSE, year) {
  
  
  event_quo <- enquo(event)
  year_full <- as.numeric(paste0(20, year))
  
  # pha_mcaid_demo df has incorrect age values due to round instead of floor
  # in the code. Need to recreate age here
  
  df <- df %>%
    mutate(age_temp = floor(interval(start = dob_c,
                                     end = as.Date(paste0(year_full, "-12-31"),
                                                   origin = "1970-01-01"))
                            / years(1)),
           age_temp_num = case_when(
             age_temp < 18 ~ 1,
             between(age_temp, 18, 24.99) ~ 2,
             between(age_temp, 25, 44.99) ~ 3,
             between(age_temp, 45, 61.99) ~ 4,
             between(age_temp, 62, 64.99) ~ 5,
             age_temp >= 65 ~ 6,
             is.na(age_temp) ~ 99
           ),
           age_temp_char = case_when(
             age_temp < 18 ~ "0-17",
             between(age_temp, 18, 24.99) ~ "18-24",
             between(age_temp, 25, 44.99) ~ "25-44",
             between(age_temp, 45, 61.99) ~ "45-61",
             between(age_temp, 62, 64.99) ~ "62-64",
             age_temp >= 65 ~ "65+",
             is.na(age_temp) ~ "Unknown"
           )
    )

  
  if (number == TRUE) {
    agex <- rlang::sym("age_temp_num")
    lengthx <- rlang::sym(paste0("length", quo_name(year), "_num"))
  } else {
    agex <- rlang::sym("age_temp_char")
    lengthx <- rlang::sym(paste0("length", quo_name(year), "_grp"))
  }
  
  if (str_detect(quo_name(event_quo), "hosp|inpatient")) {
    event_year <- quo(hosp_year)
  } else if (str_detect(quo_name(event_quo), "ed")) {
    event_year <- quo(ed_year)
  } else if (str_detect(quo_name(event_quo), "inj|intent|mech")) {
    event_year <- quo(inj_year)
  }
  
  
  # Set up data frame to only include appropriate year
  output <- df %>%
    filter((!!event_year) == year_full | is.na((!!event_year)))
  
  # Restrict to 1 event per person per grouping if desired
  if (person == TRUE) {
    output <- output %>%
      distinct(pid2, agency_num, enroll_type_num, dual_elig_num, !!agex, 
               gender_num, ethn_num, voucher_num, subsidy_num, operator_num, 
               portfolio_num, !!lengthx, zip_c, .keep_all = T)
  }
  
  output <- output %>%
    group_by(agency_num, enroll_type_num, dual_elig_num, !!agex, gender_num, ethn_num,
             voucher_num, subsidy_num, operator_num, portfolio_num, !!lengthx, zip_c) %>%
    summarise(count = sum(!!event_quo)) %>%
    ungroup() %>%
    mutate(age_group = !!agex,
           length = !!lengthx,
           year = as.numeric(paste0(20, year))) %>%
    select(year, agency_num, enroll_type_num, dual_elig_num, age_group, gender_num, ethn_num,
           voucher_num, subsidy_num, operator_num, portfolio_num, length, zip_c,
           count) %>%
    rename(agency = agency_num,
           enroll_type = enroll_type_num,
           dual = dual_elig_num,
           gender = gender_num,
           ethn = ethn_num,
           voucher = voucher_num,
           subsidy = subsidy_num,
           operator = operator_num,
           portfolio = portfolio_num,
           zip = zip_c)
  return(output)
}


eventcount_chronic_f <- function(df_chronic = chronic, df_pop = chronic_pop,
                                  condition = NULL, year = 12, cvd = F) {
  
  yr_chk <- as.numeric(paste0("20", year))
  year_start = as.Date(paste0("20", year, "-01-01"), origin = "1970-01-01")
  year_end = as.Date(paste0("20", year, "-12-31"), origin = "1970-01-01")
  
  if (cvd == F) {
    condition_quo <- enquo(condition)
  } else if (cvd == T) {
    condition_quo <- NULL
  } else {
    stop("Enter TRUE or FALSE for including all CVD conditions")
  }

  
  # Filter to only include people with the condition in that year
  if (cvd == F) {
    cond <- df_chronic %>%
      filter(!!condition_quo == 1 & from_date <= year_end & to_date >= year_start) %>%
      distinct(id) %>%
      mutate(condition = 1)
  } else if (cvd == T) {
    cond <- df_chronic %>%
      filter((heart_failure_ccw == 1 & from_date <= year_end & to_date >= year_start) |
               (ischemic_heart_dis_ccw == 1 & from_date <= year_end & to_date >= year_start) |
               (hypertension_ccw == 1 & from_date <= year_end & to_date >= year_start)
             )%>%
      distinct(id) %>%
      mutate(condition = 1)
  }

  
  df_pop <- df_pop %>% filter(year == yr_chk)
  
  ### Join pop and condition data to summarise
  output <- left_join(df_pop, cond, by = c("mid" = "id"))
  output <- output %>% mutate(condition = if_else(is.na(condition), 0, 1))
  
  output <- output %>%
    group_by(year, agency_num, enroll_type_num, dual_elig_num, age_group,
             gender_num, ethn_num, voucher_num, subsidy_num, operator_num,
             portfolio_num, length, zip_c) %>%
    summarise(count := sum(condition)) %>%
    ungroup() %>%
    select(year, agency_num, enroll_type_num, dual_elig_num, age_group, 
           gender_num, ethn_num, voucher_num, subsidy_num, operator_num, 
           portfolio_num, length, zip_c,
           count) %>%
    rename(agency = agency_num,
           enroll_type = enroll_type_num,
           dual = dual_elig_num,
           gender = gender_num,
           ethn = ethn_num,
           voucher = voucher_num,
           subsidy = subsidy_num,
           operator = operator_num,
           portfolio = portfolio_num,
           zip = zip_c)
  
  return(output)
  
}


# Function to count # people with acute events based on prioritized pop
eventcount_acute_persons_f <- function(df_events = acute, df_pop = chronic_pop,
                                 event = c("ed", "hosp", "wc"), year = 12) {
  
  yr_chk <- as.numeric(paste0("20", year))

  # Filter to only include people with the condition in that year
  if (event == "ed") {
    df_events <- df_events %>% filter(ed == 1 & year(from_date) == yr_chk)
  } else if (event == "hosp") {
    df_events <- df_events %>% filter(inpatient == 1 & year(from_date) == yr_chk)
  } else if (event == "wc") {
    df_events <- df_events %>% filter(clm_type_code == 27 & year(from_date) == yr_chk)
  } else {
    stop("Chose an acute event from 'ed', 'hosp', or 'wc'")
  }

  df_events <- df_events %>%
    distinct(id) %>%
    mutate(condition = 1)
  
  # Filter assigned pop to current year
  df_pop <- df_pop %>% filter(year == yr_chk)
  
  
  ### Join pop and condition data to summarise
  output <- left_join(df_pop, df_events, by = c("mid" = "id"))
  output <- output %>% mutate(condition = if_else(is.na(condition), 0, 1))
  
  output <- output %>%
    group_by(year, agency_num, enroll_type_num, dual_elig_num, age_group,
             gender_num, ethn_num, voucher_num, subsidy_num, operator_num,
             portfolio_num, length, zip_c) %>%
    summarise(count := sum(condition)) %>%
    ungroup() %>%
    select(year, agency_num, enroll_type_num, dual_elig_num, age_group, 
           gender_num, ethn_num, voucher_num, subsidy_num, operator_num, 
           portfolio_num, length, zip_c,
           count) %>%
    rename(agency = agency_num,
           enroll_type = enroll_type_num,
           dual = dual_elig_num,
           gender = gender_num,
           ethn = ethn_num,
           voucher = voucher_num,
           subsidy = subsidy_num,
           operator = operator_num,
           portfolio = portfolio_num,
           zip = zip_c)
  
  return(output)
  
}

##### BRING IN DATA #####
### Code for mapping field values
demo_codes <- read.csv(text = RCurl::getURL("https://raw.githubusercontent.com/PHSKC-APDE/Housing/master/processing/housing_mcaid%20demo%20codes.csv"), 
                   header = TRUE, stringsAsFactors = FALSE)

### Main merged data
# pha_mcaid_final <- readRDS(file = paste0(housing_path, "/OrganizedData/pha_mcaid_final.Rda"))

### Bring in formatted housing and Medicaid demographics
pha_mcaid_demo <- readRDS(file = paste0(housing_path, "/OrganizedData/pha_mcaid_demo.Rda"))

# SQL option
# pha_mcaid_demo <- dbGetQuery(db.apde51, "SELECT * FROM dbo.housing_mcaid_demo")
# # Fix up date formats
# pha_mcaid_demo <- pha_mcaid_demo %>%
#   mutate_at(vars(startdate_c, enddate_c, dob_c, start_housing),
#             funs(as.Date(., origin = "1970-01-01")))



### Bring in all chronic conditions and combine
asthma <- dbGetQuery(db.claims51, "SELECT * FROM dbo.mcaid_claim_asthma_person")
chf <- dbGetQuery(db.claims51, "SELECT * FROM dbo.mcaid_claim_heart_failure_person")
copd <- dbGetQuery(db.claims51, "SELECT * FROM dbo.mcaid_claim_copd_person")
depression <- dbGetQuery(db.claims51, "SELECT * FROM dbo.mcaid_claim_depression_person")
diabetes <- dbGetQuery(db.claims51, "SELECT * FROM dbo.mcaid_claim_diabetes_person")
hypertension <- dbGetQuery(db.claims51, "SELECT * FROM dbo.mcaid_claim_hypertension_person")
ihd <- dbGetQuery(db.claims51, "SELECT * FROM dbo.mcaid_claim_ischemic_heart_dis_person")
kidney <- dbGetQuery(db.claims51, "SELECT * FROM dbo.mcaid_claim_chr_kidney_dis_person")


chronic <- bind_rows(asthma, chf, copd, depression, diabetes, hypertension, ihd, kidney) %>%
  mutate_at(vars(from_date, to_date), funs(as.Date(., origin = "1970-01-01")))

rm(asthma, chf, copd, depression, diabetes, hypertension, ihd, kidney)

### Bring in acute events (ED visits, hospitalizations, injuries, well child)
acute <- dbGetQuery(db.claims51, 
                    "SELECT id, tcn, from_date, to_date, clm_type_code, 
                    ed, ed_bh, ed_avoid_ca, ed_emergent_nyu, ed_nonemergent_nyu,
                    ed_intermediate_nyu, ed_mh_nyu, ed_sud_nyu, ed_alc_nyu, 
                    ed_injury_nyu, ed_unclass_nyu, 
                    inpatient, intent, mechanism, ccs_final_plain_lang
                    FROM dbo.mcaid_claim_summary
                    WHERE intent IS NOT NULL OR ed = 1 OR inpatient = 1 
                    OR clm_type_code = 27")
acute <- acute %>%
  mutate_at(vars(from_date, to_date), funs(as.Date(., origin = "1970-01-01")))



#### POPULATION ####
### Enrollment population

### Run function for all demographics combined
# pop_enroll_all2 <- lapply(seq(12, 17), popcount_all_f, df = pha_mcaid_demo)
# pop_enroll_all2 <- as.data.frame(data.table::rbindlist(pop_enroll_all2))

# This is horribly slow, figure out why
pop_enroll_all <- popcount(
  pha_mcaid_demo, 
  group_var = quos(agency_num, enroll_type_num, agegrp_h, gender_num, 
                ethn_num, dual_elig_num, voucher_num, subsidy_num, 
                operator_num, portfolio_num, time_housing, zip_c),
  unit = pid2, startdate = startdate_c, enddate = enddate_c, 
  numeric = T,
  yearmin = 2012, yearmax = 2017, period = "year",
  wc = FALSE)


# Run again for well child checks
# This is also horribly slow
pop_enroll_wc <- popcount(
  pha_mcaid_demo, 
  group_var = quos(agency_num, enroll_type_num, agegrp_h, gender_num, 
                   ethn_num, dual_elig_num, voucher_num, subsidy_num, 
                   operator_num, portfolio_num, time_housing, zip_c),
  unit = pid2, startdate = startdate_c, enddate = enddate_c,
  numeric = T,
  yearmin = 2012, yearmax = 2017, period = "year",
  wc = TRUE, wc_min = 0, wc_max = 6)


# Combine into a single df
pop_enroll_combine <- bind_rows(pop_enroll_all, pop_enroll_wc)

# Format for nicer Tableau names
pop_enroll_combine <- pop_enroll_combine %>%
  rename(agency = agency_num,
         enroll_type = enroll_type_num,
         age_group = agegrp_h,
         gender = gender_num,
         ethn = ethn_num,
         dual = dual_elig_num,
         voucher = voucher_num,
         subsidy = subsidy_num,
         operator = operator_num,
         portfolio = portfolio_num,
         length = time_housing,
         zip = zip_c) %>%
  mutate(year = year(date)) %>%
  select(wc_flag, year, agency:zip, pop_ever, pt_days, pop)


#### TEMP CHANGE ####
# Write up all bivariate combinations instead of fully granular data

# First apply names to numeric codes
pop_enroll_combine_bivar <- pop_enroll_combine

# Temp fix until popcount and popcount_wc functions fixed
pop_enroll_combine_bivar <- pop_enroll_combine_bivar %>%
  mutate(length = if_else(agency == 0, 0, length))

pop_enroll_combine_bivar <- relabel_f(pop_enroll_combine_bivar)

# Fix up well child age label
pop_enroll_combine_bivar <- pop_enroll_combine_bivar %>%
  mutate(age_group = ifelse(age_group == "Children aged 3-6",
                            "Children aged 0-6",
                            age_group))

### TEMP until codes updates
pop_enroll_combine_bivar <- pop_enroll_combine_bivar %>%
  mutate(operator = if_else(operator == "", "Unknown", operator))


# Use tab_loop_f from medicaid package to summarise
tabloop_age <- tabloop_f(pop_enroll_combine_bivar, sum = list_var(pop_ever, pop, pt_days),
                         fixed = list_var(wc_flag, year, agency, enroll_type, dual, age_group),
                         loop = list_var(gender, ethn, voucher, subsidy, operator, portfolio, length, zip)) %>%
  mutate(category1 = "age_group") %>%
  rename(group1 = age_group, category2 = group_cat, group2 = group)

tabloop_gender <- tabloop_f(pop_enroll_combine_bivar, sum = list_var(pop_ever, pop, pt_days),
                         fixed = list_var(wc_flag, year, agency, enroll_type, dual, gender),
                         loop = list_var(age_group, ethn, voucher, subsidy, operator, portfolio, length, zip)) %>%
  mutate(category1 = "gender") %>%
  rename(group1 = gender, category2 = group_cat, group2 = group)

tabloop_ethn <- tabloop_f(pop_enroll_combine_bivar, sum = list_var(pop_ever, pop, pt_days),
                         fixed = list_var(wc_flag, year, agency, enroll_type, dual, ethn),
                         loop = list_var(age_group, gender, voucher, subsidy, operator, portfolio, length, zip)) %>%
  mutate(category1 = "ethn") %>%
  rename(group1 = ethn, category2 = group_cat, group2 = group)

tabloop_voucher <- tabloop_f(pop_enroll_combine_bivar, sum = list_var(pop_ever, pop, pt_days),
                         fixed = list_var(wc_flag, year, agency, enroll_type, dual, voucher),
                         loop = list_var(age_group, gender, ethn, subsidy, operator, portfolio, length, zip)) %>%
  mutate(category1 = "voucher") %>%
  rename(group1 = voucher, category2 = group_cat, group2 = group)

tabloop_subsidy <- tabloop_f(pop_enroll_combine_bivar, sum = list_var(pop_ever, pop, pt_days),
                         fixed = list_var(wc_flag, year, agency, enroll_type, dual, subsidy),
                         loop = list_var(age_group, gender, ethn, voucher, operator, portfolio, length, zip)) %>%
  mutate(category1 = "subsidy") %>%
  rename(group1 = subsidy, category2 = group_cat, group2 = group)

tabloop_operator <- tabloop_f(pop_enroll_combine_bivar, sum = list_var(pop_ever, pop, pt_days),
                         fixed = list_var(wc_flag, year, agency, enroll_type, dual, operator),
                         loop = list_var(age_group, gender, ethn, voucher, subsidy, portfolio, length, zip)) %>%
  mutate(category1 = "operator") %>%
  rename(group1 = operator, category2 = group_cat, group2 = group)

tabloop_portfolio <- tabloop_f(pop_enroll_combine_bivar, sum = list_var(pop_ever, pop, pt_days),
                              fixed = list_var(wc_flag, year, agency, enroll_type, dual, portfolio),
                              loop = list_var(age_group, gender, ethn, voucher, subsidy, operator, length, zip)) %>%
  mutate(category1 = "portfolio") %>%
  rename(group1 = portfolio, category2 = group_cat, group2 = group)

tabloop_length <- tabloop_f(pop_enroll_combine_bivar, sum = list_var(pop_ever, pop, pt_days),
                              fixed = list_var(wc_flag, year, agency, enroll_type, dual, length),
                              loop = list_var(age_group, gender, ethn, voucher, subsidy, operator, portfolio, zip)) %>%
  mutate(category1 = "length") %>%
  rename(group1 = length, category2 = group_cat, group2 = group)

tabloop_zip <- tabloop_f(pop_enroll_combine_bivar, sum = list_var(pop_ever, pop, pt_days),
                            fixed = list_var(wc_flag, year, agency, enroll_type, dual, zip),
                            loop = list_var(age_group, gender, ethn, voucher, subsidy, operator, portfolio, length)) %>%
  mutate(category1 = "zip",
         zip = as.character(zip)) %>%
  rename(group1 = zip, category2 = group_cat, group2 = group)
gc()


# Make totals
tabloop_age_tot <- tabloop_f(pop_enroll_combine_bivar, sum = list_var(pop_ever, pop, pt_days),
                             fixed = list_var(wc_flag, year, agency, enroll_type, dual),
                             loop = list_var(age_group)) %>%
  mutate(category2 = "total", group2 = "total") %>%
  rename(category1 = group_cat, group1 = group)

tabloop_gender_tot <- tabloop_f(pop_enroll_combine_bivar, sum = list_var(pop_ever, pop, pt_days),
                                fixed = list_var(wc_flag, year, agency, enroll_type, dual),
                                loop = list_var(gender)) %>%
  mutate(category2 = "total", group2 = "total") %>%
  rename(category1 = group_cat, group1 = group)

tabloop_ethn_tot <- tabloop_f(pop_enroll_combine_bivar, sum = list_var(pop_ever, pop, pt_days),
                              fixed = list_var(wc_flag, year, agency, enroll_type, dual),
                              loop = list_var(ethn)) %>%
  mutate(category2 = "total", group2 = "total") %>%
  rename(category1 = group_cat, group1 = group)

tabloop_voucher_tot <- tabloop_f(pop_enroll_combine_bivar, sum = list_var(pop_ever, pop, pt_days),
                                 fixed = list_var(wc_flag, year, agency, enroll_type, dual),
                                 loop = list_var(voucher)) %>%
  mutate(category2 = "total", group2 = "total") %>%
  rename(category1 = group_cat, group1 = group)

tabloop_subsidy_tot <- tabloop_f(pop_enroll_combine_bivar, sum = list_var(pop_ever, pop, pt_days),
                                 fixed = list_var(wc_flag, year, agency, enroll_type, dual),
                                 loop = list_var(subsidy)) %>%
  mutate(category2 = "total", group2 = "total") %>%
  rename(category1 = group_cat, group1 = group)

tabloop_operator_tot <- tabloop_f(pop_enroll_combine_bivar, sum = list_var(pop_ever, pop, pt_days),
                                  fixed = list_var(wc_flag, year, agency, enroll_type, dual),
                                  loop = list_var(operator)) %>%
  mutate(category2 = "total", group2 = "total") %>%
  rename(category1 = group_cat, group1 = group)

tabloop_portfolio_tot <- tabloop_f(pop_enroll_combine_bivar, sum = list_var(pop_ever, pop, pt_days),
                                   fixed = list_var(wc_flag, year, agency, enroll_type, dual),
                                   loop = list_var(portfolio)) %>%
  mutate(category2 = "total", group2 = "total") %>%
  rename(category1 = group_cat, group1 = group)

tabloop_length_tot <- tabloop_f(pop_enroll_combine_bivar, sum = list_var(pop_ever, pop, pt_days),
                                fixed = list_var(wc_flag, year, agency, enroll_type, dual),
                                loop = list_var(length)) %>%
  mutate(category2 = "total", group2 = "total") %>%
  rename(category1 = group_cat, group1 = group)

tabloop_zip_tot <- tabloop_f(pop_enroll_combine_bivar, sum = list_var(pop_ever, pop, pt_days),
                             fixed = list_var(wc_flag, year, agency, enroll_type, dual),
                             loop = list_var(zip)) %>%
  mutate(category2 = "total", group2 = "total") %>%
  rename(category1 = group_cat, group1 = group)


# Combine into one
pop_enroll_combine_bivar <- bind_rows(tabloop_age, tabloop_gender, tabloop_ethn,
                                      tabloop_voucher, tabloop_subsidy,
                                      tabloop_operator, tabloop_portfolio, 
                                      tabloop_length, tabloop_zip,
                                      tabloop_age_tot, tabloop_gender_tot, 
                                      tabloop_ethn_tot, tabloop_voucher_tot, 
                                      tabloop_subsidy_tot, tabloop_operator_tot, 
                                      tabloop_portfolio_tot,  tabloop_length_tot, 
                                      tabloop_zip_tot) %>%
  filter(pt_days_sum > 0) %>%
  select(wc_flag, year, agency, enroll_type, dual, category1, group1, 
         category2, group2, pop_ever_sum, pop_sum, pt_days_sum) %>%
  rename(pop_ever = pop_ever_sum, pop = pop_sum, pt_days = pt_days_sum)

# Add suppression
pop_enroll_combine_bivar <- pop_enroll_combine_bivar %>%
  mutate_at(vars(pop_ever, pop),
            funs(supp_flag = if_else(between(., 1, 10), 1, 0))) %>%
  mutate_at(vars(pop_ever, pop),
            funs(supp = if_else(between(., 1, 10), NA_real_, .)))
  

### Make suppressed version
pop_enroll_combine_bivar_suppressed <- pop_enroll_combine_bivar %>%
  select(wc_flag:group2, pt_days, pop_ever_supp, pop_supp,
         pop_ever_supp_flag, pop_supp_flag) %>%
  rename(
    pop_ever = pop_ever_supp, pop = pop_supp,
    pop_ever_supp = pop_ever_supp_flag, pop_supp = pop_supp_flag)


#### END TEMP SECTION ####

# Write out file
write.xlsx(pop_enroll_combine_bivar_suppressed, paste0(housing_path, 
                                  "/OrganizedData/Summaries/PHA_Medicaid_enrollment_", 
                                  Sys.Date(), ".xlsx"),
           sheetName = "population")

write.xlsx(pop_enroll_combine_bivar, paste0(housing_path, 
                                            "/OrganizedData/Summaries/PHA_Medicaid_enrollment_unsuppressed_", 
                                            Sys.Date(), ".xlsx"),
           sheetName = "population")

rm(list = ls(pattern = "tabloop_"))
rm(pop_enroll_all)
rm(pop_enroll_wc)
gc()

#### END POPULATION ####

#### PERSON-TIME BASED EVENTS ####
### Hospitalizations
# Join demographic and hospitalization data
pha_mcaid_hosp <- left_join(pha_mcaid_demo, acute, by = c("mid" = "id")) %>%
  filter(inpatient == 1 & from_date >= startdate_c & from_date <= enddate_c) %>%
  mutate(hosp_year = year(from_date))

# Run for total number of hospitalizations
hosp_cnt <- lapply(seq(12, 17), eventcount_acute_f, df = pha_mcaid_hosp, 
                   event = inpatient, number = T, person = F)
hosp_cnt <- as.data.frame(data.table::rbindlist(hosp_cnt)) %>%
  mutate(indicator = "Hospitalizations")

rm(pha_mcaid_hosp)
gc()


### ED visits
# Join demographics and ED events
pha_mcaid_ed <- left_join(pha_mcaid_demo, acute, by = c("mid" = "id")) %>%
  filter(ed == 1 & from_date >= startdate_c & from_date <= enddate_c) %>%
  mutate(ed_year = year(from_date))

# Run for total number of ED visits
ed_cnt <- lapply(seq(12, 17), eventcount_acute_f, df = pha_mcaid_ed, 
                 event = ed, number = T, person = F)
ed_cnt <- as.data.frame(data.table::rbindlist(ed_cnt)) %>%
  mutate(indicator = "ED visits")

# Run for number of unavoidable ED visits
ed_cnt_unavoid <- lapply(seq(12, 17), eventcount_acute_f, df = pha_mcaid_ed, 
                 event = ed_emergent_nyu, number = T, person = F)
ed_cnt_unavoid <- as.data.frame(data.table::rbindlist(ed_cnt_unavoid)) %>%
  mutate(indicator = "ED visits - unavoidable")

# Run for number of too close to call ED visits
ed_cnt_inter <- lapply(seq(12, 17), eventcount_acute_f, df = pha_mcaid_ed, 
                         event = ed_intermediate_nyu, number = T, person = F)
ed_cnt_inter <- as.data.frame(data.table::rbindlist(ed_cnt_inter)) %>%
  mutate(indicator = "ED visits - borderline avoidable")

# Run for number of avoidable ED visits
ed_cnt_avoid <- lapply(seq(12, 17), eventcount_acute_f, df = pha_mcaid_ed, 
                         event = ed_nonemergent_nyu, number = T, person = F)
ed_cnt_avoid <- as.data.frame(data.table::rbindlist(ed_cnt_avoid)) %>%
  mutate(indicator = "ED visits - potentially avoidable")

# Run for number of unclassified ED visits
ed_cnt_unclass <- lapply(seq(12, 17), eventcount_acute_f, df = pha_mcaid_ed, 
                       event = ed_unclass_nyu, number = T, person = F)
ed_cnt_unclass <- as.data.frame(data.table::rbindlist(ed_cnt_unclass)) %>%
  mutate(indicator = "ED visits - unable to determine avoidability")

# Run for number of MH ED visits
ed_cnt_mh <- lapply(seq(12, 17), eventcount_acute_f, df = pha_mcaid_ed, 
                         event = ed_mh_nyu, number = T, person = F)
ed_cnt_mh <- as.data.frame(data.table::rbindlist(ed_cnt_mh)) %>%
  mutate(indicator = "ED visits - mental health primary dx")

# Run for number of alcohol-related ED visits
ed_cnt_alc <- lapply(seq(12, 17), eventcount_acute_f, df = pha_mcaid_ed, 
                    event = ed_alc_nyu, number = T, person = F)
ed_cnt_alc <- as.data.frame(data.table::rbindlist(ed_cnt_alc)) %>%
  mutate(indicator = "ED visits - alcohol-related primary dx")

# Run for number of alcohol-related ED visits
ed_cnt_sud <- lapply(seq(12, 17), eventcount_acute_f, df = pha_mcaid_ed, 
                     event = ed_sud_nyu, number = T, person = F)
ed_cnt_sud <- as.data.frame(data.table::rbindlist(ed_cnt_sud)) %>%
  mutate(indicator = "ED visits - substance use disorder related primary dx")

# Run for number of BH-related ED visits
ed_cnt_bh <- lapply(seq(12, 17), eventcount_acute_f, df = pha_mcaid_ed, 
                     event = ed_bh, number = T, person = F)
ed_cnt_bh <- as.data.frame(data.table::rbindlist(ed_cnt_bh)) %>%
  mutate(indicator = "ED visits - behavioral health-related primary dx")

rm(pha_mcaid_ed)
gc()


### CAUSES OF ACUTE CLAIMS (HOSPITALIZATION AND ED)
acute_cause_nonpha <- bind_rows(lapply(seq(2012,2017), function(x) {
  # Set up years
  year_from <- paste0(x, "-01-01")
  year_to <- paste0(x, "-12-31")
  
  # Hospitalizations (primary dx only)
  hosp_primary <- top_causes_f(cohort = pha_mcaid_demo[pha_mcaid_demo$agency_num == 0, ],
                                 cohort_id = mid,
                                 from_date = year_from, to_date = year_to,
                                 ind_dates = T,
                                 ind_from_date = startdate_c,
                                 ind_to_date = enddate_c,
                                 inpatient = T, ed_all = F, ed_avoid_ny = F, ed_avoid_ca = F,
                                 primary_dx = T)
  hosp_primary <- hosp_primary %>%
    mutate(year = x, agency_num = 0, cause_type = "Hospitalizations", dx_level = "Primary diagnosis only")
  
  # Hospitalizations (all dx fields)
  hosp_alldx <- top_causes_f(cohort = pha_mcaid_demo[pha_mcaid_demo$agency_num == 0, ],
                               cohort_id = mid,
                               renew_ids = F,
                               from_date = year_from, to_date = year_to,
                               ind_dates = T,
                               ind_from_date = startdate_c,
                               ind_to_date = enddate_c,
                               inpatient = T, ed_all = F, ed_avoid_ny = F, ed_avoid_ca = F,
                               primary_dx = F)
  hosp_alldx <- hosp_alldx %>%
    mutate(year = x, agency_num = 0, cause_type = "Hospitalizations", dx_level = "All diagnosis fields")
  
  # All ED visits (primary dx only)
  ed_all_primary <- top_causes_f(cohort = pha_mcaid_demo[pha_mcaid_demo$agency_num == 0, ],
                                 cohort_id = mid,
                                 renew_ids = F,
                                 from_date = year_from, to_date = year_to,
                                 ind_dates = T,
                                 ind_from_date = startdate_c,
                                 ind_to_date = enddate_c,
                                 inpatient = F, ed_all = T, ed_avoid_ny = F, ed_avoid_ca = F,
                                 primary_dx = T)
  ed_all_primary <- ed_all_primary %>%
    mutate(year = x, agency_num = 0, cause_type = "All ED visits", dx_level = "Primary diagnosis only")
  
  # All ED visits (all dx fields)
  ed_all_alldx <- top_causes_f(cohort = pha_mcaid_demo[pha_mcaid_demo$agency_num == 0, ],
                               cohort_id = mid,
                               renew_ids = F,
                               from_date = year_from, to_date = year_to,
                               ind_dates = T,
                               ind_from_date = startdate_c,
                               ind_to_date = enddate_c,
                               inpatient = F, ed_all = T, ed_avoid_ny = F, ed_avoid_ca = F,
                               primary_dx = F)
  ed_all_alldx <- ed_all_alldx %>%
    mutate(year = x, agency_num = 0, cause_type = "All ED visits", dx_level = "All diagnosis fields")
  
  # Potentially avoidable ED visits (primary dx only)
  ed_avoid_primary <- top_causes_f(cohort = pha_mcaid_demo[pha_mcaid_demo$agency_num == 0, ],
                                   cohort_id = mid,
                                   renew_ids = F,
                                   from_date = year_from, to_date = year_to,
                                   ind_dates = T,
                                   ind_from_date = startdate_c,
                                   ind_to_date = enddate_c,
                                   inpatient = F, ed_all = F, ed_avoid_ny = T, ed_avoid_ca = F,
                                   primary_dx = T)
  ed_avoid_primary <- ed_avoid_primary %>%
    mutate(year = x, agency_num = 0, cause_type = "Potentially avoidable ED visits", dx_level = "Primary diagnosis only")
  
  # Potentially avoidable ED visits (all dx fields)
  ed_avoid_alldx <- top_causes_f(cohort = pha_mcaid_demo[pha_mcaid_demo$agency_num == 0, ],
                                 cohort_id = mid,
                                 renew_ids = F,
                                 from_date = year_from, to_date = year_to,
                                 ind_dates = T,
                                 ind_from_date = startdate_c,
                                 ind_to_date = enddate_c,
                                 inpatient = F, ed_all = F, ed_avoid_ny = T, ed_avoid_ca = F,
                                 primary_dx = F)
  
  ed_avoid_alldx <- ed_avoid_alldx %>%
    mutate(year = x, agency_num = 0, cause_type = "Potentially avoidable ED visits", dx_level = "All diagnosis fields")
  
  output <- bind_rows(hosp_primary, hosp_alldx,
                      ed_all_primary, ed_all_alldx, ed_avoid_primary, ed_avoid_alldx)
  return(output)
}))

acute_cause_kcha <- bind_rows(lapply(seq(2012,2017), function(x) {
  # Set up years
  year_from <- paste0(x, "-01-01")
  year_to <- paste0(x, "-12-31")
  
  # Hospitalizations (primary dx only)
  hosp_primary <- top_causes_f(cohort = pha_mcaid_demo[pha_mcaid_demo$agency_num == 1 & 
                                                         pha_mcaid_demo$enroll_type_num == 3, ],
                               cohort_id = mid,
                               from_date = year_from, to_date = year_to,
                               ind_dates = T,
                               ind_from_date = startdate_c,
                               ind_to_date = enddate_c,
                               inpatient = T, ed_all = F, ed_avoid_ny = F, ed_avoid_ca = F,
                               primary_dx = T)
  hosp_primary <- hosp_primary %>%
    mutate(year = x, agency_num = 1, cause_type = "Hospitalizations", dx_level = "Primary diagnosis only")
  
  # Hospitalizations (all dx fields)
  hosp_alldx <- top_causes_f(cohort = pha_mcaid_demo[pha_mcaid_demo$agency_num == 1 & 
                                                       pha_mcaid_demo$enroll_type_num == 3, ],
                             cohort_id = mid,
                             renew_ids = F,
                             from_date = year_from, to_date = year_to,
                             ind_dates = T,
                             ind_from_date = startdate_c,
                             ind_to_date = enddate_c,
                             inpatient = T, ed_all = F, ed_avoid_ny = F, ed_avoid_ca = F,
                             primary_dx = F)
  hosp_alldx <- hosp_alldx %>%
    mutate(year = x, agency_num = 1, cause_type = "Hospitalizations", dx_level = "All diagnosis fields")
  
  # All ED visits (primary dx only)
  ed_all_primary <- top_causes_f(cohort = pha_mcaid_demo[pha_mcaid_demo$agency_num == 1 & 
                                                           pha_mcaid_demo$enroll_type_num == 3, ],
                                 cohort_id = mid,
                                 renew_ids = F,
                                 from_date = year_from, to_date = year_to,
                                 ind_dates = T,
                                 ind_from_date = startdate_c,
                                 ind_to_date = enddate_c,
                                 inpatient = F, ed_all = T, ed_avoid_ny = F, ed_avoid_ca = F,
                                 primary_dx = T)
  ed_all_primary <- ed_all_primary %>%
    mutate(year = x, agency_num = 1, cause_type = "All ED visits", dx_level = "Primary diagnosis only")
  
  # All ED visits (all dx fields)
  ed_all_alldx <- top_causes_f(cohort = pha_mcaid_demo[pha_mcaid_demo$agency_num == 1 & 
                                                         pha_mcaid_demo$enroll_type_num == 3, ],
                               cohort_id = mid,
                               renew_ids = F,
                               from_date = year_from, to_date = year_to,
                               ind_dates = T,
                               ind_from_date = startdate_c,
                               ind_to_date = enddate_c,
                               inpatient = F, ed_all = T, ed_avoid_ny = F, ed_avoid_ca = F,
                               primary_dx = F)
  ed_all_alldx <- ed_all_alldx %>%
    mutate(year = x, agency_num = 1, cause_type = "All ED visits", dx_level = "All diagnosis fields")
  
  # Potentially avoidable ED visits (primary dx only)
  ed_avoid_primary <- top_causes_f(cohort = pha_mcaid_demo[pha_mcaid_demo$agency_num == 1 & 
                                                             pha_mcaid_demo$enroll_type_num == 3, ],
                                   cohort_id = mid,
                                   renew_ids = F,
                                   from_date = year_from, to_date = year_to,
                                   ind_dates = T,
                                   ind_from_date = startdate_c,
                                   ind_to_date = enddate_c,
                                   inpatient = F, ed_all = F, ed_avoid_ny = T, ed_avoid_ca = F,
                                   primary_dx = T)
  ed_avoid_primary <- ed_avoid_primary %>%
    mutate(year = x, agency_num = 1, cause_type = "Potentially avoidable ED visits", dx_level = "Primary diagnosis only")
  
  # Potentially avoidable ED visits (all dx fields)
  ed_avoid_alldx <- top_causes_f(cohort = pha_mcaid_demo[pha_mcaid_demo$agency_num == 1 & 
                                                           pha_mcaid_demo$enroll_type_num == 3, ],
                                 cohort_id = mid,
                                 renew_ids = F,
                                 from_date = year_from, to_date = year_to,
                                 ind_dates = T,
                                 ind_from_date = startdate_c,
                                 ind_to_date = enddate_c,
                                 inpatient = F, ed_all = F, ed_avoid_ny = T, ed_avoid_ca = F,
                                 primary_dx = F)
  
  ed_avoid_alldx <- ed_avoid_alldx %>%
    mutate(year = x, agency_num = 1, cause_type = "Potentially avoidable ED visits", dx_level = "All diagnosis fields")
  
  output <- bind_rows(hosp_primary, hosp_alldx,
                      ed_all_primary, ed_all_alldx, ed_avoid_primary, ed_avoid_alldx)
  return(output)
}))

acute_cause_sha <- bind_rows(lapply(seq(2012,2017), function(x) {
  # Set up years
  year_from <- paste0(x, "-01-01")
  year_to <- paste0(x, "-12-31")
  
  # Hospitalizations (primary dx only)
  hosp_primary <- top_causes_f(cohort = pha_mcaid_demo[pha_mcaid_demo$agency_num == 2 & 
                                                         pha_mcaid_demo$enroll_type_num == 3, ],
                               cohort_id = mid,
                               from_date = year_from, to_date = year_to,
                               ind_dates = T,
                               ind_from_date = startdate_c,
                               ind_to_date = enddate_c,
                               inpatient = T, ed_all = F, ed_avoid_ny = F, ed_avoid_ca = F,
                               primary_dx = T)
  hosp_primary <- hosp_primary %>%
    mutate(year = x, agency_num = 2, cause_type = "Hospitalizations", dx_level = "Primary diagnosis only")
  
  # Hospitalizations (all dx fields)
  hosp_alldx <- top_causes_f(cohort = pha_mcaid_demo[pha_mcaid_demo$agency_num == 2 & 
                                                       pha_mcaid_demo$enroll_type_num == 3, ],
                             cohort_id = mid,
                             renew_ids = F,
                             from_date = year_from, to_date = year_to,
                             ind_dates = T,
                             ind_from_date = startdate_c,
                             ind_to_date = enddate_c,
                             inpatient = T, ed_all = F, ed_avoid_ny = F, ed_avoid_ca = F,
                             primary_dx = F)
  hosp_alldx <- hosp_alldx %>%
    mutate(year = x, agency_num = 2, cause_type = "Hospitalizations", dx_level = "All diagnosis fields")
  
  # All ED visits (primary dx only)
  ed_all_primary <- top_causes_f(cohort = pha_mcaid_demo[pha_mcaid_demo$agency_num == 2 & 
                                                           pha_mcaid_demo$enroll_type_num == 3, ],
                                 cohort_id = mid,
                                 renew_ids = F,
                                 from_date = year_from, to_date = year_to,
                                 ind_dates = T,
                                 ind_from_date = startdate_c,
                                 ind_to_date = enddate_c,
                                 inpatient = F, ed_all = T, ed_avoid_ny = F, ed_avoid_ca = F,
                                 primary_dx = T)
  ed_all_primary <- ed_all_primary %>%
    mutate(year = x, agency_num = 2, cause_type = "All ED visits", dx_level = "Primary diagnosis only")
  
  # All ED visits (all dx fields)
  ed_all_alldx <- top_causes_f(cohort = pha_mcaid_demo[pha_mcaid_demo$agency_num == 2 & 
                                                         pha_mcaid_demo$enroll_type_num == 3, ],
                               cohort_id = mid,
                               renew_ids = F,
                               from_date = year_from, to_date = year_to,
                               ind_dates = T,
                               ind_from_date = startdate_c,
                               ind_to_date = enddate_c,
                               inpatient = F, ed_all = T, ed_avoid_ny = F, ed_avoid_ca = F,
                               primary_dx = F)
  ed_all_alldx <- ed_all_alldx %>%
    mutate(year = x, agency_num = 2, cause_type = "All ED visits", dx_level = "All diagnosis fields")
  
  # Potentially avoidable ED visits (primary dx only)
  ed_avoid_primary <- top_causes_f(cohort = pha_mcaid_demo[pha_mcaid_demo$agency_num == 2 & 
                                                             pha_mcaid_demo$enroll_type_num == 3, ],
                                   cohort_id = mid,
                                   renew_ids = F,
                                   from_date = year_from, to_date = year_to,
                                   ind_dates = T,
                                   ind_from_date = startdate_c,
                                   ind_to_date = enddate_c,
                                   inpatient = F, ed_all = F, ed_avoid_ny = T, ed_avoid_ca = F,
                                   primary_dx = T)
  ed_avoid_primary <- ed_avoid_primary %>%
    mutate(year = x, agency_num = 2, cause_type = "Potentially avoidable ED visits", dx_level = "Primary diagnosis only")
  
  # Potentially avoidable ED visits (all dx fields)
  ed_avoid_alldx <- top_causes_f(cohort = pha_mcaid_demo[pha_mcaid_demo$agency_num == 2 & 
                                                           pha_mcaid_demo$enroll_type_num == 3, ],
                                 cohort_id = mid,
                                 renew_ids = F,
                                 from_date = year_from, to_date = year_to,
                                 ind_dates = T,
                                 ind_from_date = startdate_c,
                                 ind_to_date = enddate_c,
                                 inpatient = F, ed_all = F, ed_avoid_ny = T, ed_avoid_ca = F,
                                 primary_dx = F)
  
  ed_avoid_alldx <- ed_avoid_alldx %>%
    mutate(year = x, agency_num = 2, cause_type = "Potentially avoidable ED visits", dx_level = "All diagnosis fields")
  
  output <- bind_rows(hosp_primary, hosp_alldx,
                      ed_all_primary, ed_all_alldx, ed_avoid_primary, ed_avoid_alldx)
  return(output)
}))

# Combine data and export
acute_cause <- bind_rows(acute_cause_nonpha, acute_cause_kcha, acute_cause_sha) %>%
  arrange(year, agency_num, cause_type, dx_level, -claim_cnt, ccs_final_plain_lang) %>%
  group_by(year, agency_num, cause_type, dx_level) %>%
  mutate(
    agency = case_when(
      agency_num == 0 ~ "Non-PHA",
      agency_num == 1 ~ "KCHA",
      agency_num == 2 ~ "SHA"
    ),
    # Add rank for each category
    rank = min_rank(-claim_cnt),
    # Add field to indicate when the count is suppressed
    claim_cnt_supp = ifelse(claim_cnt < 5, "<5", as.character(claim_cnt)),
    # Suppress small counts
    claim_cnt = ifelse(claim_cnt < 5, NA, claim_cnt),
    # Find the rank for the first suppressed value
    rank_max = max(rank[!is.na(claim_cnt)], na.rm = T) + 1,
    # Overwrite the rank and add a flag
    rank = ifelse(is.na(claim_cnt), rank_max, rank),
    rank_flag = ifelse(is.na(claim_cnt), "+", NA_character_)
  ) %>%
  ungroup() %>%
  select(-rank_max)

write.xlsx(acute_cause, paste0(housing_path, 
                               "/OrganizedData/Summaries/PHA_Medicaid_health_events_causes_", 
                               Sys.Date(), ".xlsx"),
           sheetName = "acute_causes")



### Injuries (restrict to 2016 and 2017 for now due to ICD issues)
# Join demographics and injury events
pha_mcaid_inj <- left_join(pha_mcaid_demo, acute, by = c("mid" = "id")) %>%
  filter(!is.na(intent) & from_date >= startdate_c & from_date <= enddate_c) %>%
  mutate(inj_year = year(from_date),
         injury = 1)

# Run for number of people with an injury
inj_pers <- lapply(seq(16, 17), eventcount_acute_f, df = pha_mcaid_inj, 
                   event = injury, number = T, person = T)
inj_pers <- as.data.frame(data.table::rbindlist(inj_pers)) %>%
  mutate(indicator = "Persons with an injury")

# Run for total number of injury visits
inj_cnt <- lapply(seq(16, 17), eventcount_acute_f, df = pha_mcaid_inj, 
                  event = injury, number = T, person = F)
inj_cnt <- as.data.frame(data.table::rbindlist(inj_cnt)) %>%
  mutate(indicator = "Injuries")
rm(pha_mcaid_inj)

rm(acute)
gc()


#### POPULATION-BASED COUNTS ####
### Set up population for chronic conditions
chronic_pop <- bind_rows(lapply(seq(12, 17), chronic_pop_f, df = pha_mcaid_demo))
chronic_pop_wc <- bind_rows(lapply(seq(12, 17), chronic_pop_f, df = pha_mcaid_demo, wc = T))


### Asthma
asthma_pers <- lapply(seq(12, 17), eventcount_chronic_f, df_chronic = chronic, 
                     df_pop = chronic_pop, condition = asthma_ccw, cvd = F)
asthma_pers <- as.data.frame(data.table::rbindlist(asthma_pers)) %>%
  mutate(indicator = "Persons with asthma")
gc()


# CHF
chf_pers <- lapply(seq(12, 17), eventcount_chronic_f, df_chronic = chronic, 
                     df_pop = chronic_pop, condition = heart_failure_ccw, cvd = F)
chf_pers <- as.data.frame(data.table::rbindlist(chf_pers)) %>%
  mutate(indicator = "Persons with cardiovascular disease: congestive heart failure")
gc()


# COPD
copd_pers <- lapply(seq(12, 17), eventcount_chronic_f, df_chronic = chronic, 
                  df_pop = chronic_pop, condition = copd_ccw, cvd = F)
copd_pers <- as.data.frame(data.table::rbindlist(copd_pers)) %>%
  mutate(indicator = "Persons with chronic obstructive pulmonary disease")
gc()


# CVD
cvd_pers <- lapply(seq(12, 17), eventcount_chronic_f, df_chronic = chronic, 
                   df_pop = chronic_pop, cvd = T)
cvd_pers <- as.data.frame(data.table::rbindlist(cvd_pers)) %>%
  mutate(indicator = "Persons with cardiovascular disease: any type")
gc()


# Depression
depression_pers <- lapply(seq(12, 17), eventcount_chronic_f, df_chronic = chronic, 
                  df_pop = chronic_pop, condition = depression_ccw, cvd = F)
depression_pers <- as.data.frame(data.table::rbindlist(depression_pers)) %>%
  mutate(indicator = "Persons with depression")
gc()


# Diabetes
diabetes_pers <- lapply(seq(12, 17), eventcount_chronic_f, df_chronic = chronic, 
                  df_pop = chronic_pop, condition = diabetes_ccw, cvd = F)
diabetes_pers <- as.data.frame(data.table::rbindlist(diabetes_pers)) %>%
  mutate(indicator = "Persons with diabetes")
gc()


# Hypertension
hypertension_pers <- lapply(seq(12, 17), eventcount_chronic_f, df_chronic = chronic, 
                  df_pop = chronic_pop, condition = hypertension_ccw, cvd = F)
hypertension_pers <- as.data.frame(data.table::rbindlist(hypertension_pers)) %>%
  mutate(indicator = "Persons with cardiovascular disease: hypertension")
gc()


# IHD
ihd_pers <- lapply(seq(12, 17), eventcount_chronic_f, df_chronic = chronic, 
                  df_pop = chronic_pop, condition = ischemic_heart_dis_ccw, cvd = F)
ihd_pers <- as.data.frame(data.table::rbindlist(ihd_pers)) %>%
  mutate(indicator = "Persons with cardiovascular disease: ischemic heart disease")
gc()


# Kidney disease
kidney_pers <- lapply(seq(12, 17), eventcount_chronic_f, df_chronic = chronic, 
                  df_pop = chronic_pop, condition = chr_kidney_dis_ccw, cvd = F)
kidney_pers <- as.data.frame(data.table::rbindlist(kidney_pers)) %>%
  mutate(indicator = "Persons with kidney disease")
gc()



# Persons with an ED visit
ed_pers <- bind_rows(lapply(seq(12, 17), eventcount_acute_persons_f, 
                              df_events = acute, df_pop = chronic_pop,
                              event = "ed")) %>%
  mutate(indicator = "Persons with ED visits")


# Persons with a hospitalization
hosp_pers <- bind_rows(lapply(seq(12, 17), eventcount_acute_persons_f, 
                              df_events = acute, df_pop = chronic_pop,
                              event = "hosp")) %>%
  mutate(indicator = "Persons with hospitalization")


# Persons with a well-child visit
wc_pers <- bind_rows(lapply(seq(12, 17), eventcount_acute_persons_f, 
                              df_events = acute, df_pop = chronic_pop_wc,
                              event = "wc")) %>%
  mutate(indicator = "Well-child check")




#### COMBINE DATA ####
health_events <- bind_rows(asthma_pers, chf_pers, copd_pers, cvd_pers,
                           depression_pers,  diabetes_pers, ed_cnt, ed_cnt_alc, 
                           ed_cnt_bh, ed_cnt_avoid, ed_cnt_inter, ed_cnt_mh, 
                           ed_cnt_sud,  ed_cnt_unavoid, ed_cnt_unclass, ed_pers, 
                           hosp_cnt,  hosp_pers, hypertension_pers, ihd_pers, 
                           # inj_pers, inj_pers, 
                           kidney_pers, wc_pers)

rm(list = ls(pattern = "_cnt$"))
rm(list = ls(pattern = "^ed_cnt"))
rm(list = ls(pattern = "_pers$"))
rm(chronic)
rm(chronic_pop)
gc()

# Recode numbers to text
health_events <- relabel_f(health_events)

# temp fix
health_events <- health_events %>%
  mutate(age_group = ifelse(age_group == "Children aged 3-6",
                            "Children aged 0-6",
                            age_group))

# Reorder columns
health_events <- health_events %>% select(year, indicator, agency:count)


# TEMP UNTIL CODES RERUN EARLIER
# Set soft unit operator to N/A
health_events <- health_events %>%
  mutate(operator = ifelse(subsidy == "Soft unit/tenant based",
                           "N/A", operator))


#### SAVE FILES TO DISK TEMP ####
saveRDS(chronic, file = paste0(housing_path, "/OrganizedData/chronic.Rda"))
saveRDS(chronic_pop, file = paste0(housing_path, "/OrganizedData/chronic_pop.Rda"))
saveRDS(health_events, file = paste0(housing_path, "/OrganizedData/health_events.Rda"))
saveRDS(pop_enroll_combine, file = paste0(housing_path, "/OrganizedData/pop_enroll_combine.Rda"))

chronic <- readRDS(paste0(housing_path, "/OrganizedData/chronic.Rda"))
chronic_pop <- readRDS(paste0(housing_path, "/OrganizedData/chronic_pop.Rda"))
health_events <- readRDS(paste0(housing_path, "/OrganizedData/health_events.Rda"))
pop_enroll_combine <- readRDS(paste0(housing_path, "/OrganizedData/pop_enroll_combine.Rda"))

#### END TEMP SAVE ####


#### AGGREGATION ####
# Use tab_loop_f from medicaid package to summarise
# Remove rows with zero count for some variables (e.g., ZIP) to save memory
tabloop_age <- tabloop_f(health_events, sum = list_var(count),
                         fixed = list_var(indicator, year, agency, enroll_type, dual, age_group),
                         loop = list_var(gender, ethn, voucher, subsidy, operator, portfolio, length, zip)) %>%
  mutate(category1 = "age_group") %>%
  filter(count_sum > 0) %>%
  rename(group1 = age_group, category2 = group_cat, group2 = group)
gc()

tabloop_gender <- tabloop_f(health_events, sum = list_var(count),
                            fixed = list_var(indicator, year, agency, enroll_type, dual, gender),
                            loop = list_var(age_group, ethn, voucher, subsidy, operator, portfolio, length, zip)) %>%
  mutate(category1 = "gender") %>%
  filter(count_sum > 0) %>%
  rename(group1 = gender, category2 = group_cat, group2 = group)
gc()

tabloop_ethn <- tabloop_f(health_events, sum = list_var(count),
                          fixed = list_var(indicator, year, agency, enroll_type, dual, ethn),
                          loop = list_var(age_group, gender, voucher, subsidy, operator, portfolio, length, zip)) %>%
  mutate(category1 = "ethn") %>%
  filter(count_sum > 0) %>%
  rename(group1 = ethn, category2 = group_cat, group2 = group)
gc()

tabloop_voucher <- tabloop_f(health_events, sum = list_var(count),
                             fixed = list_var(indicator, year, agency, enroll_type, dual, voucher),
                             loop = list_var(age_group, gender, ethn, subsidy, operator, portfolio, length, zip)) %>%
  mutate(category1 = "voucher") %>%
  filter(count_sum > 0) %>%
  rename(group1 = voucher, category2 = group_cat, group2 = group)
gc()

tabloop_subsidy <- tabloop_f(health_events, sum = list_var(count),
                             fixed = list_var(indicator, year, agency, enroll_type, dual, subsidy),
                             loop = list_var(age_group, gender, ethn, voucher, operator, portfolio, length, zip)) %>%
  mutate(category1 = "subsidy") %>%
  filter(count_sum > 0) %>%
  rename(group1 = subsidy, category2 = group_cat, group2 = group)
gc()

tabloop_operator <- tabloop_f(health_events, sum = list_var(count),
                              fixed = list_var(indicator, year, agency, enroll_type, dual, operator),
                              loop = list_var(age_group, gender, ethn, voucher, subsidy, portfolio, length, zip)) %>%
  mutate(category1 = "operator") %>%
  filter(count_sum > 0) %>%
  rename(group1 = operator, category2 = group_cat, group2 = group)
gc()

tabloop_portfolio <- tabloop_f(health_events, sum = list_var(count),
                               fixed = list_var(indicator, year, agency, enroll_type, dual, portfolio),
                               loop = list_var(age_group, gender, ethn, voucher, subsidy, operator, length, zip)) %>%
  mutate(category1 = "portfolio") %>%
  filter(count_sum > 0) %>%
  rename(group1 = portfolio, category2 = group_cat, group2 = group)
gc()

tabloop_length <- tabloop_f(health_events, sum = list_var(count),
                            fixed = list_var(indicator, year, agency, enroll_type, dual, length),
                            loop = list_var(age_group, gender, ethn, voucher, subsidy, operator, portfolio, zip)) %>%
  mutate(category1 = "length") %>%
  filter(count_sum > 0) %>%
  rename(group1 = length, category2 = group_cat, group2 = group)
gc()

# Not using these data right now (also too memory intensive to run)
# tabloop_zip <- tabloop_f(health_events, sum = list_var(count),
#                          fixed = list_var(indicator, year, agency, enroll_type, dual, zip),
#                          loop = list_var(age_group, gender, ethn, voucher, subsidy, operator, portfolio, length)) %>%
#   mutate(category1 = "zip", zip = as.character(zip)) %>%
#   filter(count_sum > 0) %>%
#   rename(group1 = zip, category2 = group_cat, group2 = group)
# gc()


### Make total columns for univariate analyses
tabloop_age_tot <- tabloop_f(health_events, sum = list_var(count),
                         fixed = list_var(indicator, year, agency, enroll_type, dual),
                         loop = list_var(age_group)) %>%
  mutate(category2 = "total", group2 = "total") %>%
  rename(category1 = group_cat, group1 = group)

tabloop_gender_tot <- tabloop_f(health_events, sum = list_var(count),
                             fixed = list_var(indicator, year, agency, enroll_type, dual),
                             loop = list_var(gender)) %>%
  mutate(category2 = "total", group2 = "total") %>%
  rename(category1 = group_cat, group1 = group)

tabloop_ethn_tot <- tabloop_f(health_events, sum = list_var(count),
                             fixed = list_var(indicator, year, agency, enroll_type, dual),
                             loop = list_var(ethn)) %>%
  mutate(category2 = "total", group2 = "total") %>%
  rename(category1 = group_cat, group1 = group)

tabloop_voucher_tot <- tabloop_f(health_events, sum = list_var(count),
                             fixed = list_var(indicator, year, agency, enroll_type, dual),
                             loop = list_var(voucher)) %>%
  mutate(category2 = "total", group2 = "total") %>%
  rename(category1 = group_cat, group1 = group)

tabloop_subsidy_tot <- tabloop_f(health_events, sum = list_var(count),
                             fixed = list_var(indicator, year, agency, enroll_type, dual),
                             loop = list_var(subsidy)) %>%
  mutate(category2 = "total", group2 = "total") %>%
  rename(category1 = group_cat, group1 = group)

tabloop_operator_tot <- tabloop_f(health_events, sum = list_var(count),
                             fixed = list_var(indicator, year, agency, enroll_type, dual),
                             loop = list_var(operator)) %>%
  mutate(category2 = "total", group2 = "total") %>%
  rename(category1 = group_cat, group1 = group)

tabloop_portfolio_tot <- tabloop_f(health_events, sum = list_var(count),
                             fixed = list_var(indicator, year, agency, enroll_type, dual),
                             loop = list_var(portfolio)) %>%
  mutate(category2 = "total", group2 = "total") %>%
  rename(category1 = group_cat, group1 = group)

tabloop_length_tot <- tabloop_f(health_events, sum = list_var(count),
                             fixed = list_var(indicator, year, agency, enroll_type, dual),
                             loop = list_var(length)) %>%
  mutate(category2 = "total", group2 = "total") %>%
  rename(category1 = group_cat, group1 = group)

tabloop_zip_tot <- tabloop_f(health_events, sum = list_var(count),
                                fixed = list_var(indicator, year, agency, enroll_type, dual),
                                loop = list_var(zip)) %>%
  mutate(category2 = "total", group2 = "total") %>%
  rename(category1 = group_cat, group1 = group)



# Combine into one data frame
health_events_combined <- bind_rows(tabloop_age, tabloop_gender, tabloop_ethn,
                           tabloop_voucher, tabloop_subsidy,
                           tabloop_operator, tabloop_portfolio, 
                           tabloop_length, #tabloop_zip,
                           tabloop_age_tot, tabloop_gender_tot, tabloop_ethn_tot,
                           tabloop_voucher_tot, tabloop_subsidy_tot,
                           tabloop_operator_tot, tabloop_portfolio_tot, 
                           tabloop_length_tot, tabloop_zip_tot) %>%
  # Filter out impossible combinations
  filter(!(enroll_type == "Housing only")) %>%
  rename(count = count_sum) %>%
  mutate(wc_flag = if_else(indicator == "Well-child check", 1, 0),
         acute = case_when(
           str_detect(indicator, "Persons with") ~ 0,
           str_detect(indicator, "ED visits") ~ 1,
           indicator == "Hospitalizations" ~ 1,
           indicator == "Well-child check" ~ 1,
           TRUE ~ NA_real_
         )) %>%
  select(indicator, acute, wc_flag, year, agency, enroll_type, dual, category1, group1:count) %>%
  arrange(indicator, year, agency, enroll_type, dual, 
          category1, group1, category2, group2)

# Add suppression
health_events_combined <- health_events_combined %>%
  mutate(
    suppressed = if_else(between(count, 1, 10), 1, 0),
    count_supp = if_else(between(count, 1, 10), NA_real_, count))

#### END TEMP AGGREGATION ####


#### COMBINE WITH POP DATA ####
health_events_combined_pop <- left_join(health_events_combined, pop_enroll_combine_bivar,
                                    by = c("year", "wc_flag", "agency", "enroll_type",
                                           "dual", "category1", "group1",
                                           "category2", "group2")) %>%
  # Filter out rows with no population
  # NB. There are 26 rows with count > 0 and no pop (total count = 31), could invesitgate further
  filter(!is.na(pop_ever))

health_events_combined_pop <- health_events_combined_pop %>%
  mutate(
    rate = case_when(
      acute == 1 & year %in% c(2012, 2016) ~ count / (pt_days / 366) * 1000,
      acute == 1 ~ count / (pt_days / 365) * 1000,
      acute == 0 & pop == 0 ~ NA_real_,
      acute == 0 ~ count / pop * 1000
      ),
    # Use exact 95% CI for Poisson
    ci_lb = case_when(
      acute == 1 & year %in% c(2012, 2016) ~  qgamma((1 - 0.95)/2, count) / (pt_days / 366) * 1000,
      acute == 1 ~ qgamma((1 - 0.95)/2, count) / (pt_days / 365) * 1000,
      acute == 0 & (pop == 0 | pop - count <= 0) ~ NA_real_,
      acute == 0 ~ qbeta((1 - 0.95)/2, count, pop - count + 1) * 1000
    ),
    ci_ub = case_when(
      acute == 1 & year %in% c(2012, 2016) ~  qgamma(1 - (1 - 0.95)/2, count + 1) / (pt_days / 366) * 1000,
      acute == 1 ~ qgamma(1 - (1 - 0.95)/2, count + 1) / (pt_days / 365) * 1000,
      acute == 0 & (pop == 0 | pop - count <= 0) ~ NA_real_,
      acute == 0 ~ qbeta(1 - (1 - 0.95)/2, count + 1, pop - count) * 1000
      )
    )
  


### Create portfolio specific comparisons and join back to main data
rest_pha <- health_events_combined_pop %>%
  filter((agency == "KCHA" | agency == "SHA") & category1 == "portfolio" & 
           dual == "Not dual eligible" & category2 == "total") %>%
  group_by(year, indicator, agency) %>%
  mutate(count_tot = sum(count), pop_tot = sum(pop), pt_tot = sum(pt_days)) %>%
  ungroup() %>%
  mutate(
    rest_count = count_tot - count,
    rest_pop = pop_tot - pop,
    rest_pt = pt_tot - pt_days,
    rest_pha_rate = case_when(
      acute == 1 & year %in% c(2012, 2016) ~ rest_count / (rest_pt / 366) * 1000,
      acute == 1 ~ rest_count / (rest_pt / 365) * 1000,
      acute == 0 & pop == 0 ~ NA_real_,
      acute == 0 ~ rest_count / rest_pop * 1000
    ),
    # Use exact 95% CI for Poisson
    rest_ci_lb = case_when(
      acute == 1 & year %in% c(2012, 2016) ~  qgamma((1 - 0.95)/2, rest_count) / (rest_pt / 366) * 1000,
      acute == 1 ~ qgamma((1 - 0.95)/2, rest_count) / (rest_pt / 365) * 1000,
      acute == 0 & (rest_pop == 0 | rest_pop - rest_count <= 0) ~ NA_real_,
      acute == 0 ~ qbeta((1 - 0.95)/2, rest_count, rest_pop - rest_count + 1) * 1000
    ),
    rest_ci_ub = case_when(
      acute == 1 & year %in% c(2012, 2016) ~  qgamma(1 - (1 - 0.95)/2, rest_count + 1) / (rest_pt / 366) * 1000,
      acute == 1 ~ qgamma(1 - (1 - 0.95)/2, rest_count + 1) / (rest_pt / 365) * 1000,
      acute == 0 & (rest_pop == 0 | rest_pop - rest_count <= 0) ~ NA_real_,
      acute == 0 ~ qbeta(1 - (1 - 0.95)/2, rest_count + 1, rest_pop - rest_count) * 1000
    )
  ) %>%
  select(year, indicator, agency, category1, group1, category2, group2,
         rest_count, rest_pop, rest_pt,
         rest_pha_rate, rest_ci_lb, rest_ci_ub)


health_events_combined_pop <- left_join(health_events_combined_pop, rest_pha,
                                        by = c("year", "indicator", "agency", 
                                               "category1", "group1", "category2", "group2"))


### Make suppressed version
health_events_combined_suppressed <- health_events_combined_pop %>%
  mutate_at(vars(rate, ci_lb, ci_ub, rest_pha_rate, rest_ci_lb, rest_ci_ub),
            funs(case_when(
              suppressed == 1 ~ NA_real_,
              acute == 0 & pop_supp_flag == 1 ~ NA_real_,
              TRUE ~ round(., 1)
            ))) %>%
  mutate_at(vars(rest_count, rest_pop), funs(ifelse(suppressed == 1 | pop_supp_flag == 1,
                                                    NA, .))) %>%
  select(indicator:group2, count_supp, pt_days, pop_ever_supp, pop_supp, rate:ci_ub, 
         rest_count, rest_pop, rest_pt, rest_pha_rate, rest_ci_lb, rest_ci_ub,
         suppressed, pop_ever_supp_flag, pop_supp_flag) %>%
  rename(
    count = count_supp, pop_ever = pop_ever_supp, pop = pop_supp,
    count_supp = suppressed, pop_ever_supp = pop_ever_supp_flag,
    pop_supp = pop_supp_flag)


#### WRITE DATA ####
write.xlsx(health_events_combined_suppressed, paste0(housing_path,
                                                     "/OrganizedData/Summaries/PHA_Medicaid_health_events_", 
                                                     Sys.Date(), ".xlsx"),
           sheetName = "conditions")
write.csv(health_events_combined_suppressed, paste0(housing_path,
                                                    "/OrganizedData/Summaries/PHA_Medicaid_health_events_", 
                                                     Sys.Date(), ".csv"),
            row.names = FALSE)


dbWriteTable(db.apde51, name = "pha_mcaid_events_suppressed_load",
             value = health_events_combined_suppressed, overwrite = T,
             field.types = c(acute = "integer",
                             year = "integer",
                             wc_flag = "integer",
                             count = "integer",
                             pt_days = "integer",
                             pop_ever = "integer",
                             pop = "integer",
                             rest_count = "integer",
                             rest_pop = "integer",
                             rest_pt = "integer",
                             count_supp = "integer",
                             pop_ever_supp = "integer",
                             pop_supp = "integer"))



# Unsuppressed
write.xlsx(health_events_combined_pop, paste0(housing_path, 
                                                     "/OrganizedData/Summaries/PHA_Medicaid_health_events_unsuppressed_", 
                                                     Sys.Date(), ".xlsx"),
           sheetName = "conditions")

write.csv(health_events_combined_pop, paste0(housing_path,
                                             "/OrganizedData/Summaries/PHA_Medicaid_health_events_unsuppressed_", 
                                             Sys.Date(), ".csv"))


rm(list = ls(pattern = "tabloop"))
rm(health_events)
gc()


