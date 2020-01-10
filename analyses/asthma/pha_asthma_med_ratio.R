###############################################################################
# Look at asthma medication ratio outcome
#
# Alastair Matheson (PHSKC-APDE)
# 2019-04
#
#
###############################################################################

##### Set up global parameter and call in libraries #####
options(max.print = 350, tibble.print_max = 30, scipen = 999, warning.length = 5000,
        knitr.kable.NA = '')

library(odbc) # Used to connect to SQL server
library(housing) # contains many useful functions for analyzing housing/Medicaid data
library(lubridate) # Used to manipulate dates
library(tidyverse) # Used to manipulate data
library(glue) # Better SQL and messages
library(claims) # Used to aggregate data
library(spdep) # Use this to identify neighbors
library(SpatialEpi) # Calculate expected number of cases
library(INLA)
library(sf) # Use this to read in shape files


##### Connect to the SQL servers #####
db_apde <- dbConnect(odbc(), "PH_APDEStore51")
db_claims <- dbConnect(odbc(), "PHClaims51")

dashh_2_path <- "C:/Users/mathesal/King County/Laurent, Amy - DASHH-Medicare/Deep Dive"


#### BRING IN RELEVANT DATA ####
# Find the most recent month we have enrollment summaries for
# Comes in as year-month
max_month <- unlist(dbGetQuery(db_claims, "SELECT MAX(year_month) FROM stage.perf_enroll_denom"))
# Now find last day of the month for going forward a month then back a day
max_month <- as.Date(parse_date_time(max_month, "Ym") %m+% months(1) - days(1))

# Set up quarters to run over
months_list <- as.list(seq(as.Date("2013-01-01"), as.Date(max_month) + 1, by = "year") - 1)


### Linked housing/Medicaid data
# Warning: pulling in multiple years takes several minutes and yields >3.7 million rows
housing <- bind_rows(lapply(months_list, function(x) {
  year_end <- as.character(x)
  year_start <- as.character(as.Date(x + days(1) - years(1)))
  
  sql_temp <- glue_sql(
    "SELECT pid2, startdate_c, enddate_c, enroll_type
    enroll_type, id_mcaid, dob_c, race_c, hisp_c, ethn_c, gender_c, 
    age12, age13, age14, age15, age16, age17, age18, 
    agency_new, subsidy_type, operator_type, vouch_type_final, 
    zip_c, kc_area_h, portfolio_final, dual_elig_m,
    pt12, pt13, pt14, pt15, pt16, pt17, pt18, 
    length12, length13, length14, length15, length16, length17, length18, 
    {year_end} AS end_period 
    FROM stage.mcaid_pha
    WHERE startdate_c <= {year_end} AND enddate_c >= {year_start}",
    .con = db_apde)
  message(glue("Collecting data for the year through to {year_end}"))
  dbGetQuery(db_apde, sql_temp)
}))


### People who met the inclusion criteria (11+ months of Medicaid, ages 5-64, non-dual)
elig_pop <- bind_rows(lapply(months_list, function(x) {
  date_month <- paste0(str_sub(x, 1, 4), str_sub(x, 6, 7))
  
  sql_temp <- glue::glue_sql("SELECT a.id_mcaid, a.year_month, b.end_month, a.end_month_age, 
                             c.dob, c.gender_me, c.race_eth_me, c.lang_max, 'enroll_flag' = 1  
                             FROM
                             (SELECT id_mcaid, year_month, end_month_age 
                               FROM [PHClaims].[stage].[perf_enroll_denom]
                               WHERE full_benefit_t_12_m >= 11 AND dual_t_12_m = 0 AND 
                               end_month_age >= 5 AND end_month_age < 65 AND 
                               year_month = {date_month}) a 
                             LEFT JOIN
                             (SELECT year_month, end_month, beg_measure_year_month 
                               FROM [ref].[perf_year_month]) b 
                             ON a.year_month = b.year_month
                             LEFT JOIN
                             (SELECT id_mcaid, dob, gender_me, race_eth_me, lang_max
                               FROM final.mcaid_elig_demo) c
                             ON a.id_mcaid = c.id_mcaid",
                             .con = db_claims)
  
  message(glue("Collecting data for the year through to {date_month}"))
  dbGetQuery(db_claims, sql_temp)
}))


### Asthma medication ratio data
# NOTE THAT THIS CODE WILL NEED TO BE UPDATED ONCE THE NEW ETL PROCESS IS RUN
amr_1_year <- dbGetQuery(db_claims,
                         "SELECT end_year_month, id_mcaid, numerator, denominator
                         FROM stage.mcaid_perf_measure
                         WHERE measure_id = 20 --AND end_year_month = '201712'")


amr <- dbGetQuery(db_claims,
                  "SELECT end_year_month, id_mcaid, numerator, denominator
                  FROM stage.mcaid_perf_measure
                  WHERE measure_id = 19 --AND end_year_month = '201712'")


### ZIP data
zip <- dbGetQuery(db_claims,
                  "SELECT zip_code as zip_c, city 
                  FROM ref.apcd_zip WHERE county_name = 'King' and state = 'WA'")


#### BRING DATA TOGETHER AND PROCESS ####
housing_amr <- inner_join(housing, elig_pop, 
                          by = c("id_mcaid", 
                                 "end_period" = "end_month")) %>%
  left_join(., amr, by = c("id_mcaid",
                           "year_month" = "end_year_month")) %>%
  left_join(., amr_1_year, by = c("id_mcaid",
                                  "year_month" = "end_year_month")) %>%
  rename(denominator = denominator.x,
         numerator = numerator.x,
         denominator_1yr = denominator.y,
         numerator_1yr = numerator.y) %>%
  replace_na(., list(denominator = 0, numerator = 0, 
                     denominator_1yr = 0, numerator_1yr = 0))

housing_amr <- right_join(housing_amr, zip, by = "zip_c")



#### FUNCTIONS ####
chronic_pop_f <- function(df, year = 17, test = F) {
  
  agex_quo <- rlang::sym(paste0("age", quo_name(year), "_grp"))
  lengthx_quo <- rlang::sym(paste0("length", quo_name(year), "_grp"))
  ptx_quo <- rlang::sym(paste0("pt", quo_name(year)))
  
  year_full <- as.numeric(paste0(20, year))
  
  
  # Make new generic vars so data table works
  # Currently only using ptx, expand to others if more code converted
  df <- df %>% 
    mutate(ptx = !!ptx_quo,
           agex = !!agex_quo,
           lengthx = !!lengthx_quo)
  
  
  # Count a person if they were in that group at any point in the period
  # Also count person time accrued in each group (in days)
  df <- setDT(df)
  
  pt <- df[!is.na(ptx), .(pid2, agency_new, enroll_type, agex, gender_c,
                          ethn_c, dual_elig_m, vouch_type_final, subsidy_type,
                          operator_type, portfolio_final, lengthx, zip_c, ptx)]
  pt <- pt[, pt_days := sum(ptx),
           by = .(pid2, agency_new, enroll_type, agex, gender_c,
                  ethn_c, dual_elig_m, vouch_type_final, subsidy_type,
                  operator_type, portfolio_final, lengthx, zip_c)]
  
  pt[, ptx := NULL]
  
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
  df_pop <- merge(df, pt, by = c("pid2", "agency_new", "enroll_type", "agex", "gender_c", 
                                 "ethn_c", "dual_elig_m", "vouch_type_final", "subsidy_type", 
                                 "operator_type", "portfolio_final", "lengthx", "zip_c"))
  setorder(df_pop, pid2, agency_new, enroll_type, -ptx)
  
  # Find the row with the most person-time in each group
  # (ties will be broken by whatever other ordering exists)
  pop <- copy(df_pop)
  pop <- pop[!is.na(ptx)]
  # pop <- df_pop[ptx > 0 & !is.na(ptx)]
  pop <- pop[pop[, .I[1], by = .(pid2, agency_new, enroll_type)]$V1]
  # pop <- pop[order(pid2, agency_new, enroll_type, -ptx)]
  # pop[pop[, .I[1], by = .(pid2, agency_new, enroll_type)]$V1]
  
  # Number of agencies, should only be one row per possibility below
  pop[, agency_count := NA_integer_]
  pop[, agency_count := ifelse(agency_new == "KCHA" & enroll_type == "h", 0L, agency_count)]
  pop[, agency_count := ifelse(agency_new == "SHA" & enroll_type == "h", 0L, agency_count)]
  pop[, agency_count := ifelse(agency_new == "KCHA" & enroll_type == "b", 1L, agency_count)]
  pop[, agency_count := ifelse(agency_new == "SHA" & enroll_type == "b", 2L, agency_count)]
  pop[, agency_count := ifelse(agency_new == "Non-PHA", 4L, agency_count)]
  
  
  # Count up permutations of agency and enrollment
  pop[, agency_sum := sum(agency_count, na.rm = T), by = pid2]
  
  # Throw up a list of IDs to check out
  if (test == T) {
    temp <- pop %>% sample_n(5)
    print(temp)
  }
  
  # Filter so only rows meeting the rules above are kept
  pop <- pop[(agency_sum == 4 & agency_count == 4) | 
               (agency_sum == 5 & agency_count == 1) |
               (agency_sum == 6 & agency_count == 2) |
               (agency_sum == 7 & agency_count == 1) | 
               (agency_sum == 7 & agency_count == 2) |
               (agency_sum == 1 & agency_count == 1) |
               (agency_sum == 2 & agency_count == 2) |
               agency_sum == 3 |
               agency_sum == 0]
  
  pop <- pop[, .(pid2, id_mcaid, agency_new, enroll_type, dual_elig_m, agex, 
                 gender_c, ethn_c, vouch_type_final, subsidy_type, operator_type, 
                 portfolio_final, lengthx, zip_c, age17, city)]
  setnames(pop, old = c("agex", "lengthx"), new = c("age", "length"))
  pop[, year := as.numeric(paste0("20", year))]
  
  return(pop)
} 

agecode_f <- function(df, x = age17) {
  col <- enquo(x)
  varname <- paste(quo_name(col), "grp", sep = "_")
  df %>%
    mutate(!!varname := case_when(
      (!!col) < 44.99 ~ "<45",
      (!!col) >= 45 ~ "45+",
      is.na((!!col)) ~ "Unknown")
    )
}
lencode_f <- function(df, x = length17, agency = agency_new) {
  col <- enquo(x)
  agency <- enquo(agency)
  
  varname <- paste(quo_name(col), "grp", sep = "_")
  df %>%
    mutate(!!varname := case_when(
      (!!col) < 3 ~ "<3 years",
      between((!!col), 3, 5.99) ~ "3-<6 years",
      (!!col) >= 6 ~ "6+ years",
      is.na((!!agency)) | (!!agency) == "Non-PHA" | (!!agency) == 0 ~ "Non-PHA",
      is.na((!!col)) ~ "Unknown")
    )
}

# Allocate people into one bucket for making chronic disease denominators
eventcount_chronic_f <- function(df_chronic = chronic, df_pop = chronic_pop,
                                 condition = NULL, year = 17) {
  
  yr_chk <- as.numeric(paste0("20", year))
  year_start = as.Date(paste0("20", year, "-01-01"), origin = "1970-01-01")
  year_end = as.Date(paste0("20", year, "-12-31"), origin = "1970-01-01")
  
  condition_quo <- enquo(condition)
  
  # Filter to only include people with the condition in that year
  cond <- df_chronic %>%
    filter(!!condition_quo == 1 & from_date <= year_end & to_date >= year_start) %>%
    distinct(id) %>%
    mutate(condition = 1)
  
  
  df_pop <- df_pop %>% filter(year == yr_chk)
  
  ### Join pop and condition data to summarise
  output <- left_join(df_pop, cond, by =  c("id_mcaid"="id"))
  output <- output %>% mutate(condition = if_else(is.na(condition), 0, 1))
  
  output <- output %>%
    group_by( id_mcaid,agency_new, enroll_type, dual_elig_m, age,
              gender_c, ethn_c, vouch_type_final, subsidy_type, operator_type,
              portfolio_final, length, zip_c) %>%
    summarise(count := sum(condition)) %>%
    ungroup() %>%
    select(id_mcaid, agency_new, enroll_type, dual_elig_m, age,
           gender_c, ethn_c, vouch_type_final, subsidy_type, operator_type,
           portfolio_final, length, zip_c,
           count) %>%
    rename(agency = agency_new,
           dual = dual_elig_m,
           gender = gender_c,
           ethn = ethn_c,
           portfolio = portfolio_final,
           zip = zip_c)
  
  return(output)
  
}




### Allocate people to a housing category based on pt in that year
# Can then drop columns not currently being used
housing_amr_cat <- bind_rows(lapply(months_list, function(x) {
  
  pt_var <- quo_name(paste0("pt", str_sub(x, 3, 4)))
  age_var <- quo_name(paste0("age", str_sub(x, 3, 4)))
  length_var <- quo_name(paste0("length", str_sub(x, 3, 4)))
  
  result <- housing_amr %>%
    filter(end_period == x) %>%
    arrange(pid2, desc(!!sym(pt_var))) %>%
    group_by(pid2) %>%
    slice(1) %>%
    ungroup() %>%
    mutate(
      age_grp = case_when(
        !!sym(age_var) >= 5 & !!sym(age_var) <= 11.999 ~ "5-11",
        !!sym(age_var) >= 12 & !!sym(age_var) <= 18.999 ~ "12-18",
        !!sym(age_var) >= 19 & !!sym(age_var) <= 30.999 ~ "19-30",
        !!sym(age_var) >= 31 & !!sym(age_var) <= 64.999 ~ "31+",
        TRUE ~ NA_character_
      ),
      length_grp = case_when(
        is.na(!!sym(length_var)) ~ NA_character_,
        !!sym(length_var) >= 0 & !!sym(length_var) < 2.999 ~ "<3 years",
        !!sym(length_var) >= 3 & !!sym(length_var) < 5.999 ~ "3-6 years",
        !!sym(length_var) >= 6 ~ "6+ years",
        TRUE ~ NA_character_
      ),
      lang_grp = case_when(
        is.na(lang_max) ~ NA_character_,
        lang_max == "ENGLISH" ~ "ENGLISH",
        lang_max == "SPANISH; CASTILIAN" ~ "SPANISH",
        lang_max == "VIETNAMESE" ~ "VIETNAMESE",
        lang_max == "CHINESE" ~ "CHINESE",
        lang_max == "SOMALI" ~ "SOMALI",
        lang_max == "RUSSIAN" ~ "RUSSIAN",
        lang_max == "ARABIC" ~ "ARABIC",
        lang_max == "KOREAN" ~ "KOREAN",
        lang_max == "UKRAINIAN" ~ "UKRAINIAN",
        lang_max == "AMHARIC" ~ "AMHARIC",
        lang_max == "BURMESE" ~ "BURMESE",
        lang_max == "TIGRINYA" ~ "TIGRINYA",
        TRUE ~ "OTHER"
      )
    ) %>%
    select(end_period, pid2, age_grp, gender_c, 
           ethn_c, lang_grp, length_grp, 
           agency_new, subsidy_type, operator_type, vouch_type_final, 
           zip_c, portfolio_final,
           numerator, denominator, denominator_1yr, numerator_1yr)
  return(result)
}))


#### ANALYZE DATA ####
#### Tableau-ready output ####
### Look at all groups combined
asthma_cnt_combined <- tabloop_f(df = housing_amr_cat,
                                 dcount = list_var(pid2),
                                 sum = list_var(denominator, numerator,
                                                denominator_1yr, numerator_1yr),
                                 fixed = list_var(end_period),
                                 loop = list_var(age_grp, gender_c, 
                                                 ethn_c, length_grp, lang_grp,
                                                 agency_new, subsidy_type,
                                                 operator_type,
                                                 vouch_type_final,
                                                 zip_c,
                                                 portfolio_final)) %>%
  mutate(agency = "Combined")


### Look at all groups combined
asthma_cnt_agency <- tabloop_f(df = housing_amr_cat,
                               dcount = list_var(pid2),
                               sum = list_var(denominator, numerator,
                                              denominator_1yr, numerator_1yr),
                               fixed = list_var(end_period, agency_new),
                               loop = list_var(age_grp, gender_c, 
                                               ethn_c, length_grp, lang_grp,
                                               subsidy_type,
                                               operator_type,
                                               vouch_type_final,
                                               zip_c,
                                               portfolio_final)) %>%
  rename(agency = agency_new)


### Bring together and calculate proportions
asthma_cnt <- bind_rows(asthma_cnt_combined, asthma_cnt_agency) %>%
  mutate(# See what proportion of the pop had persistent asthma
    asthma_pct = round(denominator_sum / pid2_dcount, 4),
    asthma_pct_lb = purrr::map2_dbl(denominator_sum, pid2_dcount,
                                    .f = function (a, b)
                                      if (b > 0) {round(binom.test(a, b)$conf.int[1], 4)} 
                                    else {NA}),
    asthma_pct_ub = purrr::map2_dbl(denominator_sum, pid2_dcount,
                                    .f = function (a, b)
                                      if (b > 0) {round(binom.test(a, b)$conf.int[2], 4)} 
                                    else {NA}),
    # See what proportion with persistent asthma managed it
    amr_pct = ifelse(denominator_sum > 0, round(numerator_sum / denominator_sum, 4), NA),
    amr_pct_lb = purrr::map2_dbl(numerator_sum, denominator_sum,
                                 .f = function (a, b)
                                   if (b > 0) {round(binom.test(a, b)$conf.int[1], 4)} 
                                 else {NA}),
    amr_pct_ub = purrr::map2_dbl(numerator_sum, denominator_sum,
                                 .f = function (a, b)
                                   if (b > 0) {round(binom.test(a, b)$conf.int[2], 4)} 
                                 else {NA}),
    # See what proportion of the pop had asthma in a single year
    asthma_1yr_pct = round(denominator_1yr_sum / pid2_dcount, 4),
    asthma_1yr_pct_lb = purrr::map2_dbl(denominator_1yr_sum, pid2_dcount,
                                        .f = function (a, b)
                                          if (b > 0) {round(binom.test(a, b)$conf.int[1], 4)} 
                                        else {NA}),
    asthma_1yr_pct_ub = purrr::map2_dbl(denominator_1yr_sum, pid2_dcount,
                                        .f = function (a, b)
                                          if (b > 0) {round(binom.test(a, b)$conf.int[2], 4)} 
                                        else {NA}),
    # See what proportion with asthma in a single year managed it
    amr_1yr_pct = ifelse(denominator_1yr_sum > 0, round(numerator_1yr_sum / denominator_1yr_sum, 4), NA),
    amr_1yr_pct_lb = purrr::map2_dbl(numerator_1yr_sum, denominator_1yr_sum,
                                     .f = function (a, b)
                                       if (b > 0) {round(binom.test(a, b)$conf.int[1], 4)} 
                                     else {NA}),
    amr_1yr_pct_ub = purrr::map2_dbl(numerator_1yr_sum, denominator_1yr_sum,
                                     .f = function (a, b)
                                       if (b > 0) {round(binom.test(a, b)$conf.int[2], 4)} 
                                     else {NA})
  ) %>%
  rename(cat1 = group_cat, cat1_group = group, population = pid2_dcount,
         asthma = denominator_sum, amr_50 = numerator_sum,
         asthma_1yr = denominator_1yr_sum, amr_50_1yr = numerator_1yr_sum) %>%
  select(end_period, agency, cat1, cat1_group, population, asthma, amr_50,
         asthma_pct, asthma_pct_lb, asthma_pct_ub, 
         amr_pct, amr_pct_lb, amr_pct_ub,
         asthma_1yr, amr_50_1yr,
         asthma_1yr_pct, asthma_1yr_pct_lb, asthma_1yr_pct_ub,
         amr_1yr_pct, amr_1yr_pct_lb, amr_1yr_pct_ub) %>%
  # Fix up category names
  mutate(cat1 = case_when(
    cat1 == "age_grp" ~ "Age group",
    cat1 == "agency_new" ~ "Agency",
    cat1 == "ethn_c" ~ "Race/ethnicity",
    cat1 == "gender_c" ~ "Gender",
    cat1 == "lang_grp" ~ "Language",
    cat1 == "length_grp" ~ "Length of time in housing",
    cat1 == "operator_type" ~ "Operator",
    cat1 == "portfolio_final" ~ "Portfolio",
    cat1 == "subsidy_type" ~ "Subsidy type",
    cat1 == "vouch_type_final" ~ "Voucher type",
    cat1 == "zip_c" ~ "ZIP",
    TRUE ~ cat1)) %>%
  # Remove annoying rows
  filter(!(cat1 %in% c("Age group", "Language") & is.na(cat1_group)) &
           !(cat1 == "ZIP" & cat1_group %in% c("0", "8261")) &
           !(cat1 %in% c("Operator", "Portfolio", "Voucher type") & cat1_group == ""))


### Add suppression flags
asthma_cnt_supp <- asthma_cnt %>%
  mutate(population_supp = ifelse(population < 11, 1, 0),
         asthma_supp = ifelse(asthma < 11, 1, 0),
         amr_50_supp = ifelse(amr_50 < 11, 1, 0),
         asthma_1yr_supp = ifelse(asthma_1yr < 11, 1, 0),
         amr_50_1yr_supp = ifelse(amr_50_1yr < 11, 1, 0))

### Check to see if the overall/combined group needs to be suppressed
# This is to prevent back calculating a suppressed number
supp_check_pop <- asthma_cnt_supp %>%
  filter(population_supp == 1) %>%
  group_by(end_period, cat1, cat1_group) %>%
  summarise(count = n()) %>% ungroup() %>%
  mutate(population_supp2 = ifelse(count == 1, 1, 0)) %>%
  select(end_period, cat1, cat1_group, population_supp2)

supp_check_asthma <- asthma_cnt_supp %>%
  filter(asthma_supp == 1) %>%
  group_by(end_period, cat1, cat1_group) %>%
  summarise(count = n()) %>% ungroup() %>%
  mutate(asthma_supp2 = ifelse(count == 1, 1, 0)) %>%
  select(end_period, cat1, cat1_group, asthma_supp2)

supp_check_amr <- asthma_cnt_supp %>%
  filter(amr_50_supp == 1) %>%
  group_by(end_period, cat1, cat1_group) %>%
  summarise(count = n()) %>% ungroup() %>%
  mutate(amr_50_supp2 = ifelse(count == 1, 1, 0)) %>%
  select(end_period, cat1, cat1_group, amr_50_supp2)

supp_check_asthma_1yr <- asthma_cnt_supp %>%
  filter(asthma_1yr_supp == 1) %>%
  group_by(end_period, cat1, cat1_group) %>%
  summarise(count = n()) %>% ungroup() %>%
  mutate(asthma_1yr_supp2 = ifelse(count == 1, 1, 0)) %>%
  select(end_period, cat1, cat1_group, asthma_1yr_supp2)

supp_check_amr_1yr <- asthma_cnt_supp %>%
  filter(amr_50_1yr_supp == 1) %>%
  group_by(end_period, cat1, cat1_group) %>%
  summarise(count = n()) %>% ungroup() %>%
  mutate(amr_50_1yr_supp2 = ifelse(count == 1, 1, 0)) %>%
  select(end_period, cat1, cat1_group, amr_50_1yr_supp2)


# Join back and suppress rows
asthma_cnt_supp <- list(asthma_cnt_supp, supp_check_pop, supp_check_asthma, supp_check_amr,
                        supp_check_asthma_1yr, supp_check_amr_1yr) %>%
  reduce(left_join, by = c("end_period" = "end_period", "cat1" = "cat1", 
                           "cat1_group" = "cat1_group")) %>%
  mutate_at(vars(population_supp2, asthma_supp2, amr_50_supp2,
                 asthma_1yr_supp2, amr_50_1yr_supp2),
            funs(ifelse(is.na(.), 0, .)))


asthma_cnt_supp <- asthma_cnt_supp %>%
  mutate(
    population = case_when(
      population_supp == 1 ~ NA_real_,
      population_supp2 == 1 & agency == "Combined" ~ NA_real_,
      TRUE ~ population)) %>%
  mutate_at(vars(asthma, amr_50, asthma_pct,
                 asthma_pct_lb, asthma_pct_ub, amr_pct,
                 amr_pct_lb, amr_pct_ub),
            funs(case_when(
              asthma_supp == 1 | is.na(population) ~ NA_real_,
              (asthma_supp2 == 1 | is.na(population)) & agency == "Combined" ~ NA_real_,
              TRUE ~ .))) %>%
  mutate_at(vars(asthma_1yr, amr_50_1yr,
                 asthma_1yr_pct, asthma_1yr_pct_lb, asthma_1yr_pct_ub,
                 amr_1yr_pct, amr_1yr_pct_lb, amr_1yr_pct_ub),
            funs(case_when(
              asthma_1yr_supp == 1 | is.na(population) ~ NA_real_,
              (asthma_1yr_supp2 == 1 | is.na(population)) & agency == "Combined" ~ NA_real_,
              TRUE ~ .))) %>%
  mutate_at(vars(amr_50, amr_pct, amr_pct_lb, amr_pct_ub),
            funs(case_when(
              amr_50_supp == 1 | is.na(asthma) ~ NA_real_,
              (amr_50_supp2 == 1 | is.na(asthma)) & agency == "Combined" ~ NA_real_,
              TRUE ~ .))) %>%
  mutate_at(vars(amr_50_1yr, amr_1yr_pct, amr_1yr_pct_lb, amr_1yr_pct_ub),
            funs(case_when(
              amr_50_1yr_supp == 1 | is.na(asthma_1yr) ~ NA_real_,
              (amr_50_1yr_supp2 == 1 | is.na(asthma_1yr)) & agency == "Combined" ~ NA_real_,
              TRUE ~ .)))


### Save for Tableau
write.csv(asthma_cnt_supp, paste0(dashh_2_path, 
                                  "/persistent_asthma_", Sys.Date(), ".csv"),
          row.names = F)







#### Regression ####
# Restrict to 2017 and recode
amr_regression_df <- housing_amr_cat %>%
  filter(end_period == "2017-12-31") %>%
  mutate(pha = ifelse(agency_new == "Non-PHA", 0, 1),
         age_grp = factor(age_grp,
                          levels = c("5-11", "12-18", "19-30", "31-50", "51-64"),
                          labels = c("5-11", "12-18", "19-30", "31-50", "51-64")),
         length_grp = factor(length_grp,
                             levels = c("<3 years", "3-6 years", "6+ years"),
                             labels = c("<3 years", "3-6 years", "6+ years"))
  )

### Compare asthma among PHA to non-PHA
asthma_overall <- glm(denominator_1yr ~ pha + age_grp + gender_c + ethn_c,
                      data = amr_regression_df,
                      family = "binomial")

summary(asthma_overall)

# Summarise output using broom package
asthma_overall_output <- tidy(asthma_overall, conf.int = T, exponentiate = T)
asthma_overall_output <- asthma_overall_output %>%
  mutate(model = rep("Asthma comparing PHA/non-PHA", nrow(asthma_overall_output))) %>%
  select(model, term, estimate, std.error, p.value, conf.low, conf.high)

asthma_overall_output_wald <- data.frame(
  model = rep("Asthma comparing PHA/non-PHA", 3),
  term = c("Age", "Gender", "Race/ethnicity"),
  chi2 = c(
    wald.test(b = coef(asthma_overall), Sigma = vcov(asthma_overall), Terms = 3:6)$result$chi2[3],
    wald.test(b = coef(asthma_overall), Sigma = vcov(asthma_overall), Terms = 7:8)$result$chi2[3],
    wald.test(b = coef(asthma_overall), Sigma = vcov(asthma_overall), Terms = 9:14)$result$chi2[3]
  )
)

### Compare asthma within PHAs in public housing
asthma_pha_hard <- glm(denominator_1yr ~ agency_new + age_grp + gender_c + ethn_c + length_grp + operator_type,
                       data = filter(amr_regression_df, agency_new != "Non-PHA" & subsidy_type == "HARD UNIT" &
                                       operator_type != ""),
                       family = "binomial")

summary(asthma_pha_hard)
asthma_pha_hard_output <- tidy(asthma_pha_hard, conf.int = T, exponentiate = T)
asthma_pha_hard_output <- asthma_pha_hard_output %>%
  mutate(model = rep("Asthma in PHA hard units", nrow(asthma_pha_hard_output))) %>%
  select(model, term, estimate, std.error, p.value, conf.low, conf.high)

asthma_pha_hard_output_wald <- data.frame(
  model = rep("Asthma in PHA hard units", 4),
  term = c("Age", "Gender", "Race/ethnicity", "Length of time"),
  chi2 = c(
    wald.test(b = coef(asthma_pha_hard), Sigma = vcov(asthma_pha_hard), Terms = 3:6)$result$chi2[3],
    wald.test(b = coef(asthma_pha_hard), Sigma = vcov(asthma_pha_hard), Terms = 7:8)$result$chi2[3],
    wald.test(b = coef(asthma_pha_hard), Sigma = vcov(asthma_pha_hard), Terms = 9:14)$result$chi2[3],
    wald.test(b = coef(asthma_pha_hard), Sigma = vcov(asthma_pha_hard), Terms = 15:16)$result$chi2[3]
  )
)


### Compare asthma within PHAs in public housing
asthma_pha_soft <- glm(denominator_1yr ~ agency_new + age_grp + gender_c + ethn_c + length_grp + vouch_type_final,
                       data = filter(amr_regression_df, agency_new != "Non-PHA" & subsidy_type == "TENANT BASED/SOFT UNIT"),
                       family = "binomial")

summary(asthma_pha_soft)

asthma_pha_soft_output <- tidy(asthma_pha_soft, conf.int = T, exponentiate = T)
asthma_pha_soft_output <- asthma_pha_soft_output %>%
  mutate(model = rep("Asthma in PHA soft units", nrow(asthma_pha_soft_output))) %>%
  select(model, term, estimate, std.error, p.value, conf.low, conf.high)

asthma_pha_soft_output_wald <- data.frame(
  model = rep("Asthma in PHA soft units", 5),
  term = c("Age", "Gender", "Race/ethnicity", "Time in housing", "Voucher type"),
  chi2 = c(
    wald.test(b = coef(asthma_pha_soft), Sigma = vcov(asthma_pha_soft), Terms = 3:6)$result$chi2[3],
    wald.test(b = coef(asthma_pha_soft), Sigma = vcov(asthma_pha_soft), Terms = 7:8)$result$chi2[3],
    wald.test(b = coef(asthma_pha_soft), Sigma = vcov(asthma_pha_soft), Terms = 9:14)$result$chi2[3],
    wald.test(b = coef(asthma_pha_soft), Sigma = vcov(asthma_pha_soft), Terms = 15:16)$result$chi2[3],
    wald.test(b = coef(asthma_pha_soft), Sigma = vcov(asthma_pha_soft), Terms = 17:21)$result$chi2[3]
  )
)


### Compare AMR among PHA to non-PHA
amr_overall <- glm(numerator_1yr ~ pha + age_grp + gender_c + ethn_c,
                   data = filter(amr_regression_df, denominator_1yr == 1),
                   family = "binomial")

summary(amr_overall)

# Summarise output using broom package
amr_overall_output <- tidy(amr_overall, conf.int = T, exponentiate = T)
amr_overall_output <- amr_overall_output %>%
  mutate(model = rep("AMR comparing PHA/non-PHA", nrow(amr_overall_output))) %>%
  select(model, term, estimate, std.error, p.value, conf.low, conf.high)

amr_overall_output_wald <- data.frame(
  model = rep("AMR comparing PHA/non-PHA", 3),
  term = c("Age", "Gender", "Race/ethnicity"),
  chi2 = c(
    wald.test(b = coef(amr_overall), Sigma = vcov(amr_overall), Terms = 3:6)$result$chi2[3],
    wald.test(b = coef(amr_overall), Sigma = vcov(amr_overall), Terms = 7:8)$result$chi2[3],
    wald.test(b = coef(amr_overall), Sigma = vcov(amr_overall), Terms = 9:14)$result$chi2[3]
  )
)

### Compare AMR within PHAs in public housing
amr_pha_hard <- glm(numerator_1yr ~ agency_new + age_grp + gender_c + ethn_c + length_grp + operator_type,
                    data = filter(amr_regression_df, agency_new != "Non-PHA" & subsidy_type == "HARD UNIT" &
                                    operator_type != "" & denominator_1yr == 1),
                    family = "binomial")

summary(amr_pha_hard)
amr_pha_hard_output <- tidy(amr_pha_hard, conf.int = T, exponentiate = T)
amr_pha_hard_output <- amr_pha_hard_output %>%
  mutate(model = rep("AMR in PHA hard units", nrow(amr_pha_hard_output))) %>%
  select(model, term, estimate, std.error, p.value, conf.low, conf.high)

amr_pha_hard_output_wald <- data.frame(
  model = rep("AMR in PHA hard units", 4),
  term = c("Age", "Gender", "Race/ethnicity", "Time in housing"),
  chi2 = c(
    wald.test(b = coef(amr_pha_hard), Sigma = vcov(amr_pha_hard), Terms = 3:6)$result$chi2[3],
    wald.test(b = coef(amr_pha_hard), Sigma = vcov(amr_pha_hard), Terms = 7:8)$result$chi2[3],
    wald.test(b = coef(amr_pha_hard), Sigma = vcov(amr_pha_hard), Terms = 9:14)$result$chi2[3],
    wald.test(b = coef(amr_pha_hard), Sigma = vcov(amr_pha_hard), Terms = 15:16)$result$chi2[3]
  )
)


### Compare asthma within PHAs in public housing
# Exclude small number with multiple gender as all fall in same AMR category
amr_pha_soft <- glm(numerator_1yr ~ agency_new + age_grp + gender_c + ethn_c + length_grp + vouch_type_final,
                    data = filter(amr_regression_df, agency_new != "Non-PHA" & 
                                    subsidy_type == "TENANT BASED/SOFT UNIT" & denominator_1yr == 1 & 
                                    gender_c != "Multiple"),
                    family = "binomial")

summary(amr_pha_soft)

amr_pha_soft_output <- tidy(amr_pha_soft, conf.int = T, exponentiate = T)
amr_pha_soft_output <- amr_pha_soft_output %>%
  mutate(model = rep("AMR in PHA soft units", nrow(amr_pha_soft_output))) %>%
  select(model, term, estimate, std.error, p.value, conf.low, conf.high)

amr_pha_soft_output_wald <- data.frame(
  model = rep("AMR in PHA soft units", 5),
  term = c("Age", "Gender", "Race/ethnicity", "Time in housing", "Voucher type"),
  chi2 = c(
    wald.test(b = coef(amr_pha_soft), Sigma = vcov(amr_pha_soft), Terms = 3:6)$result$chi2[3],
    wald.test(b = coef(amr_pha_soft), Sigma = vcov(amr_pha_soft), Terms = 7:7)$result$chi2[3],
    wald.test(b = coef(amr_pha_soft), Sigma = vcov(amr_pha_soft), Terms = 8:15)$result$chi2[3],
    wald.test(b = coef(amr_pha_soft), Sigma = vcov(amr_pha_soft), Terms = 16:17)$result$chi2[3],
    wald.test(b = coef(amr_pha_soft), Sigma = vcov(amr_pha_soft), Terms = 17:20)$result$chi2[3]
  )
)


### Combine together
model_output <- bind_rows(asthma_overall_output, asthma_pha_hard_output, asthma_pha_soft_output,
                          amr_overall_output, amr_pha_hard_output, amr_pha_soft_output) %>%
  mutate_if(is.numeric, round, 4)

model_output_wald <- bind_rows(asthma_overall_output_wald, asthma_pha_hard_output_wald, 
                               asthma_pha_soft_output_wald,
                               amr_overall_output_wald, amr_pha_hard_output_wald, 
                               amr_pha_soft_output_wald) %>%
  mutate(chi2 = round(chi2, 4))

model_output_overall <- bind_rows(model_output, model_output_wald) %>%
  mutate(term = str_replace(term, "ethn_c", "Race/ethnicity: "),
         term = str_replace(term, "gender_c", "Gender: "),
         term = str_replace(term, "age_grp", "Age: "),
         term = str_replace(term, "length_grp", "Time in housing: "),
         term = str_replace(term, "agency_new", "PHA: "),
         term = str_replace(term, "operator_type", "Operator type: "),
         term = str_replace(term, "vouch_type_final", "Voucher type: ")) %>%
  arrange(model, term) %>%
  rename(odds_ratio = estimate, SE = std.error, p = p.value,
         ci_lb = conf.low, ci_ub = conf.high) %>%
  mutate_at(vars(p, chi2),
            funs(case_when(
              . == 0 ~ as.character("<0.0001"),
              TRUE ~ as.character(.)
            )
            ))


### Write out table
options(knitr.kable.NA = '')
temp <- model_output_overall %>% 
  filter(model == "Asthma comparing PHA/non-PHA") %>%
  select(-model) %>%
  mutate(p = case_when(
    is.na(p) ~ NA_character_,
    TRUE ~ cell_spec(p, "latex", bold = ifelse(as.numeric(str_replace(p, "<", "")) < 0.05, T, F))
  ),
  chi2 = case_when(
    is.na(chi2) ~ NA_character_,
    TRUE ~ cell_spec(chi2, "latex", bold = ifelse(as.numeric(str_replace(chi2, "<", "")) < 0.05, T, F))
  )) %>%
  kable(format = "latex", booktabs = T, escape = F,
        caption = "Binomial regression model outputs for 1-yr asthma and  AMR") %>%
  collapse_rows(columns = 1, valign = "top") %>%
  footnote(general = "chi2 column shows the output of the Wald test for the entire group")

temp <- gsub("(_)", "\\\\\\1", temp)

render(file.path(getwd(), "analyses/pha_asthma_med_ratio_output.Rmd"), "pdf_document",
       output_file = file.path(dashh_2_path, "Asthma", "pha_asthma_med_ratio_output.pdf"))



