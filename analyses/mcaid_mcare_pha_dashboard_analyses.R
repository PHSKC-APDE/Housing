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
library(claims) # Used to aggregate data
library(glue) # Used to safely make SQL queries


##### Connect to the SQL servers #####
db_apde51 <- dbConnect(odbc(), "PH_APDEStore51")
db_claims51 <- dbConnect(odbc(), "PHClaims51")
db_extractstore51 <- dbConnect(odbc(), "PHExtractStore51")
db_extractstore50 <- dbConnect(odbc(), "PHExtractStore50")

housing_path <- "//phdata01/DROF_DATA/DOH DATA/Housing/Organized_data"


#### BRING IN DATA ####
### Years to look over
years <- seq(2012, 2016)

### Code for mapping field values
demo_codes <- read.csv(text = RCurl::getURL("https://raw.githubusercontent.com/PHSKC-APDE/Housing/pha_2018_data/processing/housing_mcaid%20demo%20codes.csv"), 
                   header = TRUE, stringsAsFactors = FALSE)


### Precalculated calendar year table
# Use stage for now
mcaid_mcare_pha_elig_calyear <- dbGetQuery(db_apde51, "SELECT * FROM stage.mcaid_mcare_pha_elig_calyear")

# Convert to data table for faster results
mcaid_mcare_pha_elig_calyear <- setDT(mcaid_mcare_pha_elig_calyear)
mcaid_mcare_pha_elig_calyear[, ':=' (
  dob = as.Date(dob, origin = "1970-01-01"),
  death_dt = as.Date(death_dt, origin = "1970-01-01"),
  start_housing = as.Date(start_housing, origin = "1970-01-01")
)]



### Bring in acute events (ED visits, hospitalizations, injuries, well child)
acute <- dbGetQuery(db_claims51, 
                    "SELECT id_apde, source_desc, claim_header_id, 
                    first_service_date AS from_date, 
                    last_service_date AS to_date, 
                    claim_type_mcaid_id, claim_type_mcare_id, 
                    ed, ed_bh, ed_avoid_ca, ed_emergent_nyu, ed_nonemergent_nyu,
                    ed_intermediate_nyu, ed_mh_nyu, ed_sud_nyu, ed_alc_nyu, 
                    ed_injury_nyu, ed_unclass_nyu, 
                    inpatient, intent, mechanism, ccs_final_plain_lang
                    FROM final.mcaid_mcare_claim_header
                    WHERE intent IS NOT NULL OR ed = 1 OR inpatient = 1 
                    OR claim_type_mcaid_id = 27 OR claim_type_mcare_id = 27")


# Convert to data table to speed things up
acute <- setDT(acute)
acute[, ':=' (
  from_date = as.Date(from_date, origin = "1970-01-01"),
  to_date = as.Date(to_date, origin = "1970-01-01")
)]


#### FUNCTIONS ####
### Pull in acute data
acute_query_f <- function(type = c("ed", "hosp", "inj")) {
  
  type <- match.arg(type)
  
  if (type == "ed") {
    top_vars <- glue_sql("a.ed ", .con = db_claims51)
    vars <- glue_sql("ed ", .con = db_claims51)
    where <- glue_sql("ed = 1", .con = db_claims51)
  } else if (type == "hosp") {
    top_vars <- glue_sql("a.inpatient ", .con = db_claims51)
    vars <- glue_sql("inpatient ", .con = db_claims51)
    where <- glue_sql("inpatient = 1", .con = db_claims51)
  } else if (type == "inj") {
    top_vars <- glue_sql("a.intent ", .con = db_claims51)
    vars <- glue_sql("intent ", .con = db_claims51)
    where <- glue_sql("intent IS NOT NULL", .con = db_claims51)
  }
  
  sql_query <- glue_sql(
    "SELECT a.id_apde, a.source_desc, a.claim_header_id, a.first_service_date, 
    YEAR(a.first_service_date) AS year, {top_vars}, 
    b.enroll_type, b.pha_agency, b.apde_dual, b.full_benefit, b.pha_voucher, 
    b.pha_subsidy, b.pha_operator, b.pha_portfolio, b.geo_zip, 
    c.dob, c.start_housing, c.gender_me, c.race_eth_me 
    FROM
    (SELECT id_apde, source_desc, claim_header_id, 
      first_service_date, {vars}
      FROM PHClaims.final.mcaid_mcare_claim_header
      WHERE {where}) a
    INNER JOIN
    (SELECT id_apde, from_date, to_date, 
      CASE WHEN mcaid = 0 AND mcare = 0 AND pha = 1 THEN 'h'
      WHEN mcaid = 1 AND mcare = 0 AND pha = 1 THEN 'hmd'
      WHEN mcaid = 0 AND mcare = 1 AND pha = 1 THEN 'hme'
      WHEN mcaid = 1 AND mcare = 0 AND pha = 0 THEN 'md'
      WHEN mcaid = 0 AND mcare = 1 AND pha = 0 THEN 'me'
      WHEN mcaid = 1 AND mcare = 1 AND pha = 0 THEN 'mm'
      WHEN mcaid = 1 AND mcare = 1 AND pha = 1 THEN 'a' END AS enroll_type, 
      pha_agency, apde_dual, full_benefit, pha_voucher,
      pha_subsidy, pha_operator, pha_portfolio, geo_zip
      FROM PH_APDEStore.final.mcaid_mcare_pha_elig_timevar
      WHERE mcaid = 1 OR pha = 1 OR (mcare = 1 AND geo_kc = 1)) b
    ON a.id_apde = b.id_apde AND
    a.first_service_date >= b.from_date AND a.first_service_date <= b.to_date
    LEFT JOIN
    (SELECT id_apde, dob, start_housing, gender_me, race_eth_me
      FROM PH_APDEStore.final.mcaid_mcare_pha_elig_demo) c
    ON COALESCE(a.id_apde, b.id_apde) = c.id_apde",
    .con = db_claims51)
  
  # sql_query <- glue_sql(
  #   "SELECT a.id_apde, a.source_desc, a.claim_header_id, a.first_service_date, 
  #   YEAR(a.first_service_date) AS year, {top_vars}, 
  #   b.enroll_type, b.pha_agency, b.apde_dual, b.full_benefit, b.pha_voucher, 
  #   b.pha_subsidy, b.pha_operator, b.pha_portfolio, b.geo_zip, 
  #   c.dob, c.start_housing, c.gender_me, c.race_eth_me 
  #   FROM
  #   (SELECT id_apde, source_desc, claim_header_id, 
  #     first_service_date, {vars}
  #     FROM PHClaims.final.mcaid_mcare_claim_header
  #     WHERE {where}) a
  #   INNER JOIN
  #   (SELECT id_apde, from_date, to_date, 
  #     CASE WHEN mcaid = 0 AND mcare = 0 AND pha = 1 THEN 'h'
  #     WHEN mcaid = 1 AND mcare = 0 AND pha = 1 THEN 'hmd'
  #     WHEN mcaid = 0 AND mcare = 1 AND pha = 1 THEN 'hme'
  #     WHEN mcaid = 1 AND mcare = 0 AND pha = 0 THEN 'md'
  #     WHEN mcaid = 0 AND mcare = 1 AND pha = 0 THEN 'me'
  #     WHEN mcaid = 1 AND mcare = 1 AND pha = 0 THEN 'mm'
  #     WHEN mcaid = 1 AND mcare = 1 AND pha = 1 THEN 'a' END AS enroll_type, 
  #     pha_agency, apde_dual, full_benefit, pha_voucher,
  #     pha_subsidy, pha_operator, pha_portfolio, geo_zip
  #     FROM PH_APDEStore.final.mcaid_mcare_pha_elig_timevar
  #     WHERE mcaid = 1 OR pha = 1 OR (mcare = 1 AND geo_kc = 1)) b
  #   ON a.id_apde = b.id_apde AND
  #   a.first_service_date >= b.from_date AND a.first_service_date <= b.to_date
  #   LEFT JOIN
  #   (SELECT id_apde, dob, start_housing, gender_me, race_eth_me
  #     FROM PH_APDEStore.final.mcaid_mcare_pha_elig_demo) c
  #   ON COALESCE(a.id_apde, b.id_apde) = c.id_apde",
  #   .con = db_claims51)
  
  output <- dbGetQuery(db_claims51, sql_query) %>%
    # Format vars
    mutate_at(vars(first_service_date, dob), list( ~ as.Date(.))) %>%
    mutate_at(vars(starts_with("pha_")), 
              list( ~ ifelse(enroll_type %in% c("mm", "md", "me"), "Non-PHA", .))) %>%
    # Recode output
    mutate(apde_dual = if_else(apde_dual == 1, "Dual eligible", "Not dual eligible"),
           full_benefit = case_when(
             full_benefit == 1 ~ "Full Medicaid benefits",
             full_benefit == 0 ~ "Not full Medicaid benefits"),
           enroll_type = case_when(
             enroll_type == "h" ~ "Housing only",
             enroll_type == "md" ~ "Medicaid only",
             enroll_type == "me" ~ "Medicare only",
             enroll_type == "mm" ~ "Medicaid and Medicare",
             enroll_type == "hmd" ~ "Housing and Medicaid",
             enroll_type == "hme" ~ "Housing and Medicare",
             enroll_type == "a" ~ "Housing and Medicaid and Medicare"),
           age_yr = floor(interval(start = dob, end = first_service_date) / years(1)),
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
             round(interval(start = start_housing, end = first_service_date) / years(1), 1),
           time_housing = case_when(
             is.na(pha_agency) | pha_agency == "Non-PHA" ~ "Non-PHA",
             time_housing_yr < 3 ~ "<3 years",
             between(time_housing_yr, 3, 5.99) ~ "3 to <6 years",
             time_housing_yr >= 6 ~ "6+ years",
             TRUE ~ "Unknown"),
           pha_operator = ifelse(pha_subsidy == "TENANT BASED/SOFT UNIT", NA_character_, 
                                 pha_operator)
    ) %>%
    rename(agency = pha_agency, dual = apde_dual, gender = gender_me,
           ethn = race_eth_me, )
  
  return(output)
}


### Function to pull in and summarize chronic conditions
eventcount_chronic_f <- function(condition = NULL, year = 2012, cvd = F) {
  
  
  if (cvd == F) {
    where_sql <- glue_sql("ccw_desc = {condition}", .con = db_apde51)
  } else if (cvd == T) {
    where_sql <- glue_sql("ccw_desc IN ('ccw_heart_failure', 'ccw_hypertension',
                          'ccw_ischemic_heart_dis', 'ccw_mi')", .con = db_apde51)
  }
  
  sql_query <- glue_sql(
    "SELECT DISTINCT a.*, ISNULL(b.condition, 0) AS condition 
    FROM
      (SELECT year, id_apde, enroll_type, pha_agency, apde_dual,
        full_benefit, agegrp, gender_me, race_eth_me, time_housing, 
        pha_subsidy, pha_voucher, pha_operator, pha_portfolio, geo_zip 
        FROM PH_APDEStore.stage.mcaid_mcare_pha_elig_calyear
        WHERE [year] = {year} AND enroll_type <> 'h' AND pop = 1) a
    LEFT JOIN
      (SELECT id_apde, 1 AS condition 
        FROM PHClaims.final.mcaid_mcare_claim_ccw 
        WHERE {where_sql} AND from_date <= {paste0(year, '-12-31')} AND 
        to_date >= {paste0(year, '-01-01')}) b
    ON a.id_apde = b.id_apde",
    .con = db_apde51)
  
  output <- dbGetQuery(db_apde51, sql_query)
  
  ### Summarise and rename
  output <- output %>%
    mutate_at(vars(starts_with("pha_")), 
              list( ~ ifelse(enroll_type %in% c("mm", "md", "me"), "Non-PHA", .))) %>%
    mutate(apde_dual = if_else(apde_dual == 1, "Dual eligible", "Not dual eligible"),
           full_benefit = case_when(
             full_benefit == 1 ~ "Full Medicaid benefits",
             full_benefit == 0 ~ "Not full Medicaid benefits"),
           enroll_type = case_when(
             enroll_type == "h" ~ "Housing only",
             enroll_type == "md" ~ "Medicaid only",
             enroll_type == "me" ~ "Medicare only",
             enroll_type == "mm" ~ "Medicaid and Medicare",
             enroll_type == "hmd" ~ "Housing and Medicaid",
             enroll_type == "hme" ~ "Housing and Medicare",
             enroll_type == "a" ~ "Housing and Medicaid and Medicare"),
           pha_operator = ifelse(pha_subsidy == "TENANT BASED/SOFT UNIT", NA_character_, 
                                 pha_operator),
           pha_portfolio = case_when(
             pha_operator == "NON-PHA OPERATED" ~ NA_character_,
             pha_subsidy == "HARD UNIT" & pha_portfolio == "" ~ "Unknown",
             pha_subsidy == "TENANT BASED/SOFT UNIT" & pha_portfolio == "" ~ NA_character_,
             TRUE ~ pha_portfolio)) %>%
    rename(agency = pha_agency, dual = apde_dual,
           gender = gender_me, ethn = race_eth_me,
           length = time_housing, subsidy = pha_subsidy,
           voucher = pha_voucher, operator = pha_operator,
           portfolio = pha_portfolio, zip = geo_zip) %>%
    group_by(year, enroll_type, agency, dual, full_benefit, 
             agegrp, gender, ethn, length, 
             subsidy, voucher, operator, portfolio, zip) %>%
    summarise(count := sum(condition)) %>%
    ungroup()
  
  return(output)
}



### Function to count # people with acute events based on prioritized pop
# Ned to  update this
eventcount_acute_persons_f <- function(df_events = acute, df_pop = chronic_pop,
                                       event = c("ed", "hosp", "wc"), year = 12) {
  
  yr_chk <- as.numeric(paste0("20", year))
  
  # Filter to only include people with the condition in that year
  if (event == "ed") {
    df_events <- df_events %>% filter(ed == 1 & year(from_date) == yr_chk)
  } else if (event == "hosp") {
    df_events <- df_events %>% filter(inpatient == 1 & year(from_date) == yr_chk)
  } else if (event == "wc") {
    df_events <- df_events %>% filter(clm_type_mcaid_id == 27 & year(from_date) == yr_chk)
  } else {
    stop("Chose an acute event from 'ed', 'hosp', or 'wc'")
  }
  
  df_events <- df_events %>%
    distinct(id_apde) %>%
    mutate(condition = 1)
  
  # Filter assigned pop to current year
  df_pop <- df_pop %>% filter(year == yr_chk)
  
  
  ### Join pop and condition data to summarise
  output <- left_join(df_pop, df_events, by = "id_apde")
  output <- output %>% mutate(condition = if_else(is.na(condition), 0, 1))
  
  output <- output %>%
    group_by(year, agency_num, enroll_type_num, dual_elig_num, agegrp,
             gender_num, ethn_num, voucher_num, subsidy_num, operator_num,
             portfolio_num, length, zip_c) %>%
    summarise(count := sum(condition)) %>%
    ungroup() %>%
    select(year, agency_num, enroll_type_num, dual_elig_num, agegrp, 
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


#### POPULATION ####
#### Enrollment population for joint Medicaid/Medicare data ####
pop_enroll_all_mm <- bind_rows(lapply(seq_along(years), function(x) {
  sql_query <- glue_sql(
    "SELECT [year], enroll_type, pha_agency, apde_dual, full_benefit, 
    agegrp, gender_me, race_eth_me, pha_subsidy, pha_voucher, pha_operator, 
    pha_portfolio, time_housing, geo_zip, SUM(pt_days) AS pt_days, 
    SUM(pop_ever) AS pop_ever, SUM(pop) AS pop 
    FROM stage.mcaid_mcare_pha_elig_calyear 
    WHERE [year] = {years[x]} 
    GROUP BY [year], enroll_type, pha_agency, apde_dual, full_benefit, agegrp, 
    gender_me, race_eth_me, pha_subsidy, pha_voucher, pha_operator, 
    pha_portfolio, time_housing, geo_zip",
    .con = db_apde51)
  
  pop <- dbGetQuery(db_apde51, sql_query) %>%
    mutate(wc_flag = 0L)
  return(pop)
}))


### Repeat for well-child population
pop_enroll_all_mm_wc <- bind_rows(lapply(seq_along(years), function(x) {
  sql_query <- glue_sql(
    "SELECT [year], enroll_type, pha_agency, apde_dual, full_benefit, 
    agegrp, gender_me, race_eth_me, pha_subsidy, pha_voucher, pha_operator, 
    pha_portfolio, time_housing, geo_zip, SUM(pt_days) AS pt_days, 
    SUM(pop_ever) AS pop_ever, SUM(pop) AS pop 
    FROM stage.mcaid_mcare_pha_elig_calyear 
    WHERE [year] = {years[x]} AND age_wc IS NOT NULL 
    GROUP BY [year], enroll_type, pha_agency, apde_dual, full_benefit, agegrp, 
    gender_me, race_eth_me, pha_subsidy, pha_voucher, pha_operator, 
    pha_portfolio, time_housing, geo_zip",
    .con = db_apde51)
  
  pop <- dbGetQuery(db_apde51, sql_query) %>%
    mutate(wc_flag = 1L)
  return(pop)
}))


### Combine into one
pop_enroll_all_mm <- bind_rows(pop_enroll_all_mm, pop_enroll_all_mm_wc)
pop_enroll_all_mm <- pop_enroll_all_mm %>%
  mutate_at(vars(starts_with("pha_")), 
            list( ~ ifelse(enroll_type %in% c("mm", "md", "me"), "Non-PHA", .))) %>%
  mutate(apde_dual = if_else(apde_dual == 1, "Dual eligible", "Not dual eligible"),
         full_benefit = case_when(
           full_benefit == 1 ~ "Full Medicaid benefits",
           full_benefit == 0 ~ "Not full Medicaid benefits"),
         enroll_type = case_when(
           enroll_type == "h" ~ "Housing only",
           enroll_type == "md" ~ "Medicaid only",
           enroll_type == "me" ~ "Medicare only",
           enroll_type == "mm" ~ "Medicaid and Medicare",
           enroll_type == "hmd" ~ "Housing and Medicaid",
           enroll_type == "hme" ~ "Housing and Medicare",
           enroll_type == "a" ~ "Housing and Medicaid and Medicare"),
         pha_operator = ifelse(pha_subsidy == "TENANT BASED/SOFT UNIT", NA_character_, 
                               pha_operator),
         pha_portfolio = case_when(
           pha_operator == "NON-PHA OPERATED" ~ NA_character_,
           pha_subsidy == "HARD UNIT" & pha_portfolio == "" ~ "Unknown",
           pha_subsidy == "TENANT BASED/SOFT UNIT" & pha_portfolio == "" ~ NA_character_,
           TRUE ~ pha_portfolio)) %>% 
  rename(agency = pha_agency, dual = apde_dual,
         gender = gender_me, ethn = race_eth_me,
         length = time_housing, subsidy = pha_subsidy,
         voucher = pha_voucher, operator = pha_operator,
         portfolio = pha_portfolio, zip = geo_zip)


#### Collapse into bivariate totals ####
# Use tabloop_f from medicaid package to summarise

# This is using a horrible mix of quosures and lists for now.
# The tabloop function will be rewritten some day to make this easier.
fixed_list <- list_var(wc_flag, year, agency, enroll_type, dual)
loop_list <- c("agegrp", "gender", "ethn", "voucher", "subsidy", "operator", 
               "portfolio", "length", "zip")

pop_enroll_bivariate <- bind_rows(lapply(loop_list, function(x) {
  
  # Set up quosure for each loop var
  loop_quo <- rlang::as_quosure(rlang::sym(x), env = environment()) 
  
  ### Make new versions of each list to feed to tabloop
  # This is super clunky but should work
  fixed_list_new <- rlang::new_quosures(c(fixed_list, loop_quo))
  loop_list_new <- loop_list[!loop_list %in% x]
  loop_list_new <- rlang::as_quosures(rlang::syms(loop_list_new), env = environment())
  
  ### Run tabloop
  output <- tabloop_f(pop_enroll_all_mm, sum = list_var(pop_ever, pop, pt_days),
                      fixed = fixed_list_new, loop = loop_list_new) %>%
    mutate(category1 = rlang::quo_name(x)) %>%
    # Remove rows with zero count to save memory
    filter(pt_days_sum > 0) %>%
    rename(group1 = !!x, category2 = group_cat, group2 = group)
  
  return(output)
}))


### Make total columns for univariate analyses
# Make this a set of quosures for expediency's sake
loop_list <- list_var(agegrp, gender, ethn, voucher, subsidy, operator, portfolio, length, zip)

pop_enroll_total <- bind_rows(lapply(loop_list, function(x) {
  output <- pop_enroll_all_mm %>%
    group_by(wc_flag, year, agency, enroll_type, dual, !!x) %>%
    summarise(pop_ever_sum = sum(pop_ever, na.rm = T), pop_sum = sum(pop, na.rm = T),
              pt_days_sum = sum(pt_days, na.rm = T)) %>%
    ungroup() %>%
    mutate(
      category1 = quo_name(x), group1 = !!x,
      category2 = "total", group2 = "total") %>%
    select(-(!!x))
  
  return(output)
}))

rm(fixed_list, loop_list)


#### Combine into one ####
pop_enroll_combine_bivar <- bind_rows(pop_enroll_bivariate, pop_enroll_total) %>%
  filter(pt_days_sum > 0) %>%
  select(year, agency, enroll_type, dual, category1, group1, 
         category2, group2, pop_ever_sum, pop_sum, pt_days_sum, wc_flag) %>%
  rename(pop_ever = pop_ever_sum, pop = pop_sum, pt_days = pt_days_sum) 


# Add suppression and data source
pop_enroll_combine_bivar <- pop_enroll_combine_bivar %>%
  mutate_at(vars(pop_ever, pop),
            list(supp_flag = ~ if_else(between(., 1, 10), 1, 0))) %>%
  mutate_at(vars(pop_ever, pop),
            list(supp = ~ if_else(between(., 1, 10), NA_real_, .))) %>%
  mutate(source = "Medicaid and Medicare")
  

### Make suppressed version
pop_enroll_combine_bivar_suppressed <- pop_enroll_combine_bivar %>%
  select(year:group2, pt_days, pop_ever_supp, pop_supp,
         pop_ever_supp_flag, pop_supp_flag, wc_flag, source) %>%
  rename(
    pop_ever = pop_ever_supp, pop = pop_supp,
    pop_ever_supp = pop_ever_supp_flag, pop_supp = pop_supp_flag)




#### Temp way to populate existing Medicaid-only data ####
# Will only work once then need to change code
# Only needed until the combined mcaid_mcare_pha table is remade with dual flag
pop_enroll_all_md <- dbGetQuery(db_extractstore51, 
"SELECT * FROM APDE_WIP.mcaid_pha_enrollment WHERE source = 'Medicaid only'")


### Combine data
pop_enroll_combine_bivar_suppressed <- bind_rows(pop_enroll_combine_bivar_suppressed, 
                                                 pop_enroll_all_md)


#### Write to SQL ####
DBI::dbWriteTable(db_extractstore51,
             name = DBI::Id(schema = "APDE_WIP", table = "mcaid_mcare_pha_enrollment"),
             value = pop_enroll_combine_bivar_suppressed,
             append = F, overwrite = T,
             field.types = c(year = "integer", pt_days = "integer", 
                             pop_ever = "integer", 
                             pop = "integer", pop_ever_supp = "integer",
                             pop_supp = "integer", wc_flag = "integer"))



#### POPULATION TO JOIN TO CHRONIC EVENTS DATA ####
# For looking at enrollment alone, we disaggregate by additional factors 
#    (e.g., enrollment type, dual)
# For health events, especially from the Medicaid/Medicare data, we have a shorter
#    list. Therefore need to rerun population data again

fixed_list <- list_var(year, wc_flag, agency)
# fixed_list <- list_var(year, agency, dual) # Medicaid-only data
loop_list <- c("agegrp", "gender", "ethn", "voucher", "subsidy", "operator", 
               "portfolio", "length", "zip")

pop_health_bivariate <- bind_rows(lapply(loop_list, function(x) {
  
  # Set up quosure for each loop var
  loop_quo <- rlang::as_quosure(rlang::sym(x), env = environment()) 
  
  ### Make new versions of each list to feed to tabloop
  # This is super clunky but should work
  fixed_list_new <- rlang::new_quosures(c(fixed_list, loop_quo))
  loop_list_new <- loop_list[!loop_list %in% x]
  loop_list_new <- rlang::as_quosures(rlang::syms(loop_list_new), env = environment())
  
  ### Run tabloop
  output <- tabloop_f(pop_enroll_all_mm, sum = list_var(pop_ever, pop, pt_days),
                      fixed = fixed_list_new, loop = loop_list_new) %>%
    mutate(category1 = rlang::quo_name(x)) %>%
    # Remove rows with zero count to save memory
    filter(pt_days_sum > 0) %>%
    rename(group1 = !!x, category2 = group_cat, group2 = group)
  
  return(output)
}))


### Make total columns for univariate analyses
# Make this a set of quosures for expediency's sake
loop_list <- list_var(agegrp, gender, ethn, voucher, subsidy, operator, portfolio, length, zip)

pop_health_univariate <- bind_rows(lapply(loop_list, function(x) {
  output <- pop_enroll_all_mm %>%
    group_by(year, wc_flag, agency, !!x) %>%
    summarise(pop_ever_sum = sum(pop_ever, na.rm = T), pop_sum = sum(pop, na.rm = T),
              pt_days_sum = sum(pt_days, na.rm = T)) %>%
    ungroup() %>%
    mutate(
      category1 = quo_name(x), group1 = !!x,
      category2 = "total", group2 = "total") %>%
    select(-(!!x))
  
  return(output)
}))


# Make overall columns for each agency
pop_health_total <- pop_enroll_all_mm %>%
  group_by(year, wc_flag, agency) %>%
  summarise(pop_ever_sum = sum(pop_ever, na.rm = T), pop_sum = sum(pop, na.rm = T),
            pt_days_sum = sum(pt_days, na.rm = T)) %>%
  ungroup() %>%
  mutate(
    category1 = "overall", group1 = "overall",
    category2 = "total", group2 = "total")

rm(fixed_list, loop_list)


#### Combine into one ####
pop_health_combine <- bind_rows(pop_health_bivariate, pop_health_univariate, pop_health_total) %>%
  filter(pt_days_sum > 0) %>%
  select(year, wc_flag, agency, category1, group1, category2, group2, 
         pop_ever_sum, pop_sum, pt_days_sum) %>%
  rename(pop_ever = pop_ever_sum, pop = pop_sum, pt_days = pt_days_sum) 


# Add suppression flags and data source
# Keep all vars for now because pop is neede for rest_pha calcs
pop_health_combine <- pop_health_combine %>%
  mutate_at(vars(pop_ever, pop),
            list(supp_flag = ~ if_else(between(., 1, 10), 1, 0))) %>%
  mutate_at(vars(pop_ever, pop),
            list(supp = ~ if_else(between(., 1, 10), NA_real_, .))) %>%
  mutate(source = "Medicaid and Medicare")


#### CLEAN UP ####
rm(pop_enroll_all_mm, pop_enroll_all_mm_wc)
rm(pop_enroll_bivariate, pop_enroll_total)
gc()

#### END POPULATION ####


#### PERSON-TIME BASED EVENTS ####
### Hospitalizations
# Join demographic and hospitalization data
pha_mcaid_hosp <- acute_query_f(type = "hosp")

# Run for total number of hospitalizations
hosp_cnt <- pha_mcaid_hosp %>%
  group_by(year, enroll_type, agency, dual, full_benefit, 
           agegrp, gender_me, race_eth_me, time_housing, 
           pha_subsidy, pha_voucher, pha_operator, pha_portfolio, geo_zip) %>%
  summarise(count = sum(inpatient)) %>%
  mutate(indicator = "Hospitalizations")


### ED visits
# Join demographics and ED events
pha_mcaid_ed <- acute_query_f(type = "ed")

# Run for total number of ED visits
ed_cnt <- pha_mcaid_ed %>%
  group_by(year, enroll_type, agency, dual, full_benefit, 
           agegrp, gender_me, race_eth_me, time_housing, 
           pha_subsidy, pha_voucher, pha_operator, pha_portfolio, geo_zip) %>%
  summarise(count = sum(ed)) %>%
  mutate(indicator = "ED visits")



#### CHRONIC CONDITIONS ####
### Alzheimer's related
alzheimer_pers <- bind_rows(lapply(seq(2012, 2016), eventcount_chronic_f, 
                                   condition = "ccw_alzheimer_related", cvd = F)) %>%
  mutate(indicator = "Persons with Alzheimer's-related conditions")


### Asthma
asthma_pers <- bind_rows(lapply(seq(2012, 2016), eventcount_chronic_f, 
                                condition = "ccw_asthma", cvd = F)) %>%
  mutate(indicator = "Persons with asthma")


### Cancer - breast
cancer_breast_pers <- bind_rows(lapply(seq(2012, 2016), eventcount_chronic_f, 
                                condition = "ccw_cancer_breast", cvd = F)) %>%
  mutate(indicator = "Persons with cancer: breast")


### Cancer - colorectal
cancer_colorectal_pers <- bind_rows(lapply(seq(2012, 2016), eventcount_chronic_f, 
                                           condition = "ccw_cancer_colorectal", cvd = F)) %>%
  mutate(indicator = "Persons with cancer: colorectal")


### CHF
chf_pers <- bind_rows(lapply(seq(2012, 2016), eventcount_chronic_f, 
                             condition = "ccw_heart_failure", cvd = F)) %>%
  mutate(indicator = "Persons with cardiovascular disease: congestive heart failure")


# COPD
copd_pers <- bind_rows(lapply(seq(2012, 2016), eventcount_chronic_f, 
                             condition = "ccw_copd", cvd = F)) %>%
  mutate(indicator = "Persons with chronic obstructive pulmonary disease")


### CVD
cvd_pers <- bind_rows(lapply(seq(2012, 2016), eventcount_chronic_f, 
                             condition = "", cvd = T)) %>%
  mutate(indicator = "Persons with cardiovascular disease: any type")


### Depression
depression_pers <- bind_rows(lapply(seq(2012, 2016), eventcount_chronic_f, 
                             condition = "ccw_depression", cvd = F)) %>%
  mutate(indicator = "Persons with depression")


### Diabetes
diabetes_pers <- bind_rows(lapply(seq(2012, 2016), eventcount_chronic_f, 
                                  condition = "ccw_diabetes", cvd = F)) %>%
  mutate(indicator = "Persons with diabetes")


### Hypertension
hypertension_pers <- bind_rows(lapply(seq(2012, 2016), eventcount_chronic_f, 
                                      condition = "ccw_hypertension", cvd = F)) %>%
  mutate(indicator = "Persons with cardiovascular disease: hypertension")


### IHD
ihd_pers <- bind_rows(lapply(seq(2012, 2016), eventcount_chronic_f, 
                                      condition = "ccw_ischemic_heart_dis", cvd = F)) %>%
  mutate(indicator = "Persons with cardiovascular disease: ischemic heart disease")


### Kidney disease
kidney_pers <- bind_rows(lapply(seq(2012, 2016), eventcount_chronic_f, 
                             condition = "ccw_chr_kidney_dis", cvd = F)) %>%
  mutate(indicator = "Persons with kidney disease")


### Myocardial infarction
mi_pers <- bind_rows(lapply(seq(2012, 2016), eventcount_chronic_f, 
                                condition = "ccw_mi", cvd = F)) %>%
  mutate(indicator = "Persons with cardiovascular disease: myocardial infarction")


#### COMBINE DATA ####
health_events <- bind_rows(alzheimer_pers, asthma_pers, cancer_breast_pers,
                           cancer_colorectal_pers, chf_pers, copd_pers, cvd_pers, 
                           depression_pers, diabetes_pers, hypertension_pers, 
                           ihd_pers, kidney_pers, mi_pers)


rm(list = ls(pattern = "_cnt$"))
rm(list = ls(pattern = "^ed_cnt"))
rm(list = ls(pattern = "_pers$"))


#### AGGREGATION ####
# Use tabloop_f from medicaid package to summarise
# Don't loop over ZIP because not using data (and memory intensive)


### NOTE: THIS IS CURRENTLY SET UP TO ANALYZE THE MEDICAID/MEDICARE DATA
# REWORK TO LOOP OVER DUAL IF USING MEDICAID-ONLY DATA


# This is using a horrible mix of quosures and lists for now.
# The tabloop function will be rewritten some day to make this easier.
fixed_list <- list_var(indicator, year, agency)
# fixed_list <- list_var(indicator, year, agency, dual) # Medicaid-only version
loop_list <- c("agegrp", "gender", "ethn", "voucher", "subsidy", "operator", "portfolio", "length")


health_events_bivariate <- bind_rows(lapply(loop_list, function(x) {
  
  # Set up quosure for each loop var
  loop_quo <- rlang::as_quosure(rlang::sym(x), env = environment()) 
  
  ### Make new versions of each list to feed to tabloop
  # This is super clunky but should work
  fixed_list_new <- rlang::new_quosures(c(fixed_list, loop_quo))
  loop_list_new <- loop_list[!loop_list %in% x]
  loop_list_new <- rlang::as_quosures(rlang::syms(loop_list_new), env = environment())
  
  ### Run tabloop
  output <- tabloop_f(health_events, sum = list_var(count),
                      fixed = fixed_list_new, loop = loop_list_new) %>%
    mutate(category1 = rlang::quo_name(x)) %>%
    rename(group1 = !!x, category2 = group_cat, group2 = group)

  return(output)
}))


# Make this a set of quosures for expediency's sake
loop_list <- list_var(agegrp, gender, ethn, voucher, subsidy, operator, portfolio, length, zip)

### Make total columns for univariate analyses
health_events_univariate <- bind_rows(lapply(loop_list, function(x) {
  output <- health_events %>%
    group_by(indicator, year, agency, !!x) %>%
    # group_by(indicator, year, agency, enroll_type, dual, !!x) %>% # Medicaid-only version
    summarise(count_sum = sum(count)) %>%
    ungroup() %>%
    mutate(
      category1 = quo_name(x), group1 = !!x,
      category2 = "total", group2 = "total") %>%
    select(-(!!x))
  
  return(output)
}))

rm(fixed_list, loop_list)


### Make overall 
health_events_total <- health_events %>%
  group_by(indicator, year, agency) %>%
  summarise(count_sum = sum(count)) %>%
  ungroup() %>%
  mutate(
    category1 = "overall", group1 = "overall",
    category2 = "total", group2 = "total")


#### Combine into one data frame ####
health_events_combined <- bind_rows(health_events_bivariate, health_events_univariate, health_events_total) %>%
  # Filter out impossible combinations
  filter(!(indicator == "Injuries" & year < 2016)) %>%
  rename(count = count_sum) %>%
  mutate(wc_flag = if_else(indicator == "Well-child check", 1L, 0L),
         acute = case_when(
           str_detect(indicator, "Persons with") ~ 0L,
           str_detect(indicator, "ED visits") ~ 1L,
           indicator %in% c("Hospitalizations", "Well-child check", "Injuries") ~ 1L,
           TRUE ~ NA_integer_)) %>%
  # select(indicator, acute, wc_flag, year, agency, dual, category1, group1:count) %>%
  # arrange(indicator, year, agency, dual, category1, group1, category2, group2)
  select(indicator, acute, wc_flag, year, agency, category1, group1:count) %>%
  arrange(indicator, year, agency, category1, group1, category2, group2)


# Add suppression
health_events_combined <- health_events_combined %>%
  mutate(suppressed = if_else(between(count, 1, 10), 1, 0),
         count_supp = if_else(between(count, 1, 10), NA_real_, count)) 



#### COMBINE WITH POP DATA ####
health_events_combined_pop <- left_join(
  health_events_combined, pop_health_combine,
  by = c("year", "wc_flag", "agency", "category1", "group1", "category2", "group2")) %>%
  # Filter out rows with no population
  filter(!is.na(pop_ever))


# FVery slow but seems to work (~6.5 mins)
# Adapted from here: 
# https://stackoverflow.com/questions/49222353/how-to-use-purrrs-map-function-to-perform-row-wise-prop-tests-and-add-results-t

health_events_combined_pop <- health_events_combined_pop %>% 
  mutate(
    denominator = ifelse(year %in% c(2012, 2016), pt_days / 366, pt_days / 365),
    rate = case_when(
      acute == 1 ~ count / denominator * 1000,
      acute == 0 & (is.na(pop) | pop == 0) ~ NA_real_,
      acute == 0 ~ count / pop * 1000)) %>%
  mutate(chronic_cols = map2(count, pop, ~ if (.y < 1) {NA} else {prop.test(.x, n = .y, correct = F)})) %>%
  mutate(ci_lb_chronic = map_dbl(chronic_cols, function(x) {if (max(is.na(x)) == 1) {NA} else {x[["conf.int"]][[1]]}}),
         ci_ub_chronic = map_dbl(chronic_cols, function(x) {if (max(is.na(x)) == 1) {NA} else {x[["conf.int"]][[2]]}})) %>%
  select(-chronic_cols) %>%
  mutate(acute_cols = map2(count, denominator, ~ poisson.test(.x, T = .y) %>% 
                             {tibble(ci_lb_acute = .[["conf.int"]][[1]],
                                     ci_ub_acute = .[["conf.int"]][[2]])})) %>%
  unnest() %>%
  mutate(ci_lb = ifelse(acute == 1, ci_lb_acute, ci_lb_chronic) * 1000,
         ci_ub = ifelse(acute == 1, ci_ub_acute, ci_ub_chronic) * 1000) %>%
  select(-ci_lb_acute, -ci_ub_acute, -ci_lb_chronic, -ci_ub_chronic)



### Create portfolio specific comparisons and join back to main data

#### SET UP FOR MEDICAID/MEDICARE DATA
# NEED TO MODIFY FOR MEDICAID-ONLY DATA

rest_pha <- health_events_combined_pop %>%
  filter((agency == "KCHA" | agency == "SHA") & category1 == "portfolio" & 
           category2 == "total") %>%
  group_by(year, acute, wc_flag, indicator, agency) %>%
  mutate(count_tot = sum(count), pop_tot = sum(pop), pt_tot = sum(pt_days)) %>%
  ungroup() %>%
  mutate(
    rest_count = count_tot - count,
    rest_pop = pop_tot - pop,
    rest_pt = pt_tot - pt_days,
    rest_denominator = ifelse(year %in% c(2012, 2016), rest_pt / 366, rest_pt / 365),
    rest_pha_rate = case_when(
      acute == 1 ~ rest_count / rest_denominator * 1000,
      acute == 0 & pop == 0 ~ NA_real_,
      acute == 0 ~ rest_count / rest_pop * 1000)) %>%
  mutate(chronic_cols = map2(rest_count, rest_pop, ~ if (.y < 1) {NA} else {prop.test(.x, n = .y, correct = F)})) %>%
  mutate(rest_ci_lb_chronic = map_dbl(chronic_cols, function(x) {if (max(is.na(x)) == 1) {NA} else {x[["conf.int"]][[1]]}}),
         rest_ci_ub_chronic = map_dbl(chronic_cols, function(x) {if (max(is.na(x)) == 1) {NA} else {x[["conf.int"]][[2]]}})) %>%
  select(-chronic_cols) %>%
  mutate(acute_cols = map2(rest_count, rest_denominator, ~ poisson.test(.x, T = .y) %>% 
                             {tibble(rest_ci_lb_acute = .[["conf.int"]][[1]],
                                     rest_ci_ub_acute = .[["conf.int"]][[2]])})) %>%
  unnest() %>%
  mutate(rest_ci_lb = ifelse(acute == 1, rest_ci_lb_acute * 1000, rest_ci_lb_chronic * 1000),
         rest_ci_ub = ifelse(acute == 1, rest_ci_ub_acute * 1000, rest_ci_ub_chronic * 1000)) %>%
  select(year, acute, indicator, agency, category1, group1, category2, group2,
         rest_count, rest_denominator, rest_pop, rest_pt,
         rest_pha_rate, rest_ci_lb, rest_ci_ub)


health_events_combined_pop <- left_join(health_events_combined_pop, rest_pha,
                                        by = c("year", "acute", "indicator", "agency", 
                                               "category1", "group1", "category2", "group2"))


### Make suppressed version
health_events_combined_suppressed <- health_events_combined_pop %>%
  mutate_at(vars(rate, ci_lb, ci_ub, rest_pha_rate, rest_ci_lb, rest_ci_ub),
            list( ~ case_when(
              suppressed == 1 ~ NA_real_,
              acute == 0 & pop_supp_flag == 1 ~ NA_real_,
              TRUE ~ round(., 1)
            ))) %>%
  mutate_at(vars(rest_count, rest_pop), 
            list( ~ ifelse(suppressed == 1 | pop_supp_flag == 1, NA, .))) %>%
  select(indicator:group2, count_supp, denominator, pt_days, pop_ever_supp, pop_supp, rate:ci_ub, 
         rest_count, rest_denominator, rest_pop, rest_pt, rest_pha_rate, rest_ci_lb, rest_ci_ub,
         suppressed, pop_ever_supp_flag, pop_supp_flag, source) %>%
  rename(
    count = count_supp, pop_ever = pop_ever_supp, pop = pop_supp,
    count_supp = suppressed, pop_ever_supp = pop_ever_supp_flag,
    pop_supp = pop_supp_flag)



#### TEMP - BRING IN EXISTING DATA AND JOIN ####
health_events_md <- dbGetQuery(
  db_extractstore50, "SELECT * FROM APDE.mcaid_mcare_pha_events WHERE source = 'Medicaid only'")


### Combine data
health_events_combined_suppressed <- bind_rows(health_events_combined_suppressed, health_events_md) %>%
  select(indicator, acute, wc_flag, year, agency, enroll_type, dual, 
         category1, group1, category2, group2, count, denominator, pt_days, 
         pop_ever, pop, rate, ci_lb, ci_ub, 
         rest_count, rest_denominator, rest_pop, rest_pt, rest_pha_rate, rest_ci_lb, 
         rest_ci_ub,count_supp, pop_ever_supp, pop_supp, source)



#### WRITE DATA ####
dbWriteTable(db_extractstore51, 
             name = DBI::Id(schema = "APDE_WIP", table = "mcaid_mcare_pha_events"),
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
rm(list = ls(pattern = "tabloop"))
rm(health_events)
gc()


#### MOVE DATA TO 50 SERVER ####
# If the Tableau workbook and data pass internal QA, move to external WIP file
# for partners to QA
DBI::dbWriteTable(db_extractstore50,
                  name = DBI::Id(schema = "APDE", table = "mcaid_pha_enrollment"),
                  value = pop_enroll_combine_bivar_suppressed,
                  append = F, overwrite = T,
                  field.types = c(pt_days = "integer", pop_ever = "integer",
                                  pop = "integer", pop_ever_supp = "integer",
                                  pop_supp = "integer", wc_flag = "integer"))

DBI::dbWriteTable(db_extractstore50,
                  name = DBI::Id(schema = "APDE", name = "mcaid_pha_event_causes"),
                  value = acute_cause,
                  append = F, overwrite = T)

dbWriteTable(db_extractstore50, 
             name = DBI::Id(schema = "APDE", table = "mcaid_pha_events"),
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






#### OLD CODE --------------------------------------------------

### ED visits
# Join demographics and ED events
pha_mcaid_ed <- left_join(mcaid_mcare_pha_elig_demo,
                          filter(acute, ed == 1), 
                          by = "id_apde") %>%
  filter(from_date >= startdate_c & from_date <= enddate_c) %>%
  mutate(ed_year = year(from_date))


# Run for total number of ED visits
ed_cnt <- lapply(seq(12, 18), eventcount_acute_f, df = pha_mcaid_ed, 
                 event = ed, number = T, person = F)
ed_cnt <- as.data.frame(data.table::rbindlist(ed_cnt)) %>%
  mutate(indicator = "ED visits")

# Run for number of unavoidable ED visits
ed_cnt_unavoid <- lapply(seq(12, 18), eventcount_acute_f, df = pha_mcaid_ed, 
                 event = ed_emergent_nyu, number = T, person = F)
ed_cnt_unavoid <- as.data.frame(data.table::rbindlist(ed_cnt_unavoid)) %>%
  mutate(indicator = "ED visits - unavoidable")

# Run for number of too close to call ED visits
ed_cnt_inter <- lapply(seq(12, 18), eventcount_acute_f, df = pha_mcaid_ed, 
                         event = ed_intermediate_nyu, number = T, person = F)
ed_cnt_inter <- as.data.frame(data.table::rbindlist(ed_cnt_inter)) %>%
  mutate(indicator = "ED visits - borderline avoidable")

# Run for number of avoidable ED visits
ed_cnt_avoid <- lapply(seq(12, 18), eventcount_acute_f, df = pha_mcaid_ed, 
                         event = ed_nonemergent_nyu, number = T, person = F)
ed_cnt_avoid <- as.data.frame(data.table::rbindlist(ed_cnt_avoid)) %>%
  mutate(indicator = "ED visits - potentially avoidable")

# Run for number of unclassified ED visits
ed_cnt_unclass <- lapply(seq(12, 18), eventcount_acute_f, df = pha_mcaid_ed, 
                       event = ed_unclass_nyu, number = T, person = F)
ed_cnt_unclass <- as.data.frame(data.table::rbindlist(ed_cnt_unclass)) %>%
  mutate(indicator = "ED visits - unable to determine avoidability")

# Run for number of MH ED visits
ed_cnt_mh <- lapply(seq(12, 18), eventcount_acute_f, df = pha_mcaid_ed, 
                         event = ed_mh_nyu, number = T, person = F)
ed_cnt_mh <- as.data.frame(data.table::rbindlist(ed_cnt_mh)) %>%
  mutate(indicator = "ED visits - mental health primary dx")

# Run for number of alcohol-related ED visits
ed_cnt_alc <- lapply(seq(12, 18), eventcount_acute_f, df = pha_mcaid_ed, 
                    event = ed_alc_nyu, number = T, person = F)
ed_cnt_alc <- as.data.frame(data.table::rbindlist(ed_cnt_alc)) %>%
  mutate(indicator = "ED visits - alcohol-related primary dx")

# Run for number of alcohol-related ED visits
ed_cnt_sud <- lapply(seq(12, 18), eventcount_acute_f, df = pha_mcaid_ed, 
                     event = ed_sud_nyu, number = T, person = F)
ed_cnt_sud <- as.data.frame(data.table::rbindlist(ed_cnt_sud)) %>%
  mutate(indicator = "ED visits - substance use disorder related primary dx")

# Run for number of BH-related ED visits
ed_cnt_bh <- lapply(seq(12, 18), eventcount_acute_f, df = pha_mcaid_ed, 
                     event = ed_bh, number = T, person = F)
ed_cnt_bh <- as.data.frame(data.table::rbindlist(ed_cnt_bh)) %>%
  mutate(indicator = "ED visits - behavioral health-related primary dx")

rm(pha_mcaid_ed)
gc()



### Injuries (restrict to >= 2016 for now due to ICD issues)
# Join demographics and injury events
pha_mcaid_inj <- left_join(mcaid_mcare_pha_elig_demo, filter(acute, !is.na(intent)), by = "id_apde") %>%
  filter(from_date >= startdate_c & from_date <= enddate_c) %>%
  mutate(inj_year = year(from_date),
         injury = 1)

# Run for number of people with an injury
inj_pers <- lapply(seq(16, 18), eventcount_acute_f, df = pha_mcaid_inj, 
                   event = injury, number = T, person = T)
inj_pers <- as.data.frame(data.table::rbindlist(inj_pers)) %>%
  mutate(indicator = "Persons with an injury")

# Run for total number of injury visits
inj_cnt <- lapply(seq(16, 18), eventcount_acute_f, df = pha_mcaid_inj, 
                  event = injury, number = T, person = F)
inj_cnt <- as.data.frame(data.table::rbindlist(inj_cnt)) %>%
  mutate(indicator = "Injuries")
rm(pha_mcaid_inj)



#### CAUSES OF ACUTE CLAIMS (HOSPITALIZATION AND ED) ####
acute_cause_nonpha <- bind_rows(lapply(seq(2012,2018), function(x) {
  # Set up years
  year_from <- paste0(x, "-01-01")
  year_to <- paste0(x, "-12-31")
  
  # Hospitalizations (primary dx only)
  hosp_primary <- top_causes_f(cohort = mcaid_mcare_pha_elig_demo[mcaid_mcare_pha_elig_demo$agency_num == 0, ],
                                 cohort_id = id_apde, server = db_claims51, 
                                 from_date = year_from, to_date = year_to,
                                 ind_dates = T,
                                 ind_from_date = startdate_c,
                                 ind_to_date = enddate_c,
                                 inpatient = T, ed_all = F, ed_avoid_ny = F, ed_avoid_ca = F,
                                 primary_dx = T)
  hosp_primary <- hosp_primary %>%
    mutate(year = x, agency_num = 0, cause_type = "Hospitalizations", dx_level = "Primary diagnosis only")
  
  # Hospitalizations (all dx fields)
  hosp_alldx <- top_causes_f(cohort = mcaid_mcare_pha_elig_demo[mcaid_mcare_pha_elig_demo$agency_num == 0, ],
                               cohort_id = id_apde, server = db_claims51, 
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
  ed_all_primary <- top_causes_f(cohort = mcaid_mcare_pha_elig_demo[mcaid_mcare_pha_elig_demo$agency_num == 0, ],
                                 cohort_id = id_apde, server = db_claims51, 
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
  ed_all_alldx <- top_causes_f(cohort = mcaid_mcare_pha_elig_demo[mcaid_mcare_pha_elig_demo$agency_num == 0, ],
                               cohort_id = id_apde, server = db_claims51, 
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
  ed_avoid_primary <- top_causes_f(cohort = mcaid_mcare_pha_elig_demo[mcaid_mcare_pha_elig_demo$agency_num == 0, ],
                                   cohort_id = id_apde, server = db_claims51, 
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
  ed_avoid_alldx <- top_causes_f(cohort = mcaid_mcare_pha_elig_demo[mcaid_mcare_pha_elig_demo$agency_num == 0, ],
                                 cohort_id = id_apde, server = db_claims51, 
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

acute_cause_kcha <- bind_rows(lapply(seq(2012,2018), function(x) {
  # Set up years
  year_from <- paste0(x, "-01-01")
  year_to <- paste0(x, "-12-31")
  
  # Hospitalizations (primary dx only)
  hosp_primary <- top_causes_f(cohort = mcaid_mcare_pha_elig_demo[mcaid_mcare_pha_elig_demo$agency_num == 1 & 
                                                         mcaid_mcare_pha_elig_demo$enroll_type_num == 3, ],
                               cohort_id = id_apde, server = db_claims51, 
                               from_date = year_from, to_date = year_to,
                               ind_dates = T,
                               ind_from_date = startdate_c,
                               ind_to_date = enddate_c,
                               inpatient = T, ed_all = F, ed_avoid_ny = F, ed_avoid_ca = F,
                               primary_dx = T)
  hosp_primary <- hosp_primary %>%
    mutate(year = x, agency_num = 1, cause_type = "Hospitalizations", dx_level = "Primary diagnosis only")
  
  # Hospitalizations (all dx fields)
  hosp_alldx <- top_causes_f(cohort = mcaid_mcare_pha_elig_demo[mcaid_mcare_pha_elig_demo$agency_num == 1 & 
                                                       mcaid_mcare_pha_elig_demo$enroll_type_num == 3, ],
                             cohort_id = id_apde, server = db_claims51, 
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
  ed_all_primary <- top_causes_f(cohort = mcaid_mcare_pha_elig_demo[mcaid_mcare_pha_elig_demo$agency_num == 1 & 
                                                           mcaid_mcare_pha_elig_demo$enroll_type_num == 3, ],
                                 cohort_id = id_apde, server = db_claims51, 
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
  ed_all_alldx <- top_causes_f(cohort = mcaid_mcare_pha_elig_demo[mcaid_mcare_pha_elig_demo$agency_num == 1 & 
                                                         mcaid_mcare_pha_elig_demo$enroll_type_num == 3, ],
                               cohort_id = id_apde, server = db_claims51, 
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
  ed_avoid_primary <- top_causes_f(cohort = mcaid_mcare_pha_elig_demo[mcaid_mcare_pha_elig_demo$agency_num == 1 & 
                                                             mcaid_mcare_pha_elig_demo$enroll_type_num == 3, ],
                                   cohort_id = id_apde, server = db_claims51, 
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
  ed_avoid_alldx <- top_causes_f(cohort = mcaid_mcare_pha_elig_demo[mcaid_mcare_pha_elig_demo$agency_num == 1 & 
                                                           mcaid_mcare_pha_elig_demo$enroll_type_num == 3, ],
                                 cohort_id = id_apde, server = db_claims51, 
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

acute_cause_sha <- bind_rows(lapply(seq(2012,2018), function(x) {
  # Set up years
  year_from <- paste0(x, "-01-01")
  year_to <- paste0(x, "-12-31")
  
  # Hospitalizations (primary dx only)
  hosp_primary <- top_causes_f(cohort = mcaid_mcare_pha_elig_demo[mcaid_mcare_pha_elig_demo$agency_num == 2 & 
                                                         mcaid_mcare_pha_elig_demo$enroll_type_num == 3, ],
                               cohort_id = id_apde, server = db_claims51, 
                               from_date = year_from, to_date = year_to,
                               ind_dates = T,
                               ind_from_date = startdate_c,
                               ind_to_date = enddate_c,
                               inpatient = T, ed_all = F, ed_avoid_ny = F, ed_avoid_ca = F,
                               primary_dx = T)
  hosp_primary <- hosp_primary %>%
    mutate(year = x, agency_num = 2, cause_type = "Hospitalizations", dx_level = "Primary diagnosis only")
  
  # Hospitalizations (all dx fields)
  hosp_alldx <- top_causes_f(cohort = mcaid_mcare_pha_elig_demo[mcaid_mcare_pha_elig_demo$agency_num == 2 & 
                                                       mcaid_mcare_pha_elig_demo$enroll_type_num == 3, ],
                             cohort_id = id_apde, server = db_claims51, 
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
  ed_all_primary <- top_causes_f(cohort = mcaid_mcare_pha_elig_demo[mcaid_mcare_pha_elig_demo$agency_num == 2 & 
                                                           mcaid_mcare_pha_elig_demo$enroll_type_num == 3, ],
                                 cohort_id = id_apde, server = db_claims51, 
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
  ed_all_alldx <- top_causes_f(cohort = mcaid_mcare_pha_elig_demo[mcaid_mcare_pha_elig_demo$agency_num == 2 & 
                                                         mcaid_mcare_pha_elig_demo$enroll_type_num == 3, ],
                               cohort_id = id_apde, server = db_claims51, 
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
  ed_avoid_primary <- top_causes_f(cohort = mcaid_mcare_pha_elig_demo[mcaid_mcare_pha_elig_demo$agency_num == 2 & 
                                                             mcaid_mcare_pha_elig_demo$enroll_type_num == 3, ],
                                   cohort_id = id_apde, server = db_claims51, 
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
  ed_avoid_alldx <- top_causes_f(cohort = mcaid_mcare_pha_elig_demo[mcaid_mcare_pha_elig_demo$agency_num == 2 & 
                                                           mcaid_mcare_pha_elig_demo$enroll_type_num == 3, ],
                                 cohort_id = id_apde, server = db_claims51, 
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
    claim_cnt_supp = ifelse(claim_cnt < 10, "<10", as.character(claim_cnt)),
    # Suppress small counts
    claim_cnt = ifelse(claim_cnt < 10, NA, claim_cnt),
    # Find the rank for the first suppressed value
    rank_max = max(rank[!is.na(claim_cnt)], na.rm = T) + 1,
    # Overwrite the rank and add a flag
    rank = ifelse(is.na(claim_cnt), rank_max, rank),
    rank_flag = ifelse(is.na(claim_cnt), "+", NA_character_)
  ) %>%
  ungroup() %>%
  select(-rank_max)

# Write to SQL
DBI::dbWriteTable(db_extractstore51,
                  name = DBI::Id(schema = "APDE_WIP", name = "mcaid_pha_event_causes"),
                  value = acute_cause,
                  append = F, overwrite = T)



#### POPULATION-BASED COUNTS ####
### Set up population for chronic conditions
chronic_pop <- bind_rows(lapply(seq(12, 18), chronic_pop_f, df = mcaid_mcare_pha_elig_demo))
chronic_pop_wc <- bind_rows(lapply(seq(12, 18), chronic_pop_f, df = mcaid_mcare_pha_elig_demo, wc = T))


# Alzheimer's
alzheimer_pers <- bind_rows(lapply(seq(12, 18), 
                                   eventcount_chronic_f, df_chronic = chronic, 
                                   df_pop = chronic_pop, condition = "ccw_alzheimer", 
                                   cvd = F)) %>%
  mutate(indicator = "Persons with Alzheimer's")


# Asthma
asthma_pers <- bind_rows(lapply(seq(12, 18), 
                                eventcount_chronic_f, df_chronic = chronic, 
                                df_pop = chronic_pop, condition = "ccw_asthma", 
                                cvd = F)) %>%
  mutate(indicator = "Persons with asthma")


# Cancer - breast
cancer_breast_pers <- bind_rows(lapply(seq(12, 18), 
                                eventcount_chronic_f, df_chronic = chronic, 
                                df_pop = chronic_pop, condition = "ccw_cancer_breast", 
                                cvd = F)) %>%
  mutate(indicator = "Persons with cancer: breast")


# Cancer - colorectal
cancer_colorectal_pers <- bind_rows(lapply(seq(12, 18), 
                                       eventcount_chronic_f, df_chronic = chronic, 
                                       df_pop = chronic_pop, condition = "ccw_cancer_colorectal", 
                                       cvd = F)) %>%
  mutate(indicator = "Persons with cancer: colorectal")


# CHF
chf_pers <- bind_rows(lapply(seq(12, 18), 
                             eventcount_chronic_f, df_chronic = chronic, 
                             df_pop = chronic_pop, condition = "ccw_heart_failure", 
                             cvd = F)) %>%
  mutate(indicator = "Persons with cardiovascular disease: congestive heart failure")


# COPD
copd_pers <- bind_rows(lapply(seq(12, 18), 
                              eventcount_chronic_f, df_chronic = chronic, 
                              df_pop = chronic_pop, condition = "ccw_copd", 
                              cvd = F)) %>%
  mutate(indicator = "Persons with chronic obstructive pulmonary disease")


# CVD
cvd_pers <- bind_rows(lapply(seq(12, 18), 
                             eventcount_chronic_f, df_chronic = chronic, 
                             df_pop = chronic_pop, cvd = T)) %>%
  mutate(indicator = "Persons with cardiovascular disease: any type")


# Depression
depression_pers <- bind_rows(lapply(seq(12, 18), 
                                    eventcount_chronic_f, df_chronic = chronic, 
                                    df_pop = chronic_pop, condition = "ccw_depression", 
                                    cvd = F)) %>%
  mutate(indicator = "Persons with depression")


# Diabetes
diabetes_pers <- bind_rows(lapply(seq(12, 18), 
                                  eventcount_chronic_f, df_chronic = chronic, 
                                  df_pop = chronic_pop, condition = "ccw_diabetes", 
                                  cvd = F)) %>%
  mutate(indicator = "Persons with diabetes")


# Hypertension
hypertension_pers <- bind_rows(lapply(seq(12, 18), 
                                      eventcount_chronic_f, df_chronic = chronic, 
                                      df_pop = chronic_pop, condition = "ccw_hypertension", 
                                      cvd = F)) %>%
  mutate(indicator = "Persons with cardiovascular disease: hypertension")


# IHD
ihd_pers <- bind_rows(lapply(seq(12, 18), 
                             eventcount_chronic_f, df_chronic = chronic, 
                             df_pop = chronic_pop, condition = "ccw_ischemic_heart_dis", 
                             cvd = F)) %>%
  mutate(indicator = "Persons with cardiovascular disease: ischemic heart disease")


# Kidney disease
kidney_pers <- bind_rows(lapply(seq(12, 18), 
                                eventcount_chronic_f, df_chronic = chronic, 
                                df_pop = chronic_pop, condition = "ccw_chr_kidney_dis", 
                                cvd = F)) %>%
  mutate(indicator = "Persons with kidney disease")



# Persons with an ED visit
ed_pers <- bind_rows(lapply(seq(12, 18), eventcount_acute_persons_f, 
                              df_events = acute, df_pop = chronic_pop,
                              event = "ed")) %>%
  mutate(indicator = "Persons with ED visits")


# Persons with a hospitalization
hosp_pers <- bind_rows(lapply(seq(12, 18), eventcount_acute_persons_f, 
                              df_events = acute, df_pop = chronic_pop,
                              event = "hosp")) %>%
  mutate(indicator = "Persons with hospitalization")


# Persons with a well-child visit
wc_pers <- bind_rows(lapply(seq(12, 18), eventcount_acute_persons_f, 
                              df_events = acute, df_pop = chronic_pop_wc,
                              event = "wc")) %>%
  mutate(indicator = "Well-child check")




#### COMBINE DATA ####
health_events <- bind_rows(alzheimer_pers, asthma_pers, cancer_breast_pers, 
                           cancer_colorectal_pers, chf_pers, copd_pers, cvd_pers, 
                           depression_pers, diabetes_pers, ed_cnt, ed_cnt_alc, 
                           ed_cnt_bh, ed_cnt_avoid, ed_cnt_inter, ed_cnt_mh, 
                           ed_cnt_sud,  ed_cnt_unavoid, ed_cnt_unclass, ed_pers, 
                           hosp_cnt,  hosp_pers, hypertension_pers, ihd_pers, 
                           inj_cnt, inj_pers, 
                           kidney_pers, wc_pers)










