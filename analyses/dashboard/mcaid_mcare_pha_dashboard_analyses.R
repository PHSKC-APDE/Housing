###############################################################################
# Code to generate data for the housing/Medicaid dashboard
#
# Alastair Matheson (PHSKC-APDE)
# 2018-01-24
#
#
###############################################################################

##### Set up global parameter and call in libraries #####
options(max.print = 350, tibble.print_max = 30, scipen = 999, warning.length = 8170)

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
years <- seq(2012, 2018)


### Precalculated calendar year table
# Use stage for now
mcaid_mcare_pha_elig_calyear <- dbGetQuery(db_apde51, "SELECT * FROM stage.mcaid_mcare_pha_elig_calyear")

mcaid_mcare_pha_elig_calyear <- mcaid_mcare_pha_elig_calyear %>%
  mutate_at(vars(dob, death_dt, start_housing), list(~ as.Date(., origin = "1970-01-01")))

mcaid_mcare_pha_elig_calyear <- setDT(mcaid_mcare_pha_elig_calyear)

#### FUNCTIONS ####
### Standard recode/rename function
recode_f <- function(df) {
  
  if (is.data.table(df) == F) {
    return_df <- T
  } else if (is.data.table(df) == T) {
    return_df <- F
  } else {
    stop("df was neither data frame nor data table")
  }
  
  # Mutate_at much faster than the DT code I tried so use that then convert to DT for 
  # remaining processing
  output <- df %>%
    mutate_at(vars(starts_with("pha_")), 
              list( ~ ifelse(enroll_type %in% c("mm", "md", "me"), "Non-PHA", .)))
  
  output <- setDT(output)
  output[, ':=' (
    dual = if_else(dual == 1, "Dual eligible", "Not dual eligible"),
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
      TRUE ~ pha_portfolio)
  )]
  setnames(output, 
           c("pha_agency", "gender_me", "race_eth_me", "time_housing", "pha_subsidy",
             "pha_voucher", "pha_operator", "pha_portfolio", "geo_zip"),
           c("agency", "gender", "ethn", "length", "subsidy", 
             "voucher", "operator", "portfolio", "zip"))
  
  if ("full_criteria" %in% names(output)) {
    output[, full_criteria := case_when(full_criteria == 1 ~ "Met full criteria",
                                        full_criteria == 0 ~ "Did not meet full criteria")]
  }
  
  if ("full_criteria_12" %in% names(output)) {
    output[, full_criteria_12 := case_when(full_criteria_12 == 1 ~ "Met full criteria",
                                           full_criteria_12 == 0 ~ "Did not meet full criteria")]
  }  
  
  
  if (return_df == T) {
    output <- as.data.frame(output)
  }
  return(output)
}


### Function to pull in acute data and join to demogs
acute_events_f <- function(type = c("ed", "hosp", "inj"), summarise = T,
                           source = c("all", "mcaid")) {
  
  # NB Age taken from end of calyear table to match denom data
  # Other time-varying elements come from timevar table
  
  type <- match.arg(type)
  source <- match.arg(source)
  
  if (type == "ed") {
    top_vars <- glue_sql("a.ed_pophealth_id ", .con = db_claims51)
    vars <- glue_sql("ed_pophealth_id ", .con = db_claims51)
    where <- glue_sql("ed_pophealth_id IS NOT NULL", .con = db_claims51)
  } else if (type == "hosp") {
    top_vars <- glue_sql("a.inpatient_id ", .con = db_claims51)
    vars <- glue_sql("inpatient_id ", .con = db_claims51)
    where <- glue_sql("inpatient_id IS NOT NULL", .con = db_claims51)
  } else if (type == "inj") {
    top_vars <- glue_sql("a.intent ", .con = db_claims51)
    vars <- glue_sql("intent ", .con = db_claims51)
    where <- glue_sql("intent IS NOT NULL", .con = db_claims51)
  }
  
  sql_query <- glue_sql(
    "SELECT DISTINCT a.id_apde, a.first_service_date, YEAR(a.first_service_date) AS year, 
    {top_vars}, b.mcaid, b.mcare, 
    b.enroll_type, b.pha_agency, b.dual, b.full_benefit, b.full_criteria, 
    b.pha_voucher, b.pha_subsidy, b.pha_operator, b.pha_portfolio, b.geo_zip, 
    c.dob, c.start_housing, c.gender_me, c.race_eth_me 
    FROM
    (SELECT id_apde, MIN(first_service_date) AS first_service_date, {vars}
      FROM PHClaims.final.mcaid_mcare_claim_header
      WHERE {where}
      GROUP BY id_apde, {vars}) a
    INNER JOIN
    (SELECT id_apde, from_date, to_date, mcaid, mcare, 
      CASE WHEN mcaid = 0 AND mcare = 0 AND pha = 1 THEN 'h'
      WHEN mcaid = 1 AND mcare = 0 AND pha = 1 THEN 'hmd'
      WHEN mcaid = 0 AND mcare = 1 AND pha = 1 THEN 'hme'
      WHEN mcaid = 1 AND mcare = 0 AND pha = 0 THEN 'md'
      WHEN mcaid = 0 AND mcare = 1 AND pha = 0 THEN 'me'
      WHEN mcaid = 1 AND mcare = 1 AND pha = 0 THEN 'mm'
      WHEN mcaid = 1 AND mcare = 1 AND pha = 1 THEN 'a' END AS enroll_type, 
      pha_agency, dual, full_benefit, full_criteria, pha_voucher,
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
  
  output <- dbGetQuery(db_claims51, sql_query) %>% 
    # Add additional recodes (do this BEFORE standard recoding because of time_housing)
    mutate_at(vars(first_service_date, dob), list( ~ as.Date(.))) %>%
    mutate(age_yr = floor(interval(start = dob, 
                                   end = as.Date(paste0(year, "-12-31"), origin = "1970-01-01")) 
                          / years(1)),
           adult = case_when(age_yr >= 18 ~ 1L, age_yr < 18 ~ 0L),
           senior = case_when(age_yr >= 62 ~ 1L, age_yr < 62 ~ 0L),
           agegrp = case_when(
             age_yr < 18 ~ "<18",
             data.table::between(age_yr, 18, 24.99, NAbounds = NA) ~ "18-24",
             data.table::between(age_yr, 25, 44.99, NAbounds = NA) ~ "25-44",
             data.table::between(age_yr, 45, 64.99, NAbounds = NA) ~ "45-64",
             age_yr >= 65 ~ "65+",
             is.na(age_yr) ~ NA_character_),
           agegrp_expanded = case_when(
             age_yr < 10 ~ "<10",
             data.table::between(age_yr, 10, 17.99, NAbounds = NA) ~ "10-17",
             data.table::between(age_yr, 18, 24.99, NAbounds = NA) ~ "18-24",
             data.table::between(age_yr, 25, 44.99, NAbounds = NA) ~ "25-44",
             data.table::between(age_yr, 45, 64.99, NAbounds = NA) ~ "45-64",
             data.table::between(age_yr, 65, 74.99, NAbounds = NA) ~ "65-74",
             age_yr >= 75 ~ "75+",
             is.na(age_yr) ~ NA_character_),
           age_wc = case_when(between(age_yr, 0, 6.99) ~ "Children aged 0-6", 
                              TRUE ~ NA_character_),
           time_housing_yr = 
             round(interval(start = start_housing, end = as.Date(paste0(year, "-12-31"), origin = "1970-01-01"))
                   / years(1), 1),
           time_housing = case_when(
             is.na(pha_agency) | pha_agency == "Non-PHA" ~ "Non-PHA",
             time_housing_yr < 3 ~ "<3 years",
             between(time_housing_yr, 3, 5.99) ~ "3 to <6 years",
             time_housing_yr >= 6 ~ "6+ years",
             TRUE ~ "Unknown")
    )
  
  # Run through standardized recoding/renaming
  output <- recode_f(output)
  
  # Summarise if desired
  if (summarise == T & source == "all") {
    output <- output %>%
      # Remove the few people who had events when they appear to not be enrolled
      filter(!is.na(full_criteria)) %>%
      group_by(year, enroll_type, agency, dual, full_criteria, 
               agegrp_expanded, gender, ethn, length, 
               subsidy, voucher, operator, portfolio, zip) %>%
      summarise(count = n()) %>%
      ungroup() %>%
      rename(agegrp = agegrp_expanded) %>%
      mutate(source = "Medicaid and Medicare")
  } else if (summarise == T & source == "mcaid") {
    output <- output %>%
      # Remove the few people who had events when they appear to not be enrolled
      filter(!is.na(full_criteria) & mcare == 0) %>%
      group_by(year, enroll_type, agency, dual, full_criteria, 
               agegrp, gender, ethn, length, 
               subsidy, voucher, operator, portfolio, zip) %>%
      summarise(count = n()) %>%
      ungroup() %>%
      mutate(source = "Medicaid only")
  }
  
  return(output)
}


### Function to count # people with acute events based on prioritized pop
acute_persons_f <- function(type = c("ed", "hosp", "inj", "wc"), year = 2012,
                            summarise = T, source = c("all", "mcaid")) {
  
  type <- match.arg(type)
  source <- match.arg(source)
  
  if (type == "ed") {
    pop_sql <- DBI::SQL("")
    where_sql <- glue_sql("ed_pophealth_id IS NOT NULL", .con = db_claims51)
  } else if (type == "hosp") {
    pop_sql <- DBI::SQL("")
    where_sql <- glue_sql("inpatient_id IS NOT NULL", .con = db_claims51)
  } else if (type == "inj") {
    pop_sql <- DBI::SQL("")
    where_sql <- glue_sql("intent IS NOT NULL", .con = db_claims51)
  } else if (type == "wc") {
    pop_sql <- glue_sql(" AND age_wc IS NOT NULL", .con = db_claims51)
    where_sql <- glue_sql("claim_type_mcaid_id = 27", .con = db_claims51)
  }
  
  sql_query <- glue_sql(
    "SELECT DISTINCT a.*, ISNULL(b.event, 0) AS event 
    FROM
      (SELECT [year], id_apde, mcaid, mcare, enroll_type, pha_agency, dual,
        full_benefit, full_criteria, full_criteria_12, 
        agegrp, agegrp_expanded, gender_me, race_eth_me, time_housing, 
        pha_subsidy, pha_voucher, pha_operator, pha_portfolio, geo_zip 
        FROM PH_APDEStore.stage.mcaid_mcare_pha_elig_calyear
        WHERE [year] = {year} AND enroll_type <> 'h' AND pop = 1 {pop_sql}) a
    LEFT JOIN
      (SELECT DISTINCT id_apde, 1 AS event
      FROM PHClaims.final.mcaid_mcare_claim_header
      WHERE {where_sql} AND first_service_date <= {paste0(year, '-12-31')} AND 
        first_service_date >= {paste0(year, '-01-01')}) b
    ON a.id_apde = b.id_apde",
    .con = db_apde51)
  
  # Run query
  output <- dbGetQuery(db_claims51, sql_query)
  
  # Run through standardized recoding/renaming
  output <- recode_f(output)
  
  # Summarise if desired
  if (summarise == T & source == "all") {
    output <- output %>%
      filter(full_criteria_12 == "Met full criteria") %>%
      group_by(year, enroll_type, agency, dual, full_criteria_12, 
               agegrp_expanded, gender, ethn, length, 
               subsidy, voucher, operator, portfolio, zip) %>%
      summarise(count := sum(event)) %>%
      ungroup() %>%
      rename(agegrp = agegrp_expanded) %>%
      mutate(source = "Medicaid and Medicare")
  } else if (summarise == T & source == "mcaid") {
    output <- output %>%
      filter(full_criteria_12 == "Met full criteria" & mcare == 0) %>%
      group_by(year, enroll_type, agency, dual, full_criteria_12, 
             agegrp, gender, ethn, length, 
             subsidy, voucher, operator, portfolio, zip) %>%
      summarise(count := sum(event)) %>%
      ungroup() %>%
      mutate(source = "Medicaid only")
  }
  
  return(output)
}


### Function to pull in and summarize chronic conditions
eventcount_chronic_f <- function(condition = NULL, year = 2012, cvd = F,
                                 summarise = T, source = c("all", "mcaid")) {
  
  source <- match.arg(source)
  
  if (cvd == F) {
    where_sql <- glue_sql("ccw_desc = {condition}", .con = db_apde51)
  } else if (cvd == T) {
    where_sql <- glue_sql("ccw_desc IN ('ccw_heart_failure', 'ccw_hypertension',
                          'ccw_ischemic_heart_dis', 'ccw_mi')", .con = db_apde51)
  }
  
  sql_query <- glue_sql(
    "SELECT DISTINCT a.*, ISNULL(b.condition, 0) AS condition 
    FROM
      (SELECT year, id_apde, mcaid, mcare, enroll_type, pha_agency, dual,
        full_benefit, full_criteria, full_criteria_12, 
        agegrp, agegrp_expanded, gender_me, race_eth_me, time_housing, 
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
  
  # Run query
  output <- dbGetQuery(db_apde51, sql_query)
  
  # Run through standardized recoding/renaming
  output <- recode_f(output)
  
  # Summarise if desired
  if (summarise == T & source == "all") {
    output <- output %>%
      filter(full_criteria_12 == "Met full criteria") %>%
      group_by(year, enroll_type, agency, dual, full_criteria_12, 
               agegrp_expanded, gender, ethn, length, 
             subsidy, voucher, operator, portfolio, zip) %>%
      summarise(count := sum(condition)) %>%
      ungroup() %>%
      rename(agegrp = agegrp_expanded) %>%
      mutate(source = "Medicaid and Medicare")
  } else if (summarise == T & source == "mcaid") {
    output <- output %>%
      filter(full_criteria_12 == "Met full criteria" & mcare == 0) %>%
      group_by(year, enroll_type, agency, dual, full_criteria_12, 
             agegrp, gender, ethn, length, 
             subsidy, voucher, operator, portfolio, zip) %>%
      summarise(count := sum(condition)) %>%
      ungroup() %>%
      mutate(source = "Medicaid only")
  }
  
  return(output)
}


#### ENROLLMENT POPULATION ####
# Run different age groups for Medicaid/Medicare vs Medicaid-only data
# Also differentiate between pop counts for enrollment vs denominator data
# Denominator data excludes people on Medicare and duals for the Medicaid-only data
pop_enroll_mm <- mcaid_mcare_pha_elig_calyear[year <= 2017 & pop == 1]
pop_enroll_mm <- pop_enroll_mm[, .(pop = sum(pop), pt = sum(pt_tot)), 
                               by = .(year, enroll_type, pha_agency, dual, full_benefit, full_criteria_12, 
                                      agegrp_expanded, gender_me, race_eth_me, pha_subsidy, pha_voucher, pha_operator, 
                                      pha_portfolio, time_housing, geo_zip)]
pop_enroll_mm[, ':=' (wc_flag = 0L, source = "Medicaid and Medicare", for_enrollment = 1L)]
setnames(pop_enroll_mm, "agegrp_expanded", "agegrp")


pop_enroll_md <- mcaid_mcare_pha_elig_calyear[(pha == 1 | mcaid == 1) & pop == 1]
pop_enroll_md <- pop_enroll_md[, .(pop = sum(pop), pt = sum(pt_tot)), 
                               by = .(year, enroll_type, pha_agency, dual, full_benefit, full_criteria_12, 
                                      agegrp, gender_me, race_eth_me, pha_subsidy, pha_voucher, pha_operator, 
                                      pha_portfolio, time_housing, geo_zip)]
pop_enroll_md[, ':=' (wc_flag = 0L, source = "Medicaid only", for_enrollment = 1L)]


pop_enroll_md_health <- mcaid_mcare_pha_elig_calyear[(pha == 1 | mcaid == 1) & pop == 1 & mcare == 0 & dual == 0]
pop_enroll_md_health <- pop_enroll_md_health[, .(pop = sum(pop), pt = sum(pt_tot)), 
                                             by = .(year, enroll_type, pha_agency, dual, full_benefit, full_criteria_12, 
                                                    agegrp, gender_me, race_eth_me, pha_subsidy, pha_voucher, pha_operator, 
                                                    pha_portfolio, time_housing, geo_zip)]
pop_enroll_md_health[, ':=' (wc_flag = 0L, source = "Medicaid only", for_enrollment = 0L)]


### Repeat for well-child population
pop_enroll_md_wc <- mcaid_mcare_pha_elig_calyear[(pha == 1 | mcaid == 1) & pop == 1 & !is.na(age_wc)]
pop_enroll_md_wc <- pop_enroll_md_wc[, .(pop = sum(pop), pt = sum(pt_tot)), 
                                     by = .(year, enroll_type, pha_agency, dual, full_benefit, full_criteria_12, 
                                            age_wc, gender_me, race_eth_me, pha_subsidy, pha_voucher, pha_operator, 
                                            pha_portfolio, time_housing, geo_zip)]
pop_enroll_md_wc[, ':=' (wc_flag = 1L, source = "Medicaid only", for_enrollment = 1L)]
setnames(pop_enroll_md_wc, "age_wc", "agegrp")


pop_enroll_md_wc_health <- mcaid_mcare_pha_elig_calyear[(pha == 1 | mcaid == 1) & pop == 1 & 
                                                          mcare == 0 & dual == 0 & !is.na(age_wc)]
pop_enroll_md_wc_health <- pop_enroll_md_wc_health[, .(pop = sum(pop), pt = sum(pt_tot)), 
                                                   by = .(year, enroll_type, pha_agency, dual, full_benefit, full_criteria_12, 
                                                          age_wc, gender_me, race_eth_me, pha_subsidy, pha_voucher, pha_operator, 
                                                          pha_portfolio, time_housing, geo_zip)]
pop_enroll_md_wc_health[, ':=' (wc_flag = 1L, source = "Medicaid only", for_enrollment = 0L)]
setnames(pop_enroll_md_wc_health, "age_wc", "agegrp")


### Combine into one
pop_enroll_all <- bind_rows(pop_enroll_mm, pop_enroll_md, pop_enroll_md_health, 
                            pop_enroll_md_wc, pop_enroll_md_wc_health)
pop_enroll_all <- recode_f(pop_enroll_all)
pop_enroll_all <- pop_enroll_all %>% select(for_enrollment, source, wc_flag, year:pt)
pop_enroll_all <- setDT(pop_enroll_all)


### Collapse into bivariate totals
# Use tabloop_f from medicaid package to summarise

# This is using a horrible mix of quosures and lists for now.
# The tabloop function will be rewritten some day to make this easier.
fixed_list <- list_var(source, wc_flag, year, agency, enroll_type, dual)
loop_list <- c("agegrp", "gender", "ethn", "voucher", "subsidy", "operator", 
               "portfolio", "length", "zip")

pop_enroll_bivariate <- bind_rows(lapply(loop_list, function(x) {
  
  # Restrict to rows for enrollment purposes
  input <- pop_enroll_all %>% filter(for_enrollment == 1)
  
  # Set up quosure for each loop var
  loop_quo <- rlang::as_quosure(rlang::sym(x), env = environment()) 
  
  ### Make new versions of each list to feed to tabloop
  # This is super clunky but should work
  fixed_list_new <- rlang::new_quosures(c(fixed_list, loop_quo))
  
  loop_list_new <- loop_list[!loop_list %in% x]
  loop_list_new <- rlang::as_quosures(rlang::syms(loop_list_new), env = environment())
  
  ### Run tabloop
  output <- tabloop_f(input, sum = list_var(pop, pt),
                      fixed = fixed_list_new, loop = loop_list_new) %>%
    mutate(category1 = rlang::quo_name(x)) %>%
    # Remove rows with zero count to save memory
    filter(pt_sum > 0) %>%
    rename(group1 = !!x, category2 = group_cat, group2 = group)
  
  return(output)
}))


### Make total columns for univariate analyses
# Make this a set of quosures for expediency's sake
loop_list <- list_var(agegrp, gender, ethn, voucher, subsidy, operator, portfolio, length, zip)

pop_enroll_total <- bind_rows(lapply(loop_list, function(x) {
  output <- pop_enroll_all %>%
    filter(for_enrollment == 1) %>%
    group_by(source, wc_flag, year, agency, enroll_type, dual, !!x) %>%
    summarise(pop_sum = sum(pop, na.rm = T), pt_sum = sum(pt, na.rm = T)) %>%
    ungroup() %>%
    mutate(
      category1 = quo_name(x), group1 = !!x,
      category2 = "total", group2 = "total") %>%
    select(-(!!x))
  
  return(output)
}))

rm(fixed_list, loop_list)


### Combine into one
pop_enroll_combine_bivar <- bind_rows(pop_enroll_bivariate, pop_enroll_total) %>%
  filter(pt_sum > 0) %>%
  select(source, wc_flag, year, agency, enroll_type, dual, category1, group1, 
         category2, group2, pop_sum, pt_sum) %>%
  rename(pop = pop_sum, pt = pt_sum)


# Add suppression and data source
pop_enroll_combine_bivar <- pop_enroll_combine_bivar %>%
  mutate(pop_supp_flag = if_else(between(pop, 1, 10), 1, 0),
         pop_supp = if_else(between(pop, 1, 10), NA_real_, pop))


### Make suppressed version
pop_enroll_combine_bivar_suppressed <- pop_enroll_combine_bivar %>%
  select(source:group2, pt, pop_supp, pop_supp_flag) %>%
  rename(pop = pop_supp)


### Write to SQL
DBI::dbWriteTable(db_extractstore51,
                  name = DBI::Id(schema = "APDE_WIP", table = "mcaid_mcare_pha_enrollment"),
                  value = pop_enroll_combine_bivar_suppressed,
                  append = F, overwrite = T,
                  field.types = c(year = "integer", pt = "integer", pop = "integer", 
                                  pop_supp_flag = "integer", wc_flag = "integer"))



#### POPULATION TO JOIN TO ACUTE/CHRONIC DATA ####
# For looking at enrollment alone, we disaggregate by additional factors 
#    (e.g., enrollment type)
# For health outcomes, especially from the Medicaid/Medicare data, we have a shorter
#    list. Therefore need to rerun population data again. 
# Need to filter out people only enrolled in housing
# Also need to change grouping based on source

### First set up acute populations
pop_acute_mm <- mcaid_mcare_pha_elig_calyear[year <= 2017 & pop_ever == 1]
pop_acute_mm <- pop_acute_mm[, .(pop_ever = sum(pop_ever), pt = sum(pt)), 
                             by = .(year, enroll_type, pha_agency, dual, full_benefit, full_criteria, 
                                    agegrp_expanded, gender_me, race_eth_me, pha_subsidy, pha_voucher, pha_operator, 
                                    pha_portfolio, time_housing, geo_zip)]
pop_acute_mm[, ':=' (wc_flag = 0L, source = "Medicaid and Medicare")]
setnames(pop_acute_mm, "agegrp_expanded", "agegrp")


pop_acute_md <- mcaid_mcare_pha_elig_calyear[(pha == 1 | mcaid == 1) & pop_ever == 1 & mcare == 0 & dual == 0]
pop_acute_md <- pop_acute_md[, .(pop_ever = sum(pop_ever), pt = sum(pt)), 
                             by = .(year, enroll_type, pha_agency, dual, full_benefit, full_criteria, 
                                    agegrp, gender_me, race_eth_me, pha_subsidy, pha_voucher, pha_operator, 
                                    pha_portfolio, time_housing, geo_zip)]
pop_acute_md[, ':=' (wc_flag = 0L, source = "Medicaid only")]


pop_acute_md_wc <- mcaid_mcare_pha_elig_calyear[(pha == 1 | mcaid == 1) & pop_ever == 1 & 
                                                  mcare == 0 & dual == 0 & !is.na(age_wc)]
pop_acute_md_wc <- pop_acute_md_wc[, .(pop_ever = sum(pop_ever), pt = sum(pt)), 
                                   by = .(year, enroll_type, pha_agency, dual, full_benefit, full_criteria, 
                                          age_wc, gender_me, race_eth_me, pha_subsidy, pha_voucher, pha_operator, 
                                          pha_portfolio, time_housing, geo_zip)]
pop_acute_md_wc[, ':=' (wc_flag = 1L, source = "Medicaid only")]
setnames(pop_acute_md_wc, "age_wc", "agegrp")


# Combine into one
pop_acute_all <- bind_rows(pop_acute_mm, pop_acute_md, pop_acute_md_wc)
pop_acute_all <- recode_f(pop_acute_all)
pop_acute_all <- pop_acute_all %>% select(source, wc_flag, year:pt)
pop_acute_all <- setDT(pop_acute_all)


### Set up sources to loop over
fixed_list_chronic <- list_var(source, wc_flag, year, full_criteria_12, agency, dual)
fixed_list_acute <- list_var(source, wc_flag, year, full_criteria, agency, dual)
loop_list <- c("agegrp", "gender", "ethn", "subsidy", "voucher", "operator", "portfolio", "length", "zip")

pop_health_bivariate <- bind_rows(lapply(loop_list, function(x) {
  
  # Set up quosure for each loop var
  loop_quo <- rlang::as_quosure(rlang::sym(x), env = environment()) 
  
  ### Make new versions of each list to feed to tabloop
  # This is super clunky but should work
  fixed_list_chronic_new <- rlang::new_quosures(c(fixed_list_chronic, loop_quo))
  fixed_list_acute_new <- rlang::new_quosures(c(fixed_list_acute, loop_quo))
  loop_list_new <- loop_list[!loop_list %in% x]
  loop_list_new <- rlang::as_quosures(rlang::syms(loop_list_new), env = environment())
  
  
  ### RUN FOR CHRONIC CONDITIONS
  # Set up data frame with appropriate filtering
  input_chronic <- pop_enroll_all %>% 
    filter(!enroll_type %in% c("h", "Housing only") &
             (for_enrollment == 1 | source == "Medicaid and Medicare"))
  
  # Run inner loop
  output_chronic <- tabloop_f(input_chronic, sum = list_var(pop, pt),
                              fixed = fixed_list_chronic_new, loop = loop_list_new) %>%
    mutate(category1 = rlang::quo_name(x)) %>%
    # Remove rows with zero count to save memory
    filter(pt_sum > 0) %>%
    rename(group1 = !!x, category2 = group_cat, group2 = group)
  
  
  ### RUN FOR ACUTE CONDITIONS
  # Set up data frame with appropriate filtering
  input_acute <- pop_acute_all %>% filter(!enroll_type %in% c("h", "Housing only"))
  
  # Run inner loop
  output_acute <- tabloop_f(input_acute, sum = list_var(pop_ever, pt),
                            fixed = fixed_list_acute_new, loop = loop_list_new) %>%
    mutate(category1 = rlang::quo_name(x)) %>%
    # Remove rows with zero count to save memory
    filter(pt_sum > 0) %>%
    rename(group1 = !!x, category2 = group_cat, group2 = group)
  
  
  output <- bind_rows(output_chronic, output_acute)
  return(output)
}))


### Make total columns for univariate analyses
# Make this a set of quosures for expediency's sake
loop_list <- list_var(agegrp, gender, ethn, voucher, subsidy, operator, portfolio, length, zip)
pop_health_univariate <- bind_rows(lapply(loop_list, function(x) {
  ### RUN FOR CHRONIC CONDITIONS
  output_chronic <- pop_enroll_all %>%
    filter(!enroll_type %in% c("h", "Housing only") & 
             (for_enrollment == 1 | source == "Medicaid and Medicare")) %>%
    group_by(source, year, wc_flag, full_criteria_12, agency, dual, !!x) %>%
    summarise(pop_sum = sum(pop, na.rm = T), pt_sum = sum(pt, na.rm = T)) %>%
    ungroup() %>%
    mutate(
      category1 = quo_name(x), group1 = !!x,
      category2 = "total", group2 = "total") %>%
    select(-(!!x))
  
  ### RUN FOR ACUTE CONDITIONS
  output_acute <- pop_acute_all %>%
    filter(!enroll_type %in% c("h", "Housing only")) %>%
    group_by(source, year, wc_flag, full_criteria, agency, dual, !!x) %>%
    summarise(pop_ever_sum = sum(pop_ever, na.rm = T), pt_sum = sum(pt, na.rm = T)) %>%
    ungroup() %>%
    mutate(
      category1 = quo_name(x), group1 = !!x,
      category2 = "total", group2 = "total") %>%
    select(-(!!x))
  
  output <- bind_rows(output_chronic, output_acute)
  return(output)
}))


### Make overall columns for each agency
pop_health_total_chronic <- pop_enroll_all[!enroll_type %in% c("h", "Housing only") & 
                                             (for_enrollment == 1 | source == "Medicaid and Medicare")]
pop_health_total_chronic <- pop_health_total_chronic[, .(pop_sum = sum(pop, na.rm = T), pt_sum = sum(pt, na.rm = T)), 
                                                     by = .(source, year, wc_flag, full_criteria_12, agency, dual)]
pop_health_total_chronic[, ':=' (category1 = "overall", group1 = "overall",
                                 category2 = "total", group2 = "total")]


pop_health_total_acute <- pop_acute_all[!enroll_type %in% c("h", "Housing only")]
pop_health_total_acute <- pop_health_total_acute[, .(pop_ever_sum = sum(pop_ever, na.rm = T), pt_sum = sum(pt, na.rm = T)), 
                                                 by = .(source, year, wc_flag, full_criteria, agency, dual)]
pop_health_total_acute[, ':=' (category1 = "overall", group1 = "overall",
                               category2 = "total", group2 = "total")]


### Combine into one
pop_health_combine <- bind_rows(pop_health_bivariate, pop_health_univariate, 
                                pop_health_total_chronic, pop_health_total_acute) %>%
  filter(pt_sum > 0) %>%
  mutate(full_criteria = ifelse(is.na(full_criteria), full_criteria_12, full_criteria)) %>%
  select(source, year, wc_flag, full_criteria, agency, dual, category1, group1, category2, group2, 
         pop_ever_sum, pop_sum, pt_sum) %>%
  rename(pop_ever = pop_ever_sum, pop = pop_sum, pt = pt_sum)


# Add suppression flags and data source
# Keep all vars for now because pop is needed for rest_pha calcs
pop_health_combine <- pop_health_combine %>%
  # Remove combinations we won't show (e.g., duals when source = Medicaid)
  filter(full_criteria == "Met full criteria" & 
           (source == "Medicaid and Medicare" | 
              (source == "Medicaid only" & dual == "Not dual eligible"))) %>%
  mutate_at(vars(pop_ever, pop),
            list(supp_flag = ~ if_else(between(., 1, 10), 1, 0))) %>%
  mutate_at(vars(pop_ever, pop),
            list(supp = ~ if_else(between(., 1, 10), NA_real_, .)))


#### CLEAN UP ####
rm(fixed_list, loop_list)
rm(pop_enroll_mm, pop_enroll_md, pop_enroll_md_health, 
   pop_enroll_md_wc, pop_enroll_md_wc_health)
rm(pop_enroll_all, pop_enroll_bivariate, pop_enroll_total)
rm(pop_acute_mm, pop_acute_md, pop_acute_md_wc)
rm(pop_health_bivariate, pop_health_univariate, 
   pop_health_total_chronic, pop_health_total_acute)


#### END POPULATION ####


#### PERSON-TIME BASED EVENTS ####
### Hospitalizations
hosp_events_all <- acute_events_f(type = "hosp", source = "all") %>% 
  mutate(indicator = "Hospitalizations")

hosp_events_mcaid <- acute_events_f(type = "hosp", source = "mcaid") %>% 
  mutate(indicator = "Hospitalizations")

### ED visits
ed_events_all <- acute_events_f(type = "ed", source = "all") %>% 
  mutate(indicator = "ED visits")

ed_events_mcaid <- acute_events_f(type = "ed", source = "mcaid") %>% 
  mutate(indicator = "ED visits")

### Injuries
# Only for mcaid at the moment
inj_events_mcaid <- acute_events_f(type = "inj", source = "mcaid") %>% 
  mutate(indicator = "Injuries")


#### PERSON-BASED ACUTE EVENTS ####
### Hospitalizations
hosp_pers_all <- bind_rows(lapply(seq(2012, 2017), acute_persons_f, 
                              type = "hosp", source = "all")) %>%
  mutate(indicator = "Persons with hospitalizations")

hosp_pers_mcaid <- bind_rows(lapply(seq(2012, 2018), acute_persons_f, 
                                  type = "hosp", source = "mcaid")) %>%
  mutate(indicator = "Persons with hospitalizations")


### ED visits
ed_pers_all <- bind_rows(lapply(seq(2012, 2017), acute_persons_f, 
                                  type = "ed", source = "all")) %>%
  mutate(indicator = "Persons with ED visits")

ed_pers_mcaid <- bind_rows(lapply(seq(2012, 2018), acute_persons_f, 
                                    type = "ed", source = "mcaid")) %>%
  mutate(indicator = "Persons with ED visits")


### Injuries
inj_pers_mcaid <- bind_rows(lapply(seq(2012, 2018), acute_persons_f, 
                                  type = "inj", source = "mcaid")) %>%
  mutate(indicator = "Persons with injuries")


### Well-child checks
wc_pers_all <- bind_rows(lapply(seq(2012, 2017), acute_persons_f, 
                                 type = "wc", source = "all")) %>%
  mutate(indicator = "Persons with well-child checks")

wc_pers_mcaid <- bind_rows(lapply(seq(2012, 2018), acute_persons_f, 
                                   type = "wc", source = "mcaid")) %>%
  mutate(indicator = "Persons with well-child checks")


#### CHRONIC CONDITIONS ####
### Alzheimer's related
alzheimer_all <- bind_rows(lapply(seq(2012, 2017), eventcount_chronic_f, 
                                   condition = "ccw_alzheimer_related", cvd = F,
                                   source = "all")) %>%
  mutate(indicator = "Persons with Alzheimer's-related conditions")

alzheimer_mcaid <- bind_rows(lapply(seq(2012, 2018), eventcount_chronic_f, 
                                       condition = "ccw_alzheimer_related", cvd = F,
                                       source = "mcaid")) %>%
  mutate(indicator = "Persons with Alzheimer's-related conditions")


### Asthma
asthma_all <- bind_rows(lapply(seq(2012, 2017), eventcount_chronic_f, 
                                       condition = "ccw_asthma", cvd = F,
                                       source = "all")) %>%
  mutate(indicator = "Persons with asthma")

asthma_mcaid <- bind_rows(lapply(seq(2012, 2018), eventcount_chronic_f, 
                                         condition = "ccw_asthma", cvd = F,
                                         source = "mcaid")) %>%
  mutate(indicator = "Persons with asthma")


### Cancer - breast
cancer_breast_all <- bind_rows(lapply(seq(2012, 2017), eventcount_chronic_f, 
                                    condition = "ccw_cancer_breast", cvd = F,
                                    source = "all")) %>%
  mutate(indicator = "Persons with cancer: breast")

cancer_breast_mcaid <- bind_rows(lapply(seq(2012, 2018), eventcount_chronic_f, 
                                      condition = "ccw_cancer_breast", cvd = F,
                                      source = "mcaid")) %>%
  mutate(indicator = "Persons with cancer: breast")


### Cancer - colorectal
cancer_colorectal_all <- bind_rows(lapply(seq(2012, 2017), eventcount_chronic_f, 
                                      condition = "ccw_cancer_colorectal", cvd = F,
                                      source = "all")) %>%
  mutate(indicator = "Persons with cancer: breast")

cancer_colorectal_mcaid <- bind_rows(lapply(seq(2012, 2018), eventcount_chronic_f, 
                                        condition = "ccw_cancer_colorectal", cvd = F,
                                        source = "mcaid")) %>%
  mutate(indicator = "Persons with cancer: colorectal")


### CHF
chf_all <- bind_rows(lapply(seq(2012, 2017), eventcount_chronic_f, 
                                          condition = "ccw_heart_failure", cvd = F,
                                          source = "all")) %>%
  mutate(indicator = "Persons with cardiovascular disease: congestive heart failure")

chf_mcaid <- bind_rows(lapply(seq(2012, 2018), eventcount_chronic_f, 
                                            condition = "ccw_heart_failure", cvd = F,
                                            source = "mcaid")) %>%
  mutate(indicator = "Persons with cardiovascular disease: congestive heart failure")


# COPD
copd_all <- bind_rows(lapply(seq(2012, 2017), eventcount_chronic_f, 
                            condition = "ccw_copd", cvd = F,
                            source = "all")) %>%
  mutate(indicator = "Persons with chronic obstructive pulmonary disease")

copd_mcaid <- bind_rows(lapply(seq(2012, 2018), eventcount_chronic_f, 
                              condition = "ccw_copd", cvd = F,
                              source = "mcaid")) %>%
  mutate(indicator = "Persons with chronic obstructive pulmonary disease")


### CVD
cvd_all <- bind_rows(lapply(seq(2012, 2017), eventcount_chronic_f, 
                            condition = "", cvd = T,
                            source = "all")) %>%
  mutate(indicator = "Persons with cardiovascular disease: any type")

cvd_mcaid <- bind_rows(lapply(seq(2012, 2018), eventcount_chronic_f, 
                              condition = "", cvd = T,
                              source = "mcaid")) %>%
  mutate(indicator = "Persons with cardiovascular disease: any type")


### Depression
depression_all <- bind_rows(lapply(seq(2012, 2017), eventcount_chronic_f, 
                             condition = "ccw_depression", cvd = F,
                             source = "all")) %>%
  mutate(indicator = "Persons with depression")

depression_mcaid <- bind_rows(lapply(seq(2012, 2018), eventcount_chronic_f, 
                               condition = "ccw_depression", cvd = F,
                               source = "mcaid")) %>%
  mutate(indicator = "Persons with depression")


### Diabetes
diabetes_all <- bind_rows(lapply(seq(2012, 2017), eventcount_chronic_f, 
                                   condition = "ccw_diabetes", cvd = F,
                                   source = "all")) %>%
  mutate(indicator = "Persons with diabetes")

diabetes_mcaid <- bind_rows(lapply(seq(2012, 2018), eventcount_chronic_f, 
                                     condition = "ccw_diabetes", cvd = F,
                                     source = "mcaid")) %>%
  mutate(indicator = "Persons with diabetes")


### Hypertension
hypertension_all <- bind_rows(lapply(seq(2012, 2017), eventcount_chronic_f, 
                            condition = "ccw_hypertension", cvd = F,
                            source = "all")) %>%
  mutate(indicator = "Persons with cardiovascular disease: hypertension")

hypertension_mcaid <- bind_rows(lapply(seq(2012, 2018), eventcount_chronic_f, 
                              condition = "ccw_hypertension", cvd = F,
                              source = "mcaid")) %>%
  mutate(indicator = "Persons with cardiovascular disease: hypertension")


### IHD
ihd_all <- bind_rows(lapply(seq(2012, 2017), eventcount_chronic_f, 
                            condition = "ccw_ischemic_heart_dis", cvd = F,
                            source = "all")) %>%
  mutate(indicator = "Persons with cardiovascular disease: ischemic heart disease")

ihd_mcaid <- bind_rows(lapply(seq(2012, 2018), eventcount_chronic_f, 
                              condition = "ccw_ischemic_heart_dis", cvd = F,
                              source = "mcaid")) %>%
  mutate(indicator = "Persons with cardiovascular disease: ischemic heart disease")


### Kidney disease
kidney_all <- bind_rows(lapply(seq(2012, 2017), eventcount_chronic_f, 
                                 condition = "ccw_chr_kidney_dis", cvd = F,
                                 source = "all")) %>%
  mutate(indicator = "Persons with kidney disease")

kidney_mcaid <- bind_rows(lapply(seq(2012, 2018), eventcount_chronic_f, 
                                   condition = "ccw_chr_kidney_dis", cvd = F,
                                   source = "mcaid")) %>%
  mutate(indicator = "Persons with kidney disease")


### Myocardial infarction
mi_all <- bind_rows(lapply(seq(2012, 2017), eventcount_chronic_f, 
                                     condition = "ccw_mi", cvd = F,
                                     source = "all")) %>%
  mutate(indicator = "Persons with cardiovascular disease: myocardial infarction")

mi_mcaid <- bind_rows(lapply(seq(2012, 2018), eventcount_chronic_f, 
                                       condition = "ccw_mi", cvd = F,
                                       source = "mcaid")) %>%
  mutate(indicator = "Persons with cardiovascular disease: myocardial infarction")


#### COMBINE DATA ####
health_events <- bind_rows(alzheimer_all, asthma_all, cancer_breast_all,
                           cancer_colorectal_all, chf_all, copd_all, cvd_all, 
                           depression_all, diabetes_all, hypertension_all, 
                           ihd_all, kidney_all, mi_all,
                           hosp_events_all, ed_events_all,
                           hosp_pers_all, ed_pers_all, wc_pers_all,
                           alzheimer_mcaid, asthma_mcaid, cancer_breast_mcaid,
                           cancer_colorectal_mcaid, chf_mcaid, copd_mcaid, cvd_mcaid, 
                           depression_mcaid, diabetes_mcaid, hypertension_mcaid, 
                           ihd_mcaid, kidney_mcaid, mi_mcaid,
                           hosp_events_mcaid, ed_events_mcaid, inj_events_mcaid,
                           hosp_pers_mcaid, ed_pers_mcaid, inj_pers_mcaid, wc_pers_mcaid) %>%
  mutate(full_criteria = ifelse(is.na(full_criteria), full_criteria_12, full_criteria)) %>%
  select(-full_criteria_12) %>%
  # remove the few people who had acute events when they appeared to only be enrolled
  # in housing
  filter(!enroll_type %in% c("h", "Housing only")) %>%
  # Remove events for years where we don't have pop data
  filter(year < 2019) %>%
  # Remove combinations we won't show (e.g., duals when source = Medicaid)
  filter(full_criteria == "Met full criteria" & 
           (source == "Medicaid and Medicare" | 
              (source == "Medicaid only" & dual == "Not dual eligible")))

# rm(list = ls(pattern = "_all$"))
# rm(list = ls(pattern = "_mcaid$"))


#### AGGREGATION ####
# Don't loop over ZIP because not using data (and memory intensive)

# This is using a horrible mix of quosures and lists for now.
# The tabloop function will be rewritten some day to make this easier.
fixed_list <- list_var(source, indicator, year, full_criteria, agency, dual)
loop_list <- c("agegrp", "gender", "ethn", "subsidy", "voucher", "operator", "portfolio", "length")


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
    filter(count_sum > 0) %>%
    mutate(category1 = rlang::quo_name(x)) %>%
    rename(group1 = !!x, category2 = group_cat, group2 = group)
  
  return(output)
}))


# Make this a set of quosures for expediency's sake
loop_list <- list_var(agegrp, gender, ethn, voucher, subsidy, operator, portfolio, length, zip)

### Make total columns for univariate analyses
health_events_univariate <- bind_rows(lapply(loop_list, function(x) {
  output <- health_events %>%
    group_by(source, indicator, year, full_criteria, agency, dual, !!x) %>%
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
  group_by(source, indicator, year, full_criteria, agency, dual) %>%
  summarise(count_sum = sum(count)) %>%
  ungroup() %>%
  mutate(
    category1 = "overall", group1 = "overall",
    category2 = "total", group2 = "total")


#### Combine into one data frame ####
health_events_combined <- bind_rows(health_events_bivariate, health_events_univariate, health_events_total) %>%
  # TEMP: filter out source/year combos with no populations
  filter((source == "Medicaid and Medicare" & year < 2018) |
           (source == "Medicaid only" & year < 2019)) %>%
  rename(count = count_sum) %>%
  mutate(wc_flag = if_else(str_detect(indicator, "(W|w)ell-child check"), 1L, 0L),
         acute = case_when(
           str_detect(indicator, "Persons with") ~ 0L,
           indicator %in% c("Hospitalizations", "ED visits", "Injuries") ~ 1L,
           TRUE ~ NA_integer_)) %>%
  select(source, acute, wc_flag, indicator, year, full_criteria, agency, dual, category1, group1:count) %>%
  arrange(source, indicator, year, full_criteria, agency, dual, category1, group1, category2, group2)


# Add suppression
health_events_combined <- health_events_combined %>%
  mutate(suppressed = if_else(between(count, 1, 10), 1, 0),
         count_supp = if_else(between(count, 1, 10), NA_real_, count)) 



#### COMBINE WITH POP DATA ####
# Need to separate acute and chronic pop joins
# Set up pop frame with all indicators

### Acute
pop_health_combine_acute <- pop_health_combine %>% 
  filter(!is.na(pop_ever) & wc_flag == 0 &
           # Remove bivariate rows with ZIP since events are not aggregated this way
           category2 != "zip" & !(category1 == "zip" & category2 != "total"))
pop_health_combine_acute <- cbind(
  pop_health_combine_acute, 
  indicator = rep(unique(health_events_combined$indicator[health_events_combined$acute == 1]), 
                  each = nrow(pop_health_combine_acute))) %>%
  # Remove injuries from mcaid/mcare rows since not available
  filter(!(source == "Medicaid and Medicare" & indicator == "Injuries"))

health_events_combined_pop_acute <- left_join(
  pop_health_combine_acute,
  filter(health_events_combined, acute == 1), 
  by = c("source", "indicator", "year", "wc_flag", "full_criteria", "agency", 
         "dual", "category1", "group1", "category2", "group2"))

health_events_combined_pop_acute %>% group_by(acute) %>% summarise(count = n())
health_events_combined_pop_acute %>% 
  filter(is.na(acute) & 
           !(indicator == "ED visits" & year %in% c(2012, 2013) & 
               (group1 %in% c("Asian_PI", "Other") | group2 %in% c("Asian_PI", "Other"))) &
           !(indicator == "Hospitalizations" & year %in% c(2012) & 
               (group1 %in% c("Asian_PI", "Other") | group2 %in% c("Asian_PI", "Other")))
         ) %>% 
  arrange(-pop_ever) %>% head()

health_events_combined_pop_acute %>% 
  filter(source == "Medicaid and Medicare" & year == 2013 & group1 == "Asian_PI" & 
          category2 == "length" & group2 == "Non-PHA" & agency == "Non-PHA" & dual == "Not dual eligible")

health_events_combined %>% 
  filter(source == "Medicaid and Medicare" & year == 2013 & group1 == "Asian_PI" & 
           category2 == "length" & group2 == "Non-PHA" & agency == "Non-PHA" & 
           dual == "Not dual eligible")

health_events %>% 
  
  ed_events_all %>%
  filter(source == "Medicaid and Medicare" & 
           ethn %in% c("Asian_PI", "Other") & length == "Non-PHA" & dual == "Not dual eligible") %>%
  group_by(ethn, year) %>%
  summarise(count = sum(count)) %>% as.data.frame()


### Chronic
pop_health_combine_chronic <- pop_health_combine %>% filter(!is.na(pop))
pop_health_combine_chronic <- cbind(
  pop_health_combine_chronic, 
  indicator = rep(unique(health_events_combined$indicator[health_events_combined$acute == 0]), 
                  each = nrow(pop_health_combine_chronic)))

health_events_combined_pop_chronic <- left_join(
  pop_health_combine_chronic,
  filter(health_events_combined, acute == 0), 
  by = c("source", "indicator", "year", "wc_flag", "full_criteria", "agency", 
         "dual", "category1", "group1", "category2", "group2"))

health_events_combined_pop <- bind_rows(health_events_combined_pop_acute,
                                        health_events_combined_pop_chronic) %>%
  mutate(suppressed = ifelse(is.na(count), 1L, 0L),
         count = ifelse(is.na(count), 0L, count))

rm(pop_health_combine_acute, health_events_combined_pop_acute, 
   pop_health_combine_chronic, health_events_combined_pop_chronic)

# FVery slow but seems to work (~6.5 mins)
# Adapted from here: 
# https://stackoverflow.com/questions/49222353/how-to-use-purrrs-map-function-to-perform-row-wise-prop-tests-and-add-results-t

health_events_combined_pop <- health_events_combined_pop %>%
  mutate(
    denominator = ifelse(year %in% c(2012, 2016), pt / 366, pt / 365),
    rate = case_when(
      acute == 1 ~ count / denominator * 1000,
      acute == 0 & (is.na(pop) | pop == 0) ~ NA_real_,
      acute == 0 ~ count / pop * 1000)) %>%
  mutate(chronic_cols = map2(count, pop, 
                             ~ if (is.na(.y) | .y < 1) {NA} else {prop.test(.x, n = .y, correct = F)})) %>%
  mutate(ci_lb_chronic = map_dbl(chronic_cols, function(x) {
    if (max(is.na(x)) == 1) {NA} else {x[["conf.int"]][[1]]}}),
         ci_ub_chronic = map_dbl(chronic_cols, function(x) {
           if (max(is.na(x)) == 1) {NA} else {x[["conf.int"]][[2]]}})) %>%
  select(-chronic_cols) %>%
  mutate(acute_cols = map2(count, denominator, 
                           ~ if (is.na(.y) | .y < 1) {NA} else {poisson.test(.x, T = .y)})) %>% 
  mutate(ci_lb_acute = map_dbl(acute_cols, function(x) {
    if (max(is.na(x)) == 1) {NA} else (x[["conf.int"]][[1]])}),
         ci_ub_acute = map_dbl(acute_cols, function(x) {
           if (max(is.na(x)) == 1) {NA} else (x[["conf.int"]][[2]])})) %>%
  select(-acute_cols) %>%
  mutate(ci_lb = ifelse(acute == 1, ci_lb_acute, ci_lb_chronic) * 1000,
         ci_ub = ifelse(acute == 1, ci_ub_acute, ci_ub_chronic) * 1000) %>%
  select(-ci_lb_acute, -ci_ub_acute, -ci_lb_chronic, -ci_ub_chronic)



### Create portfolio specific comparisons and join back to main data

#### SET UP FOR MEDICAID/MEDICARE DATA
# NEED TO MODIFY FOR MEDICAID-ONLY DATA

rest_pha <- health_events_combined_pop %>%
  filter((agency == "KCHA" | agency == "SHA") & category1 == "portfolio" & 
           !is.na(group1) & category2 == "total") %>%
  group_by(source, indicator, year, full_criteria, agency, dual) %>%
  mutate(count_tot = sum(count), pop_tot = sum(pop), pt_tot = sum(pt)) %>%
  ungroup() %>%
  mutate(
    rest_count = count_tot - count,
    rest_pop = pop_tot - pop,
    rest_pt = pt_tot - pt,
    rest_denominator = ifelse(year %in% c(2012, 2016), rest_pt / 366, rest_pt / 365),
    rest_pha_rate = case_when(
      acute == 1 ~ rest_count / rest_denominator * 1000,
      acute == 0 & pop == 0 ~ NA_real_,
      acute == 0 ~ rest_count / rest_pop * 1000)) %>%
  mutate(chronic_cols = map2(rest_count, rest_pop, ~ if (is.na(.y) | .y < 1) {NA} else {prop.test(.x, n = .y, correct = F)})) %>%
  mutate(rest_ci_lb_chronic = map_dbl(chronic_cols, function(x) {
    if (max(is.na(x)) == 1) {NA} else {x[["conf.int"]][[1]]}}),
         rest_ci_ub_chronic = map_dbl(chronic_cols, function(x) {
           if (max(is.na(x)) == 1) {NA} else {x[["conf.int"]][[2]]}})) %>%
  select(-chronic_cols) %>%
  mutate(acute_cols = map2(rest_count, rest_denominator, 
                           ~ if (is.na(.y) | .y < 1) {NA} else {poisson.test(.x, T = .y)})) %>% 
  mutate(rest_ci_lb_acute = map_dbl(acute_cols, function(x) {
    if (max(is.na(x)) == 1) {NA} else (x[["conf.int"]][[1]])}),
    rest_ci_ub_acute = map_dbl(acute_cols, function(x) {
      if (max(is.na(x)) == 1) {NA} else (x[["conf.int"]][[2]])})) %>%
  select(-acute_cols) %>%
  mutate(rest_ci_lb = ifelse(acute == 1, rest_ci_lb_acute * 1000, rest_ci_lb_chronic * 1000),
         rest_ci_ub = ifelse(acute == 1, rest_ci_ub_acute * 1000, rest_ci_ub_chronic * 1000)) %>%
  select(source, indicator, year, full_criteria, agency, dual, 
         category1, group1, category2, group2,
         rest_count, rest_denominator, rest_pop, rest_pt,
         rest_pha_rate, rest_ci_lb, rest_ci_ub)


health_events_pop_final <- left_join(
  health_events_combined_pop, rest_pha, 
  by = c("source", "year", "indicator", "full_criteria", "agency", "dual", 
         "category1", "group1", "category2", "group2"))

### Make suppressed version
health_events_pop_final_suppressed <- health_events_pop_final %>%
  # Remove people who didn't meet criteria
  filter(full_criteria == "Met full criteria") %>%
  mutate(denominator = round(denominator, 3),
         rest_denominator = round(rest_denominator, 3)) %>%
  mutate_at(vars(acute, year, wc_flag, count, pt, pop_ever, pop, rest_count,
                 rest_pop, rest_pt, count_supp, pop_ever_supp, pop_supp,
                 suppressed, pop_ever_supp_flag, pop_supp_flag),
            list( ~ as.integer(.))) %>%
  mutate_at(vars(rate, ci_lb, ci_ub, rest_pha_rate, rest_ci_lb, rest_ci_ub),
            list( ~ case_when(
              suppressed == 1 ~ NA_real_,
              acute == 0 & pop_supp_flag == 1 ~ NA_real_,
              TRUE ~ round(., 1)
            ))) %>%
  mutate_at(vars(rest_count, rest_pop), 
            list( ~ ifelse(suppressed == 1 | pop_supp_flag == 1, NA_integer_, .))) %>%
  select(source:group2, count_supp, denominator, pt, pop_ever_supp, pop_supp, rate:ci_ub, 
         rest_count, rest_denominator, rest_pop, rest_pt, rest_pha_rate, rest_ci_lb, rest_ci_ub,
         suppressed, pop_ever_supp_flag, pop_supp_flag) %>%
  rename(count = count_supp, pop_ever = pop_ever_supp, pop = pop_supp, 
         count_supp = suppressed, pop_ever_supp = pop_ever_supp_flag,
         pop_supp = pop_supp_flag)



#### WRITE DATA ####
dbWriteTable(db_extractstore51, 
             name = DBI::Id(schema = "APDE_WIP", table = "mcaid_mcare_pha_events"),
             value = health_events_combined_suppressed[654218:654220,], overwrite = T,
             field.types = c(acute = "tinyint",
                             year = "integer",
                             wc_flag = "tinyint",
                             count = "integer",
                             pt = "integer",
                             pop_ever = "integer",
                             pop = "integer",
                             rest_count = "integer",
                             rest_pop = "integer",
                             rest_pt = "integer",
                             count_supp = "integer",
                             pop_ever_supp = "tinyint",
                             pop_supp = "tinyint",
                             rest_pha_rate = "float",
                             rest_ci_lb = "float",
                             rest_ci_ub = "float"))



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
                  field.types = c(pt = "integer", pop_ever = "integer",
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
                             pt = "integer",
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










