###############################################################################
# Look at asthma medication ratio outcome
#
# Alastair Matheson (PHSKC-APDE)
# 2019-04
#
#
###############################################################################

##### Set up global parameter and call in libraries #####
options(max.print = 350, tibble.print_max = 30, scipen = 999, warning.length = 5000)

library(odbc) # Used to connect to SQL server
library(housing) # contains many useful functions for analyzing housing/Medicaid data
library(lubridate) # Used to manipulate dates
library(tidyverse) # Used to manipulate data
library(claims) # Used to aggregate data


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
  
  sql_temp <- glue::glue_sql("SELECT pid2, startdate_c, enddate_c, 
                             enroll_type, mid, dob_c, race_c, hisp_c, ethn_c, gender_c, 
                             age12, age13, age14, age15, age16, age17, 
                             agency_new, subsidy_type, operator_type, vouch_type_final, 
                             zip_c, kc_area, portfolio_final,
                             pt12, pt13, pt14, pt15, pt16, pt17, 
                             length12, length13, length14, length15, length16, length17,
                             {year_end} AS end_period 
                             FROM dbo.housing_mcaid
                             WHERE startdate_c <= {year_end} AND enddate_c >= {year_start}",
                             .con = db_apde)
  print(paste0("Collecting data for the year through to ", year_end))
  dbGetQuery(db_apde, sql_temp)
}))

# Single year version for 2017 only
# housing <- dbGetQuery(db_apde,
#                       "SELECT pid2, startdate_h, enddate_h, startdate_m, enddate_m, 
#                       startdate_o, enddate_o, overlap_type, startdate_c, enddate_c, 
#                       enroll_type, mid, dob_c, age17, race_c, hisp_c, ethn_c, gender_c, 
#                       agency_new, subsidy_type, operator_type, vouch_type_final, 
#                       zip_c, kc_area, portfolio_final,
#                       port_in, port_out_kcha, port_out_sha,
#                       pt17_h, pt17_m, pt17_o, pt17, length17
#                       FROM dbo.housing_mcaid
#                       WHERE startdate_c <= '2017-12-31' AND enddate_c >= '2017-01-01'")


### People who met the inclusion criteria (11+ months of Medicaid, ages 5-64, non-dual)
elig_pop <- bind_rows(lapply(months_list, function(x) {
  date_month <- paste0(str_sub(x, 1, 4), str_sub(x, 6, 7))
  
  sql_temp <- glue::glue_sql("SELECT a.id, a.year_month, b.end_month, a.end_month_age, 
                             c.dobnew, c.gender_mx, c.race_eth_mx, c.maxlang, 'enroll_flag' = 1  
                             FROM
                             (SELECT id, year_month, end_month_age 
                               FROM [PHClaims].[stage].[perf_enroll_denom]
                               WHERE full_benefit_t_12_m >= 11 AND dual_t_12_m = 0 AND 
                               end_month_age >= 5 AND end_month_age < 65 AND 
                               year_month = {date_month}) a 
                             LEFT JOIN
                             (SELECT year_month, end_month, beg_measure_year_month 
                               FROM [ref].[perf_year_month]) b 
                             ON a.year_month = b.year_month
                             LEFT JOIN
                             (SELECT id, dobnew, gender_mx, race_eth_mx, maxlang
                               FROM dbo.mcaid_elig_demoever) c
                             ON a.id = c.id",
                             .con = db_claims)
  
  print(paste0("Collecting data for the year through to ", date_month))
  dbGetQuery(db_claims, sql_temp)
}))

# Single year version for 2017 only
# elig_pop <- dbGetQuery(db_claims,
#                        "SELECT a.id, a.year_month, b.end_month, a.end_month_age, 
#                        c.dobnew, c.gender_mx, c.race_eth_mx, c.maxlang, 'enroll_flag' = 1  
#                        FROM
#                         (SELECT id, year_month, end_month_age 
#                           FROM [PHClaims].[stage].[perf_enroll_denom]
#                           WHERE full_benefit_t_12_m >= 11 AND dual_t_12_m = 0 AND 
#                             end_month_age >= 5 AND end_month_age < 65 AND 
#                               year_month = '201712') a 
#                         LEFT JOIN
#                         (SELECT year_month, end_month, beg_measure_year_month 
#                           FROM [ref].[perf_year_month]) b 
#                         ON a.year_month = b.year_month
#                         LEFT JOIN
#                         (SELECT id, dobnew, gender_mx, race_eth_mx, maxlang
#                           FROM dbo.mcaid_elig_demoever) c
#                         ON a.id = c.id")


### Asthma medication ratio data
# If prep code has been run, can pull in people with asthma but ignore the 
# requirement to have persistent asthma
amr_1_year <- dbGetQuery(db_claims,
           "SELECT a.id, a.year_month AS end_year_month, b.amr, 
           'denominator_1yr' = 1, 
           CASE WHEN b.amr >= 0.5 THEN 1 ELSE 0 END AS numerator_1yr 
           FROM
           (SELECT id, year_month, end_month, past_year, end_month_age, beg_measure_year_month
           FROM ##asthma_denom 
           WHERE enroll_flag = 1 AND rx_any = 1 AND dx_exclude = 0) a
           LEFT JOIN
           (SELECT id, end_month, amr FROM ##asthma_amr) b
           ON a.id = b.id AND a.end_month = b.end_month 
           --WHERE a.end_month = '2017-12-31' 
           ORDER BY a.id, a.end_month")



# NOTE THAT THIS CODE WILL NEED TO BE UPDATED ONCE THE NEW ETL PROCESS IS RUN
amr <- dbGetQuery(db_claims,
                  "SELECT end_year_month, id, numerator, denominator
                  FROM stage.mcaid_perf_measure
                  WHERE measure_id = 19 --AND end_year_month = '201712'")


#### BRING DATA TOGETHER AND PROCESS ####
housing_amr <- inner_join(housing, elig_pop, 
                          by = c("mid" = "id", 
                                 "end_period" = "end_month")) %>%
  left_join(., amr, by = c("mid" = "id",
                           "year_month" = "end_year_month")) %>%
  left_join(., amr_1_year, by = c("mid" = "id",
                                  "year_month" = "end_year_month")) %>%
  replace_na(., list(denominator = 0, numerator = 0, 
                     denominator_1yr = 0, numerator_1yr = 0))


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
        !!sym(age_var) >= 31 & !!sym(age_var) <= 50.999 ~ "31-50",
        !!sym(age_var) >= 51 & !!sym(age_var) <= 64.999 ~ "51-64",
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
        is.na(maxlang) ~ NA_character_,
        maxlang == "ENGLISH" ~ "ENGLISH",
        maxlang == "SPANISH; CASTILIAN" ~ "SPANISH",
        maxlang == "VIETNAMESE" ~ "VIETNAMESE",
        maxlang == "CHINESE" ~ "CHINESE",
        maxlang == "SOMALI" ~ "SOMALI",
        maxlang == "RUSSIAN" ~ "RUSSIAN",
        maxlang == "ARABIC" ~ "ARABIC",
        maxlang == "KOREAN" ~ "KOREAN",
        maxlang == "UKRAINIAN" ~ "UKRAINIAN",
        maxlang == "AMHARIC" ~ "AMHARIC",
        maxlang == "BURMESE" ~ "BURMESE",
        maxlang == "TIGRINYA" ~ "TIGRINYA",
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

  



