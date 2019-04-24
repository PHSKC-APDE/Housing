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
# Just looking at 2017 results for now

### Linked housing/Medicaid data
housing <- dbGetQuery(db_apde,
                      "SELECT pid2, startdate_h, enddate_h, startdate_m, enddate_m, 
                      startdate_o, enddate_o, overlap_type, startdate_c, enddate_c, 
                      enroll_type, mid, dob_c, age17, race_c, hisp_c, ethn_c, gender_c, 
                      agency_new, subsidy_type, operator_type, vouch_type_final, 
                      zip_c, kc_area, portfolio_final,
                      port_in, port_out_kcha, port_out_sha,
                      pt17_h, pt17_m, pt17_o, pt17, length17
                      FROM dbo.housing_mcaid
                      WHERE startdate_c <= '2017-12-31' AND enddate_c >= '2017-01-01'")


### People who met the inclusion criteria (11+ months of Medicaid, ages 5-64, non-dual)
elig_pop <- dbGetQuery(db_claims,
                       "SELECT a.id, a.year_month, b.end_month, a.end_month_age, 
                       c.dobnew, c.gender_mx, c.race_eth_mx, c.maxlang, 'enroll_flag' = 1  
                       FROM
                        (SELECT id, year_month, end_month_age 
                          FROM [PHClaims].[stage].[perf_enroll_denom]
                          WHERE full_benefit_t_12_m >= 11 AND dual_t_12_m = 0 AND 
                            end_month_age >= 5 AND end_month_age < 65 AND 
                              year_month = '201712') a 
                        LEFT JOIN
                        (SELECT year_month, end_month, beg_measure_year_month 
                          FROM [ref].[perf_year_month]) b 
                        ON a.year_month = b.year_month
                        LEFT JOIN
                        (SELECT id, dobnew, gender_mx, race_eth_mx, maxlang
                          FROM dbo.mcaid_elig_demoever) c
                        ON a.id = c.id")


### Asthma medication ratio data
# NOTE THAT THIS CODE WILL NEED TO BE UPDATED ONCE THE NEW ETL PROCESS IS RUN
amr <- dbGetQuery(db_claims,
                  "SELECT id, numerator, denominator
                  FROM stage.mcaid_perf_measures
                  WHERE measure_id = 19 AND end_year_month = '201712'")


#### BRING DATA TOGETHER AND PROCESS ####
housing_amr <- inner_join(housing, elig_pop, by = c("mid" = "id")) %>%
  left_join(., amr, by = c("mid" = "id")) %>%
  replace_na(., list(denominator = 0, numerator = 0))

### Allocate people to a housing category based on pt in that year
# Can then drop columns not currently being used
housing_amr <- housing_amr %>%
  arrange(pid2, -pt17) %>%
  group_by(pid2) %>%
  slice(1) %>%
  ungroup() %>%
  # Add more age and other groups
  mutate(
    age17_grp = case_when(
      between(age17, 5, 11.999) ~ "5-11",
      between(age17, 12, 18.999) ~ "12-18",
      between(age17, 19, 30.999) ~ "19-30",
      between(age17, 31, 50.999) ~ "31-50",
      between(age17, 51, 64.999) ~ "51-64",
      TRUE ~ NA_character_
      ),
    length17_grp = case_when(
      is.na(length17) ~ NA_character_,
      between(length17, 0, 2.999) ~ "<3 years",
      between(length17, 3, 5.999) ~ "3-6 years",
      length17 >= 6 ~ "6+ years",
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
  select(pid2, age17_grp, gender_c, 
         ethn_c, lang_grp, length17_grp, 
         agency_new, subsidy_type, operator_type, vouch_type_final, 
         zip_c, portfolio_final,
         numerator, denominator)



#### ANALYZE DATA ####
### Look at all groups combined
asthma_cnt_combined <- tabloop_f(df = housing_amr,
                        dcount = list_var(pid2),
                        sum = list_var(denominator, numerator),
                        loop = list_var(age17_grp, gender_c, 
                                        ethn_c, length17_grp, lang_grp,
                                        agency_new, subsidy_type,
                                        operator_type,
                                        vouch_type_final,
                                        zip_c,
                                        portfolio_final)) %>%
  mutate(agency = "Combined")


### Look at all groups combined
asthma_cnt_agency <- tabloop_f(df = housing_amr,
                        dcount = list_var(pid2),
                        sum = list_var(denominator, numerator),
                        fixed = list_var(agency_new),
                        loop = list_var(age17_grp, gender_c, 
                                        ethn_c, length17_grp, lang_grp,
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
                                      else {NA})) %>%
  rename(cat1 = group_cat, cat1_group = group, population = pid2_dcount,
         asthma = denominator_sum, amr_50 = numerator_sum) %>%
  select(agency, cat1, cat1_group, population, asthma, amr_50,
         asthma_pct, asthma_pct_lb, asthma_pct_ub, 
         amr_pct, amr_pct_lb, amr_pct_ub) %>%
  # Fix up category names
  mutate(cat1 = case_when(
    cat1 == "age17_grp" ~ "Age group",
    cat1 == "agency_new" ~ "Agency",
    cat1 == "ethn_c" ~ "Race/ethnicity",
    cat1 == "gender_c" ~ "Gender",
    cat1 == "lang_grp" ~ "Language",
    cat1 == "length17_grp" ~ "Length of time in housing",
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
         amr_50_supp = ifelse(amr_50 < 11, 1, 0))

### Check to see if the overall/combined group needs to be suppressed
# This is to prevent back calculating a suppressed number
supp_check_pop <- asthma_cnt_supp %>%
  filter(population_supp == 1) %>%
  group_by(cat1, cat1_group) %>%
  summarise(count = n()) %>% ungroup() %>%
  mutate(population_supp2 = ifelse(count == 1, 1, 0)) %>%
  select(cat1, cat1_group, population_supp2)

supp_check_asthma <- asthma_cnt_supp %>%
  filter(asthma_supp == 1) %>%
  group_by(cat1, cat1_group) %>%
  summarise(count = n()) %>% ungroup() %>%
  mutate(asthma_supp2 = ifelse(count == 1, 1, 0)) %>%
  select(cat1, cat1_group, asthma_supp2)

supp_check_amr <- asthma_cnt_supp %>%
  filter(amr_50_supp == 1) %>%
  group_by(cat1, cat1_group) %>%
  summarise(count = n()) %>% ungroup() %>%
  mutate(amr_50_supp2 = ifelse(count == 1, 1, 0)) %>%
  select(cat1, cat1_group, amr_50_supp2)


# Join back and suppress rows
asthma_cnt_supp <- list(asthma_cnt_supp, supp_check_pop, supp_check_asthma, supp_check_amr) %>%
  reduce(left_join, by = c("cat1" = "cat1", "cat1_group" = "cat1_group")) %>%
  mutate_at(vars(population_supp2, asthma_supp2, amr_50_supp2),
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
  mutate_at(vars(amr_50, amr_pct, amr_pct_lb, amr_pct_ub),
            funs(case_when(
              amr_50_supp == 1 | is.na(asthma) ~ NA_real_,
              (amr_50_supp2 == 1 | is.na(asthma)) & agency == "Combined" ~ NA_real_,
              TRUE ~ .)))


### Save for Tableau
write.csv(asthma_cnt_supp, file.path(dashh_2_path, "persistent_asthma_2017.csv"),
          row.names = F)

  



