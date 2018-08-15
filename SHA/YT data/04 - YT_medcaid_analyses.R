###############################################################################
# OVERVIEW:
# Code to examine Yesler Terrace and Scattered sites data (housing and health)
#
# STEPS:
# 01 - Set up YT parameters in combined PHA/Medicaid data
# 02 - Conduct demographic analyses and produce visualizations
# 03 - Analyze movement patterns and geographic elements (optional)
# 03 - Bring in health conditions and join to demographic data
# 04 - Conduct health condition analyses (multiple files) ### (THIS CODE) ###
#
# Alastair Matheson (PHSKC-APDE)
# alastair.matheson@kingcounty.gov
# 2018-03-15
#
###############################################################################

##### Set up global parameter and call in libraries #####
options(max.print = 350, tibble.print_max = 30, scipen = 999)

library(housing) # contains many useful functions for analyses
library(odbc) # Used to connect to SQL server
library(openxlsx) # Used to import/export Excel files
library(lubridate) # Used to manipulate dates
library(tidyverse) # Used to manipulate data
library(data.table) # Used to manipulate data
library(medicaid) # Used to aggregate data

housing_path <- "//phdata01/DROF_DATA/DOH DATA/Housing"

#### Connect to the SQL servers ####
db.apde51 <- dbConnect(odbc(), "PH_APDEStore51")
db.claims51 <- dbConnect(odbc(), "PHClaims51")


#### FUNCTIONS ####
# This function counts the population in each PHA/Medicaid grouping
popcount_all_yt_f <- function(df, year) {
  pt <- rlang::sym(paste0("pt", quo_name(year)))                
  
  # Make person-time denominator for each grouping
  pt_output <- df %>%
    filter(!is.na((!!pt))) %>%
    group_by(agency_new, enroll_type, dual_elig_m, yt, ss) %>%
    summarise(pt = sum((!!pt))) %>%
    ungroup() %>%
    mutate(year = as.numeric(paste0(20, year)),
           pt = case_when(
             year %in% c(2012, 2016) ~ pt/366,
             year %in% c(2013:2015) ~ pt/365
           )
           ) %>%
    rename(agency = agency_new, dual = dual_elig_m) %>%
    select(year, agency, enroll_type, dual, yt, ss, pt) %>%
    mutate(
      agency = ifelse(is.na(agency), "Non-PHA", agency),
      enroll_type = case_when(
        enroll_type == "b" ~ "Both",
        enroll_type == "h" ~ "Housing only",
        enroll_type == "m" ~ "Medicaid only")
    )
  
  # 
  coded <- popcode_yt_f(df, year)
  pop_output <- popcode_min_f(coded, year)
  
  pop_output <- pop_output %>%
    distinct(year, pid2, agency, enroll_type, dual, yt, ss) %>%
    group_by(year, agency, enroll_type, dual, yt, ss) %>%
    summarise(pop = n_distinct(pid2)) %>%
    ungroup()

  output <- left_join(pt_output, pop_output, 
                      by = c("year", "agency", "enroll_type",
                             "dual", "yt", "ss"))
  
  return(output)
}


### Function to summarize acute events by YT/not YT
eventcount_yt_f <- function(df, event = NULL, year) {
  
  event_quo <- enquo(event)
  
  if (str_detect(quo_name(event_quo), "hosp")) {
    event_year <- quo(hosp_year)
  } else if (str_detect(quo_name(event_quo), "ed")) {
    event_year <- quo(ed_year)
  } else if (str_detect(quo_name(event_quo), "inj")) {
    event_year <- quo(inj_year)
  }

  output <- df %>%
    filter((!!event_year) == year | is.na((!!event_year)))  %>%
    group_by(agency_new, enroll_type, dual_elig_m, yt, ss) %>%
    summarise(count = sum(!!event_quo, na.rm = T)) %>%
    ungroup() %>%
    mutate(year = as.numeric(year)) %>%
    select(year, agency_new, enroll_type, dual_elig_m, yt, ss, count) %>%
    rename(agency = agency_new, dual = dual_elig_m) %>%
    mutate(agency = ifelse(is.na(agency), "Non-PHA", agency),
           enroll_type = case_when(
             enroll_type == "b" ~ "Both",
             enroll_type == "h" ~ "Housing only",
             enroll_type == "m" ~ "Medicaid only")
    )
  return(output)
}


### Function to summarize acute events across all demographics
# Possibly add ability to differentiate by injury intention (e.g., unintentional)
yt_acute_f <- function(df, event = NULL, number = TRUE, 
                               person = FALSE, year) {
  
  event_quo <- enquo(event)
  year_full <- as.numeric(paste0(20, year))
  
  if (number == TRUE) {
    agex <- rlang::sym(paste0("age", year, "_num"))
    lengthx <- rlang::sym(paste0("length", year, "_num"))
  } else {
    agex <- rlang::sym(paste0("age", year, "_grp"))
    lengthx <- rlang::sym(paste0("length", year, "_grp"))
  }
  
  if (str_detect(quo_name(event_quo), "hosp|inpatient")) {
    event_year <- quo(hosp_year)
  } else if (str_detect(quo_name(event_quo), "ed")) {
    event_year <- quo(ed_year)
  } else if (str_detect(quo_name(event_quo), "inj|intent|mech")) {
    event_year <- quo(inj_year)
  } else if(str_detect(quo_name(event_quo), "wc")){
    # Requires more formatting to ensure only children aged 3-6 are retained
    event_year <- quo(wc_year)
    df <- df %>%
      mutate(
        age_temp = floor(interval(start = dob_c, 
                                  end = as.Date(paste0(year_full, "-12-31"),
                                                origin = "1970-01-01"))
                         / years(1))
      ) %>%
      filter(!is.na(age_temp) & age_temp >= 3 & age_temp <= 6) %>%
      mutate(!!agex := 8)
  }
  
  
  # Set up data frame to only include appropriate year
  output <- df %>%
    filter((!!event_year) == year_full | is.na((!!event_year)))
  
  # Restrict to 1 event per person per grouping if desired
  if (person == TRUE) {
    output <- output %>%
      distinct(pid2, agency_new, enroll_type, dual_elig_m, yt, ss,
               !!agex, gender_c, ethn_c, !!lengthx, .keep_all = T)
  }
  
  output <- output %>%
    group_by(agency_new, enroll_type, dual_elig_m, yt, ss,
             !!agex, gender_c, ethn_c, !!lengthx) %>%
    summarise(count = sum(!!event_quo)) %>%
    ungroup() %>%
    mutate(age_group = !!agex,
           length = !!lengthx,
           year = as.numeric(paste0(20, year))) %>%
    select(year, agency_new, enroll_type, dual_elig_m, yt, ss, 
           age_group, gender_c, ethn_c, length, count) %>%
    rename(agency = agency_new,
           dual = dual_elig_m,
           gender = gender_c,
           ethn = ethn_c)
  
  return(output)
}



yt_chronic_f <- function(df_chronic = chronic, df_pop = yt_coded_min, 
                         agency = agency_min, condition = NULL, 
                         number = FALSE, year = 12) {
  
  yr_chk <- as.numeric(paste0("20", year))
  condition_quo <- enquo(condition)
  agency_quo <- enquo(agency)
  
  if (number == TRUE) {
    agex <- rlang::sym(paste0("age", year, "_num"))
    lengthx <- rlang::sym(paste0("length", year, "_num"))
  } else {
    agex <- rlang::sym(paste0("age", year, "_grp"))
    lengthx <- rlang::sym(paste0("length", year, "_grp"))
  }

  year_start = as.Date(paste0("20", year, "-01-01"), origin = "1970-01-01")
  year_end = as.Date(paste0("20", year, "-12-31"), origin = "1970-01-01")
  
  # Filter to only include people with the condition in that year
  cond <- df_chronic %>%
    filter(!!condition_quo == 1 & from_date <= year_end & to_date >= year_start) %>%
    distinct(id, !!condition_quo)
  
  df_pop <- df_pop %>% filter(year_code == yr_chk & !!agency_quo == "SHA")
  
  ### Join pop and condition data to summarise
  output <- left_join(df_pop, cond, by = c("mid" = "id")) %>%
    mutate(condition = if_else(is.na(!!condition_quo), 0, as.numeric(!!condition_quo))) %>%
    group_by(year_code, !!agency_quo, enroll_type_min, dual_min, yt_min, ss_min,
             !!agex, gender_c, ethn_c, !!lengthx) %>%
    summarise(count := sum(condition)) %>%
    ungroup() %>%
    select(year_code, !!agency_quo, enroll_type_min, dual_min, yt_min, ss_min,
           !!agex, gender_c, ethn_c, !!lengthx, count) %>%
    rename(year = year_code,
           agency = !!agency_quo,
           enroll_type = enroll_type_min,
           dual = dual_min,
           yt = yt_min,
           ss = ss_min,
           age_group = !!agex,
           gender = gender_c,
           ethn = ethn_c,
           length = !!lengthx)
  
  return(output)
  
}



#### BRING IN DATA ####
### Bring in linked housing/Medicaid elig data with YT already designated
# Only bring in necessary columns
system.time(
yt_mcaid_final <- dbGetQuery(db.apde51, 
                             "SELECT pid2, mid, startdate_c, enddate_c, dob_c, ethn_c,
                             gender_c, pt12, pt13, pt14, pt15, pt16, pt17,
                             age12_grp, age13_grp, age14_grp, age15_grp,
                             age16_grp, age17_grp,
                             agency_new, enroll_type, dual_elig_m, yt, yt_old,
                             yt_new, ss,  yt_ever, ss_ever, place, start_type, end_type,
                             length12_grp, length13_grp, length14_grp, 
                             length15_grp, length16_grp, length17_grp, 
                             hh_inc_12_cap, hh_inc_13_cap, hh_inc_14_cap,
                             hh_inc_15_cap, hh_inc_16_cap, hh_inc_17_cap
                             FROM housing_mcaid_yt")
)


# Filter to only include YT and SS residents
yt_ss <- yt_mcaid_final %>% filter(yt == 1 | ss == 1)


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
                    inpatient, intent, mechanism
                    FROM dbo.mcaid_claim_summary
                    WHERE intent IS NOT NULL OR ed = 1 OR inpatient = 1 
                    OR clm_type_code = 27")
acute <- acute %>%
  mutate_at(vars(from_date, to_date), funs(as.Date(., origin = "1970-01-01")))


#### END BRING IN DATA SECITON ####



#### POPULATION DATA ####
# Assign people to a location for each calendar year
# NB. lapply is causing R to freeze, runnning spearately for now
# Take min to allocate a person to one place (for chronic conditions)
yt_coded12_min <- yt_popcode(yt_mcaid_final, year_pre = "pt", year = 12, year_suf = NULL, 
                         agency = agency_new, enroll_type = enroll_type, 
                         dual = dual_elig_m, yt = yt, ss = ss, pt_cut = 30, 
                         min = T)
yt_coded13_min <- yt_popcode(yt_mcaid_final, year_pre = "pt", year = 13, year_suf = NULL, 
                         agency = agency_new, enroll_type = enroll_type, 
                         dual = dual_elig_m, yt = yt, ss = ss, pt_cut = 30, 
                         min = T)
yt_coded14_min <- yt_popcode(yt_mcaid_final, year_pre = "pt", year = 14, year_suf = NULL, 
                         agency = agency_new, enroll_type = enroll_type, 
                         dual = dual_elig_m, yt = yt, ss = ss, pt_cut = 30, 
                         min = T)
yt_coded15_min <- yt_popcode(yt_mcaid_final, year_pre = "pt", year = 15, year_suf = NULL, 
                         agency = agency_new, enroll_type = enroll_type, 
                         dual = dual_elig_m, yt = yt, ss = ss, pt_cut = 30, 
                         min = T)
yt_coded16_min <- yt_popcode(yt_mcaid_final, year_pre = "pt", year = 16, year_suf = NULL, 
                         agency = agency_new, enroll_type = enroll_type, 
                         dual = dual_elig_m, yt = yt, ss = ss, pt_cut = 30, 
                         min = T)
yt_coded17_min <- yt_popcode(yt_mcaid_final, year_pre = "pt", year = 17, year_suf = NULL, 
                         agency = agency_new, enroll_type = enroll_type, 
                         dual = dual_elig_m, yt = yt, ss = ss, pt_cut = 30, 
                         min = T)


# Bind together
yt_coded_min <- bind_rows(yt_coded12_min, yt_coded13_min, yt_coded14_min,
                          yt_coded15_min, yt_coded16_min, yt_coded17_min)
rm(list = ls(pattern = "yt_coded1"))
gc()


# Need to count up person time by YT and SS (only keep SHA)
yt_pop_enroll <- bind_rows(lapply(seq(12, 17), function(year) {
  
  ptx <- rlang::sym(paste0("pt", year)) 
  lengthx <- rlang::sym(paste0("length", year, "_grp"))
  agex <- rlang::sym(paste0("age", year, "_grp"))
  year = as.numeric(paste0(20, year))

  # Make person-time denominator for each grouping
  yt_coded_min %>%
    filter(year_code == year & agency_min == "SHA") %>%
    group_by(year_code, agency_min, enroll_type_min, dual_min, yt_min, ss_min,
             !!agex, gender_c, ethn_c, !!lengthx) %>%
    summarise(pt = sum((!!ptx)),
              pop = n_distinct(pid2)) %>%
    ungroup() %>%
    mutate(pt = case_when(
      year_code %in% c(2012, 2016) ~ pt/366,
      year_code %in% c(2013:2015, 2017:2019) ~ pt/365)
    ) %>%
    rename(year = year_code, agency = agency_min, dual = dual_min, 
           enroll_type = enroll_type_min, yt = yt_min, ss = ss_min, 
           age_group = !!agex, length = !!lengthx,
           gender = gender_c, ethn = ethn_c) %>%
    select(year, agency, enroll_type, dual, yt, ss,
           age_group, gender, ethn, length, pt, pop)
}))


### Summarize into univariate columns
tabloop_age <- tabloop_f(yt_pop_enroll, sum = list_var(pt, pop),
                         fixed = list_var(year, agency, enroll_type, 
                                          dual, yt, ss, age_group),
                         loop = list_var(gender, ethn, length)) %>%
  mutate(category1 = "age_group") %>%
  rename(group1 = age_group, category2 = group_cat, group2 = group)

tabloop_gender <- tabloop_f(yt_pop_enroll, sum = list_var(pt, pop),
                         fixed = list_var(year, agency, enroll_type, 
                                          dual, yt, ss, gender),
                         loop = list_var(age_group, ethn, length)) %>%
  mutate(category1 = "gender", gender = as.character(gender)) %>%
  rename(group1 = gender, category2 = group_cat, group2 = group)

tabloop_ethn <- tabloop_f(yt_pop_enroll, sum = list_var(pt, pop),
                         fixed = list_var(year, agency, enroll_type, 
                                          dual, yt, ss, ethn),
                         loop = list_var(age_group, gender, length)) %>%
  mutate(category1 = "ethn") %>%
  rename(group1 = ethn, category2 = group_cat, group2 = group)

tabloop_length <- tabloop_f(yt_pop_enroll, sum = list_var(pt, pop),
                         fixed = list_var(year, agency, enroll_type, 
                                          dual, yt, ss, length),
                         loop = list_var(age_group, gender, ethn)) %>%
  mutate(category1 = "length") %>%
  rename(group1 = length, category2 = group_cat, group2 = group)

# Make totals
tabloop_age_tot <- tabloop_f(yt_pop_enroll, sum = list_var(pt, pop),
                             fixed = list_var(year, agency, enroll_type, 
                                              dual, yt, ss),
                             loop = list_var(age_group)) %>%
  mutate(category2 = "total", group2 = "total") %>%
  rename(category1 = group_cat, group1 = group)

tabloop_gender_tot <- tabloop_f(yt_pop_enroll, sum = list_var(pt, pop),
                             fixed = list_var(year, agency, enroll_type, 
                                              dual, yt, ss),
                             loop = list_var(gender)) %>%
  mutate(category2 = "total", group2 = "total") %>%
  rename(category1 = group_cat, group1 = group)

tabloop_ethn_tot <- tabloop_f(yt_pop_enroll, sum = list_var(pt, pop),
                                fixed = list_var(year, agency, enroll_type, 
                                                 dual, yt, ss),
                                loop = list_var(ethn)) %>%
  mutate(category2 = "total", group2 = "total") %>%
  rename(category1 = group_cat, group1 = group)

tabloop_length_tot <- tabloop_f(yt_pop_enroll, sum = list_var(pt, pop),
                              fixed = list_var(year, agency, enroll_type, 
                                               dual, yt, ss),
                              loop = list_var(length)) %>%
  mutate(category2 = "total", group2 = "total") %>%
  rename(category1 = group_cat, group1 = group)

# Combine
yt_pop_enroll_bivar <- bind_rows(tabloop_age, tabloop_gender, tabloop_ethn,
                                 tabloop_length, tabloop_age_tot, 
                                 tabloop_gender_tot, tabloop_ethn_tot,
                                 tabloop_length_tot) %>%
  filter(pt_sum > 0) %>%
  select(year, agency, enroll_type, dual, yt, ss, category1, group1, 
         category2, group2, pop_sum, pt_sum) %>%
  rename(pop = pop_sum, pt = pt_sum)


# Fix unicode hyphens
yt_pop_enroll_bivar <- yt_pop_enroll_bivar %>%
  mutate_at(vars(group1, group2),
            funs(str_replace_all(., "\u0096", "-")))
  

# Add suppression
yt_pop_enroll_bivar <- yt_pop_enroll_bivar %>%
  mutate(pop_suppressed = if_else(between(pop, 1, 4), 1, 0),
         pop = if_else(between(pop, 1, 4), NA_real_, pop))


# Write out file (optional, also written with health data below)
# write.xlsx(yt_pop_enroll_bivar, paste0(housing_path,
#                                        "/OrganizedData/Summaries/YT/",
#                                        "YT_Medicaid_enrollment_",
#                                        Sys.Date(), ".xlsx"),
#            sheetName = "population")

rm(list = ls(pattern = "tabloop_"))


#### ACUTE EVENTS ####
### Join demographics and hospitalization events
yt_mcaid_hosp <- left_join(yt_mcaid_final, acute, by = c("mid" = "id")) %>%
  filter(inpatient == 1 & from_date >= startdate_c & from_date <= enddate_c &
           agency_new == "SHA") %>%
  mutate(hosp_year = year(from_date))

# Run for number of people with a hospitalization
hosp_pers <- lapply(seq(12, 17), yt_acute_f, df = yt_mcaid_hosp, 
                    event = inpatient, number = F, person = T)
hosp_pers <- as.data.frame(data.table::rbindlist(hosp_pers)) %>%
  mutate(indicator = "Persons with hospitalization")

# Run for total number of hospitalizations
hosp_cnt <- lapply(seq(12, 17), yt_acute_f, df = yt_mcaid_hosp, 
                   event = inpatient, number = F, person = F)
hosp_cnt <- as.data.frame(data.table::rbindlist(hosp_cnt)) %>%
  mutate(indicator = "Hospitalizations")

rm(yt_mcaid_hosp)
gc()


### Join demographics and ED events
# Join demographics and ED events
yt_mcaid_ed <- left_join(yt_mcaid_final, acute, by = c("mid" = "id")) %>%
  filter(ed == 1 & from_date >= startdate_c & from_date <= enddate_c &
           agency_new == "SHA") %>%
  mutate(ed_year = year(from_date))

# Run for number of people with an ED visit
ed_pers <- lapply(seq(12, 17), yt_acute_f, df = yt_mcaid_ed, 
                  event = ed, number = F, person = T)
ed_pers <- as.data.frame(data.table::rbindlist(ed_pers)) %>%
  mutate(indicator = "Persons with ED visits")

# Run for total number of ED visits
ed_cnt <- lapply(seq(12, 17), yt_acute_f, df = yt_mcaid_ed, 
                 event = ed, number = F, person = F)
ed_cnt <- as.data.frame(data.table::rbindlist(ed_cnt)) %>%
  mutate(indicator = "ED visits")

# Run for number of unavoidable ED visits
ed_cnt_unavoid <- lapply(seq(12, 17), yt_acute_f, df = yt_mcaid_ed, 
                         event = ed_emergent_nyu, number = F, person = F)
ed_cnt_unavoid <- as.data.frame(data.table::rbindlist(ed_cnt_unavoid)) %>%
  mutate(indicator = "ED visits - unavoidable")

# Run for number of too close to call ED visits
ed_cnt_inter <- lapply(seq(12, 17), yt_acute_f, df = yt_mcaid_ed, 
                       event = ed_intermediate_nyu, number = F, person = F)
ed_cnt_inter <- as.data.frame(data.table::rbindlist(ed_cnt_inter)) %>%
  mutate(indicator = "ED visits - borderline avoidable")

# Run for number of avoidable ED visits
ed_cnt_avoid <- lapply(seq(12, 17), yt_acute_f, df = yt_mcaid_ed, 
                       event = ed_nonemergent_nyu, number = F, person = F)
ed_cnt_avoid <- as.data.frame(data.table::rbindlist(ed_cnt_avoid)) %>%
  mutate(indicator = "ED visits - potentially avoidable")

# Run for number of unclassified ED visits
ed_cnt_unclass <- lapply(seq(12, 17), yt_acute_f, df = yt_mcaid_ed, 
                         event = ed_unclass_nyu, number = F, person = F)
ed_cnt_unclass <- as.data.frame(data.table::rbindlist(ed_cnt_unclass)) %>%
  mutate(indicator = "ED visits - unable to determine avoidability")

# Run for number of MH ED visits
ed_cnt_mh <- lapply(seq(12, 17), yt_acute_f, df = yt_mcaid_ed, 
                    event = ed_mh_nyu, number = F, person = F)
ed_cnt_mh <- as.data.frame(data.table::rbindlist(ed_cnt_mh)) %>%
  mutate(indicator = "ED visits - mental health primary dx")

# Run for number of alcohol-related ED visits
ed_cnt_alc <- lapply(seq(12, 17), yt_acute_f, df = yt_mcaid_ed, 
                     event = ed_alc_nyu, number = F, person = F)
ed_cnt_alc <- as.data.frame(data.table::rbindlist(ed_cnt_alc)) %>%
  mutate(indicator = "ED visits - alcohol-related primary dx")

# Run for number of alcohol-related ED visits
ed_cnt_sud <- lapply(seq(12, 17), yt_acute_f, df = yt_mcaid_ed, 
                     event = ed_sud_nyu, number = F, person = F)
ed_cnt_sud <- as.data.frame(data.table::rbindlist(ed_cnt_sud)) %>%
  mutate(indicator = "ED visits - substance use disorder related primary dx")

# Run for number of BH-related ED visits
ed_cnt_bh <- lapply(seq(12, 17), yt_acute_f, df = yt_mcaid_ed, 
                    event = ed_bh, number = F, person = F)
ed_cnt_bh <- as.data.frame(data.table::rbindlist(ed_cnt_bh)) %>%
  mutate(indicator = "ED visits - behavioral health-related primary dx")

rm(yt_mcaid_ed)
gc()


### Join demographics and injury events
# Join demographics and injury events
yt_mcaid_inj <- left_join(yt_mcaid_final, acute, by = c("mid" = "id")) %>%
  filter(!is.na(intent) & from_date >= startdate_c & from_date <= enddate_c &
           agency_new == "SHA") %>%
  mutate(inj_year = year(from_date),
         injury = 1)

# Run for number of people with an injury
inj_pers <- lapply(seq(16, 17), yt_acute_f, df = yt_mcaid_inj, 
                   event = injury, number = F, person = T)
inj_pers <- as.data.frame(data.table::rbindlist(inj_pers)) %>%
  mutate(indicator = "Persons with an injury")

# Run for total number of injury visits
inj_cnt <- lapply(seq(16, 17), yt_acute_f, df = yt_mcaid_inj, 
                  event = injury, number = F, person = F)
inj_cnt <- as.data.frame(data.table::rbindlist(inj_cnt)) %>%
  mutate(indicator = "Injuries")

rm(yt_mcaid_inj)


#### CHRONIC CONDITIONS ####
# Use the yt_coded_min data frame as the denominator pop

### Asthma
asthma_pers <- lapply(seq(12, 17), yt_chronic_f, df_chronic = chronic, 
                      df_pop = yt_coded_min, agency = agency_min,
                      condition = asthma_ccw, number = F)
asthma_pers <- as.data.frame(data.table::rbindlist(asthma_pers)) %>%
  mutate(indicator = "Persons with asthma")
gc()


# CHF
chf_pers <- lapply(seq(12, 17), yt_chronic_f, df_chronic = chronic, 
                   df_pop = yt_coded_min, agency = agency_min,
                   condition = heart_failure_ccw, number = F)
chf_pers <- as.data.frame(data.table::rbindlist(chf_pers)) %>%
  mutate(indicator = "Persons with congestive heart failure")
gc()


# COPD
copd_pers <- lapply(seq(12, 17), yt_chronic_f, df_chronic = chronic, 
                    df_pop = yt_coded_min, agency = agency_min,
                    condition = copd_ccw, number = F)
copd_pers <- as.data.frame(data.table::rbindlist(copd_pers)) %>%
  mutate(indicator = "Persons with chronic obstructive pulmonary disease")
gc()


# Depression
depression_pers <- lapply(seq(12, 17), yt_chronic_f, df_chronic = chronic, 
                          df_pop = yt_coded_min, agency = agency_min,
                          condition = depression_ccw, number = F)
depression_pers <- as.data.frame(data.table::rbindlist(depression_pers)) %>%
  mutate(indicator = "Persons with depression")
gc()


# Diabetes
diabetes_pers <- lapply(seq(12, 17), yt_chronic_f, df_chronic = chronic, 
                        df_pop = yt_coded_min, agency = agency_min,
                        condition = diabetes_ccw, number = F)
diabetes_pers <- as.data.frame(data.table::rbindlist(diabetes_pers)) %>%
  mutate(indicator = "Persons with diabetes")
gc()


# Hypertension
hypertension_pers <- lapply(seq(12, 17), yt_chronic_f, df_chronic = chronic, 
                            df_pop = yt_coded_min, agency = agency_min,
                            condition = hypertension_ccw, number = F)
hypertension_pers <- as.data.frame(data.table::rbindlist(hypertension_pers)) %>%
  mutate(indicator = "Persons with hypertension")
gc()


# IHD
ihd_pers <- lapply(seq(12, 17), yt_chronic_f, df_chronic = chronic, 
                   df_pop = yt_coded_min, agency = agency_min,
                   condition = ischemic_heart_dis_ccw, number = F)
ihd_pers <- as.data.frame(data.table::rbindlist(ihd_pers)) %>%
  mutate(indicator = "Persons with ischemic heart disease")
gc()


# Kidney disease
kidney_pers <- lapply(seq(12, 17), yt_chronic_f, df_chronic = chronic, 
                      df_pop = yt_coded_min, agency = agency_min,
                      condition = chr_kidney_dis_ccw, number = F)
kidney_pers <- as.data.frame(data.table::rbindlist(kidney_pers)) %>%
  mutate(indicator = "Persons with kidney disease")
gc()


#### COMBINE DATA ####
health_events <- bind_rows(asthma_pers, chf_pers, copd_pers, depression_pers, 
                           diabetes_pers, ed_cnt, ed_cnt_alc, ed_cnt_bh, 
                           ed_cnt_avoid, ed_cnt_inter, ed_cnt_mh, ed_cnt_sud, 
                           ed_cnt_unavoid, ed_cnt_unclass, ed_pers, hosp_cnt, 
                           hosp_pers, hypertension_pers, ihd_pers, inj_pers, 
                           inj_pers, kidney_pers)

rm(list = ls(pattern = "_cnt$"))
rm(list = ls(pattern = "^ed_cnt"))
rm(list = ls(pattern = "_pers$"))
rm(chronic)
gc()

### Get demographics consistent
health_events <- health_events %>%
  mutate(
    enroll_type = case_when(
      enroll_type == "h" ~ "Housing only",
      enroll_type == "b" ~ "Both",
      enroll_type == "m" ~ "Medicaid only",
      TRUE ~ enroll_type
    ))



tabloop_age <- tabloop_f(health_events, sum = list_var(count),
                         fixed = list_var(indicator, year, agency, enroll_type, 
                                          dual, yt, ss, age_group),
                         loop = list_var(gender, ethn, length)) %>%
  mutate(category1 = "age_group") %>%
  rename(group1 = age_group, category2 = group_cat, group2 = group)

tabloop_gender <- tabloop_f(health_events, sum = list_var(count),
                            fixed = list_var(indicator, year, agency, enroll_type, 
                                             dual, yt, ss, gender),
                            loop = list_var(age_group, ethn, length)) %>%
  mutate(category1 = "gender", gender = as.character(gender)) %>%
  rename(group1 = gender, category2 = group_cat, group2 = group)

tabloop_ethn <- tabloop_f(health_events, sum = list_var(count),
                          fixed = list_var(indicator, year, agency, enroll_type, 
                                           dual, yt, ss, ethn),
                          loop = list_var(age_group, gender, length)) %>%
  mutate(category1 = "ethn") %>%
  rename(group1 = ethn, category2 = group_cat, group2 = group)

tabloop_length <- tabloop_f(health_events, sum = list_var(count),
                            fixed = list_var(indicator, year, agency, enroll_type, 
                                             dual, yt, ss, length),
                            loop = list_var(age_group, gender, ethn)) %>%
  mutate(category1 = "length") %>%
  rename(group1 = length, category2 = group_cat, group2 = group)

# Make totals
tabloop_age_tot <- tabloop_f(health_events, sum = list_var(count),
                             fixed = list_var(indicator, year, agency, enroll_type, 
                                              dual, yt, ss),
                             loop = list_var(age_group)) %>%
  mutate(category2 = "total", group2 = "total") %>%
  rename(category1 = group_cat, group1 = group)

tabloop_gender_tot <- tabloop_f(health_events, sum = list_var(count),
                                fixed = list_var(indicator, year, agency, enroll_type, 
                                                 dual, yt, ss),
                                loop = list_var(gender)) %>%
  mutate(category2 = "total", group2 = "total") %>%
  rename(category1 = group_cat, group1 = group)

tabloop_ethn_tot <- tabloop_f(health_events, sum = list_var(count),
                              fixed = list_var(indicator, year, agency, enroll_type, 
                                               dual, yt, ss),
                              loop = list_var(ethn)) %>%
  mutate(category2 = "total", group2 = "total") %>%
  rename(category1 = group_cat, group1 = group)

tabloop_length_tot <- tabloop_f(health_events, sum = list_var(count),
                                fixed = list_var(indicator, year, agency, enroll_type, 
                                                 dual, yt, ss),
                                loop = list_var(length)) %>%
  mutate(category2 = "total", group2 = "total") %>%
  rename(category1 = group_cat, group1 = group)


# Combine
health_events_combined <- bind_rows(tabloop_age, tabloop_gender, tabloop_ethn,
                                 tabloop_length, tabloop_age_tot, 
                                 tabloop_gender_tot, tabloop_ethn_tot,
                                 tabloop_length_tot) %>%
  filter(count_sum > 0) %>%
  select(indicator, year, agency, enroll_type, dual, yt, ss, category1, group1, 
         category2, group2, count_sum) %>%
  rename(count = count_sum)

# Fix unicode hyphens
health_events_combined <- health_events_combined %>%
  mutate_at(vars(group1, group2),
            funs(str_replace_all(., "\u0096", "-")))


# Add suppression
health_events_combined <- health_events_combined %>%
  mutate(count_suppressed = if_else(between(count, 1, 4), 1, 0),
         count = if_else(between(count, 1, 4), NA_real_, count))



# Write out file alone (optional, also written with pop below)
# write.xlsx(health_events_combined, paste0(housing_path,
#                                        "/OrganizedData/Summaries/YT/",
#                                        "YT_Medicaid_health_events_",
#                                        Sys.Date(), ".xlsx"),
#            sheetName = "conditions")

#### WRITE OUT FILE ####
sheets = list("population" = yt_pop_enroll_bivar, "conditions" = health_events_combined)

write.xlsx(sheets, paste0(housing_path, "/OrganizedData/Summaries/YT/",
                          "YT_Medicaid_dashboard_", Sys.Date(), ".xlsx"))

rm(list = ls(pattern = "tabloop"))
rm(sheets)
rm(health_events)
gc()



