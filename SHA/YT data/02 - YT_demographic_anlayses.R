###############################################################################
# OVERVIEW:
# Code to examine Yesler Terrace and Scattered sites data (housing and health)
#
# STEPS:
# 01 - Set up YT parameters in combined PHA/Medicaid data
# 02 - Conduct demographic analyses and produce visualizations ### (THIS CODE) ###
# 03 - Analyze movement patterns and geographic elements (optional)
# 03 - Bring in health conditions and join to demographic data
# 04 - Conduct health condition analyses (multiple files)
#
# Alastair Matheson (PHSKC-APDE)
# alastair.matheson@kingcounty.gov
# 2017-06-30
#
###############################################################################


#### Analyses in this code ####
# 1) General demographics of people in YT and SS at any point each year
# 2) Drop off in number of people each year (i.e,. how much person-time, cut-off points)


#### Set up global parameter and call in libraries ####
# Turn scientific notation off and other settings
options(max.print = 700, scipen = 100, digits = 5)

library(housing) # contains many useful functions for analyses
library(openxlsx) # Used to import/export Excel files
library(lubridate) # Used to manipulate dates
library(tidyverse) # Used to manipulate data
library(data.table) # Used to manipulate data
library(pastecs) # Used for summary statistics
library(medicaid) # helpful counting functions

housing_path <- "//phdata01/DROF_DATA/DOH DATA/Housing"


#### BRING IN DATA ###
### Code for mapping field values
demo_codes <- read.csv(text = RCurl::getURL("https://raw.githubusercontent.com/PHSKC-APDE/Housing/master/processing/housing_mcaid%20demo%20codes.csv"), 
                       header = TRUE, stringsAsFactors = FALSE)

### Bring in combined PHA/Medicaid data with some demographics already run ####
yt_mcaid_final <- readRDS(file = paste0(housing_path, 
                                       "/OrganizedData/SHA cleaning/yt_mcaid_final.Rds"))

# Retain only people living at YT or SS
yt_ss <- yt_mcaid_final %>% filter(yt == 1 | ss == 1)


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

# Write function to look over all demogs of interest
yt_demogs_f <- function(df, group = quos(yt), unit = pid2, period = "year") {
  
  unit2 <- enquo(unit)
  origin <- "1970-01-01"
  
  result <- popcount(df, group_var = group, yearmin = 2012, yearmax = 2017,
                     period = period, unit = !!unit2) %>%
    mutate(date = ifelse(
      period == "year", year(date),
      ifelse(period == "quarter", quarter(date, with_year = T),
             ifelse(period == "month", format(as.Date(date), "%Y-%m"),
                    date)))
    ) %>%
    # case_when throwing evaluation error due to formats
    # mutate(date = case_when(
    #   period == "year" ~ year(date),
    #   period == "quarter" ~ quarter(date, with_year = T),
    #   period == "month" ~ as.Date(date, format = "%y%m"),
    #   period == "date" ~ date,
    #   TRUE ~ date
    # )) %>%
    group_by(date, yt) %>%
    mutate(total = sum(pop, na.rm = T)) %>%
    ungroup %>%
    mutate(percent = round(pop / total*100, 1)) %>%
    select(date, yt, !!!group, pop, pt_days, total, percent, unit, period)
  
  return(result)
}

# Summarise age using different person-time thresholds
# use long = T to generate output for Tableau
age_summ_f <- function(df, year = 12, cutoff = 0, long = F,
                       pt_suf = NULL) {
  
  ptx <- rlang::sym(paste0("pt", year, pt_suf))
  agex <- rlang::sym(paste0("age", year))
  
  if (long == F) {
    output <- df %>%
      filter(!!ptx > cutoff) %>%
      distinct(pid2, yt, .keep_all = T) %>%
      group_by(yt) %>%
      summarise(median = median(!!agex, na.rm = T), 
                mean = mean(!!agex, na.rm = T)) %>%
      ungroup() %>%
      mutate(year = as.numeric(paste0(20, year))) %>%
      select(year, yt, median, mean)
  } else {
    output <- df %>%
      filter(!!ptx > cutoff) %>%
      distinct(pid2, yt, .keep_all = T) %>%
      group_by(yt) 
    
    median <- output %>%
      summarise(data = median(!!agex, na.rm = T)) %>%
      ungroup() %>%
      mutate(group = "Median (years)")
    
    mean <- output %>%
      summarise(data = mean(!!agex, na.rm = T)) %>%
      ungroup() %>%
      mutate(group = "Mean (years)")
    
    
    output <- bind_rows(median, mean) %>%
      mutate(date = as.numeric(paste0(20, year)),
             category = "Age") %>%
      select(date, yt, category, group, data)
  }
  
  return(output)
}




#### 1) Demographics of people in YT/SS at any point in the year, 2012–2017 ####
### Annual counts
yt_demogs_f(df = yt_ss, group = quos(yt), unit = pid2)
yt_demogs_f(df = yt_ss, group = quos(yt), unit = hh_id_new_h)

yt_demogs_f(df = yt_ss, group = quos(yt, enroll_type), unit = pid2)
yt_demogs_f(df = yt_ss, group = quos(yt, gender_c), unit = pid2)
yt_demogs_f(df = yt_ss, group = quos(yt, agegrp_h), unit = pid2)
yt_demogs_f(df = yt_ss, group = quos(yt, race_c), unit = pid2)
yt_demogs_f(df = yt_ss, group = quos(yt, time_housing), unit = pid2)


### Create summary stats for Tableau
total <- yt_demogs_f(df = yt_ss, group = quos(yt), unit = pid2) %>%
  mutate(category = "Number of residents",
         group = "Total",
         data = pop)
gender <- yt_demogs_f(df = yt_ss, group = quos(yt, gender_c), unit = pid2) %>%
  mutate(category = "Gender", group = gender_c, data = percent)
ethn <- yt_demogs_f(df = yt_ss, group = quos(yt, ethn_c), unit = pid2) %>%
  mutate(category = "Race/ethnicity", group = ethn_c, data = percent)
age <- bind_rows(lapply(seq(12,17), age_summ_f, df = yt_ss, cutoff = 0, 
                             pt_suf = "_h", long = T))

summ_stat_ever <- bind_rows(total, gender, ethn, age) %>%
  select(date, yt, category, group, data) %>%
  mutate(cohort = "Ever a resident")

rm(total, gender, ethn, age)
gc()



### Create line-level summaries for Tableau
# Monthly household
count_hh_mth <- yt_demogs_f(df = yt_ss, 
                            group = quos(yt, enroll_type, dual_elig_m, gender_c, 
                                         agegrp_h, ethn, time_housing), 
                            unit = hh_id_new_h,
                            period = "month")

# Monthly individual
count_pid_mth <- yt_demogs_f(df = yt_ss, 
                            group = quos(yt, enroll_type, dual_elig_m, gender_c, 
                                         agegrp_h, ethn, time_housing), 
                            unit = pid2,
                            period = "month")

# Annual household
count_hh_yr <- yt_demogs_f(df = yt_ss, 
                            group = quos(yt, enroll_type, dual_elig_m, gender_c, 
                                         agegrp_h, ethn, time_housing), 
                            unit = hh_id_new_h,
                            period = "year") %>%
  mutate(date = as.character(date))

# Annual individual
count_pid_yr <- yt_demogs_f(df = yt_ss, 
                             group = quos(yt, enroll_type, dual_elig_m, gender_c, 
                                          agegrp_h, ethn, time_housing), 
                             unit = pid2,
                             period = "year") %>%
  mutate(date = as.character(date))


# Combine files
yt_counts <- bind_rows(count_hh_mth, count_pid_mth, count_hh_yr, count_pid_yr)

# Convert a few numerical values to string
yt_counts$gender_c <- demo_codes$gender_c[match(yt_counts$gender_c, demo_codes$code)]
yt_counts$agegrp_h <- demo_codes$agegrp[match(yt_counts$agegrp_h, demo_codes$code)]
yt_counts$time_housing <- demo_codes$length_grp[match(yt_counts$time_housing, demo_codes$code)]


# Collapse to a single total
tabloop_total <- tabloop_f(yt_counts, sum = list_var(pop, pt_days),
                           fixed = list_var(unit, date, period, enroll_type, dual_elig_m),
                           loop = list_var(yt)) %>%
  filter(pt_days_sum > 0) %>%
  mutate(yt = as.numeric(group),
         group_cat = "Total",
         group = "Total")
  
tabloop_gender <- tabloop_f(yt_counts, sum = list_var(pop, pt_days),
                              fixed = list_var(yt, unit, date, period, enroll_type, dual_elig_m),
                              loop = list_var(gender_c)) %>%
  filter(!(period == "month" & !str_detect(date, "-")))

tabloop_age <- tabloop_f(yt_counts, sum = list_var(pop, pt_days),
                             fixed = list_var(yt, unit, date, period, enroll_type, dual_elig_m),
                             loop = list_var(agegrp_h)) %>%
  filter(pt_days_sum > 0)

tabloop_ethn <- tabloop_f(yt_counts, sum = list_var(pop, pt_days),
                         fixed = list_var(yt, unit, date, period, enroll_type, dual_elig_m),
                         loop = list_var(ethn)) %>%
  filter(pt_days_sum > 0)

tabloop_time <- tabloop_f(yt_counts, sum = list_var(pop, pt_days),
                         fixed = list_var(yt, unit, date, period, enroll_type, dual_elig_m),
                         loop = list_var(time_housing)) %>%
  filter(pt_days_sum > 0)

# Join dfs
yt_counts_final <- bind_rows(tabloop_total, tabloop_gender, tabloop_age, 
                             tabloop_ethn, tabloop_time)

rm(list = ls(pattern = "^count_"))
rm(list = ls(pattern = "^tabloop_"))
gc()


### Write Tableau=ready output
# Set up Excel file for export
wb_output <- createWorkbook()
addWorksheet(wb_output, "yt_stats")
addWorksheet(wb_output, "yt_demogs")

# Add data
writeData(wb_output, sheet = "yt_stats", x = summ_stat_ever)
writeData(wb_output, sheet = "yt_demogs", x = yt_counts_final)

# Export workbook
saveWorkbook(wb_output, file = paste0(housing_path, 
                                      "/OrganizedData/Summaries/YT/YT enrollment count_", 
                                      Sys.Date(), ".xlsx"),
             overwrite = T)


rm(list = ls(pattern = "^yt_count"))
rm(summ_stat)
gc()


#### 2) Demographics of people in YT/SS enrolled 30+ days on Medicaid, 2012–2017 ####
yt_coded_min <- bind_rows(lapply(seq(12,17), yt_popcode, df = yt_ss, year_pre = "pt",
                                 year_suf = NULL, agency = agency_new,
                                 enroll_type = enroll_type, dual = dual_elig_m,
                                 yt = yt, ss = ss, pt_cut = 30, min = T))

### Create summary stats for Tableau
total <- yt_demogs_f(df = yt_coded_min[yt_coded_min$pop_code %in% c(1, 2), ],
                     group = quos(yt), unit = pid2) %>%
  mutate(category = "Number of residents",
         group = "Total",
         data = pop)
gender <- yt_demogs_f(df = yt_coded_min[yt_coded_min$pop_code %in% c(1, 2), ], 
                      group = quos(yt, gender_c), unit = pid2) %>%
  mutate(category = "Gender", group = gender_c, data = percent)
ethn <- yt_demogs_f(df = yt_coded_min[yt_coded_min$pop_code %in% c(1, 2), ], 
                    group = quos(yt, ethn_c), unit = pid2) %>%
  mutate(category = "Race/ethnicity", group = ethn_c, data = percent)
age <- bind_rows(lapply(seq(12,17), age_summ_f, 
                        df = yt_coded_min[yt_coded_min$pop_code %in% c(1, 2), ],
                        cutoff = 0, long = T))

summ_stat_mcaid <- bind_rows(total, gender, ethn, age) %>%
  select(date, yt, category, group, data) %>%
  mutate(cohort = "Simultaneous non-dual Medicaid and housing coverage")

rm(total, gender, ethn, age)
gc()


### Write Tableau-ready output (requires #1 above to have been run)
summ_stat <- bind_rows(summ_stat_ever, summ_stat_mcaid)

# Export workbook
write.xlsx(summ_stat, file = paste0(housing_path, 
                                      "/OrganizedData/Summaries/YT/YT summary enrollment_", 
                                      Sys.Date(), ".xlsx"),
             overwrite = T)




#### 3) Look at drop off by year ####
# Make function to show the proportion of people still enrolled after x days
# Group results by YT (yt = 1) or SS (yt = 0)
surv_f <- function(df, year = 2012) {
  
  # Set up year-related variables
  ptx <- rlang::sym(paste0("pt", year-2000))
  
  if (year %in% c(2012, 2016)) {
    max_day <- 366
  } else {
    max_day <- 365
  }
  
  # Pull out person time for each year
  df <- df %>%
    filter((yt == 1 | ss == 1) & !is.na(!!ptx) & 
             enroll_type == "b" & dual_elig_m == "N") %>%
    select(pid2, startdate_c, enddate_c, !!ptx, yt, enroll_type) %>%
    # Sum up time in that year
    group_by(pid2, yt) %>% 
    mutate(pt_t = sum(!!ptx, na.rm = T)) %>% 
    ungroup() %>%
    select(-startdate_c, -enddate_c, -!!ptx) %>% 
    distinct()
  
  survx <- lapply(1:max_day, function(x) {
    doy <- df %>%
      group_by(yt) %>%
      mutate(num = ifelse(pt_t >= x, 1, 0)) %>%
      summarise(prop = sum(num)/n())
    })
  
  survx <- bind_rows(survx)
  return(survx)
}

# Set up number of days in a year
yr_leap <- data.frame(days = rep(1:366, each = 2))
yr <- data.frame(days = rep(1:365, each = 2))

# Apply function to all years
surv12 <- cbind(yr_leap, surv_f(df = yt_ss, year = 2012), year = 2012)
surv13 <- cbind(yr, surv_f(df = yt_ss, year = 2013), year = 2013)
surv14 <- cbind(yr, surv_f(df = yt_ss, year = 2014), year = 2014)
surv15 <- cbind(yr, surv_f(df = yt_ss, year = 2015), year = 2015)
surv16 <- cbind(yr_leap, surv_f(df = yt_ss, year = 2016), year = 2016)
surv17 <- cbind(yr, surv_f(df = yt_ss, year = 2017), year = 2017)

# Merge into single df
surv <- bind_rows(surv12, surv13, surv14, surv15, surv16, surv17) %>%
  mutate(year = as.factor(year),
         yt = factor(yt, levels = c(0, 1), labels = c("SS", "YT")))

# Plot survival curve
ggplot(surv, aes(x = days, y = prop)) +
  geom_line(aes(color = year, group = year)) +
  scale_colour_brewer(type = "qual") +
  geom_hline(yintercept = 0.9, linetype = "dashed", color = "#767F8B") +
  geom_vline(xintercept = 300, linetype = "dashed", color = "#767F8B") +
  facet_wrap( ~ yt, ncol = 1)

rm(list = ls(pattern = "surv1"))
rm(yr, yr_leap)


### Compare demographics by different cutoff groups
## 2012
# Make cutoffs
yt12 <- yt12 %>%
  mutate(gt300 = ifelse(pt_t >= 300, 1, 0),
         gt330 = ifelse(pt_t >= 330, 1, 0))
yt12_demog <- yt_elig_final %>%
  filter((yt == 1 | ss == 1) & !is.na(pt12) & enroll_type == "b") %>%
  select(pid2, yt, race_c, hisp_c, gender_c, age12_h, start_housing) %>%
  distinct() %>%
  right_join(., yt12, by = c("pid2", "yt"))

# Counts
yt12_demog %>% 
  group_by(yt, gt300) %>%
  summarise(count = n_distinct(pid2))
yt12_demog %>% 
  group_by(yt, gt330) %>%
  summarise(count = n_distinct(pid2))

# Age
stat.desc(yt12_demog$age12_h[yt12_demog$yt == 1 & yt12_demog$gt300 == 0], basic = F)
stat.desc(yt12_demog$age12_h[yt12_demog$yt == 1 & yt12_demog$gt300 == 1], basic = F)
stat.desc(yt12_demog$age12_h[yt12_demog$yt == 0 & yt12_demog$gt300 == 0], basic = F)
stat.desc(yt12_demog$age12_h[yt12_demog$yt == 0 & yt12_demog$gt300 == 1], basic = F)

stat.desc(yt12_demog$age12_h[yt12_demog$yt == 1 & yt12_demog$gt330 == 0], basic = F)
stat.desc(yt12_demog$age12_h[yt12_demog$yt == 1 & yt12_demog$gt330 == 1], basic = F)
stat.desc(yt12_demog$age12_h[yt12_demog$yt == 0 & yt12_demog$gt330 == 0], basic = F)
stat.desc(yt12_demog$age12_h[yt12_demog$yt == 0 & yt12_demog$gt330 == 1], basic = F)

# Gender
yt12_demog %>% 
  group_by(yt, gt300) %>%
  summarise(female = 2 - mean(gender_c, na.rm = T))
yt12_demog %>% 
  group_by(yt, gt330) %>%
  summarise(female = 2 - mean(gender_c, na.rm = T))

# Race
temp <- yt12_demog %>% 
  group_by(yt, gt300, race_c) %>%
  summarise(count = n_distinct(pid2)) %>%
  mutate(total = sum(count, na.rm = T),
         pct = count / total)
temp <- yt12_demog %>% 
  group_by(yt, gt330, race_c) %>%
  summarise(count = n_distinct(pid2)) %>%
  mutate(total = sum(count, na.rm = T),
         pct = count / total)
rm(temp)


## 2014
# Make cutoffs
yt14 <- yt14 %>%
  mutate(gt300 = ifelse(pt_t >= 300, 1, 0),
         gt330 = ifelse(pt_t >= 330, 1, 0))
yt14_demog <- yt_elig_final %>%
  filter((yt == 1 | ss == 1) & !is.na(pt14) & enroll_type == "b") %>%
  select(pid2, yt, race_c, hisp_c, gender_c, age14_h, start_housing) %>%
  distinct() %>%
  right_join(., yt14, by = c("pid2", "yt"))

# Counts
yt14_demog %>% 
  group_by(yt, gt300) %>%
  summarise(count = n_distinct(pid2))
yt14_demog %>% 
  group_by(yt, gt330) %>%
  summarise(count = n_distinct(pid2))

# Age
stat.desc(yt14_demog$age14_h[yt14_demog$yt == 1 & yt14_demog$gt300 == 0], basic = F)
stat.desc(yt14_demog$age14_h[yt14_demog$yt == 1 & yt14_demog$gt300 == 1], basic = F)
stat.desc(yt14_demog$age14_h[yt14_demog$yt == 0 & yt14_demog$gt300 == 0], basic = F)
stat.desc(yt14_demog$age14_h[yt14_demog$yt == 0 & yt14_demog$gt300 == 1], basic = F)

stat.desc(yt14_demog$age14_h[yt14_demog$yt == 1 & yt14_demog$gt330 == 0], basic = F)
stat.desc(yt14_demog$age14_h[yt14_demog$yt == 1 & yt14_demog$gt330 == 1], basic = F)
stat.desc(yt14_demog$age14_h[yt14_demog$yt == 0 & yt14_demog$gt330 == 0], basic = F)
stat.desc(yt14_demog$age14_h[yt14_demog$yt == 0 & yt14_demog$gt330 == 1], basic = F)

# Gender
yt14_demog %>% 
  group_by(yt, gt300) %>%
  summarise(female = 2 - mean(gender_c, na.rm = T))
yt14_demog %>% 
  group_by(yt, gt330) %>%
  summarise(female = 2 - mean(gender_c, na.rm = T))

# Race
temp <- yt14_demog %>% 
  group_by(yt, gt300, race_c) %>%
  summarise(count = n_distinct(pid2)) %>%
  mutate(total = sum(count, na.rm = T),
         pct = count / total)
temp <- yt14_demog %>% 
  group_by(yt, gt330, race_c) %>%
  summarise(count = n_distinct(pid2)) %>%
  mutate(total = sum(count, na.rm = T),
         pct = count / total)
rm(temp)


## 2016
# Make cutoffs
yt16 <- yt16 %>%
  mutate(gt300 = ifelse(pt_t >= 300, 1, 0),
         gt330 = ifelse(pt_t >= 330, 1, 0))
yt16_demog <- yt_elig_final %>%
  filter((yt == 1 | ss == 1) & !is.na(pt16) & enroll_type == "b") %>%
  select(pid2, yt, race_c, hisp_c, gender_c, age16_h, start_housing) %>%
  distinct() %>%
  right_join(., yt16, by = c("pid2", "yt"))

# Counts
yt16_demog %>% 
  group_by(yt, gt300) %>%
  summarise(count = n_distinct(pid2))
yt16_demog %>% 
  group_by(yt, gt330) %>%
  summarise(count = n_distinct(pid2))

# Age
stat.desc(yt16_demog$age16_h[yt16_demog$yt == 1 & yt16_demog$gt300 == 0], basic = F)
stat.desc(yt16_demog$age16_h[yt16_demog$yt == 1 & yt16_demog$gt300 == 1], basic = F)
stat.desc(yt16_demog$age16_h[yt16_demog$yt == 0 & yt16_demog$gt300 == 0], basic = F)
stat.desc(yt16_demog$age16_h[yt16_demog$yt == 0 & yt16_demog$gt300 == 1], basic = F)

stat.desc(yt16_demog$age16_h[yt16_demog$yt == 1 & yt16_demog$gt330 == 0], basic = F)
stat.desc(yt16_demog$age16_h[yt16_demog$yt == 1 & yt16_demog$gt330 == 1], basic = F)
stat.desc(yt16_demog$age16_h[yt16_demog$yt == 0 & yt16_demog$gt330 == 0], basic = F)
stat.desc(yt16_demog$age16_h[yt16_demog$yt == 0 & yt16_demog$gt330 == 1], basic = F)

# Gender
yt16_demog %>% 
  group_by(yt, gt300) %>%
  summarise(female = 2 - mean(gender_c, na.rm = T))
yt16_demog %>% 
  group_by(yt, gt330) %>%
  summarise(female = 2 - mean(gender_c, na.rm = T))

# Race
temp <- yt16_demog %>% 
  group_by(yt, gt300, race_c) %>%
  summarise(count = n_distinct(pid2)) %>%
  mutate(total = sum(count, na.rm = T),
         pct = count / total)
temp <- yt16_demog %>% 
  group_by(yt, gt330, race_c) %>%
  summarise(count = n_distinct(pid2)) %>%
  mutate(total = sum(count, na.rm = T),
         pct = count / total)
rm(temp)






