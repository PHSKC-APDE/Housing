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

library(odbc) # Used to connect to SQL server
library(housing) # contains many useful functions for analyses
library(lubridate) # Used to manipulate dates
library(tidyverse) # Used to manipulate data
library(reshape2) # Used to reshape data
library(data.table) # Used to manipulate data
library(geepack) # Used for GEE models
library(broom) # Calculates CIs for GEE models
library(ipw) # Used to make marginal structural models

housing_path <- "//phdata01/DROF_DATA/DOH DATA/Housing"
manuscript_path <- "C:/Users/mathesal/King County/CDIP - Yesler_Terrace/Presentations, Papers, Conferences/2019 health outcomes paper"

#### Connect to the SQL server ####
db_apde51 <- dbConnect(odbc(), "PH_APDEStore51")
db_claims51 <- dbConnect(odbc(), "PHClaims51")


#### BRING IN DATA ####
# Bring in combined PHA/Medicaid data with some demographics already run ####
# Only bring in necessary columns andf filter to only include YT and SS residents
system.time(
  yt_mcaid_final <- dbGetQuery(db_apde51, 
                               "SELECT pid2, mid, startdate_c, enddate_c, dob_c, ethn_c,
                               gender_c, pt12, pt13, pt14, pt15, pt16, pt17,
                               age12, age13, age14, age15, age16, age17,
                               agency_new, enroll_type, dual_elig_m, yt, yt_old,
                               yt_new, ss,  yt_ever, ss_ever, place, start_type, end_type,
                               length12, length13, length14, length15, length16, length17, 
                               hh_inc_12_cap, hh_inc_13_cap, hh_inc_14_cap,
                               hh_inc_15_cap, hh_inc_16_cap, hh_inc_17_cap
                               FROM housing_mcaid_yt
                               WHERE yt = 1 OR ss = 1")
  )


### Bring in all chronic conditions and combine
chronic <- dbGetQuery(db_claims51,
                      "SELECT * FROM final.mcaid_claim_ccw
                      WHERE ccw_desc IN ('ccw_asthma', 'ccw_heart_failure', 'ccw_copd',
                                         'ccw_depression', 'ccw_diabetes', 'ccw_hypertension',
                                         'ccw_ischemic_heart_dis', 'ccw_chr_kidney_dis')")

chronic <- chronic %>% mutate_at(vars(from_date, to_date), list(~ as.Date(., origin = "1970-01-01")))


# Make a yearly summary for each person
chronic_yr <- lapply(seq(2012, 2018), function(x) {
  year_start = as.Date(paste0(x, "-01-01"), origin = "1970-01-01")
  year_end = as.Date(paste0(x, "-12-31"), origin = "1970-01-01")
  
  # Filter to only include people with the condition in that year
  # Very slow even with data.table solution
  cond <- chronic %>%
    filter(from_date <= year_end & to_date >= year_start)
    
  cond <- setDT(cond)
  cond[, `:=` (
    ccw_asthma = as.integer(ifelse(sum(str_detect(ccw_desc, "ccw_asthma"), na.rm = T) > 0, 1, 0)),
    ccw_heart_failure = as.integer(ifelse(sum(str_detect(ccw_desc, "ccw_heart_failure"), na.rm = T) > 0, 1, 0)),
    ccw_copd = as.integer(ifelse(sum(str_detect(ccw_desc, "ccw_copd"), na.rm = T) > 0, 1, 0)),
    ccw_depression = as.integer(ifelse(sum(str_detect(ccw_desc, "ccw_depression"), na.rm = T) > 0, 1, 0)),
    ccw_diabetes = as.integer(ifelse(sum(str_detect(ccw_desc, "ccw_diabetes"), na.rm = T) > 0, 1, 0)),
    ccw_hypertension = as.integer(ifelse(sum(str_detect(ccw_desc, "ccw_hypertension"), na.rm = T) > 0, 1, 0)),
    ccw_ischemic_heart_dis = as.integer(ifelse(sum(str_detect(ccw_desc, "ccw_ischemic_heart_dis"), na.rm = T) > 0, 1, 0)),
    ccw_chr_kidney_dis = as.integer(ifelse(sum(str_detect(ccw_desc, "ccw_chr_kidney_dis"), na.rm = T) > 0, 1, 0))
    ), by = id_mcaid]
    
    cond <- as.data.frame(cond) %>%
      mutate(year = x) %>%
      distinct(id_mcaid, year, ccw_asthma, ccw_heart_failure, ccw_copd, ccw_depression,
             ccw_diabetes, ccw_hypertension, ccw_ischemic_heart_dis, ccw_chr_kidney_dis)
    return(cond)
})
chronic_yr <- bind_rows(chronic_yr)


#### SET UP DATA FOR ANALYSIS ####
# Goal is to create cohort of people who spent time at YT or SS
# Min of 30 continuous days at YT/SS in a year to be included
yt_ss_30 <-  bind_rows(lapply(seq(12,18), yt_popcode, df = yt_mcaid_final, year_pre = "pt", 
                           year_suf = NULL, agency = agency_new, 
                           enroll_type = enroll_type, dual = dual_elig_m, 
                           yt = yt, ss = ss, pt_cut = 30, min = F))
yt_ss_30 <- yt_ss_30 %>% filter(pop_code %in% c(1, 2))


# Join to see if person was in old YT properties or not and make new category
yt_old_cnt <- bind_rows(lapply(seq(12, 18), function(x) {
  pt <- rlang::sym(paste0("pt", x))
  year_num = as.numeric(paste0(20, x))
  
  yt_old <- yt_mcaid_final %>%
    filter(!!pt > 0 & yt_old == 1) %>%
    mutate(year = year_num) %>%
    distinct(pid2, year, yt_old)
}))
yt_old_cnt <- yt_old_cnt %>%
  distinct(pid2) %>%
  mutate(yt_orig = 1)

yt_ss_30 <- left_join(yt_ss_30, yt_old_cnt, by = "pid2") %>%
  mutate(yt_orig = ifelse(is.na(yt_orig), 0, yt_orig))
  

# Combine person-time for each year
yt_ss_30 <- bind_rows(lapply(seq(12,18), function(x) {
  ptx <- rlang::sym(paste0("pt", x))
  year <- paste0(20, x)
  
  yt_ss_30 <- yt_ss_30 %>%
    filter(year_code == year) %>%
    group_by(pid2, year_code, yt, ss) %>%
    mutate(pt = sum(!!ptx)) %>%
    ungroup()
  
  yt_ss_30 <- yt_ss_30 %>%
    arrange(pid2, startdate_c, enddate_c) %>%
    group_by(pid2) %>%
    slice(n()) %>%
    ungroup()
}))


# Set up age12 for everyone even if missing (may make negative #s)
yt_ss_30 <- yt_ss_30 %>%
  mutate(age12 = case_when(
    !is.na(age12) ~ age12,
    !is.na(age13) ~ age13 - 1,
    !is.na(age14) ~ age14 - 2,
    !is.na(age15) ~ age15 - 3,
    !is.na(age16) ~ age16 - 4,
    !is.na(age17) ~ age17 - 5,
    !is.na(age18) ~ age18 - 6
  ))

# Set up length12 for everyone even if missing (may make negative #s)
yt_ss_30 <- yt_ss_30 %>%
  mutate(length12 = case_when(
    !is.na(length12) ~ length12,
    !is.na(length13) ~ length13 - 1,
    !is.na(length14) ~ length14 - 2,
    !is.na(length15) ~ length15 - 3,
    !is.na(length16) ~ length16 - 4,
    !is.na(length17) ~ length17 - 5,
    !is.na(length18) ~ length18 - 6
  ))


# Only keep relevant columns (no need to keep SS since YT = 0 == SS)
# Only need age and length from baseline (2012)
yt_ss_30 <- yt_ss_30 %>%
  select(pid2, mid, year_code, yt, yt_orig, ethn_c, gender_c, age12,
         length12, hh_inc_12_cap:hh_inc_18_cap, pt) %>%
  rename(year = year_code) %>%
  distinct()



### Join demographics and chronic status now that only YT and SS people are kept
yt_ss_chronic <- left_join(yt_ss_30, chronic_yr, by = c("id_mcaid", "year")) %>%
  mutate_at(vars(contains("ccw_")), list(~ifelse(is.na(.), 0, .)))


### Need to reshape household income vars in a single column
yt_ss_chronic <- melt(yt_ss_chronic,
              id.vars = c("year", "pid2", "id_mcaid", "yt", "yt_orig", "ethn_c", "gender_c", 
                          "age12", "length12", "pt", "ccw_asthma", "ccw_heart_failure",
                          "ccw_copd", "ccw_depression", "ccw_diabetes", "ccw_hypertension",
                          "ccw_ischemic_heart_dis", "ccw_chr_kidney_dis"),
              variable.name = "inc_yr", value.name = "hh_inc_cap")

# Format and summarize
# Need distinct as there were multiple income group combos per year
yt_ss_chronic <- yt_ss_chronic %>%
  mutate(inc_yr = as.numeric(paste0("20", str_sub(inc_yr, -6, -5)))) %>%
  filter(year == inc_yr) %>%
  arrange(pid2, year, pt) %>%
  select(-inc_yr) %>%
  distinct()


### Set up censoring and most recent hh_inc per capita
yt_ss_chronic <- yt_ss_chronic %>%
  arrange(pid2, year) %>%
  group_by(pid2) %>%
  mutate(cens_l = ifelse((is.na(lag(year, 1)) & year > 2012) | 
                           (year - lag(year, 1) > 1 & !is.na(lag(year, 1))), 1, 0),
         cens_r = ifelse((is.na(lead(year, 1)) & year <= 2019) | 
                           (lead(year, 1) - year > 1 & !is.na(lead(year, 1))), 1, 0),
         # Take first non-missing hh_inc
         inc_min = hh_inc_cap[which(!is.na(hh_inc_cap))[1]]
  ) %>%
  ungroup()


### Set up for modeling
yt_chronic <- yt_ss_chronic %>%
  arrange(pid2, yt, year) %>%
  group_by(pid2, yt) %>%
  mutate(timevar = row_number() - 1) %>%
  ungroup() %>%
  mutate(year_0 = year - 2012) %>%
  filter(!(is.na(ethn_c) | is.na(gender_c) | 
             is.na(age12) | is.na(length12) | is.na(hh_inc_cap)) &
           year <= 2019) %>%
  # Set up cumulative sum for time in YT since redevelopment
  mutate(yt_cumsum = ifelse(yt == 1 & !is.na(yt), 1, 0)) %>%
  # Make lagged variables
  group_by(pid2) %>%
  mutate(
    yt_lag = case_when(
      year == 2012 ~ NA_real_,
      TRUE ~ lag(yt, 1)
    ),
    yt_cumsum = cumsum(yt_cumsum)
  ) %>%
  ungroup()
yt_chronic <- as.data.frame(yt_chronic)

# People who were originally at YT and have stayed/returned vs. new arrivals
yt_orig_chronic <- yt_chronic %>%
  arrange(pid2, yt, year) %>%
  filter(yt == 1) %>%
  group_by(pid2, yt_orig) %>%
  mutate(timevar = row_number() - 1) %>%
  ungroup() %>%
  mutate(year_0 = year - 2012) %>%
  filter(!(is.na(ethn_c) | is.na(gender_c) | 
             is.na(age12) | is.na(length12) | is.na(hh_inc_cap)) &
           year <= 2019)
yt_orig_chronic <- as.data.frame(yt_orig_chronic)



# Make matrix of all possible years
all_years <- yt_chronic %>% distinct(pid2)
all_years <- data.frame(pid2 = rep(all_years$pid2, each = 7), year = rep(seq(2012, 2018), times = length(all_years)))

yt_chronic_all_yr <- left_join(all_years, yt_chronic, by = c("pid2", "year")) %>%
  group_by(pid2) %>%
  fill(ethn_c, gender_c, age12, hh_inc_cap, length12) %>%
  ungroup()
yt_chronic_all_yr <- yt_chronic_all_yr %>% 
  mutate(
    cens = ifelse(is.na(yt), 1, 0),
    # Remake year_0
    year_0 = year - 2012,
    # Set up cumulative sum for time in YT since redevelopment
    yt_cumsum = ifelse(yt == 1 & !is.na(yt), 1, 0)) %>%
  # Make lagged variables
  group_by(pid2) %>%
  mutate(
    yt_lag = case_when(
      year == 2012 ~ NA_real_,
      TRUE ~ lag(yt, 1)
      ),
    yt_cumsum = cumsum(yt_cumsum)
    ) %>%
  ungroup()



#### END DATA SETUP ####


#### FUNCTIONS ####
predict_f <- function(model, df, group_var, group1, group2, yt_only = F) {
  
  #Set up quosure
  group_quo <- enquo(group_var)
 
  predicted <- df %>%
    mutate(adj_pred = predict(model, newdata = df, type = "response"))
  
  if (yt_only == T) {
    predicted <- predicted %>% filter(year < 2019 & yt == 1)
  } else(
    predicted <- predicted %>% filter(year < 2019)
  )
  
  predicted <- predicted %>%
    group_by(!!group_quo, year) %>%
    summarise(ed_count = sum(ed_count, na.rm = T),
              adj_pred_count = sum(adj_pred, na.rm = T),
              pt = sum(pt, na.rm = T)) %>%
    ungroup() %>%
    mutate(
      rate =  case_when(
        year %in% c(2012, 2016) ~ ed_count/ (pt / 366) * 1000,
        TRUE ~ ed_count/ (pt / 365) * 1000
      ),
      adj_pred_rate =  case_when(
        year %in% c(2012, 2016) ~ adj_pred_count/ (pt / 366) * 1000,
        TRUE ~ adj_pred_count/ (pt / 365) * 1000
      ),
      yt_group = ifelse(!!group_quo == 1, group1, group2)
    ) %>%
    rowwise() %>%
    mutate(
      ci95_lb = case_when(
        year %in% c(2012, 2016) ~ poisson.test(ed_count)$conf.int[1] / (pt / 366) * 1000,
        TRUE ~ poisson.test(ed_count)$conf.int[1] / (pt / 365) * 1000
      ),
      ci95_ub = case_when(
        year %in% c(2012, 2016) ~ poisson.test(ed_count)$conf.int[2] / (pt / 366) * 1000,
        TRUE ~ poisson.test(ed_count)$conf.int[2] / (pt / 365) * 1000
      ),
      ci95_lb_adj = case_when(
        year %in% c(2012, 2016) ~ poisson.test(round(adj_pred_count, 0))$conf.int[1] / (pt / 366) * 1000,
        TRUE ~ poisson.test(round(adj_pred_count, 0))$conf.int[1] / (pt / 365) * 1000
      ), 
      ci95_ub_adj = case_when(
        year %in% c(2012, 2016) ~ poisson.test(round(adj_pred_count, 0))$conf.int[2] / (pt / 366) * 1000,
        TRUE ~ poisson.test(round(adj_pred_count, 0))$conf.int[2] / (pt / 365) * 1000
      )
    ) %>%
    ungroup()
  
  return(predicted)
}

plot_f <- function(df, labels, title, groups) {
  
  # Make quosure for groups
  groups_quo <- enquo(groups)
  
  plot <- ggplot(data = df, aes(x = year)) +
    geom_ribbon(aes(ymin = ci95_lb_adj, ymax = ci95_ub_adj, group = !!groups_quo, 
                    fill = !!groups_quo),
                data = df[df$rate_type == "Crude rate", ],
                alpha = 0.4,
                show.legend = T) +
    scale_fill_manual(labels = labels,
                      values = c("orange", "cyan2"), name="95% CI") +
    geom_line(aes(y = rate, color = !!groups_quo, group = interaction(!!groups_quo, rate_type),
                  linetype = !!groups_quo),
              size = 1.3,
              data = df[df$rate_type == "Crude rate", ]) +
    geom_line(aes(y = rate, color = yt_group, group = interaction(yt_group, rate_type),
                  linetype = yt_group),
              size = 0.8,
              data = df[df$rate_type == "Predicted rate", ]) +
    scale_linetype_manual(name = "YT residency", 
                          values = rep(c("solid", "dashed"), times = 2)) +
    scale_color_manual(name = "YT residency", 
                       values = c(rep(c("orangered3", "blue2"), each = 2))) +
    guides(
      linetype = guide_legend(override.aes = list(size = 0.8)),
      color = guide_legend(override.aes = list(fill=NA))
    ) +
    ggtitle(title) +
    xlab("Year") +
    ylab("Rate (per 1,000 person-years)") +
    theme(title = element_text(size = 18),
          axis.text = element_text(size = 14),
          axis.title = element_text(size = 16),
          legend.title = element_text(size = 14),
          legend.text = element_text(size = 12),
          panel.background = element_rect(fill = "grey95"))
  
  return(plot)
}

plot_simple_f <- function(df, title, groups) {
  
  # Make quosure for groups
  groups_quo <- enquo(groups)
  
  plot <- ggplot(data = df, aes(x = year, y = prop)) +
    geom_ribbon(aes(ymin = lb_95, ymax = ub_95, group = !!groups_quo, fill = yt == "YT"), 
                alpha = 0.4,
                show.legend = F) +
    geom_line(aes(color = yt, group = !!groups_quo), size = 1.3) +
    scale_fill_manual(values = c("orange", "cyan2"), name="fill") +
    scale_color_manual("YT residency", 
                       values = c("YT" = "blue2", "Scattered sites" = "orangered3")) +
    ggtitle(title) +
    xlab("Year") +
    ylab("Percent") +
    theme(axis.text = element_text(size = 14),
          axis.title = element_text(size = 16),
          legend.title = element_blank(),
          legend.text = element_text(size = 14),
          panel.background = element_rect(fill = "grey95"))
  return(plot)
}


summary_f <- function(df, outcome, outcome_name = NULL) {
  
  outcome_quo <- enquo(outcome)
  
  output <- df %>%
    filter(year < 2019) %>%
    group_by(yt, year) %>%
    summarise(success = sum(!!outcome_quo), failure = length(!!outcome_quo) - sum(!!outcome_quo)) %>%
    ungroup() %>%
    mutate(yt = ifelse(yt == 1, "YT", "Scattered sites"),
           outcome = outcome_name) %>%
    select(outcome, yt, year, success, failure)
  
  return(output)
}

binom_f <- function(s, f) {
  binom <- binom.test(c(s, f))
  result <- data.frame(prop = round(binom$estimate*100, 2),
                       lb_95 = round(binom$conf.int[1]*100, 2),
                       ub_95 = round(binom$conf.int[2]*100, 2))
  return(result)
}

#### BASIC STATS ####
### Review missingness
yt_ss_chronic %>% filter(is.na(ethn_c)) %>% group_by(yt, year) %>% summarise(count= n())
yt_ss_chronic %>% filter(is.na(gender_c)) %>% group_by(yt, year) %>% summarise(count= n())
yt_ss_chronic %>% filter(is.na(age12)) %>% group_by(yt, year) %>% summarise(count= n())
yt_ss_chronic %>% filter(is.na(length12)) %>% group_by(yt, year) %>% summarise(count= n())
yt_ss_chronic %>% filter(is.na(inc)) %>% group_by(yt, year) %>% summarise(count= n())

# Take a look at the rates
mean(yt_chronic$ccw_asthma)*100
mean(yt_chronic$ccw_heart_failure)*100
mean(yt_chronic$ccw_copd)*100
mean(yt_chronic$ccw_depression)*100
mean(yt_chronic$ccw_diabetes)*100
mean(yt_chronic$ccw_hypertension)*100
mean(yt_chronic$ccw_ischemic_heart_dis)*100
mean(yt_chronic$ccw_chr_kidney_dis)*100

binom.test(cbind(sum(yt_chronic$ccw_asthma), (length(yt_chronic$ccw_asthma) - sum(yt_chronic$ccw_asthma))))
binom.test(cbind(sum(yt_chronic$ccw_heart_failure), (length(yt_chronic$ccw_heart_failure) - sum(yt_chronic$ccw_heart_failure))))
binom.test(cbind(sum(yt_chronic$ccw_copd), (length(yt_chronic$ccw_copd) - sum(yt_chronic$ccw_copd))))
binom.test(cbind(sum(yt_chronic$ccw_depression), (length(yt_chronic$ccw_depression) - sum(yt_chronic$ccw_depression))))
binom.test(cbind(sum(yt_chronic$ccw_diabetes), (length(yt_chronic$ccw_diabetes) - sum(yt_chronic$ccw_diabetes))))
binom.test(cbind(sum(yt_chronic$ccw_hypertension), (length(yt_chronic$ccw_hypertension) - sum(yt_chronic$ccw_hypertension))))
binom.test(cbind(sum(yt_chronic$ccw_ischemic_heart_dis), (length(yt_chronic$ccw_ischemic_heart_dis) - sum(yt_chronic$ccw_ischemic_heart_dis))))
binom.test(cbind(sum(yt_chronic$ccw_chr_kidney_dis), (length(yt_chronic$ccw_chr_kidney_dis) - sum(yt_chronic$ccw_chr_kidney_dis))))


### Asthma proportion over time
yt_asthma_sum <- summary_f(df = yt_chronic, outcome = ccw_asthma, outcome_name = "Asthma")
yt_asthma_sum_bi <- bind_rows(mapply(binom_f, yt_asthma_sum$success, yt_asthma_sum$failure, SIMPLIFY = F))
yt_asthma_sum <- cbind(yt_asthma_sum, yt_asthma_sum_bi)

# Prepare simple plot
asthma_plot <- plot_simple_f(df = yt_asthma_sum, 
                             title = "Proportion with asthma diagnosis (unadjusted)",
                             groups = yt)
asthma_plot


### COPD proportion over time
yt_copd_sum <- summary_f(df = yt_chronic, outcome = ccw_copd, outcome_name = "COPD")
yt_copd_sum_bi <- bind_rows(mapply(binom_f, yt_copd_sum$success, yt_copd_sum$failure, SIMPLIFY = F))
yt_copd_sum <- cbind(yt_copd_sum, yt_copd_sum_bi)

# Prepare simple plot
copd_plot <- plot_simple_f(df = yt_copd_sum, 
                           title = "Proportion with depression diagnosis (unadjusted)",
                           groups = yt)
copd_plot


### Depression proportion over time
yt_dep_sum <- summary_f(df = yt_chronic, outcome = ccw_depression, outcome_name = "Depression")
yt_dep_sum_bi <- bind_rows(mapply(binom_f, yt_dep_sum$success, yt_dep_sum$failure, SIMPLIFY = F))
yt_dep_sum <- cbind(yt_dep_sum, yt_dep_sum_bi)

# Prepare simple plot
dep_plot <- plot_simple_f(df = yt_dep_sum, 
                           title = "Proportion with depression diagnosis (unadjusted)",
                           groups = yt)
dep_plot


### Diabetes proportion over time
yt_diab_sum <- summary_f(df = yt_chronic, outcome = ccw_diabetes, outcome_name = "Diabetes")
yt_diab_sum_bi <- bind_rows(mapply(binom_f, yt_diab_sum$success, yt_diab_sum$failure, SIMPLIFY = F))
yt_diab_sum <- cbind(yt_diab_sum, yt_diab_sum_bi)

# Prepare simple plot
diab_plot <- plot_simple_f(df = yt_diab_sum, 
                             title = "Proportion with diabetes diagnosis (unadjusted)",
                             groups = yt)
diab_plot


### Hypertension proportion over time
yt_hypt_sum <- summary_f(df = yt_chronic, outcome = ccw_hypertension, outcome_name = "Hypertension")
yt_hypt_sum_bi <- bind_rows(mapply(binom_f, yt_hypt_sum$success, yt_hypt_sum$failure, SIMPLIFY = F))
yt_hypt_sum <- cbind(yt_hypt_sum, yt_hypt_sum_bi)

# Prepare simple plot
hypt_plot <- plot_simple_f(df = yt_hypt_sum, 
                           title = "Proportion with hypertension diagnosis (unadjusted)",
                           groups = yt)
hypt_plot

# Figure out why the adjustment below makes such a big difference
yt_chronic %>%
  filter(ccw_hypertension == 1) %>%
  mutate(
    gender2 = case_when(
      gender_c == "Female" ~ 1,
      gender_c == "Male" ~ 0,
      TRUE ~ NA_real_
      ),
    black = case_when(
      ethn_c == "Black" ~ 1,
      ethn_c != "Black" ~ 0
    ),
    asian = case_when(
      ethn_c == "Asian" ~ 1,
      ethn_c != "Asian" ~ 0
    ),
    white = case_when(
      ethn_c == "White" ~ 1,
      ethn_c != "White" ~ 0
    )
    ) %>%
  group_by(yt) %>%
  distinct(pid2, .keep_all = T) %>%
  summarise(count = n(),
            avg_age = mean(age12),
            med_age = median(age12),
            female = mean(gender2, na.rm = T),
            los = mean(length12),
            black = mean(black, na.rm = T),
            asian = mean(asian, na.rm = T),
            white = mean(white, na.rm = T)) %>%
  ungroup()


### Plot asthma, depression, and hypertension together
chronic_combined <- bind_rows(yt_asthma_sum, yt_dep_sum, yt_hypt_sum)

chronic_plot <- ggplot(data = chronic_combined, aes(x = year, y = prop)) +
  geom_line()

chronic_plot <- plot_simple_f(df = chronic_combined, 
                              title = "Proportion with chronic condition diagnosis (unadjusted)",
                              groups = yt)
chronic_plot <- chronic_plot +
  expand_limits(y=0) +
  theme(plot.title = element_text(size = 20),
        axis.text = element_text(size = 14),
        axis.title = element_text(size = 16),
        axis.line = element_line(color = "black"),
        legend.title = element_blank(),
        legend.text = element_text(size = 12),
        legend.position = "bottom",
        panel.background = element_blank(),
        panel.grid.major = element_line(color = "grey40"),
        strip.text.y = element_text(size = 11)
  )
chronic_plot + facet_grid(outcome ~ .)

# Prepare cleaner plot for use in journal paper
chronic_plot_plain <- ggplot(data = chronic_combined, aes(x = year)) +
  geom_line(aes(y = prop, linetype = yt), size = 1.3) +
  # ggtitle("Emergency department visit rates (unadjusted)") +
  xlab("Year") +
  ylab("Percent") +
  geom_label_repel(aes(y = prop, label=ifelse(year %in% c(2012, 2018), round(prop, 1), ''))) +
  # expand_limits(y=0) +
  theme(plot.title = element_text(size = 20),
        axis.text = element_text(size = 12),
        axis.title = element_text(size = 12),
        # axis.line = element_line(color = "black"),
        legend.title = element_blank(),
        legend.text = element_text(size = 12),
        legend.position = "bottom",
        legend.background = element_rect(fill = "transparent"),
        legend.box.background = element_rect(fill = "transparent"),
        panel.background = element_rect(fill = "transparent"),
        panel.grid.major = element_line(color = "grey40"),
        panel.grid.major.x = element_blank(),
        axis.line.y.left = element_line(),
        panel.spacing.y = unit(1.5, "lines"),
        plot.background = element_rect(fill = "transparent", color = NA),
        strip.text.y = element_text(size = 11)
  )
manuscript_chronic_plot <- chronic_plot_plain + facet_grid(outcome ~ .)
chronic_plot_plain + facet_grid(outcome ~ ., scales = "free_y")

manuscript_chronic_plot


# Save plot and file associated with it
ggsave(manuscript_chronic_plot, 
       filename = file.path(manuscript_path, "chronic_plot.png"),
       bg = "transparent")
write.csv(chronic_combined, 
          file = file.path(manuscript_path, "chronic_plot_data.csv"), 
          row.names = F)



#### MODELS ####
#### Run simple GEE model ####
### Asthma
m_asth_gee_crude <- geeglm(ccw_asthma ~ yt,
                      id = pid2, corstr = "independence",
                      family = "binomial", data = yt_chronic)
m_asth_gee_crude <- geeglm(ccw_asthma ~ yt*year_0,
                           id = pid2, corstr = "independence",
                           family = "binomial", data = yt_chronic)

summary(m_asth_gee_crude)
exp(cbind(Estimate = coef(m_asth_gee_crude), confint_tidy(m_asth_gee_crude)))


### Depression
m_dep_gee_crude <- geeglm(ccw_depression ~ yt,
                           id = pid2, corstr = "independence",
                           family = "binomial", data = yt_chronic)
m_dep_gee_crude <- geeglm(ccw_depression ~ yt*year_0,
                           id = pid2, corstr = "independence",
                           family = "binomial", data = yt_chronic)

summary(m_dep_gee_crude)
exp(cbind(Estimate = coef(m_dep_gee_crude), confint_tidy(m_dep_gee_crude)))


### Hypertension
m_hypt_gee_crude <- geeglm(ccw_hypertension ~ yt,
                           id = pid2, corstr = "independence",
                           family = "binomial", data = yt_chronic)
m_hypt_gee_crude <- geeglm(ccw_hypertension ~ yt*year_0,
                           id = pid2, corstr = "independence",
                           family = "binomial", data = yt_chronic)

summary(m_hypt_gee_crude)
exp(cbind(Estimate = coef(m_hypt_gee_crude), confint_tidy(m_hypt_gee_crude)))


#### Run adjusted GEE model ####
### Asthma
m_asth_gee_adj <- geeglm(ccw_asthma ~ yt*year_0 + ethn_c + gender_c + 
                             age12 + length12 + hh_inc_cap,
                           id = pid2, corstr = "independence",
                           family = "binomial", data = yt_chronic)

summary(m_asth_gee_adj)
exp(cbind(Estimate = coef(m_asth_gee_adj), confint_tidy(m_asth_gee_adj)))

# Try using the Zelig package to make predicted values easier to run
# Currently not working with the offset term
mz_asth_gee_adj <- zelig(ccw_asthma ~ yt*year_0 + ethn_c + gender_c + 
                      age12 + length12 + hh_inc_cap,
                    data = yt_chronic, id = "pid2",
                    model = "logit.gee")
summary(mz_asth_gee_adj)
exp(mz_asth_gee_adj$get_coef()[[1]])

mz_gee_asthm_adj_x <- setx(mz_asth_gee_adj)
summary(sim(mz_asth_gee_adj, x = setx(mz_asth_gee_adj)))


### Hypertension
m_hypt_gee_adj <- geeglm(ccw_hypertension ~ yt*year_0 + ethn_c + gender_c + 
                           age12 + length12 + hh_inc_cap,
                         id = pid2, corstr = "independence",
                         family = "binomial", data = yt_chronic)

summary(m_hypt_gee_adj)
exp(cbind(Estimate = coef(m_hypt_gee_adj), confint_tidy(m_hypt_gee_adj)))


#### Run marginal structural model ####
# Set up IPT weights and join back
w1 <- ipwtm(exposure = yt, 
            family = "binomial",
            link = "logit",
            numerator = ~ ethn_c + gender_c + age12 + length12,
            denominator = ~ ethn_c + gender_c + age12 + length12 + hh_inc_cap + year_0,
            id = pid2,
            timevar = timevar,
            type = "all",
            trunc = 0.01,
            data = yt_chronic)

# Check mean of weights near 1
pastecs::stat.desc(w1$ipw.weights)
pastecs::stat.desc(w1$weights.trunc)

# Set up weights for left- and right-censoring
w_lc <- ipwtm(exposure = cens_l, 
              family = "binomial",
              link = "logit",
              numerator = ~ ethn_c + gender_c + age12 + length12,
              denominator = ~ ethn_c + gender_c + age12 + length12 + hh_inc_cap + year_0,
              id = pid2,
              timevar = timevar,
              type = "all",
              trunc = 0.01,
              data = yt_chronic)

w_rc <- ipwtm(exposure = cens_r, 
              family = "binomial",
              link = "logit",
              numerator = ~ ethn_c + gender_c + age12 + length12,
              denominator = ~ ethn_c + gender_c + age12 + length12 + hh_inc_cap + year_0,
              id = pid2,
              timevar = timevar,
              type = "all",
              trunc = 0.01,
              data = yt_chronic)


# Check mean of weights near 1
pastecs::stat.desc(w_lc$ipw.weights)
pastecs::stat.desc(w_lc$weights.trunc)
pastecs::stat.desc(w_rc$ipw.weights)
pastecs::stat.desc(w_rc$weights.trunc)

# Combine weights
yt_chronic$w1 <- w1$weights.trunc
yt_chronic$w_lc <- w_lc$weights.trunc
yt_chronic$w_rc <- w_rc$weights.trunc

yt_chronic <- yt_chronic %>%
  mutate(iptw = case_when(
    !is.na(w1) & !is.na(w_lc) & !is.na(w_rc) ~ w1 * w_lc * w_rc,
    !is.na(w1) & !is.na(w_lc) ~ w1 * w_lc,
    !is.na(w1) & !is.na(w_rc) ~ w1 * w_rc
  ))

pastecs::stat.desc(yt_chronic$iptw)

### Asthma
# Run GEE binomial MSM
m_asth_msm_gee <- geeglm(ccw_asthma ~ yt*year_0 + ethn_c + gender_c + 
                            age12 + length12 + hh_inc_cap,
                          id = pid2, corstr = "independence",
                          family = "binomial", data = yt_chronic,
                          weights = iptw)
summary(m_asth_msm_gee)
exp(cbind(Estimate = coef(m_asth_msm_gee), confint_tidy(m_asth_msm_gee)))


### Depression
# Run GEE Poisson MSM
m_dep_msm_gee <- geeglm(ccw_depression ~ yt*year_0 + ethn_c + gender_c + 
                            age12 + length12 + hh_inc_cap,
                          id = pid2, corstr = "independence",
                          family = "binomial", data = yt_chronic,
                          weights = iptw)
summary(m_dep_msm_gee)
exp(cbind(Estimate = coef(m_dep_msm_gee), confint_tidy(m_dep_msm_gee)))


### Hypertension
# Run GEE Poisson MSM
m_hypt_msm_gee <- geeglm(ccw_hypertension ~ yt*year_0 + ethn_c + gender_c + 
                            age12 + length12 + hh_inc_cap,
                          id = pid2, corstr = "independence",
                          family = "binomial", data = yt_chronic,
                          weights = iptw)
summary(m_hypt_msm_gee)
exp(cbind(Estimate = coef(m_hypt_msm_gee), confint_tidy(m_hypt_msm_gee)))

  
#### Run history-adjusted marginal structural model ####
# Set up IPT weights and join back

w1 <- ipwtm(exposure = yt, 
            family = "binomial",
            link = "logit",
            numerator = ~ ethn_c + gender_c + age12 + length12,
            denominator = ~ ethn_c + gender_c + age12 + length12 + hh_inc_cap + year,
            id = pid2,
            timevar = timevar,
            type = "all",
            trunc = 0.01,
            data = yt_ed_all_yr)

# Check mean of weights near 1
pastecs::stat.desc(w1$ipw.weights)
pastecs::stat.desc(w1$weights.trunc)

# Set up weights for left- and right-censoring
w_lc <- ipwtm(exposure = cens_l, 
              family = "binomial",
              link = "logit",
              numerator = ~ ethn_c + gender_c + age12 + length12,
              denominator = ~ ethn_c + gender_c + age12 + length12 + hh_inc_cap + year,
              id = pid2,
              timevar = timevar,
              type = "all",
              trunc = 0.01,
              data = yt_ed)

w_rc <- ipwtm(exposure = cens_r, 
              family = "binomial",
              link = "logit",
              numerator = ~ ethn_c + gender_c + age12 + length12,
              denominator = ~ ethn_c + gender_c + age12 + length12 + hh_inc_cap + year,
              id = pid2,
              timevar = timevar,
              type = "all",
              trunc = 0.01,
              data = yt_ed)




#### Run structured nested mean model ####




#### TESTING AREA ####
# Make single year df to try point formula
# t1 code comes from pg 53 here: https://pdfs.semanticscholar.org/bdf2/73f2db7b981725003f2d6811eecee9c926b1.pdf
yt_ed_temp2013 <- yt_ed2 %>% filter(year == 2013)
yt_ed_temp2013 <- as.data.frame(yt_ed_temp2013)

yt3 <- rbinom(nrow(yt_ed_temp2013), 1, 0.4)

yt_ed_temp2013_2 <- cbind(yt_ed_temp2013, yt3)


w1_2013 <- ipwpoint(
  exposure = yt,
  family = "binomial",
  link = "logit",
  numerator = ~ 1,
  denominator = ~ ethn_c + hisp_c + gender_c + age12 + length12,
  trunc = .05,
  data = yt_ed_temp2013
)


ed2013_w <- as.data.frame(w1_2013$ipw.weights) %>% rename(iptw = `w1_2013$ipw.weights`)
pastecs::stat.desc(ed2013_w$iptw)
yt_ed_temp2013 <- cbind(yt_ed_temp2013, ed2013_w)
summary(glm(ed_count ~ yt + offset(log(pt)), family = "poisson", data = yt_ed_temp2013, weights = iptw))


# Run regular regression model
summary(glm(ed_count ~ yt + offset(log(pt)), family = "poisson", data = yt_ed2))
summary(glm(ed_count ~ yt + as.factor(ethn_c) + hisp_c + gender_c + age12 + length12 + year + offset(log(pt)), family = "poisson", data = yt_ed2))



# Set up IPT weights for each year
w_year <- bind_rows(lapply(seq(2012, 2017), function(x) {
  # Run weights up until and including that year
  weight <- ipwtm(exposure = yt, 
                 family = "binomial",
                 link = "logit",
                 numerator = ~ ethn_c + gender_c + age12 + length12,
                 denominator = ~ ethn_c + gender_c + age12 + length12 + hh_inc_cap + year,
                 id = pid2,
                 timevar = timevar,
                 type = "all",
                 trunc = 0.01,
                 data = yt_ed[yt_ed$year <= x, ])
  
  # Check mean of weights near 1
  print(paste0("Year: ", x))
  print(pastecs::stat.desc(weight$weights.trunc))
  
  # Join back to main data and filter for just that year
  result <- yt_ed[yt_ed$year <= x, ]
  result$weight <- weight$weights.trunc
  
  # Add censoring weights
  if (x > 2012) {
    weight_lc <- ipwtm(exposure = cens_l,
                       family = "binomial",
                       link = "logit",
                       numerator = ~ ethn_c + gender_c + age12 + length12,
                       denominator = ~ ethn_c + gender_c + age12 + length12 + hh_inc_cap + year,
                       id = pid2,
                       timevar = timevar,
                       type = "all",
                       trunc = 0.01,
                       data = yt_ed[yt_ed$year <= x, ])
    print(pastecs::stat.desc(weight_lc$weights.trunc))
    result$weight_lc <- weight_lc$weights.trunc
  } else {
    result$weight_lc <- 1
  }
  
  if (x < 2017) {
    weight_rc <- ipwtm(exposure = cens_r,
                       family = "binomial",
                       link = "logit",
                       numerator = ~ ethn_c + gender_c + age12 + length12,
                       denominator = ~ ethn_c + gender_c + age12 + length12 + hh_inc_cap + year,
                       id = pid2,
                       timevar = timevar,
                       type = "all",
                       trunc = 0.01,
                       data = yt_ed[yt_ed$year <= x, ])
    print(pastecs::stat.desc(weight_rc$weights.trunc))
    result$weight_rc <- weight_rc$weights.trunc
  } else {
    result$weight_rc <- 1
    }

  # Restrict to current year and multiple weights
  result <- result %>% filter(year == x) %>%
    mutate(iptw = case_when(
      !is.na(weight) & !is.na(weight_lc) & !is.na(weight_rc) ~ weight * weight_lc * weight_rc,
      !is.na(weight) & !is.na(weight_lc) ~ weight * weight_lc,
      !is.na(weight) & !is.na(weight_rc) ~ weight * weight_rc
    )) %>%
    arrange(pid2, year)
}))
pastecs::stat.desc(w_year$iptw)

m_msm_yr_gee <- geeglm(ed_count ~ yt + offset(log(pt)),
                    id = pid2, corstr = "independence",
                    family = "poisson", data = w_year,
                    weights = iptw)
summary(m_msm_yr_gee)
exp(cbind(Estimate = coef(m_msm_yr_gee), confint_tidy(m_msm_yr_gee)))



yt_ed <- yt_ed %>% left_join(., yt_ed_all_yr)


#### END TESTING AREA ####





