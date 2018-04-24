###############################################################################
# Code to analyze ED visits in YT vs SS
#
# Alastair Matheson (PHSKC-APDE)
# 2018-03-15
#
# NOTE THAT THIS CODE IS A WORK IN PROGRESS
#
###############################################################################

##### Set up global parameter and call in libraries #####
options(max.print = 350, tibble.print_max = 30, scipen = 999)

library(lubridate) # Used to manipulate dates
library(dplyr) # Used to manipulate data
library(reshape2) # Used to reshape data
library(stringr) # Used to manipulate string data
library(ipw) # Used to make marginal structural models


#### Assumptions ####
# 1) Code run in YT_medicaid_analyses to set up functions (until moved into a package)
# 2) Code run in YT_medicaid_analyses to join ED visits and yt_elig_final


#### Set up data ####
# Goal is to create cohort of people who spent time at YT or SS
# Min of 30 days in a year to be included

# Temp - join with income data
yt_ss <- left_join(yt_ss, sha_inc_trunc,
                   by = c("ssn_h" = "ssn", "lname_h" = "lname", 
                          "fname_h" = "fname", "dob_h" = "dob"))

yt_ss <- yt_ss %>%
  group_by(hhold_id_new, startdate_h) %>%
  mutate(hhold_size_new = n_distinct(pid2)) %>%
  ungroup() %>%
  mutate_at(vars(starts_with("hh_inc_tot")), funs(cap = . / hhold_size_new))
  


# Things take too long to load after a restart or crash to store data on secure drive
# saveRDS(yt_elig_ed, file = "K:/Housing/OrganizedData/SHA cleaning/temp_yt_elig_ed.Rda")
# yt_elig_ed <- readRDS(file = "K:/Housing/OrganizedData/SHA cleaning/temp_yt_elig_ed.Rda")

# Filter to only include YT and SS residents and relevant variables
yt_ed <- yt_elig_ed %>%
  filter(yt == 1 | ss == 1) %>%
  select(pid2, startdate_c, enddate_c, enroll_type, agency_new, dual_elig_m, yt, ss,
         race_c, hisp_c, gender_c, age12, length12, 
         inc_12:inc_17, asset_inc_12:asset_inc_17, hh_inc_tot_12:hh_inc_tot_17,
         hh_inc_tot_12_cap:hh_inc_tot_17_cap, hhold_size_new, pt12:pt16,
         from_date, ed_year)

# Apply code to ensure 30+ days at YT/SS
yt_ed <- lapply(seq(12, 16), popcode_yt_f, df = yt_ed)
yt_ed <- as.data.frame(data.table::rbindlist(yt_ed))

yt_ed <- yt_ed %>% filter(pop_code %in% c(1, 2))

# Deduplicate ED visits and summarise ED visit counts
yt_ed <- yt_ed %>% 
  mutate_at(vars(from_date, ed_year),
            funs(ifelse(!is.na(ed_year) & ed_year != year_code, NA, .))) %>%
  filter(is.na(ed_year) | ed_year == year_code) %>%
  distinct() %>%
  mutate(from_date = as.Date(from_date, origin = "1970-01-01"))

yt_ed_cnt <- yt_ed %>%
  filter(!is.na(ed_year)) %>%
  group_by(pid2, year_code) %>%
  summarise(ed_cnt = n()) %>%
  ungroup()

yt_ed <- left_join(yt_ed, yt_ed_cnt, by = c("pid2", "year_code")) %>%
  select(-from_date, -ed_year) %>%
  mutate(ed_cnt = ifelse(is.na(ed_cnt), 0, ed_cnt)) %>%
  distinct()

rm(yt_ed_cnt)

# Drop unnecessary columns
# Everyone should be SHA and enroll_type = b, no longer need start/end dates
yt_ed <- yt_ed %>%
  select(year_code, pid2, yt, ss, race_c:length12, 
         # Keep just household income divided by hhold size for now
         hh_inc_tot_12_cap:hh_inc_tot_16_cap,
         pt12:pt16, pop_code, ed_cnt) %>%
  rename(year = year_code)


# Group together time at YT or SS (mostly SS) in a year
# Need to reshape data and get time in one column
yt_ed <- melt(yt_ed,
               id.vars = c("year", "pid2", "yt", "ss", "race_c", "hisp_c",
                           "gender_c", "age12", "length12", "pop_code", "ed_cnt",
                           "hh_inc_tot_12_cap", "hh_inc_tot_13_cap", 
                           "hh_inc_tot_14_cap", "hh_inc_tot_15_cap", "hh_inc_tot_16_cap"),
               variable.name = "pt_yr", value.name = "pt")

# Format and summarize
yt_ed <- yt_ed %>%
  mutate(pt_year = as.numeric(paste0("20", str_sub(pt_yr, -2, -1)))) %>%
  filter(year == pt_year) %>%
  arrange(pid2, year, pt) %>%
  select(-pt_yr)


# Now do the same for income
yt_ed <- melt(yt_ed,
              id.vars = c("year", "pid2", "yt", "ss", "race_c", "hisp_c",
                          "gender_c", "age12", "length12", "pop_code", "ed_cnt",
                          "pt_year", "pt"),
              variable.name = "inc_yr", value.name = "inc")

yt_ed <- yt_ed %>%
  mutate(inc_yr = as.numeric(paste0("20", str_sub(inc_yr, -6, -5)))) %>%
  filter(year == inc_yr) %>%
  arrange(pid2, year, pt)



yt_ed <- yt_ed %>%
  group_by(pid2, year) %>%
  mutate(ed_cnt = sum(ed_cnt),
         pt = sum(pt)) %>%
  ungroup() %>%
  select(-pt_year, -inc_yr) %>%
  distinct()


### Set up censoring
yt_ed <- yt_ed %>%
  arrange(pid2, year) %>%
  group_by(pid2) %>%
  mutate(cens_l = ifelse((is.na(lag(year, 1)) & year > 2012) | 
                           (year - lag(year, 1) > 1 & !is.na(lag(year, 1))), 1, 0),
         cens_r = ifelse((is.na(lead(year, 1)) & year < 2016) | 
                           (lead(year, 1) - year > 1 & !is.na(lead(year, 1))), 1, 0)) %>%
  ungroup()

# Summary of censoring
yt_ed %>% group_by(yt, year) %>% 
  summarise(cens_l = sum(cens_l), cens_r = sum(cens_r), pop = n_distinct(pid2)) %>% 
  mutate(pct_l = round(cens_l/pop*100,1), pct_r = round(cens_r/pop*100,1))


### Review missingness
yt_ed %>% filter(is.na(race_c)) %>% group_by(yt, year) %>% summarise(count= n())
yt_ed %>% filter(is.na(hisp_c)) %>% group_by(yt, year) %>% summarise(count= n())
yt_ed %>% filter(is.na(gender_c)) %>% group_by(yt, year) %>% summarise(count= n())
yt_ed %>% filter(is.na(age12)) %>% group_by(yt, year) %>% summarise(count= n())
yt_ed %>% filter(is.na(length12)) %>% group_by(yt, year) %>% summarise(count= n())
yt_ed %>% filter(is.na(inc)) %>% group_by(yt, year) %>% summarise(count= n())


# Set up for modeling
yt_ed2 <- yt_ed %>%
  arrange(pid2, yt, year) %>%
  group_by(pid2, yt) %>%
  mutate(timevar = row_number() - 1) %>%
  ungroup() %>%
  mutate(rate = case_when(
    year %in% c(2012, 2016) ~ ed_cnt / (pt / 366),
    TRUE ~ ed_cnt / (pt / 365)
    )) %>%
  filter(!(is.na(race_c) | is.na(hisp_c) | is.na(gender_c) | 
             is.na(age12) | is.na(length12) | is.na(inc)))
yt_ed2 <- as.data.frame(yt_ed2)


# Take a look at the rates
pastecs::stat.desc(yt_ed2$rate)
pastecs::stat.desc(yt_ed2$rate[yt_ed2$rate != 0])
hist(yt_ed2$rate)
hist(yt_ed2$rate[yt_ed2$rate != 0])

# Set up IPT weights and join back
w1 <- ipwtm(exposure = yt, 
            family = "binomial",
            link = "logit",
            numerator = ~ race_c + hisp_c + gender_c + age12 + length12,
            denominator = ~ race_c + hisp_c + gender_c + age12 + length12 + inc + year,
            id = pid2,
            timevar = timevar,
            type = "all",
            data = yt_ed2)

# Check mean of weights near 1
pastecs::stat.desc(w1$ipw.weights)


# Set up weights for left- and right-censoring
w_lc <- ipwtm(exposure = left_censor, 
              family = "binomial",
              link = "logit",
              numerator = ~ race_c + hisp_c + gender_c + age12 + length12,
              denominator = ~ race_c + hisp_c + gender_c + age12 + length12 + inc + year,
              id = pid2,
              timevar = timevar,
              type = "all",
              data = yt_ed2)



# Join back to main data
ed_weights <- as.data.frame(w1$ipw.weights) %>% rename(iptw = `w1$ipw.weights`)
yt_ed2 <- cbind(yt_ed2, ed_weights)

# Run simple Poisson model
ed_m1 <- glm(ed_cnt ~ yt + offset(log(pt)), family = "poisson", data = yt_ed2, weights = iptw)
summary(ed_m1)


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
  denominator = ~ race_c + hisp_c + gender_c + age12 + length12,
  trunc = .05,
  data = yt_ed_temp2013
)


ed2013_w <- as.data.frame(w1_2013$ipw.weights) %>% rename(iptw = `w1_2013$ipw.weights`)
pastecs::stat.desc(ed2013_w$iptw)
yt_ed_temp2013 <- cbind(yt_ed_temp2013, ed2013_w)
summary(glm(ed_cnt ~ yt + offset(log(pt)), family = "poisson", data = yt_ed_temp2013, weights = iptw))


# Run regular regression model
summary(glm(ed_cnt ~ yt + offset(log(pt)), family = "poisson", data = yt_ed2))
summary(glm(ed_cnt ~ yt + as.factor(race_c) + hisp_c + gender_c + age12 + length12 + year + offset(log(pt)), family = "poisson", data = yt_ed2))


w1_2 <- ipwpoint2(
  exposure = yt3,
  family = "binomial",
  link = "logit",
  numerator = ~ as.factor(race_c) + hisp_c + gender_c + age12 + length12,
  denominator = ~ 1,
  trunc = .05,
  data = yt_ed_temp2013
)





#### END TESTING AREA ####





