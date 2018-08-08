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
library(tidyverse) # Used to manipulate data
library(reshape2) # Used to reshape data
library(ipw) # Used to make marginal structural models


#### Assumptions ####
# 1) Code run in YT_medicaid_analyses to set up functions (until moved into a package)
# 2) Code run in YT_medicaid_analyses to join ED visits and yt_elig_final


#### Set up data ####
# Goal is to create cohort of people who spent time at YT or SS
# Min of 30 days in a year to be included

# Apply code to ensure 30+ days at YT/SS
yt_ed <- lapply(seq(12, 17), popcode_yt_f, df = yt_mcaid_ed)
yt_ed <- as.data.frame(data.table::rbindlist(yt_ed)) %>%
  arrange(pid2, startdate_c) %>%
  rename(year = year_code)

yt_ed <- yt_ed %>% filter(pop_code %in% c(1, 2))


### Remove extra rows
# Sum ED visits by year and YT (already have restricted to only ppl in housing
# and Medicaid who were not duals and were at YT or SS - using popcode above)
yt_ed_cnt <- yt_ed %>%
  filter(ed_year == year) %>%
  group_by(pid2, yt, year, ed_year) %>%
  summarise_at(
    vars(ed_cnt, ed_avoid),
    funs(ifelse(is.na(sum(., na.rm = T)), NA, sum(., na.rm = T)))
    ) %>%
  ungroup()

# Now sum person-time amd income by year and YT
yt_pt_cnt <- yt_ed %>%
  select(pid2, yt, year, pt12, pt13, pt14, pt15, pt16, pt17, 
         hh_inc_12_cap, hh_inc_13_cap, hh_inc_14_cap, hh_inc_15_cap, 
         hh_inc_16_cap, hh_inc_17_cap) %>%
  distinct() %>%
  group_by(pid2, yt, year) %>%
  mutate_at(
    vars(starts_with("pt")),
    funs(ifelse(is.na(sum(., na.rm = T)), NA, sum(., na.rm = T)))
  ) %>%
  mutate_at(
    vars(starts_with("hh_inc_1")),
    funs(ifelse(is.na(mean(., na.rm = T)), NA, mean(., na.rm = T)))
  ) %>%
  ungroup() %>%
  distinct()

# Join back and restrict to relevant variables
yt_ed <- yt_ed %>%
  distinct(pid2, year, yt, race_c, gender_c, age12, length12) %>%
  left_join(., yt_pt_cnt, by = c("pid2", "yt", "year")) %>%
  left_join(., yt_ed_cnt, by = c("pid2", "yt", "year")) %>%
  mutate_at(vars(ed_cnt, ed_avoid),
            funs(ifelse(is.na(.), 0, .))) %>%
  filter(is.na(ed_year) | ed_year == year) %>%
  select(-ed_year) %>%
  distinct()


# Group together time at YT or SS (mostly SS) in a year
# Need to reshape data and get time in one column
yt_ed <- melt(yt_ed,
               id.vars = c("year", "pid2", "yt", "race_c", "gender_c", 
                           "age12", "length12", "ed_cnt", "ed_avoid",
                           "hh_inc_12_cap", "hh_inc_13_cap", "hh_inc_14_cap", 
                           "hh_inc_15_cap", "hh_inc_16_cap", "hh_inc_17_cap"),
               variable.name = "pt_yr", value.name = "pt")

# Format and summarize
yt_ed <- yt_ed %>%
  mutate(pt_yr = as.numeric(paste0("20", str_sub(pt_yr, -2, -1)))) %>%
  filter(year == pt_yr) %>%
  arrange(pid2, year, pt) %>%
  select(-pt_yr)


# Now do the same for income
yt_ed <- melt(yt_ed,
              id.vars = c("year", "pid2", "yt", "race_c", "gender_c", 
                          "age12", "length12", "ed_cnt", "ed_avoid", "pt"),
              variable.name = "inc_yr", value.name = "inc")

yt_ed <- yt_ed %>%
  mutate(inc_yr = as.numeric(paste0("20", str_sub(inc_yr, -6, -5)))) %>%
  filter(year == inc_yr) %>%
  arrange(pid2, year, pt) %>%
  select(-inc_yr)
  

### Set up censoring
yt_ed <- yt_ed %>%
  arrange(pid2, year) %>%
  group_by(pid2) %>%
  mutate(cens_l = ifelse((is.na(lag(year, 1)) & year > 2012) | 
                           (year - lag(year, 1) > 1 & !is.na(lag(year, 1))), 1, 0),
         cens_r = ifelse((is.na(lead(year, 1)) & year < 2017) | 
                           (lead(year, 1) - year > 1 & !is.na(lead(year, 1))), 1, 0)) %>%
  ungroup()


### Set up for modeling
yt_ed2 <- yt_ed %>%
  arrange(pid2, yt, year) %>%
  group_by(pid2, yt) %>%
  mutate(timevar = row_number() - 1) %>%
  ungroup() %>%
  mutate(rate = case_when(
    year %in% c(2012, 2016) ~ ed_cnt / (pt / 366),
    TRUE ~ ed_cnt / (pt / 365)
    )) %>%
  filter(!(is.na(race_c) | is.na(gender_c) | 
             is.na(age12) | is.na(length12) | is.na(inc)))
yt_ed2 <- as.data.frame(yt_ed2)

#### END DATA SETUP ####


#### BASIC STATS ####
# Summary of censoring
yt_ed %>% group_by(yt, year) %>% 
  summarise(cens_l = sum(cens_l), cens_r = sum(cens_r), pop = n_distinct(pid2)) %>% 
  mutate(pct_l = round(cens_l/pop*100,1), pct_r = round(cens_r/pop*100,1))

### Review missingness
yt_ed %>% filter(is.na(race_c)) %>% group_by(yt, year) %>% summarise(count= n())
yt_ed %>% filter(is.na(gender_c)) %>% group_by(yt, year) %>% summarise(count= n())
yt_ed %>% filter(is.na(age12)) %>% group_by(yt, year) %>% summarise(count= n())
yt_ed %>% filter(is.na(length12)) %>% group_by(yt, year) %>% summarise(count= n())
yt_ed %>% filter(is.na(inc)) %>% group_by(yt, year) %>% summarise(count= n())


# Take a look at the rates
pastecs::stat.desc(yt_ed2$rate)
pastecs::stat.desc(yt_ed2$rate[yt_ed2$rate != 0])
hist(yt_ed2$rate)
hist(yt_ed2$rate[yt_ed2$rate != 0])


### Plot ED visits over time
yt_ed_sum <- yt_ed %>%
  filter(year < 2018) %>%
  group_by(yt, year) %>%
  summarise(ed_cnt = sum(ed_cnt, na.rm = T),
            pt = sum(pt, na.rm = T)) %>%
  ungroup() %>%
  mutate(rate = ed_cnt/pt * 100000,
         yt = ifelse(yt == 1, "YT", "Scattered sites"))

ggplot(data = yt_ed_sum, aes(x = year, y = rate)) +
  geom_line(aes(color = yt, group = yt), size = 1.3) +
  #ggtitle("Emergency department visit rates") +
  xlab("Year") +
  ylab("Rate (per 100,000 person-years") +
  theme(axis.text = element_text(size = 14),
        axis.title = element_text(size = 16),
        legend.title = element_blank(),
        legend.text = element_text(size = 14),
        panel.background = element_rect(fill = "grey95"))



#### MODELS ####

# Run simple negative binomial models
crude <- MASS::glm.nb(ed_cnt ~ yt + as.factor(year) + offset(log(pt)), data = yt_ed2[yt_ed2$year < 2017, ])
summary(crude)
exp(crude$coefficients)

ed_m1 <- MASS::glm.nb(ed_cnt ~ yt + as.factor(year) + race_c + gender_c + age12 + length12 + inc + offset(log(pt)), 
             data = yt_ed2[yt_ed2$year < 2017, ])
summary(ed_m1)
exp(ed_m1$coefficients)

#### Run zero inflated model ####
library(pscl)
library(snow)


summary(ed_zero <- zeroinfl(ed_cnt ~ yt + as.factor(year) + race_c + gender_c + age12 + length12 + offset(log(pt)) |
                              race_c + gender_c + age12 + length12 + offset(log(pt)),
                            data = yt_ed2))


# Look at 2012/2017
yt_ed_12 <- yt_ed2 %>% filter(year == 2012)
summary(zeroinfl(ed_cnt ~ yt + race_c + gender_c + age12 + length12 + offset(log(pt)) |
                              race_c + gender_c + age12 + length12 + offset(log(pt)),
                            data = yt_ed_12))


# Compare zero-infalted model with ordinary Poisson
vuong(ed_m1, ed_zero)


### Bootstrap CIs
dput(coef(ed_zero, "count"))
dput(coef(ed_zero, "zero"))

boot_zero_f <- function(data, i) {
  m <- pscl::zeroinfl(ed_cnt ~ yt + as.factor(year) + race_c + gender_c + age12 + length12 + offset(log(pt)) | 
                  race_c + gender_c + age12 + length12 + offset(log(pt)),
                data = data[i, ],
                start = list(count = c(-6.013, -0.1316, 0.1035, 0.1047, 0.1593, 0.0444,
                                       -0.1481, 0.9677, -0.3216, -0.8693, 0.3145,
                                       0.1215, 0.4881, 0.4773, -0.5049, 0.0443,
                                       0.3706, 0.5350, -0.2259, 0.0104, 0.0080),
                             zero = c(-6.846, -10.826, 1.465,
                                      0.7551, 0.9261, 0.7366, 0.8743,
                                      0.8961, -0.6448, -13.447, 0.7879,
                                      0.7159, -0.0497, 0.0041, 0.0600)))
  as.vector(t(do.call(rbind, coef(summary(m)))[, 1:2]))
}

set.seed(10)
ed_zero_boot <- boot(yt_ed2, boot_zero_f, R = 100, parallel = "snow", ncpus = 4)



#### End zero-inflated section ####

# Set up IPT weights and join back
w1 <- ipwtm(exposure = yt, 
            family = "binomial",
            link = "logit",
            numerator = ~ race_c + gender_c + age12 + length12,
            denominator = ~ race_c + gender_c + age12 + length12 + inc + year,
            id = pid2,
            timevar = timevar,
            type = "all",
            data = yt_ed2)

# Check mean of weights near 1
pastecs::stat.desc(w1$ipw.weights)


# Set up weights for left- and right-censoring
w_lc <- ipwtm(exposure = cens_l, 
              family = "binomial",
              link = "logit",
              numerator = ~ race_c + gender_c + age12 + length12,
              denominator = ~ race_c + gender_c + age12 + length12 + inc + year,
              id = pid2,
              timevar = timevar,
              type = "all",
              data = yt_ed2)

# Check mean of weights near 1
pastecs::stat.desc(w_lc$ipw.weights)


# Combine weights



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





