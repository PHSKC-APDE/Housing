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
library(openxlsx) # Used to import/export Excel files
library(housing) # contains many useful functions for analyses
library(medicaid) # contains many useful functions for analyses
library(lubridate) # Used to manipulate dates
library(tidyverse) # Used to manipulate data
library(reshape2) # Used to reshape data
library(ggrepel) # Used to fix up labels on graphs
library(geepack) # Used for GEE models
library(broom) # Calculates CIs for GEE models
library(ipw) # Used to make marginal structural models

housing_path <- "//phdata01/DROF_DATA/DOH DATA/Housing"

#### Connect to the SQL server ####
db.apde51 <- dbConnect(odbc(), "PH_APDEStore51")
db.claims51 <- dbConnect(odbc(), "PHClaims51")


#### FUNCTIONS ####
predict_f <- function(model, group_var, group1, group2, yt_only = F) {
  
  #Set up quosure
  group_quo <- enquo(group_var)
  
  predicted <- yt_ss_ed_join %>%
    mutate(adj_pred = predict(model, newdata = yt_ss_ed_join, type = "response"))
  
  if (yt_only == T) {
    predicted <- predicted %>% filter(year < 2018 & yt == 1)
  } else(
    predicted <- predicted %>% filter(year < 2018)
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


#### BRING IN DATA ####
# Bring in combined PHA/Medicaid data with some demographics already run ####
# Only bring in necessary columns
system.time(
  yt_mcaid_final <- dbGetQuery(db.apde51, 
                               "SELECT pid2, mid, startdate_c, enddate_c, dob_c, ethn_c,
                               gender_c, pt12, pt13, pt14, pt15, pt16, pt17,
                               age12, age13, age14, age15, age16, age17,
                               agency_new, enroll_type, dual_elig_m, yt, yt_old,
                               yt_new, ss,  yt_ever, ss_ever, place, start_type, end_type,
                               length12, length13, length14, length15, length16, length17, 
                               hh_inc_12_cap, hh_inc_13_cap, hh_inc_14_cap,
                               hh_inc_15_cap, hh_inc_16_cap, hh_inc_17_cap
                               FROM housing_mcaid_yt")
  )

# Filter to only include YT and SS residents
yt_ss <- yt_mcaid_final %>% filter(yt == 1 | ss == 1)


# ED visits (using broad definition)
system.time(
  ed <- dbGetQuery(db.claims51, 
                   "SELECT id, tcn, from_date, to_date, ed, ed_emergent_nyu, ed_nonemergent_nyu
                   FROM dbo.mcaid_claim_summary
                   WHERE ed = 1")
  )


#### SET UP DATA FOR ANALYSIS ####
# Goal is to create cohort of people who spent time at YT or SS
# Min of 30 continuous days at YT/SS in a year to be included
yt_ss_30 <-  bind_rows(lapply(seq(12,17), yt_popcode, df = yt_ss, year_pre = "pt", 
                           year_suf = NULL, agency = agency_new, 
                           enroll_type = enroll_type, dual = dual_elig_m, 
                           yt = yt, ss = ss, pt_cut = 30, min = F))
yt_ss_30 <- yt_ss_30 %>% filter(pop_code %in% c(1, 2))


# Join to see if person was in old YT properties or not and make new category
yt_old_cnt <- bind_rows(lapply(seq(12, 17), function(x) {
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
  


### Join demographics and ED events now that only YT and SS people are kept
yt_ss_ed <- left_join(yt_ss_30, ed, by = c("mid" = "id")) %>%
  filter(ed == 1 & from_date >= startdate_c & from_date <= enddate_c &
           agency_new == "SHA" & year(from_date) == year_code) %>%
  mutate(ed_year = year(from_date))


# Count number of ED visits each person had during each period
yt_ss_ed_sum <- bind_rows(lapply(seq(12, 17), count_acute,
                                 df = yt_ss_ed,
                                 group_var = quos(pid2),
                                 age_var = NULL, len_var = NULL,
                                 event = ed, event_year = ed_year,
                                 unit = pid2, person = F,
                                 birth = NULL))

yt_ss_ed_avoid_sum <- bind_rows(lapply(seq(12, 17), count_acute,
                                 df = yt_ss_ed,
                                 group_var = quos(pid2),
                                 age_var = NULL, len_var = NULL,
                                 event = ed_nonemergent_nyu, event_year = ed_year,
                                 unit = pid2, person = F,
                                 birth = NULL))

yt_ss_ed_unavoid_sum <- bind_rows(lapply(seq(12, 17), count_acute,
                                       df = yt_ss_ed,
                                       group_var = quos(pid2),
                                       age_var = NULL, len_var = NULL,
                                       event = ed_emergent_nyu, event_year = ed_year,
                                       unit = pid2, person = F,
                                       birth = NULL))


# Combine person-time for each year
yt_ss_30 <- bind_rows(lapply(seq(12,17), function(x) {
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
    !is.na(age17) ~ age17 - 5
  ))

# Set up length12 for everyone even if missing (may make negative #s)
yt_ss_30 <- yt_ss_30 %>%
  mutate(length12 = case_when(
    !is.na(length12) ~ length12,
    !is.na(length13) ~ length13 - 1,
    !is.na(length14) ~ length14 - 2,
    !is.na(length15) ~ length15 - 3,
    !is.na(length16) ~ length16 - 4,
    !is.na(length17) ~ length17 - 5
  ))


# Only keep relevant columns (no need to keep SS since YT = 0 == SS)
# Only need age and length from baseline (2012)
yt_ss_30 <- yt_ss_30 %>%
  select(pid2, mid, year_code, yt, yt_orig, ethn_c, gender_c, age12,
         length12, hh_inc_12_cap:hh_inc_17_cap, pt) %>%
  rename(year = year_code) %>%
  distinct()



# Join ED visits back to overall population
yt_ss_ed_join <- left_join(yt_ss_30, yt_ss_ed_sum,
                           by = c("pid2", "year")) %>%
  mutate(count = ifelse(is.na(count), 0, count)) %>%
  rename(ed_count = count)

yt_ss_ed_join <- left_join(yt_ss_ed_join, yt_ss_ed_avoid_sum,
                           by = c("pid2", "year")) %>%
  mutate(count = ifelse(is.na(count), 0, count)) %>%
  rename(ed_avoid_count = count)

yt_ss_ed_join <- left_join(yt_ss_ed_join, yt_ss_ed_unavoid_sum,
                           by = c("pid2", "year")) %>%
  mutate(count = ifelse(is.na(count), 0, count)) %>%
  rename(ed_unavoid_count = count)


### Need to reshape household income vars in a single column
yt_ss_ed_join <- melt(yt_ss_ed_join,
              id.vars = c("year", "mid", "pid2", "yt", "yt_orig", "ethn_c", "gender_c", 
                          "age12", "length12", "pt", 
                          "ed_count", "ed_avoid_count", "ed_unavoid_count"),
              variable.name = "inc_yr", value.name = "inc")

# Format and summarize
# Need distinct as there were multiple income group combos per year
yt_ss_ed_join <- yt_ss_ed_join %>%
  mutate(inc_yr = as.numeric(paste0("20", str_sub(inc_yr, -6, -5)))) %>%
  filter(year == inc_yr) %>%
  arrange(pid2, year, pt) %>%
  select(-inc_yr) %>%
  distinct()


### Set up censoring and most recent hh_inc per capita
yt_ss_ed_join <- yt_ss_ed_join %>%
  arrange(pid2, year) %>%
  group_by(pid2) %>%
  mutate(cens_l = ifelse((is.na(lag(year, 1)) & year > 2012) | 
                           (year - lag(year, 1) > 1 & !is.na(lag(year, 1))), 1, 0),
         cens_r = ifelse((is.na(lead(year, 1)) & year < 2017) | 
                           (lead(year, 1) - year > 1 & !is.na(lead(year, 1))), 1, 0),
         # Take first non-missing hh_inc
         inc_min = inc[which(!is.na(inc))[1]]
         ) %>%
  ungroup()


### Set up for modeling
yt_ed <- yt_ss_ed_join %>%
  arrange(pid2, yt, year) %>%
  group_by(pid2, yt) %>%
  mutate(timevar = row_number() - 1) %>%
  ungroup() %>%
  mutate(
    ed_rate = case_when(
      year %in% c(2012, 2016) ~ ed_count / (pt / 366),
      TRUE ~ ed_count / (pt / 365)
      ),
    ed_avoid_rate = case_when(
      year %in% c(2012, 2016) ~ ed_avoid_count / (pt / 366),
      TRUE ~ ed_avoid_count / (pt / 365)
    ),
    ed_unavoid_rate = case_when(
      year %in% c(2012, 2016) ~ ed_unavoid_count / (pt / 366),
      TRUE ~ ed_unavoid_count / (pt / 365)
    ),
    # Set up variables where transformation doesn't work in Zelig function
    pt_log = log(pt),
    year_0 = year - 2012
  ) %>%
  filter(!(is.na(ethn_c) | is.na(gender_c) | 
             is.na(age12) | is.na(length12) | is.na(inc)) &
           year <= 2017) %>%
  # Set up cumulative sum for time in YT since redevelopment
  mutate(yt_cumsum = ifelse(yt == 1 & !is.na(yt), 1, 0)) %>%
  # Make lagged variables
  group_by(pid2) %>%
  mutate(
    yt_lag = case_when(
      year == 2012 ~ NA_real_,
      TRUE ~ lag(yt, 1)
    ),
    rate_lag = case_when(
      ed_rate == 2012 ~ NA_real_,
      TRUE ~ lag(ed_rate, 1)
    ),
    yt_cumsum = cumsum(yt_cumsum)
  ) %>%
  ungroup()
yt_ed <- as.data.frame(yt_ed)

# People who were originally at YT and have stayed/returned vs. new arrivals
yt_orig_ed <- yt_ss_ed_join %>%
  arrange(pid2, yt, year) %>%
  filter(yt == 1) %>%
  group_by(pid2, yt_orig) %>%
  mutate(timevar = row_number() - 1) %>%
  ungroup() %>%
  mutate(
    ed_rate = case_when(
      year %in% c(2012, 2016) ~ ed_count / (pt / 366),
      TRUE ~ ed_count / (pt / 365)
    ),
    ed_avoid_rate = case_when(
      year %in% c(2012, 2016) ~ ed_avoid_count / (pt / 366),
      TRUE ~ ed_avoid_count / (pt / 365)
    ),
    ed_unavoid_rate = case_when(
      year %in% c(2012, 2016) ~ ed_unavoid_count / (pt / 366),
      TRUE ~ ed_unavoid_count / (pt / 365)
    ),
    # Set up variables where transformation doesn't work in Zelig function
    pt_log = log(pt),
    year_0 = year - 2012
  ) %>%
  filter(!(is.na(ethn_c) | is.na(gender_c) | 
             is.na(age12) | is.na(length12) | is.na(inc)) &
           year <= 2017)
yt_orig_ed <- as.data.frame(yt_orig_ed)



# Make matrix of all possible years
all_years <- yt_ed %>% distinct(pid2)
all_years <- data.frame(pid2 = rep(all_years$pid2, each = 6), year = rep(seq(2012, 2017), times = length(all_years)))
# yt_ed_all_yr <- left_join(all_years, distinct(yt_ed, pid2, ethn_c, gender_c, age12, inc_min), by = c("pid2"))
# yt_ed_all_yr <- left_join(yt_ed_all_yr, select(yt_ed, pid2, year, yt, yt_orig, length12, pt, ed_count, inc, rate, pt_log), by = c("pid2", "year")) %>%
#   # Remake year_0
#   mutate(year_0 = year - 2012)

yt_ed_all_yr <- left_join(all_years, yt_ed, by = c("pid2", "year")) %>%
  group_by(pid2) %>%
  fill(ethn_c, gender_c, age12, inc_min, length12) %>%
  ungroup()

yt_ed_all_yr <- yt_ed_all_yr %>% 
  mutate(
    cens = ifelse(is.na(yt), 1, 0),
    # Remake year_0
    year_0 = year - 2012,
    # Set up cumulative sum for time in YT since redevelopment
    yt_cumsum = ifelse(yt == 1 & !is.na(yt), 1, 0)
    ) %>%
  # Make lagged variables
  group_by(pid2) %>%
  mutate(
    yt_lag = case_when(
      year == 2012 ~ NA_real_,
      TRUE ~ lag(yt, 1)
      ),
    rate_lag = case_when(
      ed_rate == 2012 ~ NA_real_,
      TRUE ~ lag(ed_rate, 1)
      ),
    yt_cumsum = cumsum(yt_cumsum)
    ) %>%
  ungroup()



#### END DATA SETUP ####


#### BASIC STATS ####
# Summary of censoring
yt_ed %>% group_by(yt, year) %>% 
  summarise(cens_l = sum(cens_l), cens_r = sum(cens_r), pop = n_distinct(pid2)) %>% 
  mutate(pct_l = round(cens_l/pop*100,1), pct_r = round(cens_r/pop*100,1))

### Review missingness
yt_ss_ed_join %>% filter(is.na(ethn_c)) %>% group_by(yt, year) %>% summarise(count= n())
yt_ss_ed_join %>% filter(is.na(gender_c)) %>% group_by(yt, year) %>% summarise(count= n())
yt_ss_ed_join %>% filter(is.na(age12)) %>% group_by(yt, year) %>% summarise(count= n())
yt_ss_ed_join %>% filter(is.na(length12)) %>% group_by(yt, year) %>% summarise(count= n())
yt_ss_ed_join %>% filter(is.na(inc)) %>% group_by(yt, year) %>% summarise(count= n())


# One or more ED visits
yt_ss_ed_join %>%
  mutate(any_ed = if_else(ed_count > 0, 1, 0)) %>%
  group_by(year, yt, any_ed) %>%
  summarise(count = n()) %>%
  group_by(year, yt) %>%
  mutate(pop = sum(count)) %>%
  ungroup() %>%
  mutate(pct = round(count/pop, 3)) %>%
  filter(any_ed == 1) %>%
  select(yt, year, pop, pct) %>%
  arrange(yt, year)
  

# Take a look at the rates
pastecs::stat.desc(yt_ed$rate)
pastecs::stat.desc(yt_ed$rate[yt_ed$rate != 0])
hist(yt_ed$rate)
hist(yt_ed$rate[yt_ed$rate != 0])


#### Plot ED visits over time ####
yt_ed_sum_f <- function(ed_type = c("all", "avoid", "unavoid")) {
  
  if (ed_type == "all") {
    ed <- quo(ed_count)
    type <- "All ED visits"
  } else if (ed_type == "avoid") {
    ed <- quo(ed_avoid_count)
    type <- "Potentially avoidable ED visits"
  } else if (ed_type == "unavoid") {
    ed <- quo(ed_unavoid_count)
    type <- "Unavoidable ED visits"
  }
  
  output <- yt_ss_ed_join %>%
    filter(year < 2018) %>%
    group_by(yt, year) %>%
    summarise(ed_count = sum(!!ed, na.rm = T),
              pt = sum(pt, na.rm = T)) %>%
    ungroup() %>%
    mutate(
      rate = case_when(
        year %in% c(2012, 2016) ~ ed_count / (pt / 366) * 1000,
        TRUE ~ ed_count / (pt / 365) * 1000
      ),
      yt = ifelse(yt == 1, "YT", "Scattered sites"),
      ed_type = type) %>%
    rowwise() %>%
    mutate(
      ci95_lb = case_when(
        year %in% c(2012, 2016) ~ poisson.test(ed_count)$conf.int[1] / (pt / 366) * 1000,
        TRUE ~ poisson.test(ed_count)$conf.int[1] / (pt / 365) * 1000
      ),
      ci95_ub = case_when(
        year %in% c(2012, 2016) ~ poisson.test(ed_count)$conf.int[2] / (pt / 366) * 1000,
        TRUE ~ poisson.test(ed_count)$conf.int[2] / (pt / 365) * 1000
      )
    )
  
  return(output)
}

yt_ed_sum_all <- yt_ed_sum_f(ed_type = "all")
yt_ed_sum_avoid <- yt_ed_sum_f(ed_type = "avoid")
yt_ed_sum_unavoid <- yt_ed_sum_f(ed_type = "unavoid")


yt_ed_sum <- bind_rows(yt_ed_sum_all, yt_ed_sum_avoid, yt_ed_sum_unavoid) %>%
  select(yt, year, ed_type, ed_count, pt, rate, ci95_lb, ci95_ub)


# YT vs SS, only all ED and no CI (used in journal article)
ggplot(data = yt_ed_sum_all, aes(x = year)) +
  geom_line(aes(y = rate, linetype = yt), size = 1.3) +
  # ggtitle("Emergency department visit rates (unadjusted)") +
  xlab("Year") +
  ylab("Rate (per 1,000 person-years)") +
  geom_label_repel(aes(y = rate, label=ifelse(year %in% c(2012, 2017), round(rate, 0), ''))) +
  # expand_limits(y=0) +
  theme(plot.title = element_text(size = 20),
        axis.text = element_text(size = 12),
        axis.title = element_text(size = 12),
        # axis.line = element_line(color = "black"),
        legend.title = element_blank(),
        legend.text = element_text(size = 12),
        legend.position = "bottom",
        panel.background = element_blank(),
        panel.grid.major = element_line(color = "grey40"),
        panel.grid.major.x = element_blank(),
        strip.text.y = element_text(size = 11)
  )


# YT vs SS, broken out by ED type
ed_plot <- ggplot(data = yt_ed_sum, aes(x = year)) +
  geom_line(aes(y = rate, color = yt), size = 1.3) +
  scale_color_manual(name = "YT residency", 
                     values = c("YT" = "blue2",
                                "Scattered sites" = "orangered3")) +
  geom_ribbon(aes(ymin = ci95_lb, ymax = ci95_ub, fill = yt),
              alpha = 0.4) + 
  scale_fill_manual(name = "YT residency",
                    values = c("YT" = "cyan2",
                               "Scattered sites" = "orange")) +
  ggtitle("Emergency department visit rates (unadjusted)") +
  xlab("Year") +
  ylab("Rate (per 1,000 person-years)") +
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
ed_plot + facet_grid(ed_type ~ .)
ed_plot + facet_grid(ed_type ~ ., scales = "free_y")



# ED visit types, broken out by YT/SS
ed_plot <- ggplot(data = yt_ed_sum, aes(x = year)) +
  geom_line(aes(y = rate, color = ed_type), size = 1.3) +
  scale_color_manual(name = "ED visit type", 
                     values = c("All ED visits" = "blue2",
                                "Potentially avoidable ED visits" = "orangered3",
                                "Unavoidable ED visits" = "grey23")) +
  geom_ribbon(aes(ymin = ci95_lb, ymax = ci95_ub, fill = ed_type),
              alpha = 0.4) + 
  scale_fill_manual(name = "ED visit type",
                    values = c("All ED visits" = "cyan2",
                               "Potentially avoidable ED visits" = "orange",
                               "Unavoidable ED visits" = "grey48")) +
  ggtitle("Emergency department visit rates (unadjusted)") +
  xlab("Year") +
  ylab("Rate (per 1,000 person-years)") +
  theme(axis.text = element_text(size = 14),
        axis.title = element_text(size = 16),
        legend.title = element_blank(),
        legend.text = element_text(size = 12),
        legend.position = "bottom",
        panel.background = element_rect(fill = "grey95"))
ed_plot + facet_grid(yt ~ .)



#### CAUSES OF ED VISITS ####

### Pull in top causes of ED visits by year and group
yt_causes_prim <- bind_rows(lapply(seq(2012, 2017), function(x) {
  from <- paste0(x, "-01-01")
  to <- paste0(x, "-12-31")
  
  ed_all <- top_causes_f(cohort = filter(yt_ss_ed, yt == 1), cohort_id = mid,
                         server = db.claims51,
                         from_date = from, to_date = to,
                         top = 20,
                         primary_dx = T, catch_all = F,
                         ed_all = T,
                         inpatient = F)
  ed_all <- ed_all %>%
    mutate(year = x,
           yt = "Yesler Terrace",
           ed_type = "All ED visits",
           dx_level = "Primary diagnosis only") %>%
    select(yt, year, ed_type, dx_level, ccs_final_description, ccs_final_plain_lang, claim_cnt)
  
  ed_avoid <- top_causes_f(cohort = filter(yt_ss_ed, yt == 1), cohort_id = mid,
                           server = db.claims51,
                           from_date = from, to_date = to,
                           top = 20,
                           primary_dx = T, catch_all = F,
                           ed_all = F, ed_avoid_ny = T, ed_avoid_ca = F,
                           inpatient = F)
  ed_avoid <- ed_avoid %>%
    mutate(year = x,
           yt = "Yesler Terrace",
           ed_type = "Potentially avoidable ED visits",
           dx_level = "Primary diagnosis only") %>%
    select(yt, year, ed_type, dx_level, ccs_final_description, ccs_final_plain_lang, claim_cnt)
  
  output <- bind_rows(ed_all, ed_avoid)
  return(output)
}))


yt_causes_all <- bind_rows(lapply(seq(2012, 2017), function(x) {
  from <- paste0(x, "-01-01")
  to <- paste0(x, "-12-31")
  
  ed_all <- top_causes_f(cohort = filter(yt_ss_ed, yt == 1), cohort_id = mid,
                         server = db.claims51,
                         from_date = from, to_date = to,
                         top = 20,
                         primary_dx = F, catch_all = F,
                         ed_all = T,
                         inpatient = F)
  ed_all <- ed_all %>%
    mutate(year = x,
           yt = "Yesler Terrace",
           ed_type = "All ED visits",
           dx_level = "All diagnosis fields") %>%
    select(yt, year, ed_type, dx_level, ccs_final_description, ccs_final_plain_lang, claim_cnt)
  
  ed_avoid <- top_causes_f(cohort = filter(yt_ss_ed, yt == 1), cohort_id = mid,
                         server = db.claims51,
                         from_date = from, to_date = to,
                         top = 20,
                         primary_dx = F, catch_all = F,
                         ed_all = F, ed_avoid_ny = T, ed_avoid_ca = F,
                         inpatient = F)
  ed_avoid <- ed_avoid %>%
    mutate(year = x,
           yt = "Yesler Terrace",
           ed_type = "Potentially avoidable ED visits",
           dx_level = "All diagnosis fields") %>%
    select(yt, year, ed_type, dx_level, ccs_final_description, ccs_final_plain_lang, claim_cnt)
  
  output <- bind_rows(ed_all, ed_avoid)
  return(output)
}))



ss_causes_prim <- bind_rows(lapply(seq(2012, 2017), function(x) {
  from <- paste0(x, "-01-01")
  to <- paste0(x, "-12-31")
  
  ed_all <- top_causes_f(cohort = filter(yt_ss_ed, yt == 0), cohort_id = mid,
                         server = db.claims51,
                         from_date = from, to_date = to,
                         top = 20,
                         primary_dx = T, catch_all = F,
                         ed_all = T,
                         inpatient = F)
  ed_all <- ed_all %>%
    mutate(year = x,
           yt = "Scattered Sites",
           ed_type = "All ED visits",
           dx_level = "Primary diagnosis only") %>%
    select(yt, year, ed_type, dx_level, ccs_final_description, ccs_final_plain_lang, claim_cnt)
  
  ed_avoid <- top_causes_f(cohort = filter(yt_ss_ed, yt == 0), cohort_id = mid,
                           server = db.claims51,
                           from_date = from, to_date = to,
                           top = 20,
                           primary_dx = T, catch_all = F,
                           ed_all = F, ed_avoid_ny = T, ed_avoid_ca = F,
                           inpatient = F)
  ed_avoid <- ed_avoid %>%
    mutate(year = x,
           yt = "Scattered Sites",
           ed_type = "Potentially avoidable ED visits",
           dx_level = "Primary diagnosis only") %>%
    select(yt, year, ed_type, dx_level, ccs_final_description, ccs_final_plain_lang, claim_cnt)
  
  output <- bind_rows(ed_all, ed_avoid)
  return(output)
}))


ss_causes_all <- bind_rows(lapply(seq(2012, 2017), function(x) {
  from <- paste0(x, "-01-01")
  to <- paste0(x, "-12-31")
  
  ed_all <- top_causes_f(cohort = filter(yt_ss_ed, yt == 0), cohort_id = mid,
                         server = db.claims51,
                         from_date = from, to_date = to,
                         top = 20,
                         primary_dx = F, catch_all = F,
                         ed_all = T,
                         inpatient = F)
  ed_all <- ed_all %>%
    mutate(year = x,
           yt = "Scattered Sites",
           ed_type = "All ED visits",
           dx_level = "All diagnosis fields") %>%
    select(yt, year, ed_type, dx_level, ccs_final_description, ccs_final_plain_lang, claim_cnt)
  
  ed_avoid <- top_causes_f(cohort = filter(yt_ss_ed, yt == 0), cohort_id = mid,
                           server = db.claims51,
                           from_date = from, to_date = to,
                           top = 20,
                           primary_dx = F, catch_all = F,
                           ed_all = F, ed_avoid_ny = T, ed_avoid_ca = F,
                           inpatient = F)
  ed_avoid <- ed_avoid %>%
    mutate(year = x,
           yt = "Scattered Sites",
           ed_type = "Potentially avoidable ED visits",
           dx_level = "All diagnosis fields") %>%
    select(yt, year, ed_type, dx_level, ccs_final_description, ccs_final_plain_lang, claim_cnt)
  
  output <- bind_rows(ed_all, ed_avoid)
  return(output)
}))



### Combine, add suppression, and export for Tableau
ed_causes <- bind_rows(yt_causes_prim, yt_causes_all, ss_causes_prim, ss_causes_all) %>%
  arrange(year, yt, ed_type, dx_level, -claim_cnt, ccs_final_plain_lang) %>%
  group_by(year, yt, ed_type, dx_level) %>%
  mutate(
    # Add rank for each category
    rank = min_rank(-claim_cnt),
    # Add field to indicate when the count is suppressed
    claim_cnt_supp = ifelse(claim_cnt < 5, "<5", as.character(claim_cnt)),
    # Suppress small counts
    claim_cnt = ifelse(claim_cnt < 5, NA, claim_cnt),
    # Find the rank for the first suppressed value
    rank_max = max(rank[!is.na(claim_cnt)], na.rm = T) + 1,
    # Overwrite the rank and add a flag
    rank = ifelse(is.na(claim_cnt), rank_max, rank),
    rank_flag = ifelse(is.na(claim_cnt), "+", NA_character_)
    ) %>%
  ungroup() %>%
  select(-rank_max)


write.xlsx(ed_causes, file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/Summaries/YT/yt_ed_causes.xlsx")


#### MODELS - ALL ED VISITS ####
#### Run simple GEE model ####
# Focus is on YT/SS
m_gee_crude <- geeglm(ed_count ~ yt + offset(log(pt)),
                      id = pid2, corstr = "independence",
                      family = "poisson", data = yt_ed_all_yr)
m_gee_crude <- geeglm(ed_count ~ yt*year_0 + offset(log(pt)),
                      id = pid2, corstr = "independence",
                      family = "poisson", data = yt_ed_all_yr)

# Focus is on time spent at YT
m_gee_crude <- geeglm(ed_count ~ yt_cumsum + offset(log(pt)),
                      id = pid2, corstr = "independence",
                      family = "poisson", data = yt_ed_all_yr)
m_gee_crude <- geeglm(ed_count ~ yt_cumsum*year_0 + offset(log(pt)),
                      id = pid2, corstr = "independence",
                      family = "poisson", data = yt_ed_all_yr)

summary(m_gee_crude)
exp(cbind(Estimate = coef(m_gee_crude), confint_tidy(m_gee_crude)))

# Look at predicted estimates and plot
m_gee_crude_pred <- yt_ed_sum %>% 
  mutate(
    yt = as.numeric(ifelse(yt == "YT", 1, 0)),
    yt_group = ifelse(yt == 1, "YT - predicted", "Scattered sites - predicted"),
    crude_pred = as.numeric(predict(m_crude, newdata = pred_data, type = "response")),
    crude_pred_rate = case_when(
      year %in% c(2012, 2016) ~ crude_pred/ (pt / 366) * 1000,
      TRUE ~ crude_pred/ (pt / 365) * 1000
    ))


#### Run adjusted GEE model ####
# Focus is on YT/SS after accounting for calendar year
m_gee_adj <- geeglm(ed_count ~ yt*year_0 + ethn_c + gender_c + 
                      age12 + length12 + inc_min +offset(log(pt)),
                      id = pid2, corstr = "independence",
                      family = "poisson", data = yt_ed_all_yr)

# Focus is on time spent at YT after accounting for calendar year
m_gee_adj <- geeglm(ed_count ~ yt_cumsum*year_0 + ethn_c + gender_c + 
                      age12 + length12 + inc_min + offset(log(pt)),
                    id = pid2, corstr = "independence",
                    family = "poisson", data = yt_ed_all_yr)
summary(m_gee_adj)
exp(cbind(Estimate = coef(m_gee_adj), confint_tidy(m_gee_adj)))


mz_gee_adj <- zelig(ed_count ~ yt*year_0 + ethn_c + gender_c + 
                      age12 + length12 + inc_min + offset(pt_log),
                    data = yt_ed, id = "pid2", corstr = "independence",
                    model = "poisson.gee")
summary(mz_gee_adj)
exp(mz_gee_adj$get_coef()[[1]])

mz_gee_adj_x <- setx(mz_gee_adj)

summary(sim(mz_gee_adj, x = setx(mz_gee_adj)))


# Run adjusted GEE model for original vs new YT
m_gee_adj_orig <- geeglm(ed_count ~ yt_orig*as.numeric(year-2012) + ethn_c + gender_c + 
                           age12 + length12 + inc_min + offset(log(pt)),
                         id = pid2, corstr = "independence",
                         family = "poisson", data = yt_orig_ed)
summary(m_gee_adj_orig)
exp(cbind(Estimate = coef(m_gee_adj_orig), confint_tidy(m_gee_adj_orig)))

zelig(ed_count ~ yt*as.numeric(year-2012), # + offset(log(pt)),
      data = yt_ed, id = "pid2", corstr = "independence",
      model = "poisson.gee")

# Look at predicted estimates and plot
m_gee_adj_orig_pred <- yt_ed_sum %>% 
  mutate(
    yt = as.numeric(ifelse(yt == "YT", 1, 0)),
    yt_group = ifelse(yt == 1, "YT - predicted", "Scattered sites - predicted"),
    crude_pred = as.numeric(predict(m_crude, newdata = pred_data, type = "response")),
    crude_pred_rate = case_when(
      year %in% c(2012, 2016) ~ crude_pred/ (pt / 366) * 1000,
      TRUE ~ crude_pred/ (pt / 365) * 1000
    ))

m_gee_adj_orig_pred <- yt_ss_ed_join %>%
  filter(yt == 1) %>%
  mutate(adj_pred = predict(m_gee_adj_orig, newdata = yt_ss_ed_join[yt_ss_ed_join$yt == 1, ], 
                            type = "response")) %>%
  filter(year < 2018) %>%
  group_by(yt_orig, year) %>%
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
    yt_orig_group = ifelse(yt_orig == 1, "YT original - adjusted", "New to YT - adjusted")
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


#### Run simple negative binomial model ####
# Focus is on YT/SS
m_nb_crude <- MASS::glm.nb(ed_count ~ yt + offset(log(pt)), data = yt_ed_all_yr)
m_nb_crude <- MASS::glm.nb(ed_count ~ yt*year_0 + offset(log(pt)), 
                      data = yt_ed_all_yr)

# Focus is on time spent at YT
m_nb_crude <- MASS::glm.nb(ed_count ~ yt_cumsum + offset(log(pt)), data = yt_ed_all_yr)
m_nb_crude <- MASS::glm.nb(ed_count ~ yt_cumsum*year_0 + offset(log(pt)), 
                           data = yt_ed_all_yr)

summary(m_nb_crude)
exp(cbind(Estimate = coef(m_nb_crude), confint(m_nb_crude)))


# Look at predicted estimates and plot
m_nb_crude_pred <- yt_ed_sum %>% 
  mutate(
    yt = as.numeric(ifelse(yt == "YT", 1, 0)),
    yt_group = ifelse(yt == 1, "YT - predicted", "Scattered sites - predicted"),
    crude_pred = as.numeric(predict(m_nb_crude, newdata = yt_ss_ed_join, type = "response")),
    crude_pred_rate = case_when(
      year %in% c(2012, 2016) ~ crude_pred/ (pt / 366) * 1000,
      TRUE ~ crude_pred/ (pt / 365) * 1000
      ))


# set up for plotting
m_nb_crude_plot_data <- m_nb_crude_pred %>%
  select(yt, yt_group, year, rate, ci95_lb, ci95_ub, crude_pred_rate) %>%
  melt(., id.vars = c("yt", "yt_group", "year", "ci95_lb", "ci95_ub"), 
       variable.name = "rate_type", value.name = "rate") %>%
  mutate(rate_type = if_else(rate_type == "rate", "Crude rate", "Predicted rate"),
         yt = if_else(yt == 1, "YT", "Scattered sites"))


ggplot(data = m_nb_crude_plot_data, aes(x = year)) +
  geom_ribbon(aes(ymin = ci95_lb, ymax = ci95_ub, group = yt, fill = yt),
              data = m_nb_crude_plot_data[m_nb_crude_plot_data$rate_type == "Crude rate", ],
              alpha = 0.4,
              show.legend = T) +
  scale_fill_manual(labels = c("Scattered sites", "YT"),
                    values = c("orange", "cyan2"), name="95% CI") +
  geom_line(aes(y = rate, color = yt, group = interaction(yt, rate_type),
                linetype = yt),
            size = 1.3,
            data = m_nb_crude_plot_data[m_nb_crude_plot_data$rate_type == "Crude rate", ]) +
  geom_line(aes(y = rate, color = yt_group, group = interaction(yt_group, rate_type),
                linetype = yt_group),
            size = 0.8,
            data = m_nb_crude_plot_data[m_nb_crude_plot_data$rate_type == "Predicted rate", ]) +
  scale_linetype_manual(name = "YT residency", 
                        values = rep(c("solid", "dashed"), times = 2)) +
  scale_color_manual(name = "YT residency", 
                     values = c(rep(c("orangered3", "blue2"), each = 2))) +
  guides(
    linetype = guide_legend(override.aes = list(size = 0.8)),
    color = guide_legend(override.aes = list(fill=NA))
    ) +
  ggtitle("Emergency department visit rates (unadjusted)") +
  xlab("Year") +
  ylab("Rate (per 1,000 person-years)") +
  theme(title = element_text(size = 18),
        axis.text = element_text(size = 14),
        axis.title = element_text(size = 16),
        legend.title = element_text(size = 14),
        legend.text = element_text(size = 12),
        panel.background = element_rect(fill = "grey95"))


#### Run adjusted negative binomial model ####
# Focus is on YT/SS
m_nb_adj <- MASS::glm.nb(ed_count ~ yt*year_0 + ethn_c + gender_c + 
                        age12 + length12 + inc_min + offset(log(pt)), 
             data = yt_ed_all_yr)
# Focus is on time spent at YT
m_nb_adj <- MASS::glm.nb(ed_count ~ yt_cumsum*year_0 + ethn_c + gender_c + 
                           age12 + length12 + inc_min + offset(log(pt)), 
                         data = yt_ed_all_yr)
summary(m_nb_adj)
exp(cbind(Estimate = coef(m_nb_adj), confint(m_nb_adj)))

m_nb_adj_pred <- predict_f(model = m_nb_adj, group_var = yt,
                           group1 = "YT - adjusted",
                           group2 = "Scattered sites - adjusted")

# Look at predicted estimates and plot
m_nb_adj_pred_data <- m_nb_adj_pred %>%
  select(yt, yt_group, year, rate, ci95_lb, ci95_ub, 
         adj_pred_rate, ci95_lb_adj, ci95_ub_adj) %>%
  melt(., id.vars = c("yt", "yt_group", "year", "ci95_lb", "ci95_ub",
                      "ci95_lb_adj", "ci95_ub_adj"), 
       variable.name = "rate_type", value.name = "rate") %>%
  mutate(rate_type = if_else(rate_type == "rate", "Crude rate", "Predicted rate"),
         yt = if_else(yt == 1, "YT - crude", "Scattered sites - crude"))


plot_f(df = m_nb_adj_pred_data, labels = c("Scattered sites", "YT"),
       title = "Emergency department visit rates (adjusted)", groups = yt)


#### Run adjusted negative binomial model for YT original vs. new to YT ####
m_nb_adj_orig <- MASS::glm.nb(ed_count ~ yt_orig*as.numeric(year-2012) + ethn_c + gender_c + 
                           age12 + length12 + inc_min + offset(log(pt)), 
                         data = yt_orig_ed)
summary(m_nb_adj_orig)
exp(cbind(Estimate = coef(m_nb_adj_orig), confint(m_nb_adj_orig)))

m_nb_adj_orig_pred <- predict_f(model = m_nb_adj_orig, group_var = yt_orig,
                                group1 = "YT original - adjusted",
                                group2 = "New to YT - adjusted",
                                yt_only = T)

# Look at predicted estimates and plot
m_nb_adj_orig_pred_data <- m_nb_adj_orig_pred %>%
  select(yt_orig, yt_group, year, rate, ci95_lb, ci95_ub, 
         adj_pred_rate, ci95_lb_adj, ci95_ub_adj) %>%
  melt(., id.vars = c("yt_orig", "yt_group", "year", "ci95_lb", "ci95_ub",
                      "ci95_lb_adj", "ci95_ub_adj"), 
       variable.name = "rate_type", value.name = "rate") %>%
  mutate(rate_type = if_else(rate_type == "rate", "Crude rate", "Predicted rate"),
         yt_orig = if_else(yt_orig == 1, "YT original - crude", "New to YT - crude"))


plot_f(df = m_nb_adj_orig_pred_data, labels = c("New to YT", "YT original"),
       title = "Emergency department visit rates (adjusted)", groups = yt_orig)


#### Run zero inflated model ####
library(pscl)
library(snow)
library(boot)


m_zero <- zeroinfl(ed_count ~ yt*year_0 + ethn_c + gender_c + age12 + 
                     length12 + inc + offset(log(pt)),
                   data = yt_ed, dist = "negbin")
m_zero <- zeroinfl(ed_count ~ yt*year_0 + ethn_c + gender_c +  
                     length12 + offset(log(pt)),
                   data = yt_ed_all_yr, dist = "negbin")
summary(m_zero)

m_zero_pred <- yt_ss_ed_join %>%
  mutate(adj_pred = predict(m_zero, newdata = yt_ss_ed_join, type = "response")) %>%
  filter(year < 2018) %>%
  group_by(yt, year) %>%
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
    yt_group = ifelse(yt == 1, "YT - adjusted", "Scattered sites - adjusted")
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


# Look at predicted estimates and plot
m_zero_pred_data <- m_zero_pred %>%
  select(yt, yt_group, year, rate, ci95_lb, ci95_ub, 
         adj_pred_rate, ci95_lb_adj, ci95_ub_adj) %>%
  melt(., id.vars = c("yt", "yt_group", "year", "ci95_lb", "ci95_ub",
                      "ci95_lb_adj", "ci95_ub_adj"), 
       variable.name = "rate_type", value.name = "rate") %>%
  mutate(rate_type = if_else(rate_type == "rate", "Crude rate", "Predicted rate"),
         yt = if_else(yt == 1, "YT", "Scattered sites"))


ggplot(data = m_zero_pred_data, aes(x = year)) +
  geom_ribbon(aes(ymin = ci95_lb_adj, ymax = ci95_ub_adj, group = yt, fill = yt),
              data = m_zero_pred_data[m_zero_pred_data$rate_type == "Crude rate", ],
              alpha = 0.4,
              show.legend = T) +
  scale_fill_manual(labels = c("Scattered sites", "YT"),
                    values = c("orange", "cyan2"), name="95% CI") +
  geom_line(aes(y = rate, color = yt, group = interaction(yt, rate_type),
                linetype = yt),
            size = 1.3,
            data = m_zero_pred_data[m_zero_pred_data$rate_type == "Crude rate", ]) +
  geom_line(aes(y = rate, color = yt_group, group = interaction(yt_group, rate_type),
                linetype = yt_group),
            size = 0.8,
            data = m_zero_pred_data[m_zero_pred_data$rate_type == "Predicted rate", ]) +
  scale_linetype_manual(name = "YT residency", 
                        values = rep(c("solid", "dashed"), times = 2)) +
  scale_color_manual(name = "YT residency", 
                     values = c(rep(c("orangered3", "blue2"), each = 2))) +
  guides(
    linetype = guide_legend(override.aes = list(size = 0.8)),
    color = guide_legend(override.aes = list(fill=NA))
  ) +
  ggtitle("Emergency department visit rates (adjusted zero-inflated model)") +
  xlab("Year") +
  ylab("Rate (per 1,000 person-years)") +
  theme(title = element_text(size = 18),
        axis.text = element_text(size = 14),
        axis.title = element_text(size = 16),
        legend.title = element_text(size = 14),
        legend.text = element_text(size = 12),
        panel.background = element_rect(fill = "grey95"))


### Bootstrap CIs
start <- dput(coef(m_zero, "count"))
zero <- dput(coef(m_zero, "zero"))

boot_zero_f <- function(data, i) {
  m <- pscl::zeroinfl(ed_count ~ yt*year_0 + ethn_c + gender_c + age12 + 
                        length12 + inc_min + offset(log(pt)),
                      data = data[i, ],
                      start = list(count = start,
                             zero = zero))
  as.vector(t(do.call(rbind, coef(summary(m)))[, 1:2]))
}

set.seed(10)
m_zero_boot <- boot(yt_ed_all_yr, boot_zero_f, R = 100, parallel = "snow", ncpus = 4)




#### Run marginal structural model ####
# Set up IPT weights and join back
w1 <- ipwtm(exposure = yt, 
            family = "binomial",
            link = "logit",
            numerator = ~ ethn_c + gender_c + age12 + length12,
            denominator = ~ ethn_c + gender_c + age12 + length12 + inc + year,
            id = pid2,
            timevar = timevar,
            type = "all",
            trunc = 0.01,
            data = yt_ed)

# Check mean of weights near 1
pastecs::stat.desc(w1$ipw.weights)
pastecs::stat.desc(w1$weights.trunc)

# Set up weights for left- and right-censoring
w_lc <- ipwtm(exposure = cens_l, 
              family = "binomial",
              link = "logit",
              numerator = ~ ethn_c + gender_c + age12 + length12,
              denominator = ~ ethn_c + gender_c + age12 + length12 + inc + year,
              id = pid2,
              timevar = timevar,
              type = "all",
              trunc = 0.01,
              data = yt_ed)

w_rc <- ipwtm(exposure = cens_r, 
              family = "binomial",
              link = "logit",
              numerator = ~ ethn_c + gender_c + age12 + length12,
              denominator = ~ ethn_c + gender_c + age12 + length12 + inc + year,
              id = pid2,
              timevar = timevar,
              type = "all",
              trunc = 0.01,
              data = yt_ed)


# Check mean of weights near 1
pastecs::stat.desc(w_lc$ipw.weights)
pastecs::stat.desc(w_lc$weights.trunc)
pastecs::stat.desc(w_rc$ipw.weights)
pastecs::stat.desc(w_rc$weights.trunc)

# Combine weights
yt_ed$w1 <- w1$weights.trunc
yt_ed$w_lc <- w_lc$weights.trunc
yt_ed$w_rc <- w_rc$weights.trunc

yt_ed <- yt_ed %>%
  mutate(iptw = case_when(
    !is.na(w1) & !is.na(w_lc) & !is.na(w_rc) ~ w1 * w_lc * w_rc,
    !is.na(w1) & !is.na(w_lc) ~ w1 * w_lc,
    !is.na(w1) & !is.na(w_rc) ~ w1 * w_rc
  ))

pastecs::stat.desc(yt_ed$iptw)


# Run negative binomial MSM
m_msm_nb <- MASS::glm.nb(ed_count ~ yt*year_0 + ethn_c + gender_c + age12 + 
                           length12 + inc_min + offset(log(pt)), 
                         data = yt_ed, weights = iptw)
summary(m_msm_nb)
exp(cbind(Estimate = coef(m_msm_nb), confint(m_msm_nb)))


# Run GEE Poisson MSM
m_msm_gee <- geeglm(ed_count ~ yt*year_0 + ethn_c + gender_c + age12 + 
                      length12 + inc_min + offset(log(pt)),
                    id = pid2, corstr = "independence",
                    family = "poisson", data = yt_ed,
                    weights = iptw)
summary(m_msm_gee)
exp(cbind(Estimate = coef(m_msm_gee), confint_tidy(m_msm_gee)))

# Run zero inflated Poisson
# Currently doesn't work
m_msm_znb <- zeroinfl(ed_count ~ yt*year_0 + ethn_c + gender_c + age12 + 
                        length12 + inc_min + offset(log(pt)),
                      data = yt_ed, dist = "negbin",
                      weights = iptw)
summary(m_zero)
  
  
  
#### Run history-adjusted marginal structural model ####
# Set up IPT weights and join back

w1 <- ipwtm(exposure = yt, 
            family = "binomial",
            link = "logit",
            numerator = ~ ethn_c + gender_c + age12 + length12,
            denominator = ~ ethn_c + gender_c + age12 + length12 + inc + year,
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
              denominator = ~ ethn_c + gender_c + age12 + length12 + inc + year,
              id = pid2,
              timevar = timevar,
              type = "all",
              trunc = 0.01,
              data = yt_ed)

w_rc <- ipwtm(exposure = cens_r, 
              family = "binomial",
              link = "logit",
              numerator = ~ ethn_c + gender_c + age12 + length12,
              denominator = ~ ethn_c + gender_c + age12 + length12 + inc + year,
              id = pid2,
              timevar = timevar,
              type = "all",
              trunc = 0.01,
              data = yt_ed)




#### Run structured nested mean model ####


#### MODELS - STRATIFIED ED OVERALL ####
### Look at larger race/ethnicity groups
# Run GEE Poisson MSM
m_msm_gee_asian <- geeglm(ed_count ~ yt*year_0 + gender_c + age12 + 
                      length12 + inc_min + offset(log(pt)),
                    id = pid2, corstr = "independence",
                    family = "poisson", data = yt_ed[yt_ed$ethn_c == "Asian", ],
                    weights = iptw)
summary(m_msm_gee_asian)
exp(cbind(Estimate = coef(m_msm_gee_asian), confint_tidy(m_msm_gee_asian)))


m_msm_gee_black <- geeglm(ed_count ~ yt*year_0 + gender_c + age12 + 
                            length12 + inc_min + offset(log(pt)),
                          id = pid2, corstr = "independence",
                          family = "poisson", data = yt_ed[yt_ed$ethn_c == "Black", ],
                          weights = iptw)
summary(m_msm_gee_black)
exp(cbind(Estimate = coef(m_msm_gee_black), confint_tidy(m_msm_gee_black)))

# See if there's an interaction by race and YT/SS
m_msm_gee_race <- geeglm(ed_count ~ yt*year_0 + yt*ethn_c + gender_c + age12 + 
                      length12 + inc_min + offset(log(pt)),
                    id = pid2, corstr = "independence",
                    family = "poisson", data = yt_ed,
                    weights = iptw)
summary(m_msm_gee_race)
exp(cbind(Estimate = coef(m_msm_gee_race), confint_tidy(m_msm_gee_race)))



#### MODELS - AVOIDABLE ED VISITS ####
### Run simple GEE model
# Focus is on YT/SS
m_gee_crude_avoid <- geeglm(ed_avoid_count ~ yt + offset(log(pt)),
                      id = pid2, corstr = "independence",
                      family = "poisson", data = yt_ed_all_yr)
m_gee_crude_avoid <- geeglm(ed_avoid_count ~ yt*year_0 + offset(log(pt)),
                      id = pid2, corstr = "independence",
                      family = "poisson", data = yt_ed_all_yr)

summary(m_gee_crude_avoid)
exp(cbind(Estimate = coef(m_gee_crude_avoid), confint_tidy(m_gee_crude_avoid)))


### Run marginal structural model
# Set up IPT weights above
m_msm_gee_avoid <- geeglm(ed_avoid_count ~ yt*year_0 + ethn_c + gender_c + age12 + 
                      length12 + inc_min + offset(log(pt)),
                    id = pid2, corstr = "independence",
                    family = "poisson", data = yt_ed,
                    weights = iptw)
summary(m_msm_gee_avoid)
exp(cbind(Estimate = coef(m_msm_gee_avoid), confint_tidy(m_msm_gee_avoid)))



#### MODELS - UNAVOIDABLE ED VISITS ####
### Run simple GEE model
# Focus is on YT/SS
m_gee_crude_unavoid <- geeglm(ed_unavoid_count ~ yt + offset(log(pt)),
                            id = pid2, corstr = "independence",
                            family = "poisson", data = yt_ed_all_yr)
m_gee_crude_unavoid <- geeglm(ed_unavoid_count ~ yt*year_0 + offset(log(pt)),
                            id = pid2, corstr = "independence",
                            family = "poisson", data = yt_ed_all_yr)

summary(m_gee_crude_unavoid)
exp(cbind(Estimate = coef(m_gee_crude_unavoid), confint_tidy(m_gee_crude_unavoid)))


### Run marginal structural model
# Set up IPT weights above
m_msm_gee_unavoid <- geeglm(ed_unavoid_count ~ yt*year_0 + ethn_c + gender_c + age12 + 
                            length12 + inc_min + offset(log(pt)),
                          id = pid2, corstr = "independence",
                          family = "poisson", data = yt_ed,
                          weights = iptw)
summary(m_msm_gee_unavoid)
exp(cbind(Estimate = coef(m_msm_gee_unavoid), confint_tidy(m_msm_gee_unavoid)))




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
                 denominator = ~ ethn_c + gender_c + age12 + length12 + inc + year,
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
                       denominator = ~ ethn_c + gender_c + age12 + length12 + inc + year,
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
                       denominator = ~ ethn_c + gender_c + age12 + length12 + inc + year,
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





