# plots for ppt to PHAs
# precious esie
# 11-20-2023
# dependencies: pha-excess-deaths.Rmd 

# figure 1: annual rate of deaths in 2017-2021 ----
# annual observed numbers
fig1a <- preds %>% 
  group_by(agency, year) %>%
  summarise(
    pop = sum(pop),
    deaths = sum(deaths),
    pred = sum(predicted),
    excess = sum(excess)
  ) %>% 
  ungroup() %>%
  gather("rate", "value", -c(agency,pop,year))
## plot
fig1a %>% filter(rate == "deaths", agency == "SHA") %>%
  ggplot(data = ., aes(x = year, y = value)) +
  geom_col() +
  #facet_free( ~ agency) +
  geom_hline(yintercept = 0, size = 1) + 
  theme_bw() +
  labs(x = "", y = "Deaths", title = "Annual Number of Deaths, 2017-2021") +
  theme(text=element_text(size=22))

# annual observed rates
fig1b <- preds %>% 
  group_by(agency, year) %>%
  summarise(
    avg_pop = sum(pop)/12,
    avg_deaths_rate = sum(rate)/12,
    avg_pred_rate = sum(pred_rate)/12,
    avg_excess_rate = sum(excess_rate)/12
  ) %>% 
  ungroup() %>%
  gather("rate", "value", -c(agency,avg_pop,year))
## plot
fig1b %>% filter(rate == "avg_deaths_rate", agency == "SHA") %>%
  ggplot(data = ., aes(x = year, y = value)) +
  geom_col() +
  #facet_free( ~ agency) +
  geom_hline(yintercept = 0, size = 1) + 
  theme_bw() +
  #ylim(-2,16) +
  labs(x = "", y = "Deaths per 10,000", title = "Average Monthly Death Rate, 2017-2021") +
  theme(text=element_text(size=22))

# annual rate of deaths in 2017-2021 by age ----
fig1c <- preds_by_age %>% 
  group_by(agency, year, age3) %>%
  summarise(
    avg_pop = sum(pop)/12,
    avg_deaths_rate = sum(rate)/12,
    avg_pred_rate = sum(pred_rate)/12,
    avg_excess_rate = sum(excess_rate)/12
    #avg_95excess_rate = sum(excess95_rate)/12
  ) %>% 
  ungroup() %>%
  gather("rate", "value", -c(agency,avg_pop,year,age3))
## plot
fig1c %>% filter(rate == "avg_deaths_rate") %>%
  ggplot(data = ., aes(x = year, y = value)) +
  geom_col() +
  facet_free(age3 ~ agency) +
  geom_hline(yintercept = 0, size = 1) + 
  theme_bw() +
  labs(x = "", y = "Deaths per 10,000", title = "Annual Death Rate, 2017-2021") +
  theme(text=element_text(size=22))


fig1d <- preds_by_age %>% 
  group_by(agency, age3, covid) %>%
  summarise(
    months = n(),
    avg_deaths = sum(deaths)/months) %>%
  mutate(covid = as.factor(covid))

## plot
fig1d %>% filter(agency == "SHA") %>%
  ggplot(data = ., aes(x = age3, y = avg_deaths, fill = covid)) +
  geom_bar(stat = "identity", position = "dodge", show.legend = FALSE) +
  geom_hline(yintercept = 0, size = 1) + 
  theme_bw() +
  scale_fill_manual("", values = c("gray70","gray30")) +
  labs(x = "", y = "Deaths per 10,000", title = "Average number of deaths per month") +
  theme(text=element_text(size=22))


# figure 2: rate of deaths in 2020-2021 ----
fig2 <- preds_by_age %>% 
  filter(covid == 1) %>% 
  group_by(agency, age3) %>%
  summarise(
    avg_pop = sum(pop)/22,
    avg_deaths_rate = sum(rate)/22,
    avg_pred_rate = sum(pred_rate)/22,
    avg_excess_rate = sum(excess_rate)/22,
    avg_95excess_rate = sum(excess_lwr_rate)/22,
    avg_deaths = sum(deaths)/22
  ) %>% arrange(agency, match(age3, c("<45", "45-64", "65+"))) %>%
  ungroup() %>%
  gather("rate", "value", -c(agency,age3,avg_pop))

# avg monthly death rate
fig2 %>% filter(rate == "avg_deaths_rate") %>%
ggplot(data = ., aes(x = age3, y = value)) +
  geom_col() +
  facet_free(~ agency) +
  geom_hline(yintercept = 0, size = 1) + 
  theme_bw() +
  labs(
    x = "", y = "Deaths per 10,000", 
    title = "Average Monthly Death Rate During COVID-19 Period", 
    subtitle = "March 2020 - December 2021"
  ) +
  theme(text=element_text(size=22))

# avg predicted & excess death rate
fig2 %>% 
  filter(rate %in% c("avg_excess_rate","avg_pred_rate"), agency == "SHA") %>%
  mutate(rate = factor(rate, levels = c("avg_excess_rate","avg_pred_rate"), labels = c("Excess", "Predicted"))) %>%
  ggplot(data = ., aes(x = age3, y = value, group = rate, fill = rate)) +
  geom_col() +
  geom_hline(yintercept = 0, size = 1) + 
  #facet_free(~ agency) +
  scale_fill_manual("", values = c("orange", "gray50")) +
  theme_bw() +
  labs(
    x = "", y = "Deaths per 10,000", 
    title = "Average Monthly Death Rate During COVID-19 Period", 
    subtitle = "March 2020 - December 2021"
  ) +  
  theme(text=element_text(size=22)) +
  theme(legend.position="none")

# table 1: number of deaths ----
tab1 <- preds %>% 
  filter(covid == 1) %>%
  group_by(agency) %>% 
  summarise(
    tot_observed = sum(deaths),
    tot_pred = sum(predicted),
    tot_excess = sum(excess),
    pct_excess = tot_excess/tot_observed * 100,
    tot_excess95 = sum(excess_lwr),
    pct_excess95 = tot_excess95/tot_observed * 100
  ) %>% as.data.frame()

tab2 <- preds_by_age %>% 
  filter(covid == 1) %>%
  group_by(agency, age3) %>% 
  summarise(
    tot_observed = sum(deaths),
    tot_pred = sum(predicted),
    tot_excess = sum(excess),
    pct_excess = tot_excess/tot_observed * 100,
    tot_excess95 = sum(excess_lwr),
    pct_excess95 = tot_excess95/tot_observed * 100
  ) %>% as.data.frame()


# pre-pandemic vs during pandemic
preds %>% group_by(agency, covid) %>%
  summarise(
    avg_deaths = sum(deaths) / n(),
    avg_rate = sum(rate)/ n(),
    tot_deaths = sum(deaths),
    tot_pred = sum(predicted)
  )

# figure 4: monthly excess deaths during COVID-19 pandemic ----
date_seq <- seq(from = ymd("2020-03-01"), to = ymd("2021-12-01"), by = "3 months")

# KCHA
preds_by_age %>% filter(covid == 1, agency == "KCHA") %>%
  ggplot(data = ., aes(x = date_mo_01, y = excess_rate)) +
  geom_col(size = 1, aes(fill = alarm)) +
  geom_errorbar(aes(ymax = excess_upr_rate, ymin=excess_lwr_rate), linewidth = 0.5, width = 8) +
  geom_hline(yintercept = 0, linewidth = 1) +
  scale_fill_manual(
    values=c("alarm" = "orange","none" = "gray50"), 
    labels = c("Significantly above 0","Not significantly above 0"), drop = FALSE
  ) + 
  scale_x_date(labels = date_format("%b %Y"), breaks = date_seq) + 
  labs(
    title = "Monthly excess deaths (all causes)", 
    subtitle = "King County Housing Authority",
    x = "", y = "Deaths per 10,000 residents",
    fill = ""
  ) + theme_bw() +
  theme(text=element_text(size=22)) +
  facet_grid(age3 ~ ., scale = "free") +
  theme(legend.position="bottom")

# SHA
preds_by_age_SHA %>% filter(covid == 1) %>%
  ggplot(data = ., aes(x = date_mo_01, y = excess_rate)) +
  geom_col(size = 1, aes(fill = legend)) +
  geom_errorbar(aes(ymax = excess_upr_rate, ymin=excess_lwr_rate), linewidth = 0.5, width = 8) +
  geom_hline(yintercept = 0, linewidth = 1) +
  #facet_wrap(~ year, scales = "free", nrow = 2) +
  scale_fill_manual(
    values=c("2" = "#F16913", "1" = "#FDD0A2", "0" = "gray70"), 
    labels = c("Significantly above 0", "Above 0", "Less than or equal to 0")
  ) + 
  scale_x_date(labels = date_format("%b %Y"), breaks = date_seq) + 
  labs(
    title = "Monthly excess deaths per 10,000 residents, all causes", 
    subtitle = "Seattle Housing Authority",
    x = "", y = "Excess deaths per 10,000 residents",
    fill = ""
  )  + 
  theme_bw() +
  theme(text=element_text(size=22)) +
  facet_grid(age3 ~ ., scale = "free") +
  theme(legend.position="bottom")


# figure 5: limitations ----
preds_by_age_SHA %>% 
  filter(age3 == "65+") %>% 
  ggplot(data = ., aes(x = date_mo_01, y = pred_rate)) +
  geom_vline(xintercept = ymd("2020-02-15"),  lty=2, linewidth=.5) + 
  geom_col(
    inherit.aes = FALSE,
    aes(x = date_mo_01, y = rate, fill = "Observed deaths"), alpha = 0.8
  ) +
  geom_point(size = 3, aes(color = "Predicted deaths")) +
  geom_errorbar(aes(ymin=pred_rate_lwr,ymax = pred_rate_upr), color= "black", linewidth = 0.8, width = 10) +
  labs(
    y = "Deaths per 10,000 PHA residents", 
    x = "", 
    title = "Observed and predicted (with 95% CIs) deaths among PHA residents, all causes",
    subtitle = paste0("King County Housing Authority"),
    caption = "CI = confidence interval\nModel was fit using data from Jan 2017 to Dec 2019.\nValues to the left of the dashed line are from Jan 2017 to Feb 2020, and values to the right are from Mar 2020 to Dec 2021."
  ) +
  scale_color_manual("", breaks = c("Predicted deaths"), values = c("black")) +
  scale_fill_manual("", breaks = c("Observed deaths"), values = c("orange")) +
  scale_x_date(labels = date_format("%b %Y"), breaks = "7 months") + 
  theme_bw() +
  theme(legend.spacing.y = unit(-.1, "cm")) +
  theme(legend.box="vertical") +
  theme(text=element_text(size=22)) +
  theme(plot.caption = element_text(hjust = 0)) +
  facet_wrap(~ age3, scales = "free", nrow = 3)


# observed deaths only
preds_by_age_SHA %>% 
  filter(age3 == "65+") %>% 
  ggplot(data = ., aes(x = date_mo_01, y = deaths, fill = "Observed deaths"), alpha = 0.8) +
  geom_col(show.legend = FALSE) + 
  geom_vline(xintercept = ymd("2020-02-15"),  lty=2, linewidth=.5) + 
  labs(
    y = "Number of deaths", 
    x = "", 
    title = "Observed deaths among PHA residents, all causes",
    subtitle = paste0("Seattle Housing Authority")
  ) +
  scale_fill_manual("", breaks = c("Observed deaths"), values = c("orange")) +
  scale_x_date(labels = date_format("%b %Y"), breaks = "7 months") + 
  theme_bw() +
  theme(legend.spacing.y = unit(-.1, "cm")) +
  theme(legend.box="vertical") +
  theme(text=element_text(size=22)) +
  theme(plot.caption = element_text(hjust = 0)) +
  facet_wrap(~ age3, scales = "free", nrow = 3)

# model performance
SHA_age3 <- deaths_by_month %>%
  # baseline data only
  filter(covid == 0, age3 == "65+", agency == "SHA") %>%
  glm(
    deaths ~ year_num + month_f + offset(logpop), 
    data = .,
    family= 'poisson'
  )

preds_by_age_SHA %>% 
  filter(agency == "SHA", age3 == "65+") %>%
  ggplot(aes(x = predicted, y = deaths)) +
  geom_point() + 
  labs(x = "Predicted",y = "Observed") + 
  geom_abline(intercept = 0, slope = 1, linetype = "dashed", color = "red") +
  theme_bw() +
  theme(text=element_text(size=22)) 
  
