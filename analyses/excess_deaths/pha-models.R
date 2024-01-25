# get data ----
#' model fitting performed on data where age is not distinguished
#' This approach allows for examining the relationship between features 
#' (time and agency) and deaths within each age group independently.

# data frame for denominators by month, summarized EXCLUDING AGE
pop_by_month_tot <- pop_by_month %>%
  group_by(across(c(-age3,-pop,-agency))) %>%
  summarise(pop = sum(pop))

# collapse ages
deaths_by_month_tot <- deaths_by_month %>% 
  group_by(across(-c(age3,deaths,rate,pop,logpop,agency))) %>% 
  summarise(deaths = sum(deaths, na.rm = TRUE)) %>%
  ungroup() %>%
  # add denominators
  left_join(pop_by_month_tot, by = c("month_year")) %>%
  mutate(
    # add before/during covid indicator
    covid = ifelse(month_year < "Mar 2020", 0, 1),
    # rate per 10,000
    rate = deaths/pop * 10000
  )


# model fitting ----
# linear year
m1 <- deaths_by_month_tot %>%
  # pre-covid
  filter(covid == 0) %>%
  glm(deaths ~ year_num + offset(log(pop)), data = ., family= 'poisson')

# linear year, monthly categorical
m2 <- deaths_by_month_tot %>%
  # pre-covid
  filter(covid == 0) %>%
  glm(deaths ~ year_num + month_f + offset(log(pop)), data = ., family= 'poisson')

# spline (linear year, linear month)
m3 <- deaths_by_month_tot %>%
  # pre-covid
  filter(covid == 0) %>%
  gam(deaths ~ year_num + s(month, bs = "cc", k = 12) + offset(log(pop)), data = ., family= 'poisson', method = 'ML')



# model predictions ----
model_preds <- function(model) {
  ggpredict(model, terms = list(
    year_num = c(0:4)
  )) %>% 
    as.data.frame() %>%
    left_join(
      deaths_by_month_tot,
      by = c("x" = "year_num"),
      multiple = "all"
    ) %>%
    mutate(
      diff = deaths - predicted,
      diff_lwr = deaths - conf.high,
      diff_upr = deaths - conf.low,
      pred_rate = predicted/pop * 10000,
      pred_rate_lwr = conf.low/pop * 10000,
      pred_rate_upr = conf.high/pop * 10000,
      alarm = ifelse(deaths > conf.high, "alarm", "none") %>% as.factor()
    ) %>%
    rename(year_num = x)
}

m1_out <- model_preds(m1)
m2_out <- model_preds(m2)
m3_out <- model_preds(m3)

# extract model information ----
## ANOVA/LRT
comp1 <- anova(m1,m2, test = "LRT")
comp2 <- anova(m1,m3, test = "LRT")


## insert information into table
lrt_res <- tibble(
  Model = c("Model 1", "Model 2", "Model 3"),
  Deviance = c(deviance(m1), deviance(m2), deviance(m3)),
  `Residual DF` = c(df.residual(m1), df.residual(m2), df.residual(m3)),
  `Change in DF` = c(NA, comp1$Df[2], comp2$Df[2]),
  `Change in Deviance` = c(NA, comp1$Deviance[2], comp2$Deviance[2]),
  `P-value [note]` = c(NA, comp1$`Pr(>Chi)`[2], comp2$`Pr(>Chi)`[2]),
) %>% mutate(across(!Model & !`P-value [note]`, ~round(.,2))) %>%
  mutate(`P-value [note]` = round(`P-value [note]`,3))

# predictive accuracy
fit_res <- tibble(
  Model = c("Model 1", "Model 2", "Model 3"),
  AIC = c(AIC(m1), AIC(m2), AIC(m3)),
  BIC = c(BIC(m1), BIC(m2), BIC(m3)),
  MSE = c(
    mse(actual = m1_out$deaths[m1_out$covid==0], predicted = m1_out$predicted[m1_out$covid==0]),
    mse(actual = m2_out$deaths[m2_out$covid==0], predicted = m2_out$predicted[m2_out$covid==0]),
    mse(actual = m3_out$deaths[m3_out$covid==0], predicted = m3_out$predicted[m3_out$covid==0])
  ),
  MAE = c(
    mae(actual = m1_out$deaths[m1_out$covid==0], predicted = m1_out$predicted[m1_out$covid==0]),
    mae(actual = m2_out$deaths[m2_out$covid==0], predicted = m2_out$predicted[m2_out$covid==0]),
    mae(actual = m3_out$deaths[m3_out$covid==0], predicted = m3_out$predicted[m3_out$covid==0])
  ),
  `Nagelkerke R2` = c(
    NagelkerkeR2(m1)[[2]],
    NagelkerkeR2(m2)[[2]],
    NagelkerkeR2(m3)[[2]]
  )
) %>% mutate(across(!Model, ~round(.,2)))

# print
lrt_res %>%
  kableExtra::kbl(caption = "Likelihood Ratio Test summary") %>% 
  kable_styling(bootstrap_options = c("hover"), full_width = T) %>%
  add_footnote(
    "P-values are from likelihood ratio tests using Model 1 as the reference.", 
    notation = "number"
  )
# p<0.05 => models 2 and 3 provide a better fit than model 1

# print
fit_res %>%
  kableExtra::kbl(caption = "Model fit summary") %>% 
  kable_styling(bootstrap_options = c("hover"), full_width = T, ) %>%
  add_footnote(
    "Note: Models were fit using data from Jan 2017 to Feb 2020.", notation = "none"
  )

m2_out %>% 
  ggplot(data = ., aes(x = date_mo_01, y = pred_rate)) +
  geom_vline(xintercept = ymd("2020-02-15"),  lty=2, linewidth=.5) + 
  geom_col(
    inherit.aes = FALSE,
    aes(x = date_mo_01, y = rate, fill = "Observed deaths"), alpha = 0.8
  ) +
  #geom_col(aes(fill = "Predicted deaths"), color = "black", alpha = 0.7, show.legend = FALSE) +
  geom_point(size = 3, aes(color = "Predicted deaths")) +
  geom_errorbar(aes(ymin=pred_rate_lwr,ymax = pred_rate_upr), color= "black", linewidth = 0.8, width = 10) +
  labs(
    y = "Deaths", 
    x = "", 
    title = "Observed and Predicted (with 95% CIs) Number of Deaths, all causes",
    subtitle = "King County & Seattle Housing Authorities",
    caption = "CI = confidence interval\nModel was fit using data from Jan 2017 to Dec 2019.\nValues to the left of the dashed line are from Jan 2017 to Feb 2020, and values to the right are from Mar 2020 to Dec 2021."
  ) +
  scale_color_manual("", breaks = c("Predicted deaths"), values = c("black")) +
  scale_fill_manual("", breaks = c("Observed deaths"), values = c("orange")) +
  scale_x_date(labels = date_format("%b %Y"), breaks = "4 months") + 
  theme_bw() +
  theme(legend.spacing.y = unit(-.3, "cm")) +
  theme(legend.box="vertical") +
  theme(plot.caption = element_text(hjust = 0)) 


# monthly number of deaths ----
fig1_fx <- function(x){
  
  x %>% filter(year > 2019) %>%
    ggplot(data = ., aes(x = date_mo_01, y = pred_rate)) +
    geom_col(
      inherit.aes = FALSE,
      aes(x = date_mo_01, y = rate, fill = alarm), alpha = 0.8
    ) +
    geom_point(size = 3, aes(color = "Predicted deaths")) +
    geom_errorbar(aes(ymin=pred_rate_lwr,ymax = pred_rate_upr), color= "black", linewidth = 0.8, width = 10) +
    #geom_col(aes(fill = "Upper Threshold"), alpha = 0.7, color = "black", show.legend = F) +
    #geom_text(aes(label = ifelse(alarm=="alarm", "*", ""), y = 10), size = 20 / .pt, ) +
    labs(
      y = "Deaths per 10,000 residents", 
      x = "", 
      title = "Predicted (with 95% CIs) and Observed Number of Deaths per 10,000 (all causes)",
      subtitle = "King County Housing Authority",
      caption = "* Above upper bound threshold\nUpper bound threshold refers to upper 95% prediction interval. Observed deaths above this value are considered to be significantly more than expected."
    ) +
    scale_color_manual("", breaks = c("Predicted deaths"), values = c("black")) +
    #scale_fill_manual("", breaks = c("Observed deaths"), values = c("orange")) +
    scale_fill_manual("", values=c("alarm" = "orange","none" = "gray50"), labels = c("Observed deaths significantly\nabove expected","Observed deaths not significantly\nabove expected"))+
    scale_x_date(labels = date_format("%b %Y")) + 
    #facet_grid(agency ~ year, scales = "free") + 
    #guides(fill = guide_legend(override.aes = list(alpha = c(0.7,1)))) + 
    theme_bw() +
    theme(
      plot.caption = element_text(hjust = 0),
      legend.spacing = unit(-0.25, "cm")
    ) + guides(
      color  = guide_legend(byrow = F, order = 1),
      fill = guide_legend(byrow = T, order = 2)
    ) 
  
}

fig1_fx(m1_out)
fig1_fx(m2_out)
fig1_fx(m3_out)
fig1_fx(m4_out)


# monthly excess deaths ----
fig2_fx <- function(x){
  
  x %>% filter(year > 2019) %>%
    ggplot(data = ., aes(x = date_mo_01, y = diff)) +
    geom_col(size = 1, aes(fill = alarm)) +
    geom_errorbar(aes(ymax = diff_upr, ymin=diff_lwr), width = 5) +
    geom_hline(yintercept = 0, linewidth = 1) +
    facet_wrap(~ year, scales = "free_x", nrow = 2) +
    scale_fill_manual(values=c("alarm" = "orange","none" = "gray50"), labels = c("Significantly above 0","Not significantly above 0")) + 
    labs(
      title = "Monthly excess deaths, all causes", 
      subtitle = "King County Housing Authority",
      x = "", y = "Excess deaths",
      fill = ""
    ) + theme_bw() +
    facet_grid(~ year, scales = c("free")) 
  
}

fig2_fx(m1_out)
fig2_fx(m2_out)
fig2_fx(m3_out)
fig2_fx(m4_out)

 

