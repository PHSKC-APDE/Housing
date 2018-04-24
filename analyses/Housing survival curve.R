# Temp file to look at drop off in Medicaid enrollment
library(dplyr)
library(ggplot2)

temp <- pha_elig_final %>% select(pid2, pt12_m:pt16_m) %>% distinct() %>% mutate_at(vars(ends_with("_m")), funs(ifelse(. < 300, 0, 1)))
temp <- pha_elig_final %>% select(pid2, pt12_m:pt16_m) %>% distinct()
temp2 <- temp %>% slice(1:100)

# Make function to show the proportion of people still enrolled after x days
surv_f <- function(df, pt, x) {
  pt_col <- enquo(pt)
  df %>%
    filter(!is.na((!!pt_col))) %>%
    mutate(num = ifelse((!!pt_col) >= x, 1, 0)) %>%
    summarise(prop = sum(num)/n())
}


# Set up number of days in a year
yr_leap <- data.frame(days = 1:366)
yr <- data.frame(days = 1:365)

# Apply function to all years
surv12 <- cbind(yr_leap, bind_rows(lapply(1:366, surv_f, pt = pt12_m, df = temp)), year = 2012)
surv13 <- cbind(yr, bind_rows(lapply(1:365, surv_f, pt = pt13_m, df = temp)), year = 2013)
surv14 <- cbind(yr, bind_rows(lapply(1:365, surv_f, pt = pt14_m, df = temp)), year = 2014)
surv15 <- cbind(yr, bind_rows(lapply(1:365, surv_f, pt = pt15_m, df = temp)), year = 2015)
surv16 <- cbind(yr_leap, bind_rows(lapply(1:366, surv_f, pt = pt16_m, df = temp)), year = 2016)

# Merge into single df
surv <- bind_rows(surv12, surv13, surv14, surv15, surv16) %>%
  mutate(year = as.factor(year))

# Plot survival curve
ggplot(surv, aes(x = days, y = prop)) +
  geom_line(aes(color = year, group = year)) +
  scale_colour_brewer(type = "qual") +
  geom_hline(yintercept = 0.9, linetype = "dashed", color = "#767F8B") +
  geom_vline(xintercept = 300, linetype = "dashed", color = "#767F8B")
