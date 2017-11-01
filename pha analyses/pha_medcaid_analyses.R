###############################################################################
# Code to join Yesler Terrace housing data with Medicaid claims data
#
# Alastair Matheson (PHSKC-APDE)
# 2017-06-30
#
# NOTE THAT THIS CODE IS A WORK IN PROGRESS
#
###############################################################################

##### Set up global parameter and call in libraries #####
options(max.print = 350, tibble.print_max = 30, scipen = 999)

library(RODBC) # Used to connect to SQL server
library(lubridate) # Used to manipulate dates
library(dplyr) # Used to manipulate data
library(tidyr) # Used to reshape data
library(ggplot2) # Used to plot data

##### Connect to the SQL servers #####
db.claims51 <- odbcConnect("PHClaims51")


##### BRING IN DATA #####
### Bring in linked housing/Medicaid elig data with YT already designated
yt_elig_final <- readRDS("//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/SHA cleaning/yt_elig_final.Rds")


### Bring in asthma visit data
ptm01 <- proc.time() # Times how long this query takes (~275 secs)
asthma <- sqlQuery(db.claims51,
                   "SELECT DISTINCT MEDICAID_RECIPIENT_ID as id, FROM_SRVC_DATE AS asthma_from, TO_SRVC_DATE AS asthma_to
                   FROM (SELECT * 
                           FROM (SELECT MEDICAID_RECIPIENT_ID, FROM_SRVC_DATE, TO_SRVC_DATE,
                                 PRIMARY_DIAGNOSIS_CODE AS dx1, DIAGNOSIS_CODE_2 AS dx2, DIAGNOSIS_CODE_3 AS dx3, DIAGNOSIS_CODE_4 AS dx4, 
                                 DIAGNOSIS_CODE_5 AS dx5, DIAGNOSIS_CODE_6 AS dx6, DIAGNOSIS_CODE_7 AS dx7, DIAGNOSIS_CODE_8 AS dx8, 
                                 DIAGNOSIS_CODE_9 AS dx9, DIAGNOSIS_CODE_10 AS dx10, DIAGNOSIS_CODE_11 AS dx11, DIAGNOSIS_CODE_12 AS dx12,
                                 CLM_TYPE_CID, PLACE_OF_SERVICE
                                 FROM [PHClaims].[dbo].[NewClaims]
                                 WHERE (CLM_TYPE_CID IN (3, 12, 23, 26, 31) OR PLACE_OF_SERVICE LIKE '%OFFICE%')) AS a
                         UNPIVOT(value FOR col IN(dx1, dx2, dx3, dx4, dx5, dx6, dx7, dx8, dx9, dx10, dx11, dx12)) AS b
                         WHERE b.value LIKE '493%' OR b.value LIKE 'J45%') AS c
                   ORDER BY MEDICAID_RECIPIENT_ID, asthma_from")
proc.time() - ptm01


# Fix up dates and make year of service variable (to count visits by year)
asthma <- asthma %>%
  mutate_at(vars(asthma_from, asthma_to), funs(as.Date(., origin = "1970-01-01"))) %>% 
  mutate(id = as.character(id), asthma_year = year(asthma_from))

#### END BRING IN DATA SECITON ####


#### JOIN PHA/ELIG TABLE WITH CLAIMS ####
yt_med_claims <- left_join(yt_elig_final, asthma, by = c("MEDICAID_RECIPIENT_ID" = "id"))


##### PROCESS DATA #####
### Asthma visits
yt_med_claims <- yt_med_claims %>%
  # Remove Medicaid claims that fall outside of the time a person was in housing
  mutate(
    asthma_year = ifelse(!is.na(asthma_from) & (asthma_from < startdate_c | asthma_from > enddate_c), NA, asthma_year),
    asthma_to = ifelse(!is.na(asthma_from) & (asthma_from < startdate_c | asthma_from > enddate_c), NA, asthma_to),
    asthma_from = ifelse(!is.na(asthma_from) & (asthma_from < startdate_c | asthma_from > enddate_c), NA, asthma_from)) %>%
  distinct()


# Collapse to count of the number of asthma visits per period of overlapping housing/Medicaid coverage
asthma_sum_tmp <- yt_med_claims %>%
  filter(!is.na(asthma_from)) %>%
  distinct(pid2, startdate_c, asthma_from, asthma_year) %>%
  group_by(pid2, startdate_c, asthma_year) %>%
  summarise(asthma_count = n()) %>%
  ungroup() %>%
  # Reshape so one row per pid, startdate
  spread(., key = asthma_year, value = asthma_count) %>%
  rename(asthma12 = `2012`, asthma13 = `2013`, asthma14 = `2014`, asthma15 = `2015`, asthma16 = `2016`, asthma17 = '2017')

yt_med_claims <- left_join(yt_med_claims, asthma_sum_tmp, by = c("pid2", "startdate_c")) %>%
  select(-asthma_from, -asthma_to, -asthma_year) %>%
  distinct()



##### PROCESS DATA #####
### Asthma visits
# Rates overall by year
asthma_rate12 <- pha_med_claims %>% 
  summarise(
    asthma_cnt = sum(asthma12, na.rm = T),
    medpop = n_distinct(pid[!is.na(pt12_m) | !is.na(pt12_o)]),
    pt_med = ((sum(pt12_m, na.rm = T) + sum(pt12_o, na.rm = T))/365.25),
    asthma_rate = sum(asthma12, na.rm = T)/((sum(pt12_m, na.rm = T) + sum(pt12_o, na.rm = T))/365.25) * 100000
  ) %>%
  mutate(year = 2012)
asthma_rate13 <- pha_med_claims %>%
  summarise(
    asthma_cnt = sum(asthma13, na.rm = T),
    medpop = n_distinct(pid[!is.na(pt13_m) | !is.na(pt13_o)]),
    pt_med = ((sum(pt13_m, na.rm = T) + sum(pt13_o, na.rm = T))/365.25),
    asthma_rate = sum(asthma13, na.rm = T)/((sum(pt13_m, na.rm = T) + sum(pt13_o, na.rm = T))/365.25) * 100000
  ) %>%
  mutate(year = 2013)
asthma_rate14 <- pha_med_claims %>%
  summarise(
    asthma_cnt = sum(asthma14, na.rm = T),
    medpop = n_distinct(pid[!is.na(pt14_m) | !is.na(pt14_o)]),
    pt_med = ((sum(pt14_m, na.rm = T) + sum(pt14_o, na.rm = T))/365.25),
    asthma_rate = sum(asthma14, na.rm = T)/((sum(pt14_m, na.rm = T) + sum(pt14_o, na.rm = T))/365.25) * 100000
  ) %>%
  mutate(year = 2014)
asthma_rate15 <- pha_med_claims %>% 
  summarise(
    asthma_cnt = sum(asthma15, na.rm = T),
    medpop = n_distinct(pid[!is.na(pt15_m) | !is.na(pt15_o)]),
    pt_med = ((sum(pt15_m, na.rm = T) + sum(pt15_o, na.rm = T))/365.25),
    asthma_rate = sum(asthma15, na.rm = T)/((sum(pt15_m, na.rm = T) + sum(pt15_o, na.rm = T))/365.25) * 100000
  ) %>%
  mutate(year = 2015)
asthma_rate16 <- pha_med_claims %>% 
  summarise(
    asthma_cnt = sum(asthma16, na.rm = T),
    medpop = n_distinct(pid[!is.na(pt16_m) | !is.na(pt16_o)]),
    pt_med = ((sum(pt16_m, na.rm = T) + sum(pt16_o, na.rm = T))/365.25),
    asthma_rate = sum(asthma16, na.rm = T)/((sum(pt16_m, na.rm = T) + sum(pt16_o, na.rm = T))/365.25) * 100000
  ) %>%
  mutate(year = 2016)


asthma_rate_overall <- bind_rows(asthma_rate12, asthma_rate13, asthma_rate14, asthma_rate15, asthma_rate16)


# Look at rates by program and year
asthma_rate12 <- pha_med_claims %>% 
  group_by(agency_new, major_prog, prog_group, portfolio_group) %>%
  summarise(
    asthma_cnt = sum(asthma12, na.rm = T),
    medpop = n_distinct(pid[!is.na(pt12_m) | !is.na(pt12_o)]),
    pt_med = ((sum(pt12_m, na.rm = T) + sum(pt12_o, na.rm = T))/365.25),
    asthma_rate = asthma_cnt/pt_med * 100000,
    asthma_se = sqrt(asthma_cnt / pt_med) * 100000,
    asthma_l95exact = (qchisq(0.025, 2*asthma_cnt)/2) / pt_med * 100000,
    asthma_u95exact = (qchisq(0.975, 2*(asthma_cnt+1))/2) / pt_med * 100000
  ) %>%
  mutate(year = 2012)
asthma_rate13 <- pha_med_claims %>% 
  group_by(agency_new, major_prog, prog_group, portfolio_group) %>%
  summarise(
    asthma_cnt = sum(asthma13, na.rm = T),
    medpop = n_distinct(pid[!is.na(pt13_m) | !is.na(pt13_o)]),
    pt_med = ((sum(pt13_m, na.rm = T) + sum(pt13_o, na.rm = T))/365.25),
    asthma_rate = asthma_cnt/pt_med * 100000,
    asthma_se = sqrt(asthma_cnt / pt_med) * 100000,
    asthma_l95exact = (qchisq(0.025, 2*asthma_cnt)/2) / pt_med * 100000,
    asthma_u95exact = (qchisq(0.975, 2*(asthma_cnt+1))/2) / pt_med * 100000
  ) %>%
  mutate(year = 2013)
asthma_rate14 <- pha_med_claims %>% 
  group_by(agency_new, major_prog, prog_group, portfolio_group) %>%
  summarise(
    asthma_cnt = sum(asthma14, na.rm = T),
    medpop = n_distinct(pid[!is.na(pt14_m) | !is.na(pt14_o)]),
    pt_med = ((sum(pt14_m, na.rm = T) + sum(pt14_o, na.rm = T))/365.25),
    asthma_rate = asthma_cnt/pt_med * 100000,
    asthma_se = sqrt(asthma_cnt / pt_med) * 100000,
    asthma_l95exact = (qchisq(0.025, 2*asthma_cnt)/2) / pt_med * 100000,
    asthma_u95exact = (qchisq(0.975, 2*(asthma_cnt+1))/2) / pt_med * 100000
  ) %>%
  mutate(year = 2014)
asthma_rate15 <- pha_med_claims %>% 
  group_by(agency_new, major_prog, prog_group, portfolio_group) %>%
  summarise(
    asthma_cnt = sum(asthma15, na.rm = T),
    medpop = n_distinct(pid[!is.na(pt15_m) | !is.na(pt15_o)]),
    pt_med = ((sum(pt15_m, na.rm = T) + sum(pt15_o, na.rm = T))/365.25),
    asthma_rate = asthma_cnt/pt_med * 100000,
    asthma_se = sqrt(asthma_cnt / pt_med) * 100000,
    asthma_l95exact = (qchisq(0.025, 2*asthma_cnt)/2) / pt_med * 100000,
    asthma_u95exact = (qchisq(0.975, 2*(asthma_cnt+1))/2) / pt_med * 100000
  ) %>%
  mutate(year = 2015)
asthma_rate16 <- pha_med_claims %>% 
  group_by(agency_new, major_prog, prog_group, portfolio_group) %>%
  summarise(
    asthma_cnt = sum(asthma16, na.rm = T),
    medpop = n_distinct(pid[!is.na(pt16_m) | !is.na(pt16_o)]),
    pt_med = ((sum(pt16_m, na.rm = T) + sum(pt16_o, na.rm = T))/365.25),
    asthma_rate = asthma_cnt/pt_med * 100000,
    asthma_se = sqrt(asthma_cnt / pt_med) * 100000,
    asthma_l95exact = (qchisq(0.025, 2*asthma_cnt)/2) / pt_med * 100000,
    asthma_u95exact = (qchisq(0.975, 2*(asthma_cnt+1))/2) / pt_med * 100000
  ) %>%
  mutate(year = 2016)


asthma_rate_prog <- bind_rows(asthma_rate12, asthma_rate13, asthma_rate14, asthma_rate15, asthma_rate16)
# Set up single field to plot on
asthma_rate_prog <- asthma_rate_prog %>%
  ungroup() %>%
  # Replace NAs
  #mutate_at(vars(prog_group, portfolio_group), funs(str_replace(NA, ""))) %>%
  mutate(
    prog_group = ifelse(is.na(prog_group), "", prog_group),
    portfolio_group = ifelse(is.na(portfolio_group), "", portfolio_group),
    prog_final = paste(agency_new, major_prog, prog_group, portfolio_group, sep = "-"),
    # Replace extra separators
    prog_final = str_replace(prog_final, "--", "-"),
    prog_final = str_replace(prog_final, "-$", ""),
    prog_final = as.factor(prog_final)
    )


# Look at rates by YT vs. SHA
asthma_rate12 <- yt_med_claims %>% 
  group_by(agency_new, yt, yt_new) %>%
  summarise(
    asthma_cnt = sum(asthma12, na.rm = T),
    medpop = n_distinct(pid2[!is.na(pt12_m) | !is.na(pt12_o)]),
    pt_med = ((sum(pt12_m, na.rm = T) + sum(pt12_o, na.rm = T))/365.25),
    asthma_rate = asthma_cnt/pt_med * 100000,
    asthma_se = sqrt(asthma_cnt / pt_med) * 100000,
    asthma_l95exact = (qchisq(0.025, 2*asthma_cnt)/2) / pt_med * 100000,
    asthma_u95exact = (qchisq(0.975, 2*(asthma_cnt+1))/2) / pt_med * 100000
  ) %>%
  mutate(year = 2012)
asthma_rate13 <- yt_med_claims %>% 
  group_by(agency_new, yt, yt_new) %>%
  summarise(
    asthma_cnt = sum(asthma13, na.rm = T),
    medpop = n_distinct(pid2[!is.na(pt13_m) | !is.na(pt13_o)]),
    pt_med = ((sum(pt13_m, na.rm = T) + sum(pt13_o, na.rm = T))/365.25),
    asthma_rate = asthma_cnt/pt_med * 100000,
    asthma_se = sqrt(asthma_cnt / pt_med) * 100000,
    asthma_l95exact = (qchisq(0.025, 2*asthma_cnt)/2) / pt_med * 100000,
    asthma_u95exact = (qchisq(0.975, 2*(asthma_cnt+1))/2) / pt_med * 100000
  ) %>%
  mutate(year = 2013)
asthma_rate14 <- yt_med_claims %>% 
  group_by(agency_new, yt, yt_new) %>%
  summarise(
    asthma_cnt = sum(asthma14, na.rm = T),
    medpop = n_distinct(pid2[!is.na(pt14_m) | !is.na(pt14_o)]),
    pt_med = ((sum(pt14_m, na.rm = T) + sum(pt14_o, na.rm = T))/365.25),
    asthma_rate = asthma_cnt/pt_med * 100000,
    asthma_se = sqrt(asthma_cnt / pt_med) * 100000,
    asthma_l95exact = (qchisq(0.025, 2*asthma_cnt)/2) / pt_med * 100000,
    asthma_u95exact = (qchisq(0.975, 2*(asthma_cnt+1))/2) / pt_med * 100000
  ) %>%
  mutate(year = 2014)
asthma_rate15 <- yt_med_claims %>% 
  group_by(agency_new, yt, yt_new) %>%
  summarise(
    asthma_cnt = sum(asthma15, na.rm = T),
    medpop = n_distinct(pid2[!is.na(pt15_m) | !is.na(pt15_o)]),
    pt_med = ((sum(pt15_m, na.rm = T) + sum(pt15_o, na.rm = T))/365.25),
    asthma_rate = asthma_cnt/pt_med * 100000,
    asthma_se = sqrt(asthma_cnt / pt_med) * 100000,
    asthma_l95exact = (qchisq(0.025, 2*asthma_cnt)/2) / pt_med * 100000,
    asthma_u95exact = (qchisq(0.975, 2*(asthma_cnt+1))/2) / pt_med * 100000
  ) %>%
  mutate(year = 2015)
asthma_rate16 <- yt_med_claims %>% 
  group_by(agency_new, yt, yt_new) %>%
  summarise(
    asthma_cnt = sum(asthma16, na.rm = T),
    medpop = n_distinct(pid2[!is.na(pt16_m) | !is.na(pt16_o)]),
    pt_med = ((sum(pt16_m, na.rm = T) + sum(pt16_o, na.rm = T))/365.25),
    asthma_rate = asthma_cnt/pt_med * 100000,
    asthma_se = sqrt(asthma_cnt / pt_med) * 100000,
    asthma_l95exact = (qchisq(0.025, 2*asthma_cnt)/2) / pt_med * 100000,
    asthma_u95exact = (qchisq(0.975, 2*(asthma_cnt+1))/2) / pt_med * 100000
  ) %>%
  mutate(year = 2016)


asthma_rate_yt <- bind_rows(asthma_rate12, asthma_rate13, asthma_rate14, asthma_rate15, asthma_rate16)
# Set up single field to plot on
asthma_rate_yt <- asthma_rate_yt %>% ungroup()

rm(list = ls(pattern = "^asthma_rate1"))
gc()


# Export data
write.xlsx(asthma_rate_prog, file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/Summaries/PHA_asthma_2017-08-13.xlsx")

# Plot results
a <- ggplot(data = asthma_rate_prog, aes(x = year, y = asthma_rate))
a + geom_line(aes(linetype = prog_final))
qplot(x = year, y = asthma_rate, data = asthma_rate_prog, facets = agency_new~.)



pha_med_claims2 %>%
  filter(agency_new == "SHA") %>%
  group_by(major_prog, prog_group, portfolio_group) %>%
  summarise(
    asthma_12 = sum(asthma_count[asthma_year == 2012], na.rm = TRUE),
    asthma_13 = sum(asthma_count[asthma_year == 2013], na.rm = TRUE),
    asthma_14 = sum(asthma_count[asthma_year == 2014], na.rm = TRUE),
    asthma_15 = sum(asthma_count[asthma_year == 2015], na.rm = TRUE),
    asthma_16 = sum(asthma_count[asthma_year == 2016], na.rm = TRUE),
    pt_12_o = sum(pt12_o, na.rm = TRUE),
    pt_13_o = sum(pt13_o, na.rm = TRUE),
    pt_14_o = sum(pt14_o, na.rm = TRUE),
    pt_15_o = sum(pt15_o, na.rm = TRUE),
    pt_16_o = sum(pt16_o, na.rm = TRUE)
  ) %>%
  mutate(
    rate_12 = asthma_12 / pt_12_o * 100000,
    rate_13 = asthma_13 / pt_13_o * 100000,
    rate_14 = asthma_14 / pt_14_o * 100000,
    rate_15 = asthma_15 / pt_15_o * 100000,
    rate_16 = asthma_16 / pt_16_o * 100000
  )


### Output to Excel for use in Tableau
write.xlsx(yt_claims_asthma2, file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/Summaries/pha_asthma_visits.xlsx")





########## TESTING AREA #############
yt_claims %>% 
  filter(drop == 1 & !is.na(asthma_from)) %>%
  select(ssn_new_pha, startdate_h:enddate_o, pt15_o, dob_h, ed_from, asthma_from, drop) %>%
  head(.)


yt_claims_ed %>% 
  filter(!is.na(ed_from)) %>%
  select(ssn_new_pha, startdate_h:enddate_o, pt15_o, dob_h, ed_from, ed_to, drop, ed_count, ed_year) %>%
  head(.)


yt_claims_ed %>% 
  filter(ssn_new_pha == "100158522") %>%
  select(ssn_new_pha, startdate_h:enddate_o, pt15_o, dob_h, ed_from, ed_to, drop, ed_count, ed_year)


yt_claims_ed2 %>% 
  select(ssn_new_pha, dob_h, startdate_h:enddate_o, pt13_o, pt14_o, pt15_o, pt16_o, ed_year, ed_count) %>%
  head(.)


temp2 <- yt_claims_ed2 %>%
  filter(yt == 1 & ed_year == 2015 & !is.na(ed_count)) %>%
  select(ssn_new_pha, dob_h, startdate_h:enddate_o, pt13_o, pt14_o, pt15_o, pt16_o, ed_year, ed_count)
