###############################################################################
# Code to join examine demographics of public housing authority residents
#
# Alastair Matheson (PHSKC-APDE)
# alastair.matheson@kingcounty.gov
# 2017-12-31
#
# NOTE THAT THIS CODE IS A WORK IN PROGRESS
#
###############################################################################

#### Set up global parameter and call in libraries ####
# Turn scientific notation off and other settings
options(max.print = 700, scipen = 100, digits = 5)

library(openxlsx) # Used to import/export Excel files
library(lubridate) # Used to manipulate dates
library(dplyr) # Used to manipulate data
library(pastecs) # Used for summary statistics


#### Bring in data ####
pha_elig_final <- readRDS(file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/pha_elig_final.Rda")


#### Set up variables for analysis ####
# Single age grouping
pha_elig_final <- pha_elig_final %>%
  mutate(agegrp = ifelse(adult == 0 & !is.na(adult), "Youth",
                         ifelse(adult == 1 & senior == 0 & !is.na(adult), "Working age",
                                ifelse(adult == 1 & senior == 1 & !is.na(adult), "Senior", NA))))

# Recode gender and disability
pha_elig_final <- pha_elig_final %>%
  mutate(gender2 = car::recode(gender_new_m6, "'1' = 'Female'; '2' = 'Male'; else = NA"),
         disability2 = car::recode(disability, "'1' = 'Disabled'; '0' = 'Not disabled'; else = NA"))


# ZIPs to restrict to KC
zips <- read.xlsx("//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/ZIP filter for KC.xlsx")
pha_elig_final <- pha_elig_final %>%
  mutate(unit_zip_new = as.numeric(unit_zip_new)) %>%
  left_join(., zips, by = c("unit_zip_new" = "zip"))


# Set up presence/absence in housing, Medicaid, and both at decmber 31 each year
pha_elig_final <- pha_elig_final %>%
  mutate(
    # Enrolled in housing
    dec12_h = ifelse(startdate_c <= as.Date("2012-12-31", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2012-12-31", origin = "1970-01-01") & enroll_type %in% c("b", "h"), 1, 0),
    dec13_h = ifelse(startdate_c <= as.Date("2013-12-31", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2013-12-31", origin = "1970-01-01") & enroll_type %in% c("b", "h"), 1, 0),
    dec14_h = ifelse(startdate_c <= as.Date("2014-12-31", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2014-12-31", origin = "1970-01-01") & enroll_type %in% c("b", "h"), 1, 0),
    dec15_h = ifelse(startdate_c <= as.Date("2015-12-31", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2015-12-31", origin = "1970-01-01") & enroll_type %in% c("b", "h"), 1, 0),
    dec16_h = ifelse(startdate_c <= as.Date("2016-12-31", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2016-12-31", origin = "1970-01-01") & enroll_type %in% c("b", "h"), 1, 0),
    # Enrolled in Medicaid (will be NA if never enrolled in Medicaid)
    dec12_m = ifelse(startdate_c <= as.Date("2012-12-31", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2012-12-31", origin = "1970-01-01") & enroll_type %in% c("b", "m"), 1, 0),
    dec13_m = ifelse(startdate_c <= as.Date("2013-12-31", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2013-12-31", origin = "1970-01-01") & enroll_type %in% c("b", "m"), 1, 0),
    dec14_m = ifelse(startdate_c <= as.Date("2014-12-31", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2014-12-31", origin = "1970-01-01") & enroll_type %in% c("b", "m"), 1, 0),
    dec15_m = ifelse(startdate_c <= as.Date("2015-12-31", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2015-12-31", origin = "1970-01-01") & enroll_type %in% c("b", "m"), 1, 0),
    dec16_m = ifelse(startdate_c <= as.Date("2016-12-31", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2016-12-31", origin = "1970-01-01") & enroll_type %in% c("b", "m"), 1, 0)
  )



# Set up presence/absence in housing for the entire year (to allow comparisons with KCHA numbers)
pha_elig_final <- pha_elig_final %>%
  mutate(
    # Enrolled in housing at all in that year
    any12_h = ifelse(startdate_c <= as.Date("2012-12-31", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2012-01-01", origin = "1970-01-01") & enroll_type %in% c("b", "h"), 1, 0),
    any13_h = ifelse(startdate_c <= as.Date("2013-12-31", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2013-01-01", origin = "1970-01-01") & enroll_type %in% c("b", "h"), 1, 0),
    any14_h = ifelse(startdate_c <= as.Date("2014-12-31", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2014-01-01", origin = "1970-01-01") & enroll_type %in% c("b", "h"), 1, 0),
    any15_h = ifelse(startdate_c <= as.Date("2015-12-31", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2015-01-01", origin = "1970-01-01") & enroll_type %in% c("b", "h"), 1, 0),
    any16_h = ifelse(startdate_c <= as.Date("2016-12-31", origin = "1970-01-01") & 
                       enddate_c >= as.Date("2016-01-01", origin = "1970-01-01") & enroll_type %in% c("b", "h"), 1, 0)
  )


# Set up person-time each year
# First set up intervals for each year
i2012 <- interval(start = "2012-01-01", end = "2012-12-31")
i2013 <- interval(start = "2013-01-01", end = "2013-12-31")
i2014 <- interval(start = "2014-01-01", end = "2014-12-31")
i2015 <- interval(start = "2015-01-01", end = "2015-12-31")
i2016 <- interval(start = "2016-01-01", end = "2016-12-31")


# Person-time in housing, needs to be done separately to avoid errors
pt_temp_h <- pha_elig_final %>%
  filter(enroll_type == "h") %>%
  distinct(pid, startdate_c, enddate_c) %>%
  mutate(
    pt12_h = (lubridate::intersect(interval(start = startdate_c, end = enddate_c), i2012) / ddays(1)) + 1,
    pt13_h = (lubridate::intersect(interval(start = startdate_c, end = enddate_c), i2013) / ddays(1)) + 1,
    pt14_h = (lubridate::intersect(interval(start = startdate_c, end = enddate_c), i2014) / ddays(1)) + 1,
    pt15_h = (lubridate::intersect(interval(start = startdate_c, end = enddate_c), i2015) / ddays(1)) + 1,
    pt16_h = (lubridate::intersect(interval(start = startdate_c, end = enddate_c), i2016) / ddays(1)) + 1
  )

pha_elig_final <- left_join(pha_elig_final, pt_temp_h, by = c("pid", "startdate_c", "enddate_c"))
rm(pt_temp_h)

# Person-time in Medicaid, needs to be done separately to avoid errors
pt_temp_m <- pha_elig_final %>%
  filter(enroll_type == "m") %>%
  distinct(pid, startdate_c, enddate_c) %>%
  mutate(
    pt12_m = (lubridate::intersect(interval(start = startdate_c, end = enddate_c), i2012) / ddays(1)) + 1,
    pt13_m = (lubridate::intersect(interval(start = startdate_c, end = enddate_c), i2013) / ddays(1)) + 1,
    pt14_m = (lubridate::intersect(interval(start = startdate_c, end = enddate_c), i2014) / ddays(1)) + 1,
    pt15_m = (lubridate::intersect(interval(start = startdate_c, end = enddate_c), i2015) / ddays(1)) + 1,
    pt16_m = (lubridate::intersect(interval(start = startdate_c, end = enddate_c), i2016) / ddays(1)) + 1
  )

pha_elig_final <- left_join(pha_elig_final, pt_temp_m, by = c("pid", "startdate_c", "enddate_c"))
rm(pt_temp_m)

# Person-time in both, needs to be done separately to avoid errors
pt_temp_o <- pha_elig_final %>%
  filter(enroll_type == "b") %>%
  distinct(pid, startdate_c, enddate_c) %>%
  mutate(
    pt12_o = (lubridate::intersect(interval(start = startdate_c, end = enddate_c), i2012) / ddays(1)) + 1,
    pt13_o = (lubridate::intersect(interval(start = startdate_c, end = enddate_c), i2013) / ddays(1)) + 1,
    pt14_o = (lubridate::intersect(interval(start = startdate_c, end = enddate_c), i2014) / ddays(1)) + 1,
    pt15_o = (lubridate::intersect(interval(start = startdate_c, end = enddate_c), i2015) / ddays(1)) + 1,
    pt16_o = (lubridate::intersect(interval(start = startdate_c, end = enddate_c), i2016) / ddays(1)) + 1
  )

pha_elig_final <- left_join(pha_elig_final, pt_temp_o, by = c("pid", "startdate_c", "enddate_c"))
rm(pt_temp_o)


# Age at each date
pha_elig_final <- pha_elig_final %>%
  mutate(age12 = round(interval(start = dob_h, end = ymd(20121231)) / years(1), 1),
         age13 = round(interval(start = dob_h, end = ymd(20131231)) / years(1), 1),
         age14 = round(interval(start = dob_h, end = ymd(20141231)) / years(1), 1),
         age15 = round(interval(start = dob_h, end = ymd(20151231)) / years(1), 1),
         age16 = round(interval(start = dob_h, end = ymd(20161231)) / years(1), 1)
  )


#### Optional: Output to Excel for use in Tableau ####
# # Warning: makes huge file and takes a long time
# temp <- pha_elig_final %>%
#   # Strip out identifying variables
#   select(pid, gender_new_m6, race2, disability, adult, senior, agency_new, major_prog:property_id, unit_zip_new, startdate_h:age16)
# 
# write.xlsx(temp, file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/Summaries/pha_elig_demogs_2017-08-09.xlsx")

#### End of optional section ####






#### Testing out a function approach to look at each month/year/quarter ####

# Grouping function
f_phacount_grouped <- function(df, ...) {
  group_var <- quos(...)
  df %>% group_by(!!!group_var)
}

f_phacount <- function(df, yearmin = 2012, yearmax = 2016, period = c("month", "year", "quarter"), group_var, unit = c("pid", "hhold_id_new")) {
  # Set up time period
  if (period == "month") {
    timestart <- seq(as.Date(paste0(yearmin, "-01-01")), length = (yearmax - yearmin + 1) * 12, by = "1 month")
    timeend <- seq(as.Date(paste0(yearmin, "-02-01")), length = (yearmax - yearmin + 1) * 12, by = "1 month") - 1
  }
  if (period == "year") {
    timestart <- seq(as.Date(paste0(yearmin, "-01-01")), length = (yearmax - yearmin + 1), by = "1 year")
    timeend <- seq(as.Date(paste0(yearmin + 1, "-01-01")), length = (yearmax - yearmin + 1), by = "1 year") - 1
  }
  if (period == "quarter") {
    timestart <- seq(as.Date(paste0(yearmin, "-01-01")), length = (yearmax - yearmin + 1) * 4, by = "1 quarter")
    timesend <- seq(as.Date(paste0(yearmin, "-04-01")), length = (yearmax - yearmin + 1) * 4, by = "1 quarter") - 1
  }
  
  # Set up quosures
  grouping_vars <- rlang::syms(group_var)
  print("print1")
  print(group_var)
  print("print3")
  print(grouping_vars)
  
  ### Should convert this to an apply function at some point
  # Make empty list to add data to
  templist = list()
  
  for (i in 1:length(timestart)) {
    # Apply function over list of dates
    templist[[i]] <- df %>%
      filter(startdate_c <= as.Date(timeend[i]) & enddate_c >= as.Date(timestart[i]) & enroll_type %in% c("b", "h"))
    
    templist[[i]] <- f_phacount_grouped(df, !!!grouping_vars) %>%
      summarise(count = n_distinct(!!unit)) %>%
      mutate(date = timestart[i],
             period = !!period,
             unit = quo_name(unit))
  }
  
  phacount <- rbindlist(templist)
  return(phacount)
}


f_phacount(pha_elig_final, group_var = agency_new, unit = "household")
f_phacount(pha_elig_final, period = "year", group_var = "agency_new", unit = "household")
f_phacount(test, period = "year", group_var = c("agency_new", "major_prog"), unit = "household")

test <- pha_elig_final %>% filter(row_number() < 1001)
test2 <- f_phacount_grouped(test, agency_new, major_prog)

f_phacount_grouped(test, agency_new, major_prog) %>%
  summarise(count = n_distinct(hhold_id_new))
test2 %>% summarise(count = n_distinct(hhold_id_new))



f_phacount_grouped(test, rlang::syms(c("agency_new", "major_prog"))) %>%
  summarise(count = n_distinct(hhold_id_new))


f_phacount(test, period = "year", group_var = "agency_new", unit = "hhold_id_new")


pha_elig_final %>% 
  group_by(agency_new, major_prog) %>%
  summarise(count = n_distinct(hhold_id_new))


pha_elig_final %>% 
  filter(startdate_c <= timeend[1] & enddate_c >= timestart[1] & enroll_type %in% c("b", "h")) %>%
  group_by(agency_new) %>%
  summarise(count = n_distinct(hhold_id_new))




### Hardcode groupings for now
f_phacount_agency <- function(df, yearmin = 2012, yearmax = 2016, period = c("date","month", "year", "quarter"), date = "-12-31",
                                unit = c("pid2", "hhold_id_new")) {
  # Set up time period
  if (period == "date") {
    timestart <- seq(as.Date(paste0(yearmin, date)), length = (yearmax - yearmin + 1), by = "1 year")
  }
  if (period == "month") {
    timestart <- seq(as.Date(paste0(yearmin, "-01-01")), length = (yearmax - yearmin + 1) * 12, by = "1 month")
    timeend <- seq(as.Date(paste0(yearmin, "-02-01")), length = (yearmax - yearmin + 1) * 12, by = "1 month") - 1
  }
  if (period == "year") {
    timestart <- seq(as.Date(paste0(yearmin, "-01-01")), length = (yearmax - yearmin + 1), by = "1 year")
    timeend <- seq(as.Date(paste0(yearmin + 1, "-01-01")), length = (yearmax - yearmin + 1), by = "1 year") - 1
  }
  if (period == "quarter") {
    timestart <- seq(as.Date(paste0(yearmin, "-01-01")), length = (yearmax - yearmin + 1) * 4, by = "1 quarter")
    timesend <- seq(as.Date(paste0(yearmin, "-04-01")), length = (yearmax - yearmin + 1) * 4, by = "1 quarter") - 1
  }
  
  ### Should convert this to an apply function at some point
  # Make empty list to add data to
  templist = list()
  
  if (period != "date") {
  for (i in 1:length(timestart)) {
    # Apply function over list of dates
    templist[[i]] <- df %>%
      filter(startdate_c <= as.Date(timeend[i]) & enddate_c >= as.Date(timestart[i]) & enroll_type %in% c("b", "h")) %>%
      group_by(agency_new, major_prog, enroll_type) %>%
      summarise(count = n_distinct(!!unit)) %>%
      mutate(date = timestart[i],
             period = !!period,
             unit = quo_name(unit))
  }
  }
  
  if (period == "date") {
    for (i in 1:length(timestart)) {
      # Apply function over list of dates
      templist[[i]] <- df %>%
        filter(startdate_c <= as.Date(timestart[i]) & enddate_c >= as.Date(timestart[i]) & enroll_type %in% c("b", "h")) %>%
        group_by(agency_new, major_prog, enroll_type) %>%
        summarise(count = n_distinct(!!unit)) %>%
        mutate(date = timestart[i],
               period = !!period,
               unit = quo_name(unit))
    }
  }
  
  phacount <- rbindlist(templist)
  return(phacount)
}

f_phacount_prog <- function(df, yearmin = 2012, yearmax = 2016, period = c("month", "year", "quarter"), unit = c("pid2", "hhold_id_new")) {
  # Set up time period
  if (period == "month") {
    timestart <- seq(as.Date(paste0(yearmin, "-01-01")), length = (yearmax - yearmin + 1) * 12, by = "1 month")
    timeend <- seq(as.Date(paste0(yearmin, "-02-01")), length = (yearmax - yearmin + 1) * 12, by = "1 month") - 1
  }
  if (period == "year") {
    timestart <- seq(as.Date(paste0(yearmin, "-01-01")), length = (yearmax - yearmin + 1), by = "1 year")
    timeend <- seq(as.Date(paste0(yearmin + 1, "-01-01")), length = (yearmax - yearmin + 1), by = "1 year") - 1
  }
  if (period == "quarter") {
    timestart <- seq(as.Date(paste0(yearmin, "-01-01")), length = (yearmax - yearmin + 1) * 4, by = "1 quarter")
    timesend <- seq(as.Date(paste0(yearmin, "-04-01")), length = (yearmax - yearmin + 1) * 4, by = "1 quarter") - 1
  }
  
  ### Should convert this to an apply function at some point
  # Make empty list to add data to
  templist = list()
  
  for (i in 1:length(timestart)) {
    # Apply function over list of dates
    templist[[i]] <- df %>%
      filter(startdate_c <= as.Date(timeend[i]) & enddate_c >= as.Date(timestart[i]) & enroll_type %in% c("b", "h")) %>%
      group_by(agency_new, major_prog, prog_final, spec_purp_type, enroll_type) %>%
      summarise(count = n_distinct(!!unit)) %>%
      mutate(date = timestart[i],
             period = !!period,
             unit = quo_name(unit))
  }
  
  phacount <- rbindlist(templist)
  return(phacount)
}

f_phacount_portfolio <- function(df, yearmin = 2012, yearmax = 2016, period = c("month", "year", "quarter"), unit = c("pid2", "hhold_id_new")) {
  # Set up time period
  if (period == "month") {
    timestart <- seq(as.Date(paste0(yearmin, "-01-01")), length = (yearmax - yearmin + 1) * 12, by = "1 month")
    timeend <- seq(as.Date(paste0(yearmin, "-02-01")), length = (yearmax - yearmin + 1) * 12, by = "1 month") - 1
  }
  if (period == "year") {
    timestart <- seq(as.Date(paste0(yearmin, "-01-01")), length = (yearmax - yearmin + 1), by = "1 year")
    timeend <- seq(as.Date(paste0(yearmin + 1, "-01-01")), length = (yearmax - yearmin + 1), by = "1 year") - 1
  }
  if (period == "quarter") {
    timestart <- seq(as.Date(paste0(yearmin, "-01-01")), length = (yearmax - yearmin + 1) * 4, by = "1 quarter")
    timesend <- seq(as.Date(paste0(yearmin, "-04-01")), length = (yearmax - yearmin + 1) * 4, by = "1 quarter") - 1
  }
  
  ### Should convert this to an apply function at some point
  # Make empty list to add data to
  templist = list()
  
  for (i in 1:length(timestart)) {
    # Apply function over list of dates
    templist[[i]] <- df %>%
      filter(startdate_c <= as.Date(timeend[i]) & enddate_c >= as.Date(timestart[i]) & enroll_type %in% c("b", "h")) %>%
      group_by(agency_new, major_prog, portfolio_final, enroll_type) %>%
      summarise(count = n_distinct(!!unit)) %>%
      mutate(date = timestart[i],
             period = !!period,
             unit = quo_name(unit))
  }
  
  phacount <- rbindlist(templist)
  return(phacount)
}

f_phacount_race <- function(df, yearmin = 2012, yearmax = 2016, period = c("month", "year", "quarter"), unit = c("pid2", "hhold_id_new")) {
  # Set up time period
  if (period == "month") {
    timestart <- seq(as.Date(paste0(yearmin, "-01-01")), length = (yearmax - yearmin + 1) * 12, by = "1 month")
    timeend <- seq(as.Date(paste0(yearmin, "-02-01")), length = (yearmax - yearmin + 1) * 12, by = "1 month") - 1
  }
  if (period == "year") {
    timestart <- seq(as.Date(paste0(yearmin, "-01-01")), length = (yearmax - yearmin + 1), by = "1 year")
    timeend <- seq(as.Date(paste0(yearmin + 1, "-01-01")), length = (yearmax - yearmin + 1), by = "1 year") - 1
  }
  if (period == "quarter") {
    timestart <- seq(as.Date(paste0(yearmin, "-01-01")), length = (yearmax - yearmin + 1) * 4, by = "1 quarter")
    timesend <- seq(as.Date(paste0(yearmin, "-04-01")), length = (yearmax - yearmin + 1) * 4, by = "1 quarter") - 1
  }
  
  ### Should convert this to an apply function at some point
  # Make empty list to add data to
  templist = list()
  
  for (i in 1:length(timestart)) {
    # Apply function over list of dates
    templist[[i]] <- df %>%
      filter(startdate_c <= as.Date(timeend[i]) & enddate_c >= as.Date(timestart[i]) & enroll_type %in% c("b", "h")) %>%
      group_by(agency_new, race2, gender2, enroll_type) %>%
      summarise(count = n_distinct(!!unit)) %>%
      mutate(date = timestart[i],
             period = !!period,
             unit = quo_name(unit))
  }
  
  phacount <- rbindlist(templist)
  return(phacount)
}

f_phacount_agegrp <- function(df, yearmin = 2012, yearmax = 2016, period = c("month", "year", "quarter"), unit = c("pid2", "hhold_id_new")) {
  # Set up time period
  if (period == "month") {
    timestart <- seq(as.Date(paste0(yearmin, "-01-01")), length = (yearmax - yearmin + 1) * 12, by = "1 month")
    timeend <- seq(as.Date(paste0(yearmin, "-02-01")), length = (yearmax - yearmin + 1) * 12, by = "1 month") - 1
  }
  if (period == "year") {
    timestart <- seq(as.Date(paste0(yearmin, "-01-01")), length = (yearmax - yearmin + 1), by = "1 year")
    timeend <- seq(as.Date(paste0(yearmin + 1, "-01-01")), length = (yearmax - yearmin + 1), by = "1 year") - 1
  }
  if (period == "quarter") {
    timestart <- seq(as.Date(paste0(yearmin, "-01-01")), length = (yearmax - yearmin + 1) * 4, by = "1 quarter")
    timesend <- seq(as.Date(paste0(yearmin, "-04-01")), length = (yearmax - yearmin + 1) * 4, by = "1 quarter") - 1
  }
  
  ### Should convert this to an apply function at some point
  # Make empty list to add data to
  templist = list()
  
  for (i in 1:length(timestart)) {
    # Apply function over list of dates
    templist[[i]] <- df %>%
      filter(startdate_c <= as.Date(timeend[i]) & enddate_c >= as.Date(timestart[i]) & enroll_type %in% c("b", "h")) %>%
      group_by(agency_new, agegrp, disability2, gender2, enroll_type) %>%
      summarise(count = n_distinct(!!unit)) %>%
      mutate(date = timestart[i],
             period = !!period,
             unit = quo_name(unit))
  }
  
  phacount <- rbindlist(templist)
  return(phacount)
}

f_phacount_zip <- function(df, yearmin = 2012, yearmax = 2016, period = c("month", "year", "quarter"), unit = c("pid2", "hhold_id_new")) {
  # Set up time period
  if (period == "month") {
    timestart <- seq(as.Date(paste0(yearmin, "-01-01")), length = (yearmax - yearmin + 1) * 12, by = "1 month")
    timeend <- seq(as.Date(paste0(yearmin, "-02-01")), length = (yearmax - yearmin + 1) * 12, by = "1 month") - 1
  }
  if (period == "year") {
    timestart <- seq(as.Date(paste0(yearmin, "-01-01")), length = (yearmax - yearmin + 1), by = "1 year")
    timeend <- seq(as.Date(paste0(yearmin + 1, "-01-01")), length = (yearmax - yearmin + 1), by = "1 year") - 1
  }
  if (period == "quarter") {
    timestart <- seq(as.Date(paste0(yearmin, "-01-01")), length = (yearmax - yearmin + 1) * 4, by = "1 quarter")
    timesend <- seq(as.Date(paste0(yearmin, "-04-01")), length = (yearmax - yearmin + 1) * 4, by = "1 quarter") - 1
  }
  
  ### Should convert this to an apply function at some point
  # Make empty list to add data to
  templist = list()
  
  for (i in 1:length(timestart)) {
    # Apply function over list of dates
    templist[[i]] <- df %>%
      filter(startdate_c <= as.Date(timeend[i]) & enddate_c >= as.Date(timestart[i]) & enroll_type %in% c("b", "h")) %>%
      group_by(agency_new, unit_zip_new, kc_area, enroll_type) %>%
      summarise(count = n_distinct(!!unit)) %>%
      mutate(date = timestart[i],
             period = !!period,
             unit = quo_name(unit))
  }
  
  phacount <- rbindlist(templist)
  return(phacount)
}


# Test things out
f_phacount_prog(pha_elig_final, period = "year", unit = quo(hhold_id_new))
f_phacount_agency(pha_elig_final, yearmin = 2013, yearmax = 2013, period = "date", unit = quo(hhold_id_new))
# Comparison
pha_elig_final %>% 
  filter(startdate_c <= timeend[1] & enddate_c >= timestart[1] & enroll_type %in% c("b", "h")) %>%
  group_by(agency_new, major_prog, prog_final, spec_purp_type) %>%
  summarise(count = n_distinct(hhold_id_new))

f_phacount_prog(pha_elig_final, period = "month", unit = quo(pid2))
pha_elig_final %>% 
  filter(startdate_c <= timeend[1] & enddate_c >= timestart[1] & enroll_type %in% c("b", "h")) %>%
  group_by(agency_new, major_prog, prog_final, spec_purp_type) %>%
  summarise(count = n_distinct(pid2))


### Make files for Tableau export
# Monthly
agency_count_hh <- f_phacount_agency(pha_elig_final, period = "month", unit = quo(hhold_id_new))
program_count_hh <- f_phacount_prog(pha_elig_final, period = "month", unit = quo(hhold_id_new))
portfolio_count_hh <- f_phacount_portfolio(pha_elig_final, period = "month", unit = quo(hhold_id_new))
race_count_hh <- f_phacount_race(pha_elig_final, period = "month", unit = quo(hhold_id_new))
agegrp_count_hh <- f_phacount_agegrp(pha_elig_final, period = "month", unit = quo(hhold_id_new))
zip_count_hh <- f_phacount_zip(pha_elig_final, period = "month", unit = quo(hhold_id_new))

agency_count_ind <- f_phacount_agency(pha_elig_final, period = "month", unit = quo(pid2))
program_count_ind <- f_phacount_prog(pha_elig_final, period = "month", unit = quo(pid2))
portfolio_count_ind <- f_phacount_portfolio(pha_elig_final, period = "month", unit = quo(pid2))
race_count_ind <- f_phacount_race(pha_elig_final, period = "month", unit = quo(pid2))
agegrp_count_ind <- f_phacount_agegrp(pha_elig_final, period = "month", unit = quo(pid2))
zip_count_ind <- f_phacount_zip(pha_elig_final, period = "month", unit = quo(pid2))

# Yearly
agency_count_yr_hh <- f_phacount_agency(pha_elig_final, period = "year", unit = quo(hhold_id_new))
program_count_yr_hh <- f_phacount_prog(pha_elig_final, period = "year", unit = quo(hhold_id_new))
portfolio_count_yr_hh <- f_phacount_portfolio(pha_elig_final, period = "year", unit = quo(hhold_id_new))
race_count_yr_hh <- f_phacount_race(pha_elig_final, period = "year", unit = quo(hhold_id_new))
agegrp_count_yr_hh <- f_phacount_agegrp(pha_elig_final, period = "year", unit = quo(hhold_id_new))
zip_count_yr_hh <- f_phacount_zip(pha_elig_final, period = "year", unit = quo(hhold_id_new))

agency_count_yr_ind <- f_phacount_agency(pha_elig_final, period = "year", unit = quo(pid2))
program_count_yr_ind <- f_phacount_prog(pha_elig_final, period = "year", unit = quo(pid2))
portfolio_count_yr_ind <- f_phacount_portfolio(pha_elig_final, period = "year", unit = quo(pid2))
race_count_yr_ind <- f_phacount_race(pha_elig_final, period = "year", unit = quo(pid2))
agegrp_count_yr_ind <- f_phacount_agegrp(pha_elig_final, period = "year", unit = quo(pid2))
zip_count_yr_ind <- f_phacount_zip(pha_elig_final, period = "year", unit = quo(pid2))


# Combine files
agency_count <- bind_rows(agency_count_hh, agency_count_ind) %>%
  mutate(category = "Agency", group = major_prog)
program_count <- bind_rows(program_count_hh, program_count_ind) %>%
  mutate(category = "Program", group = ifelse(!is.na(spec_purp_type), paste(prog_final, spec_purp_type, sep = " - "), prog_final))
portfolio_count <- bind_rows(portfolio_count_hh, portfolio_count_ind) %>%
  mutate(category = "Portfolio", group = portfolio_final)
race_count <- bind_rows(race_count_hh, race_count_ind) %>%
  mutate(category = "Race", group = race2)
agegrp_count <- bind_rows(agegrp_count_hh, agegrp_count_ind) %>%
  mutate(category = "Age group", group = paste(agegrp, disability2, sep = " - "))
zip_count <- bind_rows(zip_count_hh, zip_count_ind) %>%
  mutate(category = "Zip", group = as.character(unit_zip_new))

agency_count_yr <- bind_rows(agency_count_yr_hh, agency_count_yr_ind) %>%
  mutate(category = "Agency", group = major_prog)
program_count_yr <- bind_rows(program_count_yr_hh, program_count_yr_ind) %>%
  mutate(category = "Program", group = ifelse(!is.na(spec_purp_type), paste(prog_final, spec_purp_type, sep = " - "), prog_final))
portfolio_count_yr <- bind_rows(portfolio_count_yr_hh, portfolio_count_yr_ind) %>%
  mutate(category = "Portfolio", group = portfolio_final)
race_count_yr <- bind_rows(race_count_yr_hh, race_count_yr_ind) %>%
  mutate(category = "Race", group = race2)
agegrp_count_yr <- bind_rows(agegrp_count_yr_hh, agegrp_count_yr_ind) %>%
  mutate(category = "Age group", group = paste(agegrp, disability2, sep = " - "))
zip_count_yr <- bind_rows(zip_count_yr_hh, zip_count_yr_ind) %>%
  mutate(category = "Zip", group = as.character(unit_zip_new))

pha_count <- bind_rows(agency_count, program_count, portfolio_count, race_count, agegrp_count, zip_count,
                       agency_count_yr, program_count_yr, portfolio_count_yr, race_count_yr, agegrp_count_yr, zip_count_yr) %>%
  mutate(medicaid = car::recode(enroll_type, "'b' = 'Medicaid'; 'h' = 'No Medicaid'"),
         unit = car::recode(unit, "'hhold_id_new' = 'Households'; 'pid2' = 'Individuals'"),
         date_yr = ifelse(period == "year", year(date), NA))

write.xlsx(pha_count, file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/Summaries/PHA enrollment count_2017-09-07.xlsx")


rm(list = ls(pattern = "^agency"))
rm(list = ls(pattern = "^program"))
rm(list = ls(pattern = "^portfolio"))
rm(list = ls(pattern = "^race"))
rm(list = ls(pattern = "^zip"))
rm(list = ls(pattern = "^age"))
gc()

#### End function testing ####



#### Counts overall ####

# In housing as at Dec 31
pha_elig_final %>% filter(dec12_h == 1) %>% summarise(count = n_distinct(hhold_id_new))
pha_elig_final %>% filter(dec13_h == 1) %>% summarise(count = n_distinct(hhold_id_new))
pha_elig_final %>% filter(dec14_h == 1) %>% summarise(count = n_distinct(hhold_id_new))
pha_elig_final %>% filter(dec15_h == 1) %>% summarise(count = n_distinct(hhold_id_new))
pha_elig_final %>% filter(dec16_h == 1) %>% summarise(count = n_distinct(hhold_id_new))

pha_elig_final %>% filter(dec12_h == 1) %>% summarise(count = n_distinct(pid))
pha_elig_final %>% filter(dec13_h == 1) %>% summarise(count = n_distinct(pid))
pha_elig_final %>% filter(dec14_h == 1) %>% summarise(count = n_distinct(pid))
pha_elig_final %>% filter(dec15_h == 1) %>% summarise(count = n_distinct(pid))
pha_elig_final %>% filter(dec16_h == 1) %>% summarise(count = n_distinct(pid))

# Ever in housing that year
pha_elig_final %>% filter(any12_h == 1) %>% summarise(count = n_distinct(hhold_id_new))
pha_elig_final %>% filter(any13_h == 1) %>% summarise(count = n_distinct(hhold_id_new))
pha_elig_final %>% filter(any14_h == 1) %>% summarise(count = n_distinct(hhold_id_new))
pha_elig_final %>% filter(any15_h == 1) %>% summarise(count = n_distinct(hhold_id_new))
pha_elig_final %>% filter(any16_h == 1) %>% summarise(count = n_distinct(hhold_id_new))

pha_elig_final %>% filter(any12_h == 1) %>% summarise(count = n_distinct(pid))
pha_elig_final %>% filter(any13_h == 1) %>% summarise(count = n_distinct(pid))
pha_elig_final %>% filter(any14_h == 1) %>% summarise(count = n_distinct(pid))
pha_elig_final %>% filter(any15_h == 1) %>% summarise(count = n_distinct(pid))
pha_elig_final %>% filter(any16_h == 1) %>% summarise(count = n_distinct(pid))


#### Counts of households ####
# NB. Can't look at Mediciad numbers because different members within a household have different enrollment in Medicaid
# In housing as at Dec 31
pha_elig_final %>% filter(dec12_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(hhold_id_new))
pha_elig_final %>% filter(dec13_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(hhold_id_new))
pha_elig_final %>% filter(dec14_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(hhold_id_new))
pha_elig_final %>% filter(dec15_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(hhold_id_new))
pha_elig_final %>% filter(dec16_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(hhold_id_new))

# Ever in housing that year
pha_elig_final %>% filter(any12_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(hhold_id_new))
pha_elig_final %>% filter(any13_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(hhold_id_new))
pha_elig_final %>% filter(any14_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(hhold_id_new))
pha_elig_final %>% filter(any15_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(hhold_id_new))
pha_elig_final %>% filter(any16_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(hhold_id_new))


#### Counts of people ####
# In housing as at Dec 31
pha_elig_final %>% filter(dec12_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(pid))
pha_elig_final %>% filter(dec13_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(pid))
pha_elig_final %>% filter(dec14_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(pid))
pha_elig_final %>% filter(dec15_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(pid))
pha_elig_final %>% filter(dec16_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(pid))

# Ever in housing that year
pha_elig_final %>% filter(any12_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(pid))
pha_elig_final %>% filter(any13_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(pid))
pha_elig_final %>% filter(any14_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(pid))
pha_elig_final %>% filter(any15_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(pid))
pha_elig_final %>% filter(any16_h == 1) %>% group_by(agency_new) %>% summarise(count = n_distinct(pid))



### Enrolled in housing by program
pha_elig_final %>% filter(dec12_h == 1) %>% distinct(pid, .keep_all = TRUE) %>% group_by(agency_new, major_prog, prog_type_new, spec_purp_type, portfolio_new) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count))
pha_elig_final %>% filter(dec13_h == 1) %>% distinct(pid, .keep_all = TRUE) %>% group_by(agency_new, major_prog, prog_type_new, spec_purp_type, portfolio_new) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count))
pha_elig_final %>% filter(dec14_h == 1) %>% distinct(pid, .keep_all = TRUE) %>% group_by(agency_new, major_prog, prog_type_new, spec_purp_type, portfolio_new) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count))
pha_elig_final %>% filter(dec15_h == 1) %>% distinct(pid, .keep_all = TRUE) %>% group_by(agency_new, major_prog, prog_type_new, spec_purp_type, portfolio_new) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count))
pha_elig_final %>% filter(dec16_h == 1) %>% distinct(pid, .keep_all = TRUE) %>% group_by(agency_new, major_prog, prog_type_new, spec_purp_type, portfolio_new) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count))



### Enrolled in Medicaid
pha_elig_final %>% filter(dec12_m == 1) %>% distinct(pid) %>% summarise(count = n())
pha_elig_final %>% filter(dec13_m == 1) %>% distinct(pid) %>% summarise(count = n())
pha_elig_final %>% filter(dec14_m == 1) %>% distinct(pid) %>% summarise(count = n())
pha_elig_final %>% filter(dec15_m == 1) %>% distinct(pid) %>% summarise(count = n())
pha_elig_final %>% filter(dec16_m == 1) %>% distinct(pid) %>% summarise(count = n())


### Enrolled in both housing and Medicaid by housing
pha_elig_final %>% filter(dec12_h == 1 & dec12_m == 1) %>% distinct(pid, .keep_all = TRUE) %>% group_by(agency_new, major_prog, prog_type_new, spec_purp_type) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count))
pha_elig_final %>% filter(dec13_h == 1 & dec13_m == 1) %>% distinct(pid, .keep_all = TRUE) %>% group_by(agency_new, major_prog, prog_type_new, spec_purp_type) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count))
pha_elig_final %>% filter(dec14_h == 1 & dec14_m == 1) %>% distinct(pid, .keep_all = TRUE) %>% group_by(agency_new, major_prog, prog_type_new, spec_purp_type) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count))
pha_elig_final %>% filter(dec15_h == 1 & dec15_m == 1) %>% distinct(pid, .keep_all = TRUE) %>% group_by(agency_new, major_prog, prog_type_new, spec_purp_type) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count))
pha_elig_final %>% filter(dec16_h == 1 & dec16_m == 1) %>% distinct(pid, .keep_all = TRUE) %>% group_by(agency_new, major_prog, prog_type_new, spec_purp_type) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count))



#### SHA ONLY ####
### As at Dec 31
# Households
temp12_sha <- pha_elig_final %>% filter(dec12_h == 1 & agency_new == "SHA") %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2012)
temp13_sha <- pha_elig_final %>% filter(dec13_h == 1 & agency_new == "SHA") %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2013)
temp14_sha <- pha_elig_final %>% filter(dec14_h == 1 & agency_new == "SHA") %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2014)
temp15_sha <- pha_elig_final %>% filter(dec15_h == 1 & agency_new == "SHA") %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2015)
temp16_sha <- pha_elig_final %>% filter(dec16_h == 1 & agency_new == "SHA") %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2016)

sha_count_hh <- bind_rows(temp12_sha, temp13_sha, temp14_sha, temp15_sha, temp16_sha)
sha_count_hh <- mutate(sha_count_hh, count_type = "Households")

# Individuals
temp12_sha <- pha_elig_final %>% filter(dec12_h == 1 & agency_new == "SHA") %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2012)
temp13_sha <- pha_elig_final %>% filter(dec13_h == 1 & agency_new == "SHA") %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2013)
temp14_sha <- pha_elig_final %>% filter(dec14_h == 1 & agency_new == "SHA") %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2014)
temp15_sha <- pha_elig_final %>% filter(dec15_h == 1 & agency_new == "SHA") %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2015)
temp16_sha <- pha_elig_final %>% filter(dec16_h == 1 & agency_new == "SHA") %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2016)

sha_count_ind <- bind_rows(temp12_sha, temp13_sha, temp14_sha, temp15_sha, temp16_sha)
sha_count_ind <- mutate(sha_count_ind, count_type = "Individuals")

sha_count <- bind_rows(sha_count_hh, sha_count_ind) %>%
  select(count_type, year, major_prog:total)
write.xlsx(sha_count, file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/Summaries/SHA enrollment count_2017-08-09.xlsx")



#### KCHA ONLY ####
### As at Dec 31
# Households
temp12_kcha <- pha_elig_final %>% filter(dec12_h == 1 & agency_new == "KCHA") %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2012)
temp13_kcha <- pha_elig_final %>% filter(dec13_h == 1 & agency_new == "KCHA") %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2013)
temp14_kcha <- pha_elig_final %>% filter(dec14_h == 1 & agency_new == "KCHA") %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2014)
temp15_kcha <- pha_elig_final %>% filter(dec15_h == 1 & agency_new == "KCHA") %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2015)
temp16_kcha <- pha_elig_final %>% filter(dec16_h == 1 & agency_new == "KCHA") %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2016)

kcha_count_hh <- bind_rows(temp12_kcha, temp13_kcha, temp14_kcha, temp15_kcha, temp16_kcha)
kcha_count_hh <- mutate(kcha_count_hh, count_type = "Households")

# Individuals
temp12_kcha <- pha_elig_final %>% filter(dec12_h == 1 & agency_new == "KCHA") %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2012)
temp13_kcha <- pha_elig_final %>% filter(dec13_h == 1 & agency_new == "KCHA") %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2013)
temp14_kcha <- pha_elig_final %>% filter(dec14_h == 1 & agency_new == "KCHA") %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2014)
temp15_kcha <- pha_elig_final %>% filter(dec15_h == 1 & agency_new == "KCHA") %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2015)
temp16_kcha <- pha_elig_final %>% filter(dec16_h == 1 & agency_new == "KCHA") %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2016)

kcha_count_ind <- bind_rows(temp12_kcha, temp13_kcha, temp14_kcha, temp15_kcha, temp16_kcha)
kcha_count_ind <- mutate(kcha_count_ind, count_type = "Individuals")

kcha_count_dec <- bind_rows(kcha_count_hh, kcha_count_ind) %>%
  select(count_type, year, major_prog:total)
# write.xlsx(kcha_count_dec, file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/Summaries/kcha enrollment count_2017-08-09.xlsx",
#            sheetName = "dec_30")


### Any point during the year
# Households
temp12_kcha <- pha_elig_final %>% filter(any12_h == 1 & agency_new == "KCHA") %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2012)
temp13_kcha <- pha_elig_final %>% filter(any13_h == 1 & agency_new == "KCHA") %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2013)
temp14_kcha <- pha_elig_final %>% filter(any14_h == 1 & agency_new == "KCHA") %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2014)
temp15_kcha <- pha_elig_final %>% filter(any15_h == 1 & agency_new == "KCHA") %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2015)
temp16_kcha <- pha_elig_final %>% filter(any16_h == 1 & agency_new == "KCHA") %>% distinct(hhold_id_new, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2016)

kcha_count_fy_hh <- bind_rows(temp12_kcha, temp13_kcha, temp14_kcha, temp15_kcha, temp16_kcha)
kcha_count_fy_hh <- mutate(kcha_count_fy_hh, count_type = "Households")

# Individuals
temp12_kcha <- pha_elig_final %>% filter(any12_h == 1 & agency_new == "KCHA") %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2012)
temp13_kcha <- pha_elig_final %>% filter(any13_h == 1 & agency_new == "KCHA") %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2013)
temp14_kcha <- pha_elig_final %>% filter(any14_h == 1 & agency_new == "KCHA") %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2014)
temp15_kcha <- pha_elig_final %>% filter(any15_h == 1 & agency_new == "KCHA") %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2015)
temp16_kcha <- pha_elig_final %>% filter(any16_h == 1 & agency_new == "KCHA") %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(major_prog, prog_type_new, spec_purp_type, portfolio_new, port_in) %>% 
  summarise(count = n()) %>% mutate(total = sum(.$count), year = 2016)

kcha_count_fy_ind <- bind_rows(temp12_kcha, temp13_kcha, temp14_kcha, temp15_kcha, temp16_kcha)
kcha_count_fy_ind <- mutate(kcha_count_fy_ind, count_type = "Individuals")

kcha_count_fy <- bind_rows(kcha_count_fy_hh, kcha_count_fy_ind) %>%
  select(count_type, year, major_prog:total)


# Write combined file (Dec 31 and full year data)
kcha_count_list <- list("dec_31" = kcha_count_dec, "Full_year" = kcha_count_fy)
write.xlsx(kcha_count_list, file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/Summaries/KCHA enrollment count_2017-09-08.xlsx")


#### Demographics ####

#### Try all demographics in one hit (should use apply function to shorten code) ####
temp12 <- pha_elig_final %>% filter(dec12_h == 1 & dec12_m == 0) %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(agency_new, major_prog, prog_type_new, spec_purp_type, portfolio_new, property_name_new, gender_new_m6, adult, senior, disability, race2) %>% 
  summarise(count = n()) %>% mutate(year = 2012)
temp13 <- pha_elig_final %>% filter(dec13_h == 1 & dec13_m == 0) %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(agency_new, major_prog, prog_type_new, spec_purp_type, portfolio_new, property_name_new, gender_new_m6, adult, senior, disability, race2) %>% 
  summarise(count = n()) %>% mutate(year = 2013)
temp14 <- pha_elig_final %>% filter(dec14_h == 1 & dec14_m == 0) %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(agency_new, major_prog, prog_type_new, spec_purp_type, portfolio_new, property_name_new, gender_new_m6, adult, senior, disability, race2) %>% 
  summarise(count = n()) %>% mutate(year = 2014)
temp15 <- pha_elig_final %>% filter(dec15_h == 1 & dec15_m == 0) %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(agency_new, major_prog, prog_type_new, spec_purp_type, portfolio_new, property_name_new, gender_new_m6, adult, senior, disability, race2) %>% 
  summarise(count = n()) %>% mutate(year = 2015)
temp16 <- pha_elig_final %>% filter(dec16_h == 1 & dec16_m == 0) %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(agency_new, major_prog, prog_type_new, spec_purp_type, portfolio_new, property_name_new, gender_new_m6, adult, senior, disability, race2) %>% 
  summarise(count = n()) %>% mutate(year = 2016)

temp2 <- bind_rows(temp12, temp13, temp14, temp15, temp16)
temp2 <- temp2 %>% mutate(area = "Housing alone")

# Summarise people who were in both housing and Medicaid
temp12 <- pha_elig_final %>% filter(dec12_h == 1 & dec12_m == 1) %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(agency_new, major_prog, prog_type_new, spec_purp_type, portfolio_new, property_name_new, gender_new_m6, adult, senior, disability, race2) %>% 
  summarise(count = n()) %>% mutate(year = 2012)
temp13 <- pha_elig_final %>% filter(dec13_h == 1 & dec13_m == 1) %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(agency_new, major_prog, prog_type_new, spec_purp_type, portfolio_new, property_name_new, gender_new_m6, adult, senior, disability, race2) %>% 
  summarise(count = n()) %>% mutate(year = 2013)
temp14 <- pha_elig_final %>% filter(dec14_h == 1 & dec14_m == 1) %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(agency_new, major_prog, prog_type_new, spec_purp_type, portfolio_new, property_name_new, gender_new_m6, adult, senior, disability, race2) %>% 
  summarise(count = n()) %>% mutate(year = 2014)
temp15 <- pha_elig_final %>% filter(dec15_h == 1 & dec15_m == 1) %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(agency_new, major_prog, prog_type_new, spec_purp_type, portfolio_new, property_name_new, gender_new_m6, adult, senior, disability, race2) %>% 
  summarise(count = n()) %>% mutate(year = 2015)
temp16 <- pha_elig_final %>% filter(dec16_h == 1 & dec16_m == 1) %>% distinct(pid, .keep_all = TRUE) %>% 
  group_by(agency_new, major_prog, prog_type_new, spec_purp_type, portfolio_new, property_name_new, gender_new_m6, adult, senior, disability, race2) %>% 
  summarise(count = n()) %>% mutate(year = 2016)

temp3 <- bind_rows(temp12, temp13, temp14, temp15, temp16)
temp3 <- temp3 %>% mutate(area = "Both housing and Medicaid")

temp_join <- bind_rows(temp2, temp3)

# Now add Medicaid-only numbers (no demographics for now)
temp12 <- pha_elig_final %>% filter(dec12_h == 0 & dec12_m == 1) %>% distinct(pid) %>% summarise(count = n()) %>% mutate(year = 2012)
temp13 <- pha_elig_final %>% filter(dec13_h == 0 & dec13_m == 1) %>% distinct(pid) %>% summarise(count = n()) %>% mutate(year = 2013)
temp14 <- pha_elig_final %>% filter(dec14_h == 0 & dec14_m == 1) %>% distinct(pid) %>% summarise(count = n()) %>% mutate(year = 2014)
temp15 <- pha_elig_final %>% filter(dec15_h == 0 & dec15_m == 1) %>% distinct(pid) %>% summarise(count = n()) %>% mutate(year = 2015)
temp16 <- pha_elig_final %>% filter(dec16_h == 0 & dec16_m == 1) %>% distinct(pid) %>% summarise(count = n()) %>% mutate(year = 2016)

temp4 <- bind_rows(temp12, temp13, temp14, temp15, temp16)
temp4 <- temp4 %>% mutate(area = "Medicaid")

temp_join <- bind_rows(temp_join, temp4)


# Make list for exporting
summary_list <- list("Housing" = temp_join, "Medicaid" = temp4)
write.xlsx(temp_join, file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/Summaries/pha_elig_demogs_summary_2017-08-09.xlsx")






# Look at gender each year in housing
pha_elig_final %>%
  filter(dec12_h == 1) %>%
  distinct(pid, .keep_all = TRUE) %>%
  group_by(agency_new, prog_type_new, prog_subtype) %>%
  summarise(female = sum(gender_new_m6[gender_new_m6 == 1], na.rm = TRUE),
            male = sum(gender_new_m6[gender_new_m6 == 2] - 1, na.rm = TRUE),
            unknown = sum(is.na(gender_new_m6)),
            total = n()) %>%
  mutate(femaleper = female / total, maleper = male / total, unkper = unknown / total)

pha_elig_final %>%
  filter(dec13_h == 1) %>%
  distinct(pid, .keep_all = TRUE) %>%
  group_by(agency_new) %>%
  summarise(female = sum(gender_new_m6[gender_new_m6 == 1], na.rm = TRUE),
            male = sum(gender_new_m6[gender_new_m6 == 2] - 1, na.rm = TRUE),
            unknown = sum(is.na(gender_new_m6)),
            total = n()) %>%
  mutate(femaleper = female / total, maleper = male / total, unkper = unknown / total)

pha_elig_final %>%
  filter(dec14_h == 1) %>%
  distinct(pid, .keep_all = TRUE) %>%
  group_by(agency_new) %>%
  summarise(female = sum(gender_new_m6[gender_new_m6 == 1], na.rm = TRUE),
            male = sum(gender_new_m6[gender_new_m6 == 2] - 1, na.rm = TRUE),
            unknown = sum(is.na(gender_new_m6)),
            total = n()) %>%
  mutate(femaleper = female / total, maleper = male / total, unkper = unknown / total)

pha_demogs %>%
  filter(dec15_h == 1) %>%
  distinct(pid, .keep_all = TRUE) %>%
  group_by(agency_new) %>%
  summarise(female = sum(gender_new_m6[gender_new_m6 == 1], na.rm = TRUE),
            male = sum(gender_new_m6[gender_new_m6 == 2] - 1, na.rm = TRUE),
            unknown = sum(is.na(gender_new_m6)),
            total = n()) %>%
  mutate(femaleper = female / total, maleper = male / total, unkper = unknown / total)

pha_elig_final %>%
  filter(dec16_h == 1) %>%
  distinct(pid, .keep_all = TRUE) %>%
  group_by(agency_new) %>%
  summarise(female = sum(gender_new_m6[gender_new_m6 == 1], na.rm = TRUE),
            male = sum(gender_new_m6[gender_new_m6 == 2] - 1, na.rm = TRUE),
            unknown = sum(is.na(gender_new_m6)),
            total = n()) %>%
  mutate(femaleper = female / total, maleper = male / total, unkper = unknown / total)


### Look at age each year
temp <- pha_demogs %>%
  filter(dec12 == 1) %>%
  distinct(pid, .keep_all = TRUE)
stat.desc(temp$age12[temp$agency_new == "KCHA"], basic = F)
stat.desc(temp$age12[temp$agency_new == "SHA"], basic = F)

temp <- pha_demogs %>%
  filter(dec13 == 1) %>%
  distinct(pid, .keep_all = TRUE)
stat.desc(temp$age13[temp$agency_new == "KCHA"], basic = F)
stat.desc(temp$age13[temp$agency_new == "SHA"], basic = F)

temp <- pha_demogs %>%
  filter(dec14 == 1) %>%
  distinct(pid, .keep_all = TRUE)
stat.desc(temp$age14[temp$agency_new == "KCHA"], basic = F)
stat.desc(temp$age14[temp$agency_new == "SHA"], basic = F)

temp <- pha_demogs %>%
  filter(dec15 == 1) %>%
  distinct(pid, .keep_all = TRUE)
stat.desc(temp$age15[temp$agency_new == "KCHA"], basic = F)
stat.desc(temp$age15[temp$agency_new == "SHA"], basic = F)

temp <- pha_demogs %>%
  filter(dec16 == 1) %>%
  distinct(pid, .keep_all = TRUE)
stat.desc(temp$age16[temp$agency_new == "KCHA"], basic = F)
stat.desc(temp$age16[temp$agency_new == "SHA"], basic = F)



### Look at race each year
pha_demogs %>%
  filter(dec12 == 1) %>%
  distinct(pid, .keep_all = TRUE) %>%
  summarise(aian = sum(r_aian_new_alone, na.rm = TRUE),
            asian = sum(r_asian_new_alone, na.rm = TRUE),
            black = sum(r_black_new_alone, na.rm = TRUE),
            multi = sum(r_multi_new, na.rm = TRUE),
            nhpi = sum(r_nhpi_new_alone, na.rm = TRUE),
            white = sum(r_white_new_alone, na.rm = TRUE)
            )

pha_demogs %>%
  filter(dec13 == 1) %>%
  distinct(pid, .keep_all = TRUE) %>%
  summarise(aian = sum(r_aian_new_alone, na.rm = TRUE),
            asian = sum(r_asian_new_alone, na.rm = TRUE),
            black = sum(r_black_new_alone, na.rm = TRUE),
            multi = sum(r_multi_new, na.rm = TRUE),
            nhpi = sum(r_nhpi_new_alone, na.rm = TRUE),
            white = sum(r_white_new_alone, na.rm = TRUE)
  )

pha_demogs %>%
  filter(dec14 == 1) %>%
  distinct(pid, .keep_all = TRUE) %>%
  summarise(aian = sum(r_aian_new_alone, na.rm = TRUE),
            asian = sum(r_asian_new_alone, na.rm = TRUE),
            black = sum(r_black_new_alone, na.rm = TRUE),
            multi = sum(r_multi_new, na.rm = TRUE),
            nhpi = sum(r_nhpi_new_alone, na.rm = TRUE),
            white = sum(r_white_new_alone, na.rm = TRUE)
  )

pha_demogs %>%
  filter(dec15 == 1) %>%
  distinct(pid, .keep_all = TRUE) %>%
  summarise(aian = sum(r_aian_new_alone, na.rm = TRUE),
            asian = sum(r_asian_new_alone, na.rm = TRUE),
            black = sum(r_black_new_alone, na.rm = TRUE),
            multi = sum(r_multi_new, na.rm = TRUE),
            nhpi = sum(r_nhpi_new_alone, na.rm = TRUE),
            white = sum(r_white_new_alone, na.rm = TRUE)
  )

pha_elig_final %>%
  filter(dec16_h == 1) %>%
  distinct(pid, .keep_all = TRUE) %>%
  summarise(aian = sum(r_aian_new_alone, na.rm = TRUE),
            asian = sum(r_asian_new_alone, na.rm = TRUE),
            black = sum(r_black_new_alone, na.rm = TRUE),
            multi = sum(r_multi_new, na.rm = TRUE),
            nhpi = sum(r_nhpi_new_alone, na.rm = TRUE),
            white = sum(r_white_new_alone, na.rm = TRUE)
  )



#### Clear out addresses with 


#### Find if people were ever at YT ####
yt <- pha_demogs %>%
  filter(property_id == "1") %>%
  distinct(pid) %>%
  mutate(yt = 1)

pha_demogs <- left_join(pha_demogs, yt, by = c("pid"))


#### Look at moves between agencies ####
pha_demogs <- pha_demogs %>%
  mutate(
    move_pha = ifelse((pid == lag(pid, 1) | is.na(lag(pid, 1))) & agency_new != lag(agency_new, 1), 1, 0),
    move_to_sha = ifelse((pid == lag(pid, 1) | is.na(lag(pid, 1))) & agency_new != lag(agency_new, 1) &
                           agency_new == "SHA", 1, 0),
    move_to_kcha = ifelse((pid == lag(pid, 1) | is.na(lag(pid, 1))) & agency_new != lag(agency_new, 1) &
                            agency_new == "KCHA", 1, 0),
    move_to_yt = ifelse((pid == lag(pid, 1) | is.na(lag(pid, 1))) & property_id == "1" & !is.na(property_id) &
                          (lag(property_id, 1) != "1" | is.na(lag(property_id, 1))),
                        1, 0),
    move_from_yt = ifelse((pid == lead(pid, 1) | is.na(lead(pid, 1))) & property_id == "1" & !is.na(property_id) &
                            (lead(property_id, 1) != "1" | is.na(lead(property_id, 1))),
                          1, 0)
             )

table(pha_demogs$move_to_kcha[pha_demogs$startdate <= as.Date("2012-01-01", origin = "1970-01-01") & 
                                pha_demogs$startdate >= as.Date("2012-12-31", origin = "1970-01-01")], useNA = 'always')
table(pha_demogs$move_to_sha[pha_demogs$startdate <= as.Date("2012-01-01", origin = "1970-01-01") & 
                                pha_demogs$startdate >= as.Date("2012-12-31", origin = "1970-01-01")], useNA = 'always')

table(pha_demogs$move_to_kcha[pha_demogs$startdate <= as.Date("2013-01-01", origin = "1970-01-01") & 
                                pha_demogs$startdate >= as.Date("2013-12-31", origin = "1970-01-01")], useNA = 'always')
table(pha_demogs$move_to_sha[pha_demogs$startdate <= as.Date("2013-01-01", origin = "1970-01-01") & 
                               pha_demogs$startdate >= as.Date("2013-12-31", origin = "1970-01-01")], useNA = 'always')


