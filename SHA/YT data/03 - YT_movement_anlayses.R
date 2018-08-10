###############################################################################
# OVERVIEW:
# Code to examine Yesler Terrace and Scattered sites data (housing and health)
#
# STEPS:
# 01 - Set up YT parameters in combined PHA/Medicaid data
# 02 - Conduct demographic analyses and produce visualizations
# 03 - Analyze movement patterns and geographic elements (optional) ### (THIS CODE) ###
# 03 - Bring in health conditions and join to demographic data
# 04 - Conduct health condition analyses (multiple files)
#
# Alastair Matheson (PHSKC-APDE)
# alastair.matheson@kingcounty.gov
# 2017-06-30
#
###############################################################################


#### Analyses in this code ####
# 1) Maps of where YT and SS residents live (mostly SS)
# 2) Statistics on movement
# 3) Sankey diagram of movement

### Movements within data - simple version
# Use simplified system for describing movement

# First letter of start_type describes previous address,
# Second letter of start_type describes current address

# First letter of end_type describes current address,
# Second letter of end_type describes next address

#   K = KCHA
#   N = YT address (new unit)
#   O = non-YT, non-scattered site SHA unit
#   S = SHA scattered site
#   U = unknown (i.e., new into SHA system, mostly people who only had Medicaid but not PHA coverage)
#   Y = YT address (old unit)


#### Set up global parameter and call in libraries ####
# Turn scientific notation off and other settings
options(max.print = 700, scipen = 100, digits = 5)

library(housing) # contains many useful functions for analyses
library(openxlsx) # Used to import/export Excel files
library(tidyverse) # Used to manipulate data
library(lubridate) # Used to manipulate dates
library(pastecs) # Used for summary statistics
library(ggplot2) # Used to make plots
library(ggmap) # Used to incorporate Google maps data
library(sf) # newer package for working with spatial data
library(networkD3) # Used to make sankey diagram and other fancy plots

housing_path <- "//phdata01/DROF_DATA/DOH DATA/Housing"


#### BRING IN DATA ####
# Bring in combined PHA/Medicaid data with some demographics already run ####
yt_mcaid_final <- readRDS(file = paste0(housing_path, 
                                        "/OrganizedData/SHA cleaning/yt_mcaid_final.Rds"))


# Retain only people living at YT or SS
yt_ss <- yt_mcaid_final %>% filter(yt == 1 | ss == 1)


#### FUNCTIONS ####
# Counts the number of people each year including move ins/outs (only includes non-dual Medicaid enrollees)
move_count_yt_f <- function(df, year, place = "yt") {
  
  yr_start <- as.Date(paste0(year, "-01-01"), origin = "1970-01-01")
  yr_end <- as.Date(paste0(year, "-12-31"), origin = "1970-01-1")
  
  if (place == "yt") {
    place <- quo((place == "Ym" | place == "Nm"))
    start_type <- quo(start_type %in% c("YmYm", "YmNm", "NmYm", "NmNm"))
    end_type <- quo(end_type %in% c("YmYm", "YmNm", "NmYm", "NmNm"))
  } else if(place == "ss") {
    place <- quo(place == "Sm")
    start_type <- quo(start_type == "SmSm")
    end_type <- quo(end_type == "SmSm")
  }
  
  
  # Pop at the start of the year (exludes move ins on Jan 1)
  start <- df %>% filter(!!place & startdate_c < yr_start & enddate_c >= yr_start) %>% 
    summarise(start = n())
  
  # Move ins/coverage start on Jan 1
  jan1 <- df %>% filter(!!place & startdate_c == yr_start & enddate_c >= yr_start) %>% 
    summarise(jan1 = n())
  
  # Number of move-ins or coverage gains in that year (people can be counted 1+ times)
  move_in <- df %>% filter(!!place & startdate_c <= yr_end & 
                             ((!(!!start_type) & startdate_c > yr_start) |
                                (!!start_type & startdate_c == yr_start)
                             )) %>%
    summarise(move_in = n())
  
  # Number move outs or lost Medicaid coverage in that year (ppl can be counted 1+ times)
  move_out <- df %>% filter(!!place & enddate_c <= yr_end & 
                              ((!(!!end_type) & enddate_c >= yr_start) |
                                 (!!end_type & enddate_c == yr_end)
                              )) %>% 
    summarise(move_out = n())
  
  # Pop at midnight at end of the year
  end <- df %>% filter(!!place & 
                         startdate_c <= yr_end & enddate_c > yr_end) %>% 
    summarise(end = n())
  
  ever <- df %>% filter(!!place & startdate_c <= yr_end & enddate_c >= yr_start) %>%
    summarise(ever = n_distinct(pid2))
  
  output <- as.data.frame(cbind(year, start, jan1, move_in, move_out, end, ever))
  
  return(output)
  
}


# Counts movement between YT and SS among non-dual Medicaid enrollees
yt_ss_moves_f <- function(df, year, place = "yt") {
  yr_start <- as.Date(paste0(year, "-01-01"), origin = "1970-01-01")
  yr_end <- as.Date(paste0(year, "-12-31"), origin = "1970-01-1")
  
  if (place == "yt") {
    from_name <- "moves_from_ss"
    to_name <- "moves_to_ss"
    
    moves_from <- quo(start_type %in% c("SmYm", "SmNm"))
    moves_to <- quo(end_type %in% c("YmSm", "NmSm"))
    
  } else if(place == "ss") {
    from_name <- "moves_from_yt"
    to_name <- "moves_to_yt"
    
    moves_from <- quo(start_type %in% c("YmSm", "NmSm"))
    moves_to <- quo(end_type %in% c("SmYm", "SmNm"))
  }
  
  output_from <- df %>%
    filter(!!moves_from & startdate_c >= yr_start & startdate_c <= yr_end) %>%
    summarise(!!from_name := n())
  
  output_to <- df %>%
    filter(!!moves_to & enddate_c >= yr_start & enddate_c <= yr_end) %>%
    summarise(!!to_name := n())
  
  output <- as.data.frame(cbind(year, output_from, output_to))
  
  return(output)
  
}


# Function to look at status each date over a given period
# Uses the time_range function from the housing package
period_place_f <- function(df, startdate = NULL, enddate = NULL, 
                           place = place,
                           enroll = enroll_type,
                           medicaid = F, kcha = F, ...) {
  
  # Set up quosures and other variables
  if(!is.null(startdate)) {
    start_var <- enquo(startdate)
  } else if("startdate_c" %in% names(df)) {
    start_var <- quo(startdate_c)
  } else if("startdate" %in% names(df)) {
    start_var <- quo(startdate)
  } else {
    stop("No valid startdate found")
  }
  
  if(!is.null(enddate)) {
    end_var <- enquo(enddate)
  } else if("enddate_c" %in% names(df)) {
    end_var <- quo(enddate_c)
  } else if ("enddate" %in% names(df)) {
    end_var <- quo(enddate)
  } else {
    stop("No valid enddate found")
  }
  
  place <- enquo(place)
  enroll <- enquo(enroll)
  
  # Recode place into smaller groups
  # Put SS first so the Sankey diagram sorts better
  # 1 = SS and Medicaid
  # 2 = SS and not Medicaid
  # 3 = YT and Mediciad
  # 4 = YT and not Medicaid
  # 5 = Other SHA and Medicaid
  # 6 = Other SHA and not Medicaid
  # 7 = KCHA (all Medicaid statuses)
  # 8 = Medicaid only
  if(medicaid == T & kcha == T) {
    df <- df %>%
      mutate(place_new = case_when(
        !!place == "S" & !!enroll %in% c("b", "m") ~ 1,
        !!place == "S" & !!enroll == "h" ~ 2,
        !!place %in% c("Y", "N") & !!enroll %in% c("b", "m") ~ 3,
        !!place %in% c("Y", "N") & !!enroll == "h" ~ 4,
        !!place == "O" & !!enroll %in% c("b", "m") ~ 5,
        !!place == "O" & !!enroll == "h" ~ 6,
        !!place == "K" ~ 7,
        !!place == "U" ~ 8
      ))
  } else if(medicaid == F & kcha == T) {
    df <- df %>%
      mutate(place_new = case_when(
        !!place == "S" & !!enroll %in% c("b", "m") ~ 1,
        !!place == "S" & !!enroll == "h" ~ 2,
        !!place %in% c("Y", "N") & !!enroll %in% c("b", "m") ~ 3,
        !!place %in% c("Y", "N") & !!enroll == "h" ~ 4,
        !!place == "O" & !!enroll %in% c("b", "m") ~ 5,
        !!place == "O" & !!enroll == "h" ~ 6,
        !!place == "K" ~ 7
      ))
  } else if(medicaid == T & kcha == F) {
    df <- df %>%
      mutate(place_new = case_when(
        !!place == "S" & !!enroll %in% c("b", "m") ~ 1,
        !!place == "S" & !!enroll == "h" ~ 2,
        !!place %in% c("Y", "N") & !!enroll %in% c("b", "m") ~ 3,
        !!place %in% c("Y", "N") & !!enroll == "h" ~ 4,
        !!place == "O" & !!enroll %in% c("b", "m") ~ 5,
        !!place == "O" & !!enroll == "h" ~ 6,
        !!place == "U" ~ 8
      ))
  } else if(medicaid == F & kcha == F) {
    df <- df %>%
      mutate(place_new = case_when(
        !!place == "S" & !!enroll %in% c("b", "m") ~ 1,
        !!place == "S" & !!enroll == "h" ~ 2,
        !!place %in% c("Y", "N") & !!enroll %in% c("b", "m") ~ 3,
        !!place %in% c("Y", "N") & !!enroll == "h" ~ 4,
        !!place == "O" & !!enroll %in% c("b", "m") ~ 5,
        !!place == "O" & !!enroll == "h" ~ 6
      ))
    
  }
  
  # Set up time period and capture period used for output
  timestart <- time_range(...)[[1]]
  
  ### Should convert this to an apply function at some point
  # Make empty list to add data to
  templist = list()
  
  for (i in 1:length(timestart)) {
    
    templist[[i]] <- df %>%
      filter((!!start_var) <= timestart[i] & (!!end_var) >= timestart[i]) %>% 
      distinct(pid2, place_new) %>%
      mutate(date = timestart[i]) %>%
      select(date, pid2, place_new)
    
  }
  
  output <- data.table::rbindlist(templist) %>%
    arrange(date, pid2)
  return(output)
}


#### 1) Maps of where YT and SS residents live (mostly SS) ####
### NB. As of 2018-08-10 some YT addresses have been geocoded to incorrect coords
#   Need to look into this and possible rerun geocoding.

# Set up spatial data frame
yt_mapdata <- st_as_sf(yt_ss, coords = c("lon_h", "lat_h"), remove = F)
st_crs(yt_mapdata) <- 4326

# Add basemap
yt_map <- get_map(location = c(lon = mean(yt_mapdata$lon_h, na.rm = T), 
                               lat = mean(yt_mapdata$lat_h, na.rm = T)), 
                  zoom = 16, crop = T)

plot(yt_mapdata["yt"])
# Slow version
ggplot() +
  geom_sf(data = yt_mapdata, aes(color = yt))
# Doesn't play nicely with the basemap
ggplot(yt_map) +
  geom_sf(data = yt_mapdata)



#### 2) Statistics on movement ####
### YT
# Move ins and outs
as.data.frame(data.table::rbindlist(
  lapply(seq(2012, 2017), move_count_yt_f, df = yt_movement, place = "yt")
  ))

# Movement from YT to SS
as.data.frame(data.table::rbindlist(
  lapply(seq(2012, 2017), yt_ss_moves_f, df = yt_movement, place = "yt")
))


# Mean person-time at site
yt_movement %>% filter(place == "Ym" | place == "Nm") %>% 
  distinct(pid2, pt12_h, pt13_h, pt14_h, pt15_h, pt16_h) %>% 
  summarise(mean12 = mean(pt12_h, na.rm = T), mean13 = mean(pt13_h, na.rm = T), 
            mean14 = mean(pt14_h, na.rm = T), mean15 = mean(pt15_h, na.rm = T), 
            mean16 = mean(pt16_h, na.rm = T))


### Scattered sites
# Move ins and outs
as.data.frame(data.table::rbindlist(
  lapply(seq(2012, 2016), move_count_yt_f, df = yt_movement, place = "ss")
))

# Movement from SS to YT
as.data.frame(data.table::rbindlist(
  lapply(seq(2012, 2016), yt_ss_moves_f, df = yt_movement, place = "ss")
))

# Mean person-time at site
yt_movement %>% filter(place == "Sm") %>% 
  distinct(pid2, pt12_h, pt13_h, pt14_h, pt15_h, pt16_h) %>% 
  summarise(mean12 = mean(pt12_h, na.rm = T), mean13 = mean(pt13_h, na.rm = T), 
            mean14 = mean(pt14_h, na.rm = T), mean15 = mean(pt15_h, na.rm = T), 
            mean16 = mean(pt16_h, na.rm = T))



#### 3) Make a Sankey diagram ####

sankey_data_setup_f <- function(df, period = c("year", "quarter", "binannual"), 
                                medicaid = T, kcha = T, ...) {
  
  # Set up parameters to send to function
  period2 <- period
  medicaid2 <- medicaid
  kcha2 <- kcha
  
  
  movement <- period_place_f(df, period = period2,
                             place = place,
                             enroll = enroll_type, 
                             medicaid = medicaid2,
                             kcha = kcha2)
  
  # Get a source and target for everyone
  date <- as.Date(time_range(period = period2)[[1]], origin = "1970-01-01")
  date_id <- seq(0, length(date) - 1)
  dates <- data.frame(date_id, date)
  
  
  # Make more readable enrollment types
  if (medicaid == T & kcha == T) {
    type <- c("SS: M", "SS: no M",
              "YT: M", "YT: no M",
              "SHA: M", "SHA: no M",
              "KCHA", "M only",
              "Not enrolled")
    group <- c(1, 1, 2, 2, 3, 3, 4, 5, 6)
  } else if (medicaid == T & kcha == F) {
    type <- c("SS: M", "SS: no M",
              "YT: M", "YT: no M",
              "SHA: M", "SHA: no M",
              "M only",
              "Not enrolled")
    group <- c(1, 1, 2, 2, 3, 3, 4, 5)
  } else if (medicaid == F & kcha == T) {
    type <- c("SS: M", "SS: no M",
              "YT: M", "YT: no M",
              "SHA: M", "SHA: no M",
              "KCHA",
              "Not enrolled")
    group <- c(1, 1, 2, 2, 3, 3, 4, 5)
  } else if (medicaid == F & kcha == F) {
    type <- c("SS: M", "SS: no M",
              "YT: M", "YT: no M",
              "SHA: M", "SHA: no M",
              "Not enrolled")
    group <- c(1, 1, 2, 2, 3, 3, 4)
  }
  
  types <- data.frame(place_new = seq(1, length(type)), group, type) %>%
    mutate(type = as.character(type))
  
  # Use joint nodes for more efficient naming
  nodes <- expand.grid(date, type) %>%
    rename(date = Var1, type = Var2) %>%
    mutate(type = as.character(type)) %>%
    left_join(., dates, by = "date") %>%
    left_join(., types, by = "type") %>%
    arrange(date, place_new)
  
  nodes <- nodes %>% 
    mutate(id = seq(0, nrow(nodes) - 1), 
           combo = paste0(format(date, "%y-%m"), ": ", type))
  
  # Expand out so each person has a row per time point
  unique_ids <- distinct(movement, pid2) %>% 
    slice(rep(1:n(), each = length(date)))
  
  full_frame <- data.frame(date = rep(date, as.integer(summarise(movement, n_distinct(pid2)))),
                           pid2 = unique_ids)
  
  # Join all together
  movement2 <- left_join(full_frame, movement, by = c("date", "pid2")) %>%
    mutate(place_new = ifelse(is.na(place_new), 9, place_new))
  movement2 <- left_join(movement2, dates, by = "date")
  movement2 <- left_join(movement2, nodes, by = c("date_id", "date", "place_new"))
  
  # Make summary version
  movement_sum <- movement2 %>%
    arrange(pid2, date) %>%
    mutate(source = id,
           target = ifelse(pid2 != lead(pid2, 1) | is.na(lead(pid2, 1)), NA, lead(id, 1)),
           target_date = ifelse(pid2 != lead(pid2, 1) | is.na(lead(pid2, 1)), NA, lead(date_id, 1)),
           target_group = ifelse(pid2 != lead(pid2, 1) | is.na(lead(pid2, 1)), NA, lead(group, 1))
    ) %>%
    filter(!is.na(target)) %>%
    group_by(combo, source, target, target_date, target_group) %>%
    summarise(value = n()) %>%
    ungroup() %>%
    arrange(source, target) %>%
    left_join(., nodes, by = "combo") %>%
    select(date, date_id, target_date, source, target, group, target_group, type, combo, place_new, value)
  
  return(movement_sum)
}

### Try annual status
movement <- period_place_f(yt_mcaid_final, period = "year", place = place,
                           enroll = enroll_type, medicaid = T, kcha = T)
movement <- movement %>% filter(!is.na(place_new)) %>% distinct(pid2, date, .keep_all = T)

# Get a source and target for everyone
date <- as.Date(time_range(period = "year")[[1]], origin = "1970-01-01")
date_id <- seq(0, length(date) - 1)
dates <- data.frame(date_id, date)



### Try quarterly status
movement <- period_place_f(yt_movement, period = "quarter", medicaid = F, kcha = F)
movement <- movement %>% filter(!is.na(place_new)) %>% distinct(pid2, date, .keep_all = T)

# Get a source and target for everyone
date <- as.Date(time_range(period = "quarter")[[1]], origin = "1970-01-01")
date_id <- seq(0, length(date) - 1)
dates <- data.frame(date_id, date)


### Try six-monthly status
movement <- period_place_f(filter(yt_movement, yt_ever == 1 | ss_ever == 1), period = "biannual", medicaid = T, kcha = T)
movement <- movement %>% filter(!is.na(place_new)) %>% distinct(pid2, date, .keep_all = T)

# Get a source and target for everyone
date <- as.Date(time_range(period = "biannual")[[1]], origin = "1970-01-01")
date_id <- seq(0, length(date) - 1)
dates <- data.frame(date_id, date)





movement_sum <- sankey_data_setup_f(df = yt_mcaid_final, period = "year",
                                    medicaid = T, kcha = T)





# Optional: filter rows that don't deal with YT/SS
movement_sum <- movement_sum %>% filter(group %in% c(1, 2) | target_group %in% c(1, 2))
# Make sure data are in data frame
movement_sum <- as.data.frame(movement_sum)

# Only keep nodes that are in final data (not working for some reason)
nodes2 <- bind_rows(distinct(movement_sum, source) %>% rename(id = source), 
                    distinct(movement_sum, target) %>% rename(id = target)) %>%
  distinct() %>%
  left_join(., nodes, by = "id")


# View results
yt_moves_local <- sankeyNetwork(Links = movement_sum, Nodes = nodes, 
                                Source = "source", Target = "target", Value = "value", 
                                NodeID = "combo", NodeGroup = "type", units = "ppl", 
                                #iterations = 0,
                                height = 700, width = 1400,
                                fontSize = 14, nodeWidth = 50)

htmlwidgets::onRender(
  yt_moves_local,
  '
  function(el, x) {
    d3.selectAll(".node text").attr("text-anchor", "begin").attr("x", -20);
  }
  '
)

# Save results
yt_moves <- sankeyNetwork(Links = movement_sum, Nodes = nodes, 
              Source = "source", Target = "target", Value = "value", 
              NodeID = "combo", NodeGroup = "type", units = "ppl", 
              #iterations = 0,
              fontSize = 14, nodeWidth = 50)

saveNetwork(yt_moves, file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/SHA cleaning/movement patterns.html")



