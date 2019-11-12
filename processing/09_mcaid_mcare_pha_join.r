###############################################################################
# OVERVIEW:
# Code to combine Medicaid-Medicare & Public housing data to create two tables
# Public housing includes King County Housing Authority and Seattle Housing Authority data
# elig_demo: contains time invariant data. Should be 1 row per unique individual.
# elig_timevar: contains time varying data. Multiple rows per unique individual.
#
# STEPS:
# 01 - Process raw KCHA data and load to SQL database
# 02 - Process raw SHA data and load to SQL database
# 03 - Bring in individual PHA datasets and combine into a single file
# 04 - Deduplicate data and tidy up via matching process
# 05 - Recode race and other demographics
# 06 - Clean up addresses
# 06a - Geocode addresses
# 07 - Consolidate data rows
# 08 - Add in final data elements and set up analyses
# 09 - Join with Medicaid-Medicare eligibility & time varying data ### (THIS CODE) ###
# 10 - Set up joint housing/Medicaid analyses
#
# Alastair Matheson (PHSKC-APDE) | Danny Colombara (PHSKC-APDE)
# alastair.matheson@kingcounty.gov | dcolombara@kingcounty.gov
# 2016-08-13, split into separate files 2017-10, rewritten 
# 2019-11-07, rewritten to created normalized mcaid_mcare_pha elig_demo & elig_timevar
# 
###############################################################################

##### !!!URGENT!!! Make sure the underlying data is up to date! #####
  # Ensure the master cross-walk table linking Medicaid, Medicare, and Public housing IDs is up to date
  # - code 1: https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/phclaims/stage/tables/load_stage.xwalk_apde_mcaid_mcare_pha.r
  # - code 2: https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/phclaims/final/tables/load_final.xwalk_apde_mcaid_mcare_pha.sql
  # - SQL: [PHClaims].[final].[xwalk_apde_mcaid_mcare_pha]
  
  # Ensure the joint Medicaid-Medicare elig_demo table is up to date
  # - code 1: https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/phclaims/stage/tables/load_stage.mcaid_mcare_elig_demo.R
  # - code 2: https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/phclaims/final/tables/load_final.mcaid_mcare_elig_demo.sql
  # - SQL: [PHClaims].[final].[mcaid_mcare_elig_demo]
  
  # Ensure the joing Medicaid-Medicare elig_timevar table is up to date
  # - code 1: https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/phclaims/stage/tables/load_stage.mcaid_mcare_elig_timevar.R
  # - code 2: https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/phclaims/final/tables/load_final.mcaid_mcare_elig_timevar.sql
  # - SQL: [PHClaims].[final].[mcaid_mcare_elig_timevar]


##### Set up global parameter and call in libraries #####
options(max.print = 350, tibble.print_max = 30, scipen = 999)

library(odbc) # Used to connect to SQL server
library(glue) # Used to put together SQL queries
library(lubridate) # Used to manipulate dates
library(tidyverse) # Used to manipulate data
library(data.table) # Used to manipulate data

kc.zips.url <- "https://raw.githubusercontent.com/PHSKC-APDE/reference-data/master/spatial_data/zip_admin.csv"

yaml.elig <- "https://raw.githubusercontent.com/PHSKC-APDE/Housing/master/processing/09_mcaid_mcare_pha_elig_demo.yaml"

yaml.timevar <- "https://raw.githubusercontent.com/PHSKC-APDE/Housing/master/processing/09_mcaid_mcare_pha_elig_timevar.yaml"

##### Connect to the servers #####
db_apde51 <- dbConnect(odbc(), "PH_APDEStore51")
db_claims51 <- dbConnect(odbc(), "PHClaims51")

##### ------------------ ####
##### Load data from SQL #####
  ### Medicaid-Medicare-Public Housing ID crosswalk ----
  xwalk <- setDT(odbc::dbGetQuery(db_claims51, "SELECT id_apde, pid FROM [PHClaims].[final].[xwalk_apde_mcaid_mcare_pha]"))
  
  ### Public Housing ----
  # use stage schema for now but switch to final once QA approach is sorted
  pha <- setDT(odbc::dbGetQuery(db_apde51, "SELECT 
        pid, start_housing, startdate, enddate, dob_m6, 
        gender_new_m6,
        race_new, r_aian_new, r_asian_new, r_black_new, r_nhpi_new, r_white_new, r_hisp_new,
        unit_zip_new, unit_city_new, unit_add_new, unit_apt, unit_apt2, 
        agency_new, operator_type, portfolio_final, subsidy_type, vouch_type_final
                                FROM [PH_APDEStore].[stage].[pha] WHERE enddate >= '2012-01-01'"))
  
  ### Joint Medicaid-Medicare elig_demo ----
  elig.mm <- setDT(odbc::dbGetQuery(db_claims51, "SELECT * FROM [PHClaims].[final].[mcaid_mcare_elig_demo]"))
  elig.mm[, c("last_run") := NULL]
  
  ### Joint Medicaid-Medicare elig_timevar ----
  timevar.mm <- setDT(odbc::dbGetQuery(db_claims51, "SELECT * FROM [PHClaims].[final].[mcaid_mcare_elig_timevar]"))
  timevar.mm[, c("contiguous", "last_run", "cov_time_day") := NULL]
  
##### Clean / prep housing data -----
# merge on id_apde
pha <- merge(pha, xwalk, by = "pid", all.x = TRUE, all.y = FALSE)
pha[, pid := NULL]
rm(xwalk)

# fix dates
date.vars <- c("start_housing", "from_date", "to_date", "dob")
setnames(pha, c("start_housing", "startdate", "enddate", "dob_m6"), date.vars)
pha[, c(date.vars) := lapply(.SD, as.Date), .SDcols = date.vars]
rm(date.vars)

# normalize gender
pha[gender_new_m6 == 1, gender_me := "Female"] 
pha[gender_new_m6 == 2, gender_me := "Male"] 
pha[, gender_female := 0][gender_me == "Female", gender_female := 1]
pha[, gender_male := 0][gender_me == "Male", gender_male := 1]
pha[, gender_new_m6 := NULL]

# identify most recent gender
setorder(pha, id_apde, -from_date) # sort to identify the most recent time per id
pha[, counter := 1:.N, by = c("id_apde")]
gender.recent <- pha[counter == 1, .(id_apde, gender_me)] # keep most recent gender
setnames(gender.recent, "gender_me", "gender_recent")
pha <- merge(pha, gender.recent, by = "id_apde", all.x = T, all.y = T)
pha[, counter := NULL]
rm(gender.recent)

# prep race
pha[, race_me := as.character(factor(race_new, 
                                        levels = c("AIAN only", "Asian only", "Black only", "Multiple race", "NHPI only", "White only", ""), 
                                        labels = c("AI/AN", "Asian", "Black", "Multiple", "NH/PI", "White", "Unknown")))]
pha[, race_new := NULL]

pha[, race_eth_me := race_me]
pha[r_hisp_new == 1, race_eth_me := "Latino"]

setnames(pha, 
         c("r_aian_new", "r_asian_new", "r_black_new", "r_nhpi_new", "r_white_new", "r_hisp_new"), 
         c("race_aian", "race_asian", "race_black", "race_nhpi", "race_white", "race_latino") )

# identify most recent race
setorder(pha, id_apde, -from_date) # sort to identify the most recent time per id
pha[, counter := 1:.N, by = c("id_apde")]
race.recent <- pha[counter == 1, .(id_apde, race_me, race_eth_me)] # keep most recent gender
setnames(race.recent, c("race_me", "race_eth_me"), c("race_recent", "race_eth_recent"))
pha <- merge(pha, race.recent, by = "id_apde", all.x = T, all.y = T)
pha[, counter := NULL]
rm(race.recent)

# ascribe ever KC residence status
pha[, geo_kc_ever := 1] # PHA data is always 1 because everyone lived or lives in either Seattle or King County Pubic Housing

##### Create Mcaid-Mcare-PHA elig_demo -----
  ### Create PHA elig_demo ----
  elig.pha <- copy(pha)
  setorder(elig.pha, id_apde, -from_date) # sort to identify the most recent time per id
  elig.pha[, counter := 1:.N, by = c("id_apde")]
  elig.pha <- elig.pha[counter == 1]
  elig.pha[, counter := NULL]
  elig.pha <- elig.pha[, .(id_apde, dob, geo_kc_ever, start_housing, gender_me, gender_recent, gender_female, gender_male, race_me, 
                           race_eth_me, race_recent, race_eth_recent, race_aian, race_asian, race_black, 
                           race_nhpi, race_white, race_latino)]

  ### Identify IDs in both Mcaid-Mcare & PHA and split from non-linked IDs ----
        linked.id <- intersect(elig.mm$id_apde, elig.pha$id_apde)
        
        elig.pha.solo <- elig.pha[!id_apde %in% linked.id]
        elig.mm.solo <- elig.mm[!id_apde %in% linked.id]  
        
        elig.pha.linked <- elig.pha[id_apde %in% linked.id]
        elig.mm.linked <- elig.mm[id_apde %in% linked.id]
        
  ### Combine the data for linked IDs ----
      # some data is assumed to be more reliable in one dataset compared to the other
      linked <- merge(x = elig.mm.linked, y = elig.pha.linked, by = "id_apde")
      setnames(linked, names(linked), gsub(".x$", ".elig.mm", names(linked))) # clean up suffixes to eliminate confusion
      setnames(linked, names(linked), gsub(".y$", ".elig.pha", names(linked))) # clean up suffixes to eliminate confusion
      
      # loop for vars that default to Mcaid-Mcare data
      for(i in c("dob", "gender_me", "gender_female", "gender_male", "gender_recent", "race_eth_recent", "race_recent",
                 "race_me", "race_eth_me", "race_aian", "race_asian", "race_black", "race_nhpi", "race_white", "race_latino")){
        linked[, paste0(i) := get(paste0(i, ".elig.mm"))] # default is to use Mcaid-Mcare data
        linked[is.na(get(paste0(i))), paste0(i) := get(paste0(i, ".elig.pha"))] # If NA b/c missing Mcaid-Mcare data, then fill with PHA data
        linked[, paste0(i, ".elig.mm") := NULL][, paste0(i, ".elig.pha") := NULL]
      }    
      
      # loop for vars that default to PHA data
      for(i in c("geo_kc_ever")){
        linked[, paste0(i) := get(paste0(i, ".elig.pha"))] # default is to use Mcaid-Mcare data
        linked[is.na(get(paste0(i))), paste0(i) := get(paste0(i, ".elig.mm"))] # If NA b/c missing Mcaid-Mcare data, then fill with PHA data
        linked[, paste0(i, ".elig.pha") := NULL][, paste0(i, ".elig.mm") := NULL]
      } 
  
      # add flag for triple linkage (Mcaid-Mcare-PHA)
      linked[, mcaid_mcare_pha := 1]
      
  ### Append the linked to the non-linked ----
      elig <- rbindlist(list(linked, elig.mm.solo, elig.pha.solo), use.names = TRUE, fill = TRUE)
      elig[is.na(mcaid_mcare_pha), mcaid_mcare_pha := 0] # fill in duals flag    
      
  ### Prep for pushing to SQL ----
      # recreate race unknown indicator
      elig[, race_unk := 0]
      elig[race_aian==0 & race_asian==0 & race_asian_pi==0 & race_black==0 & race_latino==0 & race_nhpi==0 & race_white==0, race_unk := 1] 
      
      # create time stamp
      elig[, last_run := Sys.time()]  
      
      # order columns
      setcolorder(elig, c("id_apde", "mcaid_mcare_pha", "apde_dual"))
      
      # clean objects no longer used
      rm(elig.mm, elig.mm.linked, elig.mm.solo, elig.pha, elig.pha.linked, elig.pha.solo, linked)
      
##### Create Mcaid-Mcare-PHA elig_timevar -----
  ### Create PHA elig_timevar ----
  timevar.pha <- copy(pha)
  timevar.pha <- timevar.pha[, .(id_apde, from_date, to_date, 
                                 unit_add_new, unit_apt, unit_apt2, unit_city_new, unit_zip_new, 
                                 agency_new, subsidy_type, vouch_type_final, operator_type,portfolio_final)]

  ### Identify IDs in both Mcaid-Mcare & PHA and split from non-linked IDs ----
      linked.id <- intersect(timevar.mm$id_apde, timevar.pha$id_apde)
      
      timevar.pha.solo <- timevar.pha[!id_apde %in% linked.id]
      timevar.mm.solo <- timevar.mm[!id_apde %in% linked.id]  
      
      timevar.pha.linked <- timevar.pha[id_apde %in% linked.id]
      timevar.mm.linked <- timevar.mm[id_apde %in% linked.id]
      
  ### Linked IDs Part 1: Create master list of time intervals by ID ----
      #-- Create all possible permutations of date interval combinations from mcaid_mcare and pha for each id ----
      linked <- merge(timevar.pha.linked[, .(id_apde, from_date, to_date)], timevar.mm.linked[, .(id_apde, from_date, to_date)], by = "id_apde", allow.cartesian = TRUE)
      setnames(linked, names(linked), gsub("\\.x$", ".elig.pha", names(linked))) # clean up suffixes to eliminate confusion
      setnames(linked, names(linked), gsub("\\.y$", ".elig.mm", names(linked))) # clean up suffixes to eliminate confusion
      
      #-- Identify the type of overlaps & number of duplicate rows needed ----
      temp <- linked %>%
        mutate(overlap_type = case_when(
          # First ID the non-matches
          is.na(from_date.elig.pha) | is.na(from_date.elig.mm) ~ 0,
          # Then figure out which overlapping date comes first
          # Exactly the same dates
          from_date.elig.pha == from_date.elig.mm & to_date.elig.pha == to_date.elig.mm ~ 1,
          # PHA before mcaid_mcare (or exactly the same dates)
          from_date.elig.pha <= from_date.elig.mm & from_date.elig.mm <= to_date.elig.pha & 
            to_date.elig.pha <= to_date.elig.mm ~ 2,
          # mcaid_mcare before PHA
          from_date.elig.mm <= from_date.elig.pha & from_date.elig.pha <= to_date.elig.mm & 
            to_date.elig.mm <= to_date.elig.pha ~ 3,
          # mcaid_mcare dates competely within PHA dates or vice versa
          from_date.elig.mm >= from_date.elig.pha & to_date.elig.mm <= to_date.elig.pha ~ 4,
          from_date.elig.pha >= from_date.elig.mm & to_date.elig.pha <= to_date.elig.mm ~ 5,
          # PHA coverage only before mcaid_mcare (or mcaid_mcare only after PHA)
          from_date.elig.pha < from_date.elig.mm & to_date.elig.pha < from_date.elig.mm ~ 6,
          # PHA coverage only after mcaid_mcare (or mcaid_mcare only before PHA)
          from_date.elig.pha > to_date.elig.mm & to_date.elig.pha > to_date.elig.mm ~ 7,
          # Anyone rows that are left
          TRUE ~ 8),
          # Calculate overlapping dates
          from_date_o = as.Date(case_when(
            overlap_type %in% c(1, 2, 4) ~ from_date.elig.mm,
            overlap_type %in% c(3, 5) ~ from_date.elig.pha), origin = "1970-01-01"),
          to_date_o = as.Date(ifelse(overlap_type %in% c(1:5),
                                     pmin(to_date.elig.mm, to_date.elig.pha),
                                     NA), origin = "1970-01-01"),
          # Need to duplicate rows to separate out non-overlapping PHA and mcaid_mcare periods
          repnum = case_when(
            overlap_type %in% c(2:5) ~ 3,
            overlap_type %in% c(6:7) ~ 2,
            TRUE ~ 1)
        ) %>%
        select(id_apde, from_date.elig.pha, to_date.elig.pha, from_date.elig.mm, to_date.elig.mm, 
               from_date_o, to_date_o, overlap_type, repnum) %>%
        arrange(id_apde, from_date.elig.pha, from_date.elig.mm, from_date_o, 
                to_date.elig.pha, to_date.elig.mm, to_date_o)
      
      #-- Expand out rows to separate out overlaps ----
      temp_ext <- temp[rep(seq(nrow(temp)), temp$repnum), 1:ncol(temp)]
      
      #-- Process the expanded data ----
      temp_ext <- temp_ext %>% 
        group_by(id_apde, from_date.elig.pha, to_date.elig.pha, from_date.elig.mm, to_date.elig.mm) %>% 
        mutate(rownum_temp = row_number()) %>%
        ungroup() %>%
        arrange(id_apde, from_date.elig.pha, to_date.elig.pha, from_date.elig.mm, to_date.elig.mm, from_date_o, 
                to_date_o, overlap_type, rownum_temp) %>%
        mutate(
          # Remove non-overlapping dates
          from_date.elig.pha = as.Date(ifelse((overlap_type == 6 & rownum_temp == 2) | 
                                                (overlap_type == 7 & rownum_temp == 1), 
                                              NA, from_date.elig.pha), origin = "1970-01-01"), 
          to_date.elig.pha = as.Date(ifelse((overlap_type == 6 & rownum_temp == 2) | 
                                              (overlap_type == 7 & rownum_temp == 1), 
                                            NA, to_date.elig.pha), origin = "1970-01-01"),
          from_date.elig.mm = as.Date(ifelse((overlap_type == 6 & rownum_temp == 1) | 
                                               (overlap_type == 7 & rownum_temp == 2), 
                                             NA, from_date.elig.mm), origin = "1970-01-01"), 
          to_date.elig.mm = as.Date(ifelse((overlap_type == 6 & rownum_temp == 1) | 
                                             (overlap_type == 7 & rownum_temp == 2), 
                                           NA, to_date.elig.mm), origin = "1970-01-01")) %>%
        distinct(id_apde, from_date.elig.pha, to_date.elig.pha, from_date.elig.mm, to_date.elig.mm, from_date_o, 
                 to_date_o, overlap_type, rownum_temp, .keep_all = TRUE) %>%
        # Remove first row if start dates are the same or PHA is only one day
        filter(!(overlap_type %in% c(2:5) & rownum_temp == 1 & 
                   (from_date.elig.pha == from_date.elig.mm | from_date.elig.pha == to_date.elig.pha))) %>%
        # Remove third row if to_dates are the same
        filter(!(overlap_type %in% c(2:5) & rownum_temp == 3 & to_date.elig.pha == to_date.elig.mm))
      
      #-- Calculate the finalized date columms----
      temp_ext <- temp_ext %>%
        # Set up combined dates
        mutate(
          # Start with rows with only PHA or mcaid_mcare, or when both sets of dates are identical
          from_date = as.Date(
            case_when(
              (!is.na(from_date.elig.pha) & is.na(from_date.elig.mm)) | overlap_type == 1 ~ from_date.elig.pha,
              !is.na(from_date.elig.mm) & is.na(from_date.elig.pha) ~ from_date.elig.mm), origin = "1970-01-01"),
          to_date = as.Date(
            case_when(
              (!is.na(to_date.elig.pha) & is.na(to_date.elig.mm)) | overlap_type == 1 ~ to_date.elig.pha,
              !is.na(to_date.elig.mm) & is.na(to_date.elig.pha) ~ to_date.elig.mm), origin = "1970-01-01"),
          # Now look at overlapping rows and rows completely contained within the other data's dates
          from_date = as.Date(
            case_when(
              overlap_type %in% c(2, 4) & rownum_temp == 1 ~ from_date.elig.pha,
              overlap_type %in% c(3, 5) & rownum_temp == 1 ~ from_date.elig.mm,
              overlap_type %in% c(2:5) & rownum_temp == 2 ~ from_date_o,
              overlap_type %in% c(2:5) & rownum_temp == 3 ~ to_date_o + 1,
              TRUE ~ from_date), origin = "1970-01-01"),
          to_date = as.Date(
            case_when(
              overlap_type %in% c(2:5) & rownum_temp == 1 ~ lead(from_date_o, 1) - 1,
              overlap_type %in% c(2:5) & rownum_temp == 2 ~ to_date_o,
              overlap_type %in% c(2, 5) & rownum_temp == 3 ~ to_date.elig.mm,
              overlap_type %in% c(3, 4) & rownum_temp == 3 ~ to_date.elig.pha,
              TRUE ~ to_date), origin = "1970-01-01"),
          # Deal with the last line for each person if it's part of an overlap
          from_date = as.Date(ifelse((id_apde != lead(id_apde, 1) | is.na(lead(id_apde, 1))) &
                                       overlap_type %in% c(2:5) & 
                                       to_date.elig.pha != to_date.elig.mm, 
                                     lag(to_date_o, 1) + 1, 
                                     from_date), origin = "1970-01-01"),
          to_date = as.Date(ifelse((id_apde != lead(id_apde, 1) | is.na(lead(id_apde, 1))) &
                                     overlap_type %in% c(2:5), 
                                   pmax(to_date.elig.pha, to_date.elig.mm, na.rm = TRUE), 
                                   to_date), origin = "1970-01-01")
        ) %>%
        arrange(id_apde, from_date, to_date, from_date.elig.pha, from_date.elig.mm, 
                to_date.elig.pha, to_date.elig.mm, overlap_type)
      
      #-- Label and clean summary interval data ----
      temp_ext <- temp_ext %>%
        mutate(
          # Identify which type of enrollment this row represents
          enroll_type = 
            case_when(
              (overlap_type == 2 & rownum_temp == 1) | 
                (overlap_type == 3 & rownum_temp == 3) |
                (overlap_type == 6 & rownum_temp == 1) | 
                (overlap_type == 7 & rownum_temp == 2) |
                (overlap_type == 4 & rownum_temp %in% c(1, 3)) |
                (overlap_type == 0 & is.na(from_date.elig.mm)) ~ "PHA",
              (overlap_type == 3 & rownum_temp == 1) | 
                (overlap_type == 2 & rownum_temp == 3) |
                (overlap_type == 6 & rownum_temp == 2) | 
                (overlap_type == 7 & rownum_temp == 1) | 
                (overlap_type == 5 & rownum_temp %in% c(1, 3)) |
                (overlap_type == 0 & is.na(from_date.elig.pha)) ~ "mcaid_mcare",
              overlap_type == 1 | (overlap_type %in% c(2:5) & rownum_temp == 2) ~ "both",
              TRUE ~ "x"
            ),
          # Drop rows from enroll_type == h/m when they are fully covered by an enroll_type == b
          drop = 
            case_when(
              id_apde == lag(id_apde, 1) & !is.na(lag(id_apde, 1)) & 
                from_date == lag(from_date, 1) & !is.na(lag(from_date, 1)) &
                to_date >= lag(to_date, 1) & !is.na(lag(to_date, 1)) & 
                # Fix up quirk from PHA data where two rows present for the same day
                !(lag(enroll_type, 1) != "mcaid_mcare" & lag(to_date.elig.pha, 1) == lag(from_date.elig.pha, 1)) &
                enroll_type != "both" ~ 1,
              id_apde == lead(id_apde, 1) & !is.na(lead(id_apde, 1)) & 
                from_date == lead(from_date, 1) & !is.na(lead(from_date, 1)) &
                to_date <= lead(to_date, 1) & !is.na(lead(to_date, 1)) & 
                # Fix up quirk from PHA data where two rows present for the same day
                !(lead(enroll_type, 1) != "mcaid_mcare" & lead(to_date.elig.pha, 1) == lead(from_date.elig.pha, 1)) &
                enroll_type != "both" & lead(enroll_type, 1) == "both" ~ 1,
              # Fix up other oddities when the date range is only one day
              id_apde == lag(id_apde, 1) & !is.na(lag(id_apde, 1)) & 
                from_date == lag(from_date, 1) & !is.na(lag(from_date, 1)) &
                from_date == to_date & !is.na(from_date) & 
                ((enroll_type == "mcaid_mcare" & lag(enroll_type, 1) %in% c("both", "PHA")) |
                   (enroll_type == "PHA" & lag(enroll_type, 1) %in% c("both", "mcaid_mcare"))) ~ 1,
              id_apde == lag(id_apde, 1) & !is.na(lag(id_apde, 1)) & 
                from_date == lag(from_date, 1) & !is.na(lag(from_date, 1)) &
                from_date == to_date & !is.na(from_date) &
                from_date.elig.pha == lag(from_date.elig.pha, 1) & to_date.elig.pha == lag(to_date.elig.pha, 1) &
                !is.na(from_date.elig.pha) & !is.na(lag(from_date.elig.pha, 1)) &
                enroll_type != "both" ~ 1,
              id_apde == lead(id_apde, 1) & !is.na(lead(id_apde, 1)) & 
                from_date == lead(from_date, 1) & !is.na(lead(from_date, 1)) &
                from_date == to_date & !is.na(from_date) &
                ((enroll_type == "mcaid_mcare" & lead(enroll_type, 1) %in% c("both", "PHA")) |
                   (enroll_type == "PHA" & lead(enroll_type, 1) %in% c("both", "mcaid_mcare"))) ~ 1,
              # Drop rows where the to_date < from_date due to 
              # both data sources' dates ending at the same time
              to_date < from_date ~ 1,
              TRUE ~ 0
            )
        ) %>%
        filter(drop == 0 | is.na(drop)) %>%
        # Truncate remaining overlapping end dates
        mutate(to_date = as.Date(
          ifelse(id_apde == lead(id_apde, 1) & !is.na(lead(from_date, 1)) &
                   from_date < lead(from_date, 1) &
                   to_date >= lead(to_date, 1),
                 lead(from_date, 1) - 1,
                 to_date),
          origin = "1970-01-01")
        ) %>%
        select(-drop, -repnum, -rownum_temp) %>%
        # With rows truncated, now additional rows with enroll_type == h/m that 
        # are fully covered by an enroll_type == b
        # Also catches single day rows that now have to_date < from_date
        mutate(
          drop = case_when(
            id_apde == lag(id_apde, 1) & from_date == lag(from_date, 1) &
              to_date == lag(to_date, 1) & lag(enroll_type, 1) == "both" & 
              enroll_type != "both" ~ 1,
            id_apde == lead(id_apde, 1) & from_date == lead(from_date, 1) &
              to_date <= lead(to_date, 1) & lead(enroll_type, 1) == "both" ~ 1,
            id_apde == lag(id_apde, 1) & from_date >= lag(from_date, 1) &
              to_date <= lag(to_date, 1) & enroll_type != "both" &
              lag(enroll_type, 1) == "both" ~ 1,
            id_apde == lead(id_apde, 1) & from_date >= lead(from_date, 1) &
              to_date <= lead(to_date, 1) & enroll_type != "both" &
              lead(enroll_type, 1) == "both" ~ 1,
            TRUE ~ 0)
        ) %>%
        filter(drop == 0 | is.na(drop)) %>%
        select(id_apde, from_date, to_date, enroll_type)
      
      linked <- setDT(copy(temp_ext))
      rm(temp, temp_ext)
      
  ### Linked IDs Part 2: join mcaid_mcare & PHA data based on ID & overlapping time periods ----
      # foverlaps ... https://github.com/Rdatatable/data.table/blob/master/man/foverlaps.Rd
      #-- structure data for use of foverlaps ----
      linked[, c("from_date", "to_date") := lapply(.SD, as.integer), .SDcols = c("from_date", "to_date")] 
      setkey(linked, id_apde, from_date, to_date)    
      
      timevar.pha.linked[, c("from_date", "to_date") := lapply(.SD, as.integer), .SDcols = c("from_date", "to_date")] 
      setkey(timevar.pha.linked, id_apde, from_date, to_date)
      
      timevar.mm.linked[, c("from_date", "to_date") := lapply(.SD, as.integer), .SDcols = c("from_date", "to_date")] 
      setkey(timevar.mm.linked, id_apde, from_date, to_date)
      
      #-- join on the mcaid_mcare linked data ----
      linked <- foverlaps(linked, timevar.mm.linked, type = "any", mult = "all")
      linked[, from_date := i.from_date] # the complete set of proper from_dates are in i.from_date
      linked[, to_date := i.to_date] # the complete set of proper to_dates are in i.to_date
      linked[, c("i.from_date", "i.to_date") := NULL] # no longer needed
      setkey(linked, id_apde, from_date, to_date)
      
      #-- join on the PHA linked data ----
      linked <- foverlaps(linked, timevar.pha.linked, type = "any", mult = "all")
      linked[, from_date := i.from_date] # the complete set of proper from_dates are in i.from_date
      linked[, to_date := i.to_date] # the complete set of proper to_dates are in i.to_date
      linked[, c("i.from_date", "i.to_date") := NULL] # no longer needed    
      
  ### Append linked and non-linked data ----
      linked[, c("from_date", "to_date") := lapply(.SD, as.Date, origin = "1970-01-01"), .SDcols = c("from_date", "to_date")]
      timevar <- rbindlist(list(linked, timevar.pha.solo, timevar.mm.solo), use.names = TRUE, fill = TRUE)
      setkey(timevar, id_apde, from_date) # order dual data     
      
  ### Collapse data if dates are contiguous and all data is the same ----
      #-- Create unique ID for data chunks ----
      timevar.vars <- setdiff(names(timevar), c("from_date", "to_date")) # all vars except date vars
      timevar[, group := .GRP, by = timevar.vars] # create group id
      timevar[, group := cumsum( c(0, diff(group)!=0) )] # in situation like a:a:a:b:b:b:b:a:a:a, want to distinguish first set of "a" from second set of "a"
      
      #-- Create unique ID for contiguous times within a given data chunk ----
      setkey(timevar, id_apde, from_date)
      timevar[, prev_to_date := c(NA, to_date[-.N]), by = "group"] # create row with the previous 'to_date', MUCH faster than the shift "lag" function in data.table
      timevar[, diff.prev := from_date - prev_to_date] # difference between from_date & prev_to_date will be 1 (day) if they are contiguous
      timevar[diff.prev != 1, diff.prev := NA] # set to NA if difference is not 1 day, i.e., it is not contiguous, i.e., it starts a new contiguous chunk
      timevar[is.na(diff.prev), contig.id := .I] # Give a unique number for each start of a new contiguous chunk (i.e., section starts with NA)
      setkey(timevar, group, from_date) # need to order the data so the following line will work.
      timevar[, contig.id  := contig.id[1], by=  .( group , cumsum(!is.na(contig.id))) ] # fill forward by group
      timevar[, c("prev_to_date", "diff.prev") := NULL] # drop columns that were just intermediates
      
      #-- Collapse rows where data chunks are constant and time is contiguous ----      
      timevar[, from_date := min(from_date), by = c("group", "contig.id")]
      timevar[, to_date := max(to_date), by = c("group", "contig.id")]
      timevar[, c("group", "contig.id") := NULL]
      timevar <- unique(timevar)
      
  ### Prep for pushing to SQL ----
      #-- Create program flags ----
      timevar[, mcare := 0][part_a==1 | part_b == 1 | part_c==1, mcare := 1]
      timevar[, mcaid := 0][!is.na(cov_type), mcaid := 1]
      timevar[, pha := 0][!is.na(agency_new), pha := 1]
      timevar[, apde_dual := 0][mcare == 1 & mcaid == 1, apde_dual := 1]
      timevar[, mcaid_mcare_pha := 0][mcaid == 1 & mcare==1 & pha == 1, mcaid_mcare_pha := 1]
      timevar[, enroll_type := NULL] # kept until now for comparison with the dual flag
      timevar <- timevar[!(mcare==0 & mcaid==0 & pha == 0)]
      
      #-- Create contiguous flag ----  
      # If contiguous with the PREVIOUS row, then it is marked as contiguous. This is the same as mcaid_elig_timevar
      timevar[, prev_to_date := c(NA, to_date[-.N]), by = "id_apde"] # MUCH faster than the shift "lag" function in data.table
      timevar[, contiguous := 0]
      timevar[from_date - prev_to_date == 1, contiguous := 1]
      timevar[, prev_to_date := NULL] # drop because no longer needed
      
      #-- Create cov_time_date ----
      timevar[, cov_time_day := as.integer(to_date - from_date)]
      
      #-- Select PHA address data over Mcaid-Mcare when available ----
      # street
      timevar[!is.na(unit_add_new), geo_add1 := unit_add_new][, unit_add_new := NULL]
      # apartment
      timevar[!is.na(unit_apt), geo_add2 := unit_apt][, c("unit_apt", "unit_apt2") := NULL]
      # city
      timevar[!is.na(unit_city_new ), geo_city := unit_city_new ][, unit_city_new  := NULL]
      # zip
      timevar[!is.na(unit_zip_new), geo_zip := unit_zip_new][, unit_zip_new := NULL]

      #-- Add KC flag based on zip code ----  
      kc.zips <- fread(kc.zips.url)
      timevar[, geo_kc := 0]
      timevar[geo_zip %in% unique(as.character(kc.zips$zip)), geo_kc := 1]
      rm(kc.zips)
      
      #-- create time stamp ----
      timevar[, last_run := Sys.time()]  
      
      #-- normalize pha variables ----
      setnames(timevar,
               c("agency_new", "operator_type", "portfolio_final", "subsidy_type", "vouch_type_final"),
               c("pha_agency", "pha_subsidy", "pha_voucher", "pha_type", "pha_portfolio"))

      #-- clean up ----
      rm(linked, pha, timevar.mm, timevar.mm.linked, timevar.mm.solo, timevar.pha, timevar.pha.linked, timevar.pha.solo)
      
##### Write elig_demo to SQL ----
  ### Write to SQL ----
  # Pull YAML from GitHub
  table_config <- yaml::yaml.load(RCurl::getURL(yaml.elig))
  
  # Create table ID
  tbl_id <- DBI::Id(schema = table_config$schema, 
                    table = table_config$table)  
  
  # Ensure columns are in same order in R & SQL
  setcolorder(elig, names(table_config$vars))
  
  # Write table to SQL
  dbWriteTable(db_apde51, 
               tbl_id, 
               value = as.data.frame(elig),
               overwrite = T, append = F, 
               field.types = unlist(table_config$vars))
  
  ### Simple QA ----
      #-- confirm that all rows were loaded to SQL ----
      stage.count <- as.numeric(odbc::dbGetQuery(db_apde51, "SELECT COUNT (*) FROM stage.mcaid_mcare_pha_elig_demo"))
      if(stage.count != nrow(elig))
        stop("Mismatching row count, error writing data")    
      
      #-- check that rows in stage are not less than the last time that it was created ----
      last_run <- as.POSIXct(odbc::dbGetQuery(db_apde51, "SELECT MAX (last_run) FROM stage.mcaid_mcare_pha_elig_demo")[[1]]) # data for the run that was just uploaded
      
      # count number of rows
      previous_rows <- as.numeric(
        odbc::dbGetQuery(db_apde51, 
                         "SELECT c.qa_value from
                             (SELECT a.* FROM
                             (SELECT * FROM metadata.qa_mcaid_mcare_pha_values
                             WHERE table_name = 'stage.mcaid_mcare_pha_elig_demo' AND
                             qa_item = 'row_count') a
                             INNER JOIN
                             (SELECT MAX(qa_date) AS max_date 
                             FROM metadata.qa_mcaid_mcare_pha_values
                             WHERE table_name = 'stage.mcaid_mcare_pha_elig_demo' AND
                             qa_item = 'row_count') b
                             ON a.qa_date = b.max_date)c"))
      
      if(is.na(previous_rows)){previous_rows = 0}
      
      row_diff <- stage.count - previous_rows
      
      if (row_diff < 0) {
        odbc::dbGetQuery(
          conn = db_apde51,
          glue::glue_sql("INSERT INTO metadata.qa_mcaid_mcare_pha
                             (last_run, table_name, qa_item, qa_result, qa_date, note) 
                             VALUES ({last_run}, 
                             'stage.mcaid_mcare_pha_elig_demo',
                             'Number new rows compared to most recent run', 
                             'FAIL', 
                             {Sys.time()}, 
                             'There were {row_diff} fewer rows in the most recent table 
                             ({stage.count} vs. {previous_rows})')",
                         .con = db_apde51))
        
        problem.elig.row_diff <- glue::glue("Fewer rows than found last time.  
                                           Check metadata.qa_mcaid_mcare_pha for details (last_run = {last_run})
                                           \n")
      } else {
        odbc::dbGetQuery(
          conn = db_apde51,
          glue::glue_sql("INSERT INTO metadata.qa_mcaid_mcare_pha
                             (last_run, table_name, qa_item, qa_result, qa_date, note) 
                             VALUES ({last_run}, 
                             'stage.mcaid_mcare_pha_elig_demo',
                             'Number new rows compared to most recent run', 
                             'PASS', 
                             {Sys.time()}, 
                             'There were {row_diff} more rows in the most recent table 
                             ({stage.count} vs. {previous_rows})')",
                         .con = db_apde51))
        
        problem.elig.row_diff <- glue::glue(" ") # no problem, so empty error message
        
      }
      
      #-- check that the number of distinct IDs not less than the last time that it was created ----
      # get count of unique id 
      current.unique.id <- as.numeric(odbc::dbGetQuery(
        db_apde51, "SELECT COUNT (DISTINCT id_apde) 
            FROM stage.mcaid_mcare_pha_elig_demo"))
      
      previous.unique.id <- as.numeric(
        odbc::dbGetQuery(db_apde51, 
                         "SELECT c.qa_value from
                             (SELECT a.* FROM
                             (SELECT * FROM metadata.qa_mcaid_mcare_pha_values
                             WHERE table_name = 'stage.mcaid_mcare_pha_elig_demo' AND
                             qa_item = 'id_count') a
                             INNER JOIN
                             (SELECT MAX(qa_date) AS max_date 
                             FROM metadata.qa_mcaid_mcare_pha_values
                             WHERE table_name = 'stage.mcaid_mcare_pha_elig_demo' AND
                             qa_item = 'id_count') b
                             ON a.qa_date = b.max_date)c"))
      
      if(is.na(previous.unique.id)){previous.unique.id = 0}
      
      id_diff <- current.unique.id - previous.unique.id
      
      if (id_diff < 0) {
        odbc::dbGetQuery(
          conn = db_apde51,
          glue::glue_sql("INSERT INTO metadata.qa_mcaid_mcare_pha
                             (last_run, table_name, qa_item, qa_result, qa_date, note) 
                             VALUES ({last_run}, 
                             'stage.mcaid_mcare_pha_elig_demo',
                             'Number distinct IDs compared to most recent run', 
                             'FAIL', 
                             {Sys.time()}, 
                             'There were {id_diff} fewer IDs in the most recent table 
                             ({current.unique.id} vs. {previous.unique.id})')",
                         .con = db_apde51))
        
        problem.elig.id_diff <- glue::glue("Fewer unique IDs than found last time.  
                                           Check metadata.qa_mcaid_mcare_pha for details (last_run = {last_run})
                                           \n")
      } else {
        odbc::dbGetQuery(
          conn = db_apde51,
          glue::glue_sql("INSERT INTO metadata.qa_mcaid_mcare_pha
                             (last_run, table_name, qa_item, qa_result, qa_date, note) 
                             VALUES ({last_run}, 
                             'stage.mcaid_mcare_pha_elig_demo',
                             'Number distinct IDs compared to most recent run', 
                             'PASS', 
                             {Sys.time()}, 
                             'There were {id_diff} more IDs in the most recent table 
                             ({current.unique.id} vs. {previous.unique.id})')",
                         .con = db_apde51))
        
        problem.elig.id_diff <- glue::glue(" ") # no problem, so empty error message
      }
      
  ### Fill qa_mcare_values table ----
  qa.values <- glue::glue_sql("INSERT INTO metadata.qa_mcaid_mcare_pha_values
                                (table_name, qa_item, qa_value, qa_date, note) 
                                VALUES ('stage.mcaid_mcare_pha_elig_demo',
                                'row_count', 
                                {stage.count}, 
                                {Sys.time()}, 
                                '')",
                              .con = db_apde51)
  
  odbc::dbGetQuery(conn = db_apde51, qa.values)
  
  qa.values2 <- glue::glue_sql("INSERT INTO metadata.qa_mcaid_mcare_pha_values
                                (table_name, qa_item, qa_value, qa_date, note) 
                                VALUES ('stage.mcaid_mcare_pha_elig_demo',
                                'id_count', 
                                {current.unique.id}, 
                                {Sys.time()}, 
                                '')",
                               .con = db_apde51)
  
  odbc::dbGetQuery(conn = db_apde51, qa.values2)
  
##### Write elig_timevar to SQL ----
  ### Write to SQL ----
  # Pull YAML from GitHub
  table_config <- yaml::yaml.load(RCurl::getURL(yaml.timevar))
  
  # Create table ID
  tbl_id <- DBI::Id(schema = table_config$schema, 
                    table = table_config$table)  
  
  # Ensure columns are in same order in R & SQL
  setcolorder(timevar, names(table_config$vars))
  
  # Write table to SQL
  dbWriteTable(db_apde51, 
               tbl_id, 
               value = as.data.frame(timevar),
               overwrite = T, append = F, 
               field.types = unlist(table_config$vars))
  
  ### Simple QA ----
      #-- confirm that all rows were loaded to SQL ----
      stage.count <- as.numeric(odbc::dbGetQuery(db_apde51, "SELECT COUNT (*) FROM stage.mcaid_mcare_pha_elig_timevar"))
      if(stage.count != nrow(timevar))
        stop("Mismatching row count, error writing data")    
      
      #-- check that rows in stage are not less than the last time that it was created ----
      last_run <- as.POSIXct(odbc::dbGetQuery(db_apde51, "SELECT MAX (last_run) FROM stage.mcaid_mcare_pha_elig_timevar")[[1]]) # data for the run that was just uploaded
      
      # count number of rows
      previous_rows <- as.numeric(
        odbc::dbGetQuery(db_apde51, 
                         "SELECT c.qa_value from
                         (SELECT a.* FROM
                         (SELECT * FROM metadata.qa_mcaid_mcare_pha_values
                         WHERE table_name = 'stage.mcaid_mcare_pha_elig_timevar' AND
                         qa_item = 'row_count') a
                         INNER JOIN
                         (SELECT MAX(qa_date) AS max_date 
                         FROM metadata.qa_mcaid_mcare_pha_values
                         WHERE table_name = 'stage.mcaid_mcare_pha_elig_timevar' AND
                         qa_item = 'row_count') b
                         ON a.qa_date = b.max_date)c"))
      
      if(is.na(previous_rows)){previous_rows = 0}
      
      row_diff <- stage.count - previous_rows
      
      if (row_diff < 0) {
        odbc::dbGetQuery(
          conn = db_apde51,
          glue::glue_sql("INSERT INTO metadata.qa_mcaid_mcare_pha
                         (last_run, table_name, qa_item, qa_result, qa_date, note) 
                         VALUES ({last_run}, 
                         'stage.mcaid_mcare_pha_elig_timevar',
                         'Number new rows compared to most recent run', 
                         'FAIL', 
                         {Sys.time()}, 
                         'There were {row_diff} fewer rows in the most recent table 
                         ({stage.count} vs. {previous_rows})')",
                         .con = db_apde51))
        
        problem.timevar.row_diff <- glue::glue("Fewer rows than found last time.  
                                       Check metadata.qa_mcaid_mcare_pha for details (last_run = {last_run})
                                       \n")
      } else {
        odbc::dbGetQuery(
          conn = db_apde51,
          glue::glue_sql("INSERT INTO metadata.qa_mcaid_mcare_pha
                         (last_run, table_name, qa_item, qa_result, qa_date, note) 
                         VALUES ({last_run}, 
                         'stage.mcaid_mcare_pha_elig_timevar',
                         'Number new rows compared to most recent run', 
                         'PASS', 
                         {Sys.time()}, 
                         'There were {row_diff} more rows in the most recent table 
                         ({stage.count} vs. {previous_rows})')",
                         .con = db_apde51))
        
        problem.timevar.row_diff <- glue::glue(" ") # no problem, so empty error message
        
      }
      
      #-- check that the number of distinct IDs not less than the last time that it was created ----
      # get count of unique id 
      current.unique.id <- as.numeric(odbc::dbGetQuery(
        db_apde51, "SELECT COUNT (DISTINCT id_apde) 
        FROM stage.mcaid_mcare_pha_elig_timevar"))
      
      previous.unique.id <- as.numeric(
        odbc::dbGetQuery(db_apde51, 
                         "SELECT c.qa_value from
                         (SELECT a.* FROM
                         (SELECT * FROM metadata.qa_mcaid_mcare_pha_values
                         WHERE table_name = 'stage.mcaid_mcare_pha_elig_timevar' AND
                         qa_item = 'id_count') a
                         INNER JOIN
                         (SELECT MAX(qa_date) AS max_date 
                         FROM metadata.qa_mcaid_mcare_pha_values
                         WHERE table_name = 'stage.mcaid_mcare_pha_elig_timevar' AND
                         qa_item = 'id_count') b
                         ON a.qa_date = b.max_date)c"))
      
      if(is.na(previous.unique.id)){previous.unique.id = 0}
      
      id_diff <- current.unique.id - previous.unique.id
      
      if (id_diff < 0) {
        odbc::dbGetQuery(
          conn = db_apde51,
          glue::glue_sql("INSERT INTO metadata.qa_mcaid_mcare_pha
                         (last_run, table_name, qa_item, qa_result, qa_date, note) 
                         VALUES ({last_run}, 
                         'stage.mcaid_mcare_pha_elig_timevar',
                         'Number distinct IDs compared to most recent run', 
                         'FAIL', 
                         {Sys.time()}, 
                         'There were {id_diff} fewer IDs in the most recent table 
                         ({current.unique.id} vs. {previous.unique.id})')",
                         .con = db_apde51))
        
        problem.timevar.id_diff <- glue::glue("Fewer unique IDs than found last time.  
                                       Check metadata.qa_mcaid_mcare_pha for details (last_run = {last_run})
                                       \n")
      } else {
        odbc::dbGetQuery(
          conn = db_apde51,
          glue::glue_sql("INSERT INTO metadata.qa_mcaid_mcare_pha
                         (last_run, table_name, qa_item, qa_result, qa_date, note) 
                         VALUES ({last_run}, 
                         'stage.mcaid_mcare_pha_elig_timevar',
                         'Number distinct IDs compared to most recent run', 
                         'PASS', 
                         {Sys.time()}, 
                         'There were {id_diff} more IDs in the most recent table 
                         ({current.unique.id} vs. {previous.unique.id})')",
                         .con = db_apde51))
        
        problem.timevar.id_diff <- glue::glue(" ") # no problem, so empty error message
      }
      
  ### Fill qa_mcare_values table ----
  qa.values <- glue::glue_sql("INSERT INTO metadata.qa_mcaid_mcare_pha_values
                            (table_name, qa_item, qa_value, qa_date, note) 
                            VALUES ('stage.mcaid_mcare_pha_elig_timevar',
                            'row_count', 
                            {stage.count}, 
                            {Sys.time()}, 
                            '')",
                              .con = db_apde51)
  
  odbc::dbGetQuery(conn = db_apde51, qa.values)
  
  qa.values2 <- glue::glue_sql("INSERT INTO metadata.qa_mcaid_mcare_pha_values
                            (table_name, qa_item, qa_value, qa_date, note) 
                            VALUES ('stage.mcaid_mcare_pha_elig_timevar',
                            'id_count', 
                            {current.unique.id}, 
                            {Sys.time()}, 
                            '')",
                               .con = db_apde51)
  
  odbc::dbGetQuery(conn = db_apde51, qa.values2)
        
### Print error messages ----
  #-- create summary of errors
  problems <- glue::glue(
    problem.timevar.row_diff, "\n",
    problem.timevar.id_diff, "\n",
    problem.elig.row_diff, "\n",
    problem.elig.id_diff
  )
  
  if(problems >1){
    message(glue::glue("WARNING ... MCAID_MCARE_PHA_ELIG_TIMEVAR OR ELIG_DEMO FAILED AT LEAST ONE QA TEST", "\n",
                       "Summary of problems in new tables: ", "\n", 
                       problems))
  }else{message("Staged MCAID_MCARE_PHA_ELIG_TIMEVAR & ELIG_DEMO passed all QA tests")}
  
# the end ----  
