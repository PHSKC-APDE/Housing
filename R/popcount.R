#' Counting population and person-time in the joint PHA/Medicaid/Medicare data
#' 
#' \code{popcount} summarizes population data in the joint PHA/Medicaid/Medicare data.
#' 
#' This function takes the date range supplied by \code{\link{time_range}} and
#' counts individuals enrolled and person time by group.
#' The function works for PHA data regardless of whether or not it has also
#' been matched to Medicaid/Medicare data.
#' 
#' @param df A data frame
#' @param group_var A set of variables to group counts by. Default is PH agency.
#' Must be set as a quosure (use quos(<group1>, <group2>))
#' @param agency A named variable that specifies the agency a person is in for 
#' that period of time (usually KCHA, SHA, or NA/Medicaid/Medicare only). Used to 
#' allocate individuals who moved between multiple agencies/enrollment types 
#' in the period. Default is pha_agency.
#' @param enroll A named variable that specifies the type of enrollment a person
#' is in for that period of time (field should contain the following codes: 
#' "m" = Medicaid only, "b" = housing and Medicaid, or "h" = housing only).
#' Used to allocate individuals who moved between 
#' multiple agencies/enrollment types in the period. Default is enroll_type.
#' @param unit A named variable that determines the unit to count over. 
#' Default unit of analysis is pid2 (individuals) but pid should be used with 
#' data not matched to Medicaid. The other option is hhold_id_new for households.
#' @param startdate A string that specifies the variable name for the time an
#' individual or household starts at that address/program. Default is set for
#' the linked PHA/Medicaid data (startdate_c) but if that is not present then
#' the next default is startdate.
#' @param enddate A string that specifies the variable name for the time an
#' individual or household ends participation at that address/program. 
#' Default is set for the linked PHA/Medicaid data (enddate_c) but if that is 
#' not present then the next default is enddate.
#' @param birth A named variable that determines which date of birth field to 
#' use when calculating age (normally dob_c or dob_h but can also use 
#' hh_dob_m6 if calculating age at the household level). Only needed if using an
#' age grouping or the well-child flag.
#' @param length A named variable that determines which start date to 
#' use when calculating length of time in housing (normally start_housing).
#' @param numeric Determine whether to recode age and length of time groups to
#' be numeric or remain as character.
#' @param wc Choose whether to just run pop counts for the well-child indicator
#' age group (ages 3-6 years)
#' @param wc_min Minimum age (inclusive) for inclusion in the well child check indicator
#' @param wc_max Maximum age (inclusive) for inclusion in the well child check indicator
#' @param ... Additional arguments for the \code{\link{time_range}} function
#' (yearmin, yearmax, period, and date).
#' 
#' @examples
#' \dontrun{
#' popcount(pha_longitudinal)
#' popcount(pha_longitudinal, group_var = quos(agency_new, major_prog),
#' agency = "kcha", unit = hhold_id_new)
#' popcount(pha_longitudinal, yearmin = 2014, yearmax = 2016, period = "month")
#' }
#' 
#' @importFrom data.table setDT `:=`
#' @import dplyr
#' @import stringr
#' @export


popcount <- function(df,
                     group_var = quos(agency_new),
                     agency = NULL,
                     enroll = NULL,
                     unit = NULL,
                     startdate = NULL,
                     enddate = NULL,
                     birth = NULL,
                     length = NULL,
                     numeric = TRUE,
                     wc = FALSE,
                     wc_min = 0,
                     wc_max = 6,
                     ...) {
  
  
  # Warn about missing unit of analysis
  if (missing(unit)) {
    print("Attempting to use default unit of analysis (individuals (pid/id_apde)). 
          Possible options: pid, id_apde, hhold_id_new")
  }
  
  # Set up quosures and other variables
  if(!missing(agency)) {
    agency <- enquo(agency)
  } else if("pha_agency" %in% names(df)) {
    agency <- quo(agency_new)
  } else if("agency_new" %in% names(df)) {
    agency <- quo(agency_new)
  } else if("agency" %in% names(df)) {
    agency <- quo(agency)
  } else if("agency_num" %in% names(df)) {
    agency <- quo(agency_num)
  } else {
    stop("No valid agency field specified")
  }
  
  # Make agency upper case here to avoid repeating it later
  df <- df %>% mutate(agency := toupper(!!agency))
  
  if(!missing(enroll)) {
    enroll <- enquo(enroll)
  } else if("enroll_type" %in% names(df)) {
    enroll <- quo(enroll_type)
  } else if("enrtype" %in% names(df)) {
    enroll <- quo(enrtype)
  } else if("enroll_type_num" %in% names(df)) {
    enroll <- quo(enroll_type_num)
  } else {
    stop("No valid enrollment type field specified")
  }
  
  if(!missing(unit)) {
    unit <- enquo(unit)
  } else if("pid2" %in% names(df)) {
    unit <- quo(pid2)
  } else if("pid" %in% names(df)) {
    unit <- quo(pid)
  } else if("hhold_id_new" %in% names(df)) {
    unit <- quo(hhold_id_new)
  } else {
    stop("No valid unit of analysis found")
  }
  
  if(!missing(startdate)) {
    start_var <- enquo(startdate)
  } else if("startdate_c" %in% names(df)) {
    start_var <- quo(startdate_c)
  } else if("startdate" %in% names(df)) {
    start_var <- quo(startdate)
  } else {
    stop("No valid startdate found")
  }
  
  if(!missing(enddate)) {
    end_var <- enquo(enddate)
  } else if("enddate_c" %in% names(df)) {
    end_var <- quo(enddate_c)
  } else if ("enddate" %in% names(df)) {
    end_var <- quo(enddate)
  } else {
    stop("No valid enddate found")
  }
  
  
  # Allow new age and length of time variables to be created if they don't exist
  # Will be recalculated below
  if (str_detect(paste(group_var, collapse = ""), "agegrp") &
      max(str_detect(names(df), "agegrp")) == 0) {
    agegrp_h = NULL
  }
  
  if (str_detect(paste(group_var, collapse = ""), "time_") &
      max(str_detect(names(df), "time_")) == 0) {
    time_housing = NULL
    time_pha = NULL
    time_prog = NULL
  }
  
  
  print(paste0("Grouping by: ", paste(group_var, collapse = ", ")))
  
  # Figure out which DOB field to use (if needed for age calcs)
  # Required if looking at well-child age
  if (str_detect(paste(group_var, collapse = ""), "agegrp_h|adult|senior") |
      wc == TRUE) {
    if(!missing(birth)) {
      birth <- enquo(birth)
    } else if("dob_c" %in% names(df)) {
      birth <- quo(dob_c)
    } else if("dob_h" %in% names(df)) {
      birth <- quo(dob_h)
    } else if("dob_m6" %in% names(df)) {
      birth <- quo(dob_m6)
    } else if("hh_dob_m6" %in% names(df)) {
      birth <- quo(hh_dob_m6)
    } else {
      stop("No valid dob found")
    }
  }
  
  # Set up time period and capture period used for output
  timestart <- time_range(...)[[1]]
  timeend <- time_range(...)[[2]]
  period <- time_range(...)[[3]]
  
  ### Should convert this to an apply function at some point
  # Make empty list to add data to
  templist = list()
  
  for (i in 1:length(timestart)) {
    
    message(glue::glue("Working on {timestart[i]} to {timeend[i]}"))
    
    df_temp <- df
    
    # Pull out heads of households if that is the level of analysis
    if (quo_name(unit) == "hhold_id_new") {
      df_temp <- df_temp %>% filter(mbr_num == 1)
    }
    
    
    # Recalculate age and age groups if they are one of the grouping variables
    if (str_detect(paste(group_var, collapse = ""), "agegrp_h|adult|senior") &
        wc == FALSE) {
      df_temp <- df_temp %>% mutate(
        age_temp = floor(interval(start = !!birth, end = timeend[i]) / years(1)),
        adult = ifelse(age_temp >= 18, 1, 0),
        senior = ifelse(age_temp >= 62, 1, 0),
        agegrp_h = as.numeric(case_when(
          age_temp < 18 ~ 1,
          between(age_temp, 18, 24.99) ~ 2,
          between(age_temp, 25, 44.99) ~ 3,
          between(age_temp, 45, 61.99) ~ 4,
          between(age_temp, 62, 64.99) ~ 5,
          age_temp >= 65 ~ 6,
          is.na(age_temp) ~ 99
        )
        ))
    }
    
    if (wc == TRUE) {
      df_temp <- df_temp %>%
        mutate(
          age_temp = floor(interval(start = !!birth, end = timeend[i]) / years(1))
        ) %>%
        filter(!is.na(age_temp) & age_temp >= wc_min & age_temp <= wc_max) %>%
        mutate(agegrp_h = 8)
    }
    
    # Recalculate length of stay groups if they are in the grouping variables
    if (str_detect(paste(group_var, collapse = ""), "time_housing")) {
      df_temp <- df_temp %>% mutate(
        time_housing_temp = 
          round(interval(start = start_housing, end = timeend[i]) / years(1), 1),
        time_housing = as.numeric(case_when(
          time_housing_temp < 3 ~ 1,
          between(time_housing_temp, 3, 5.99) ~ 2,
          time_housing_temp >= 6 ~ 3,
          TRUE ~ 99
        )
        ))
    }
    
    if (str_detect(paste(group_var, collapse = ""), "time_pha")) {
      df_temp <- df_temp %>% mutate(
        time_pha_temp = 
          round(interval(start = start_pha, end = timeend[i]) / years(1), 1),
        time_pha = as.numeric(case_when(
          time_pha_temp < 3 ~ 1,
          between(time_pha_temp, 3, 5.99) ~ 2,
          time_pha_temp >= 6 ~ 3,
          TRUE ~ 99
        )
        ))
    }
    
    if (str_detect(paste(group_var, collapse = ""), "time_prog")) {
      df_temp <- df_temp %>% mutate(
        time_prog_temp = 
          round(interval(start = start_prog, end = timeend[i]) / years(1), 1),
        time_prog = as.numeric(case_when(
          time_prog_temp < 3 ~ 1,
          between(time_prog_temp, 3, 5.99) ~ 2,
          time_prog_temp >= 6 ~ 3,
          TRUE ~ 99
        )
        ))
    }
    
    
    # Set up overlap between time period of interest and enrollment
    df_temp <- df_temp %>%
      mutate(
        overlap_amount = as.numeric(lubridate::intersect(
          #time_int,
          lubridate::interval((!!start_var), (!!end_var)),
          lubridate::interval(timestart[i], timeend[i])) / ddays(1) + 1)
      ) %>%
      # Remove any rows that don't overlap
      filter(!is.na(overlap_amount))
    
    # Count a person if they were in that group at any point in the period
    # Also count person time accrued in each group (in days)
    ever <- df_temp %>%
      group_by(!!!(group_var)) %>%
      summarise(pop_ever = n_distinct(!!unit),
                pt_days = sum(overlap_amount)) %>%
      ungroup()
    
    # Allocate an individual to a PHA/program based on rules:
    # 1) Medicaid only/Medicare only and PHA only = Medicaid/Medicare row with most time
    #   (rationale is we can look at the health data for this portion at least)
    # 2) Medicaid/Medicare only and PHA + Medicaid/Medicare = PHA group with most 
    #    person-time where person was enrolled in both housing and Medicaid/Medicare
    # 3) Multiple PHAs = PHA group with most person-time for EACH PHA where
    #    person was enrolled in both housing and Medicaid/Medicare
    # 4) PHA only = group with most person-time (for one or more PHAs)
    # Note that this only allocates individuals, not person-time, which should
    # be allocated to each group in which it is accrued
    
    # Find the row with the most person-time in each group
    # (ties will be broken by whatever other ordering exists)
    pt <- df_temp %>%
      group_by(!!unit, !!!(group_var)) %>%
      summarise(pt_ind = sum(overlap_amount))

    # Join back to a single df
    pop <- left_join(df_temp, pt, by = c(quo_name(unit), sapply(group_var, quo_name))) %>%
      arrange((!!unit), (!!agency), (!!enroll), desc(pt_ind))
    
    # Use data table approach to take first row in group
    # From https://stackoverflow.com/questions/34753050/data-table-select-first-n-rows-within-group
    pop <- pop %>%
      mutate(unit_norm = !!unit,
             agency_norm = !!agency,
             enroll_norm = !!enroll)
    
    pop <- setDT(pop)
    pop <- pop[pop[, .I[1], by = list(unit_norm, agency_norm, enroll_norm)]$V1]
    
    
    # Number of agencies, should only be one row per possibility below
    if (is.character(pop$agency_norm) & is.character(pop$enroll_norm)) {
      pop <- pop %>%
        mutate(agency_count = case_when(
          agency_norm == "KCHA" & enroll_norm == "h" ~ 0,
          agency_norm == "SHA" & enroll_norm == "h" ~ 0,
          agency_norm == "KCHA" & enroll_norm == "b" ~ 1,
          agency_norm == "SHA" & enroll_norm == "b" ~ 2,
          enroll_norm == "m" ~ 4
        ))
    } else if (is.numeric(pop$agency_norm) & is.numeric(pop$enroll_norm)) {
      pop <- pop %>%
        mutate(agency_count = case_when(
          agency_norm == 1 & enroll_norm == 1 ~ 0,
          agency_norm == 2 & enroll_norm == 1 ~ 0,
          agency_norm == 1 & enroll_norm == 3 ~ 1,
          agency_norm == 2 & enroll_norm == 3 ~ 2,
          agency_norm == 0 ~ 4
        ))
    } else if (str_detect(str_to_lower(paste(group_var, collapse = "")), "agency") &
               str_detect(str_to_lower(paste(group_var, collapse = "")), "enroll")) {
      stop("Agency and enroll must be both numeric or character")
    }
    
    # Use data table for faster grouped operation then back to DF for group quosure
    pop <- setDT(pop)
    pop[, agency_sum := sum(agency_count, na.rm = T), by = unit_norm]
    pop <- as.data.frame(pop)
    
    
    # Filter so only rows meeting the rules above are kept
    pop <- pop %>%
      filter((agency_sum == 4 & agency_count == 4) | 
               (agency_sum == 5 & agency_count == 1) |
               (agency_sum == 6 & agency_count == 2) |
               (agency_sum == 7 & agency_count == 1) | 
               (agency_sum == 7 & agency_count == 2) |
               (agency_sum == 1 & agency_count == 1) |
               (agency_sum == 2 & agency_count == 2) |
               agency_sum == 3 |
               agency_sum == 0) %>%
      group_by(!!!(group_var)) %>%
      summarise(pop = n_distinct(!!unit)) %>%
      ungroup()
    
    
    # Join back to a single df
    templist[[i]] <- left_join(ever, pop, by = sapply(group_var, quo_name)) %>%
      mutate(date = timestart[i])
  }
  
  phacount <- bind_rows(templist)
  
  phacount <- phacount %>%
    mutate(pop = if_else(is.na(pop), 0, as.numeric(pop)),
           period = quo_name(period),
           unit = quo_name(unit),
           wc_flag = if_else(wc == TRUE, 1, 0))
  
  
  if(numeric == F) {
    if ("agegrp_h" %in% names(phacount)) {
      if (wc == FALSE) {
        phacount <- phacount %>%
          mutate(
            agegrp_h = case_when(
              agegrp_h == 1 ~ "<18 years",
              agegrp_h == 2 ~ "18 to <25 years",
              agegrp_h == 3 ~ "25 to <45 years",
              agegrp_h == 4 ~ "45 to <62 years",
              agegrp_h == 5 ~ "62 to <65 years",
              agegrp_h == 6 ~ "65+ years",
              agegrp_h == 99 ~ NA_character_
            )
          )
      } else if (wc == TRUE) {
        phacount <- phacount %>%
          mutate(agegrp_h == paste0("Children aged ", wc_min, "-", wc_max))
      }
    }
    
    
    if("time_housing" %in% names(phacount)) {
      phacount <- phacount %>%
        mutate(
          time_housing = case_when(
            time_housing == 1 ~ "<3 years",
            time_housing == 2 ~ "3 to <6 years",
            time_housing == 3 ~ "6+ years",
            time_housing == 99 ~ NA_character_
          )
        )
    }
    
    if("time_pha" %in% names(phacount)) {
      phacount <- phacount %>%
        mutate(
          time_pha = case_when(
            time_pha == 1 ~ "<3 years",
            time_pha == 2 ~ "3 to <6 years",
            time_pha == 3 ~ "6+ years",
            time_pha == 99 ~ NA_character_
          )
        )
    }
    
    if("time_prog" %in% names(phacount)) {
      phacount <- phacount %>%
        mutate(
          time_prog = case_when(
            time_prog == 1 ~ "<3 years",
            time_prog == 2 ~ "3 to <6 years",
            time_prog == 3 ~ "6+ years",
            time_prog == 99 ~ NA_character_
          )
        )
    }
  }
  
  return(phacount)
}