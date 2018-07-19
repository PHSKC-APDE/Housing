#' Counting population and person-time in the joint PHA/Medicaid data
#' 
#' \code{popcount} summarizes population data in the joint PHA/Medicaid data.
#' 
#' This function takes the date range supplied by \code{\link{time_range}} and
#' counts individuals enrolled and person time by group.
#' The function works for PHA data regardless of whether or not it has also
#' been matched to Medicaid data.
#' 
#' @param df A data frame
#' @param group_var A set of variables to group counts by. Default is PH agency.
#' @param agency A named variable that specifies the agency a person is in for 
#' that period of time (usually KCHA, SHA, or NA/Medicaid only). Used to 
#' allocate individuals who moved between multiple agencies/enrollment types 
#' in the period. Default is agency_new.
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
#' @param  birth A named variable that determines which date of birth field to 
#' use when calculating age (normally dob_c or dob_h but can also use 
#' hh_dob_m6 if calculating age at the household level).
#' @param ... Additional arguments for the \code{\link{time_range}} function
#' (yearmin, yearmax, period, and date).
#' 
#' @examples
#' \dontrun{
#' counts(pha_longitudinal)
#' counts(pha_longitudinal, group_var = c("agency_new", "major_prog"),
#' agency = "kcha", unit = hhold_id_new)
#' counts(pha_longitudinal, yearmin = 2014, yearmax = 2016, period = "month")
#' }
#' 
#' @export


popcount <- function(df, 
                     group_var = c("agency_new"),
                     agency = NULL,
                     enroll = NULL,
                     unit = NULL,
                     startdate = NULL,
                     enddate = NULL,
                     birth = NULL,
                     ...) {
  
  
  # Warn about missing unit of analysis
  if (missing(unit)) {
    print("Attempting to use default unit of analysis (individuals (pid/pid2)). 
          Possible options: pid, pid2, hhold_id_new")
  }
  
  # Set up quosures and other variables
  grouping_vars <- rlang::syms(group_var)
  print(paste0("Grouping by: ", paste(group_var, collapse = ", ")))
  
  
  if(!missing(agency)) {
    agency <- enquo(agency)
  } else if("agency_new" %in% names(df)) {
    agency <- quo(agency_new)
  } else if("agency" %in% names(df)) {
    agency <- quo(agency)
  } else if("agency_num" %in% names(df)) {
    agency <- quo(agency)
  } else {
    stop("No valid agency field specified")
  }
  
  if(!missing(enroll)) {
    enroll <- enquo(enroll)
  } else if("enroll_type" %in% names(df)) {
    enroll <- quo(enroll_type)
  } else if("enrtype" %in% names(df)) {
    enroll <- quo(enrtype)
  } else if("enroll_type_num" %in% names(df)) {
    enroll <- quo(enrtype)
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
  
  # Figure out which DOB field to use (if needed for age calcs)
  if (str_detect(paste(grouping_vars, collapse = ""), "agegrp|adult|senior")) {
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
    
    df_temp <- df
    
    # Pull out heads of households if that is the level of analysis
    if (quo_name(unit) == "hhold_id_new") {
      df_temp <- df_temp %>% filter(mbr_num == 1)
    }
    
    
    # Recalculate age and age groups if they are one of the grouping variables
    if (str_detect(paste(grouping_vars, collapse = ""), "agegrp|adult|senior")) {
      df_temp <- df_temp %>% mutate(
        age_temp = 
          floor(interval(start = !!birth, end = timestart[i]) / years(1)),
        adult = ifelse(age_temp >= 18, 1, 0),
        senior = ifelse(age_temp >= 62, 1, 0),
        agegrp = ifelse(adult == 0 & !is.na(adult), "Youth",
                        ifelse(adult == 1 & senior == 0 & 
                                 !is.na(adult), "Working age",
                               ifelse(adult == 1 & senior == 1 & 
                                        !is.na(adult), "Senior", NA)))
      )
    }
    
    # Recalculate length of stay groups if they are in the grouping variables
    if (str_detect(paste(grouping_vars, collapse = ""), "time_housing")) {
      df_temp <- df_temp %>% mutate(
        time_housing_temp = 
          round(interval(start = start_housing, end = timeend[i]) / years(1), 1),
        time_housing = 
          car::recode(time_housing_temp, "0:1.99 = '<2 years'; 2:4.99 = '2 to <5 years';
                      5:hi = '5+ years'; else = NA")
          )
    }
    
    if (str_detect(paste(grouping_vars, collapse = ""), "time_pha")) {
      df_temp <- df_temp %>% mutate(
        time_pha_temp = 
          round(interval(start = start_pha, end = timeend[i]) / years(1), 1),
        time_pha = 
          car::recode(time_pha_temp, "0:1.99 = '<2 years'; 2:4.99 = '2 to <5 years';
                      5:hi = '5+ years'; else = NA")
          )
    }
    
    if (str_detect(paste(grouping_vars, collapse = ""), "time_prog")) {
      df_temp <- df_temp %>% mutate(
        time_prog_temp = 
          round(interval(start = start_prog, end = timeend[i]) / years(1), 1),
        time_prog = 
          car::recode(time_prog_temp, "0:1.99 = '<2 years'; 2:4.99 = '2 to <5 years';
                      5:hi = '5+ years'; else = NA")
          )
    }
    
    # Set up overlap between time period of interest and enrollment
    df_temp <- df_temp %>%
      mutate(
        overlap_amount = lubridate::intersect(
          lubridate::interval((!!start_var), (!!end_var)),
          lubridate::interval(timestart[i], timeend[i])) / ddays(1) + 1
        ) %>%
      # Remove any rows that don't overlap
      filter(!is.na(overlap_amount))

    # Count a person if they were in that group at any point in the period
    # Also count person time accrued in each group (in days)
    ever <- df_temp %>%
      group_by(!!!(grouping_vars)) %>%
      summarise(pop_ever = n_distinct(!!unit),
                pt_days = sum(overlap_amount)) %>%
      ungroup()

    # Allocate an individual to a PHA/program based on rules:
    # 1) Medicaid only and PHA only = Medicaid row with most time
    #   (rationale is we can look at the health data for Medicaid portion at least)
    # 2) Medicaid only and PHA/Medicaid = PHA group with most person-time where
    #    person was enrolled in both housing and Medicaid
    # 3) Multiple PHAs = PHA group with most person-time for EACH PHA where
    #    person was enrolled in both housing and Medicaid
    # 4) PHA only = group with most person-time (for one or more PHAs)
    # Note that this only allocates individuals, not person-time, which should
    # be allocated to each group in which it is accrued
    
    # Find the row with the most person-time in each group
    # (ties will be broken by whatever other ordering exists)
    pop <- df_temp %>%
      arrange((!!unit), (!!agency), (!!enroll), desc(overlap_amount)) %>%
      group_by((!!unit), (!!agency), (!!enroll)) %>%
      filter(row_number() == 1) %>%
      ungroup()
    
    # Number of agencies, should only be one row per possibility below
    pop <- pop %>%
      mutate(agency_count = case_when(
        toupper((!!agency)) == "KCHA" & (!!enroll) == "h" ~ 0,
        toupper((!!agency)) == "SHA" & (!!enroll) == "h" ~ 0,
        toupper((!!agency)) == "KCHA" & (!!enroll) == "b" ~ 1,
        toupper((!!agency)) == "SHA" & (!!enroll) == "b" ~ 2,
        (!!enroll) == "m" ~ 4
      )) %>%
      group_by((!!unit)) %>%
      mutate(agency_sum = sum(agency_count, na.rm = T)) %>%
      ungroup()
    

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
      group_by(!!!(grouping_vars)) %>%
      summarise(pop = n_distinct(!!unit)) %>%
      ungroup()

    
    # Join back to a single df
    templist[[i]] <- left_join(ever, pop, by = group_var) %>%
      mutate(total_pop = sum(pop, na.rm = T), date = timestart[i], 
             period = quo_name(period), unit = quo_name(unit))
  }
  
  phacount <- data.table::rbindlist(templist)
  return(phacount)
  }