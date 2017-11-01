#' Counting enrollment in PHAs
#' 
#' \code{counts} summarizes the number of enrollees in the PHA data.
#' 
#' This function takes the date range supplied by \code{\link{time_range}} and
#' counts the number of individuals or households enrolled. Grouping options
#' are available and set using \code{\link{group_vars}}.
#' The function works for PHA data regardless of whether or not it has also
#' been matched to Medicaid data.
#' 
#' @param df A data frame
#' @param group_var A set of variables to group counts by. Default is PH agency.
#' @param agency A string that restricts analysis to KCHa, SHA or both. Default
#' is to count across both agencies (note that an individual or housheold will 
#' be counted only once if they appear in both agencies or >1 group for the
#' given time period). Not case sensitive.
#' @param unit A named variable that determines the unit to count over. 
#' Default unit of analysis is pid (individuals) but pid2 should be used with 
#' Medicaid-matched data. The other option is hhold_id_new for households.
#' @param filter A quosure that is a clunky way of adding the ability to add
#' extra filters. Default is missing. Addtional filters must be set up in a 
#' quo() syntax without quotation marks.
#' @param startdate A string that specifies the variable name for the time an
#' individual or household starts at that address/program. Default is set for
#' the linked PHA/Medicaid data (startdate_c) but if that is not present then
#' the next default is startdate.
#' @param enddate A string that specifies the variable name for the time an
#' individual or household ends participation at that address/program. 
#' Default is set for the linked PHA/Medicaid data (enddate_c) but if that is 
#' not present then the next default is enddate.
#' @param  birth A named variable that determines which date of birth field to 
#' use when calculating age (normally dob_h or dob_m6 but can also use hh_dob_m6
#' if calculating age at the household level).
#' @param ... Additional arguments for the \code{\link{time_range}} function
#' (yearmin, yearmax, period, and date).
#' 
#' @examples
#' #' \dontrun{
#' counts(pha_longitudinal)
#' counts(pha_longitudinal, group_var = c("agency_new", "major_prog"),
#' agency = "kcha", unit = hhold_id_new)
#' counts(pha_longitudinal, filter = quo(disability == 1))
#' }
#' 
#' @export

counts <- function(df, 
                   group_var = c("agency_new"), 
                   agency = "both", 
                   unit = NULL, 
                   filter = "",
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

  # Can't get the quosure to work properly in the filter below.
  # Using kludgy workaround for now but leaving code in for future work.
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
  if(!missing(birth)) {
    birth <- enquo(birth)
  } else if("dob_h" %in% names(df)) {
    birth <- quo(dob_h)
  } else if("dob_m6" %in% names(df)) {
    birth <- quo(dob_m6)
  } else if("hh_dob_m6" %in% names(df)) {
    birth <- quo(hh_dob_m6)
  } else {
    stop("No valid dob found")
  }
  
  # Set up time period and capture period used for output
  timestart <- time_range(...)[[1]]
  timeend <- time_range(...)[[2]]
  period <- time_range(...)[[3]]
  
  # Restrict to one or both PHAs
  if ((str_detect(tolower(agency), "kcha") & 
       str_detect(tolower(agency), "sha")) | tolower(agency) == "both") {
    agency_filter <- c("KCHA", "SHA")
  } else if (str_detect(tolower(agency), "kcha") & 
             str_detect(tolower(agency), "sha") == F) {
    agency_filter <- "KCHA"
  } else if (str_detect(tolower(agency), "kcha") == F & 
             str_detect(tolower(agency), "sha")) {
    agency_filter <- "SHA"
  } else {
    agency_filter <- "error with PHA selection"
    }
  print(paste0("Restricting to these PHAs: ", 
               paste(agency_filter, collapse = ", ")))
  
  ### Should convert this to an apply function at some point
  # Make empty list to add data to
  templist = list()
 
  for (i in 1:length(timestart)) {
    df_temp <- df %>%
      filter((!!start_var) <= as.Date(timeend[i]) &
               (!!end_var) >= as.Date(timestart[i]) &
               agency_new %in% agency_filter)
    
    if (quo_name(unit) == "hhold_id_new") {
      df_temp <- df_temp %>% filter(mbr_num == 1)
      }
    
    # Clunky way of adding ability to provide additional filters. 
    # Need to use quo(<filter conditions>) in the function call
    if(!missing(filter)) {
      df_temp <- df_temp %>% filter(!!filter)
    }
    
    # Limit to one record for that individual/household in the time period
    df_temp <- df_temp %>% distinct(!!unit, .keep_all = TRUE)
    
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
    
    # Recalculate length of stay groups if they are one of the grouping variables
    if (str_detect(paste(grouping_vars, collapse = ""), "time_")) {
      df_temp <- df_temp %>% mutate(
        time_housing_temp = 
          round(interval(start = start_housing, end = timeend[i]) / years(1), 1),
        time_housing = 
          car::recode(time_housing_temp, "0:1.99 = '<2 years'; 2:4.99 = '2 to <5 years';
                      5:hi = '5+ years'; else = NA"),
        time_pha_temp = 
          round(interval(start = start_pha, end = timeend[i]) / years(1), 1),
        time_pha = 
          car::recode(time_pha_temp, "0:1.99 = '<2 years'; 2:4.99 = '2 to <5 years';
                      5:hi = '5+ years'; else = NA"),
        time_prog_temp = 
          round(interval(start = start_prog, end = timeend[i]) / years(1), 1),
        time_prog = 
          car::recode(time_prog_temp, "0:1.99 = '<2 years'; 2:4.99 = '2 to <5 years';
                      5:hi = '5+ years'; else = NA")
      )
    }
      
    templist[[i]] <- group_vars(df_temp, !!!grouping_vars) %>%
      summarise(count = n_distinct(!!unit)) %>% 
      mutate(total = sum(.$count), date = timestart[i], 
             period = quo_name(period), unit = quo_name(unit))
    }
  phacount <- data.table::rbindlist(templist)
  return(phacount)
  }