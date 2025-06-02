#' Count acute health events by desired grouping
#' 
#' This function counts the number of acute health events for a given grouping 
#' and calendar year. It is currently specific to the joined Medicaid/housing 
#' data sets but could be generalized to other data. Can also be made to work 
#' with other time periods, but age and other demographics would need to be 
#' recalculated in the function.
#' 
#' @param df A data frame containing acute health event data.
#' @param year The calendar year (2 digits) of interest.
#' @param group_var A set of variables to group counts by. Default is PH agency.
#' Must be set as a quosure (use quos(<group1>, <group2>)). Use \code{age_var} and 
#' \code{len_var} to add age or length of time in housing to the grouping.
#' @param age_var Denotes the suffix to use when adding pre-calculated age 
#' fields to the grouping variables. Most likely to be \code{'_grp'}. Should be 
#' a string. Currently hard coded to attach to ageXX where XX is the year.
#' @param len_var Denotes the suffix to use when adding pre-calculated length 
#' fields to the grouping variables. Most likely to be \code{'_grp'}. Should be a 
#' string. Currently hard coded to attach to ageXX where XX is the year.
#' @param event The acute health event of interest. Should match a field name in
#' the df.
#' @param event_year The field that identifies the calendar year in which the 
#' event occurred.
#' @param unit A named variable that determines the unit to count over. 
#' Default unit of analysis is pid2 (individuals) but pid should be used with 
#' data not matched to Medicaid. The other option is hhold_id_new for households.
#' @param person A binary flag to indicator whether all acute events should be
#' counted or just one per person in the time period.
#' @param birth A named variable that determines which date of birth field to 
#' use when calculating age (normally dob_c or dob_h but can also use 
#' hh_dob_m6 if calculating age at the household level). Currently only needed 
#' if using a well-child indicator (could be extended to calculate age for a
#' non-calendar year period though).
#' 
#' @return A data frame with counts of acute events grouped by the specified 
#' variables.
#' 
#' @examples
#' \dontrun{
#' count_acute(pha_longitudinal, year = 16, event = emergency_visits)
#' count_acute(pha_longitudinal, year = 16, 
#'             group_var = quos(agency_new, major_prog),
#'             event = hospitalizations, unit = hhold_id_new)
#' count_acute(pha_longitudinal, year = 15, event = emergency_visits,
#'             age_var = "_grp", person = TRUE)
#' }
#' 
#' @import dplyr
#' @import stringr
#' 
#' @export
# Need to fix up joining quosures (agex)

count_acute <- function(df,
                       year,
                       group_var = quos(agency_new),
                       age_var = NULL,
                       len_var = NULL,
                       event = NULL,
                       event_year = event_year,
                       unit = NULL,
                       person = FALSE,
                       birth = NULL
                       ) {
  
  event_quo <- enquo(event)
  year_full <- as.numeric(paste0(20, year))
  event_year_quo <- enquo(event_year)
  
  # Figure out which unit of analysis variable to use
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
  
  # Figure out which DOB field to use (if needed for age calcs)
  # Required if looking at well-child age
  if (str_detect(quo_name(event_quo), "wc") == TRUE | 
      (!missing(birth) & !is.null(birth))) {
    
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
    
    df <- df %>%
      mutate(
        age_temp = floor(interval(start = !!birth, 
                                  end = as.Date(paste0(year_full, "-12-31"),
                                                origin = "1970-01-01"))
                         / years(1))
      ) %>%
      filter(!is.na(age_temp) & age_temp >= 3 & age_temp <= 6) %>%
      # Hard coding to match current housing/Medicaid data codes
      mutate(!!agex := 8)
  }
  
  # Add age and length year-specific variables to grouping id needed
  if (!missing(age_var) & !is.null(age_var)) {
    agex <- quo(!! rlang::sym(paste0("age", year, age_var)))
    # Make a new variable with the name we want to use in grouping
    df <- df %>%
      mutate(age_group = !!agex)
    # Add the age var to the grouping
    group_var <- append(group_var, quo(age_group))
  } 
  
  if (!missing(len_var) & !is.null(len_var)) {
    lengthx <- quo(!! rlang::sym(paste0("length", year, len_var)))
    # Make a new variable with the name we want to use in grouping
    df <- df %>%
      mutate(length = !!lengthx)
    # Add the length var to the grouping
    group_var <- append(group_var, quo(length))
  } 
  

  # Set up data frame to only include appropriate year
  acute <- df %>%
    filter((!!event_year_quo) == year_full | is.na((!!event_year_quo)))
  
  print(paste0("Grouping by: ", paste(group_var, collapse = ", ")))
  
  # Restrict to 1 event per person per grouping if desired
  if (person == TRUE) {
    acute <- acute %>%
      distinct(!!unit, !!!group_var, .keep_all = T)
  }
  
  acute <- acute %>%
    group_by(!!!group_var) %>%
    summarise(count = sum(!!event_quo)) %>%
    ungroup()
  
  # Track year
  acute <- acute %>% mutate(year = as.numeric(paste0(20, year))) %>%
    select(year, !!!group_var, count)

  return(acute)
}


