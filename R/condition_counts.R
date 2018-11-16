#' Count the number of events/population with condition by desired grouping
#' 
#' These functions count the number of acute events/people with conditions for
#' a given grouping and calendar year. It is currently specific to the joined
#' Medicaid/housing data sets but could be generalized to other data.
#' Can also be made to work with other time periods, but age and other demogs
#' would need to be recalculated in the function.
#' 
#' @param df A data frame containing health event/condition data.
#' @param year The calendar year (2 digits) of interest.
#' @param group_var A set of variables to group counts by. Default is PH agency.
#' Must be set as a quosure (use quos(<group1>, <group2>)). Use age_var and 
#' len_var to add age or length of time in housing to the grouping.
#' @param age_var Denotes the suffix to use when adding pre-calculated age 
#' fields to the grouping variables. Most likely to be '_grp'. Should be a 
#' string. Currently hard coded to attach to ageXX where XX is the year.
#' @param len_var Denotes the suffix to use when adding pre-calculated length 
#' fields to the grouping variables. Most likely to be '_grp'. Should be a 
#' string. Currently hard coded to attach to ageXX where XX is the year.
#' @param event The acute health event of interest. Should match a field name in
#' the df.
#' @param event_year The field that identifies which calendar year the event 
#' occurred in (acute events only).
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
#' 
#' @examples
#' \dontrun{
#' popcount(pha_longitudinal)
#' popcount(pha_longitudinal, group_var = quos(agency_new, major_prog),
#' agency = "kcha", unit = hhold_id_new)
#' popcount(pha_longitudinal, yearmin = 2014, yearmax = 2016, period = "month")
#' }
#' 
#' @name condition_counts
#' 
#' @export
#' @rdname condition_counts
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






#' @export
#' @rdname condition_counts
count_chronic <- function(df_chronic = chronic, df_pop = chronic_pop,
                                 condition = NULL, year = 12) {
  
  yr_chk <- as.numeric(paste0("20", year))
  condition_quo <- enquo(condition)
  
  agex_quo <- rlang::sym(paste0("age", quo_name(year), "_num"))
  lengthx_quo <- rlang::sym(paste0("length", quo_name(year), "_num"))
  
  year_start = as.Date(paste0("20", year, "-01-01"), origin = "1970-01-01")
  year_end = as.Date(paste0("20", year, "-12-31"), origin = "1970-01-01")
  
  # Filter to only include people with the condition in that year
  cond <- df_chronic %>%
    filter(!!condition_quo == 1 & from_date <= year_end & to_date >= year_start) %>%
    distinct(id, !!condition_quo)
  
  df_pop <- df_pop %>% filter(year == yr_chk)
  
  ### Join pop and condition data to summarise
  output <- left_join(df_pop, cond, by = c("mid" = "id")) %>%
    mutate(condition = if_else(is.na(!!condition_quo), 0, as.numeric(!!condition_quo))) %>%
    group_by(year, agency_num, enroll_type_num, dual_elig_num, age_group,
             gender_num, ethn_num, voucher_num, subsidy_num, operator_num,
             portfolio_num, length, zip_c) %>%
    summarise(count := sum(condition)) %>%
    ungroup() %>%
    select(year, agency_num, enroll_type_num, dual_elig_num, age_group, 
           gender_num, ethn_num, voucher_num, subsidy_num, operator_num, 
           portfolio_num, length, zip_c,
           count) %>%
    rename(agency = agency_num,
           enroll_type = enroll_type_num,
           dual = dual_elig_num,
           gender = gender_num,
           ethn = ethn_num,
           voucher = voucher_num,
           subsidy = subsidy_num,
           operator = operator_num,
           portfolio = portfolio_num,
           zip = zip_c)
  
  return(output)
}