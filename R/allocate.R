#' Allocate people in the joint PHA/Medicaid/Medicare data
#' 
#' \code{allocate} summarizes population data in the joint PHA/Medicaid/Medicare data.
#' 
#' This function allocates people within a specified time range to a set of 
#' housing and Medicaid/Medicare groups based on a hierarchy of rules. Useful 
#' for examining demographic distributions across PHA groups or creating 
#' population denominators #' for chronic conditions.
#' The function works for PHA data regardless of whether or not it has also
#' been matched to Medicaid/Medicare data.
#' 
#' @param df A data frame
#' @param starttime Start date for the period of interest. Required. Use YYYY-MM-DD.
#' @param endtime End date for the period of interest. Required. Use YYYY-MM-DD.
#' @param agency A named variable that specifies the agency a person is in for 
#' that period of time (must be KCHA, SHA, or NA). Used to 
#' allocate individuals who moved between multiple agencies/enrollment types 
#' in the period. Required, default is pha_agency.
#' @param enroll A named variable that specifies the type of enrollment a person
#' is in for that period of time (field should contain the following codes: 
#' "h" = housing only, "md" = Medicaid only, "me" = Medicare only ,
#' "hmd" = housing and Medicaid, "hme" = housing and Medicare,
#' "mm" = Medicaid and Medicare (dual eligible), "a" = all three.
#' Used to allocate individuals who moved between multiple agencies/enrollment 
#' types in the period. Default is enroll_type.
#' @param unit A named variable that determines the unit of analysis. 
#' Default is id_apde (individuals) but pid should be used with unmatched PHA 
#' data. Other options include hhold_id_new for households.
#' @param from_date A string that specifies the variable name for the time an
#' individual or household starts at that address/program. Default is set for
#' the linked PHA/Medicaid data (from_date) but if that is not present then
#' the next default is startdate.
#' @param to_date A string that specifies the variable name for the time an
#' individual or household ends participation at that address/program. 
#' Default is set for the linked PHA/Medicaid data (to_date) but if that is 
#' not present then the next default is enddate.
#' @param ... A set of variables to allocate over (do not include agency or 
#' enroll vars).
#' 
#' @examples
#' \dontrun{
#' allocate(mcaid_mcare_pha_elig_timevar, starttime = "2015-01-01", 
#' endtime = "2015-12-31", enroll = enroll_type, unit = id_apde, 
#' from_date = from_date, o_date = to_date, pha_agency, pha_subsidy)
#' }
#' 
#' @export


allocate <- function(df,
                     starttime = NULL,
                     endtime = NULL,
                     agency = NULL,
                     enroll = NULL,
                     unit = NULL,
                     from_date = NULL,
                     to_date = NULL,
                     ...) {
  
  
  # Start and end dates
  starttime <- as.Date(starttime)
  endtime <- as.Date(endtime)
  
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
  
  # Enrollment
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
  
  # Unit
  # Warn about missing unit of analysis
  if (missing(unit)) {
    print("Attempting to use default unit of analysis (individuals (pid/id_apde)). 
          Possible options: pid, id_apde, hhold_id_new")
  }
  
  if(!missing(unit)) {
    unit <- enquo(unit)
  } else if("id_apde" %in% names(df)) {
    unit <- quo(id_apde)
  } else if("pid" %in% names(df)) {
    unit <- quo(pid)
  } else if("hhold_id_new" %in% names(df)) {
    unit <- quo(hhold_id_new)
  } else {
    stop("No valid unit of analysis found")
  }
  
  # Date variables
  if(!missing(from_date)) {
    start_var <- enquo(from_date)
  } else if("from_date" %in% names(df)) {
    start_var <- quo(from_date)
  } else if("startdate" %in% names(df)) {
    start_var <- quo(startdate)
  } else {
    stop("No valid startdate found")
  }
  
  if(!missing(to_date)) {
    end_var <- enquo(to_date)
  } else if("to_date" %in% names(df)) {
    end_var <- quo(to_date)
  } else if ("enddate" %in% names(df)) {
    end_var <- quo(enddate)
  } else {
    stop("No valid enddate found")
  }
  
  
  #### Set up group vars ####
  group_var <- enquos(...)
  
  if (length(group_var) == 0) {
    warning("No grouping variables specified. Just using enrollment and agency")
  } else {
    message(paste0("Grouping by: ", paste(group_var, collapse = ", ")))
  }
 
  
  #### Also set up other variables that will be shown ####
  # Remainder of variables that are not used for grouping but need to be shown
  #    in the allocated rows
  # Show all possible groups (maybe ultimately make each variable a flag in this function)
  # Right now this only applies to the join Medicaid/Medicaid/PHA table
  group_var_all <- c("apde_dual", "part_a", "part_b", "part_c", "partial", "buy_in", 
                     "dual", "tpl", "bsp_group_cid", "full_benefit", "cov_type", 
                     "mco_id", "pha_agency", "pha_subsidy", "pha_voucher", 
                     "pha_operator", "pha_portfolio", "geo_add1", "geo_add2", 
                     "geo_city", "geo_state", "geo_zip", "geo_zip_centroid", 
                     "geo_street_centroid", "geo_county_code", "geo_tract_code", 
                     "geo_hra_code", "geo_school_code")
  
  # Exclude enrollment and agency vars also
  group_var_all <- group_var_all[is.na(match(group_var_all, quo_name(agency)))]
  group_var_all <- group_var_all[is.na(match(group_var_all, quo_name(enroll)))]
  
  # Keep the groups are even in this DF
  group_var_all <- group_var_all[group_var_all %in% names(df)]
  
  # Remove variables used for grouping
  group_var_all <- group_var_all[is.na(match(group_var_all, lapply(group_var, quo_name)))]
  
  
  #### Set up data frame ####
  # Pull out heads of households if that is the level of analysis
  if (quo_name(unit) == "hhold_id_new") {
    df <- df %>% dplyr::filter(mbr_num == 1)
  }
  
  # Set up overlap between time period of interest and enrollment
  df <- df %>%
    mutate(
      overlap_amount = as.numeric(lubridate::intersect(
        #time_int,
        lubridate::interval((!!start_var), (!!end_var)),
        lubridate::interval(starttime, endtime)) / ddays(1) + 1)
    ) %>%
    # Remove any rows that don't overlap
    dplyr::filter(!is.na(overlap_amount))
  
  
  #### Allocate to a group ####
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
  
  # Find the row with the most person-time in each agency and group
  # (ties will be broken by whatever other ordering exists)
  pt <- df %>%
    group_by(!!unit, !!enroll, !!agency, !!!(group_var)) %>%
    summarise(pt_allocate = sum(overlap_amount))
  
  # Also make a record of total time person was enrolled somewhere
  pt_tot <- df %>%
    group_by(!!unit) %>%
    summarise(pt_tot = sum(overlap_amount))
  
  # Join back to a single df and sort so largest time is first in the group
  if (length(group_var) == 0) {
    pop <- left_join(df, pt, by = c(quo_name(unit), quo_name(enroll), quo_name(agency))) %>%
      left_join(., pt_tot, by = c(quo_name(unit))) %>%
      arrange((!!unit), (!!enroll), (!!agency), desc(pt_allocate), desc(overlap_amount))
  } else {
    pop <- left_join(df, pt, by = c(quo_name(unit), quo_name(enroll), 
                                    quo_name(agency), sapply(group_var, quo_name))) %>%
      left_join(., pt_tot, by = c(quo_name(unit))) %>%
      arrange((!!unit), (!!enroll), (!!agency), desc(pt_allocate), desc(overlap_amount))
  }

  
  # Use data table approach to take first row in group
  # From https://stackoverflow.com/questions/34753050/data-table-select-first-n-rows-within-group
  pop <- pop %>%
    mutate(unit_norm = !!unit,
           # Use a collapsed enrollment for the purposes of selecting rows
           enroll_norm = case_when(
             !!enroll %in% c("hmd", "hme", "a", "b") ~ "hm",
             !!enroll %in% c("md", "me", "mm", "m") ~ "mm",
             TRUE ~ !!enroll),
           agency_norm = !!agency)
  
  pop <- setDT(pop)
  pop <- pop[pop[, .I[1], by = .(unit_norm, enroll_norm, agency_norm)]$V1]
  

  # Number of agencies, should only be one row per possibility below
  pop[, agency_count := NA_integer_]
  pop[agency_norm == "KCHA" & enroll_norm == "h", agency_count := 0L]
  pop[agency_norm == "SHA" & enroll_norm == "h", agency_count := 0L]
  pop[agency_norm == "KCHA" & enroll_norm == "hm", agency_count := 1L]
  pop[agency_norm == "SHA" & enroll_norm == "hm", agency_count := 2L]
  pop[enroll_norm == "mm", agency_count := 4L]
  pop[agency_norm == "KCHA" & enroll_norm == "h", agency_count := 0L]
  

  # Make sum of agency rows per ID
  pop[, agency_sum := sum(agency_count, na.rm = T), by = unit_norm]
  
  
  # Filter so only rows meeting the rules above are kept
  pop <- pop[(agency_count == 1 & agency_sum %in% c(1, 3, 5, 7)) | # KCHA and Medicaid/Medicare
               (agency_count == 2 & agency_sum %in% c(2, 3, 6, 7)) | # SHA and Medicaid/Medicare
               (agency_count == 4 & agency_sum %in% c(4, 8, 12)) |
               agency_sum == 0, ]
  
  # Add a column to indicate that the person was allocated to this group
  pop[, pop := 1L]

  # Remove junk columns or columns with no meaning
  pop <- pop %>%
    dplyr::select(-from_date, -to_date, -contiguous, -cov_time_day, -agency, -overlap_amount, -unit_norm, 
           -agency_norm, -agency_count, -agency_sum, -enroll_norm)
  
  return(pop)
}