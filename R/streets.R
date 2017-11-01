#' Standardizes street names.
#' 
#' \code{streets} creates a move in date for calculating time in public housing.
#' 
#' This function applies standard USPS abbreviations to variables containing
#' street names
#' 
#' @param df A data frame
#' @param ... A set of variables names that need cleaning
#' 
#' @examples 
#' #' \dontrun{
#' los(pha_longitudinal, unit_add_new)
#' }
#' 
#' @export


# NOT COMPLETE< NEED TO WRITE FUNCTION BELOW (CURRENTLY LOS FUNCTION)

streets <- function(df){
  # Any time in SHA or KCHA
  df <- overlap(df, pid, period)
  df <- df %>%
    mutate(
      selector = 1:nrow(.) - cumul_overlap(overlap), 
      start_housing = startdate[selector]) %>%
    select(-overlap, -selector)
  
  # Time in PHA
  df <- overlap(df, pid, period, agency_new)
  df <- df %>%
    mutate(
      selector = 1:nrow(.) - cumul_overlap(overlap), 
      start_pha = startdate[selector]) %>%
    select(-overlap, -selector)
  
  # Time in program
  df <- overlap(df, pid, period, agency_new, major_prog)
  df <- df %>%
    mutate(
      selector = 1:nrow(.) - cumul_overlap(overlap), 
      start_prog = startdate[selector]) %>%
    select(-overlap, -selector)
  return(df)
}