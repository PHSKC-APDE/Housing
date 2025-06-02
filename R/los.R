#' Sets up length of stay.
#' 
#' \code{los} creates a move in date for calculating time in public housing.
#' 
#' This function takes the cleaned up/combined PHA data and calculates a set
#' of move-in dates. These can be used to calculate the length of time in 
#' housing overall (if the person moved between KCHA and SHA), at the
#' PHA, and in a given program.
#' Relies on \code{\link{overlap}} and \code{\link{cumul_overlap}}
#' For now, it is assumed that the df has variables called pid, period, 
#' agency_new, and subsidy_type.
#' 
#' @param df A data frame
#' 
#' @examples 
#' \dontrun{
#' los(pha_longitudinal)
#' }
#' 
#' @importFrom data.table `:=`
#' @import dplyr
#' 
#' @export

los <- function(df){
   if(is.data.table(df)) {
    # Any time in SHA or KCHA
    df <- overlap(df, pid, period)
    df[, selector := 1:nrow(df) - cumul_overlap(overlap)]
    df[, start_housing := startdate[selector]]
    df[, ':=' (overlap = NULL, selector = NULL)]
    
    # Time in PHA
    df <- overlap(df, pid, period, agency_new)
    df[, selector := 1:nrow(df) - cumul_overlap(overlap)]
    df[, start_pha := startdate[selector]]
    df[, ':=' (overlap = NULL, selector = NULL)]
    
    # Time in program
    df <- overlap(df, pid, period, agency_new, subsidy_type)
    df[, selector := 1:nrow(df) - cumul_overlap(overlap)]
    df[, start_prog := startdate[selector]]
    df[, ':=' (overlap = NULL, selector = NULL)]
    
    return(df)
    
  } else {
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
    df <- overlap(df, pid, period, agency_new, subsidy_type)
    df <- df %>%
      mutate(
        selector = 1:nrow(.) - cumul_overlap(overlap), 
        start_prog = startdate[selector]) %>%
      select(-overlap, -selector)
    return(df)
  }
}