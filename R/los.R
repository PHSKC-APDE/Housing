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
#' agency_new, and major_program.
#' 
#' @param df A data frame
#' 
#' @examples 
#' #' \dontrun{
#' los(pha_longitudinal)
#' }
#' 
#' @export

los <- function(df){
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