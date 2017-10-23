#' @title Grouping function.
#' 
#' @description 
#' \code{group_vars} creates a grouped data frame for use in other functions.
#' 
#' @details 
#' This function creates a grouped data frame and is used in
#' the \code{\link{counts}} function. Not needed for general use
#' as the \pkg{dplyr} group_by command is more useful.
#' 
#' @param df A data frame
#' @param ... String arguments used to group df.
group_vars <- function(df, ...) {
  group_var <- quos(...)
  df %>% group_by(!!!group_var)
}