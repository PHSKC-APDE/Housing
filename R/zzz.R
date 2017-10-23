#' @title Default options.
#' 
#' @description 
#' \code{apde_vars} sets default options for housing code specific to PHSKC use.
#' 
#' @details 
#' This function sets up the default options for working with the PHA data
#' while at PHSKC. Not needed for general use.
#' Also not working as desired. Setting global values in other R scripts for now

housing_default_options <- list(
  housing.maxprint = 350,
  housing.tibble.print_max = 50,
  housing.scipen = 999
)

.onLoad <- function(libname, pkgname) {
  # File paths
  housing_path <- "//phdata01/DROF_DATA/DOH DATA/Housing"
  
  # SQL server names
  housing_db <- odbcConnect("PH_APDEStore51")
  
  # Options
  op <- options()
  
  toset <- !(names(housing_default_options) %in% names(op))
  if(any(toset)) options(housing_default_options[toset])
  
  invisible()
}


