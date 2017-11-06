#' Set up time periods.
#' 
#' \code{time_range} creates vectors of start and end dates to count over
#' 
#' This function sets up the vectors for which we want to count PHA enrollment.
#' Its output is a list of start and end vectors, plus a string showing the 
#' period of time being counted in each start/end combo.
#' 
#' @param yearmin An integer that sets the starting point for the time range.
#' @param yearmax An integer that sets the end point for the time range.
#' @param period A string of either date, month, quarter, or year the determines
#' the length of time to count over. Default is year.
#' @param date A string that can be used to set the specific date to look at.
#' Must be used with period set to date. Takes the format "-MM-DD". Non-valid
#' dates will produce a charToDate(x) error.
#' 
#' @examples 
#' \dontrun{
#' time_range()
#' time_range(yearmin = 2014, yearmax = 2015, period = "quarter")
#' time_range(period = "date", date = "-06-30")
#' 
#' time_range(yearmin = 2017, yearmax = 2016)
#' }
#' 
#' @export

time_range <- function(yearmin = 2012, yearmax = 2016, period = "year", 
                       date = "-12-31") {
  # Error checks
  if(yearmin > yearmax) {
    stop("yearmin must be <= yearmax")
  }
  if(!period %in% c("date", "month", "quarter", "year")) {
    stop("Period must be date, month, quarter, or year")
  }
  
  if (missing(period)) {
    print("Default time period (year) used. Possible options: date, month, quarter, year")
  }
  
  # Set up time period
  if (period == "date") {
    timestart <- seq(as.Date(paste0(yearmin, date)), length = (yearmax - yearmin + 1), by = "1 year")
    timeend <- seq(as.Date(paste0(yearmin, date)), length = (yearmax - yearmin + 1), by = "1 year")
  }
  if (period == "month") {
    timestart <- seq(as.Date(paste0(yearmin, "-01-01")), length = (yearmax - yearmin + 1) * 12, by = "1 month")
    timeend <- seq(as.Date(paste0(yearmin, "-02-01")), length = (yearmax - yearmin + 1) * 12, by = "1 month") - 1
  }
  if (period == "year") {
    timestart <- seq(as.Date(paste0(yearmin, "-01-01")), length = (yearmax - yearmin + 1), by = "1 year")
    timeend <- seq(as.Date(paste0(yearmin + 1, "-01-01")), length = (yearmax - yearmin + 1), by = "1 year") - 1
  }
  if (period == "quarter") {
    timestart <- seq(as.Date(paste0(yearmin, "-01-01")), length = (yearmax - yearmin + 1) * 4, by = "1 quarter")
    timeend <- seq(as.Date(paste0(yearmin, "-04-01")), length = (yearmax - yearmin + 1) * 4, by = "1 quarter") - 1
  }
  time_list <- list(timestart = timestart, timeend = timeend, period = period)
  return(time_list)
}