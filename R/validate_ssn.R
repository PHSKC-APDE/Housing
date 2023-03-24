#' Cleans SSN
#' 
#' \code{validate_ssn} tidies and standardizes a designated SSN column when 
#' possible and sets the value equal to NA when the value is clearly not an SSN. 
#' Distinct from the \code{junk_ssn} function in this package. 
#' 
#' @param DTx A data.table
#' @param ssnx Name of variable to check for accuracy of a SSN or PHA ID
#' 
#' @name validate_ssn
#' 
#' @export
#' @rdname validate_ssn
#' 

validate_ssn <- function(DTx, ssnx = NULL){
  setDT(DTx)
  # ensure nine digits long ----
  DTx[, paste0(ssnx) := as.character(get(ssnx))]
  DTx[, paste0(ssnx) := gsub('\\D+','', get(ssnx))] # only keep numbers 
  DTx[nchar(get(ssnx)) > 9, paste0(ssnx) := NA] # drop if SSN is > 9 digits
  DTx[nchar(get(ssnx)) < 7, paste0(ssnx) := NA] # drop if SSN is < 7 digits (b/c can only have two preceding zeros)
  DTx[nchar(get(ssnx)) < 9, paste0(ssnx) := gsub(" ", "0", sprintf("%9i", as.numeric(get(ssnx))))] # add preceding zeros to make 9 digits
  
  # drop illogical values with regular expressions ---- 
  # https://www.ssa.gov/employer/randomizationfaqs.html
  DTx[!grepl("[0-8]{1}[0-9]{2}[0-9]{2}[0-9]{4}", get(ssnx)), paste0(ssnx) := NA] # generalized acceptable pattern
  DTx[grepl("^000|^666|^9[0-9][0-9]", get(ssnx)), paste0(ssnx) := NA ] # drop if first three digits are forbidden
  DTx[grepl("^[0-9]{3}00", get(ssnx)), paste0(ssnx) := NA ] # drop if middle two digits are forbidden
  DTx[grepl("^[0-9]{5}0000", get(ssnx)), paste0(ssnx) := NA ] # drop if last four digits are forbidden 
  
  # drop specific known false SSN ----
  DTx[get(ssnx) == c("078051120"), paste0(ssnx) := NA] # Woolworth wallet SSN
  DTx[get(ssnx) == c("219099999"), paste0(ssnx) := NA] # SS Administration advertisement SSN
  DTx[get(ssnx) == c("457555462"), paste0(ssnx) := NA] # Lifelock CEO Todd Davis (at least 13 cases of identity theft)
  DTx[get(ssnx) == c("123456789"), paste0(ssnx) := NA] # garbage filler
  DTx[get(ssnx) %in% c("010010101", "011111111", "011223333", "111111111", 
                       "111119999", "111223333", "112234455", "123121234", 
                       "123123123", "123456789", "222111212", "222332222", 
                       "333333333", "444444444", "555112222", "555115555", 
                       "555555555", "555555566", "699999999", "888888888", 
                       "898888899", "898989898"), 
      paste0(ssnx) := NA] # additional unacceptable SSN
  return(DTx)
  
}