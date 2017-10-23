#' Makes a flag to indicate whether or not a SSN is junk
#' 
#' \code{junk_ssn} makes a flag to indicate whether or not a SSN is junk.
#' 
#' This function checks to see whether the Social Security Number provided could
#' be legitimate or not. See here for guidance: 
#' https://www.ssa.gov/employer/verifySSN.htm
#' The function also checks whether or not
#' 
#' @param df A data frame
#' @param id Name of variable to check for accuracy of a SSN or PHA ID
#' 
#' @name junk_ssn

#' @rdname junk_ssn
junk_ssn_num <- function(df, id = NULL) {
  if(missing(id)) {
    stop("Select a SSN variable")
  }
  
  id_quo <- enquo(id)
  
  # Would like to add check for numeric value but can't get quosures to work
  # if(is.character(id_quo)) {
  #   stop("SSN/ID variable must be numeric")
  # }

  # Keep the same name as input variable but add _junk suffix
  junk_name <- paste0(quo_name(id_quo), "_junk")
  
  df <- df %>% 
  mutate(
    # Seem to need to copy the variable here, some issue with quosures
    temp = !!id_quo,
    !!junk_name :=
      ifelse(is.na(temp) | temp < 1000000 | temp >= 900000000 |
               (temp >= 666000000 & temp <= 666999999) |
               str_sub(as.character(temp), -4, -1) == "0000", 1, 0)
         ) %>%
    select(-temp)
}


#' @rdname junk_ssn
junk_ssn_char <- function(df, id = ssn_c) {
  if(missing(id)) {
    stop("Select a SSN variable")
  }
  
  id_quo <- enquo(id)
  
  # if(is.numeric(id)) {
  #   stop("SSN/ID variable must be character")
  # }

  # Keep the same name as input variable but add _junk suffix
  junk_name <- paste0(quo_name(id_quo), "_junk")
 
  df <- df %>%
    mutate(
      # Seem to need to copy the variable here, some issue with quosures
      temp <- !!id_quo,
      !!junk_name := 
             ifelse(!!temp %in% c("XXX-XX-XXXX", "XXXXXXXXX", "A00-00-0000"),
                    1, 0))
}