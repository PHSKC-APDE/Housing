#' Makes a flag to indicate whether or not a SSN is junk
#' 
#' \code{junk_ssn} makes a flag to indicate whether or not a SSN is junk.
#' 
#' This function checks to see whether the Social Security Number provided could
#' be legitimate or not. See here for guidance: 
#' https://www.ssa.gov/employer/verifySSN.htm
#' The function also checks whether or not a housing ID is likely to be a 
#' temporary designation that is shared by many others.
#' 
#' @param df A data frame
#' @param id Name of variable to check for accuracy of a SSN or PHA ID
#' 
#' @name junk_ssn

 
#' @export
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
      temp = as.numeric(!!id_quo),
      !!junk_name :=
        case_when(is.na(temp) ~ 1L,
                  temp < 1000000 | temp >= 900000000 |
                    (temp >= 666000000 & temp <= 666999999) |
                    temp %in% c(10010101, 11223333, 111111111, 112234455, 
                                111119999, 123456789, 222111212, 
                                333333333, 444444444, 
                                555115555, 555555555, 555555566,
                                699999999,  
                                888888888, 898989898, 898888899) |
                    str_sub(as.character(temp), -4, -1) == "0000" |
                    str_sub(as.character(temp), 1, 3) == "999" ~ 1L,
                  TRUE ~ 0L)
    ) %>%
    select(-temp)
}


#' @export
#' @rdname junk_ssn
junk_ssn_char <- function(df, id = NULL) {
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
      temp = !!id_quo,
      !!junk_name := 
             case_when(is.na(temp) ~ 1L,
                       str_detect(temp, "XXX-|XXXX|A00-0|A000") | temp == "" ~ 1L,
                       TRUE ~ 0L)) %>%
    select(-temp)
}

#' @export
#' @rdname junk_ssn
junk_ssn_all <- function(df, id = NULL) {
  if(missing(id)) {
    stop("Select a SSN variable")
  }
  
  id_quo <- enquo(id)

  # Keep the same name as input variable but add _junk suffix
  junk_name <- paste0(quo_name(id_quo), "_junk")
  
  df <- df %>% 
    mutate(
      # Seem to need to copy the variable here, some issue with quosures
      temp = !!id_quo,
      temp_num = as.numeric(temp),
      !!junk_name :=
        ifelse(is.na(temp) | temp == "" | 
                 temp %in% c(010010101, 011223333, 111111111, 112234455, 
                             111119999, 123456789, 222111212, 
                             333333333, 444444444, 
                             555115555, 555555555, 555555566,
                             699999999,  
                             888888888, 898989898, 898888899) |
                 (!is.na(temp_num) & (temp_num < 1000000 | temp_num >= 900000000 | 
                    (temp_num >= 666000000 & temp_num <= 666999999))) |
                 (is.character(temp) & 
                    (str_sub(temp, -4, -1) == "0000" |
                       str_sub(temp, 1, 3) == "999" |
                       str_detect(temp, "XXX-|XXXX|A00-0|A000"))), 
               1, 0)
    ) %>%
    select(-temp)
  return(df)
}

