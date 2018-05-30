#' @title General reformatting.
#' 
#' @description Various functions that reformat variables as needed. These are
#' specific to the public housing authority datasets.
#' \code{\link{date_ymd_f}} takes a character string and converts it to date.
#' \code{\link{yesno_f}} converts various string responses into a numeric binary.
#' \code{\link{char_f}} changes a variable to character format
#' \code{\link{trim_f}} trims white space
#' 
#' @param df A data frame
#' @param ... Variables that need to be formatted
#' 
#' @name reformatting

 
#' @export
#' @rdname reformatting
date_ymd_f <- function(df, ...) {
  vars <- quos(...)
  df <- df %>% mutate_at(vars(!!!vars), funs(as.Date(., format = "%Y-%m-%d")))
}

#' @export
#' @rdname reformatting
yesno_f <- function(df, ...) {
  vars <- quos(...)
  df <- df %>%
    mutate_at(vars(!!!vars), 
              funs(as.numeric(car::recode(., "'Y' = 1; 'Yes' = 1; '1' = 1; 
                                          'N' = 0; 'No' = 0; 'No ' = 0; '0' = 0;
                                          'N/A' = NA; 'SRO' = NA; 
                                          'NULL' = NA; else = NA"))))
}

#' @export
#' @rdname reformatting
char_f <- function(df, ...) {
  vars <- quos(...)
  df <- df %>% mutate_at(vars(!!!vars), funs(as.character(.)))
}

#' @export
#' @rdname reformatting
trim_f <- function(df, ...) {
  vars <- quos(...)
  df <- df %>% mutate_at(vars(!!!vars), funs(str_trim(.)))
}