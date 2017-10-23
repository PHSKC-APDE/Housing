#' @title General reformatting.
#' 
#' @description Various functions that reformat variables as needed. These are
#' specific to the public housing authority datasets.
#' 
#' @param df A data frame
#' @param ... Variables that need to be formatted
#' 
#' @name format

#' @rdname format
date_ymd_f <- function(df, ...) {
  vars <- quos(...)
  df <- df %>% mutate_at(vars(!!!vars), funs(as.Date(., format = "%Y-%m-%d")))
}

#' @rdname format
yesno_f <- function(df, ...) {
  vars <- quos(...)
  df <- df %>%
    mutate_at(vars(!!!vars), 
              funs(as.numeric(car::recode(., "'Y' = 1; 'Yes' = 1; 'N' = 0; 
                                          'No ' = 0; 'N/A' = NA; 'SRO' = NA; 
                                          'NULL' = NA; else = NA"))))
}

#' @rdname format
char_f <- function(df, ...) {
  vars <- quos(...)
  df <- df %>% mutate_at(vars(!!!vars), funs(as.character(.)))
}

#' @rdname format
trim_f <- function(df, ...) {
  vars <- quos(...)
  df <- df %>% mutate_at(vars(!!!vars), funs(str_trim(.)))
}