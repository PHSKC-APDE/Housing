#' Flags a row as overlapping with the previous row or not.
#' 
#' \code{overlap} creates a flag indicating this row matches the previous one.
#' 
#' This function checks to see whether elements of this row match the previous
#' row's elements. Its output is a new variable in the data frame. The main 
#' purpose is for use in the \code{\link{los}} function.
#' 
#' @param df A data frame
#' @param ... Arguments used to identify elements to check for matches
#' 
#' @examples
#' \dontrun{
#' overlap(pha_longitudinal, pid, agency_new)
#' }
#' 
#' @export

overlap <- function(df, ...) {
  elements <- quos(...)

  df <- df %>% mutate(
    elements = paste0(!!!elements),
    overlap = ifelse(is.na(lag(elements, 1)) | elements != lag(elements, 1), 0,
                      ifelse(elements == lag(elements, 1), 1, NA))
    ) %>%
    select(-elements)
  return(df)
}