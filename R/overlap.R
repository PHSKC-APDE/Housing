#' Flags a row as overlapping with the previous row or not.
#' 
#' \code{overlap} creates a flag indicating this row matches the previous one.
#' 
#' This function checks to see whether elements of this row match the previous
#' row's elements. Its output is a new variable in the data frame. The main 
#' purpose is for use in the \code{\link{los}} function.
#' 
#' @param df A data frame OR data table
#' @param ... Arguments used to identify elements to check for matches
#' 
#' @examples
#' \dontrun{
#' overlap(pha_longitudinal, pid, agency_new)
#' }
#' 
#' @export

overlap <- function(df, ...) {
  if (is.data.table(df)) {
    # Extract supplied columns to check for overlaps
    # match.call comes from here:
    # https://stackoverflow.com/questions/13353847/how-to-expand-an-ellipsis-argument-without-evaluating-it-in-r
    elements <- as.character(match.call(expand.dots = F)$`...`)
    
    # Make variable from concatendated overlap columns
    df[, elements := do.call(paste0, .SD), .SDcols = elements]
    
    # Find overlaps
    df[, overlap := case_when(
      is.na(lag(elements, 1)) | elements != lag(elements, 1) ~ 0,
      elements == lag(elements, 1) ~ 1,
      TRUE ~ NA_real_)]
    
    # Clean up
    df[, elements := NULL]
    # Return data
    return(df)
    
  } else {
    # Extract supplied columns to check for overlaps
    elements <- quos(...)
    df <- df %>% mutate(
      # Make variable from concatendated overlap columns
      elements = paste0(!!!elements),
      # Find overlaps
      overlap = ifelse(is.na(lag(elements, 1)) | elements != lag(elements, 1), 0,
                       ifelse(elements == lag(elements, 1), 1, NA))
    ) %>%
      # Clean up
      select(-elements)
    # Return data
    return(df)
  }
}