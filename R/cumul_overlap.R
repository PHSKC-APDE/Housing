#' Counts the number of overlapping rows.
#' 
#' \code{cumul_overlap} counts the number of overlapping rows.
#' 
#' This function builds on \code{\link{overlap}} and counts the number of 
#' overlapping rows. Used in the \code{\link{los}} function.
#' Based on https://stackoverflow.com/questions/5012516/count-how-many-consecutive-values-are-true
#' 
#' @param x The variable to count overlaps by (usually will be overlap)
#' 
#' @examples
#' \dontrun{
#' cumul_overlap(overlap)
#' }
#' 
#' @export

cumul_overlap <- function(x) {
  rl <- rle(x)
  len <- rl$lengths
  v <- rl$values
  cumul_len <- cumsum(len)
  z <- x
  # replace the 0 at the end of each zero-block in z by the 
  # negative of the length of the preceding 1-block....
  iDrops <- c(0, diff(v)) < 0
  z[ cumul_len[ iDrops ] ] <- -len[ c(iDrops[-1],FALSE) ]
  # ... to ensure that the cumsum below does the right thing.
  # We zap the cumsum with x so only the cumsums for the 1-blocks survive:
  x*cumsum(z)
}