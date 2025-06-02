#' Function to load big data to SQL (chunk.loader()) 
#' 
#' @description
#' \strong{!!!STOP!!! This function has been deprecated.} Please use
#' \code{rads::tsql_chunk_loader()} instead.
#'
#' @param ... Not used.
#'
#' @section Deprecation:
#' Please use \code{rads::tsql_chunk_loader()} instead.
#'
#' @export
chunk_loader <- function(...) {
  stop("\n\U1F6D1 chunk_loader() has been replaced. \nPlease use rads::tsql_chunk_loader() instead.", call. = FALSE)
}
