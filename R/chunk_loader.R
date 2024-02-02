#' Function to load big data to SQL (chunk.loader()) 
#' 
#' \code{chunk_loader} divides a data.frame/ data.table into smaller tables
#' so it can be easily loaded to SQL. Experience has shows that loading large 
#' tables in 'chunks' is less likely to cause errors. 
#' 
#' 
#' @param DTx A data.table/data.frame
#' @param connx The name of the relevant database connection that you have open
#' @param chunk.size The number of rows that you desire to have per chunk
#' @param schemax The name of the schema where you want to write the data
#' @param tablex The name of the table where you want to write the data
#' @param overwritex Do you want to overwrite the existing tables? Logical (T|F).
#' @param appendx Do you want to append to an existing table? Logical (T|F). 
#'  
#' intentionally redundant with \code{overwritex} to ensure that tables are not 
#' accidentally overwritten.
#' @param field.typesx Optional ability to specify the fieldtype, e.g., INT, 
#' VARCHAR(32), etc. 
#' 
#' @name chunk_loader
#' 
#' @export
#' @rdname chunk_loader
#' 

chunk_loader <- function(DTx, # R data.frame/data.table
                         connx, # connection name
                         chunk.size = 1000, 
                         schemax = NULL, # schema name
                         tablex = NULL, # table name
                         overwritex = F, # overwrite?
                         appendx = T, # append?
                         field.typesx = NULL){ # want to specify specific field types?
  # set initial values
  max.row.num <- nrow(DTx)
  number.chunks <-  ceiling(max.row.num/chunk.size) # number of chunks to be uploaded
  starting.row <- 1 # the starting row number for each chunk to be uploaded. Will begin with 1 for the first chunk
  ending.row <- chunk.size  # the final row number for each chunk to be uploaded. Will begin with the overall chunk size for the first chunk
  
  # If asked to overwrite, will DROP the table first (if it exists) and then just append
  if(overwritex == T){
    DBI::dbGetQuery(conn = connx, 
                    statement = paste0("IF OBJECT_ID('", schemax, ".", tablex, "', 'U') IS NOT NULL ", 
                                       "DROP TABLE ", schemax, ".", tablex))
    overwritex = F
    appendx = T
  }
  
  # Create loop for appending new data
  for(i in 1:number.chunks){
    # counter so we know it is not stuck
    message(paste0(Sys.time(), ": Loading chunk ", format(i, big.mark = ','), " of ", format(number.chunks, big.mark = ','), ": rows ", format(starting.row, big.mark = ','), "-", format(ending.row, big.mark = ',')))  
    
    # subset the data (i.e., create a data 'chunk')
    temp.DTx <- setDF(copy(DTx[starting.row:ending.row,])) 
    
    # load the data chunk into SQL
    if(is.null(field.typesx)){
      DBI::dbWriteTable(conn = connx, 
                        name = DBI::Id(schema = schemax, table = tablex), 
                        value = temp.DTx, 
                        append = appendx,
                        row.names = F)} 
    if(!is.null(field.typesx)){
      DBI::dbWriteTable(conn = connx, 
                        name = DBI::Id(schema = schemax, table = tablex), 
                        value = temp.DTx, 
                        append = F, # set to false so can use field types
                        row.names = F, 
                        field.types = field.typesx)
      field.typesx = NULL # because only use once, does not make sense to have it when appending      
    }
    
    # set the starting and ending rows for the next chunk to be uploaded
    starting.row <- starting.row + chunk.size
    ifelse(ending.row + chunk.size < max.row.num, 
           ending.row <- ending.row + chunk.size,
           ending.row <- max.row.num)
  } 
}
