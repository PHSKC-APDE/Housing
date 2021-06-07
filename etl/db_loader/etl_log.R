# FUNCTIONS TO LOAD DATA TO ETL LOG TABLE AND RETRIEVE DATA ----
# Alastair Matheson

# Note: these functions are similar to others that exist for claims and DOH data:
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/scripts_general/etl_log.R
# https://github.com/PHSKC-APDE/DOHdata/blob/master/ETL/general/scripts_general/etl_log.R
# These should eventually be combined into a single function that lives in the apde package.

# conn = ODBC connection to use
# to_schema = name of the schema the ETL log table is in
# to_table = name of the ETL log table
# data_source = where the data came from (currently can only be kcha or sha)
# data_type = what kind of data is this (currently can only be exit, hcv, hcv_ph, ph, or waitlist)
# date_min = the minimum action date expected in the data
# date_max = the maximum action date expected in the data
# date_rec = date the data were received
# note = any notes about the data
# auto_proceed = T allows skipping of checks against existing ETL entries. 
# Use with caution to avoid creating duplicate entries.
# Note that this will not overwrite checking for near-exact matches. 


load_metadata_etl_log <- function(conn = NULL,
                                  to_schema = NULL,
                                  to_table = NULL,
                                  data_source = c("kcha", "sha"),
                                  data_type = c("exit", "hcv", "hcv_ph", "ph", "waitlist"),
                                  date_min = NULL,
                                  date_max = NULL,
                                  date_delivery = NULL,
                                  note = NULL,
                                  auto_proceed = F) {
  
  # ERROR CHECKS ----
  if (is.null(date_min) | is.null(date_max)) {
    stop("Both date_min and date_max must be entered. Use YYYY-01-01 and YYYY-12-31 for full-year data.")
  }
  
  if (is.null(date_delivery)) {
    stop("date_delivery must be entered. Use YYYY-MM-15 if only a month is known.")
  }
  
  if (is.na(as.Date(as.character(date_min), format = "%Y-%m-%d")) |
      is.na(as.Date(as.character(date_max), format = "%Y-%m-%d")) |
      is.na(as.Date(as.character(date_delivery), format = "%Y-%m-%d"))) {
    stop("Dates must be in YYYY-MM-DD format and in quotes")
  }
  
  if (is.null(note)) {
    stop("Enter a note to describe this data")
  }
  
  
  # VARIABLES ----
  data_source <- match.arg(data_source)
  data_type <- match.arg(data_type)
  
  
  # CHECK EXISTING ENTRIES ----
  latest <- odbc::dbGetQuery(conn, 
                             glue::glue_sql("SELECT TOP (1) * FROM {`to_schema`}.{`to_table`} ORDER BY etl_batch_id DESC",
                                            .con = conn))
  
  latest_source <- odbc::dbGetQuery(conn, 
                                    glue::glue_sql(
                                      "SELECT TOP (1) * FROM {`to_schema`}.{`to_table`}
                                      WHERE data_source = {data_source} 
                                      ORDER BY etl_batch_id DESC",
                                      .con = conn))
  
  matches <- odbc::dbGetQuery(conn, 
                              glue::glue_sql(
                                "SELECT * FROM {`to_schema`}.{`to_table`}
                                WHERE data_source = {data_source} AND 
                                data_type = {data_type} AND 
                                date_max = {date_max} AND 
                                date_delivery = {date_delivery}
                                ORDER BY etl_batch_id DESC",
                                .con = conn))
  
  # SET DEFAULTS ----
  proceed <- T # Move ahead with the load
  if (nrow(latest) > 0) {
    etl_batch_id <- latest$etl_batch_id + 1
  } else {
    etl_batch_id <- 1
  }
  
  
  # CHECK AGAINST EXISTING ENTRIES ----
  # (assume if there is no record for this data source already then yes)
  if (nrow(matches) > 0) {
    print(matches)
    
    proceed_msg <- glue::glue("There are already entries in the table that \\
                              look similar to what you are attempting to enter. \\
                              See the console window. \n
                              
                              Do you still want to make a new entry?")
    proceed <- askYesNo(msg = proceed_msg)
  } else {
    if (nrow(latest) > 0 & nrow(latest_source) > 0 & auto_proceed == F) {
      proceed_msg <- glue::glue("
                                The most recent entry in the etl_log is as follows:
                                etl_batch_id: {latest$etl_batch_id}
                                data_source: {latest$data_source}
                                data_type: {latest$data_type}
                                date_min: {latest$date_min}
                                date_max: {latest$date_max}
                                date_delivery: {latest$date_delivery}
                                note: {latest$note}
                                
                                The most recent entry in the etl_log FOR THIS DATA SOURCE is as follows:
                                etl_batch_id: {latest_source$etl_batch_id}
                                data_source: {latest_source$data_source}
                                data_type: {latest_source$data_type}
                                date_min: {latest_source$date_min}
                                date_max: {latest_source$date_max}
                                date_delivery: {latest_source$date_delivery}
                                note: {latest_source$note}
                                
                                Do you still want to make a new entry?")
      
      proceed <- askYesNo(msg = proceed_msg)
    }
  }
  
  
  if (is.na(proceed)) {
    stop("ETL log load cancelled at user request")
    
  } else if (proceed == F & nrow(matches) > 0) {
    reuse <- askYesNo(msg = "Would you like to reuse the most recent existing entry that matches?")
    
    if (reuse == T) {
      etl_batch_id <- matches$etl_batch_id[1]
      message(glue::glue("Reusing ETL batch #{etl_batch_id}"))
      return(etl_batch_id)
    } else if (reuse == F) {
      
      etl_batch_id <- select.list(matches$etl_batch_id, 
                                  title = "Select an ETL ID to use from the following (or enter 0 to cancel)")
      
      if (etl_batch_id == 0L | etl_batch_id == "") {
        stop("ETL log load cancelled at user request")
      } else {
        message(glue::glue("Reusing ETL batch #{etl_batch_id}"))
        return(etl_batch_id)
      }
      
    } else if (is.na(proceed)) {
      stop("ETL log load cancelled at user request")
    }
    
  } else if (proceed == T) {
    sql_load <- glue::glue_sql(
      "INSERT INTO {`to_schema`}.{`to_table`} 
      (etl_batch_id, data_source, data_type, date_min, date_max, date_delivery, note) 
      VALUES ({etl_batch_id}, {data_source}, {data_type}, {date_min}, {date_max}, 
      {date_delivery}, {note})",
      .con = conn
    )
    
    odbc::dbGetQuery(conn, sql_load)
    
    # Finish with a message and return the latest etl_batch_id
    # (users should be assigning this to a current_batch_id object)
    message(glue::glue("ETL batch #{etl_batch_id} loaded"))
    return(etl_batch_id)
  }
}


# FUNCTION TO DISPLAY DATA ASSOCIATED WITH AN ETL_BATCH_ID ----
retrieve_metadata_etl_log <- function(conn = NULL, 
                                      to_schema = NULL,
                                      to_table = NULL,
                                      etl_batch_id = NULL) {
  ### run query
  odbc::dbGetQuery(conn, 
                   glue::glue_sql("SELECT * FROM {`to_schema`}.{`to_table`}
                                  WHERE etl_batch_id = {etl_batch_id}",
                                  .con = conn))
}