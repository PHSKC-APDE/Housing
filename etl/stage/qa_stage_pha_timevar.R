# Alastair Matheson
# 2019-05

# Code to QA the PHA stage_demo table



### Function elements
# conn = database connection
# server = whether we are working in HHSAW or PHClaims
# to_schema = name of the schema to load data to
# to_table = name of the table to load data to
# qa_schema = name of the schema the QA lives in (likely the same as the to_schema if working in HHSAW)
# qa_table = name of the table that holds QA outcomes
# load_only = only enter new values to that table, no other QA

qa_stage_pha_timevar <- function(conn = NULL,
                                 to_schema = "pha",
                                 to_table = "stage_timevar",
                                 demo_schema = "pha",
                                 demo_table = "stage_demo",
                                 qa_schema = "pha",
                                 qa_table = "metadata_qa",
                                 qa_values = "metadata_qa_values",
                                 load_only = F) {
  
  
  # If this is the first time ever loading data, skip some checks.
  #   Otherwise, check against existing QA values
  message("Running QA on ", to_schema, ".", to_table)
  
  # small function to update QA table 
  update_qa <- function(myupdate = NULL, mytable = qa_table){
    odbc::dbWriteTable(conn = conn, 
                       name = DBI::Id(schema = qa_schema, table = mytable), 
                       value = as.data.frame(myupdate), 
                       append = T, 
                       overwrite = F)}
  
  # PULL OUT VALUES NEEDED MULTIPLE TIMES ----
  # Rows in current table
  row_count <- as.numeric(
    odbc::dbGetQuery(conn, 
                     glue::glue_sql("SELECT COUNT (*) FROM {`to_schema`}.{`to_table`}",
                                    .con = conn)))
  
  
  ### Pull out run date of stage_timevar
  last_run <- as.POSIXct(
    odbc::dbGetQuery(conn, 
                     glue::glue_sql("SELECT MAX (last_run) FROM {`to_schema`}.{`to_table`}", 
                                    .con = conn))[[1]])
  
  if (load_only == F) {
    #### COUNT NUMBER OF ROWS ####
    # Pull in the reference value
    previous_rows <- as.numeric(
      odbc::dbGetQuery(conn, 
                       glue::glue_sql("SELECT a.qa_value FROM
                       (SELECT * FROM {`qa_schema`}.{`qa_values`}
                         WHERE table_name = '{DBI::SQL(`to_schema`)}.{DBI::SQL(`to_table`)}' AND
                          qa_item = 'row_count') a
                       INNER JOIN
                       (SELECT MAX(qa_date) AS max_date 
                         FROM {`qa_schema`}.{`qa_values`}
                         WHERE table_name = '{DBI::SQL(`to_schema`)}.{DBI::SQL(`to_table`)}' AND
                          qa_item = 'row_count') b
                       ON a.qa_date = b.max_date",
                                      .con = conn)))
    
    row_diff <- row_count - previous_rows
    
    if (row_diff < 0) {
      row_qa_fail <- 1
      
      refresh <- data.table(last_run = last_run, 
                            table_name = paste0(to_schema, '.', to_table), 
                            qa_item = 'Number new rows compared to most recent run', 
                            qa_result = 'FAIL', 
                            qa_date = Sys.time(), 
                            note = paste0('There were ', abs(row_diff), ' fewer rows in the most recent table (', 
                                          row_count, ' vs. ', previous_rows, ')'))
      update_qa(myupdate = refresh)
      
      message(glue::glue("\U00026A0 Fewer rows than found last time.  
                  Check {qa_schema}.{qa_table} for details (last_run = {last_run})"))
    } else {
      row_qa_fail <- 0
      
      refresh <- data.table(last_run = last_run, 
                            table_name = paste0(to_schema, '.', to_table), 
                            qa_item = 'Number new rows compared to most recent run', 
                            qa_result = 'PASS', 
                            qa_date = Sys.time(), 
                            note = paste0('There were ', row_diff, ' more rows in the most recent table (', 
                                          row_count, ' vs. ', previous_rows, ')'))
      update_qa(myupdate = refresh)
      
      message(glue::glue("\U0001f642 The new number of rows is the same or greater than the number loaded last time.  
                  Check {qa_schema}.{qa_table} for details (last_run = {last_run})"))
    }
  }
  
  
  # CHECK TIMEVAR DISTINCT IDS <= DISTINCT IN STAGE_DEMO ----
  # Expect that demo will have more because some rows are dropped where the person 
  # only had a single entry etc.
  id_count_timevar <- as.numeric(odbc::dbGetQuery(
    conn, glue::glue_sql("SELECT COUNT (DISTINCT KCMASTER_ID) AS count FROM {`to_schema`}.{`to_table`}",
                         .con = conn)))
  
  id_count_demo <- as.numeric(odbc::dbGetQuery(
    conn, glue::glue_sql("SELECT COUNT (DISTINCT KCMASTER_ID) as count FROM {`demo_schema`}.{`demo_table`}",
                         .con = conn)))
  
  if (id_count_timevar > id_count_demo) {
    id_distinct_qa_fail <- 1
    
    refresh <- data.table(last_run = last_run, 
                          table_name = paste0(to_schema, '.', to_table), 
                          qa_item = 'Number distinct IDs', 
                          qa_result = 'FAIL', 
                          qa_date = Sys.time(), 
                          note = paste0('There were more timevar IDs (', id_count_timevar, ') than demo table IDs (', id_count_demo, ')'))
    update_qa(myupdate = refresh)
    
    warning(glue::glue("\U00026A0 Number of distinct IDs doesn't match the number of rows. 
                      Check {qa_schema}.{qa_table} for details (last_run = {last_run}"))
  } else {
    id_distinct_qa_fail <- 0
    
    refresh <- data.table(last_run = last_run, 
                          table_name = paste0(to_schema, '.', to_table), 
                          qa_item = 'Number distinct IDs', 
                          qa_result = 'PASS', 
                          qa_date = Sys.time(), 
                          note = paste0('The number of distinct IDs (', id_count_timevar, ') was the same or less than in the demo table (', id_count_demo, ')'))
    update_qa(myupdate = refresh)
    
    message(glue::glue("\U0001f642 The number of distinct IDs ({id_count_timevar}) was the same or 
                     less than in the demo table ({id_count_demo})"))
  }
  
  
  # CHECK FOR DUPLICATE ROWS ----
  dup_row_count <- as.numeric(odbc::dbGetQuery(
    conn, 
    glue::glue_sql("SELECT COUNT (*) AS count FROM 
                   (SELECT DISTINCT KCMASTER_ID, from_date, to_date 
                   FROM {`to_schema`}.{`to_table`}) a",
                   .con = conn)))
  
  
  if (dup_row_count != row_count) {
    dup_row_qa_fail <- 1
    
    refresh <- data.table(last_run = last_run, 
                          table_name = paste0(to_schema, '.', to_table), 
                          qa_item = 'Duplicate rows', 
                          qa_result = 'FAIL', 
                          qa_date = Sys.time(), 
                          note = paste0('There were ', dup_row_count, ' distinct rows but ', row_count, ' rows overall (should be the same)'))
    update_qa(myupdate = refresh)
    
    warning(glue::glue("\U00026A0 There appear to be duplicate rows. 
                      Check {qa_schema}.{qa_table} for details (last_run = {last_run}"))
  } else {
    dup_row_qa_fail <- 0
    
    refresh <- data.table(last_run = last_run, 
                          table_name = paste0(to_schema, '.', to_table), 
                          qa_item = 'Duplicate rows', 
                          qa_result = 'PASS', 
                          qa_date = Sys.time(), 
                          note = paste0('The number of distinct rows matched number total rows (', row_count, ')'))
    update_qa(myupdate = refresh)
    
    message(glue::glue("\U0001f642 The number of distinct rows matched number total rows ({row_count})"))
  }
  
  
  
  # LOAD VALUES TO QA_VALUES TABLE ----
  message("Loading values to ", qa_schema, ".", qa_values)
  
  refresh <- data.table(table_name = paste0(to_schema, '.', to_table), 
                        qa_item = 'row_count', 
                        qa_value = as.integer(row_count), 
                        qa_date = Sys.time(), 
                        note = 'Count after refresh')
  update_qa(myupdate = refresh, mytable = qa_values)
  
  
  message("QA complete, see above for any error messages")
  
  if (load_only == F) {
    qa_total <- row_qa_fail + id_distinct_qa_fail + dup_row_qa_fail
  } else {
    qa_total <- id_distinct_qa_fail + dup_row_qa_fail
  }
  
  return(qa_total)
  
}
