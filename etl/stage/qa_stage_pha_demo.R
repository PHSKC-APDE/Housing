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

qa_stage_pha_demo <- function(conn = NULL,
                              to_schema = "pha",
                              to_table = "stage_demo",
                              qa_schema = "pha",
                              qa_table = "metadata_qa",
                              qa_values = "metadata_qa_values",
                              load_only = F) {
  
  
  # If this is the first time ever loading data, skip some checks.
  #   Otherwise, check against existing QA values
  
  message("Running QA on ", to_schema, ".", to_table)
  
  
  #### PULL OUT VALUES NEEDED MULTIPLE TIMES ####
  # Rows in current table
  row_count <- as.numeric(odbc::dbGetQuery(conn, 
                                           glue::glue_sql("SELECT COUNT (*) FROM {`to_schema`}.{`to_table`}",
                                                          .con = conn)))
  
  
  ### Pull out run date of stage_demo
  last_run <- as.POSIXct(odbc::dbGetQuery(conn, 
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
      DBI::dbExecute(
        conn = conn,
        glue::glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`}
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Number new rows compared to most recent run', 
                   'FAIL', 
                   {Sys.time()}, 
                   'There were {row_diff} fewer rows in the most recent table 
                       ({row_count} vs. {previous_rows})')",
                       .con = conn))
      
      message(glue::glue("\U00026A0 Fewer rows than found last time.  
                  Check {qa_schema}.{qa_table} for details (last_run = {last_run})"))
    } else {
      row_qa_fail <- 0
      DBI::dbExecute(
        conn = conn,
        glue::glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`}
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Number new rows compared to most recent run', 
                   'PASS', 
                   {Sys.time()}, 
                   'There were {row_diff} more rows in the most recent table 
                       ({row_count} vs. {previous_rows})')",
                       .con = conn))
      
      message(glue::glue("\U0001f642 There were {row_diff} more rows than last time."))
    }
  }
  
  
  #### CHECK DISTINCT IDS = NUMBER OF ROWS ####
  id_count <- as.numeric(odbc::dbGetQuery(
    conn, glue::glue_sql("SELECT COUNT (DISTINCT id_kc_pha) FROM {`to_schema`}.{`to_table`}", .con = conn)))
  
  if (id_count != row_count) {
    id_distinct_qa_fail <- 1
    DBI::dbExecute(
      conn = conn,
      glue::glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`}
                       (last_run, table_name, qa_item, qa_result, qa_date, note) 
                       VALUES ({last_run}, 
                       '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                       'Number distinct IDs', 
                       'FAIL', 
                       {Sys.time()}, 
                       'There were {id_count} distinct IDs but {row_count} rows (should be the same)')",
                     .con = conn))
    
    message(glue::glue("\U0001f47f Number of distinct IDs ({id_count}) doesn't match the number of rows ({row_count}). 
                      Check {qa_schema}.{qa_table} for details (last_run = {last_run})"))
  } else {
    id_distinct_qa_fail <- 0
    DBI::dbExecute(
      conn = conn,
      glue::glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`}
                       (last_run, table_name, qa_item, qa_result, qa_date, note) 
                       VALUES ({last_run}, 
                       '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                       'Number distinct IDs', 
                       'PASS', 
                       {Sys.time()}, 
                       'The number of distinct IDs matched the number of rows ({id_count})')",
                     .con = conn))
    
    message(glue::glue("\U0001f642 The number of distinct IDs ({id_count}) matches the number of rows ({row_count}) -- as they should. 
                      Check {qa_schema}.{qa_table} for details (last_run = {last_run})"))
  }
  
  
  
  #### LOAD VALUES TO QA_VALUES TABLE ####
  message("Loading values to ", qa_schema, ".", qa_values)
  
  load_sql <- glue::glue_sql("INSERT INTO {`qa_schema`}.{`qa_values`}
                             (table_name, qa_item, qa_value, qa_date, note) 
                             VALUES ('{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                                     'row_count', 
                                     {row_count}, 
                                     {Sys.time()}, 
                                     'Count after refresh')",
                             .con = conn)
  
  DBI::dbExecute(conn = conn, load_sql)
  
  message("QA complete, see above for any error messages")
  
  if (load_only == F) {
    qa_total <- row_qa_fail + id_distinct_qa_fail
  } else {
    qa_total <- id_distinct_qa_fail
  }
  
  return(qa_total)
  
}
