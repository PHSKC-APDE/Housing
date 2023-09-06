# Author: Danny Colombara
# R Version: 4.2.2
# Date: August 2023
# Purpose: Code to QA the [pha].[stage_calyear] table


### Function elements
# conn = database connection
# from_schema = name of the schema containing the calyear data
# from_table = name of the table containing the calyear data
# timevar_schema = name of the schema containing the timevar data
# timevar_table = name of the table containing the timevar data
# qa_schema = name of the schema the QA lives in (likely the same as the from_schema if working in HHSAW)
# qa_table = name of the table that holds QA outcomes (e.g., PASS | FAIL)
# qa_values = name of the table that holds QA values (e.g., cell counts)
# load_only = only enter new values to that table, no other QA

qa_stage_pha_calyear <- function(conn = NULL,
                                 from_schema = "pha",
                                 from_table = "stage_calyear",
                                 timevar_schema = "pha",
                                 timevar_table = "stage_timevar",
                                 qa_schema = "pha",
                                 qa_table = "metadata_qa",
                                 qa_values = "metadata_qa_values",
                                 load_only = F) {
  
  
  # If this is the first time ever loading data, skip some checks.
  #   Otherwise, check against existing QA values
  message("Running QA on ", from_schema, ".", from_table)

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
                     glue::glue_sql("SELECT COUNT (*) FROM {`from_schema`}.{`from_table`}",
                                    .con = conn)))
  
  
  # Pull out run date of stage_calyear
  last_run <- as.POSIXct(
    odbc::dbGetQuery(conn, 
                     glue::glue_sql("SELECT MAX (last_run) FROM {`from_schema`}.{`from_table`}", 
                                    .con = conn))[[1]])
  
  # COMPARE NUMBER OF ROWS to previous number recorded in QA table ----
      if (load_only == F) {
        # Pull in the reference value
        previous_rows <- as.numeric(
          odbc::dbGetQuery(conn, 
                           glue::glue_sql("SELECT a.qa_value FROM
                           (SELECT * FROM {`qa_schema`}.{`qa_values`}
                             WHERE table_name = '{DBI::SQL(`from_schema`)}.{DBI::SQL(`from_table`)}' AND
                              qa_item = 'row_count') a
                           INNER JOIN
                           (SELECT MAX(qa_date) AS max_date 
                             FROM {`qa_schema`}.{`qa_values`}
                             WHERE table_name = '{DBI::SQL(`from_schema`)}.{DBI::SQL(`from_table`)}' AND
                              qa_item = 'row_count') b
                           ON a.qa_date = b.max_date",
                                          .con = conn)))
        
        row_diff <- row_count - previous_rows
        
        if (row_diff < 0) {
          row_qa_fail <- 1
          
          refresh <- data.table(last_run = last_run, 
                                table_name = paste0(from_schema, '.', from_table), 
                                qa_item = 'Number of new rows', 
                                qa_result = 'FAIL', 
                                qa_date = Sys.time(), 
                                note = paste0('There were ', abs(row_diff), ' fewer rows in the most recent table (', 
                                              row_count, ' vs. ', previous_rows, ') ... likely due to problems flagged in IDH'))
          update_qa(myupdate = refresh)
          
          message(glue::glue("\U00026A0 Fewer rows than found last time.  
                      Check {qa_schema}.{qa_table} for details (last_run = {last_run})"))
        } else {
          row_qa_fail <- 0
          
          refresh <- data.table(last_run = last_run, 
                                table_name = paste0(from_schema, '.', from_table), 
                                qa_item = 'Number of new rows', 
                                qa_result = 'PASS', 
                                qa_date = Sys.time(), 
                                note = paste0('There were ', row_diff, ' more rows in the most recent table (', 
                                          row_count, ' vs. ', previous_rows, ')'))
          update_qa(myupdate = refresh)
          
          message(glue::glue("\U0001f642 The new number of rows is the same or greater than the number loaded last time.  
                      Check {qa_schema}.{qa_table} for details (last_run = {last_run})"))
        }
      }
  
  # COMPARE NUMBER OF DISTINCT IDS to number of distinct IDs in stage_timevar ----
    id_count_calyear <- as.numeric(odbc::dbGetQuery(
      conn, glue::glue_sql("SELECT COUNT (DISTINCT KCMASTER_ID) AS count FROM {`from_schema`}.{`from_table`}",
                           .con = conn)))
  
    if (load_only == F) {
        id_count_timevar <- as.numeric(odbc::dbGetQuery(
          conn, glue::glue_sql("SELECT COUNT (DISTINCT KCMASTER_ID) as count FROM {`timevar_schema`}.{`timevar_table`} WHERE to_date >= '2012-01-01'",
                               .con = conn))) # >= 2012 because prep of calyear is only for years 2012+
        
        if (id_count_calyear != id_count_timevar) {
          id_distinct_qa_fail <- 1
          
          refresh <- data.table(last_run = last_run, 
                                table_name = paste0(from_schema, '.', from_table), 
                                qa_item = 'Number of distinct IDs', 
                                qa_result = 'FAIL', 
                                qa_date = Sys.time(), 
                                note = paste0('There were ', id_count_calyear, ' distinct IDs but ', id_count_timevar, ' in the timevar table (>=2012). They should be the same.'))
          update_qa(myupdate = refresh)

          warning(glue::glue("\U00026A0 Number of distinct IDs in calyear doesn't match the number of distinct IDs in timevar (>= 2012). 
                            Check {qa_schema}.{qa_table} for details (last_run = {last_run}"))
        } else {
          id_distinct_qa_fail <- 0
          
          refresh <- data.table(last_run = last_run, 
                                table_name = paste0(from_schema, '.', from_table), 
                                qa_item = 'Number of distinct IDs', 
                                qa_result = 'PASS', 
                                qa_date = Sys.time(), 
                                note = paste0('The number of distinct IDs (', id_count_calyear, ') was the same as the number (', id_count_timevar, ') in the timevar table (>=2012)'))
          update_qa(myupdate = refresh)
          
          message(glue::glue("\U0001f642 The number of distinct IDs ({id_count_calyear}) was the same as the number ({id_count_timevar}) in the timevar table (>=2012)"))
        }
    }
    
  # COMPARE THE AVAILABLE YEARS ----
      distinct_years <- length(odbc::dbGetQuery(
        conn, 
        glue::glue_sql("SELECT DISTINCT year 
                         FROM {`from_schema`}.{`from_table`}",
                       .con = conn))[]$year)
      
      if (load_only == F) {
        previous_years <- as.numeric(
          odbc::dbGetQuery(conn, 
                           glue::glue_sql("SELECT a.qa_value FROM
                           (SELECT * FROM {`qa_schema`}.{`qa_values`}
                             WHERE table_name = '{DBI::SQL(`from_schema`)}.{DBI::SQL(`from_table`)}' AND
                              qa_item = 'year_count') a
                           INNER JOIN
                           (SELECT MAX(qa_date) AS max_date 
                             FROM {`qa_schema`}.{`qa_values`}
                             WHERE table_name = '{DBI::SQL(`from_schema`)}.{DBI::SQL(`from_table`)}' AND
                              qa_item = 'year_count') b
                           ON a.qa_date = b.max_date",
                                          .con = conn)))    

      if (distinct_years < previous_years) {
        years_qa_fail <- 1
        
        refresh <- data.table(last_run = last_run, 
                              table_name = paste0(from_schema, '.', from_table), 
                              qa_item = 'Number of distinct years', 
                              qa_result = 'FAIL', 
                              qa_date = Sys.time(), 
                              note = paste0('There were ', distinct_years, ' distinct years but ', previous_years, ' distinct years the last time. The number of years should be >= the previous number'))
        update_qa(myupdate = refresh)
        
        warning(glue::glue("\U00026A0 There were {distinct_years} distinct years but {previous_years} distinct years the last time. The number of years should be >= the previous number') 
                          Check {qa_schema}.{qa_table} for details (last_run = {last_run}"))
      } else {
        years_qa_fail <- 0
        
        refresh <- data.table(last_run = last_run, 
                              table_name = paste0(from_schema, '.', from_table), 
                              qa_item = 'Number of distinct years', 
                              qa_result = 'PASS', 
                              qa_date = Sys.time(), 
                              note = paste0('The number of distinct years (', distinct_years, ') is >= the number of distinct years (', previous_years, ') previously loaded.'))
        update_qa(myupdate = refresh)
        
        message(glue::glue("\U0001f642 The number of distinct years ({distinct_years}) is >= the number of distinct years ({previous_years}) previously loaded."))
      }
    }
  
  
  # LOAD VALUES TO QA_VALUES TABLE ----
  message("Loading values to ", qa_schema, ".", qa_values)
  
  refresh <- data.table(table_name = paste0(from_schema, '.', from_table), 
                        qa_item = 'row_count', 
                        qa_value = as.integer(row_count), 
                        qa_date = Sys.time(), 
                        note = 'Count after refresh')
  update_qa(myupdate = refresh, mytable = qa_values)
  
  refresh <- data.table(table_name = paste0(from_schema, '.', from_table), 
                        qa_item = 'id_count', 
                        qa_value = as.integer(id_count_calyear), 
                        qa_date = Sys.time(), 
                        note = 'Count after refresh')
  update_qa(myupdate = refresh, mytable = qa_values)

  refresh <- data.table(table_name = paste0(from_schema, '.', from_table), 
                        qa_item = 'year_count', 
                        qa_value = as.integer(distinct_years), 
                        qa_date = Sys.time(), 
                        note = 'Count after refresh')
  update_qa(myupdate = refresh, mytable = qa_values)

  message("QA complete, see above for any error messages")
  
  if (load_only == F) {
    qa_total <- row_qa_fail + id_distinct_qa_fail + years_qa_fail
  } 
  
  if(!exists('qa_total')){qa_total <- c("Either `load_only = T` or there were no QA failures for the stage_calyear table.")}
  return(qa_total)
  
}
