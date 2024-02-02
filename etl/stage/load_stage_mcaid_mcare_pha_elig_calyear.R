###############################################################################
# Code to generate data for the housing/Medicaid dashboard
# Allocates each person into a single category for each calendar year
# Follows the logic set up in the allocate function of the housing package
# Used as denominator for chronic conditions and for summarizing enrollment.
#
# Also adds a summary of person-time (days) for each calendar year for each 
#    combination of variables. Used as a denominator for acute events.
#
# Alastair Matheson (PHSKC-APDE)
# 2018-01-24
# Tweaked by Danny Colombara (PHSKC-APE, 2024-02-01)
#
###############################################################################

# BRING IN DATA ####
  ### Main merged data
    demo <- setDT(dbGetQuery(db_hhsaw, "SELECT * FROM claims.final_mcaid_mcare_pha_elig_demo"))
    timevar <- setDT(dbGetQuery(db_hhsaw,"SELECT * FROM claims.final_mcaid_mcare_pha_elig_timevar
                                          WHERE mcaid = 1 OR pha = 1 OR (mcare = 1 AND geo_kc = 1)"))
  
  
  ### Fix up formats
    demo[, c('dob', 'death_dt', 'start_housing') := lapply(.SD, as.Date), 
                              .SDcols = c('dob', 'death_dt', 'start_housing')]
    
    timevar[, c('from_date', 'to_date') := lapply(.SD, as.Date), 
                              .SDcols = c('from_date', 'to_date')]
    
    mcare_vars <- c("part_a", "part_b", "part_c", "partial", "buy_in", "full_benefit", "full_criteria")
    timevar[, (mcare_vars) := lapply(.SD, function(x) ifelse(mcaid == 0 & mcare == 0, 0L, x)), .SDcols = mcare_vars]
  
  
  ### Make enrollment field
    timevar <- timevar[, enroll_type := fcase(
      mcaid == 0 & mcare == 0 & pha == 1, "h",
      mcaid == 1 & mcare == 0 & pha == 1, "hmd",
      mcaid == 0 & mcare == 1 & pha == 1, "hme",
      mcaid == 1 & mcare == 0 & pha == 0, "md",
      mcaid == 0 & mcare == 1 & pha == 0, "me",
      mcaid == 1 & mcare == 1 & pha == 0, "mm",
      mcaid == 1 & mcare == 1 & pha == 1, "a"
    )]
    
# LOAD YAML ----
    yaml_calyear <- paste0(here::here(), "/etl/stage/create_stage_mcaid_mcare_pha_elig_calyear.yaml") 
    table_config_calyear <- yaml::read_yaml(yaml_calyear)
    
# ALLOCATION of PERSON TIME TO SPECIFIC AGENCY & ENROLLMENT TYPE (md, hmd, h, etc.) FOR EACH CALENDAR YEAR (for chronic disease denominator) ####
  # Used for chronic disease denominator and enrollment analyses
  # Set up calendar years
    years <- seq(2012, maxyear)

    allocated <- rbindlist(lapply(seq_along(years), function(x) {
      
      message(glue("Working on allocation for {years[x]}"))
      
      tempy <- allocate(df = timevar, 
                       starttime = as.Date(paste0(years[x], "-01-01")), 
                       endtime = as.Date(paste0(years[x], "-12-31")), 
                       agency = pha_agency, 
                       enroll = enroll_type,
                       unit = id_apde,
                       from_date = from_date, 
                       to_date = to_date)
      
      tempy[, year := years[x]]
      
      tempy[, c('last_run', 'geo_add1', 'geo_add2', 'geo_city', 'geo_state') := NULL]
      
      return(tempy)
    }))

  # CORRECT IMPOSSIBLE pt_tot VALUES
    # A handful of rows (currently ~600/200 IDs) have pt_tot >365/6
    # Some/most stem from duplicate rows in the PHA data where a person was recorded
    # at multiple addresses during the same time period.
    # For now, just truncate to 365/6
    # First check that it occures less than 0.1% of the time
    if (100*uniqueN(allocated$id_apde[allocated$pt_tot > 366])/nrow(allocated) > 0.1) {
      stop("More people than expected had pt_tot > 366 days")
    }
    
    allocated[, pt_tot := ifelse(year %in% seq(2012, 2100, 4) & pt_tot > 366, 
                                 366,
                                 ifelse(pt_tot >= 365, 
                                        365, 
                                        pt_tot))]

# MAKE PT AND POP_EVER FIELDS (for acute event denominator) ####
  # Want to keep a row for any combination of groups vars that appeared ... get person-time in days
    pt_rows <- rbindlist(lapply(seq_along(years), function(x) {
      
      message(glue("Working on patient time for {years[x]}"))
      
      output <- setDT(timevar)
      
      output[, overlap_amount:= as.numeric(lubridate::intersect( # lubridate::intersect provide the overlap in seconds
        lubridate::interval(from_date, to_date),
        lubridate::interval(as.Date(paste0(years[x], "-01-01")), 
                            as.Date(paste0(years[x], "-12-31")))) / ddays(1) + 1)] # divide by ddays(1) to change from seconds to days
      
      # Remove any rows that don't overlap
      output <- output[!is.na(overlap_amount)]
      
      # Make summary data
      output <- output[, .(pt = sum(overlap_amount)),
                       by = .(id_apde, KCMASTER_ID, mcaid, mcare, pha, mcaid_mcare_pha, enroll_type,
                              apde_dual, part_a, part_b, part_c, partial, buy_in, 
                              dual, tpl, bsp_group_cid, full_benefit, full_criteria, cov_type, 
                              mco_id, pha_agency, pha_subsidy, pha_voucher, 
                              pha_operator, pha_portfolio, geo_kc, geo_zip, geo_zip_centroid, 
                              geo_street_centroid, geo_county_code, geo_tract_code, 
                              geo_hra_code, geo_school_code)]
      
      output[, pop_ever := 1L]
      
      output[, year := years[x]]
      
      return(output)
    }))


  # MAKE FLAG TO INDICATE FULL CRITERIA FOR EACH YEAR
    # Definition: 11+ months coverage with full_criteria
    full_criteria <- pt_rows[, .(pt_tot = sum(pt)), by = .(id_apde, year, full_criteria)]

    full_criteria[, full_criteria_12 := fcase(year %in% c(2012, 2016, 2020, 2024) & pt_tot >= 11/12 * 366, 1L, 
                                              pt_tot >= 11/12 * 365, 1L, 
                                              default = 0L)] 
                  
    full_criteria <- full_criteria[, .(full_criteria_12 = max(full_criteria_12)), by = .(id_apde, year)]
  
  # Join back to main data
    allocated <- merge(allocated, full_criteria, by = c("year", "id_apde"), all.x = T, all.y = F)


# BRING INTO A SINGLE DATA FRAME () ####
  calyear <- rbindlist(list(allocated, # for chronic disease denominators (aggregated time chunks)
                            pt_rows # for acute event denominators (individual time chunks)
                            ), fill = T)


# JOIN TO ELIG_DEMO AND ADD CALCULATED FIELDS ####
    calyear <- merge(calyear, 
                      demo[, !c("mcaid_mcare_pha", "apde_dual", "last_run"), with = FALSE], 
                      by = c('id_apde', 'KCMASTER_ID'), 
                      all.x = T, all.y = F)

    
    calyear[, age_yr := floor(interval(start = dob, end = paste0(year, "-12-31")) / years(1))]
    calyear[, adult := fcase(
      age_yr >= 18, 1L,
      age_yr < 18, 0L
    )]
    
    calyear[, senior := fcase(
      age_yr >= 62, 1L,
      age_yr < 62, 0L
    )]
    
    calyear[, agegrp := fcase(
      age_yr < 18, "<18",
      data.table::between(age_yr, 18, 24.99, NAbounds = NA), "18-24",
      data.table::between(age_yr, 25, 44.99, NAbounds = NA), "25-44",
      data.table::between(age_yr, 45, 64.99, NAbounds = NA), "45-64",
      age_yr >= 65, "65+",
      is.na(age_yr), NA_character_
    )]
    
    calyear[, agegrp_expanded := fcase(
      age_yr < 10, "<10",
      data.table::between(age_yr, 10, 17.99, NAbounds = NA), "10-17",
      data.table::between(age_yr, 18, 24.99, NAbounds = NA), "18-24",
      data.table::between(age_yr, 25, 44.99, NAbounds = NA), "25-44",
      data.table::between(age_yr, 45, 64.99, NAbounds = NA), "45-64",
      data.table::between(age_yr, 65, 74.99, NAbounds = NA), "65-74",
      age_yr >= 75, "75+",
      is.na(age_yr), NA_character_
    )]
    
    calyear[, age_wc := fifelse(
      between(age_yr, 0, 6.99, NAbounds = NA), 
      "Children aged 0-6",
      NA_character_)]
    
    calyear[, time_housing_yr := rads::round2(interval(start = start_housing, end = paste0(year, "-12-31")) / years(1), 1)]
    
    calyear[, time_housing := fcase(
      is.na(pha_agency) | pha_agency == "Non-PHA", "Non-PHA",
      time_housing_yr < 3, "<3 years",
      between(time_housing_yr, 3, 5.99, NAbounds = NA), "3 to <6 years",
      time_housing_yr >= 6, "6+ years",
      default = 'Unknown'
    )]

    calyear[, last_run := Sys.time()]


#### WRITE DATA TO SQL SERVER ####
  source("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/add_index.R")


  # Ensure columns are in the correct order
  # Ensure columns are in same order in R & SQL & that we drop extraneous variables
    keep.calyear <- names(table_config_calyear$vars)
    calyear <- calyear[, ..keep.calyear]
    rads::validate_yaml_data(DF = calyear, YML = table_config_calyear, VARS = 'vars')

  # Load to SQL
    message("Loading elig_calyear data to SQL")
    chunk_loader(DTx = calyear, # R data.frame/data.table
                 connx = db_hhsaw, # connection name
                 chunk.size = 10000, 
                 schemax = table_config_calyear $schema, # schema name
                 tablex =  table_config_calyear $table, # table name
                 overwritex = T, # overwrite?
                 appendx = F, # append?
                 field.typesx = unlist(table_config_calyear $vars))  


#### QA TABLE AND MOVE TO FINAL ####
    ### confirm that all rows were loaded to SQL ----
    stage.count <- as.numeric(odbc::dbGetQuery(db_hhsaw, "SELECT COUNT (*) FROM claims.stage_mcaid_mcare_pha_elig_calyear"))
    if(stage.count != nrow(calyear)) {
      stop("Mismatching row count, error writing data")  
    }else{message("\U0001f642 All elig_calyear rows were loaded to SQL")}
    
    
    ### check that rows in stage are not less than the last time that it was created ----
      ## Overall ----
        last_run <- as.POSIXct(odbc::dbGetQuery(db_hhsaw, "SELECT MAX (last_run) FROM claims.stage_mcaid_mcare_pha_elig_calyear")[[1]]) # data for the run that was just uploaded
        
        # count number of rows
        previous_rows <- as.numeric(
          odbc::dbGetQuery(db_hhsaw, 
                           "SELECT c.qa_value from
                                           (SELECT a.* FROM
                                           (SELECT * FROM claims.metadata_qa_mcaid_mcare_pha_values
                                           WHERE table_name = 'claims.stage_mcaid_mcare_pha_elig_calyear' AND
                                           qa_item = 'row_count') a
                                           INNER JOIN
                                           (SELECT MAX(qa_date) AS max_date 
                                           FROM claims.metadata_qa_mcaid_mcare_pha_values
                                           WHERE table_name = 'claims.stage_mcaid_mcare_pha_elig_calyear' AND
                                           qa_item = 'row_count') b
                                           ON a.qa_date = b.max_date)c"))
        
        if(is.na(previous_rows)){previous_rows = 0}
        
        row_diff <- stage.count - previous_rows
        
        if (row_diff < 0) {
          temp_qa_dt <- data.table(last_run = last_run, 
                                   table_name = 'claims.stage_mcaid_mcare_pha_elig_calyear', 
                                   qa_item = 'Number new rows compared to most recent run', 
                                   qa_result = 'FAIL', 
                                   qa_date = Sys.time(), 
                                   note = paste0('There were ', row_diff, ' fewer rows in the most recent table (', 
                                                 stage.count, ' vs. ', previous_rows, ')'))
          
          problem.calyear.row_diff <- glue::glue("Fewer rows than found last time in elig_calyear.  
                                                         Check metadata.qa_mcaid_mcare_pha for details (last_run = {last_run})
                                                         \n")
        } else {
          temp_qa_dt <- data.table(last_run = last_run, 
                                   table_name = 'claims.stage_mcaid_mcare_pha_elig_calyear', 
                                   qa_item = 'Number new rows compared to most recent run', 
                                   qa_result = 'PASS', 
                                   qa_date = Sys.time(), 
                                   note = paste0('There were ', row_diff, ' more rows in the most recent table (', 
                                                 stage.count, ' vs. ', previous_rows, ')'))
          
          problem.calyear.row_diff <- glue::glue(" ") # no problem, so empty error message
          
        }
        
        odbc::dbWriteTable(conn = db_hhsaw, 
                           name = DBI::Id(schema = 'claims', table = 'metadata_qa_mcaid_mcare_pha'), 
                           value = as.data.frame(temp_qa_dt), 
                           append = T, 
                           overwrite = F)
        
      ## By YEAR ----
        # current rows
        current_year_rows <- setDT(dbGetQuery(conn = db_hhsaw, "select year, qa_value = count(*)  FROM [claims].[stage_mcaid_mcare_pha_elig_calyear] group by year order by year desc"))
        current_year_rows <- current_year_rows[, .(qa_item = paste0('row_count_', year), qa_value)]

        # count number of rows
          previous_year_rows <- setDT(
            odbc::dbGetQuery(db_hhsaw, 
                             "WITH RankedData AS (
                                  SELECT *, ROW_NUMBER() OVER (PARTITION BY qa_item ORDER BY qa_date DESC) AS RowNum
                                  FROM claims.metadata_qa_mcaid_mcare_pha_values
                                  WHERE qa_item LIKE 'row_count_[0-9]%'
                              )
                              SELECT table_name, qa_item, qa_value, qa_date
                              FROM RankedData
                              WHERE RowNum = 1;"))
        print(previous_year_rows)
        previous_year_rows <- previous_year_rows[, .(qa_item, qa_value = as.integer(qa_value))]
        
        if(nrow(previous_year_rows) == 0){previous_year_rows <- copy(current_year_rows)[, qa_value := 0]}
        
        row_diff <- merge(current_year_rows, previous_year_rows, by = 'qa_item')[, .(qa_item, qa_value = qa_value.x - qa_value.y)]

        if (any(row_diff$qa_value < 0) ) {
          temp_qa_dt <- data.table(last_run = last_run, 
                                   table_name = 'claims.stage_mcaid_mcare_pha_elig_calyear', 
                                   qa_item = 'Number new rows BY YEAR compared to most recent run', 
                                   qa_result = 'FAIL', 
                                   qa_date = Sys.time(), 
                                   note = paste0('There were ', nrow(row_diff[qa_value < 0]), ' years with fewer rows in the most recent table (', 
                                                 paste0(row_diff[qa_value < 0][, problems := paste0(gsub('row_count_', '', qa_item), ": ", qa_value)]$problems, collapse = ', '), ')'))
          
          problem.calyear.row_diff_year <- glue::glue("Fewer rows BY YEAR than found last time in elig_calyear.  
                                                         Check metadata.qa_mcaid_mcare_pha for details (last_run = {last_run})
                                                         \n")
        } else {
          temp_qa_dt <- data.table(last_run = last_run, 
                                   table_name = 'claims.stage_mcaid_mcare_pha_elig_calyear', 
                                   qa_item = 'Number new rows BY YEAR compared to most recent run', 
                                   qa_result = 'PASS', 
                                   qa_date = Sys.time(), 
                                   note = paste0('There were ', nrow(row_diff[qa_value > 0]), ' YEARS with more rows in the most recent table (', 
                                                 paste0(row_diff[qa_value > 0][, problems := paste0(gsub('row_count_', '', qa_item), ": ", qa_value)]$problems, collapse = ', '), ')'))
          
          problem.calyear.row_diff_year <- glue::glue(" ") # no problem, so empty error message
          
        }
        
        odbc::dbWriteTable(conn = db_hhsaw, 
                           name = DBI::Id(schema = 'claims', table = 'metadata_qa_mcaid_mcare_pha'), 
                           value = as.data.frame(temp_qa_dt), 
                           append = T, 
                           overwrite = F)
    
      ## By SOURCE ----
        # current rows
        current_source_rows <- setDT(dbGetQuery(conn = db_hhsaw, "select qa_item = 'row_count_mcaid', qa_value = count(*) FROM [claims].[stage_mcaid_mcare_pha_elig_calyear] where mcaid = 1
                                              UNION ALL
                                              select qa_item = 'row_count_mcare', qa_value = count(*) FROM [claims].[stage_mcaid_mcare_pha_elig_calyear] where mcare = 1
                                              UNION ALL
                                              select qa_item = 'row_count_pha', qa_value = count(*) FROM [claims].[stage_mcaid_mcare_pha_elig_calyear] where pha = 1"))

        # count number of rows
        previous_source_rows <- setDT(
          odbc::dbGetQuery(db_hhsaw, 
                           "WITH RankedData AS (
                                  SELECT *, ROW_NUMBER() OVER (PARTITION BY qa_item ORDER BY qa_date DESC) AS RowNum
                                  FROM claims.metadata_qa_mcaid_mcare_pha_values
                                  WHERE qa_item LIKE 'row_count_[a-zA-Z]%'
                              )
                              SELECT table_name, qa_item, qa_value, qa_date
                              FROM RankedData
                              WHERE RowNum = 1;"))
        print(previous_source_rows)
        previous_source_rows <- previous_source_rows[, .(qa_item, qa_value = as.integer(qa_value))]
        
        if(nrow(previous_source_rows) == 0){previous_source_rows <- copy(current_source_rows)[, qa_value := 0]}
        
        row_diff <- merge(current_source_rows, previous_source_rows, by = 'qa_item')[, .(qa_item, qa_value = qa_value.x - qa_value.y)]
        
        if (any(row_diff$qa_value < 0) ) {
          temp_qa_dt <- data.table(last_run = last_run, 
                                   table_name = 'claims.stage_mcaid_mcare_pha_elig_calyear', 
                                   qa_item = 'Number new rows BY SOURCE compared to most recent run', 
                                   qa_result = 'FAIL', 
                                   qa_date = Sys.time(), 
                                   note = paste0('There were ', nrow(row_diff[qa_value < 0]), ' sources with fewer rows in the most recent table (', 
                                                 paste0(row_diff[qa_value < 0][, problems := paste0(gsub('row_count_', '', qa_item), ": ", qa_value)]$problems, collapse = ', '), ')'))
          
          problem.calyear.row_diff_source <- glue::glue("Fewer rows BY SOURCE than found last time in elig_calyear.  
                                                         Check metadata.qa_mcaid_mcare_pha for details (last_run = {last_run})
                                                         \n")
        } else {
          temp_qa_dt <- data.table(last_run = last_run, 
                                   table_name = 'claims.stage_mcaid_mcare_pha_elig_calyear', 
                                   qa_item = 'Number new rows BY SOURCE compared to most recent run', 
                                   qa_result = 'PASS', 
                                   qa_date = Sys.time(), 
                                   note = paste0('There were ', nrow(row_diff[qa_value > 0]), ' sources with more rows in the most recent table (', 
                                                 paste0(row_diff[qa_value > 0][, problems := paste0(gsub('row_count_', '', qa_item), ": ", qa_value)]$problems, collapse = ', '), ')'))
          
          problem.calyear.row_diff_source <- glue::glue(" ") # no problem, so empty error message
        }
        
        odbc::dbWriteTable(conn = db_hhsaw, 
                           name = DBI::Id(schema = 'claims', table = 'metadata_qa_mcaid_mcare_pha'), 
                           value = as.data.frame(temp_qa_dt), 
                           append = T, 
                           overwrite = F)    
        
    ### check that the number of distinct IDs not less than the last time that it was created ----
      ## OVERALL ----
        # get count of unique id 
        current.unique.id <- as.numeric(odbc::dbGetQuery(
          db_hhsaw, "SELECT COUNT (DISTINCT id_apde) 
                          FROM claims.stage_mcaid_mcare_pha_elig_calyear"))
        
        previous.unique.id <- as.numeric(
          odbc::dbGetQuery(db_hhsaw, 
                           "SELECT c.qa_value from
                                           (SELECT a.* FROM
                                           (SELECT * FROM claims.metadata_qa_mcaid_mcare_pha_values
                                           WHERE table_name = 'claims.stage_mcaid_mcare_pha_elig_calyear' AND
                                           qa_item = 'id_count') a
                                           INNER JOIN
                                           (SELECT MAX(qa_date) AS max_date 
                                           FROM claims.metadata_qa_mcaid_mcare_pha_values
                                           WHERE table_name = 'claims.stage_mcaid_mcare_pha_elig_calyear' AND
                                           qa_item = 'id_count') b
                                           ON a.qa_date = b.max_date)c"))
        
        if(is.na(previous.unique.id)){previous.unique.id = 0}
        
        id_diff <- current.unique.id - previous.unique.id
        
        if (id_diff < 0) {
          temp_qa_dt <- data.table(last_run = last_run, 
                                   table_name = 'claims.stage_mcaid_mcare_pha_elig_calyear', 
                                   qa_item = 'Number distinct IDs compared to most recent run', 
                                   qa_result = 'FAIL', 
                                   qa_date = Sys.time(), 
                                   note = paste0('There were ', id_diff, ' fewer IDs in the most recent table (', 
                                                 current.unique.id, ' vs. ', previous.unique.id, ')'))
          
          problem.calyear.id_diff <- glue::glue("Fewer unique IDs than found last time in elig_calyear.  
                                                         Check metadata.qa_mcaid_mcare_pha for details (last_run = {last_run})
                                                         \n")
        } else {
          temp_qa_dt <- data.table(last_run = last_run, 
                                   table_name = 'claims.stage_mcaid_mcare_pha_elig_calyear', 
                                   qa_item = 'Number distinct IDs compared to most recent run', 
                                   qa_result = 'PASS', 
                                   qa_date = Sys.time(), 
                                   note = paste0('There were ', id_diff, ' more IDs in the most recent table (', 
                                                 current.unique.id, ' vs. ', previous.unique.id, ')'))
          
          problem.calyear.id_diff <- glue::glue(" ") # no problem, so empty error message
        }
        
        odbc::dbWriteTable(conn = db_hhsaw, 
                           name =DBI::Id(schema = 'claims', table = 'metadata_qa_mcaid_mcare_pha'), 
                           value = as.data.frame(temp_qa_dt), 
                           append = T, 
                           overwrite = F)
        
      ## BY YEAR ----
        # current rows
        current_year_ids <- setDT(dbGetQuery(conn = db_hhsaw, "select year, qa_value = COUNT (DISTINCT id_apde)  FROM [claims].[stage_mcaid_mcare_pha_elig_calyear] group by year order by year desc"))
        current_year_ids <- current_year_ids[, .(qa_item = paste0('id_count_', year), qa_value)]
        
        # count number of rows
        previous_year_ids <- setDT(
          odbc::dbGetQuery(db_hhsaw, 
                           "WITH RankedData AS (
                                  SELECT *, ROW_NUMBER() OVER (PARTITION BY qa_item ORDER BY qa_date DESC) AS RowNum
                                  FROM claims.metadata_qa_mcaid_mcare_pha_values
                                  WHERE qa_item LIKE 'id_count_[0-9]%'
                              )
                              SELECT table_name, qa_item, qa_value, qa_date
                              FROM RankedData
                              WHERE RowNum = 1;"))
        print(previous_year_ids)
        previous_year_ids <- previous_year_ids[, .(qa_item, qa_value = as.integer(qa_value))]
        
        if(nrow(previous_year_ids) == 0){previous_year_ids <- copy(current_year_ids)[, qa_value := 0]}
        
        id_diff <- merge(current_year_ids, previous_year_ids, by = 'qa_item')[, .(qa_item, qa_value = qa_value.x - qa_value.y)]
        
        if (any(id_diff$qa_value < 0) ) {
          temp_qa_dt <- data.table(last_run = last_run, 
                                   table_name = 'claims.stage_mcaid_mcare_pha_elig_calyear', 
                                   qa_item = 'Number new IDs BY YEAR compared to most recent run', 
                                   qa_result = 'FAIL', 
                                   qa_date = Sys.time(), 
                                   note = paste0('There were ', nrow(id_diff[qa_value < 0]), ' years with fewer IDs in the most recent table (', 
                                                 paste0(id_diff[qa_value < 0][, problems := paste0(gsub('id_count_', '', qa_item), ": ", qa_value)]$problems, collapse = ', '), ')'))
          
          problem.calyear.id_diff_year <- glue::glue("Fewer rows BY YEAR than found last time in elig_calyear.  
                                                         Check metadata.qa_mcaid_mcare_pha for details (last_run = {last_run})
                                                         \n")
        } else {
          temp_qa_dt <- data.table(last_run = last_run, 
                                   table_name = 'claims.stage_mcaid_mcare_pha_elig_calyear', 
                                   qa_item = 'Number new IDs BY YEAR compared to most recent run', 
                                   qa_result = 'PASS', 
                                   qa_date = Sys.time(), 
                                   note = paste0('There were ', nrow(id_diff[qa_value > 0]), ' YEARS with more IDs in the most recent table (', 
                                                 paste0(id_diff[qa_value > 0][, problems := paste0(gsub('id_count_', '', qa_item), ": ", qa_value)]$problems, collapse = ', '), ')'))
          
          problem.calyear.id_diff_year <- glue::glue(" ") # no problem, so empty error message
          
        }
        
        odbc::dbWriteTable(conn = db_hhsaw, 
                           name = DBI::Id(schema = 'claims', table = 'metadata_qa_mcaid_mcare_pha'), 
                           value = as.data.frame(temp_qa_dt), 
                           append = T, 
                           overwrite = F)
        
      ## BY SOURCE ----
        # current rows
        current_source_ids <- setDT(dbGetQuery(conn = db_hhsaw,                                         
                                               "select qa_item = 'id_count_mcaid', qa_value = COUNT (DISTINCT id_apde) FROM [claims].[stage_mcaid_mcare_pha_elig_calyear] where mcaid = 1
                                              UNION ALL
                                              select qa_item = 'id_count_mcare', qa_value = COUNT (DISTINCT id_apde) FROM [claims].[stage_mcaid_mcare_pha_elig_calyear] where mcare = 1
                                              UNION ALL
                                              select qa_item = 'id_count_pha', qa_value = COUNT (DISTINCT id_apde) FROM [claims].[stage_mcaid_mcare_pha_elig_calyear] where pha = 1"))
        
        # count number of rows
        previous_source_ids <- setDT(
          odbc::dbGetQuery(db_hhsaw, 
                           "WITH RankedData AS (
                                  SELECT *, ROW_NUMBER() OVER (PARTITION BY qa_item ORDER BY qa_date DESC) AS RowNum
                                  FROM claims.metadata_qa_mcaid_mcare_pha_values
                                  WHERE qa_item LIKE 'id_count_[a-zA-Z]%'
                              )
                              SELECT table_name, qa_item, qa_value, qa_date
                              FROM RankedData
                              WHERE RowNum = 1;"))
        print(previous_source_ids)
        previous_source_ids <- previous_source_ids[, .(qa_item, qa_value = as.integer(qa_value))]
        
        if(nrow(previous_source_ids) == 0){previous_source_ids <- copy(current_source_ids)[, qa_value := 0]}
        
        id_diff <- merge(current_source_ids, previous_source_ids, by = 'qa_item')[, .(qa_item, qa_value = qa_value.x - qa_value.y)]
        
        if (any(id_diff$qa_value < 0) ) {
          temp_qa_dt <- data.table(last_run = last_run, 
                                   table_name = 'claims.stage_mcaid_mcare_pha_elig_calyear', 
                                   qa_item = 'Number new IDs BY SOURCE compared to most recent run', 
                                   qa_result = 'FAIL', 
                                   qa_date = Sys.time(), 
                                   note = paste0('There were ', nrow(id_diff[qa_value < 0]), ' sources with fewer IDs in the most recent table (', 
                                                 paste0(id_diff[qa_value < 0][, problems := paste0(gsub('id_count_', '', qa_item), ": ", qa_value)]$problems, collapse = ', '), ')'))
          
          problem.calyear.id_diff_source <- glue::glue("Fewer IDs BY SOURCE than found last time in elig_calyear.  
                                                         Check metadata.qa_mcaid_mcare_pha for details (last_run = {last_run})
                                                         \n")
        } else {
          temp_qa_dt <- data.table(last_run = last_run, 
                                   table_name = 'claims.stage_mcaid_mcare_pha_elig_calyear', 
                                   qa_item = 'Number new IDs BY SOURCE compared to most recent run', 
                                   qa_result = 'PASS', 
                                   qa_date = Sys.time(), 
                                   note = paste0('There were ', nrow(id_diff[qa_value > 0]), ' SOURCES with more IDs in the most recent table (', 
                                                 paste0(id_diff[qa_value > 0][, problems := paste0(gsub('id_count_', '', qa_item), ": ", qa_value)]$problems, collapse = ', '), ')'))
          
          problem.calyear.id_diff_source <- glue::glue(" ") # no problem, so empty error message
        }
        
        odbc::dbWriteTable(conn = db_hhsaw, 
                           name = DBI::Id(schema = 'claims', table = 'metadata_qa_mcaid_mcare_pha'), 
                           value = as.data.frame(temp_qa_dt), 
                           append = T, 
                           overwrite = F)

    ### Fill qa_mcare_values table ----
      # rows
        temp_qa_values1 <- data.table(table_name = 'claims.stage_mcaid_mcare_pha_elig_calyear', 
                                     qa_item = 'row_count', 
                                     qa_value = as.integer(stage.count), 
                                     qa_date = Sys.time(), 
                                     note = '')
        
      # rows by YEAR
        temp_qa_values2 <- data.table(table_name = 'claims.stage_mcaid_mcare_pha_elig_calyear', 
                                      qa_item = current_year_rows$qa_item, 
                                      qa_value = current_year_rows$qa_value, 
                                      qa_date = Sys.time(), 
                                      note = '')
        
      # rows by SOURCE
        temp_qa_values3 <- data.table(table_name = 'claims.stage_mcaid_mcare_pha_elig_calyear', 
                                      qa_item = current_source_rows$qa_item, 
                                      qa_value = current_source_rows$qa_value, 
                                      qa_date = Sys.time(), 
                                      note = '')
        
      # IDs
        temp_qa_values4 <- data.table(table_name = 'claims.stage_mcaid_mcare_pha_elig_calyear', 
                                      qa_item = 'id_count', 
                                      qa_value = as.integer(current.unique.id), 
                                      qa_date = Sys.time(), 
                                      note = '')
      # IDs by YEAR
        temp_qa_values5 <- data.table(table_name = 'claims.stage_mcaid_mcare_pha_elig_calyear', 
                                      qa_item = current_year_ids$qa_item, 
                                      qa_value = current_year_ids$qa_value, 
                                      qa_date = Sys.time(), 
                                      note = '')
      
      # IDs by SOURCE
        temp_qa_values6 <- data.table(table_name = 'claims.stage_mcaid_mcare_pha_elig_calyear', 
                                      qa_item = current_source_ids$qa_item, 
                                      qa_value = current_source_ids$qa_value, 
                                      qa_date = Sys.time(), 
                                      note = '')
        for(i in 1:6){
          odbc::dbWriteTable(conn = db_hhsaw, 
                       name = DBI::Id(schema = 'claims', table = 'metadata_qa_mcaid_mcare_pha_values'), 
                       value = as.data.frame(get(paste0('temp_qa_values', i))), 
                       append = T, 
                       overwrite = F)
        }

    
#### THE END! ----