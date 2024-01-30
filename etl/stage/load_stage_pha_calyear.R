#### CODE TO CREATE PRE-CALCULATED CALENDAR YEAR TABLE
# Alastair Matheson, PHSKC (APDE)
#
# 2021-07

### Run from main_pha_load script
# https://github.com/PHSKC-APDE/Housing/blob/main/claims_db/etl/db_loader/main_pha_load.R
# Assumes relevant libraries are already loaded

# Allocates each person into a single value for each category by calendar year
# Also adds a summary of person-time (days) for each each category

load_stage_pha_calyear <- function(conn = NULL,
                                   max_year = NULL, 
                                   to_schema = "pha",
                                   to_table = "stage_calyear",
                                   demo_schema = "pha",
                                   demo_table = "final_demo",
                                   timevar_schema = "pha",
                                   timevar_table = "final_timevar") {
  
  # BRING IN DATA ----
  pha_demo <- setDT(dbGetQuery(conn, glue_sql("SELECT * FROM {`demo_schema`}.{`demo_table`}",
                                              .con = conn)))
  pha_timevar <- setDT(dbGetQuery(conn, glue_sql("SELECT TVdata.*
                                            	,GEO.geo_id20_tract
                                            FROM (
                                            	SELECT *
                                            	FROM {`timevar_schema`}.{`timevar_table`}
                                            	) TVdata
                                            LEFT JOIN (
                                            	SELECT DISTINCT geo_hash_clean
                                            		,geo_hash_geocode
                                            	FROM ref.address_clean
                                            	) ADDDRESS ON TVdata.geo_hash_clean = ADDDRESS.geo_hash_clean
                                            LEFT JOIN (
                                            	SELECT DISTINCT geo_hash_geocode
                                            		,geo_id20_tract
                                            	FROM ref.address_geocode
                                            	) GEO ON ADDDRESS.geo_hash_geocode = GEO.geo_hash_geocode",
                                                 .con = conn)))
  
  pha_xwalk <- unique(rbind(pha_demo[, .(KCMASTER_ID, id_apde)], pha_timevar[, .(KCMASTER_ID, id_apde)]))
  pha_demo[, id_apde := NULL] # drop id_apde to simplify data wrangling below. Will merge back on at the end
  pha_timevar[, id_apde := NULL] # drop id_apde to simplify data wrangling below. Will merge back on at the end
  
  
  
  # RUN ALLOCATION FOR EACH CALENDAR YEAR ----
  # Allocate an individual to a PHA/program based on rules:
  # 1) Multiple PHAs = PHA group with most person-time for EACH PHA.
  # 2) Single PHA only = group with most person-time
  
  # Find the row with the most person-time in each agency and group of by_vars
  # (ties will be broken by whatever other ordering exists)
  allocate_calyear <- function(df,
                       time_start = NULL,
                       time_end = NULL,
                       by_vars = NULL) {
   
    
    ## Set up overlap between time period of interest and enrollment ----
    setDT(df)
    df[, overlap_amount := as.numeric(
                            lubridate::intersect(
                              lubridate::interval(from_date, to_date),
                              lubridate::interval(time_start, time_end)
                            ) # close intersection 
                            / ddays(1) + 1)] # convert time interval measure to days, need to add 1 to get correct number. E.g., Jan 30 would be 29 days otherwise
      
    df <- df[!is.na(overlap_amount)]  
    
    ## Find the row with the most person-time in each agency and group ----
    # (ties will be broken by whatever other ordering exists)
    pt <- df[, .(pt = sum(overlap_amount)), by = c('agency', 'KCMASTER_ID', by_vars)]

    # Join back to a single df and sort so largest time is first in the group
    pop <- merge(df, 
                 pt, 
                 by = c("agency", "KCMASTER_ID", by_vars), 
                 all.x = T, all.y = F)
    setorder(pop, KCMASTER_ID, agency, -pt, -overlap_amount)

    # Take first row in group
    pop <- pop[, .SD[1], .(agency, KCMASTER_ID)]

    # Remove junk columns or columns with no meaning
    pop <- pop[, c('KCMASTER_ID', 'agency', by_vars, 'pt'), with = FALSE]

    return(pop)
  }
  

  
  # Set up calendar years
  years <- seq(2012, max_year)
  
  # Set up each grouping variable
  categories <- c("hh_KCMASTER_ID", "disability", "major_prog", "subsidy_type", "prog_type",
                  "operator_type", "vouch_type_final", "property_id", "portfolio_final",
                  "geo_zip_clean", "geo_id20_tract")
  
  
  allocated <- rbindlist(lapply(years, function(x) {
    
    message("Working on ", x)
    
    # Run over agency in general
    total <- allocate_calyear(df = pha_timevar, 
                              time_start = paste0(x, "-01-01"), 
                              time_end = paste0(x, "-12-31"))
    setnames(total, 'pt', 'pt_total')
    
    cats <- rbindlist(lapply(categories, function(y) {
      message("working on ", y)
      cat <- allocate_calyear(df = pha_timevar, 
                               time_start = paste0(x, "-01-01"), 
                               time_end = paste0(x, "-12-31"),
                               by_vars = y)
      setnames(cat, y, 'group')
      cat[, category := paste0(y)]
      cat[, group := as.character(group)]
    }))
    output <- merge(total, 
                    cats, 
                    by = c("KCMASTER_ID", "agency"), 
                    all.x = T, all.y = F) 
    output[, year := x]
    output <- output[, .(year, KCMASTER_ID, agency, pt_total, category, group, pt)]
  }))
  
  
  # Reshape to get wide output and reorder
  # Also truncate the few (~145) rows with pt > 365/6
  allocated[year %in% seq(2012, 2100, 4) & pt > 366, pt := 366] # leap years legitimately have 366 days
  allocated[!year %in% seq(2012, 2100, 4) & pt > 365, pt := 365]

  allocated[year %in% seq(2012, 2100, 4) & pt_total > 366, pt_total := 366] # leap years legitimately have 366 days
  allocated[!year %in% seq(2012, 2100, 4) & pt_total > 365, pt_total := 365]
  
  allocated_wide <- dcast(allocated, 
                          formula = year + KCMASTER_ID + agency + pt_total ~ category, 
                          value.var = c('pt', 'group'))
  setnames(allocated_wide, gsub("group_", "", names(allocated_wide)))
  setnames(allocated_wide, grep('^pt_', names(allocated_wide), value = T), paste0(gsub("pt_", "", grep('^pt_', names(allocated_wide), value = T)), '_pt')) # change prefix to suffix
  setnames(allocated_wide, 'total_pt', 'pt_total') # switch this one back because it was not made by the reshaping to wide process

  # ID AN ADMIT DATE FOR EACH YEAR ----
  # A person's first admit_date overall and for each PHA is captured in the pha_demo table
  # However, we may want to use a from_date as the admit_date if there has been a lengthy
  #   gap in their coverage. For example, if a person has coverage from 2012-2014 then 2016-2020,
  #   we would want their admit date to be 2016 for the second period.
  # For now, treat gaps of 1+ years as sufficient to trigger a new admit_date
  admit_date_setup <- copy(pha_timevar)[from_date == period_start]
  admit_date_setup <- admit_date_setup[gap_length != 0 | is.na(gap_length)] # filter out where people have duplicate rows
  admit_date_setup[, use_new_date := fcase(is.na(gap_length), 0L, 
                                           gap_length >= 365, 1L, 
                                           default = 0L)]
  admit_date_setup <- unique(admit_date_setup[, .(KCMASTER_ID, period_start, use_new_date)])
  
  pha_timevar <- merge(pha_timevar, 
                       admit_date_setup, 
                       by = c("KCMASTER_ID", "period_start"), 
                       all.x = T, all.y = F)
  
  
  # Find a admit_date for each year
  admit_dates <- rbindlist(lapply(years, function(x) {
    message("Working on ", x)
    
    output <- copy(pha_timevar)
    
    output[, overlap_amount := as.numeric(
      lubridate::intersect(
          lubridate::interval(from_date, to_date),
          lubridate::interval(as.Date(paste0(x, "-01-01")), as.Date(paste0(x, "-12-31")))) / ddays(1) + 1)]
    
    # Remove any rows that don't overlap
    output <- output[!is.na(overlap_amount)]
    
    output <- merge(output, 
                    pha_demo[, c('KCMASTER_ID', grep('^admit_date', names(pha_demo), value = T)), with = FALSE], 
                    by = 'KCMASTER_ID', 
                    all.x = T, 
                    all.y = F)

    output[, admit_date_yr := case_when(use_new_date == 1 ~ period_start,
                                        year(admit_date_all) > x ~ period_start,
                                        !is.na(admit_date_all) ~ admit_date_all,
                                        agency == "KCHA" & !is.na(admit_date_kcha) ~ admit_date_kcha,
                                        agency == "SHA" & !is.na(admit_date_sha) ~ admit_date_sha,
                                        TRUE ~ period_start)]
    
    output <- unique(output[, .(KCMASTER_ID, agency, admit_date_yr)])
    output <- output[, .(admit_date_yr = max(admit_date_yr, na.rm = T)), KCMASTER_ID][, year := x]
    
    return(output)
  }))


  # Join back to other year table
  allocated_wide <- merge(allocated_wide, 
                          admit_dates, 
                          by = c("KCMASTER_ID", "year"), 
                          all.x = T, all.y = F)
  
  
  # JOIN TO DEMO TABLE AND ADD CALCULATED FIELDS ----
    calyear <- merge(allocated_wide,
                     pha_demo[, grep('_t$|last_run', names(pha_demo), invert = T), with = F],
                     by = "KCMASTER_ID", 
                     all.x = T, all.y = F)
      
    
    calyear[, age_yr := rads::calc_age(from = dob, to = paste0(year, "-12-31"))]
    calyear[age_yr < 0, age_yr := NA] # As of 8/7/23 there are four rows where the age is -1
    calyear[, adult := fcase(age_yr >= 18, 1L, 
                             age_yr < 18, 0L)]
    calyear[, senior := fcase(age_yr >= 62, 1L, 
                              age_yr < 62, 0L)]
    calyear[, agegrp := 
              fcase(age_yr < 18,  "<18",
                    data.table::between(age_yr, 18, 24.99, NAbounds = NA), "18-24",
                    data.table::between(age_yr, 25, 44.99, NAbounds = NA), "25-44",
                    data.table::between(age_yr, 45, 64.99, NAbounds = NA), "45-64",
                    age_yr >= 65, "65+",
                    is.na(age_yr), NA_character_)]
    calyear[, agegrp_expanded := 
              fcase(age_yr < 10, "<10",
                    data.table::between(age_yr, 10, 17.99, NAbounds = NA), "10-17",
                    data.table::between(age_yr, 18, 24.99, NAbounds = NA), "18-24",
                    data.table::between(age_yr, 25, 44.99, NAbounds = NA), "25-44",
                    data.table::between(age_yr, 45, 64.99, NAbounds = NA), "45-64",
                    data.table::between(age_yr, 65, 74.99, NAbounds = NA), "65-74",
                    age_yr >= 75, "75+",
                    is.na(age_yr), NA_character_)]
    calyear[, time_housing_yr := round(interval(start = admit_date_yr, end = paste0(year, "-12-31")) / years(1), 1)]
    calyear[, time_housing := 
              fcase(time_housing_yr < 3, "<3 years",
                    data.table::between(time_housing_yr, 3, 5.99, NAbounds = NA), "3 to <6 years",
                    time_housing_yr >= 6, "6+ years",
                    TRUE, "Unknown")]
    calyear[, last_run := Sys.time()]
  
  
  # MERGE id_apde ONTO calyear ----
    calyear <- merge(calyear, pha_xwalk, by = 'KCMASTER_ID', all.x = T, all.y = F)
  
  
  # WRITE DATA TO SQL SERVER ----
  ## Select and arrange columns ----
  cols_select <- c(
    # Core variables
    "year", "KCMASTER_ID", "id_apde", "agency", "pt_total", 
    "admit_date_all", "admit_date_kcha", "admit_date_sha", "time_housing_yr", "time_housing", 
    # Head of household variables
    "hh_KCMASTER_ID", "hh_KCMASTER_ID_pt",
    # Demog variables
    "dob", "age_yr", "agegrp", "agegrp_expanded", "adult", "senior",
    "gender_me", "gender_recent", "gender_female", "gender_male",
    "race_me", "race_eth_me", "race_recent", "race_eth_recent",
    "race_aian", "race_asian", "race_black", "race_latino", "race_nhpi",
    "race_white", "race_unk", "race_eth_unk",
    "disability", "disability_pt",
    # Program info
    "major_prog", "major_prog_pt", "subsidy_type", "subsidy_type_pt", 
    "prog_type", "prog_type_pt", "operator_type", "operator_type_pt", 
    "vouch_type_final", "vouch_type_final_pt", 
    # Address and portfolio info
    "geo_zip_clean", "geo_zip_clean_pt", 
    "property_id", "property_id_pt",
    "portfolio_final", "portfolio_final_pt", 
    "geo_id20_tract", "geo_id20_tract_pt",
    # Other info
    "last_run")
  
  calyear <- calyear[, ..cols_select]
  
  ## Order/sort rows ----
  setorder(calyear, KCMASTER_ID, year)
  
  ## Load to SQL ----
  # Split into smaller tables to avoid SQL connection issues
  housing::chunk_loader(DTx = as.data.frame(calyear), 
                        connx = conn, 
                        chunk.size = 10000, 
                        schemax = to_schema, 
                        tablex = to_table, 
                        overwritex = T, 
                        appendx = F)
  
  # Quick QA
  sqlcount <- odbc::dbGetQuery(conn, paste0("SELECT count(*) FROM ", to_schema, ".", to_table))
  if(sqlcount == nrow(calyear)){message("\U0001f642 It looks like all of your data loaded to SQL!")
  }else{warning("\n\U00026A0 The number of rows in `calyear` are not the same as those in the SQL table.")}
}
