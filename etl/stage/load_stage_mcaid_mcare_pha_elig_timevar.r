# HEADER ----
# Author: Danny Colombara (dcolombara [at] kincounty.org), based heavily upon work by Alastair Matheson
# Date: January 3, 2024
# R version: 4.3.1
# Purpose: Creation of a PHA-Claims joint time varying covariates (`elig_timevar`) table 
#          Can have multiple rows per unique individual.
#
#          This code should be run from `Housing\etl\db_loader\main_mcaid_mcare_pha_load.r` in order to 
#          load the proper packages.
#
#          The code combines data from 
#          - SHA & KCHA combined ([pha].[final_timevar])
#          - Medicaid & Medicare combined ([claims].[final_mcaid_elig_timevar])
#          - A crosswalk from the Integrated Data Hub (IDH) ([IDMatch].[IM_HISTORY_TABLE])
#          The result is a single table with the KCMASTER_ID serving as the unique identifier
#
#          *ACHTUNG! 重要! ¡Escucha bien!
#           When Medicare data is available, this code will have to change to use a KCMASTER_ID, Medicaid, 
#           Medicare, id_apde crosswalk table and the id_apde will serve as a unique identifier
# 
# Note: This code include some basic QA that is written to the [claims]. metadatatables. There are no distinct QA scripts. 
#
# Note: This is based on code that was previously embedded in Housing\etl\db_loader\main_mcaid_mcare_pha_load.R
#       It has been moved here because that file was supposed to be a 'control tower' for the process of 
#       combining PHA and claims data


# LOAD YAML ----
  yaml_timevar <- paste0(here::here(), "/etl/stage/create_stage_mcaid_mcare_pha_elig_timevar.yaml") 
  table_config_timevar <- yaml::read_yaml(yaml_timevar)
  
# LOAD DATA FROM SQL ----
  ## xwalk: Medicaid-Medicare <> KCMASTER_ID crosswalk ----
    message('This IDH crosswalk table will be replaced by a crosswalk table created \nby the claims-data repo once Medicare data are available.')
    xwalk <- setDT(odbc::dbGetQuery(db_idh, "SELECT DISTINCT KCMASTER_ID, id_mcaid = MEDICAID_ID, updated = [SOURCE_LAST_UPDATED] 
                                          FROM [IDMatch].[IM_HISTORY_TABLE] WHERE MEDICAID_ID IS NOT NULL AND SOURCE_SYSTEM = 'MEDICAID'"))
  
  ## timevar.pha: PHA Timevar ----
    # geo_hash_geocode not currently kept in the PHA table so need to join
    # Bring in just geo_add2_clean because the other geo_add fields will come from the join
    #  to the ref geocode table
    timevar.pha <- setDT(dbGetQuery(db_hhsaw, 
                                    "SELECT a.*, b.geo_hash_geocode 
                                      FROM
                                      (SELECT KCMASTER_ID, from_date, to_date, geo_add2_clean, geo_hash_clean, 
                                      agency, operator_type, portfolio_final, subsidy_type, vouch_type_final
                                      FROM pha.final_timevar
                                      WHERE to_date >= '2012-01-01') a
                                      LEFT JOIN
                                      (SELECT DISTINCT geo_hash_clean, geo_hash_geocode FROM ref.address_clean) b
                                      ON a.geo_hash_clean = b.geo_hash_clean"))
    
  ## timevar.mm: Joint Medicaid-Medicare Timevar ----
    timevar.mm <- setDT(odbc::dbGetQuery(db_hhsaw, "SELECT * FROM [claims].[final_mcaid_elig_timevar]"))
    timevar.mm[, c("contiguous", "last_run", "cov_time_day") := NULL]
    
  ## ref.geo: Geography reference table ----
    ref.geo <- setDT(odbc::dbGetQuery(db_hhsaw, "
        SELECT DISTINCT geo_add1_clean, 
        geo_city_clean, 
        geo_state_clean, 
        geo_zip_clean, 
        geo_hash_geocode, 
        geo_id20_county AS geo_county_code,
        geo_id20_tract AS geo_tract_code, 
        geo_id20_hra AS geo_hra_code, 
        geo_id20_schooldistrict AS geo_school_code 
        FROM ref.address_geocode"))
    
    

# CLEAN / PREP IMPORTED DATA -----
  ## xwalk ----
    xwalk[, updated := as.Date(updated, format = "%m/%d/%Y")]
  
    # keep the most recent copy for each combination of ids
    setorder(xwalk, -updated)  
    xwalk <- xwalk[, .SD[1], by = .(KCMASTER_ID, id_mcaid)]
    
    # keep the most recent linkage when id_mcaid occurs more than once (should not happen, but it does)  
    setorder(xwalk, id_mcaid, -updated)  
    xwalk <- xwalk[, .SD[1], by = .(id_mcaid)] 
    
    # keep the most recent linkage when KCMASTER_ID occures more than once (this is not necessarily an error, but it can cause problems and is rare)
    setorder(xwalk, KCMASTER_ID, -updated)  
    xwalk <- xwalk[, .SD[1], by = .(KCMASTER_ID)] 
    
    xwalk[, updated := NULL]

    # quality checks
    if(identical(nrow(xwalk), uniqueN(xwalk$id_mcaid))){
      message("\U0001f642 The number of rows in the xwalk table equals the number of unique values of id_mcaid")}else{
        warning("\U00026A0 The number of rows in the xwalk table does not equal the number of unique values of id_mcaid")
      }
    
    if(identical(nrow(xwalk), uniqueN(xwalk$KCMASTER_ID))){
      message("\U0001f642 The number of rows in the xwalk table equals the number of unique values of KCMASTER_ID")}else{
        warning("\U00026A0 The number of rows in the xwalk table does not equal the number of unique values of KCMASTER_ID")
      }

  ## timevar.pha ----
    timevar.pha <- merge(timevar.pha, xwalk, by = "KCMASTER_ID", all.x = TRUE, all.y = FALSE)
    
    # Need to remove duplicates
    timevar.pha <- unique(timevar.pha)
    setorder(timevar.pha, KCMASTER_ID, from_date, to_date, id_mcaid, na.last = T) # ordered by mcaid id, so not truly random but want to make sure we preferentially select rows with an ID match
    timevar.pha <- timevar.pha[, .SD[1], by = .(KCMASTER_ID, from_date, to_date)]
    
    # Also need to reset the from/to dates because there will still be overlap, which throws things off
    # Truncate overlapping dates and remove any single day lines that overlap
    # Assume that most recent program/agency is the one to count 
    # (will be biased to the second one alphabetically when start dates are the same, if any remain)
    setorder(timevar.pha, KCMASTER_ID, from_date, to_date)
    # Make a note of which rows were truncated/deleted
    timevar.pha[, truncated := ifelse(
      KCMASTER_ID == lead(KCMASTER_ID, 1) & !is.na(lead(KCMASTER_ID, 1)) & to_date >= lead(from_date, 1), 1, 0)]
    # Now truncate
    timevar.pha[, to_date := as.Date(
      case_when(
        # If the start dates aren't the same, use next start date - 1 day
        KCMASTER_ID == lead(KCMASTER_ID, 1) & !is.na(lead(KCMASTER_ID, 1)) & to_date >= lead(from_date, 1) & 
          from_date != lead(from_date, 1) ~ lead(from_date, 1) - 1,
        #  If the start dates are the same, flag for deletion by setting to from_date
        KCMASTER_ID == lead(KCMASTER_ID, 1) & !is.na(lead(KCMASTER_ID, 1)) & to_date >= lead(from_date, 1) & 
          from_date == lead(from_date, 1) ~ from_date,
        TRUE ~ to_date
      ), origin = "1970-01-01")]
    
    # See how many rows were affected
    sum(timevar.pha$truncated, na.rm = T)
    
    # Remove any new duplicates and single-day rows
    timevar.pha <- timevar.pha[!(from_date == to_date & truncated == 1)]
    timevar.pha <- unique(timevar.pha)
    
    timevar.pha[, truncated := NULL]
    
    
    # add in geo_ data so have their addresses at each time point
    timevar.pha <- merge(timevar.pha, ref.geo, by = "geo_hash_geocode", all.x = T, all.y = F)
    
  ## timevar.mm ----
    timevar.mm <- merge(timevar.mm, xwalk, by.x = 'id_mcaid', by.y = 'id_mcaid', all.x = T, all.y = F)
    
    # There are a few duplicate rows in the timevar.mm table (same KCMASTER_ID, from_date, to_date).
    # This seems to be because the IDH matches a single KCMASTER_ID with multiple id_mcaid. 
    # For now, randomly select one row since there are so few cases
    timevar.mm <- unique(timevar.mm)
    set.seed(98104)
    timevar.mm[, sorter := sample(1000, .N), by = c("KCMASTER_ID", "from_date", "to_date")]
    setorder(timevar.mm, KCMASTER_ID, from_date, to_date, sorter)
    timevar.mm[, sorter := NULL]
    timevar.mm <- timevar.mm[, .SD[1], by = .(KCMASTER_ID, from_date, to_date)]
    
# CREATE MCAID-MCARE-PHA ELIG_TIMEVAR -----
  ## Identify IDs in both Mcaid-Mcare & PHA and split from non-linked IDs ----
    linked.id <- intersect(timevar.mm$KCMASTER_ID, timevar.pha$KCMASTER_ID)
    
    timevar.pha.solo <- timevar.pha[!KCMASTER_ID %in% linked.id]
    timevar.mm.solo <- timevar.mm[!KCMASTER_ID %in% linked.id]  
    
    timevar.pha.linked <- timevar.pha[KCMASTER_ID %in% linked.id]
    timevar.mm.linked <- timevar.mm[KCMASTER_ID %in% linked.id]
    
    
  ## Linked IDs Part 1: Create master list of time intervals by ID ----
    ### Create all possible permutations of date interval combinations from mcaid_mcare and pha for each id ----
      linked <- merge(timevar.pha.linked[, .(KCMASTER_ID, from_date, to_date)], 
                      timevar.mm.linked[, .(KCMASTER_ID, from_date, to_date)], 
                      by = "KCMASTER_ID", allow.cartesian = TRUE)
      setnames(linked, names(linked), gsub("\\.x$", ".timevar.pha", names(linked))) # clean up suffixes to eliminate confusion
      setnames(linked, names(linked), gsub("\\.y$", ".timevar.mm", names(linked))) # clean up suffixes to eliminate confusion
    
    ### Identify the type of overlaps & number of duplicate rows needed ----
    # As stated in https://github.com/PHSKC-APDE/claims_data/edit/main/claims_db/phclaims/stage/tables/load_stage.mcaid_mcare_elig_timevar.R
    # The code below was validated against a much more time intensive process where a giant table was made for every individual day within the
    # time period being analyzed. This faster / more memory efficient code was found to provide equivalent output.
      
    # Identify the type of overlap
      temp <- copy(linked)
      temp[, overlap_type := fcase(
        (is.na(from_date.timevar.pha) | is.na(from_date.timevar.mm)), 0, # First ID the non-matches
        
        from_date.timevar.pha == from_date.timevar.mm & to_date.timevar.pha == to_date.timevar.mm, 1, # Exactly the same dates, 
        
        from_date.timevar.pha <= from_date.timevar.mm & from_date.timevar.mm <= to_date.timevar.pha & 
          to_date.timevar.pha <= to_date.timevar.mm,  2, # PHA before mcaid_mcare (or exactly the same dates), 
        
        from_date.timevar.mm <= from_date.timevar.pha & from_date.timevar.pha <= to_date.timevar.mm & 
          to_date.timevar.mm <= to_date.timevar.pha, 3, # mcaid_mcare before PHA
        
        from_date.timevar.mm >= from_date.timevar.pha & to_date.timevar.mm <= to_date.timevar.pha, 4, # Mcaid_Mcare nested within PHA
        
        from_date.timevar.pha >= from_date.timevar.mm & to_date.timevar.pha <= to_date.timevar.mm, 5, # PHA nested within Mcaid_Mcare
        
        from_date.timevar.mm > to_date.timevar.pha, 6, # PHA coverage only before mcaid_mcare (or mcaid_mcare only after PHA)
        
        from_date.timevar.pha > to_date.timevar.mm, 7, # PHA coverage only after mcaid_mcare (or mcaid_mcare only before PHA), 
        
        default = 8)] # any rows that are left over
      
      # calculate overlapping dates
      temp[, from_date_o := as.Date(fcase(
        overlap_type %in% c(1, 2, 4), from_date.timevar.mm,
        overlap_type %in% c(3, 5), from_date.timevar.pha), origin = "1970-01-01")]
      
      temp[, to_date_o := as.Date(ifelse(overlap_type %in% c(1:5),
                                         pmin(to_date.timevar.mm, to_date.timevar.pha),
                                         NA), origin = "1970-01-01")]
      
      # Need to duplicate rows to separate out non-overlapping PHA and mcaid_mcare periods
      temp[, repnum := fcase(
        overlap_type %in% c(2:5), 3,
        overlap_type %in% c(6:7), 2,
        default = 1
      )]
      
      temp <- temp[, .(KCMASTER_ID, from_date.timevar.pha, to_date.timevar.pha, from_date.timevar.mm, to_date.timevar.mm, 
                       from_date_o, to_date_o, overlap_type, repnum)]
      setorder(temp, KCMASTER_ID, from_date.timevar.pha, from_date.timevar.mm, from_date_o, 
               to_date.timevar.pha, to_date.timevar.mm, to_date_o)
      
      # Check no unexpected overlap types
      if (nrow(temp[overlap_type == 8]) > 0) {
        warning("Unexpected overlap types, check temp data table")
      }

      
    
    ### Expand out rows to separate out overlaps ----
      temp_ext <- setDT(temp[rep(seq(nrow(temp)), temp$repnum), 1:ncol(temp)]) # copies the rows in temp the number of times specified in col `repnum`
      
      temp_ext[, rownum_temp := rowid(KCMASTER_ID, from_date.timevar.pha, to_date.timevar.pha, from_date.timevar.mm, to_date.timevar.mm)]
      setorder(temp_ext, KCMASTER_ID, from_date.timevar.pha, to_date.timevar.pha, from_date.timevar.mm, to_date.timevar.mm, from_date_o, 
               to_date_o, overlap_type, rownum_temp)
      # Remove non-overlapping dates
      temp_ext[, ':=' (
        from_date.timevar.pha = as.Date(ifelse((overlap_type == 6 & rownum_temp == 2) | 
                                              (overlap_type == 7 & rownum_temp == 1), 
                                            NA, from_date.timevar.pha), origin = "1970-01-01"), 
        to_date.timevar.pha = as.Date(ifelse((overlap_type == 6 & rownum_temp == 2) | 
                                            (overlap_type == 7 & rownum_temp == 1), 
                                          NA, to_date.timevar.pha), origin = "1970-01-01"),
        from_date.timevar.mm = as.Date(ifelse((overlap_type == 6 & rownum_temp == 1) | 
                                             (overlap_type == 7 & rownum_temp == 2), 
                                           NA, from_date.timevar.mm), origin = "1970-01-01"), 
        to_date.timevar.mm = as.Date(ifelse((overlap_type == 6 & rownum_temp == 1) | 
                                           (overlap_type == 7 & rownum_temp == 2), 
                                         NA, to_date.timevar.mm), origin = "1970-01-01")
      )]
      temp_ext <- unique(temp_ext)
      # Remove first row if start dates are the same or PHA is only one day
      temp_ext <- temp_ext[!(overlap_type %in% c(2:5) & rownum_temp == 1 & 
                               (from_date.timevar.pha == from_date.timevar.mm | from_date.timevar.pha == to_date.timevar.pha))]
      # Remove third row if to_dates are the same
      temp_ext <- temp_ext[!(overlap_type %in% c(2:5) & rownum_temp == 3 & to_date.timevar.pha == to_date.timevar.mm)]

    ### Calculate the finalized date columms ----
      # Set up combined dates
      # Start with rows with only PHA or mcaid_mcare, or when both sets of dates are identical
      temp_ext[, ':=' (
        from_date = as.Date(
          case_when(
            (!is.na(from_date.timevar.pha) & is.na(from_date.timevar.mm)) | overlap_type == 1 ~ from_date.timevar.pha,
            !is.na(from_date.timevar.mm) & is.na(from_date.timevar.pha) ~ from_date.timevar.mm), origin = "1970-01-01"),
        to_date = as.Date(
          case_when(
            (!is.na(to_date.timevar.pha) & is.na(to_date.timevar.mm)) | overlap_type == 1 ~ to_date.timevar.pha,
            !is.na(to_date.timevar.mm) & is.na(to_date.timevar.pha) ~ to_date.timevar.mm), origin = "1970-01-01")
      )]
      # Now look at overlapping rows and rows completely contained within the other data's dates
      temp_ext[, ':=' (
        from_date = as.Date(
          case_when(
            overlap_type %in% c(2, 4) & rownum_temp == 1 ~ from_date.timevar.pha,
            overlap_type %in% c(3, 5) & rownum_temp == 1 ~ from_date.timevar.mm,
            overlap_type %in% c(2:5) & rownum_temp == 2 ~ from_date_o,
            overlap_type %in% c(2:5) & rownum_temp == 3 ~ to_date_o + 1,
            TRUE ~ from_date), origin = "1970-01-01"),
        to_date = as.Date(
          case_when(
            overlap_type %in% c(2:5) & rownum_temp == 1 ~ lead(from_date_o, 1) - 1,
            overlap_type %in% c(2:5) & rownum_temp == 2 ~ to_date_o,
            overlap_type %in% c(2, 5) & rownum_temp == 3 ~ to_date.timevar.mm,
            overlap_type %in% c(3, 4) & rownum_temp == 3 ~ to_date.timevar.pha,
            TRUE ~ to_date), origin = "1970-01-01")
      )]
      # Deal with the last line for each person if it's part of an overlap
      temp_ext[, ':=' (
        from_date = as.Date(ifelse((KCMASTER_ID != lead(KCMASTER_ID, 1) | is.na(lead(KCMASTER_ID, 1))) &
                                     overlap_type %in% c(2:5) & 
                                     to_date.timevar.pha != to_date.timevar.mm, 
                                   lag(to_date_o, 1) + 1, 
                                   from_date), origin = "1970-01-01"),
        to_date = as.Date(ifelse((KCMASTER_ID != lead(KCMASTER_ID, 1) | is.na(lead(KCMASTER_ID, 1))) &
                                   overlap_type %in% c(2:5), 
                                 pmax(to_date.timevar.pha, to_date.timevar.mm, na.rm = TRUE), 
                                 to_date), origin = "1970-01-01")
      )]
      # Reorder in preparation for next phase
      setorder(temp_ext, KCMASTER_ID, from_date, to_date, from_date.timevar.pha, from_date.timevar.mm, 
               to_date.timevar.pha, to_date.timevar.mm, overlap_type)
    
    
    ### Label and clean summary interval data ----
      # Identify which type of enrollment this row represents
      temp_ext[, enroll_type := 
                 case_when(
                   (overlap_type == 2 & rownum_temp == 1) | 
                     (overlap_type == 3 & rownum_temp == 3) |
                     (overlap_type == 6 & rownum_temp == 1) | 
                     (overlap_type == 7 & rownum_temp == 2) |
                     (overlap_type == 4 & rownum_temp %in% c(1, 3)) |
                     (overlap_type == 0 & is.na(from_date.timevar.mm)) ~ "PHA",
                   (overlap_type == 3 & rownum_temp == 1) | 
                     (overlap_type == 2 & rownum_temp == 3) |
                     (overlap_type == 6 & rownum_temp == 2) | 
                     (overlap_type == 7 & rownum_temp == 1) | 
                     (overlap_type == 5 & rownum_temp %in% c(1, 3)) |
                     (overlap_type == 0 & is.na(from_date.timevar.pha)) ~ "mcaid_mcare",
                   overlap_type == 1 | (overlap_type %in% c(2:5) & rownum_temp == 2) ~ "both",
                   TRUE ~ "x"
                 )]
      
      # Check no bad overlaps
      temp_ext %>% count(enroll_type)
      if ("x" %in% unique(temp_ext$enroll_type)) {
        stop("Unexpected enroll_type values produced")
      }
      
      # Drop rows from enroll_type == h/m when they are fully covered by an enroll_type == b
      temp_ext[, drop := 
                 case_when(
                   KCMASTER_ID == lag(KCMASTER_ID, 1) & !is.na(lag(KCMASTER_ID, 1)) & 
                     from_date == lag(from_date, 1) & !is.na(lag(from_date, 1)) &
                     to_date >= lag(to_date, 1) & !is.na(lag(to_date, 1)) & 
                     # Fix up quirk from PHA data where two rows present for the same day
                     !(lag(enroll_type, 1) != "mcaid_mcare" & lag(to_date.timevar.pha, 1) == lag(from_date.timevar.pha, 1)) &
                     enroll_type != "both" ~ 1,
                   KCMASTER_ID == lead(KCMASTER_ID, 1) & !is.na(lead(KCMASTER_ID, 1)) & 
                     from_date == lead(from_date, 1) & !is.na(lead(from_date, 1)) &
                     to_date <= lead(to_date, 1) & !is.na(lead(to_date, 1)) & 
                     # Fix up quirk from PHA data where two rows present for the same day
                     !(lead(enroll_type, 1) != "mcaid_mcare" & lead(to_date.timevar.pha, 1) == lead(from_date.timevar.pha, 1)) &
                     enroll_type != "both" & lead(enroll_type, 1) == "both" ~ 1,
                   # Fix up other oddities when the date range is only one day
                   KCMASTER_ID == lag(KCMASTER_ID, 1) & !is.na(lag(KCMASTER_ID, 1)) & 
                     from_date == lag(from_date, 1) & !is.na(lag(from_date, 1)) &
                     from_date == to_date & !is.na(from_date) & 
                     ((enroll_type == "mcaid_mcare" & lag(enroll_type, 1) %in% c("both", "PHA")) |
                        (enroll_type == "PHA" & lag(enroll_type, 1) %in% c("both", "mcaid_mcare"))) ~ 1,
                   KCMASTER_ID == lag(KCMASTER_ID, 1) & !is.na(lag(KCMASTER_ID, 1)) & 
                     from_date == lag(from_date, 1) & !is.na(lag(from_date, 1)) &
                     from_date == to_date & !is.na(from_date) &
                     from_date.timevar.pha == lag(from_date.timevar.pha, 1) & to_date.timevar.pha == lag(to_date.timevar.pha, 1) &
                     !is.na(from_date.timevar.pha) & !is.na(lag(from_date.timevar.pha, 1)) &
                     enroll_type != "both" ~ 1,
                   KCMASTER_ID == lead(KCMASTER_ID, 1) & !is.na(lead(KCMASTER_ID, 1)) & 
                     from_date == lead(from_date, 1) & !is.na(lead(from_date, 1)) &
                     from_date == to_date & !is.na(from_date) &
                     ((enroll_type == "mcaid_mcare" & lead(enroll_type, 1) %in% c("both", "PHA")) |
                        (enroll_type == "PHA" & lead(enroll_type, 1) %in% c("both", "mcaid_mcare"))) ~ 1,
                   # Drop rows where the to_date < from_date due to 
                   # both data sources' dates ending at the same time
                   to_date < from_date ~ 1,
                   TRUE ~ 0
                 )]
      
      temp_ext <- temp_ext[drop == 0 | is.na(drop)]
      
      # Truncate remaining overlapping end dates
      temp_ext[, to_date := as.Date(ifelse(KCMASTER_ID == lead(KCMASTER_ID, 1) & !is.na(lead(from_date, 1)) & 
                                             from_date < lead(from_date, 1) & to_date >= lead(to_date, 1),
                                           lead(from_date, 1) - 1, to_date),
                                    origin = "1970-01-01")]
      
      temp_ext[, ':=' (drop = NULL, repnum = NULL, rownum_temp = NULL)]
      
      # With rows truncated, now additional rows with enroll_type == h/m that 
      # are fully covered by an enroll_type == b
      # Also catches single day rows that now have to_date < from_date
      temp_ext[, drop := case_when(KCMASTER_ID == lag(KCMASTER_ID, 1) & from_date == lag(from_date, 1) &
                                     to_date == lag(to_date, 1) & lag(enroll_type, 1) == "both" & 
                                     enroll_type != "both" ~ 1,
                                   KCMASTER_ID == lead(KCMASTER_ID, 1) & from_date == lead(from_date, 1) &
                                     to_date <= lead(to_date, 1) & lead(enroll_type, 1) == "both" ~ 1,
                                   KCMASTER_ID == lag(KCMASTER_ID, 1) & from_date >= lag(from_date, 1) &
                                     to_date <= lag(to_date, 1) & enroll_type != "both" &
                                     lag(enroll_type, 1) == "both" ~ 1,
                                   KCMASTER_ID == lead(KCMASTER_ID, 1) & from_date >= lead(from_date, 1) &
                                     to_date <= lead(to_date, 1) & enroll_type != "both" &
                                     lead(enroll_type, 1) == "both" ~ 1,
                                   TRUE ~ 0)]
      temp_ext <- temp_ext[drop == 0 | is.na(drop)]
      linked <- temp_ext[, .(KCMASTER_ID, from_date, to_date, enroll_type)]
      
      # Catch any duplicates (there are some, have not investigated why yet)
      linked <- unique(linked)
      
      rm(temp, temp_ext)
    
    
  ## Linked IDs Part 2: join mcaid_mcare & PHA data based on ID & overlapping time periods ----
    # foverlaps ... https://github.com/Rdatatable/data.table/blob/main/man/foverlaps.Rd
    ### structure data for use of foverlaps ----
    setkey(linked, KCMASTER_ID, from_date, to_date)    
    
    setkey(timevar.pha.linked, KCMASTER_ID, from_date, to_date)
    
    setkey(timevar.mm.linked, KCMASTER_ID, from_date, to_date)
    
    ### join on the mcaid_mcare linked data ----
    linked <- foverlaps(linked, timevar.mm.linked, type = "any", mult = "all")
    linked[, from_date := i.from_date] # the complete set of proper from_dates are in i.from_date ... we know this because this is from the linkage that we created
    linked[, to_date := i.to_date] # the complete set of proper to_dates are in i.to_date
    linked[, c("i.from_date", "i.to_date") := NULL] # no longer needed
    setkey(linked, KCMASTER_ID, from_date, to_date)
    
    ### join on the PHA linked data ----
    linked <- foverlaps(linked, timevar.pha.linked, type = "any", mult = "all")
    linked[, from_date := i.from_date] # the complete set of proper from_dates are in i.from_date
    linked[, to_date := i.to_date] # the complete set of proper to_dates are in i.to_date
    linked[, c("i.from_date", "i.to_date") := NULL] # no longer needed    
    
    ### Append linked and non-linked data ----
    timevar <- rbindlist(list(linked, timevar.pha.solo, timevar.mm.solo), use.names = TRUE, fill = TRUE)
    setkey(timevar, KCMASTER_ID, from_date) # order dual data     
    
    ### Collapse data if dates are contiguous and all data is the same ----
    # a little complicated, but it works! Parallel processing with future package reduces time by 60-70%
    num.cores = 6 # cores for parallel processing
    plan(multisession(workers = num.cores[1])) # tell future package how many cores to use
    
    timevar[, group := .GRP, KCMASTER_ID] # make KCMASTER_ID a numeric
    timevar[, group := (group - 1) %% num.cores[1] + 1] # group the IDs into one group per core, ensuring that IDs do not span across groups
    
    timevar_split <- split(timevar, by = 'group') # split data.table into a list of smaller data.tables
    
    timevar = rbindlist(future_lapply( # perform calculations in parallel
      X = timevar_split, 
      FUN = function(splitDT = futureX){
        splitDT[, gr := cumsum(from_date - shift(to_date, fill = 1) != 1), 
                by = c(setdiff(names(splitDT), c("from_date", "to_date")))] # unique group # (gr) for each set of contiguous dates & constant data 
        splitDT <- splitDT[, .(from_date = min(from_date), to_date = max(to_date)), 
                           by = c(setdiff(names(splitDT), c("from_date", "to_date")))] 
      } )) 
    timevar[, c('gr', 'group') := NULL]
    
    setkey(timevar, KCMASTER_ID, from_date)
    
    
  ## Prep for pushing to SQL ----
    ### Create program flags ----
    for(myvar in c('part_a', 'part_b', 'part_c')){if(!myvar %in% names(timevar)){timevar[, paste0(myvar) := NA_integer_]}}
    timevar[, mcare := 0][part_a==1 | part_b == 1 | part_c==1, mcare := 1]
    timevar[, mcaid := 0][!is.na(cov_type), mcaid := 1]
    timevar[, pha := 0][!is.na(agency), pha := 1]
    timevar[, apde_dual := 0][mcare == 1 & mcaid == 1, apde_dual := 1]
    timevar[is.na(dual), dual := 0] # is.na(dual)==T when data are only from PHA and/or Mcare
    timevar[apde_dual == 1 , dual := 1] # discussed this change via email with Alastair on 2/21/2020
    timevar[, mcaid_mcare_pha := 0][mcaid == 1 & mcare==1 & pha == 1, mcaid_mcare_pha := 1]
    timevar[, enroll_type := NULL] # kept until now for comparison with the dual flag
    if(nrow(timevar[mcare==0 & mcaid==0 & pha == 0]) > 0) {
      stop("THERE IS A SERIOUS PROBLEM WITH THE TIMEVAR DATA. Mcaid, Mcare, and PHA should never all == 0")
    } 
    
    
    ### Set Mcare/Mcaid related program flag NULLs to zero when person is only in PHA ----
    for(myvar in c('buy_in', 'full_benefit', 'full_criteria')){if(!myvar %in% names(timevar)){timevar[, paste0(myvar) := NA_integer_]}}
    timevar[mcare == 0 & mcaid == 0 & is.na(part_a), part_a := 0]
    timevar[mcare == 0 & mcaid == 0 & is.na(part_b), part_b := 0]
    timevar[mcare == 0 & mcaid == 0 & is.na(part_c), part_c := 0]
    timevar[mcare == 0 & mcaid == 0 & is.na(partial), partial := 0]
    timevar[mcare == 0 & mcaid == 0 & is.na(buy_in), buy_in := 0]
    timevar[mcare == 0 & mcaid == 0 & is.na(full_benefit), full_benefit := 0]  
    timevar[mcare == 0 & mcaid == 0 & is.na(full_criteria), full_criteria := 0]  
    
    ### Create contiguous flag ----  
    # If contiguous with the PREVIOUS row, then it is marked as contiguous. This is the same as mcaid_elig_timevar
    timevar[, prev_to_date := c(NA, to_date[-.N]), by = "KCMASTER_ID"] # MUCH faster than the shift "lag" function in data.table
    timevar[, contiguous := 0]
    timevar[from_date - prev_to_date == 1, contiguous := 1]
    timevar[, prev_to_date := NULL] # drop because no longer needed
    
    
    ### Create cov_time_date ----
    timevar[, cov_time_day := as.integer(to_date - from_date + 1)]
    
    
    ### Select PHA address data over Mcaid-Mcare when available ----
    # street
    timevar[!is.na(geo_add1_clean), geo_add1 := geo_add1_clean][, geo_add1_clean := NULL]
    # apartment
    timevar[!is.na(geo_add2_clean), geo_add2 := geo_add2_clean][, geo_add2_clean := NULL]
    # city
    timevar[!is.na(geo_city_clean ), geo_city := geo_city_clean ][, geo_city_clean  := NULL]
    # state
    timevar[!is.na(geo_state_clean ), geo_state := geo_state_clean ][, geo_state_clean  := NULL]
    # zip
    timevar[!is.na(geo_zip_clean), geo_zip := geo_zip_clean][, geo_zip_clean := NULL]
    # other geo_ variables (note that default is PHA, which is in the i. fields)
    for(myvar in c('i.geo_zip_centroid', 'i.geo_street_centroid', 'i.geo_county_code', 'i.geo_tract_code', 'i.geo_hra_code', 'i.geo_school_code')){if(!myvar %in% names(timevar)){timevar[, paste0(myvar) := NA_integer_]}}
    timevar[!is.na(i.geo_zip_centroid), geo_zip_centroid := i.geo_zip_centroid][, i.geo_zip_centroid := NULL]
    timevar[!is.na(i.geo_street_centroid), geo_street_centroid := i.geo_street_centroid][, i.geo_street_centroid := NULL]
    timevar[!is.na(i.geo_county_code), geo_county_code := i.geo_county_code][, i.geo_county_code := NULL]
    timevar[!is.na(i.geo_tract_code), geo_tract_code := i.geo_tract_code][, i.geo_tract_code := NULL]
    timevar[!is.na(i.geo_hra_code), geo_hra_code := i.geo_hra_code][, i.geo_hra_code := NULL]
    timevar[!is.na(i.geo_school_code), geo_school_code := i.geo_school_code][, i.geo_school_code := NULL]
    
    ### Add KC flag based on zip code or FIPS code as appropriate----  
    kc_zip5 <- unique(as.character(rads.data::spatial_zip_hca[]$geo_zip))
    timevar[, geo_kc := 0]
    timevar[grepl('033$', geo_county_code), geo_kc := 1]
    timevar[is.na(geo_county_code) & geo_zip %in% kc_zip5, geo_kc := 1]

    ### CREATE TIME STAMP ----
     timevar[, last_run := Sys.time()]  
    
    ### NORMALIZE PHA VARS ----
      setnames(timevar,
               c("agency", "subsidy_type", "vouch_type_final", "operator_type", "portfolio_final"),
               c("pha_agency", "pha_subsidy", "pha_voucher", "pha_operator", "pha_portfolio"))
      
      timevar[is.na(pha_agency), pha_agency := "Non-PHA"]
      timevar[pha_agency == "Non-PHA", pha_subsidy := "Non-PHA"]
      timevar[pha_agency == "Non-PHA", pha_voucher := "Non-PHA"]
      timevar[pha_agency == "Non-PHA", pha_operator := "Non-PHA"]
      timevar[pha_agency == "Non-PHA", pha_portfolio := "Non-PHA"]
    
    ### CLEAN-UP ----
      rm(kc_zip5, linked, num.cores, linked.id, myvar, ref.geo, timevar.mm, timevar.mm.linked, timevar.mm.solo, timevar.pha, timevar.pha.linked, timevar.pha.solo, timevar_split, xwalk, yaml_timevar)

      
# WRITE ELIG_TIMEVAR TO SQL ----
  ## Pull YAML from GitHub ----
      # Ensure columns are in same order in R & SQL & are limited those specified in the YAML
      keep.timevar <- names(table_config_timevar$vars)
      varmiss <- setdiff(keep.timevar, names(timevar))
      if(length(varmiss) > 0){
        for(varmissx in varmiss){
          message("Your timevar table was missing ", varmissx, " and it has been added with 100% NAs to push to SQL")
          timevar[, paste0(varmissx) := NA]
        }
      }
      
      timevar <- timevar[, ..keep.timevar]
      
      setcolorder(timevar, names(table_config_timevar$vars))
      
  ## Write table to SQL ----
      message("Loading elig_timevar data to SQL")
      chunk_loader(DTx = timevar, # R data.frame/data.table
                   connx = db_hhsaw, # connection name
                   chunk.size = 10000, 
                   schemax = table_config_timevar$schema, # schema name
                   tablex =  table_config_timevar$table, # table name
                   overwritex = T, # overwrite?
                   appendx = F, # append?
                   field.typesx = unlist(table_config_timevar$vars))  
      
  ## Simple QA ----
    ### confirm that all rows were loaded to SQL ----
      stage.count <- as.numeric(odbc::dbGetQuery(db_hhsaw, "SELECT COUNT (*) FROM claims.stage_mcaid_mcare_pha_elig_timevar"))
      if(stage.count != nrow(timevar)) {
        stop("Mismatching row count, error writing data")  
      }else{message("\U0001f642 All elig_timevar rows were loaded to SQL")}
      
      
    ### check that rows in stage are not less than the last time that it was created ----
      last_run <- as.POSIXct(odbc::dbGetQuery(db_hhsaw, "SELECT MAX (last_run) FROM claims.stage_mcaid_mcare_pha_elig_timevar")[[1]]) # data for the run that was just uploaded
      
      # count number of rows
      previous_rows <- as.numeric(
        odbc::dbGetQuery(db_hhsaw, 
                         "SELECT c.qa_value from
                               (SELECT a.* FROM
                               (SELECT * FROM claims.metadata_qa_mcaid_mcare_pha_values
                               WHERE table_name = 'claims.stage_mcaid_mcare_pha_elig_timevar' AND
                               qa_item = 'row_count') a
                               INNER JOIN
                               (SELECT MAX(qa_date) AS max_date 
                               FROM claims.metadata_qa_mcaid_mcare_pha_values
                               WHERE table_name = 'claims.stage_mcaid_mcare_pha_elig_timevar' AND
                               qa_item = 'row_count') b
                               ON a.qa_date = b.max_date)c"))
      
      if(is.na(previous_rows)){previous_rows = 0}
      
      row_diff <- stage.count - previous_rows
      
      if (row_diff < 0) {
        temp_qa_dt <- data.table(last_run = last_run, 
                                 table_name = 'claims.stage_mcaid_mcare_pha_elig_timevar', 
                                 qa_item = 'Number new rows compared to most recent run', 
                                 qa_result = 'FAIL', 
                                 qa_date = Sys.time(), 
                                 note = paste0('There were ', row_diff, ' fewer rows in the most recent table (', 
                                               stage.count, ' vs. ', previous_rows, ')'))
        
        problem.timevar.row_diff <- glue::glue("Fewer rows than found last time in elig_timevar.  
                                             Check metadata.qa_mcaid_mcare_pha for details (last_run = {last_run})
                                             \n")
      } else {
        temp_qa_dt <- data.table(last_run = last_run, 
                                 table_name = 'claims.stage_mcaid_mcare_pha_elig_timevar', 
                                 qa_item = 'Number new rows compared to most recent run', 
                                 qa_result = 'PASS', 
                                 qa_date = Sys.time(), 
                                 note = paste0('There were ', row_diff, ' more rows in the most recent table (', 
                                               stage.count, ' vs. ', previous_rows, ')'))
        
        problem.timevar.row_diff <- glue::glue(" ") # no problem, so empty error message
      }
      
      odbc::dbWriteTable(conn = db_hhsaw, 
                         name =DBI::Id(schema = 'claims', table = 'metadata_qa_mcaid_mcare_pha'), 
                         value = as.data.frame(temp_qa_dt), 
                         append = T, 
                         overwrite = F)
      
    ### check that the number of distinct IDs not less than the last time that it was created ----
      # get count of unique id 
      current.unique.id <- as.numeric(odbc::dbGetQuery(
        db_hhsaw, "SELECT COUNT (DISTINCT KCMASTER_ID) 
              FROM claims.stage_mcaid_mcare_pha_elig_timevar"))
      
      previous.unique.id <- as.numeric(
        odbc::dbGetQuery(db_hhsaw, 
                         "SELECT c.qa_value from
                               (SELECT a.* FROM
                               (SELECT * FROM claims.metadata_qa_mcaid_mcare_pha_values
                               WHERE table_name = 'claims.stage_mcaid_mcare_pha_elig_timevar' AND
                               qa_item = 'id_count') a
                               INNER JOIN
                               (SELECT MAX(qa_date) AS max_date 
                               FROM claims.metadata_qa_mcaid_mcare_pha_values
                               WHERE table_name = 'claims.stage_mcaid_mcare_pha_elig_timevar' AND
                               qa_item = 'id_count') b
                               ON a.qa_date = b.max_date)c"))
      
      if(is.na(previous.unique.id)){previous.unique.id = 0}
      
      id_diff <- current.unique.id - previous.unique.id
      
      if (id_diff < 0) {
        temp_qa_dt <- data.table(last_run = last_run, 
                                 table_name = 'claims.stage_mcaid_mcare_pha_elig_timevar', 
                                 qa_item = 'Number distinct IDs compared to most recent run', 
                                 qa_result = 'FAIL', 
                                 qa_date = Sys.time(), 
                                 note = paste0('There were ', id_diff, ' fewer IDs in the most recent table (', 
                                               current.unique.id, ' vs. ', previous.unique.id, ')'))
        
        problem.timevar.id_diff <- glue::glue("Fewer unique IDs than found last time in elig_timevar.  
                                             Check metadata.qa_mcaid_mcare_pha for details (last_run = {last_run})
                                             \n")
      } else {
        temp_qa_dt <- data.table(last_run = last_run, 
                                 table_name = 'claims.stage_mcaid_mcare_pha_elig_timevar', 
                                 qa_item = 'Number distinct IDs compared to most recent run', 
                                 qa_result = 'PASS', 
                                 qa_date = Sys.time(), 
                                 note = paste0('There were ', id_diff, ' more IDs in the most recent table (', 
                                               current.unique.id, ' vs. ', previous.unique.id, ')'))
        
        problem.timevar.id_diff <- glue::glue(" ") # no problem, so empty error message
      }
      
      odbc::dbWriteTable(conn = db_hhsaw, 
                         name =DBI::Id(schema = 'claims', table = 'metadata_qa_mcaid_mcare_pha'), 
                         value = as.data.frame(temp_qa_dt), 
                         append = T, 
                         overwrite = F)
      
    ### Fill qa_mcare_values table ----
      temp_qa_values <- data.table(table_name = 'claims.stage_mcaid_mcare_pha_elig_timevar', 
                                   qa_item = 'row_count', 
                                   qa_value = as.integer(stage.count), 
                                   qa_date = Sys.time(), 
                                   note = '')
      
      odbc::dbWriteTable(conn = db_hhsaw, 
                         name = DBI::Id(schema = 'claims', table = 'metadata_qa_mcaid_mcare_pha_values'), 
                         value = as.data.frame(temp_qa_values), 
                         append = T, 
                         overwrite = F)
      
      
      temp_qa_values2 <- data.table(table_name = 'claims.stage_mcaid_mcare_pha_elig_timevar', 
                                    qa_item = 'id_count', 
                                    qa_value = as.integer(current.unique.id), 
                                    qa_date = Sys.time(), 
                                    note = '')
      
      odbc::dbWriteTable(conn = db_hhsaw, 
                         name = DBI::Id(schema = 'claims', table = 'metadata_qa_mcaid_mcare_pha_values'), 
                         value = as.data.frame(temp_qa_values2), 
                         append = T, 
                         overwrite = F)
      
# THE END ----