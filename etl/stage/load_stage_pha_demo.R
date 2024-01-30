#### CODE TO CREATE CONSOLIDATED NON_TIME VARYING DEMOGRAPHICS
# Alastair Matheson, PHSKC (APDE)
#
# 2021-06
#
# Revised by Danny Colombara, PHSKC/APDE, 2023-03

### Run from main_pha_load script
# https://github.com/PHSKC-APDE/Housing/blob/main/claims_db/etl/db_loader/main_pha_load.R
# Assumes relevant libraries are already loaded


# conn = ODBC connection to use
# to_schema = name of the schema to load data to
# to_table = name of the table to load data to
# from_schema = name of the schema the input data are in
# from_table = common prefix of the table the input data are in
# id_schema = name of the schema the identity table is in
# id_table = name of the identity table

load_stage_demo <- function(conn = NULL,
                            to_schema = "pha",
                            to_table = "stage_demo",
                            from_schema = "pha",
                            from_table = "stage_",
                            id_schema = "pha",
                            id_table = "final_identities") {
  
  # REMINDER ABOUT NEEED FOR UPDATED TABLES ----
  message(paste0('\U00026A0\U00026A0 ACHTUNG! 重要! ¡Escucha bien!\n', 
                 'The following tables MUST be up to date. If they are not, stop this code and update those tables first\n', 
                 '   [pha].[final_identities]\n', 
                 '   [pha].[stage_kcha]\n', 
                 '   [pha].[stage_sha]'))
  
  
  # BRING IN DATA AND COMBINE ----
  message("Pull in demographic data")
  kcha <- setDT(dbGetQuery(
    conn,
    glue_sql(
      "SELECT DISTINCT IDdata.KCMASTER_ID
          ,IDdata.id_apde
        	,KCHAdata.agency
        	,KCHAdata.act_date
        	,KCHAdata.admit_date
        	,KCHAdata.dob
        	,KCHAdata.gender
        	,KCHAdata.r_aian
        	,KCHAdata.r_asian
        	,KCHAdata.r_black
        	,KCHAdata.r_hisp
        	,KCHAdata.r_nhpi
        	,KCHAdata.r_white
        FROM (
        	SELECT id_hash
        		,agency
        		,act_date
        		,admit_date
        		,dob
        		,gender
        		,r_aian
        		,r_asian
        		,r_black
        		,r_hisp
        		,r_nhpi
        		,r_white
        	FROM {`from_schema`}.{DBI::SQL(paste0(from_table, 'kcha')) }
        	WHERE id_hash IS NOT NULL
        	) KCHAdata
        LEFT JOIN (
        	SELECT id_hash
        		,KCMASTER_ID
        		,id_apde
        	FROM {`id_schema`}.{`id_table`}
        	) IDdata ON KCHAdata.id_hash = IDdata.id_hash",
      .con = conn)))
  
  sha <- setDT(dbGetQuery(
    conn,
    glue_sql(
      "SELECT DISTINCT IDdata.KCMASTER_ID
        	,IDdata.id_apde
        	,SHAdata.agency
        	,SHAdata.act_date
        	,SHAdata.admit_date
        	,SHAdata.dob
        	,SHAdata.gender
        	,SHAdata.r_aian
        	,SHAdata.r_asian
        	,SHAdata.r_black
        	,SHAdata.r_hisp
        	,SHAdata.r_nhpi
        	,SHAdata.r_white
        FROM (
        	SELECT id_hash
        		,agency
        		,act_date
        		,admit_date
        		,dob
        		,gender
        		,r_aian
        		,r_asian
        		,r_black
        		,r_hisp
        		,r_nhpi
        		,r_white
        	FROM {`from_schema`}.{DBI::SQL(paste0(from_table, 'sha')) } 
        	WHERE id_hash IS NOT NULL
        	) SHAdata
        LEFT JOIN (
        	SELECT id_hash
        		,KCMASTER_ID
        		,id_apde
        	FROM {`id_schema`}.{`id_table`}
        	) IDdata ON SHAdata.id_hash = IDdata.id_hash",
      .con = conn)))
  
  pha <- unique(setDT(rbind(kcha, sha)))
  
  # Remove the rows missing KCMASTER_ID since we won't be able to join on this
  if(nrow(pha[is.na(KCMASTER_ID)]) > 0){message('Summary of PHA data without a KCMASTER_ID'); print(pha[is.na(KCMASTER_ID), .N, agency])}
  pha <- pha[!is.na(KCMASTER_ID)] # should not exist, but just in case!
  pha.xwalk <- unique(pha[, .(KCMASTER_ID, id_apde)]) # keep so can easily merge on id_apde at end of this script

  # WORK ON DOB ----
    message("Processing DOB data")
    elig_dob <- unique(copy(pha)[, c("KCMASTER_ID", "act_date", "dob", 'admit_date')][!is.na(dob)])
    
    maxdob <- max(
      DBI::dbGetQuery(conn = conn, paste0('SELECT maxdate = max(act_date) FROM ', from_schema, '.', from_table, 'kcha'))[['maxdate']], 
      DBI::dbGetQuery(conn = conn, paste0('SELECT maxdate = max(act_date) FROM ', from_schema, '.', from_table, 'sha'))[['maxdate']])
    mindob <- maxdob - years(125) # seriously doubt anyone is > 125 years old
    
    # Change likely incorrect DOBs to be correct century
    # May need to tweak cutoff points as years go on
    elig_dob[year(dob) > year(maxdob) | year(dob) < 1000, dob := as.Date(format(dob, "19%y-%m-%d"))]
    elig_dob[between(year(dob), (1000 + as.integer(substr(year(maxdob), 3, 4)) + 1 ), 1880), dob := as.Date(format(dob, "19%y-%m-%d"))]
    elig_dob[between(year(dob), 1000, (1000 + as.integer(substr(year(maxdob), 3, 4)))), dob := as.Date(format(dob, "20%y-%m-%d"))]
    
    # drop illogical dob ... i.e., those that take place  after the admit_date (because can't be negative years old at admission)
    elig_dob[admit_date < '1941-01-01', admit_date := NA] # these PHAs didn't start until ~1940 ... see next section on Admit Date for details
    elig_dob[, tempage := rads::calc_age(from = dob, to = admit_date)]
    elig_dob <- elig_dob[is.na(tempage) | tempage >= 0][, c('admit_date', 'tempage') := NULL]

    # keep the most recent dob only
    setorder(elig_dob, KCMASTER_ID, -act_date, na.last = T)
    elig_dob <- elig_dob[, .SD[1], KCMASTER_ID][, act_date := NULL]

    # ensure all dob are reasonable
    if(max(elig_dob$dob, na.rm = T) > maxdob){
      stop("\n\U0001f47f There is at least one dob that is > the max possible dob (", maxdob, "). Fix the cleaning logic before continuing.")}
    
    if(min(elig_dob$dob, na.rm = T) < mindob){
      stop("\n\U0001f47f There is at least one dob that is < the min possible dob (", mindob, "). Fix the cleaning logic before continuing.")}
    
    
  # WORK ON ADMIT DATE ----
    # The admit date column does not always reflect when a person first entered housing support
    # (e.g., if a person switches programs it might reset)
    # Also want to derive an empirical start date using the first act_date for each person
    
    message("Processing date person entered KCHA/SHA")
    elig_admit <- copy(pha)[!is.na(admit_date)][, c("KCMASTER_ID", "agency", "act_date", "admit_date")]
    
    # Take the earliest date per ID by overall and each PHA
      # There are irrational admit dates that occurred before the PHAs existed! First set those values to NA
      elig_admit <- elig_admit[agency %in% c('SHA', 'KCHA')] # other extraneous PHAs should be dropped  
      elig_admit <- elig_admit[!(agency == 'SHA' & admit_date < '1941-01-01')] # SHA started in 1938 & opened Yesler Terrace around 1941
      elig_admit <- elig_admit[!(agency == 'KCHA' & admit_date < '1942-01-01')] # KCHA started in 1939 & opened Black Diamond site in 1942 
      
      elig_admit_all <- elig_admit[!is.na(admit_date), .(admit_date_all = min(admit_date, na.rm = T)), by = "KCMASTER_ID"]
      elig_admit_kcha <- elig_admit[!is.na(admit_date) & agency == "KCHA", .(admit_date_kcha = min(admit_date, na.rm = T)), by = "KCMASTER_ID"]
      elig_admit_sha <- elig_admit[!is.na(admit_date) & agency == "SHA", .(admit_date_sha = min(admit_date, na.rm = T)), by = "KCMASTER_ID"]
    
    # Merge back into a single table
    elig_admit <- merge(elig_admit_sha, merge(elig_admit_kcha, elig_admit_all, all = T), all = T)
    
    rm(elig_admit_all, elig_admit_kcha, elig_admit_sha)
  
  
  # WORK ON GENDER ----
    message("Processing gender data")
    elig_gender <- pha[, c("KCMASTER_ID", "act_date", "gender")]
  
    ## Create alone or in combination gender variables ----
      elig_gender[!gender %in% c('F', 'Female', 'M', 'Male'), gender := NA]
      elig_gender[, ':=' (gender_female = ifelse(str_detect(toupper(gender), "(^F|FEMALE)"), 1, 0),
                          gender_male = ifelse(str_detect(toupper(gender), "(^M|^MALE)"), 1, 0))]
  
    ## Create gender time variables ----
    # For each gender variable, count number of rows where variable = 1.
    # Divide this number by total number of rows where gender is non-missing.
    # Create _t variables for each gender variable to hold this percentage.
    # Not perfect because the time between rows is very irregular.
  
      # Create a variable to flag if gender var is missing
        elig_gender[, genderna := is.na(gender), ]
      
      ## Create gender person time vars
        elig_gender[, ':=' (gender_female_t = rads::round2(100*sum(gender_female, na.rm = T) / length(genderna[genderna == F]), 1), 
                            gender_male_t =   rads::round2(100*sum(gender_male,   na.rm = T) / length(genderna[genderna == F]), 1))
                    , by = "KCMASTER_ID"]
      
      # Replace NA person time variables with 0
        elig_gender[is.na(gender_female_t), gender_female_t := 0]
        elig_gender[is.na(gender_male_t), gender_male_t := 0]

      
    ## Find the most recent gender variable ----
      elig_gender_recent <- setorder(copy(elig_gender), KCMASTER_ID, -act_date, na.last = T)
      elig_gender_recent <- elig_gender_recent[, myrank := 1:.N, .(KCMASTER_ID)]
      elig_gender_recent <- elig_gender_recent[myrank ==1][, myrank := NULL]  

      elig_gender_recent[, gender_recent := fcase(gender_female == 1 & gender_male == 1,  "Multiple",
                                                  gender_female == 1, "Female",
                                                  gender_male == 1,  "Male",
                                                  default = "Unknown")]
      
      elig_gender_recent <- elig_gender_recent[, .(KCMASTER_ID, gender_recent)]

      # Join gender_recent back to the main data
      elig_gender <- merge(elig_gender, elig_gender_recent, by = 'KCMASTER_ID', all = T)
      rm(elig_gender_recent)
    
    
    ## Copy all non-missing gender variable values to all rows within each ID  ----
      # First make collapsed max of genders for each ID
      elig_gender_sum <- elig_gender[, .(gender_female = suppressWarnings(max(gender_female, na.rm = T)), 
                                         gender_male = suppressWarnings(max(gender_male, na.rm = T))),
                                     by = "KCMASTER_ID"] # so this is functionally a gender_ever variable
      
      # Replace infinity values with NA (generated by max function applied to NA rows, e.g., when missing gender data for all observations of an ID)
      elig_gender_sum[is.infinite(gender_female), gender_female := NA]
      elig_gender_sum[is.infinite(gender_male), gender_male := NA]

      # Now join back to main data and overwrite existing female/male vars
      elig_gender <- merge(elig_gender[, c("gender_male", "gender_female") := NULL], elig_gender_sum, by = 'KCMASTER_ID', all = T)
      rm(elig_gender_sum)
  
  
    ## Collapse to one row per ID given we have alone or in combo EVER gender variables ----
      # First remove unwanted variables
      elig_gender[, c("act_date", "gender", "genderna") := NULL]
      elig_gender_final <- unique(elig_gender)
      rm(elig_gender)
      
      #Add in variables for multiple gender (mutually exclusive categories) and missing gender
      elig_gender_final[, gender_me := fcase(gender_female_t > 0 & gender_male_t > 0, "Multiple",
                                             gender_female == 1, "Female",
                                             gender_male == 1, "Male",
                                             default = "Unknown")]
      setcolorder(elig_gender_final, c("KCMASTER_ID", "gender_me", "gender_recent", 
                                       "gender_female", "gender_male", 
                                       "gender_female_t", "gender_male_t"))
  
  
  # PROCESS RACE DATA ----
    ## Tidy/prep race data ----
        message("Processing race/ethnicity data")
        elig_race <- pha[, c("act_date", "KCMASTER_ID", "r_aian", "r_asian", "r_black", 
                             "r_hisp", "r_nhpi", "r_white")]
        
        # Adjust names to have a race_ prefix rather than r_, to be consistent with claims
        setnames(elig_race, gsub("r_", "race_", names(elig_race)))
        setnames(elig_race, "race_hisp", "race_latino")
        
        # When race vars have value of 99, replace with NA
        for(col in grep('race_', names(elig_race), value = T)) set(elig_race, i=which(elig_race[[col]]==99), j=col, value=NA)        

        # For each race variable, count number of rows where variable = 1.
        # Divide this number by total number of rows (months) where at least one race variable is non-missing.
        # Create _t variables for each race variable to hold this percentage.

        # Create a variable to flag if all race vars are NA and Latino also 0 or NA
        elig_race[, race_na := is.na(race_aian) & is.na(race_asian) & is.na(race_black) & 
                    is.na(race_nhpi) & is.na(race_white) & is.na(race_latino), ]

    ## Create race person time vars ----
      elig_race[, ':=' (race_aian_t = rads::round2(100*sum(race_aian, na.rm = T) / length(race_na[race_na == F]), 1), 
                        race_asian_t = rads::round2(100*sum(race_asian, na.rm = T) / length(race_na[race_na == F]), 1), 
                        race_black_t = rads::round2(100*sum(race_black, na.rm = T) / length(race_na[race_na == F]), 1), 
                        race_nhpi_t = rads::round2(100*sum(race_nhpi, na.rm = T) / length(race_na[race_na == F]), 1), 
                        race_white_t = rads::round2(100*sum(race_white, na.rm = T) / length(race_na[race_na == F]), 1), 
                        race_latino_t = rads::round2(100*sum(race_latino, na.rm = T) / length(race_na[race_na == F]), 1))
                  , by = "KCMASTER_ID"]  
        
      
      # Replace NA person time variables with 0
        for(col in grep('_t$', names(elig_race), value = T)) set(elig_race, i=which(is.na(elig_race[[col]])), j=col, value=0) 
        
    ## Find most recent race ----
      elig_race_recent <- setorder(copy(elig_race), KCMASTER_ID, -act_date, na.last = T)
      elig_race_recent <- elig_race_recent[, myrank := 1:.N, .(KCMASTER_ID)]
      elig_race_recent <- elig_race_recent[myrank ==1][, myrank := NULL]
      
      elig_race_recent[, ':=' 
                       # Multiple race, Latino excluded
                       (race_recent = fcase(race_aian + race_asian + race_black + race_nhpi + race_white > 1, "Multiple",
                                             race_aian == 1,  "AI/AN",
                                             race_asian == 1,  "Asian",
                                             race_black == 1,  "Black",
                                             race_nhpi == 1,  "NH/PI",
                                             race_white == 1, "White",
                                             default = "Unknown"),
                         # Multiple race, Latino included as race
                         # Note OR condition to account for NA values in latino that may make race + latino sum to NA
                         race_eth_recent = fcase((race_aian + race_asian + race_black + race_nhpi + race_white + race_latino > 1) | 
                                                       ((race_aian + race_asian + race_black + race_nhpi + race_white) > 1),  "Multiple",
                                                     race_aian == 1, "AI/AN",
                                                     race_asian == 1, "Asian",
                                                     race_black == 1, "Black",
                                                     race_nhpi == 1, "NH/PI",
                                                     race_white == 1, "White",
                                                     race_latino == 1, "Latino",
                                                     default = "Unknown"))]
      elig_race_recent <- elig_race_recent[, c("KCMASTER_ID", "race_recent", "race_eth_recent")]
      
      # Join race_recent and race_eth_recent back to the main data
      elig_race <- merge(elig_race[], elig_race_recent, by = 'KCMASTER_ID', all = T)
      
      rm(elig_race_recent)
    
    
    ## Copy all non-missing race variable values to all rows within each ID ----
      # First make collapsed max of race for each ID ...basically a race_ever var
      elig_race_sum <- elig_race[, .(race_aian = max(race_aian, na.rm = T),
                                     race_asian = max(race_asian, na.rm = T),
                                     race_black = max(race_black, na.rm = T),
                                     race_nhpi = max(race_nhpi, na.rm = T),
                                     race_white = max(race_white, na.rm = T),
                                     race_latino = max(race_latino, na.rm = T)),
                                 by = "KCMASTER_ID"]
      
      
      #Replace infinity values with NA (generated by max function applied to NA rows)
        for(col in grep('race_', names(elig_race_sum), value = T)) set(elig_race_sum, i=which(is.infinite(elig_race_sum[[col]])), j=col, value=NA) 
  
      # Now join back to main data
      elig_race <- merge(elig_race[, c("race_aian", "race_asian", "race_black", "race_nhpi", "race_white", "race_latino") := NULL], 
                     elig_race_sum, 
                     by = 'KCMASTER_ID', all = T)

      rm(elig_race_sum)
      gc()
    
    
    ## Collapse to one row per ID given we have alone or in combo EVER race variables ----
    # First remove unwanted variables
      for(dropme in c("act_date", "race_na", "race_na_len") ){
        if(dropme %in% names(elig_race)){
          elig_race[, paste0(dropme) := NULL]
        }
      }
      
    # Get table with 1 row per ID  
    elig_race_final <- unique(elig_race)
    
    # Add in variables for multiple race (mutually exclusive categories) and missing race
    elig_race_final[, ':=' (
      # Multiple race, Latino excluded
      race_me = fcase(race_aian + race_asian + race_black + race_nhpi + race_white > 1, "Multiple",
                          race_aian == 1,  "AI/AN",
                          race_asian == 1, "Asian",
                          race_black == 1,  "Black",
                          race_nhpi == 1,  "NH/PI",
                          race_white == 1, "White",
                          default = "Unknown"),
      # Multiple race, Latino included as race
      # Note OR condition to account for NA values in latino that may make race + latino sum to NA
      race_eth_me = fcase((race_aian + race_asian + race_black + race_nhpi + race_white + race_latino > 1) | 
                                ((race_aian + race_asian + race_black + race_nhpi + race_white) > 1), "Multiple",
                              race_aian == 1, "AI/AN",
                              race_asian == 1, "Asian",
                              race_black == 1, "Black",
                              race_nhpi == 1, "NH/PI",
                              race_white == 1, "White",
                              race_latino == 1, "Latino",
                              default = "Unknown"))]
    
    # Set up race_unk variable
    elig_race_final[, race_unk := ifelse(race_me == "Unknown", 1L, 0L)]
    elig_race_final[, race_eth_unk := ifelse(race_eth_me == "Unknown", 1L, 0L)]
    
    setcolorder(elig_race_final, c("KCMASTER_ID", "race_me", "race_eth_me",
                                   "race_recent", "race_eth_recent",
                                   "race_aian", "race_asian", "race_black",
                                   "race_latino", "race_nhpi", "race_white", 
                                   "race_unk", "race_eth_unk", 
                                   "race_aian_t", "race_asian_t", "race_black_t",
                                   "race_nhpi_t", "race_white_t", "race_latino_t"))
    
    #Drop temp table
    rm(elig_race)
    
  
  # JOIN ALL TABLES ----
  message("Bringing it all together")
  elig_demo_final <- Reduce(function(df1, df2) 
                              merge(df1, df2, by = "KCMASTER_ID", all.x = TRUE), 
                              list(elig_gender_final, elig_race_final, elig_dob, elig_admit, pha.xwalk))
  setcolorder(elig_demo_final, c('KCMASTER_ID', 'id_apde'))
  
 
  # Add in date for last run
  elig_demo_final[, last_run := Sys.time()]
  
  
  # LOAD TO SQL SERVER ----
  message("Loading to SQL")
  
  # LOAD YAML file (and create it if necessary)
  yaml_file <- paste0(here::here(), "/etl/stage/load_stage_pha_demo.yaml")
  if(file.exists(yaml_file)){yaml_config <- yaml::read_yaml(yaml_file)}
  
  if(!file.exists(yaml_file) || !identical( names(yaml_config$vars), names(elig_demo_final))){
    message("Generating a new yaml file because one does not exist or the data structure has changed since the previous run.")
    # create basic yaml
    tempyaml <- generate_yaml(mydt = elig_demo_final,
                  datasource = 'PHA',
                  schema = to_schema,
                  table = to_table)
    # modify yaml
    tempvars <- (gsub("NUMERIC\\(38,5\\)", "NUMERIC(4,1)", tempyaml$vars)) # want to limit to three integers and 1 decimal point for xxx_t vars
    names(tempvars) <- names(tempyaml$vars)
    tempyaml$vars <- as.list(tempvars)
    # export yaml
    yaml::write_yaml(x = tempyaml, file = paste0(here::here(), "/etl/stage/load_stage_pha_demo.yaml"))
  } else {
    message("\U0001f642 Your yaml file looks good!")
  }

  # Write data
  housing::chunk_loader(DTx = as.data.frame(elig_demo_final), 
                        connx = conn, 
                        chunk.size = 10000, 
                        schemax = to_schema, 
                        tablex = to_table, 
                        overwritex = T, 
                        appendx = F, 
                        field.typesx = unlist(yaml_config$vars))
                        
  # Quick QA
  sqlcount <- odbc::dbGetQuery(conn, paste0("SELECT count(*) FROM ", to_schema, ".", to_table))
  if(sqlcount == nrow(elig_demo_final)){message("\U0001f642 It looks like all of your data loaded to SQL!")
    }else{warning("\n\U00026A0 The number of rows in `elig_demo_final` are not the same as those in the SQL table.")}
}
  