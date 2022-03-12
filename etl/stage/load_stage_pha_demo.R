#### CODE TO CREATE CONSOLIDATED NON_TIME VARYING DEMOGRAPHICS
# Alastair Matheson, PHSKC (APDE)
#
# 2021-06

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
  
  
  # BRING IN DATA AND COMBINE ----
  kcha <- dbGetQuery(
    conn,
    glue_sql(
      "SELECT DISTINCT b.id_kc_pha, a.agency, a.act_date, a.admit_date, a.dob, a.gender, 
      a.r_aian, a.r_asian, a.r_black, a.r_hisp, a.r_nhpi, a.r_white
      FROM 
      (SELECT id_hash, agency, act_date, admit_date, dob, gender,  
        r_aian, r_asian, r_black, r_hisp, r_nhpi, r_white  
        FROM {`from_schema`}.{DBI::SQL(paste0(from_table, 'kcha'))}) a
      LEFT JOIN
      (SELECT id_hash, id_kc_pha FROM {`id_schema`}.{`id_table`}) b
      ON a.id_hash = b.id_hash",
      .con = conn))
  
  sha <- dbGetQuery(
    conn,
    glue_sql(
      "SELECT DISTINCT b.id_kc_pha, a.agency, a.act_date, a.admit_date, 
      a.dob, a.gender, a.r_aian, a.r_asian, a.r_black, a.r_hisp, a.r_nhpi, a.r_white
      FROM 
      (SELECT id_hash, agency, act_date, admit_date, dob, gender, 
        r_aian, r_asian, r_black, r_hisp, r_nhpi, r_white  
        FROM {`from_schema`}.{DBI::SQL(paste0(from_table, 'sha'))}) a
      LEFT JOIN
      (SELECT id_hash, id_kc_pha FROM {`id_schema`}.{`id_table`}) b
      ON a.id_hash = b.id_hash",
      .con = conn))
  
  
  pha <- setDT(bind_rows(kcha, sha) %>% distinct) 
  # Remove the row is missing id_kc_pha since we won't be able to join on this
  pha <- pha[!is.na(id_kc_pha)]

  
  # WORK ON DOB ----
  # Take the most common DOB per ID (use most recent for ties)
  message("Processing DOB data")
  elig_dob <- pha[, c("id_kc_pha", "act_date", "dob")]
  
  # Change likely incorrect DOBs to be correct century
  # May need to tweak cutoff points as years go on
  elig_dob[year(dob) > 2030 | year(dob) < 1000, dob := as.Date(format(dob, "19%y-%m-%d"))]
  pha[between(year(dob), 1026, 1880), dob := as.Date(format(dob, "19%y-%m-%d"))]
  elig_dob[between(year(dob), 1000, 1025), dob := as.Date(format(dob, "20%y-%m-%d"))]
  
  # Count number of times each DOB appears for an ID
  elig_dob[, dob_cnt := .N, by = c("id_kc_pha", "dob")]
  elig_dob <- elig_dob[elig_dob[order(id_kc_pha, -dob_cnt, -act_date), .I[1], by = "id_kc_pha"]$V1]
  
  # Keep relevant columns
  elig_dob[, c("act_date", "dob_cnt") := NULL]
  elig_dob <- unique(elig_dob)
  
  
  # WORK ON ADMIT DATE ----
  # The admit date column does not always reflect when a person first entered housing support
  # (e.g., if a person switches programs it might reset)
  # Also want to derive an empirical start date using the first act_date for each person
  
  message("Processing date person entered KCHA/SHA")
  elig_admit <- pha[, c("id_kc_pha", "agency", "act_date", "admit_date")]
  
  # Take the earliest date per ID by overall and each PHA
  elig_admit_all <- elig_admit[, .(admit_date_all = min(admit_date, na.rm = T)), by = "id_kc_pha"]
  elig_admit_kcha <- elig_admit[agency == "KCHA", .(admit_date_kcha = min(admit_date, na.rm = T)), by = "id_kc_pha"]
  elig_admit_sha <- elig_admit[agency == "SHA", .(admit_date_sha = min(admit_date, na.rm = T)), by = "id_kc_pha"]
  
  # Bring back into a single place
  elig_admit <- Reduce(function(x,y) y[x, on = "id_kc_pha"], list(elig_admit_all, elig_admit_kcha, elig_admit_sha))
  
  rm(elig_admit_all, elig_admit_kcha, elig_admit_sha)
  
  
  # WORK ON GENDER ----
  message("Processing gender data")
  elig_gender <- pha[, c("id_kc_pha", "act_date", "gender")]
  
  ## Create alone or in combination gender variables ----
  elig_gender[, ':=' (gender_female = ifelse(str_detect(toupper(gender), "(^F|FEMALE)"), 1, 0),
                      gender_male = ifelse(str_detect(toupper(gender), "(^M|^MALE)"), 1, 0))]
  
  
  # For each gender variable, count number of rows where variable = 1.
  # Divide this number by total number of rows where gender is non-missing.
  # Create _t variables for each gender variable to hold this percentage.
  # Not perfect because the time between rows is very irregular.
  
  # Create a variable to flag if gender var is missing
  elig_gender[, genderna := is.na(gender), ]
  
  ## Create gender person time vars ----
  elig_gender[, ':=' (gender_female_t = round(length(gender_female[gender_female == 1 & !is.na(gender_female)]) / 
                                                length(genderna[genderna == FALSE]) * 100, 1),
                      gender_male_t = round(length(gender_male[gender_male == 1 & !is.na(gender_male)]) / 
                                              length(genderna[genderna == FALSE]) * 100, 1))
              , by = "id_kc_pha"]
  
  
  # Replace NA person time variables with 0
  elig_gender[, c("gender_female_t", "gender_male_t") := 
                list(recode(gender_female_t, .missing = 0),
                     recode(gender_male_t, .missing = 0))
              , ]
  
  
  ## Find the most recent gender variable ----
  elig_gender_recent <- elig_gender[elig_gender[order(id_kc_pha, -act_date), .I[1], by = "id_kc_pha"]$V1]
  elig_gender_recent[, gender_recent := case_when(gender_female == 1 & gender_male == 1 ~ "Multiple",
                                                  gender_female == 1 ~ "Female",
                                                  gender_male == 1 ~ "Male",
                                                  TRUE ~ "Unknown")]
  elig_gender_recent[, c("act_date", "gender", "gender_female", "gender_male",
                         "genderna", "gender_female_t", "gender_male_t") := NULL]
  
  # Join gender_recent back to the main data
  elig_gender[elig_gender_recent, gender_recent := i.gender_recent, on = "id_kc_pha"]
  
  rm(elig_gender_recent)
  
  
  ## Copy all non-missing gender variable values to all rows within each ID  ----
  # First make collapsed max of genders for each ID
  elig_gender_sum <- elig_gender[, .(gender_female = max(gender_female, na.rm = T), 
                                     gender_male = max(gender_male, na.rm = T)),
                                 by = "id_kc_pha"]
  # Replace infinity values with NA (generated by max function applied to NA rows)
  cols <- c("gender_female", "gender_male")
  elig_gender_sum[, (cols) := 
                    lapply(.SD, function(x)
                      replace(x, is.infinite(x), NA)), 
                  .SDcols = cols]
  
  
  # Now join back to main data and overwrite existing female/male vars
  elig_gender[elig_gender_sum, c("gender_female", "gender_male") := list(i.gender_female, i.gender_male), 
              on = "id_kc_pha"]
  rm(elig_gender_sum)
  
  
  ## Collapse to one row per ID given we have alone or in combo EVER gender variables ----
  # First remove unwanted variables
  elig_gender[, c("act_date", "gender", "genderna") := NULL]
  elig_gender_final <- unique(elig_gender)
  
  #A dd in variables for multiple gender (mutually exclusive categories) and missing gender
  elig_gender_final[, gender_me := case_when(gender_female_t > 0 & gender_male_t > 0 ~ "Multiple",
                                             gender_female == 1 ~ "Female",
                                             gender_male == 1 ~ "Male",
                                             TRUE ~ "Unknown")]
  setcolorder(elig_gender_final, c("id_kc_pha", "gender_me", "gender_recent", 
                                   "gender_female", "gender_male", 
                                   "gender_female_t", "gender_male_t"))
  
  
  # Drop temp table
  rm(elig_gender)
  
  
  # PROCESS RACE DATA ----
  message("Processing race/ethnicity data")
  elig_race <- pha[, c("act_date", "id_kc_pha", "r_aian", "r_asian", "r_black", 
                       "r_hisp", "r_nhpi", "r_white")]
  
  # Adjust names to have a race_ prefix rather than r_, to be consistent with claims
  setnames(elig_race, gsub("r_", "race_", names(elig_race)))
  setnames(elig_race, "race_hisp", "race_latino")
  
  
  # For each race variable, count number of rows where variable = 1.
  # Divide this number by total number of rows (months) where at least one race variable is non-missing.
  # Create _t variables for each race variable to hold this percentage.
  
  # Create a variable to flag if all race vars are NA and Latino also 0 or NA
  elig_race[, race_na := is.na(race_aian) & is.na(race_asian) & is.na(race_black) & 
              is.na(race_nhpi) & is.na(race_white) & is.na(race_latino), ]
  
  
  # Create another var to count number of NA rows per ID
  # (saves having to calculate it each time below)
  elig_race[, race_na_len := length(race_na[race_na == FALSE]), by = "id_kc_pha"]
  
  
  ## Create race person time vars ----
  elig_race[, ':=' (
    race_aian_t = round(length(race_aian[race_aian == 1 & !is.na(race_aian)]) / 
                          race_na_len * 100, 1),
    race_asian_t = round(length(race_asian[race_asian == 1 & !is.na(race_asian)]) / 
                           race_na_len * 100, 1),
    race_black_t = round(length(race_black[race_black == 1 & !is.na(race_black)]) / 
                           race_na_len * 100, 1),
    race_nhpi_t = round(length(race_nhpi[race_nhpi == 1 & !is.na(race_nhpi)]) / 
                          race_na_len * 100, 1),
    race_white_t = round(length(race_white[race_white == 1 & !is.na(race_white)]) / 
                           race_na_len * 100, 1),
    race_latino_t = round(length(race_latino[race_latino == 1 & !is.na(race_latino)]) / 
                            race_na_len * 100, 1)
  )
  , by = "id_kc_pha"]
  
  
  # Replace NA person time variables with 0
  cols <- c("race_aian_t", "race_asian_t", "race_black_t", 
            "race_nhpi_t", "race_white_t", "race_latino_t")
  elig_race[, (cols) := lapply(.SD, function(x) recode(x, .missing = 0)), 
            .SDcols = cols]
  
  
  ## Find most recent race ----
  elig_race_recent <- elig_race[elig_race[order(id_kc_pha, -act_date), .I[1], by = "id_kc_pha"]$V1]
  
  elig_race_recent[, ':=' 
                   # Multiple race, Latino excluded
                   (race_recent = case_when(race_aian + race_asian + race_black + 
                                              race_nhpi + race_white > 1  ~ "Multiple",
                                            race_aian == 1 ~ "AI/AN",
                                            race_asian == 1 ~ "Asian",
                                            race_black == 1 ~ "Black",
                                            race_nhpi == 1 ~ "NH/PI",
                                            race_white == 1 ~ "White",
                                            TRUE ~ "Unknown"),
                     # Multiple race, Latino included as race
                     # Note OR condition to account for NA values in latino that may make race + latino sum to NA
                     race_eth_recent = case_when((race_aian + race_asian + race_black + 
                                                    race_nhpi + race_white + race_latino > 1) | 
                                                   ((race_aian + race_asian + race_black + 
                                                       race_nhpi + race_white) > 1)  ~ "Multiple",
                                                 race_aian == 1 ~ "AI/AN",
                                                 race_asian == 1 ~ "Asian",
                                                 race_black == 1 ~ "Black",
                                                 race_nhpi == 1 ~ "NH/PI",
                                                 race_white == 1 ~ "White",
                                                 race_latino == 1 ~ "Latino",
                                                 TRUE ~ "Unknown"))]
  elig_race_recent <- elig_race_recent[, c("id_kc_pha", "race_recent", "race_eth_recent")]
  
  # Join race_recent and race_eth_recent back to the main data
  elig_race[elig_race_recent, ':=' (race_recent = i.race_recent,
                                    race_eth_recent = i.race_eth_recent), 
            on = "id_kc_pha"]
  
  rm(elig_race_recent)
  
  
  ## Copy all non-missing race variable values to all rows within each ID ----
  # First make collapsed max of race for each ID
  elig_race_sum <- elig_race[, .(race_aian = max(race_aian, na.rm = T),
                                 race_asian = max(race_asian, na.rm = T),
                                 race_black = max(race_black, na.rm = T),
                                 race_nhpi = max(race_nhpi, na.rm = T),
                                 race_white = max(race_white, na.rm = T),
                                 race_latino = max(race_latino, na.rm = T)),
                             by = "id_kc_pha"]
  
  
  #Replace infinity values with NA (generated by max function applied to NA rows)
  cols <- c("race_aian", "race_asian", "race_black", 
            "race_nhpi", "race_white", "race_latino")
  elig_race_sum[, (cols) := lapply(.SD, function(x) replace(x, is.infinite(x), NA)), 
                .SDcols = cols]
  # Now join back to main data
  elig_race[elig_race_sum, c("race_aian", "race_asian", "race_black", 
                             "race_nhpi", "race_white", "race_latino") := 
              list(i.race_aian, i.race_asian, i.race_black, 
                   i.race_nhpi, i.race_white, i.race_latino), 
            on = "id_kc_pha"]
  rm(elig_race_sum)
  gc()
  
  
  ## Collapse to one row per ID given we have alone or in combo EVER race variables ----
  # First remove unwanted variables
  elig_race[, c("act_date", "race_na", "race_na_len") := NULL]
  elig_race_final <- unique(elig_race)
  
  # Add in variables for multiple race (mutually exclusive categories) and missing race
  elig_race_final[, ':=' (
    # Multiple race, Latino excluded
    race_me = case_when(race_aian + race_asian + race_black + 
                          race_nhpi + race_white > 1  ~ "Multiple",
                        race_aian == 1 ~ "AI/AN",
                        race_asian == 1 ~ "Asian",
                        race_black == 1 ~ "Black",
                        race_nhpi == 1 ~ "NH/PI",
                        race_white == 1 ~ "White",
                        TRUE ~ "Unknown"),
    # Multiple race, Latino included as race
    # Note OR condition to account for NA values in latino that may make race + latino sum to NA
    race_eth_me = case_when((race_aian + race_asian + race_black + 
                               race_nhpi + race_white + race_latino > 1) | 
                              ((race_aian + race_asian + race_black + 
                                  race_nhpi + race_white) > 1)  ~ "Multiple",
                            race_aian == 1 ~ "AI/AN",
                            race_asian == 1 ~ "Asian",
                            race_black == 1 ~ "Black",
                            race_nhpi == 1 ~ "NH/PI",
                            race_white == 1 ~ "White",
                            race_latino == 1 ~ "Latino",
                            TRUE ~ "Unknown"))]
  
  # Set up race_unk variable
  elig_race_final[, race_unk := ifelse(race_me == "Unknown", 1L, 0L)]
  elig_race_final[, race_eth_unk := ifelse(race_eth_me == "Unknown", 1L, 0L)]
  
  setcolorder(elig_race_final, c("id_kc_pha", "race_me", "race_eth_me",
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
  elig_demo_final <- list(elig_dob, elig_admit, elig_gender_final, elig_race_final) %>%
    Reduce(function(df1, df2) left_join(df1, df2, by = "id_kc_pha"), .)
 
  # Add in date for last run
  elig_demo_final <- elig_demo_final %>% mutate(last_run = Sys.time())
  
  
  # LOAD TO SQL SERVER ----
  message("Loading to SQL")
  
  # Set up cols
  col_types <- c("id_kc_pha" = "char(10)")
  
  # Write data
  odbc::dbWriteTable(conn, 
                     name = DBI::Id(schema = to_schema, table = to_table), 
                     value = as.data.frame(elig_demo_final),
                     overwrite = T, append = F,
                     field.types = col_types,
                     batch_rows = 10000) 
}
  