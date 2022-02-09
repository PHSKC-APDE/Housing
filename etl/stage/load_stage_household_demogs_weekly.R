## Script name: load_stage_household_demogs_daily.R
##
## Purpose of script: Set up a household demographic table for each day/week. 
## Can be used for the HUD HEARS Study to get demographics for exits.
##
## Author: Alastair Matheson, Public Health - Seattle & King County
## Date Created: 2021-12-07
## Email: alastair.matheson@kingcounty.gov
##
## Items to add to table:
##   - HoH ID
##   - Date
##   - Agency
##   - Program type
##   - Subsidy type
##   - Voucher type
##   - Portfolio
##   - Household size
##   - Any disability in household
##   - HoH disability
##   - Any children in household
##   - Single caregiver
##   - HoH age
##   - HoH is senior
##   - Any senior in household
##   - Geo_hash_clean
##   - Geo_tract
##

# SET OPTIONS AND BRING IN PACKAGES ----
options(scipen = 6, digits = 4, warning.length = 8170)

if (!require("pacman")) {install.packages("pacman")}
pacman::p_load(tidyverse, odbc, glue, data.table, lubridate)

db_hhsaw <- DBI::dbConnect(odbc::odbc(),
                           driver = "ODBC Driver 17 for SQL Server",
                           server = "tcp:kcitazrhpasqlprp16.azds.kingcounty.gov,1433",
                           database = "hhs_analytics_workspace",
                           uid = keyring::key_list("hhsaw")[["username"]],
                           pwd = keyring::key_get("hhsaw", keyring::key_list("hhsaw")[["username"]]),
                           Encrypt = "yes",
                           TrustServerCertificate = "yes",
                           Authentication = "ActiveDirectoryPassword")


# BRING IN DATA ----
pha_demo <- setDT(dbGetQuery(db_hhsaw, "SELECT * FROM pha.final_demo"))

pha_timevar <- setDT(dbGetQuery(db_hhsaw, "SELECT a.*, c.geo_tractce10 FROM
                                           (SELECT * FROM pha.final_timevar) a
                                           LEFT JOIN
                                           (SELECT DISTINCT geo_hash_clean, geo_hash_geocode FROM ref.address_clean) b
                                           ON a.geo_hash_clean = b.geo_hash_clean
                                           LEFT JOIN
                                           (SELECT DISTINCT geo_hash_geocode, geo_tractce10 FROM ref.address_geocode) c
                                           ON b.geo_hash_geocode = c.geo_hash_geocode"))




# SET UP FUNCTIONS TO SUMMARIZE BY DAY ----
hh_window_f <- function(date = NULL) {
  # Restrict to those present on that day
  daily <- pha_timevar[from_date <= date & to_date >= date]
  
  # Join to pha_demo and add relevant variables
  daily[, dob := pha_demo[.SD, on = "id_kc_pha", x.dob]]
  daily[, admit_date_all := pha_demo[.SD, on = "id_kc_pha", x.admit_date_all]]
  daily[, admit_date_kcha := pha_demo[.SD, on = "id_kc_pha", x.admit_date_kcha]]
  daily[, admit_date_sha := pha_demo[.SD, on = "id_kc_pha", x.admit_date_sha]]
  daily[, `:=` (age_yr = floor(interval(start = dob, end = date) / years(1)),
                length_all = round(interval(start = admit_date_all, end = date) / years(1), 1),
                length_pha = ifelse(agency == "KCHA",
                                    round(interval(start = admit_date_kcha, end = date) / years(1), 1),
                                    round(interval(start = admit_date_sha, end = date) / years(1), 1)),
                length_period = round(interval(start = period_start, end = date) / years(1), 1)
                )]
  daily[, child := case_when(age_yr < 18 ~ 1L, age_yr >= 18 ~ 0L)]
  daily[, adult := case_when(age_yr >= 18 ~ 1L, age_yr < 18 ~ 0L)]
  daily[, senior := case_when(age_yr >= 62 ~ 1L, age_yr < 62 ~ 0L)]
  daily[, hh_senior := case_when(id_kc_pha != hh_id_kc_pha | is.na(senior) ~ NA_integer_,
                                 id_kc_pha == hh_id_kc_pha & senior == 1 ~ 1L,
                                 id_kc_pha == hh_id_kc_pha & senior != 1 ~ 0L)]
  daily[, hh_disability := case_when(id_kc_pha != hh_id_kc_pha | is.na(disability) ~ NA_integer_,
                                     id_kc_pha == hh_id_kc_pha & disability == 1 ~ 1L,
                                     id_kc_pha == hh_id_kc_pha & disability != 1 ~ 0L)]
  
  # Sum up household structure
  hh_structure <- daily[, .(hh_size = uniqueN(id_kc_pha),
                            n_child = sum(child, na.rm = T),
                            n_adult = sum(adult, na.rm = T),
                            n_senior = sum(senior, na.rm = T),
                            n_disability = sum(disability, na.rm = T)),
                        by = "hh_id_kc_pha"]
  hh_structure[, single_caregiver := ifelse(n_child >= 1 & n_adult == 1, 1L, 0L)]

  # Restrict to only head of household then add in household structure
  hh_final <- daily[id_kc_pha == hh_id_kc_pha]
  hh_final[, date := date]
  
  hh_final[hh_structure, on = "hh_id_kc_pha", 
           `:=` (hh_size = i.hh_size,
                 n_child = i.n_child,
                 n_adult = i.n_adult,
                 n_senior = i.n_senior,
                 n_disability = i.n_disability,
                 single_caregiver = i.single_caregiver)]
  
  hh_final[, .(hh_id_kc_pha, date, agency, major_prog, subsidy_type, prog_type, operator_type, 
               vouch_type_final, portfolio_final, geo_hash_clean, geo_tractce10,
               hh_size, hh_senior, hh_disability, n_child, n_adult, n_senior, n_disability,
               single_caregiver, length_all, length_pha, length_period
               )]
}


# SET UP DATES TO ROLL OVER AND RUN FUNCTION ----
# Just 6 months of 2012 generates a DF 2.5m rows when run for each day (and takes >2 mins)
# Run for each week then join to the most recent record before the relevant date
dates <- seq(ymd('2012-01-01'),ymd('2020-12-31'), by = 'weeks')
# dates <- seq(ymd('2012-01-01'),ymd('2012-03-31'), by = 'weeks')

system.time(hh_demogs <- bind_rows(map(dates, hh_window_f)))


# LOAD TO SQL ----
col_types <- c("id_kc_pha" = "char(10)", "geo_hash_clean" = "char(64)")

# Split into smaller tables to avoid SQL connection issues
start <- 1L
max_rows <- 100000L
cycles <- ceiling(nrow(hh_demogs)/max_rows)

lapply(seq(start, cycles), function(i) {
  start_row <- ifelse(i == 1, 1L, max_rows * (i-1) + 1)
  end_row <- min(nrow(hh_demogs), max_rows * i)
  
  message("Loading cycle ", i, " of ", cycles)
  if (i == 1) {
    dbWriteTable(db_hhsaw,
                 name = DBI::Id(schema = "pha", table = "stage_hh_demogs_weekly"),
                 value = as.data.frame(hh_demogs[start_row:end_row]),
                 overwrite = T, append = F)
  } else {
    dbWriteTable(db_hhsaw,
                 name = DBI::Id(schema = "pha", table = "stage_hh_demogs_weekly"),
                 value = as.data.frame(hh_demogs[start_row:end_row]),
                 overwrite = F, append = T)
  }
})


# Add CCS
DBI::dbExecute(db_hhsaw, "CREATE CLUSTERED COLUMNSTORE INDEX stage_hh_demogs_weekly_idx 
          ON pha.stage_hh_demogs_weekly")
