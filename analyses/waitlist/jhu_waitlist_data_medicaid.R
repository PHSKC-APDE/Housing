## Script name: jhu_waitlist_data_medicaid
##
## Purpose of script: Pull Medicaid data for selected PHA IDs
##
## Author: Alastair Matheson, Public Health - Seattle & King County
## Date Created: 2022-06-27
## Email: alastair.matheson@kingcounty.gov
##
## Notes:
##   
##

# SET OPTIONS AND BRING IN PACKAGES ----
options(scipen = 6, digits = 4, warning.length = 8170)

if (!require("pacman")) {install.packages("pacman")}
pacman::p_load(tidyverse, odbc, glue, data.table, lubridate, claims)

# Connect to HHSAW
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
## ID crosswalk ----
# Reran the xwalk code because some expected pairs were not matching.
# Join on id_hash necause the id_apde values have changed
# This is stopgap until stage moves to final and all downstream tables are rerun
xwalk_old <- dbGetQuery(db_hhsaw, "SELECT DISTINCT id_apde, id_hash FROM claims.final_xwalk_apde_ids
                        WHERE id_apde IS NOT NULL")
xwalk_new <- dbGetQuery(db_hhsaw, "SELECT DISTINCT id_apde, id_kc_pha, id_mcaid, id_hash FROM claims.stage_xwalk_apde_ids")

# List of IDs
load("M:/Analyses/Alastair/jhu_waitlist_output/IDsForMedicaid_20220622.rda")
ids <- data.frame("id_apde" = ids)


mcaid_ids <- left_join(ids, xwalk_old, by = "id_apde") %>%
  left_join(., distinct(xwalk_new, id_hash, id_apde), by = "id_hash") %>%
  select(-id_hash) %>%
  distinct() %>%
  left_join(., distinct(xwalk_new, id_apde, id_mcaid) %>% filter(!is.na(id_mcaid)),
            by = c("id_apde.y" = "id_apde")) %>%
  select(-id_apde.y) %>% rename("id_apde" = "id_apde.x") %>%
  distinct()



# MEDICAID TABLES ----
# Restrict to IDs also found in the waitlist data

## Medicaid enrollment ----
mcaid_enroll <- elig_timevar_collapse(conn = db_hhsaw,
                                      server = "hhsaw", source = "mcaid",
                                      dual = T, full_benefit = T, cov_type = T,
                                      geo_add1 = T, geocode_vars = T,
                                      ids = unique(mcaid_ids$id_mcaid[!is.na(mcaid_ids$id_mcaid)]))


# Load IDs to a temp SQL table for joining to mcaid data ----
try(dbRemoveTable(db_hhsaw, "##temp_ids", temporary = T), silent = T)
dbWriteTable(db_hhsaw,
             "##temp_ids",
             mcaid_ids,
             overwrite = T)

# Add index to id and from_date for faster join
DBI::dbExecute(db_hhsaw, "CREATE NONCLUSTERED INDEX temp_ids_id ON ##temp_ids (id_mcaid)")


## Medicaid demographics ----
mcaid_demog <- dbGetQuery(db_hhsaw,
                          "SELECT a.id_apde, b.*
                           FROM 
                        (SELECT id_apde, id_mcaid FROM ##temp_ids WHERE id_mcaid IS NOT NULL) a
                        INNER JOIN
                        (SELECT * FROM claims.final_mcaid_elig_demo) b
                        ON a.id_mcaid = b.id_mcaid")

## Medicaid header ----
mcaid_header <- dbGetQuery(db_hhsaw,
                           "SELECT a.id_apde, b.*
                           FROM 
                        (SELECT id_apde, id_mcaid FROM ##temp_ids WHERE id_mcaid IS NOT NULL) a
                        INNER JOIN
                        (SELECT * FROM claims.final_mcaid_claim_header
                        WHERE sud_dx_rda_any <> 1 AND from_service_date >= '2013-01-01') b
                        ON a.id_mcaid = b.id_mcaid")

# Remove columns that don't need to be transferred
mcaid_header <- mcaid_header %>%
  select(-id_mcaid, -ends_with("nyu"), -ed_avoid_ca, -ed_avoid_ca_nohosp, -sud_dx_rda_any)


## Medicaid claim ICDCM header ----
mcaid_icdcm_header <- dbGetQuery(db_hhsaw,
                                 "SELECT a.id_apde, b.*
                                 FROM 
                                 (SELECT id_apde, id_mcaid FROM ##temp_ids WHERE id_mcaid IS NOT NULL) a
                                 INNER JOIN
                                 (SELECT * FROM claims.final_mcaid_claim_icdcm_header
                                 WHERE first_service_date >= '2013-01-01') b
                                 ON a.id_mcaid = b.id_mcaid")

# Remove any claims that were dropped from the header table
mcaid_icdcm_header <- inner_join(distinct(mcaid_header, claim_header_id),
                                 mcaid_icdcm_header,
                                 by = "claim_header_id") %>%
  select(-id_mcaid)


## Medicaid claim line ----
mcaid_line <- dbGetQuery(db_hhsaw,
                         "SELECT a.id_apde, b.*
                           FROM 
                           (SELECT id_apde, id_mcaid FROM ##temp_ids WHERE id_mcaid IS NOT NULL) a
                           INNER JOIN
                           (SELECT * FROM claims.final_mcaid_claim_line
                           WHERE first_service_date >= '2013-01-01') b
                           ON a.id_mcaid = b.id_mcaid")

# Remove any claims that were dropped from the header table
mcaid_line <- inner_join(distinct(mcaid_header, claim_header_id),
                         mcaid_line,
                         by = "claim_header_id") %>%
  select(-id_mcaid)


## Medicaid pharmacy claims ----
mcaid_pharm <- dbGetQuery(db_hhsaw,
                          "SELECT a.id_apde, b.*
                           FROM 
                           (SELECT id_apde, id_mcaid FROM ##temp_ids WHERE id_mcaid IS NOT NULL) a
                           INNER JOIN
                           (SELECT * FROM claims.final_mcaid_claim_pharm
                           WHERE rx_fill_date >= '2013-01-01') b
                         ON a.id_mcaid = b.id_mcaid")

# Remove any claims that were dropped from the header table
mcaid_pharm <- inner_join(distinct(mcaid_header, claim_header_id),
                          mcaid_pharm,
                          by = "claim_header_id") %>%
  select(-id_mcaid)


## Medicaid claim procedure codes ----
mcaid_procedure <- dbGetQuery(db_hhsaw,
                              "SELECT a.id_apde, b.*
                           FROM 
                           (SELECT id_apde, id_mcaid FROM ##temp_ids WHERE id_mcaid IS NOT NULL) a
                           INNER JOIN
                           (SELECT * FROM claims.final_mcaid_claim_procedure
                           WHERE first_service_date >= '2013-01-01') b
                         ON a.id_mcaid = b.id_mcaid")

# Remove any claims that were dropped from the header table
mcaid_procedure <- inner_join(distinct(mcaid_header, claim_header_id),
                              mcaid_procedure,
                              by = "claim_header_id") %>%
  select(-id_mcaid)


## Medicaid Chronic Condition Warehouse table ----
mcaid_ccw <- dbGetQuery(db_hhsaw,
                        "SELECT a.id_apde, b.from_date, b.to_date, b.ccw_code, b.ccw_desc 
                           FROM 
                           (SELECT id_apde, id_mcaid FROM ##temp_ids WHERE id_mcaid IS NOT NULL) a
                           INNER JOIN
                           (SELECT DISTINCT id_mcaid, 
                           CASE WHEN from_date < '2013-01-01' THEN '2013-01-01'
                            ELSE from_date END AS from_date, 
                           to_date, ccw_code, ccw_desc
                           FROM claims.final_mcaid_claim_ccw
                           WHERE ccw_desc IN ('ccw_asthma', 'ccw_depression')) b
                          ON a.id_mcaid = b.id_mcaid")


# PREPARE FINAL OUTPUT ----
## Medicaid tables ----
mcaid_demog_output <- mcaid_demog %>% 
  mutate(age_2017 = floor(interval(start = dob, end = "2017-12-31") / years(1))) %>%
  select(-id_mcaid, -dob)

mcaid_enroll_output <- left_join(mcaid_enroll,
                                 distinct(mcaid_ids, id_apde, id_mcaid) %>%
                                   filter(!is.na(id_mcaid)),
                                 by = "id_mcaid") %>%
  select(id_apde, from_date, to_date, dual, full_benefit, cov_type, geo_tract_code) %>%
  filter(to_date >= "2013-01-01") %>%
  mutate(from_date = pmax(from_date, as.Date("2013-01-01")))

# Other tables can be exported as is


# EXPORT DATA ----
tables_for_export <- list("mcaid_demog_output" = mcaid_demog_output,
                          "mcaid_enroll_output" = mcaid_enroll_output,
                          "mcaid_header" = mcaid_header,
                          "mcaid_icdcm_header" = mcaid_icdcm_header,
                          "mcaid_line" = mcaid_line,
                          "mcaid_pharm" = mcaid_pharm,
                          "mcaid_procedure" = mcaid_procedure,
                          "mcaid_ccw" = mcaid_ccw)


lapply(names(tables_for_export), function(x) {
  message("Working on ", x)
  write.csv(tables_for_export[[x]], 
            file = paste0("//dchs-shares01/DCHSDATA/DCHSPHClaimsData/Analyses/Alastair/jhu_waitlist_output/",
                          x, ".csv"),
            na = "",
            row.names = F)
})
