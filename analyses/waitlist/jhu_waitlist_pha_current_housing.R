##
## Script name: jhu_waitlist_sha_current_housing
##
## Purpose of script: Create list of waitlist members who possible are already 
##                         receiving housing assistance
##
## Author: Alastair Matheson, Public Health - Seattle & King County
## Date Created: 2022-07-22
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
## Waitlist data ----
waitlist <- dbGetQuery(db_hhsaw,
                       "SELECT a.*, b.id_kc_pha, c.id_apde 
                         FROM
                       (SELECT DISTINCT * FROM pha.stage_pha_waitlist) a
                       LEFT JOIN
                       (SELECT DISTINCT id_hash, id_kc_pha FROM pha.final_identities) b
                       ON a.id_hash = b.id_hash
                       LEFT JOIN 
                       (SELECT DISTINCT id_apde, id_kc_pha FROM claims.final_xwalk_apde_ids) c
                       ON b.id_kc_pha = c.id_kc_pha")


# PULL RELEVANT DATA ----
output <- waitlist %>% 
  distinct(agency, app_num, id_apde, subs_recipient, subs_type)


# EXPORT DATA ----
write.csv(output, "//dchs-shares01/DCHSDATA/DCHSPHClaimsData/Analyses/Alastair/jhu_waitlist_output/waitlist_current_housing.csv",
          row.names = F, na = "")


