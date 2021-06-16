#### CODE TO CLEAN AND LOAD SEATTLE HOUSING AUTHORITY PORTFOLIO DATA
# Alastair Matheson, PHSKC (APDE)
#
# 2021-06
#
# This table contains a reference list that maps building IDs and property IDs to property name and portfolio type.
# Should only needed to be loaded to the database once as it does not change.
#
# Assumes loading to prod version of HHSAW
#
# Process for making this ref table:
# 1) Bring in raw data
# 2) Load to ref schema of PHA data

# BRING IN DATA ----
# Bring in data
sha_portfolios <- data.table::fread("//phdata01/DROF_DATA/DOH DATA/Housing/SHA/Original_data/sha_buildings_programs_and_portfolios_2021-04-30.csv",
                                   na.strings = c("NA", "", "NULL", "N/A", "."), 
                                   stringsAsFactors = F)

# LOAD TO SQL ----
db_hhsaw_prod <- DBI::dbConnect(odbc::odbc(), "hhsaw_prod", uid = keyring::key_list("hhsaw")[["username"]])
DBI::dbWriteTable(conn = db_hhsaw_prod,
                  name = DBI::Id(schema = "pha", table = "ref_sha_portfolio_codes"),
                  value = as.data.frame(sha_portfolios),
                  overwrite = T)

# CLEAN UP ----
rm(sha_portfolios)
rm(db_hhsaw_prod)
