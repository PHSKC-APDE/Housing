#### CODE TO CLEAN AND LOAD KING COUNTY HOUSING AUTHORITY PORTFOLIO DATA
# Alastair Matheson, PHSKC (APDE)
#
# 2021-06
#
# This table contains a reference list that maps a property ID to property name and portfolio type.
# Should only needed to be loaded to the database once as it does not change.
#
# Process for making this ref table:
# 1) Bring in raw data and rename fields
# 2) Load to ref schema of PHA data

# BRING IN DATA ----
# Bring in data
kcha_portfolio <- openxlsx::read.xlsx("//phdata01/DROF_DATA/DOH DATA/Housing/KCHA/Original_data/Property list with project code_received_2017-07-26.xlsx")

# Bring in field names
fields <- read.csv(file.path(here::here(), "etl/ref", "field_name_mapping.csv"))


# CLEAN UP ----
## Clean up KCHA field names ----
kcha_portfolio <- data.table::setnames(kcha_portfolio, fields$common_name[match(names(kcha_portfolio), fields$kcha_modified)])
kcha_portfolio$property_name <- toupper(kcha_portfolio$property_name)


# LOAD TO SQL ----
db_hhsaw <- DBI::dbConnect(odbc::odbc(), "hhsaw_prod", uid = keyring::key_list("hhsaw_dev")[["username"]])
DBI::dbWriteTable(conn = db_hhsaw,
                  name = DBI::Id(schema = "pha", table = "ref_kcha_portfolio_codes"),
                  value = as.data.frame(kcha_portfolio),
                  overwrite = T)

# CLEAN UP ----
rm(kcha_portfolio)
rm(fields)
rm(db_hhsaw)
