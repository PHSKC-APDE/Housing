# OVERVIEW:
# Code to load SHA waitlist data to SQL
#
### Run from main_pha_waitlist_load script
# https://github.com/PHSKC-APDE/Housing/blob/main/claims_db/etl/db_loader/main_pha_waitlist_load.R
# Assumes relevant libraries are already loaded
#
#
# Alastair Matheson (PHSKC-APDE)
# alastair.matheson@kingcounty.gov


load_raw_sha_waitlist_2017 <- function(conn = NULL,
                                       to_schema = "pha",
                                       to_table = "raw_sha_waitlist_2017") {
  # BRING IN DATA ----
  # File paths are hard coded for now
  sha_waitlist <- read.csv("//phdata01/DROF_DATA/DOH DATA/Housing/SHA/Original_data/Waitlist data/SHA_HCV_2017_Lottery_data_received 2021-05-26.csv")
  
  
  ## Load to SQL ----
  dbWriteTable(conn,
               name = DBI::Id(schema = to_schema, table = to_table),
               value = sha_waitlist,
               overwrite = T)
}
