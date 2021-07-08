# OVERVIEW:
# Code to load KCHA waitlist data to SQL
#
### Run from main_pha_waitlist_load script
# https://github.com/PHSKC-APDE/Housing/blob/master/claims_db/etl/db_loader/main_pha_waitlist_load.R
# Assumes relevant libraries are already loaded
#
#
# Alastair Matheson (PHSKC-APDE)
# alastair.matheson@kingcounty.gov


load_raw_kcha_waitlist_2017 <- function(conn = NULL,
                                    to_schema = "pha",
                                    to_table = "raw_kcha_waitlist_2017") {
  # BRING IN DATA ----
  # File paths are hard coded for now
  
  # This is everyone who applied for the waitlist and went into the lottery - use this one
  kcha_waitlist <- read.csv("//phdata01/DROF_DATA/DOH DATA/Housing/KCHA/Original_data/2017 waitlist data/2017 HCV Waitlist - Applicants - JHU.csv")
  
  # This file is just those who moved from the lottery application to the waitlist
  # kcha_waitlist_final <- read.csv("//phdata01/DROF_DATA/DOH DATA/Housing/KCHA/Original_data/2017 waitlist data/2017 HCV Waitlist - Final 3500 - JHU.csv")
  
  
  
  ## Load to SQL ----
  dbWriteTable(conn,
               name = DBI::Id(schema = to_schema, table = to_table),
               value = kcha_waitlist,
               overwrite = T)
}


