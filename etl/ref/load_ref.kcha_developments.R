#### CODE TO CLEAN AND LOAD KING COUNTY HOUSING AUTHORITY DEVELOPMENT/PROPERTY DATA
# Alastair Matheson, PHSKC (APDE)
#
# 2021-06
#
# This table contains a reference list of KCHA properties.
# By joining on address, we can get the portfolio name from KCHA 50058 data.
# Should only needed to be loaded to the database once as it does not change.
#
# Process for making this ref table:
# 1) Bring in raw data and rename fields
# 2) Clean up addresses
# 3) Load to ref schema of PHA data

# LOAD LIBRARIES AND FUNCTIONS ----
library(tidyverse) # Manipulate data
library(odbc) # Read to and write from SQL
library(glue) # Safely combine SQL code
library(keyring) # Access stored credentials


# BRING IN DATA ----
# Bring in data
kcha_adds_dev <- data.table::fread(file = "//phdata01/DROF_DATA/DOH DATA/Housing/KCHA/Original_data/Development Addresses_received_2017-07-21.csv")

# Bring in field names
fields <- read.csv(file.path(here::here(), "etl/ref", "field_name_mapping.csv"))


# CLEAN UP ----
## Clean up KCHA field names ----
colnames(kcha_adds_dev) <- str_replace_all(colnames(kcha_adds_dev), "[:punct:]|[:space:]", "")
kcha_adds_dev <- data.table::setnames(kcha_adds_dev, fields$common_name[match(names(kcha_adds_dev), fields$kcha_modified)])

## Set up address fields ----
kcha_adds_dev <- kcha_adds_dev %>%
  separate(dev_city, into = c("geo_city_raw", "state_zip"), sep = ", ") %>%
  separate(state_zip, into = c("geo_state_raw", "geo_zip_raw"), sep = " ") %>%
  mutate(geo_add1_raw = dev_add,
         geo_add2_raw = case_when(dev_add == dev_add_apt ~ NA_character_,
                                  TRUE ~ str_sub(dev_add_apt, nchar(dev_add) + 1, nchar(dev_add_apt))),
         geo_add3_raw = NA_character_) %>%
  mutate(across(starts_with("geo_"), toupper)) %>%
  mutate(geo_hash_raw = as.character(toupper(openssl::sha256(paste(str_replace_na(geo_add1_raw, ''),
                                                                   str_replace_na(geo_add2_raw, ''),
                                                                   str_replace_na(geo_add3_raw, ''),
                                                                   str_replace_na(geo_city_raw, ''),
                                                                   str_replace_na(geo_state_raw, ''),
                                                                   str_replace_na(geo_zip_raw, ''),
                                                                   sep = "|"))))) %>%
  select(property_name, portfolio, geo_add1_raw, geo_add2_raw, geo_add3_raw, geo_city_raw, 
         geo_state_raw, geo_zip_raw, geo_hash_raw, dev_unit, dev_owner, dev_landlord,
         dev_unit_report, dev_unit_type, bed_cnt, portfolio_type)

## Separate out addresses ----
adds_distinct_dev <- kcha_adds_dev %>%
  select(geo_add1_raw, geo_add2_raw, geo_add3_raw, geo_city_raw, 
         geo_state_raw, geo_zip_raw, geo_hash_raw) %>%
  distinct()


# COMPARE TO EXISTING CLEAN ADDRESSES ----
## Load to a temp SQL table for cleaning ----
db_hhsaw <- dbConnect(odbc(), "hhsaw_prod", uid = keyring::key_list("hhsaw_dev")[["username"]])

try(dbRemoveTable(db_hhsaw, "##kcha_adds_dev"))
odbc::dbWriteTable(db_hhsaw,
                   name = "##kcha_adds_dev",
                   value = adds_distinct_dev,
                   overwrite = T)

## Pull in clean addresses ----
adds_already_clean_dev <- DBI::dbGetQuery(db_hhsaw,
                                      "SELECT b.* FROM 
                                 (SELECT geo_hash_raw FROM ##kcha_adds_dev) a
                                 INNER JOIN
                                 (SELECT * FROM ref.address_clean) b
                                 ON a.geo_hash_raw = b.geo_hash_raw")

## Pull in addresses that need cleaning ----
adds_to_clean_dev <- DBI::dbGetQuery(db_hhsaw,
                                 "SELECT a.* FROM 
                                 (SELECT * FROM ##kcha_adds_dev) a
                                 LEFT JOIN
                                 (SELECT geo_hash_raw, 1 AS clean
                                 FROM ref.address_clean) b
                                 ON a.geo_hash_raw = b.geo_hash_raw
                                 WHERE b.clean IS NULL")

## Load to Informatica for cleaning ----
if (nrow(adds_to_clean_dev) > 0) {
  # Add new addresses that need cleaning into the Informatica table
  timestamp_dev <- Sys.time()
  
  adds_to_clean_dev <- adds_to_clean_dev %>% mutate(geo_source = NA, timestamp = timestamp_dev)
  
  DBI::dbWriteTable(db_hhsaw, 
                    name = DBI::Id(schema = "ref", table = "informatica_address_input"),
                    value = adds_to_clean_dev,
                    overwrite = F, append = T)
}

## Retrieve cleaned addresses ----
if (nrow(adds_to_clean_dev) > 0) {
  adds_clean_dev <- dbGetQuery(db_hhsaw,
                           glue::glue_sql("SELECT * FROM ref.informatica_address_output
                           WHERE convert(varchar, timestamp, 20) = {lubridate::with_tz(timestamp_dev, 'utc')}",
                                          .con = db_hhsaw))
  
  # Keep checking each hour until the addresses have been cleaned
  while (nrow(adds_clean_dev) == 0) {
    message("Waiting on Informatica to clean addresses. Will check again in 1 hour.")
    Sys.sleep(3600)
    
    # Will need to reconnect to the DB since it will have timed out
    db_hhsaw <- dbConnect(odbc(), "hhsaw_prod", uid = keyring::key_list("hhsaw_dev")[["username"]])
    
    adds_clean_dev <- dbGetQuery(db_hhsaw,
                             glue::glue_sql("SELECT * FROM ref.informatica_address_output
                           WHERE convert(varchar, timestamp, 20) = {lubridate::with_tz(timestamp, 'utc')}",
                                            .con = db_hhsaw))
  }
  
  # Informatica seems to drop secondary designators when they start with #
  # Move over from old address
  adds_clean_dev <- adds_clean_dev %>%
    mutate(geo_add2_clean = ifelse(is.na(geo_add2_clean) & str_detect(geo_add1_raw, "^#"),
                                   geo_add1_raw, geo_add2_clean))
  
  # Tidy up some PO box messiness
  adds_clean_dev <- adds_clean_dev %>%
    mutate(geo_add1_clean = case_when((is.na(geo_add1_clean) | geo_add1_clean == "") & 
                                        !is.na(geo_po_box_clean) ~ geo_po_box_clean,
                                      TRUE ~ geo_add1_clean),
           geo_add2_clean = case_when(
             geo_add1_clean == geo_po_box_clean ~ geo_add2_clean,
             (is.na(geo_add2_clean) | geo_add2_clean == "") & !is.na(geo_po_box_clean) & 
               !is.na(geo_add1_clean) ~ geo_po_box_clean,
             !is.na(geo_add2_clean) & !is.na(geo_po_box_clean) & 
               !is.na(geo_add1_clean) ~ paste(geo_add2_clean, geo_po_box_clean, sep = " "),
             TRUE ~ geo_add2_clean)
    )
  
  # Set up variables of interest
  adds_clean_dev <- adds_clean_dev %>%
    mutate(geo_geocode_skip = 0L,
           geo_hash_clean = as.character(toupper(openssl::sha256(paste(stringr::str_replace_na(geo_add1_clean, ''), 
                                                                       stringr::str_replace_na(geo_add2_clean, ''), 
                                                                       stringr::str_replace_na(geo_city_clean, ''), 
                                                                       stringr::str_replace_na(geo_state_clean, ''), 
                                                                       stringr::str_replace_na(geo_zip_clean, ''), 
                                                                       sep = "|")))),
           geo_hash_geocode = as.character(toupper(openssl::sha256(paste(stringr::str_replace_na(geo_add1_clean, ''),  
                                                                         stringr::str_replace_na(geo_city_clean, ''), 
                                                                         stringr::str_replace_na(geo_state_clean, ''), 
                                                                         stringr::str_replace_na(geo_zip_clean, ''), 
                                                                         sep = "|")))),
           last_run = Sys.time()) %>%
    select(geo_add1_raw, geo_add2_raw, geo_add3_raw, geo_city_raw, 
           geo_state_raw, geo_zip_raw, geo_hash_raw,
           geo_add1_clean, geo_add2_clean, geo_city_clean, 
           geo_state_clean, geo_zip_clean, geo_hash_clean, geo_hash_geocode,
           geo_geocode_skip, last_run) %>%
    # Convert all blank fields to be NA
    mutate_if(is.character, list(~ ifelse(. == "", NA_character_, .)))
  
  dbWriteTable(db_hhsaw, 
               name = DBI::Id(schema = "ref",  table = "address_clean"),
               adds_clean_dev,
               overwrite = F, append = T)
  
  # Don't need to geocode at this point so skip that part
}


## Bring it all together ----
if (nrow(adds_to_clean_dev) > 0) {
  adds_final_dev <- bind_rows(adds_already_clean_dev, adds_clean_dev)
} else {
  adds_final_dev <- adds_already_clean_dev
}


## Join back to original data ----
kcha_adds_dev <- kcha_adds_dev %>%
  left_join(., select(adds_final_dev, geo_hash_raw:geo_hash_geocode),
            by = "geo_hash_raw") %>%
  select(property_name, portfolio, geo_add1_clean, geo_add2_clean, geo_city_clean, 
         geo_state_clean, geo_zip_clean, geo_hash_clean, geo_hash_geocode, 
         dev_unit, dev_owner, dev_landlord, dev_unit_report, dev_unit_type, 
         bed_cnt, portfolio_type)


# LOAD TO SQL ----
DBI::dbWriteTable(conn = db_hhsaw,
                  name = DBI::Id(schema = "pha", table = "ref_kcha_development_adds"),
                  value = as.data.frame(kcha_adds_dev),
                  overwrite = T)


# CLEAN UP ----
rm(list = ls(pattern = "_dev$"))
