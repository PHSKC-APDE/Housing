###############################################################################
# OVERVIEW:
# Code to create a cleaned person table from the combined 
# King County Housing Authority and Seattle Housing Authority data sets
# Aim is to have a single row per contiguous time in a house per person
#
# STEPS:
# 01 - Process raw KCHA data and load to SQL database
# 02 - Process raw SHA data and load to SQL database
# 03 - Bring in individual PHA datasets and combine into a single file
# 04 - Deduplicate data and tidy up via matching process
# 05 - Recode race and other demographics
# 06 - Clean up addresses ### (THIS CODE) ###
# 06a - Geocode addresses
# 07 - Consolidate data rows
# 08 - Add in final data elements and set up analyses
# 09 - Join with Medicaid-Medicare eligibility & time varying data
# 10 - Set up joint housing/Medicaid analyses
#
# Alastair Matheson (PHSKC-APDE)
# alastair.matheson@kingcounty.gov
# 2016-08-13, split into separate files 2017-10
# 
###############################################################################



#############################################################################
#### NOTE: THIS CODE SETS UP ADDRESSES FOR GEOCODING AND ALSO COMBINES
####       THE GEOCODED DATA (I.E., MAY NEED TO BE RUN IN PARTS)
#############################################################################



#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, scipen = 999)

require(openxlsx) # Used to import/export Excel files
require(data.table) # used to read in csv files and rename fields
require(tidyverse) # Used to manipulate data

script <- httr::content(httr::GET("https://raw.githubusercontent.com/PHSKC-APDE/Housing/master/processing/metadata/set_data_env.r"))
eval(parse(text = script))

housing_source_dir <- file.path(here::here(), "processing")
METADATA = RJSONIO::fromJSON(file.path(housing_source_dir, "metadata/metadata.json"))
set_data_envr(METADATA,"combined")


if (UW == TRUE) {
  "skip load of pha_recoded"
} else {
#### BRING IN DATA #####
pha_recoded <- readRDS(file = file.path(housing_path, pha_recoded_fn))
}

if(sql == TRUE) {
  library(odbc)
  db_apde51 <- dbConnect(odbc(), "PH_APDEStore51")
  db_claims <- dbConnect(odbc(), "PHClaims51")
  db_hhsaw_dev <- dbConnect(odbc(), "hhsaw_dev", uid = keyring::key_list("hhsaw_dev")[["username"]])
  db_hhsaw_prod <- dbConnect(odbc(), "hhsaw_prod", uid = keyring::key_list("hhsaw_dev")[["username"]])
}


##### SET UP ADDRESSES FOR CLEANING #####
# Remove written NAs and make actually missing
pha_cleanadd <- pha_recoded %>%
  mutate_at(vars(unit_add, unit_apt, unit_apt2, unit_city, unit_state, unit_zip),
            list(~ ifelse(is.na(.) | . == "NULL" | . == "NA" | . == "", NA_character_, .))) %>%
  # Clear out white space
  mutate_at(vars(unit_add, unit_apt, unit_apt2, unit_city, unit_state, unit_zip),
            list(~ trimws(.))) %>%
  # Fix up blank addresses where ZIP = 0
  # Note: sometimes unit_apt2 has some junk data in it and everything else is blank
  # Don't include unit_apt2 in the consideration below
  mutate(unit_zip = ifelse(is.na(unit_add) & is.na(unit_apt) & is.na(unit_add) & 
                              is.na(unit_city) & is.na(unit_state) & unit_zip == 0, 
                            NA_character_, unit_zip)) %>%
  # Some cities are in the unit_apt2 field, which messes things up when cleaning
  mutate(unit_city = ifelse(is.na(unit_city) & !is.na(unit_apt2), unit_apt2, unit_city),
         unit_apt2 = ifelse(unit_apt2 == unit_city, NA_character_, unit_apt2))


# Fix up when secondary addresses were converted to dates
pha_cleanadd <- pha_cleanadd %>%
  mutate_at(vars(unit_add, unit_apt, unit_apt2),
            funs(case_when(
              . == "1-JAN" ~ "1-1",
              . == "2-JAN" ~ "1/2",
              . == "4-JAN" ~ "1-4",
              . == "1-FEB" ~ "2-1",
              . == "2-FEB" ~ "2-2",
              . == "5-FEB" ~ "2-5",
              . == "6-FEB" ~ "2-6",
              . == "13-FEB" ~ "2-13",
              . == "4-MAR" ~ "3-4",
              . == "16-APR" ~ "4-16",
              . == "3-MAY" ~ "5-3",
              . == "4-MAY" ~ "5-4",
              . == "7-MAY" ~ "5-7",
              . == "2-JUN" ~ "6-2",
              . == "3-JUN" ~ "6-3",
              . == "4-JUN" ~ "6-4",
              . == "5-JUN" ~ "6-5",
              . == "6-JUN" ~ "6-6",
              . == "9-JUN" ~ "6-9",
              . == "JUN-31" ~ "6-31",
              . == "JUN-45" ~ "6-45",
              . == "3-JUL" ~ "7-3",
              . == "1-AUG" ~ "8-1",
              . == "2-AUG" ~ "8-2",
              . == "2-SEP" ~ "9-2",
              . == "4-SEP" ~ "9-4",
              . == "8-SEP" ~ "9-8",
              . == "SEP-24" ~ "9-24",
              . == "Sep-24" ~ "9-24",
              . == "SEP-40" ~ "9-40",
              . == "1-OCT" ~ "10-1",
              . == "3-OCT" ~ "10-3",
              . == "4-OCT" ~ "10-4",
              . == "5-OCT" ~ "10-5",
              . == "1-NOV" ~ "11-1",
              . == "3-NOV" ~ "11-3",
              . == "4-NOV" ~ "11-4",
              . == "6-NOV" ~ "11-6",
              . == "2-DEC" ~ "12-2",
              TRUE ~ .
            )))

### Set up a hash of address to join on
pha_cleanadd <- pha_cleanadd %>%
  mutate(geo_hash_raw = as.character(toupper(openssl::sha256(paste(stringr::str_replace_na(unit_add, ''), 
                                                                   stringr::str_replace_na(unit_apt, ''), 
                                                                   stringr::str_replace_na(unit_apt2, ''), 
                                                                   stringr::str_replace_na(unit_city, ''), 
                                                                   stringr::str_replace_na(unit_state, ''), 
                                                                   stringr::str_replace_na(unit_zip, ''), 
                                                                   sep = "|")))))
  

#### Specific addresses ####
if (UW == F & sql == T) {
  # Address will be first checked against a reference table that 
  # contains most cleaned addresses. Only new addresses will be run through the
  # process below
  
  # Bring in ref table
  adds_ref <- DBI::dbGetQuery(db_hhsaw_prod, "SELECT * FROM ref.address_clean")
  
  
  # Pull out distinct addresses
  adds_specific <- pha_cleanadd %>%
    distinct(unit_add, unit_apt, unit_apt2, unit_city, unit_state, unit_zip, geo_hash_raw) %>%
    rename(geo_add1_raw = unit_add,
           geo_add2_raw = unit_apt,
           geo_add3_raw = unit_apt2,
           geo_city_raw = unit_city,
           geo_state_raw = unit_state,
           geo_zip_raw = unit_zip)
  
  
  # Find the addresses that are already clean
  adds_already_clean <- inner_join(adds_specific, 
                                   select(adds_ref, geo_hash_raw:last_run), 
                                   by = "geo_hash_raw") %>%
    select(geo_add1_raw, geo_add2_raw, geo_add3_raw, geo_city_raw, 
           geo_state_raw, geo_zip_raw, geo_hash_raw,
           geo_add1_clean, geo_add2_clean, geo_city_clean, 
           geo_state_clean, geo_zip_clean, geo_hash_clean, geo_hash_geocode,
           geo_geocode_skip, last_run) %>%
    # Need to make distinct in case some blank raw fields came through and 
    # there were multiple geo_hash_raw rows as a result
    distinct()
  
  
  # See which addresses need cleaning
  adds_to_clean <- anti_join(adds_specific, adds_ref, by = "geo_hash_raw") %>%
    filter(!(trimws(paste0(geo_add1_raw, geo_add2_raw, geo_city_raw, 
                           geo_state_raw, geo_zip_raw)) == '')) %>%
    select(geo_add1_raw, geo_add2_raw, geo_add3_raw, 
           geo_city_raw, geo_state_raw, geo_zip_raw, geo_hash_raw) %>%
    distinct()
  
  
  if (nrow(adds_to_clean) > 0) {
    # Add new addresses that need cleaning into the Informatica table
    timestamp <- Sys.time()
    
    adds_to_clean <- adds_to_clean %>% mutate(geo_source = NA,
                                              timestamp = timestamp)
    
    DBI::dbWriteTable(db_hhsaw_prod, 
                      name = DBI::Id(schema = "ref", table = "informatica_address_input"),
                      value = adds_to_clean,
                      overwrite = F, append = T)
  }
  

} else {
  # Some addresses have specific issues than cannot be addressed via rules
  # However, these specific addresses should not be shared publicly
  adds_specific <- read.xlsx(file.path(housing_path, pha_specific_fn),
                             na.strings = "")
  
  adds_specific <- adds_specific %>%
    mutate_at(vars(unit_zip, unit_zip_new),
              funs(case_when(nchar(.) == 4 ~ as.character(paste0("0", .)),
                             TRUE ~ as.character(.)))) %>%
    mutate_all(funs(ifelse(is.na(.), "", .)))
  
  pha_cleanadd <- left_join(pha_cleanadd, adds_specific, 
                            by = c("unit_add", "unit_apt", "unit_apt2", "unit_city", "unit_state", "unit_zip")) %>%
    select(-date_add_added, -notes)
  
  # Bring over addresses not matched (could use overidden == 0 too)
  pha_cleanadd <- pha_cleanadd %>%
    mutate(
      unit_add_new = ifelse(is.na(unit_add_new), unit_add, unit_add_new),
      unit_apt_new = ifelse(is.na(unit_apt_new), unit_apt, unit_apt_new),
      unit_apt2_new = ifelse(is.na(unit_apt2_new), unit_apt2, unit_apt2_new),
      unit_city_new = ifelse(is.na(unit_city_new), unit_city, unit_city_new),
      unit_state_new = ifelse(is.na(unit_state_new), unit_state, unit_state_new),
      unit_zip_new = ifelse(is.na(unit_zip_new), unit_zip, unit_zip_new)
    )
  
  
  ### Make a function with all the cleaning code so different data frames can be cleaned
  # Assumes unit_x_new fields are already created
  add_clean_f <- function(df) {
    # Get rid of extra spacing in addresses and some punctuation
    result <- df %>%
      mutate(unit_add_new = str_replace_all(unit_add_new, "\\.|,", ""),
             unit_add_new = str_replace_all(unit_add_new, "[:space:]+", " "),
             unit_apt_new = str_replace_all(unit_apt_new, ",", ""),
             unit_apt2_new = str_replace_all(unit_apt2_new, ",", "")
      )
    
    # Move apartments from apt2 to apt where apt is blank (~9700 rows)
    # NB. It looks like apt2 was limited to 3 characters so some values may be truncated
    # (can tell this by looking at where apt and apt2 are both not blank)
    result <- result %>%
      mutate(unit_apt_new = ifelse(unit_apt_new == "" & unit_apt2_new != "", unit_apt2_new, unit_apt_new),
             unit_apt2_new = ifelse(unit_apt_new == unit_apt2_new, "", unit_apt2_new))
    
    
    ### Clean up road name in wrong field
    result <- result %>%
      mutate(
        unit_add_new = if_else(
          str_detect(unit_apt_new, wrong_road_name) == TRUE,
          paste(unit_add_new, str_sub(unit_apt_new, 1, 
                                      str_locate(unit_apt_new, wrong_road_name)[, 2] - 1),
                sep = " "), unit_add_new),
        unit_apt_new = if_else(str_detect(unit_apt_new, wrong_road_name) == TRUE,
                               str_sub(unit_apt_new, 
                                       str_locate(unit_apt_new, wrong_road_name)[, 2],
                                       str_length(unit_apt_new)), unit_apt_new),
        unit_add_new = if_else(str_detect(unit_apt_new, wrong_road_type) == TRUE,
                               paste(unit_add_new, 
                                     str_sub(unit_apt_new, 1, 
                                             str_locate(unit_apt_new, wrong_road_type)[, 2] - 1),
                                     sep = " "), unit_add_new),
        unit_apt_new = if_else(str_detect(unit_apt_new, wrong_road_type) == TRUE,
                               str_sub(unit_apt_new, 
                                       str_locate(unit_apt_new, wrong_road_type)[, 2],
                                       str_length(unit_apt_new)),
                               unit_apt_new)
      )
    
    
    ### Figure out when apartments are in wrong field
    ### Clean up apartments in wrong field
    result <- result %>%
      mutate(
        unit_length_diff = str_length(unit_add_new) - str_length(unit_apt_new),
        unit_apt_length = str_length(unit_apt_new) - 
          str_locate(unit_apt_new, paste0(paste(secondary, collapse = "|"), "[:space:]*"))[, 2],
        # Remove straight duplicates of apt numbers in address and apt fields
        unit_add_new = if_else(unit_apt_new != "" &
                                 str_sub(unit_add_new, unit_length_diff + 1, str_length(unit_add_new)) ==
                                 str_sub(unit_apt_new, 1, str_length(unit_apt_new)),
                               str_sub(unit_add_new, 1, unit_length_diff), unit_add_new),
        # Remove duplicates that are a little more complicated (where the secondary designator isn't repeated but the secondary number is)
        unit_add_new = if_else(unit_apt_new != "" & str_detect(unit_apt_new, paste(secondary, collapse = "|")) == TRUE &
                                 str_sub(unit_apt_new, 
                                         str_locate(unit_apt_new, paste0(paste(secondary, collapse = "|"), "[:space:]*"))[, 2] + 1, 
                                         str_length(unit_apt_new)) ==
                                 str_sub(unit_add_new, str_length(unit_add_new) - unit_apt_length + 1, str_length(unit_add_new)) &
                                 !str_sub(unit_add_new, str_length(unit_add_new) - 1, 
                                          str_length(unit_add_new)) %in% c("LA", "N", "NE", "NW", "S", "SE", "SW"),
                               str_sub(unit_add_new, 1, str_length(unit_add_new) - unit_apt_length),
                               unit_add_new),
        # ID apartment numbers that need to move into the appropriate column (1, 2)
        # Also include addresses that end in a number as many seem to be apartments (3, 4)
        unit_apt_move = case_when(
          unit_apt_new == "" & is.na(overridden) & 
            str_detect(unit_add_new, paste0("[:space:]+(", 
                                            paste(secondary, collapse = "|"), ")")) == TRUE ~ 1,
          unit_apt_new != "" & is.na(overridden) &
            str_detect(unit_add_new, paste0("[:space:]+(", paste(secondary, collapse = "|"), ")")) == TRUE ~ 2,
          unit_apt_new == "" & is.na(overridden) &
            str_detect(unit_add_new, "[:space:]+[:alnum:]*[-]*[:digit:]+$") == TRUE &
            str_detect(unit_add_new, "PO BOX|PMB") == FALSE & str_detect(unit_add_new, "HWY 99$") == FALSE ~ 3,
          unit_apt_new != "" & is.na(overridden) &
            str_detect(unit_add_new, "[:space:]+[:alnum:]*[-]*[:digit:]+$") == TRUE &
            str_detect(unit_add_new, "PO BOX|PMB") == FALSE & str_detect(unit_add_new, "HWY 99$") == FALSE ~ 4,
          TRUE ~ 0
        ),
        # Move apartment numbers to unit_apt_new if that field currently blank
        unit_apt_new = if_else(unit_apt_move == 1,
                               str_sub(unit_add_new, 
                                       str_locate(unit_add_new, 
                                                  paste0("[:space:]+(", paste(secondary, collapse = "|"), ")"))[, 1], 
                                       str_length(unit_add_new)),
                               unit_apt_new),
        unit_apt_new = if_else(unit_apt_move == 3,
                               str_sub(unit_add_new, 
                                       str_locate(unit_add_new, "[:space:]+[:alnum:]*[-]*[:digit:]+$")[, 1], 
                                       str_length(unit_add_new)),
                               unit_apt_new),
        # Merge apt data from unit_add_new with unit_apt_new if the latter is currently not blank
        unit_apt_new = if_else(unit_apt_move == 2,
                               paste(str_sub(unit_add_new, 
                                             str_locate(unit_add_new, 
                                                        paste0("[:space:]*(", paste(secondary, collapse = "|"), ")"))[, 1], 
                                             str_length(unit_add_new)),
                                     unit_apt_new, sep = " "),
                               unit_apt_new),
        unit_apt_new = 
          case_when(
            unit_apt_move == 4 & str_detect(unit_apt, "#") == FALSE ~ 
              paste(str_sub(unit_add_new, 
                            str_locate(unit_add_new, "[:space:]+[:alnum:]*[-]*[:digit:]+$")[, 1], 
                            str_length(unit_add_new)), unit_apt_new, sep = " "),
            unit_apt_move == 4 & str_detect(unit_apt, "#") == TRUE ~ 
              paste(str_sub(unit_add_new, str_locate(unit_add_new, "[:space:]+[:alnum:]*[-]*[:digit:]+$")[, 1],
                            str_length(unit_add_new)),
                    str_sub(unit_apt_new, str_locate(unit_apt_new, "[:digit:]")[, 1], str_length(unit_apt_new)),
                    sep = " "),
            TRUE ~ unit_apt_new
          ),
        # Remove apt data from the address field (this needs to happen after the above code)
        unit_add_new = if_else(unit_apt_move %in% c(1, 2),
                               str_sub(unit_add_new, 1, 
                                       str_locate(unit_add_new, 
                                                  paste0("[:space:]+(", paste(secondary, collapse = "|"), ")"))[, 1] - 1),
                               unit_add_new),
        unit_add_new = if_else(unit_apt_move %in% c(3, 4),
                               str_sub(unit_add_new, 1, str_locate(unit_add_new, "[:space:]+[:alnum:]*[-]*[:digit:]+$")[, 1] - 1),
                               unit_add_new),
        # Now pull over any straggler apartments ending in a single letter or letter prefix
        unit_apt_move = if_else(str_detect(unit_add_new, "[:space:]+[A-D|F-M|O-R|T-V|X-Z][-]*[:space:]{0,1}$") == TRUE, 5, unit_apt_move),
        unit_apt_new = if_else(unit_apt_move == 5 & str_detect(unit_apt_new, "#") == FALSE,
                               paste0(str_sub(unit_add_new, 
                                              str_locate(unit_add_new, "[:space:]+[A-D|F-M|O-R|T-V|X-Z][-]*$")[, 1] + 1,
                                              str_length(unit_add_new)), 
                                      unit_apt_new),
                               unit_apt_new),
        unit_apt_new = if_else(unit_apt_move == 5 & str_detect(unit_apt_new, "#") == TRUE,
                               paste0(str_sub(unit_add_new, 
                                              str_locate(unit_add_new, "[:space:]+[A-D|F-M|O-R|T-V|X-Z][-]*[:space:]{0,1}$")[, 1] + 1,
                                              str_length(unit_add_new)), 
                                      str_sub(unit_apt_new, str_locate(unit_apt_new, "#")[, 1] + 1, str_length(unit_apt_new))),
                               unit_apt_new),
        # Remove apt data from the address field (this needs to happen after the above code)
        unit_add_new = if_else(unit_apt_move == 5,
                               str_sub(unit_add_new, 1, str_locate(unit_add_new, "[:space:]+[A-D|F-M|O-R|T-V|X-Z][-]*[:space:]{0,1}$")[, 1] - 1),
                               unit_add_new)
      ) %>%
      # Remove any whitespace generated in the process
      mutate_at(vars(unit_add_new, unit_apt_new), funs(str_trim(.))) %>%
      mutate_at(vars(unit_add_new, unit_apt_new), funs(str_replace_all(., "[:space:]+", " "))) %>%
      select(-unit_length_diff, -unit_apt_length)
    
    
    ### Clean remaining apartment issues
    result <- result %>%
      mutate(
        # Add in hyphens between apt numbers (when no secondary designator present)
        unit_apt_new = if_else(str_detect(unit_apt_new, "[:alnum:]+[:space:]+[:digit:]+$") == TRUE & 
                                 str_detect(unit_apt_new, paste0("(", paste(secondary, collapse = "|"), ")")) == FALSE,
                               str_replace_all(unit_apt_new, "[:space:]", "-"), unit_apt_new),
        # Remove the # if in between apartment components
        unit_apt_new = if_else(
          str_detect(unit_apt_new, paste0("(", paste(secondary, collapse = "|"), ")", "[:digit:]*[:space:]*#[:space:]*[:digit:]+")) == TRUE,
          str_replace(unit_apt_new, "[:space:]*#[:space:]*", " "), unit_apt_new),
        unit_apt_new = if_else(str_detect(unit_apt_new, "[:digit:]+[:space:]*#[:space:]*[:digit:]+") == TRUE,
                               str_replace(unit_apt_new, "[:space:]*#[:space:]*", "-"), unit_apt_new),
        # Ensure a space between # and the number
        unit_apt_new = str_replace_all(unit_apt_new, "[#|\\$][:space:]*[-]*", "# "),
        # Add in an # prefix if there is no other unit designator
        unit_apt_new = if_else(str_detect(unit_apt_new, "^[:digit:]+$") == TRUE | str_detect(unit_apt_new, "^[:alnum:]{1}$") == TRUE |
                                 (str_detect(unit_apt_new, "^[:alnum:]+[-]*[:digit:]+$") == TRUE & 
                                    str_detect(unit_apt_new, paste0("(", paste(secondary, collapse = "|"), ")")) == FALSE),
                               paste0("# ", unit_apt_new), unit_apt_new),
        # Remove spaces between hyphens
        unit_apt_new = str_replace(unit_apt_new, "[:space:]-[:space:]", "-")
      ) %>%
      # Get ride of second apartment field
      select(-unit_apt2_new)
    
    
    # Clean up street addresses
    result <- result %>%
      mutate(
        # standardize street names
        unit_add_new = str_replace_all(unit_add_new, "([:space:]|^)AVENUE|[:space:]AV([:space:]|$)", " AVE "),
        unit_add_new = str_replace_all(unit_add_new, "([:space:]|^)BOULEVARD([:space:]|$)", " BLVD "),
        unit_add_new = str_replace_all(unit_add_new, "([:space:]|^)CIRCLE([:space:]|$)", " CIR "),
        unit_add_new = str_replace_all(unit_add_new, "([:space:]|^)COURT([:space:]|$)", " CT "),
        unit_add_new = str_replace_all(unit_add_new, "([:space:]|^)DRIVE([:space:]|$)", " DR "),
        unit_add_new = str_replace_all(unit_add_new, "([:space:]|^)HIGHWAY([:space:]|$)", " HWY "),
        unit_add_new = str_replace_all(unit_add_new, "([:space:]|^)LANE([:space:]|$)", " LN "),
        unit_add_new = str_replace_all(unit_add_new, "([:space:]|^)NORTH([:space:]|$)", " N "),
        unit_add_new = str_replace_all(unit_add_new, "([:space:]|^)NORTH EAST([:space:]|$)", " NE "),
        unit_add_new = str_replace_all(unit_add_new, "([:space:]|^)NORTH WEST([:space:]|$)", " NW "),
        unit_add_new = str_replace_all(unit_add_new, "([:space:]|^)PARKWAY([:space:]|$)", " PKWY "),
        unit_add_new = str_replace_all(unit_add_new, "([:space:]|^)PLACE([:space:]|$)", " PL "),
        unit_add_new = str_replace_all(unit_add_new, "([:space:]|^)ROAD([:space:]|$)", " RD "),
        unit_add_new = str_replace_all(unit_add_new, "([:space:]|^)SO([:space:]|$)|[:space:]SO([:space:]|$)", " S "),
        unit_add_new = str_replace_all(unit_add_new, "([:space:]|^)SOUTH EAST([:space:]|$)", " SE "),
        unit_add_new = str_replace_all(unit_add_new, "([:space:]|^)SOUTH WEST([:space:]|$)", " SW "),
        unit_add_new = str_replace_all(unit_add_new, "([:space:]|^)STREET([:space:]|$)", " ST "),
        unit_add_new = str_replace_all(unit_add_new, "([:space:]|^)STST([:space:]|$)", " ST "),
        unit_add_new = str_replace_all(unit_add_new, "([:space:]|^)WY([:space:]|$)", " WAY "),
        unit_add_new = str_replace_all(unit_add_new, "([:space:]|^)WEST([:space:]|$)", " W ")
      )
    
    # Clean up remaining city name issues
    result <- result %>%
      mutate(
        unit_city_new = str_replace(unit_city_new, "FEDERAL WY", "FEDERAL WAY"),
        unit_city_new = str_replace(unit_city_new, "SEATTTLE", "SEATTLE")
      )
    
    # Get rid of remaining extraneous punctuation
    result <- result %>%
      mutate(unit_add_new = str_replace_all(unit_add_new, "[-]+", " "),
             # One more cleanup of extra spaces
             unit_add_new = str_replace_all(unit_add_new, "[:space:]+", " "),
             unit_add_new = str_trim(unit_add_new)
      )
    
    return(result)
  }
  
  
  ### Set up lists used in cleaning
  # Road names
  wrong_road_name <- "^(MEM[:space:]DR|RD[:space:]SE|PKWY[:space:]SW|WAY[:space:]NE)[:space:]+"
  # Don't include WEST because it's already part of some cleaned addresses with
  # WEST at the start of the apartment field
  wrong_road_type <- paste0("^(AVENUE|AVE|BOULEVARD|BLVD|CIRCLE|CIR|COURT|CT|DRIVE|DR|EAST|HIGHWAY|HWY|",
                            "LANE|LN|NORTH|NORTH EAST|NE |NORTH WEST|NW |PARKWAY|PKWY|",
                            "PLACE|PL |ROAD|RD|SOUTH|SO |SOUTH EAST|SE |SOUTH WEST|SW |STREET|ST ",
                            "STST|WAY)")
  
  # Secondary designators
  secondary <- c("#", "\\$", "APT", "APPT", "APARTMENT", "APRT", "ATPT","BOX", "BLDG", 
                 "BLD", "BLG", "BUILDING", "DUPLEX", "FL ", "FLOOR", "HOUSE", "LOT", 
                 "LOWER", "LOWR", "LWR", "REAR", "RM", "ROOM", "SLIP", "STE", "SUITE", 
                 "SPACE", "SPC", "STUDIO", "TRAILER", "TRAILOR", "TLR", "TRL", "TRLR", 
                 "UNIT", "UPPER", "UPPR", "UPSTAIRS")
  secondary_init <- c("^#", "^\\$", "^APT", "^APPT","^APARTMENT", "^APRT", "^ATPT", 
                      "^BOX", "^BLDG", "^BLD", "^BLG", "^BUILDING", "^DUPLEX", "^FL ", 
                      "^FLOOR", "^HOUSE", "^LOT", "^LOWER", "^LOWR", "^LWR", "^REAR", 
                      "^RM", "^ROOM", "^SLIP", "^STE", "^SUITE", "^SPACE", "^SPC", 
                      "^STUDIO", "^TRAILER", "^TRAILOR", "^TLR", "^TRL", "^TRLR", 
                      "^UNIT", "^UPPER", "^UPPR", "^UPSTAIRS")
  
  
}



#### CLEAN ADDRESSES ####
if (UW == T) {
  # Run full data through the cleaning function
  pha_cleanadd <- add_clean_f(pha_cleanadd)
}

if (UW == FALSE) {
  #### Retrieve cleaned addressed from Informatica ####
  # # May need to retrieve the timestamp
  # timestamp <- dbGetQuery(db_hhsaw_prod,
  #                         "SELECT MAX(timestamp) FROM ref.informatica_address_output")[[1]]
  
  if (nrow(adds_to_clean) > 0) {
    adds_clean <- dbGetQuery(db_hhsaw_prod,
                             glue::glue_sql("SELECT * FROM ref.informatica_address_output
                           WHERE convert(varchar, timestamp, 20) = {lubridate::with_tz(timestamp, 'utc')}",
                                            .con = db_hhsaw_prod))
    
    ### Informatica seems to drop secondary designators when they start with #
    # Move over from old address
    adds_clean <- adds_clean %>%
      mutate(geo_add2_clean = ifelse(is.na(geo_add2_clean) & str_detect(geo_add1_raw, "^#"),
                                     geo_add1_raw, geo_add2_clean))
    ### Tidy up some PO box and other messiness
    adds_clean <- adds_clean %>%
      mutate(geo_add1_clean = case_when(
        is.na(geo_add1_clean) & !is.na(geo_po_box_clean) ~ geo_po_box_clean,
        TRUE ~ geo_add1_clean),
        geo_add2_clean = case_when(
          is.na(geo_add2_clean) & !is.na(geo_po_box_clean) & !is.na(geo_add1_clean) & 
            geo_add1_clean != geo_po_box_clean ~ geo_po_box_clean,
          !is.na(geo_add2_clean) & !is.na(geo_po_box_clean) & !is.na(geo_add1_clean) ~ paste(geo_add2_clean, geo_po_box_clean, sep = " "),
          TRUE ~ geo_add2_clean),
        geo_po_box_clean = as.numeric(ifelse(!is.na(geo_po_box_clean), 1, 0))
      )
    
    adds_clean <- adds_to_clean %>%
      mutate(geo_add1_clean = NA_character_,
             geo_add2_clean = NA_character_,
             geo_city_clean = NA_character_,
             geo_state_clean = NA_character_,
             geo_zip_clean = NA_character_)
      
    ### Set up variables of interest
    adds_clean <- adds_clean %>%
      mutate(geo_geocode_skip = 0L,
             geo_hash_raw = ifelse(is.na(geo_hash_raw),
                                   toupper(openssl::sha256(paste(stringr::str_replace_na(geo_add1_raw, ''), 
                                                                 stringr::str_replace_na(geo_add2_raw, ''), 
                                                                 stringr::str_replace_na(geo_add3_raw, ''), 
                                                                 stringr::str_replace_na(geo_city_raw, ''), 
                                                                 stringr::str_replace_na(geo_state_raw, ''), 
                                                                 stringr::str_replace_na(geo_zip_raw, ''), 
                                                                 sep = "|"))),
                                   geo_hash_raw),
             geo_hash_clean = toupper(openssl::sha256(paste(stringr::str_replace_na(geo_add1_clean, ''), 
                                                            stringr::str_replace_na(geo_add2_clean, ''), 
                                                            stringr::str_replace_na(geo_city_clean, ''), 
                                                            stringr::str_replace_na(geo_state_clean, ''), 
                                                            stringr::str_replace_na(geo_zip_clean, ''), 
                                                            sep = "|"))),
             geo_hash_geocode = toupper(openssl::sha256(paste(stringr::str_replace_na(geo_add1_clean, ''),  
                                                              stringr::str_replace_na(geo_city_clean, ''), 
                                                              stringr::str_replace_na(geo_state_clean, ''), 
                                                              stringr::str_replace_na(geo_zip_clean, ''), 
                                                              sep = "|"))),
             last_run = Sys.time()) %>%
      select(geo_add1_raw, geo_add2_raw, geo_add3_raw, geo_city_raw, 
             geo_state_raw, geo_zip_raw, geo_hash_raw,
             geo_add1_clean, geo_add2_clean, geo_city_clean, 
             geo_state_clean, geo_zip_clean, geo_hash_clean, geo_hash_geocode,
             geo_geocode_skip, last_run) %>%
      # Convert all blank fields to be NA
      mutate_if(is.character, list(~ ifelse(. == "", NA_character_, .)))
    
    
    #### Load new addresses to ref table ####
    dbWriteTable(db_hhsaw_prod, 
                 name = DBI::Id(schema = "ref",  table = "stage_address_clean"),
                 adds_clean,
                 overwrite = F, append = T)
    
    
    ### QA and load to final
    # Code to come, but will be picked up by the Medicaid monthly run too
    
    ## Need to run geocoding at some point too.
    ##  Will be swept up as part of the Mediciad load each month
    
    
    #### Join back to initial address data ####
    adds_final <- bind_rows(adds_clean, adds_already_clean) %>%
      rename(unit_add_new = geo_add1_clean,
             unit_apt_new = geo_add2_clean,
             unit_city_new = geo_city_clean,
             unit_state_new = geo_state_clean,
             unit_zip_new = geo_zip_clean) %>%
      select(geo_hash_raw:geo_hash_geocode)
  } else {
    adds_final <- adds_already_clean %>%
      rename(unit_add_new = geo_add1_clean,
             unit_apt_new = geo_add2_clean,
             unit_city_new = geo_city_clean,
             unit_state_new = geo_state_clean,
             unit_zip_new = geo_zip_clean) %>%
      select(geo_hash_raw:geo_hash_geocode)
  }
  
  pha_cleanadd <- left_join(pha_cleanadd, adds_final, by = "geo_hash_raw")
  
}


#### FILL IN BLANK ADDRESS ROWS ####
### Transfer data to rows where the address is blank due to ending participation in a program (mostly KCHA)
# First make everything upper case
pha_cleanadd <- pha_cleanadd %>%
  mutate_at(vars(prog_type, vouch_type, property_name, property_type, portfolio), 
            list(~ toupper(.))) %>% 
  # Make concatenated agency/prog type/subtype/spec voucher type field to make life easier
  mutate(agency_prog_concat = paste(agency_new, major_prog, prog_type, vouch_type, sep = ", "))

pha_cleanadd <- pha_cleanadd %>% arrange(pid, act_date, agency_prog_concat) %>%
  mutate_at(vars(unit_add_new, unit_apt_new, unit_city_new, unit_state_new, 
                 unit_zip_new, geo_hash_clean, geo_hash_geocode),
            list(~ ifelse(geo_hash_clean == "45CA31C3315A5978F40438AAB46040D75E99C9B125C2FD01DB6E10AC80BEF906" & 
                            lag(geo_hash_clean, 1) != "45CA31C3315A5978F40438AAB46040D75E99C9B125C2FD01DB6E10AC80BEF906" &
                            pid == lag(pid, 1) & !is.na(lag(pid, 1)) & 
                            agency_prog_concat == lag(agency_prog_concat, 1) & 
                            act_type %in% c(5, 6),
                        lag(., 1), .))) %>%
  # remove temporary agency
  select(-agency_prog_concat)


### Make a flag for a blank address ###
# Easier than checking a long hash each time
pha_cleanadd <- pha_cleanadd %>%
  mutate(geo_blank = ifelse(geo_hash_clean == "45CA31C3315A5978F40438AAB46040D75E99C9B125C2FD01DB6E10AC80BEF906",
                            1L, 0L))


if (UW == T) {
  # There shouldn't be a lot of blank ZIPs any more in the PHSKC data
  # For some reason there are a bunch of blank ZIPs even though other rows with 
  # the same address have a ZIP. Sort by address and copy over largest ZIP.
  pha_cleanadd <- pha_cleanadd %>%
    group_by(unit_add_new, unit_apt_new, unit_apt2_new, unit_city_new, unit_state_new) %>%
    mutate(unit_zip_new = ifelse(is.na(unit_zip_new), max(unit_zip_new), unit_zip_new)) %>%
    ungroup()
}



### clean up a bit
rm(adds_specific, adds_clean, adds_already_clean, adds_to_clean, adds_ref, adds_final)


#### Merge KCHA development data now that addresses are clean #####
pha_cleanadd <- pha_cleanadd %>%
  mutate(dev_city = paste0(unit_city_new, ", ", unit_state_new, " ", unit_zip_new),
    # Trim any white space
    dev_city = str_trim(dev_city))

# HCV
# Bring in data
kcha_dev_adds <- data.table::fread(file = kcha_dev_adds_path_fn)
# Bring in variable name mapping table
fields <- read.csv(text = httr::content(httr::GET("https://raw.githubusercontent.com/PHSKC-APDE/Housing/master/processing/Field%20name%20mapping.csv")), 
                   header = TRUE)

# Clean up KCHA field names
colnames(kcha_dev_adds) <- str_replace_all(colnames(kcha_dev_adds), "[:punct:]|[:space:]", "")
kcha_dev_adds <- data.table::setnames(kcha_dev_adds, fields$common_name[match(names(kcha_dev_adds), fields$kcha_modified)])


# Drop spare rows and deduplicate
# Note that only three rows (plus rows used for merging) are being kept for now.
kcha_dev_adds <- kcha_dev_adds %>% select(dev_add, dev_city, property_name, portfolio, property_type)
# Clean up addresses prior to merge
kcha_dev_adds <- kcha_dev_adds %>%
  # Make sure everything is in caps
  mutate_all(., funs(toupper(.))) %>%
  mutate(
    # standardize street names
    dev_add = str_replace_all(dev_add, "([:space:]|^)AVENUE|[:space:]AV([:space:]|$)", " AVE "),
    dev_add = str_replace_all(dev_add, "([:space:]|^)BOULEVARD([:space:]|$)", " BLVD "),
    dev_add = str_replace_all(dev_add, "([:space:]|^)CIRCLE([:space:]|$)", " CIR "),
    dev_add = str_replace_all(dev_add, "([:space:]|^)COURT([:space:]|$)", " CT "),
    dev_add = str_replace_all(dev_add, "([:space:]|^)DRIVE([:space:]|$)", " DR "),
    dev_add = str_replace_all(dev_add, "([:space:]|^)HIGHWAY([:space:]|$)", " HWY "),
    dev_add = str_replace_all(dev_add, "([:space:]|^)LANE([:space:]|$)", " LN "),
    dev_add = str_replace_all(dev_add, "([:space:]|^)NORTH([:space:]|$)", " N "),
    dev_add = str_replace_all(dev_add, "([:space:]|^)NORTH EAST([:space:]|$)", " NE "),
    dev_add = str_replace_all(dev_add, "([:space:]|^)NORTH WEST([:space:]|$)", " NW "),
    dev_add = str_replace_all(dev_add, "([:space:]|^)PARKWAY([:space:]|$)", " PKWY "),
    dev_add = str_replace_all(dev_add, "([:space:]|^)PLACE([:space:]|$)", " PL "),
    dev_add = str_replace_all(dev_add, "([:space:]|^)ROAD([:space:]|$)", " RD "),
    dev_add = str_replace_all(dev_add, "([:space:]|^)SO([:space:]|$)|[:space:]SO([:space:]|$)", " S "),
    dev_add = str_replace_all(dev_add, "([:space:]|^)SOUTH EAST([:space:]|$)", " SE "),
    dev_add = str_replace_all(dev_add, "([:space:]|^)SOUTH WEST([:space:]|$)", " SW "),
    dev_add = str_replace_all(dev_add, "([:space:]|^)STREET([:space:]|$)", " ST "),
    dev_add = str_replace_all(dev_add, "([:space:]|^)STST([:space:]|$)", " ST "),
    dev_add = str_replace_all(dev_add, "([:space:]|^)WY([:space:]|$)", " WAY "),
    dev_add = str_replace_all(dev_add, "([:space:]|^)WEST([:space:]|$)", " W "),
    # Add in missing street name
    dev_add = ifelse(str_detect(dev_add, "NE 80TH$|NE 119TH$|NE 145TH$|NE 175TH$|NE 177TH$|S 146TH$|
                                S 152ND$|S 325TH$|S 333RD$|SE 14TH$|SW 102ND$|SW 130TH|W 148TH$") == T,
                     paste0(str_sub(dev_add, 1, length(dev_add)), " ST"),
                     dev_add)
    )
# Remove duplicates created during clean up
kcha_dev_adds <- kcha_dev_adds %>% distinct()

pha_cleanadd <- left_join(pha_cleanadd, kcha_dev_adds, 
                          by = c("unit_add_new" = "dev_add", "dev_city"))
rm(kcha_dev_adds)

# Sort out which values to keep
# Based on KCHA input, using imported data
pha_cleanadd <- pha_cleanadd %>%
  mutate(portfolio = ifelse(is.na(portfolio.y), portfolio.x, portfolio.y),
         property_name = ifelse(is.na(property_name.y), property_name.x, property_name.y),
         property_type = ifelse(is.na(property_type.y), property_type.x, property_type.y)) %>%
  select(-portfolio.x, -portfolio.y, -property_name.x, -property_name.y, -property_type.x, -property_type.y)

if (UW == FALSE){
  #### Save point ####
  saveRDS(pha_cleanadd, file = file.path(housing_path, pha_cleanadd_fn))
} 

rm(fields)
rm(secondary)
rm(secondary_init)
rm(new_addresses)
rm(pha_recoded)
gc()
