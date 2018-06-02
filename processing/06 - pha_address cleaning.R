###############################################################################
# OVERVIEW:
# Code to create a cleaned person table from the combined 
# King County Housing Authority and Seattle Housing Authority data sets
# Aim is to have a single row per contiguous time in a house per person
#
# STEPS:
# Process raw KCHA data and load to SQL database
# Process raw SHA data and load to SQL database
# Bring in individual PHA datasets and combine into a single file
# Deduplicate data and tidy up via matching process
# Recode race and other demographics
# Clean up addresses and geocode ### (THIS CODE) ###
# Consolidate data rows
# Add in final data elements and set up analyses
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
housing_path <- "//phdata01/DROF_DATA/DOH DATA/Housing"


library(openxlsx) # Used to import/export Excel files
library(data.table) # used to read in csv files and rename fields
library(tidyverse) # Used to manipulate data



#### Bring in data #####
pha_recoded <- readRDS(file = paste0(housing_path, "/OrganizedData/pha_recoded.Rda"))


##### Addresses #####
# Remove written NAs and make actually missing
pha_cleanadd <- pha_recoded %>%
  arrange(ssn_id_m6, lname_new_m6, fname_new_m6, act_date) %>%
  mutate(unit_zip = as.numeric(unit_zip)) %>%
  mutate_at(vars(unit_add, unit_apt, unit_apt2, unit_city, unit_state),
            funs(ifelse(is.na(.) | . == "NULL", "", .)))


### Specific addresses
# Some addresses have specific issues than cannot be addressed via rules
# However, these specific addresses should not be shared publically
adds_specific <- read.xlsx(paste0(housing_path, "/OrganizedData/PHA_specific_addresses_fix - DO NOT SHARE FILE.xlsx"),
                           na.strings = "")
adds_specific <- adds_specific %>%
  mutate_all(funs(ifelse(is.na(.), "", .)))
  
# For some reason there seem to be duplicates in the address data, possible created when cleaning up missing in the line above
adds_specific <- adds_specific %>% distinct()
pha_cleanadd <- left_join(pha_cleanadd, adds_specific, by = c("unit_add", "unit_apt", "unit_apt2", "unit_city", "unit_state", "unit_zip")) %>%
  select(-notes)


# Bring over addresses not matched (could use overidden == 0 too, could also collapse to a mutate_at statement)
pha_cleanadd <- pha_cleanadd %>%
  mutate(
    unit_add_new = ifelse(is.na(unit_add_new), unit_add, unit_add_new),
    unit_apt_new = ifelse(is.na(unit_apt_new), unit_apt, unit_apt_new),
    unit_apt2_new = ifelse(is.na(unit_apt2_new), unit_apt2, unit_apt2_new),
    unit_city_new = ifelse(is.na(unit_city_new), unit_city, unit_city_new),
    unit_state_new = ifelse(is.na(unit_state_new), unit_state, unit_state_new),
    unit_zip_new = ifelse(is.na(unit_zip_new), unit_zip, unit_zip_new)
  )


# Get rid of extra spacing in addresses and some punctuation
pha_cleanadd <- pha_cleanadd %>%
  mutate(unit_add_new = str_replace_all(unit_add_new, "\\.|,", ""),
         unit_add_new = str_replace_all(unit_add_new, "[:space:]+", " "),
         unit_apt_new = str_replace_all(unit_apt_new, ",", ""),
         unit_apt2_new = str_replace_all(unit_apt2_new, ",", "")
  )

# Move apartments from apt2 to apt where apt is blank (~9700 rows)
# NB. It looks like apt2 was limited to 3 characters so some values may be truncated
# (can tell this by looking at where apt and apt2 are both not blank)
pha_cleanadd <- pha_cleanadd %>%
  mutate(unit_apt_new = ifelse(unit_apt_new == "" & unit_apt2_new != "", unit_apt2_new, unit_apt_new),
         unit_apt2_new = ifelse(unit_apt_new == unit_apt2_new, "", unit_apt2_new))


# Clean up road name in wrong field
pha_cleanadd <- pha_cleanadd %>%
  mutate(
    unit_add_new = if_else(
      str_detect(unit_apt_new, "^(MEM[:space:]DR|RD[:space:]SE|PKWY[:space:]SW|WAY[:space:]NE)[:space:]+") == TRUE,
      paste(unit_add_new, str_sub(unit_apt_new, 1, 
                                  str_locate(unit_apt_new, 
                                             "^(MEM[:space:]DR|RD[:space:]SE|PKWY[:space:]SW|WAY[:space:]NE)[:space:]+")[, 2] - 1),
            sep = " "), unit_add_new),
    unit_apt_new = if_else(
      str_detect(unit_apt_new, "^(MEM[:space:]DR|RD[:space:]SE|PKWY[:space:]SW|WAY[:space:]NE)[:space:]+") == TRUE,
      str_sub(unit_apt_new, 
              str_locate(unit_apt_new, "^(MEM[:space:]DR|RD[:space:]SE|PKWY[:space:]SW|WAY[:space:]NE)[:space:]+")[, 2],
              str_length(unit_apt_new)), unit_apt_new),
    unit_add_new = if_else(
      str_detect(unit_apt_new, "^(RD|PKWY|WAY|NE|SE)[:space:]+") == TRUE,
      paste(unit_add_new, str_sub(unit_apt_new, 1, 
                                  str_locate(unit_apt_new, "^(RD|PKWY|WAY|NE|SE)[:space:]+")[, 2] - 1),
            sep = " "), unit_add_new),
    unit_apt_new = if_else(str_detect(unit_apt_new, "^(RD|PKWY|WAY|NE|SE)[:space:]+") == TRUE,
                           str_sub(unit_apt_new, 
                                   str_locate(unit_apt_new, "^(RD|PKWY|WAY|NE|SE)[:space:]+")[, 2],
                                   str_length(unit_apt_new)),
                           unit_apt_new)
  )


### Figure out when apartments are in wrong field
# Set up list of secondary designators
secondary <- c("#", "\\$", "APT", "APPT", "APARTMENT", "APRT", "ATPT","BOX", "BLDG", "BLD", "BLG", "BUILDING", "DUPLEX", "FL ", 
               "FLOOR", "HOUSE", "LOT", "LOWER", "LOWR", "LWR", "REAR", "RM", "ROOM", "SLIP", "STE", "SUITE", "SPACE", "SPC", "STUDIO",
               "TRAILER", "TRAILOR", "TLR", "TRL", "TRLR", "UNIT", "UPPER", "UPPR", "UPSTAIRS")
secondary_init <- c("^#", "^\\$", "^APT", "^APPT","^APARTMENT", "^APRT", "^ATPT", "^BOX", "^BLDG", "^BLD", "^BLG", "^BUILDING", "^DUPLEX", "^FL ", 
                    "^FLOOR", "^HOUSE", "^LOT", "^LOWER", "^LOWR", "^LWR", "^REAR", "^RM", "^ROOM", "^SLIP", "^STE", "^SUITE", "^SPACE", "^SPC", 
                    "^STUDIO", "^TRAILER", "^TRAILOR", "^TLR", "^TRL", "^TRLR", "^UNIT", "^UPPER", "^UPPR", "^UPSTAIRS")

# Clean up apartments in wrong field
pha_cleanadd <- pha_cleanadd %>%
  mutate(
    # Remove straight duplicates of apt numbers in address and apt fields
    unit_add_new = if_else(unit_apt_new != "" &
                             str_sub(unit_add_new, str_length(unit_add_new) - str_length(unit_apt_new) + 1, str_length(unit_add_new)) ==
                             str_sub(unit_apt_new, 1, str_length(unit_apt_new)),
                           str_sub(unit_add_new, 1, str_length(unit_add_new) - str_length(unit_apt_new)),
                           unit_add_new),
    # Remove duplicates that are a little more complicated (where the secondary designator isn't repeated but the secondary number is)
    unit_add_new = if_else(unit_apt_new != "" & str_detect(unit_apt_new, paste(secondary, collapse = "|")) == TRUE &
                             str_sub(unit_apt_new, 
                                     str_locate(unit_apt_new, paste0(paste(secondary, collapse = "|"), "[:space:]*"))[, 2] + 1, 
                                     str_length(unit_apt_new)) ==
                             str_sub(unit_add_new, str_length(unit_add_new) - (str_length(unit_apt_new) - 
                                                                                 (str_locate(unit_apt_new, paste0(paste(secondary, collapse = "|"), "[:space:]*"))[, 2] + 1)),
                                     str_length(unit_add_new)) &
                             !str_sub(unit_add_new, str_length(unit_add_new) - 1, str_length(unit_add_new)) %in% c("LA", "N", "NE", "NW", "S", "SE", "SW"),
                           str_sub(unit_add_new, 1, str_length(unit_add_new) - (str_length(unit_apt_new) - 
                                                                                  str_locate(unit_apt_new, paste0(paste(secondary, collapse = "|"), "[:space:]*"))[, 2])),
                           unit_add_new),
    # ID apartment numbers that need to move into the appropriate column (1, 2)
    # Also include addresses that end in a number as many seem to be apartments (3, 4)
    unit_apt_move = if_else(unit_apt_new == "" & is.na(overridden) &
                              str_detect(unit_add_new, paste0("[:space:]+(", paste(secondary, collapse = "|"), ")")) == TRUE,
                            1, if_else(
                              unit_apt_new != "" & is.na(overridden) &
                                str_detect(unit_add_new, paste0("[:space:]+(", paste(secondary, collapse = "|"), ")")) == TRUE,
                              2, if_else(unit_apt_new == "" & is.na(overridden) &
                                           str_detect(unit_add_new, "[:space:]+[:alnum:]*[-]*[:digit:]+$") == TRUE &
                                           str_detect(unit_add_new, "PO BOX|PMB") == FALSE & str_detect(unit_add_new, "HWY 99$") == FALSE,
                                         3, if_else(unit_apt_new != "" & is.na(overridden) &
                                                      str_detect(unit_add_new, "[:space:]+[:alnum:]*[-]*[:digit:]+$") == TRUE &
                                                      str_detect(unit_add_new, "PO BOX|PMB") == FALSE & str_detect(unit_add_new, "HWY 99$") == FALSE,
                                                    4, 0
                                         )))),
    # Move apartment numbers to unit_apt_new if that field currently blank
    unit_apt_new = if_else(unit_apt_move == 1,
                           str_sub(unit_add_new, str_locate(unit_add_new, paste0("[:space:]+(", paste(secondary, collapse = "|"), ")"))[, 1], 
                                   str_length(unit_add_new)),
                           unit_apt_new),
    unit_apt_new = if_else(unit_apt_move == 3,
                           str_sub(unit_add_new, str_locate(unit_add_new, "[:space:]+[:alnum:]*[-]*[:digit:]+$")[, 1], str_length(unit_add_new)),
                           unit_apt_new),
    # Merge apt data from unit_add_new with unit_apt_new if the latter is currently not blank
    unit_apt_new = if_else(unit_apt_move == 2,
                           paste(str_sub(unit_add_new, str_locate(unit_add_new, paste0("[:space:]*(", paste(secondary, collapse = "|"), ")"))[, 1], 
                                         str_length(unit_add_new)),
                                 unit_apt_new, sep = " "),
                           unit_apt_new),
    unit_apt_new = if_else(unit_apt_move == 4 & str_detect(unit_apt, "#") == FALSE,
                           paste(str_sub(unit_add_new, str_locate(unit_add_new, "[:space:]+[:alnum:]*[-]*[:digit:]+$")[, 1], 
                                         str_length(unit_add_new)), unit_apt_new, sep = " "),
                           if_else(unit_apt_move == 4 & str_detect(unit_apt, "#") == TRUE,
                                   paste(str_sub(unit_add_new, str_locate(unit_add_new, "[:space:]+[:alnum:]*[-]*[:digit:]+$")[, 1], 
                                                 str_length(unit_add_new)), 
                                         str_sub(unit_apt_new, str_locate(unit_apt_new, "[:digit:]")[, 1], str_length(unit_apt_new)),
                                         sep = " "),
                                   unit_apt_new)),
    # Remove apt data from the address field (this needs to happen after the above code)
    unit_add_new = if_else(unit_apt_move %in% c(1, 2),
                           str_sub(unit_add_new, 1, str_locate(unit_add_new, paste0("[:space:]+(", paste(secondary, collapse = "|"), ")"))[, 1] - 1),
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
  mutate_at(vars(unit_add_new, unit_apt_new), funs(str_replace_all(., "[:space:]+", " ")))

# Clean remaining apartment issues
pha_cleanadd <- pha_cleanadd %>%
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
  )


# Clean up street addresses
pha_cleanadd <- pha_cleanadd %>%
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
pha_cleanadd <- pha_cleanadd %>%
  mutate(
    unit_city_new = str_replace(unit_city_new, "FEDERAL WY", "FEDERAL WAY"),
    unit_city_new = str_replace(unit_city_new, "SEATTTLE", "SEATTLE")
  )

# Get rid of remaining extraneous punctuation
pha_cleanadd <- pha_cleanadd %>%
  mutate(unit_add_new = str_replace_all(unit_add_new, "[-]+", " "),
         # One more cleanup of extra spaces
         unit_add_new = str_replace_all(unit_add_new, "[:space:]+", " "),
         unit_add_new = str_trim(unit_add_new)
  )

# Transfer data to rows where the address is blank due to ending participation in a program (mostly KCHA)
pha_cleanadd <- pha_cleanadd %>%
  mutate_at(vars(prog_type, vouch_type, property_name, property_type, portfolio), 
            funs(toupper(.))) %>% 
  # Make concatenated agency/prog type/subtype/spec voucher type field to make life easier
  mutate(agency_prog_concat = paste(agency_new, major_prog, prog_type, vouch_type, sep = ", "))

pha_cleanadd <- pha_cleanadd %>%
  arrange(pid, act_date, agency_prog_concat) %>%
  mutate_at(vars(unit_add_new, unit_apt_new, unit_apt2_new, unit_city_new, unit_state_new),
            funs(ifelse((. == "") & pid == lag(pid, 1) & !is.na(lag(pid, 1)) & 
                          agency_prog_concat == lag(agency_prog_concat, 1) & act_type %in% c(5, 6),
                        lag(., 1), .))) %>%
  # Need to do ZIP separately
  mutate(unit_zip_new = 
           ifelse(unit_zip_new %in% c(0, NA) & pid == lag(pid, 1) & !is.na(lag(pid, 1)) & 
                    agency_prog_concat == lag(agency_prog_concat, 1) & act_type %in% c(5, 6) &
                    unit_add_new == lag(unit_add_new, 1),
                  lag(unit_zip, 1), unit_zip_new)) %>%
  # remove temporary agency
  select(-agency_prog_concat)

# For some reason there are a bunch of blank ZIPs even though other rows with 
# the same address have a ZIP. Sort by address and copy over ZIP.
pha_cleanadd <- pha_cleanadd %>%
  group_by(unit_add_new, unit_apt_new, unit_apt2_new, unit_city_new, unit_state_new) %>%
  mutate(unit_zip_new = ifelse(is.na(unit_zip_new), max(unit_zip_new), unit_zip_new)) %>%
  ungroup()


# Make concatenated version of address fields
pha_cleanadd <- pha_cleanadd %>%
  mutate(unit_concat = paste(unit_add_new, unit_apt_new, unit_city_new, unit_state_new, unit_zip_new, sep = ","))

rm(adds_specific)


#### STOP HERE IF GEOCODING WILL BE RERUN ####
#saveRDS(pha_cleanadd, file = paste0(housing_path, "/OrganizedData/pha_cleanadd_midpoint.Rda"))


#### START HERE IF GEOCODING HAS BEEN COMPLETED
### Bring mid-point data back in
#pha_cleanadd <- readRDS(file = paste0(housing_path, "/OrganizedData/pha_cleanadd_midpoint.Rda"))

#### Merge with geocoded address data ####
# The geocoded address data should have more accurate street names, ZIPs, etc.
# Using these addresses will allow for better row consolidation below

# Bring in data
# adds_matched <- readRDS("//phdata01/DROF_DATA/DOH DATA/Housing/Geocoding/PHA_addresses_matched_combined.Rda")
# adds_matched <- adds_matched %>% mutate(unit_zip_new = as.numeric(unit_zip_new))
# 
# # Merge data
# pha_cleanadd <- left_join(pha_cleanadd, adds_matched, by = c("unit_add_new", "unit_city_new", "unit_state_new", "unit_zip_new"))
# pha_cleanadd <- pha_cleanadd %>% rename(unit_concat = unit_concat.x) %>%
#   select(-unit_concat.y)

# Parse out updated addresses (to come)
# pha_cleanadd <- pha_cleanadd %>%
#   mutate(unit_add_new2 = )
# 

# Remove temp files
# rm(adds_matched)


#### START HERE IF SKIPPING GEOCODING ####

#### Merge KCHA development data now that addresses are clean #####
pha_cleanadd <- pha_cleanadd %>%
  mutate(dev_city = paste0(unit_city_new, ", ", unit_state_new, " ", unit_zip_new),
    # Trim any white space
    dev_city = str_trim(dev_city)
  )

# HCV
# Bring in data
kcha_dev_adds <- fread(file = file.path(housing_path, "KCHA/Original data/", 
                                        "Development Addresses_received_2017-07-21.csv"), 
                                        stringsAsFactors = FALSE)
# Bring in variable name mapping table
fields <- read.csv(text = RCurl::getURL("https://raw.githubusercontent.com/PHSKC-APDE/Housing/master/processing/Field%20name%20mapping.csv"), 
                   header = TRUE, stringsAsFactors = FALSE)
kcha_dev_adds <- data.table::setnames(kcha_dev_adds, fields$PHSKC[match(names(kcha_dev_adds), fields$KCHA_modified)])


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


pha_cleanadd <- left_join(pha_cleanadd, kcha_dev_adds, by = c("unit_add_new" = "dev_add", "dev_city"))
rm(kcha_dev_adds)

# Sort out which values to keep
# Based on KCHA input, using imported data
pha_cleanadd <- pha_cleanadd %>%
  mutate(portfolio = ifelse(is.na(portfolio.y), portfolio.x, portfolio.y),
         property_name = ifelse(is.na(property_name.y), property_name.x, property_name.y),
         property_type = ifelse(is.na(property_type.y), property_type.x, property_type.y)) %>%
  select(-portfolio.x, -portfolio.y, -property_name.x, -property_name.y, -property_type.x, -property_type.y)


#### Save point ####
saveRDS(pha_cleanadd, file = paste0(housing_path, "/OrganizedData/pha_cleanadd_final.Rda"))

rm(fields)
rm(secondary)
rm(secondary_init)
rm(pha_recoded)
gc()
