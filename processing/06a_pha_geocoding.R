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
# 06 - Clean up addresses
# 06a - Geocode addresses ### (THIS CODE) ###
# 07 - Consolidate data rows
# 08 - Add in final data elements and set up analyses
# 09 - Join with Medicaid eligibility data
# 10 - Set up joint housing/Medicaid analyses
#
# Alastair Matheson (PHSKC-APDE)
# alastair.matheson@kingcounty.gov
# 2017-09, split into separate files 2017-10
# 
###############################################################################
if (UW == TRUE) {
  print("geocoding skipped") } else {
#### Set up global parameter and call in libraries ####
library(openxlsx) # Used to import/export Excel files
library(data.table) # used to read in csv files and rename fields
library(tidyverse) # Used to manipulate data
library(ggmap) # used to geocode addresses
library(opencage) # used to geocode addresses (alternative)
library(rgdal) # Used to convert coordinates between ESRI and Google output
library(sf) # newer package for working with spatial data



bounds <- "& bounds=47,-122.7|48,-121"
bounds_opencage <- c(-123, 46.8, -120.5, 48.5)


#### BRING IN DATA ####
pha_cleanadd <- readRDS(file.path(housing_path, "/OrganizedData/pha_cleanadd_midpoint.Rda"))

### If addresses have already been geocoded, bring them in here
# Initial geocoding results (differs from future approaches)
esri_20170824 <- read.xlsx(file.path(housing_path,
                                            PHA_addresses_matched_ESRI_fn))
goog_20170824 <- readRDS(file.path(housing_path,
                                   PHA_addresses_matched_google_fn))


#### PROCESS PREVIOUSLY GEOCODED DATA ####
if(UW == TRUE){
# Join
adds_matched <- left_join(esri_20170824, goog_20170824, by = c("FID" = "index"))

# Collapse to useful columns and select matching from each source as appropriate
adds_matched <- adds_matched %>%
   rename(unit_add_new = unit_add_n,
           unit_city_new = unit_city_,
           unit_state_new = unit_state,
           unit_zip_new = unit_zip_n,
           unit_concat = unit_conca,
           status_esri = Status,
           status_goog = status,
           score_esri = Score,
           addr_type_esri = Addr_type,
           addr_type_goog = address_type,
           add_esri = Match_addr,
           add_goog = formatted_address,
           accuracy_goog = accuracy,
           match_type_esri = Match_type,
           id_esri = FID) %>%
   mutate(add_esri = toupper(add_esri),
          add_goog = toupper(add_goog),
          X = ifelse(status_goog == "OK" & !is.na(status_goog), long, POINT_X),
          Y = ifelse(status_goog == "OK" & !is.na(status_goog), lat, POINT_Y),
          formatted_address = ifelse(status_goog == "OK" & !is.na(status_goog), add_goog, add_esri),
          source = ifelse(status_goog == "OK" & !is.na(status_goog), "Google", "ESRI")) %>%
   select(unit_add_new:unit_concat, id_esri, status_esri:add_esri, addr_type_esri, add_esri, accuracy_goog, status_goog,
          addr_type_goog, add_goog, formatted_address, X, Y, source) %>%
   mutate(unit_zip_new = as.numeric(unit_zip_new))
# Merge data
pha_cleanadd_coded <- left_join(pha_cleanadd, adds_matched, by = c("unit_add_new", "unit_city_new", "unit_state_new", "unit_zip_new"))
pha_cleanadd_coded <- pha_cleanadd_coded %>% rename(unit_concat = unit_concat.x) %>%
  select(-unit_concat.y)

#save add geocoded file
saveRDS(pha_cleanadd_coded, paste0(housing_path, 
                             pha_cleanadd_geocoded_fn))
# Remove temp variable
rm(adds_matched)
} else {
### Initial geocoding results (differs from future approaches) - 2017-08-24
adds_matched_20170824 <- left_join(esri_20170824, goog_20170824, by = "FID")
# Collapse to useful columns and select matching from each source as appropriate
adds_matched_20170824 <- adds_matched_20170824 %>%
  # Fix up truncated names
  rename(unit_add_new = unit_add_n, 
         unit_city_new = unit_city_, 
         unit_state_new = unit_state, 
         unit_zip_new = unit_zip_n, 
         unit_concat_short = unit_conca) %>%
  # Add metadata indicating where the geocode comes from and if ZIP centroid
  mutate(
    unit_zip_new = as.numeric(unit_zip_new),
    check_esri = 1,
    check_google = ifelse(!is.na(status), 1, 0),
    check_opencage = 0,
    geocode_source = ifelse(status == "OK" & !is.na(status), "google", "esri"),
    zip_centroid = ifelse(geocode_source == "esri" & 
                            Loc_name == "zip_5_digit_gc", 1, 0),
    ### Move address and coordindate data into a single field
    add_geocoded = ifelse(geocode_source == "esri", 
                          toupper(Match_addr), 
                          toupper(formatted_address)),
    zip_geocoded = ifelse(geocode_source == "esri", ARC_ZIP, 
                          str_sub(formatted_address,
                                  str_locate(formatted_address, "[:digit:]{5},")[,1],
                                  str_locate(formatted_address, "[:digit:]{5},")[,2]-1)),
    lon = ifelse(geocode_source == "esri", POINT_X, long),
    lat = ifelse(geocode_source == "esri", POINT_Y, lat)
  ) %>%
  select(unit_add_new, unit_city_new, unit_state_new, unit_zip_new, unit_concat_short, 
         check_esri, check_google, check_opencage, geocode_source, zip_centroid,
         add_geocoded, zip_geocoded, lon, lat) %>%
  # Keep only addresses that have been matched or checked across both geocoders
  filter(!is.na(lon) | (is.na(lon) & check_esri == 1 & check_google == 1))



#### FIND NEW ADDRESSES ####
# Remove confidential addresses, those associated with ports, 
# and those that can't be geocoded due to a lack of address)
adds <- pha_cleanadd %>%
  # Strip out apartment numbers and recreate unit_concat
  distinct(unit_add_new, unit_city_new, unit_state_new, unit_zip_new) %>%
  mutate(unit_concat_short = paste(unit_add_new, unit_city_new, 
                                   unit_state_new, unit_zip_new, sep = ", ")) %>%
  arrange(unit_add_new, unit_city_new, unit_state_new, unit_zip_new, 
          unit_concat_short) %>%
  filter(str_detect(unit_concat_short, "CONFI|PORTABLE|, , , 0|, , , NA") == FALSE & 
           unit_add_new != "PORT OUT")


# Anti_join to find new addresses
adds_new <- anti_join(adds, adds_matched_20170824, by = "unit_concat_short")


#### EXPORT DATA FOR GEOCODING ####
write.xlsx(adds_new, file = paste0(housing_path,
                                      "/Geocoding/PHA addresses for geocoding_",
                                      Sys.Date(), ".xlsx"))


#### BRING IN GEOCODED DATA FOR ADDITIONAL PROCESSING ####
# Use pop up window from rstudioapi if package installed
# Otherwise use base R commance line prompt
if ("rstudioapi" %in% installed.packages()[,"Package"]) {
  last_date <- rstudioapi::showPrompt(title = last_date, 
                                      message = "When was the geocoding file last created (use YYYY-MM-DD format)?")
} else {
last_date <- readline(prompt = "When was the geocoding file last created (use YYYY-MM-DD format)? ")
}

latest_file <- paste0(housing_path, "/Geocoding/PHA_addresses_geocoded_esri_",
                          last_date, ".shp")

if (file.exists(latest_file)){
  print("Found geocoded data")
  adds_new_esri <- st_read(latest_file, stringsAsFactors = F)
} else {
  stop("Check file names and that ESRI geocoding was run")
}


### Add lat/long coordinates and convert back to data frame for easier merging below
adds_new_esri <- st_transform(adds_new_esri, 4326)
adds_new_esri <- adds_new_esri %>%
  mutate(
    check_esri = 1,
    lon = st_coordinates(.)[,1],
    lat = st_coordinates(.)[,2]
    )
 st_geometry(adds_new_esri) <- NULL

### Pull out unmatched addresses
adds_new_unmatched <- adds_new_esri %>% filter(Status == "U")

### Run Opencage geocoder
# Store API key temporarily
# Sign up for a key here: https://opencagedata.com (2,500 limit per day)
if ("rstudioapi" %in% installed.packages()[,"Package"]) {
  opencage_key <- rstudioapi::askForPassword(prompt = 'Please enter API key: ')
} else {
  opencage_key <- readline(prompt = "Please enter API key: ")
}


# Make a function to geocode and return results
geocode_cage <- function(address) {
  # Query open cage servers (make sure API key is stored)
  geo_reply <- opencage_forward(address, key = opencage_key, 
                                # Keep annotations to get Mercator coords
                                no_annotations = F,
                                # ensure the search is not stored on their servers
                                no_record = T,
                                # Remove bounds to look across the US
                                #bounds = bounds_opencage,
                                countrycode = "US")
  
  #Note how many attempts are left
  print(paste0(geo_reply$rate_info$remaining, " tries remaining"))
  
  # If we are over the query limit - wait until the reset
  while(geo_reply$rate_info$remaining < 1 & 
        Sys.time() < geo_reply$rate_info$reset) {
    print(paste0("No queries remaining - resume at: ", 
                 geo_reply$rate_info$reset))
    # Putting in several hours for now since the calc fails
    #Sys.sleep(abs(geo_reply$rate_info$reset - Sys.time()))
    Sys.sleep(60*60*24)
  }
  
  # Set up response for when answer is not specific enough
  answer <- data.frame(lat = NA,
                       lon = NA,
                       x = NA_real_,
                       y = NA_real_,
                       formatted_address = NA,
                       address_type = NA,
                       confidence = NA)
  
  # Temporarily store results as a df to filter and sort
  answer_tmp <- as.data.frame(geo_reply$results)
  answer_tmp <- answer_tmp %>%
    filter(components._type == "building") %>%
    # Take the most confident (i.e., smallest bounding box)
    arrange(desc(confidence)) %>%
    slice(1)
  
  # Return NAs if we didn't get a match:
  if (nrow(answer_tmp) == 0) {
    return(answer)
  }   
  # Else, extract what we need into a dataframe:
  answer <- answer %>%
    mutate(
      lat = answer_tmp$geometry.lat,
      lon = answer_tmp$geometry.lng,
      x = answer_tmp$annotations.Mercator.x,
      y = answer_tmp$annotations.Mercator.y,
      formatted_address = answer_tmp$formatted,
      address_type = answer_tmp$components._type,
      confidence = answer_tmp$confidence
    )
  
  return(answer)
}


# Initialise a dataframe to hold the results
adds_new_opencage <- data.frame()
# Find out where to start in the address list (if the script was interrupted before):
startindex <- 1

# Use this if nrow < 2,500 (otherwise see below)
for (i in seq(startindex, nrow(adds_new_unmatched))) {
  print(paste("Working on index", i, "of", nrow(adds_new_unmatched)))
  #query the google geocoder - this will pause here if we are over the limit.
  result <- geocode_cage(adds_new_unmatched$unit_conca[i])
  result$index <- i
  result$input_add <- adds_new_unmatched$unit_conca[i]
  result$check_google <- 0
  result$check_opencage <- 1
  #append the answer to the results file.
  adds_new_opencage <- rbind(adds_new_opencage, result)
}


#### This section only needed if working with >2,500 rows ####
# If a temp file exists - load it up and count the rows!
tempfile_cage <- paste0(housing_path, "/Geocoding/PHA_addresses_geocoded_opencage_", last_date, ".rds")
if (file.exists(tempfile_cage)){
  print("Found temp file - resuming from index:")
  adds_new_opencage <- readRDS(tempfile_cage)
  startindex <- nrow(adds_new_opencage)
  print(startindex)
}

# Start the geocoding process - address by address. geocode() function takes care of query speed limit.
for (i in seq(startindex, nrow(adds_new_unmatched))) {
  print(paste("Working on index", i, "of", nrow(adds_new_unmatched)))
  #query the google geocoder - this will pause here if we are over the limit.
  result <- geocode_cage(adds_new_unmatched$address[i])
  result$index <- i
  result$input_add <- adds_new_unmatched$unit_conca[i]
  result$check_google <- 1
  result$check_opencage <- 1
  #append the answer to the results file.
  adds_new_opencage <- rbind(adds_new_opencage, result)
  #save temporary results as we are going along
  saveRDS(adds_new_opencage, tempfile_cage)
}

# Remove any duplicates that snuck in due to retarting the process
adds_new_opencage <- adds_new_opencage %>% distinct()
#### End section for large number of rows ####



#### COMBINE NEW ESRI AND OPENCAGE RESULTS ####
adds_new_combined_name <- paste0("adds_matched_", str_replace_all(last_date, "-", ""))
adds_new_combined <- left_join(adds_new_esri, adds_new_opencage, 
                                             by = c("unit_conca" = "input_add"))

# Collapse to useful columns and select matching from each source as appropriate
adds_new_combined <- adds_new_combined %>%
  # Fix up truncated names
  rename(unit_add_new = unit_add_n, 
         unit_city_new = unit_city_, 
         unit_state_new = unit_state, 
         unit_zip_new = unit_zip_n, 
         unit_concat_short = unit_conca) %>%
  # Add metadata indicating where the geocode comes from and if ZIP centroid
  mutate(
    check_google = ifelse(is.na(check_google), 0, check_google),
    check_opencage = ifelse(is.na(check_opencage), 0, check_opencage),
    geocode_source = ifelse(!is.na(index) & !is.na(lat.y), "opencage", "esri"),
    zip_centroid = ifelse(geocode_source == "esri" & 
                            Loc_name == "zip_5_digit_gc", 1, 0),
    ### Move address and coordindate data into a single field
    add_geocoded = ifelse(geocode_source == "esri", 
                          toupper(Match_addr), 
                          toupper(formatted_address)),
    zip_geocoded = ifelse(geocode_source == "esri",
                          str_sub(Match_addr,
                                  str_locate(Match_addr, ", [:digit:]{5}")[,1]+2,
                                  str_locate(Match_addr, ", [:digit:]{5}")[,2]), 
                          str_sub(formatted_address,
                                  str_locate(formatted_address, "[:digit:]{5},")[,1],
                                  str_locate(formatted_address, "[:digit:]{5},")[,2]-1)),
    lon = ifelse(geocode_source == "esri", lon.x, lon.y),
    lat = ifelse(geocode_source == "esri", lat.x, lat.y)
  ) %>%
  select(unit_add_new, unit_city_new, unit_state_new, unit_zip_new, unit_concat_short, 
         check_esri, check_google, check_opencage, geocode_source, zip_centroid,
         add_geocoded, zip_geocoded, lon, lat)

# Rename with dynamically created data frame name  
assign(adds_new_combined_name, adds_new_combined)
rm(adds_new_combined)

# Save combined file for next time
saveRDS(get(adds_new_combined_name), paste0(housing_path, "/Geocoding/", 
                                            adds_new_combined_name, ".Rda"))


#### COMBINE PREVIOUS GEOCODES WITH NEW GEOCODES ####
# Note that there some addresses were run in the intial geocode that have since
# been cleaned up, so the combined file below > number of distinct addresses
# in the data (adds)

# Add to this list as more sets of addresses are coded
adds_matched_overall <- rbind(adds_matched_20170824, get(adds_new_combined_name)) %>%
  select(-unit_concat_short)





pha_cleanadd_coded <- left_join(pha_cleanadd, adds_matched_overall,
                                by = c("unit_add_new", "unit_city_new",
                                       "unit_state_new", "unit_zip_new"))


#### SAVE DATA ####
saveRDS(pha_cleanadd_coded, paste0(housing_path, 
                             "/OrganizedData/pha_cleanadd_midpoint_geocoded.Rda"))
}
rm(list = ls(pattern = ("adds")))
rm(list = ls(pattern = ("esri")))
rm(list = ls(pattern = ("goog")))
rm(list = ls(pattern = ("bounds")))
rm(result)
rm(latest_file)
rm(last_date)
rm(opencage_key)
rm(i)

gc()
  }
