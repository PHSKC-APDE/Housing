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

library(openxlsx) # Used to import/export Excel files
library(data.table) # used to read in csv files and rename fields
library(tidyverse) # Used to manipulate data
library(reticulate) # used to pull in python-based address parser
library(ggmap) # used to geocode addresses
library(rgdal) # Used to convert coordinates between ESRI and Google output


path <- "//phdata01/DROF_DATA/DOH DATA/Housing"
bounds <- "& bounds=47,-122.7|48,-121"


### Import Python address parser
addparser <- import("usaddress")


# file = paste0(housing_path, "/OrganizedData/pha_cleanadd_midpoint.Rda"))

#### Parse addresses (will be useful for geocoding) ####

### Set up distinct addresses for parsing
# Remove confidential addresses, those associated with ports, and those that can't be geocoded due to a lack of address)
adds <- pha_cleanadd %>%
  # Strip out apartment numbers and recreate unit_concat
  distinct(unit_add_new, unit_city_new, unit_state_new, unit_zip_new) %>%
  mutate(unit_concat_short = paste(unit_add_new, unit_city_new, unit_state_new, unit_zip_new, sep = ", ")) %>%
  arrange(unit_add_new, unit_city_new, unit_state_new, unit_zip_new, unit_concat_short) %>%
  filter(str_detect(unit_concat_short, "CONFI|PORTABLE|, , , 0|, , , NA") == FALSE & 
           unit_add_new != "PORT OUT")

### Make empty list to add data to
addlist = list()
# Loop over addresses
for (i in 1:nrow(adds)) {
  add <- addparser$tag(adds$unit_concat_short[i])[1]
  addlist[[i]] <- toString(add[[1]])
}

### Export data for geocoding
write.xlsx(adds, file = paste0("//phdata01/DROF_DATA/DOH DATA/Housing/Geocoding/PHA addresses for geocoding_",
                               Sys.Date(), ".xlsx"))



##### Unmatched data #####
### ArcMap doesn't get it all right so need to use Google to geocode remaining addresses

# Bring in unmatched addresses
adds_unmatched <- read.xlsx(paste0(path, "/Geocoding/PHA_addresses_unmatched.xlsx"))
# Remove missing addresses
adds_unmatched <- adds_unmatched %>% filter(unit_add_n != " ")


### BING APPROACH
# Set up columns for Bing maps processing
adds_unmatched_bing <- adds_unmatched %>%
  # Select and rename columns
  select(FID, unit_add_n, unit_city_, unit_state, unit_zip_n) %>%
  rename(Id = FID, `GeocodeRequest/Address/AddressLine` = unit_add_n, `GeocodeRequest/Address/Locality` = unit_city_,
         `GeocodeRequest/Address/AdminDistrict` = unit_state, `GeocodeRequest/Address/PostalCode` = unit_zip_n)

# Save combined file
write.csv(adds_unmatched_bing, paste0(path, "/Geocoding/PHA_addresses_unmatched_bing.csv"), row.names = FALSE)


### GOOGLE APPROACH
addresses <- paste0(adds_unmatched$unit_conca, ", USA")

# Using function found here: https://www.shanelynn.ie/massive-geocoding-with-r-and-google-maps/
#define a function that will process googles server responses for us.
getGeoDetails <- function(address){
  #use the gecode function to query google servers
  geo_reply = geocode(address, output = 'all', messaging = TRUE, override_limit = TRUE)
  #now extract the bits that we need from the returned list
  answer <- data.frame(lat=NA, long=NA, accuracy=NA, formatted_address=NA, address_type=NA, status=NA)
  answer$status <- geo_reply$status
  
  #if we are over the query limit - want to pause for an hour
  while(geo_reply$status == "OVER_QUERY_LIMIT"){
    print("OVER QUERY LIMIT - Pausing for 1 hour at:") 
    time <- Sys.time()
    print(as.character(time))
    Sys.sleep(60*60)
    geo_reply = geocode(address, output='all', messaging=TRUE, override_limit=TRUE)
    answer$status <- geo_reply$status
  }
  
  #return Na's if we didn't get a match:
  if (geo_reply$status != "OK"){
    return(answer)
  }   
  #else, extract what we need from the Google server reply into a dataframe:
  answer$lat <- geo_reply$results[[1]]$geometry$location$lat
  answer$long <- geo_reply$results[[1]]$geometry$location$lng   
  if (length(geo_reply$results[[1]]$types) > 0){
    answer$accuracy <- geo_reply$results[[1]]$types[[1]]
  }
  answer$address_type <- paste(geo_reply$results[[1]]$types, collapse=',')
  answer$formatted_address <- geo_reply$results[[1]]$formatted_address
  
  return(answer)
}

#initialise a dataframe to hold the results
geocoded <- data.frame()
# find out where to start in the address list (if the script was interrupted before):
startindex <- 1
#if a temp file exists - load it up and count the rows!
tempfilename <- paste0(path, "/Geocoding/PHA_addresses_matched_google.rds")
if (file.exists(tempfilename)){
  print("Found temp file - resuming from index:")
  geocoded <- readRDS(tempfilename)
  startindex <- nrow(geocoded)
  print(startindex)
}

# Start the geocoding process - address by address. geocode() function takes care of query speed limit.
for (ii in seq(startindex, length(addresses))){
  print(paste("Working on index", ii, "of", length(addresses)))
  #query the google geocoder - this will pause here if we are over the limit.
  result = getGeoDetails(addresses[ii]) 
  print(result$status)     
  result$index <- ii
  #append the answer to the results file.
  geocoded <- rbind(geocoded, result)
  #save temporary results as we are going along
  saveRDS(geocoded, tempfilename)
}

# Load already completed geocoding
geocoded <- readRDS(paste0(path, "/Geocoding/PHA_addresses_matched_google.rds"))

# Remove any duplicates made when the interrupted process restarted
geocoded <- geocoded %>% distinct()

# Join with address list
adds_matched_goog <- bind_cols(adds_unmatched, geocoded)

# Blank out coordinates when address is PORT OUT or address type is route
adds_matched_goog <- adds_matched_goog %>%
  mutate_at(vars(lat, long, accuracy, formatted_address, address_type), funs(ifelse(str_detect(unit_conca, "PORT OUT"), NA, .))) %>%
  mutate_at(vars(lat, long, accuracy, formatted_address), funs(ifelse(address_type == 'route', NA, .))) %>%
  mutate(status = ifelse(str_detect(unit_conca, "PORT OUT") | address_type == 'route', "ZERO_RESULTS", status))

# Blank out specific mismatches
adds_matched_goog <- adds_matched_goog %>%
  mutate(status = ifelse(accuracy %in% c("bar", "beauty_salon", "church", "convenience_store", "dentist"), "ZERO_RESULTS", status)) %>%
  mutate_at(vars(lat, long, accuracy, formatted_address, address_type), 
            funs(ifelse(accuracy %in% c("bar", "beauty_salon", "church", "convenience_store", "dentist"), NA, .)))

#### NOW KEEPING EVERYTHING IN GOOGLE (ESRI 4326 FORMAT) ####
# # Convert lat/long from Google to NAD_1983_HARN_StatePlane_Washington_North_FIPS_4601_Feet
# # Remove blank coordinates
# adds_matched_goog_sp <- adds_matched_goog %>% filter(!is.na(lat))
# # Set up spatial data frame
# coordinates(adds_matched_goog_sp) <- ~ long + lat
# proj4string(adds_matched_goog_sp) <- CRS("+init=epsg:4326") # WGS 84 system used by Google Maps
# # Convert to WA plane
# adds_matched_goog_sp <- spTransform(adds_matched_goog_sp, CRS("+proj=lcc +lat_1=47.5 +lat_2=49.73333333333333 +lat_0=47 +lon_0=-120.8333333333333 +x_0=500000.0000000001 +y_0=0 +ellps=GRS80 +to_meter=0.3048006096012192 +no_defs"))
# 
# # Select relevant columns and extract coordinates
# # NB. It seems like somewhere the lat/long, Y/X vectors were reversed. Need to check and fix.
# adds_matched_goog <- as.data.frame(adds_matched_goog_sp)
# adds_matched_goog <- adds_matched_goog %>%
#   mutate(X = long, Y = lat) %>%
#   select(FID, unit_conca:status, X, Y)
#### END CRS CONVERSION ####

#### Bring together ESRI and Google results ####
# Bring in ESRI data
adds_matched_esri <- read.xlsx(paste0(path, "/Geocoding/PHA_addresses_matched_ESRI.xlsx"))
# Join
adds_matched <- left_join(adds_matched_esri, adds_matched_goog, by = "FID")

# Collapse to useful columns and select matching from each source as appropriate
adds_matched <- adds_matched %>%
  rename(unit_add_new = unit_add_n.x, unit_city_new = unit_city_.x, unit_state_new = unit_state.x, 
         unit_zip_new = unit_zip_n.x, unit_concat = unit_conca.x, status_esri = Status.x, 
         status_goog = status, score_esri = Score.x, addr_type_esri = Addr_type.x, addr_type_goog = address_type,
         add_esri = Match_addr.x, add_goog = formatted_address, accuracy_goog = accuracy,
         match_type_esri = Match_type.x, id_esri = FID) %>%
  mutate(add_esri = toupper(add_esri),
         add_goog = toupper(add_goog),
         X = ifelse(status_goog == "OK" & !is.na(status_goog), long, POINT_X),
         Y = ifelse(status_goog == "OK" & !is.na(status_goog), lat, POINT_Y),
         formatted_address = ifelse(status_goog == "OK" & !is.na(status_goog), add_goog, add_esri),
         source = ifelse(status_goog == "OK" & !is.na(status_goog), "Google", "ESRI")) %>%
  select(unit_add_new:unit_concat, id_esri, status_esri:add_esri, addr_type_esri, add_esri, accuracy_goog, status_goog, 
         addr_type_goog, add_goog, formatted_address, X, Y, source)


# Next steps
# Link to HRA and other spatial units



# Save data for now
saveRDS(adds_matched, paste0(path, "/Geocoding/PHA_addresses_matched_combined.Rda"))

rm(adds_matched_esri)
rm(adds_matched_goog)
rm(geocoded)
rm(adds_unmatched)
gc()