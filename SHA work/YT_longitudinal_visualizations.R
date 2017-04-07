###############################################################################
# Code to visualize a longitudinal record of each SHA resident
# Builds on previous cleaning and date processing
#
# Alastair Matheson (PHSKC-APDE)
# 2017-03-28
###############################################################################

##### Set up global parameter and call in libraries #####
options(max.print = 700, scipen = 0)


library(ggplot2) # Used to make plots
library(ggmap) # Used to incorporate Google maps data
library(gganimate) # Used to make animations (use devtools::install_github("dgrtwo/gganimate"))
library(dplyr) # Used to manipulate data
library(foreign) # Used to read in dbf file from Arcmap output


##### Bring in data #####

# TBD, currently running cleaning code first. Eventually will refer to a SQL location


##### Geocode addresses #####
### Find unique addresses and export to geocode in Arcmap
adds <- yt_cleanadd %>% distinct(unit_add_new, unit_cty, unit_zip) %>%
  # Strip out apartment numbers
  mutate(unit_add_strip = str_replace(unit_add_new, "apt|APT[:space:][:alnum:]*", ""),
         unit_add_strip = trimws(unit_add_strip, which = c("right")))

# Export only non-APT addresses for faster geocoding in ArcMap
adds_strip <- adds %>% distinct(unit_add_strip, unit_cty, unit_zip)
write.csv(adds_strip, file = "//phdata01/DROF_DATA/DOH DATA/Housing/SHA/Geocoding/unique_address.csv", row.names = FALSE)

### Arcmap misses some addresses so need to finish off with Google
# Bring in partially geocoded data
adds_geo1 <- read.dbf(file = "//phdata01/DROF_DATA/DOH DATA/Housing/SHA/Geocoding/Unique_addresses_geocoded_ArcGIS.dbf", as.is = TRUE) %>%
  select(-(X), -(Y)) %>%
  rename(X = X2, Y = Y2)

# Filter out unmatched addresses
adds_geo_u <- adds_geo1 %>%
  filter(Status %in% c("U")) %>%
  select(Status, unit_add_s:unit_zip) %>%
  mutate(unit_state = "WA")

# Geocode unmatched addresses
# Used to write out to geocode via QGIS's Google interface due to R errors. seem to be resolved
# write.csv(adds_geo_u, file = "//phdata01/DROF_DATA/DOH DATA/Housing/SHA/Geocoding/unique_address_u.csv", row.names = FALSE)
adds_geo_m <- geocode(paste(adds_geo_u$unit_add_s, adds_geo_u$unit_cty, adds_geo_u$unit_zip, sep = ","), output = "more")

# Merge Google results with original list of unmatched addresses
# Need to add row numbers to make merge work
adds_geo_u$row <- seq(1:nrow(adds_geo_u)) 
adds_geo_m$row <- seq(1:nrow(adds_geo_m))

adds_geo2 <- left_join(adds_geo_u, adds_geo_m, by = c("row")) %>%
  select(unit_add_s:unit_zip, lon, lat) %>%
  rename(X = lon, Y = lat) %>%
  mutate(source = "Google")


### Merge two sets of geocodes
adds_geo3 <- adds_geo1 %>%
  filter(Status %in% c("M", "T")) %>%
  select(unit_add_s:unit_zip, X, Y) %>%
  mutate(source = "ArcMap") %>%
  full_join(., adds_geo2, by = c("unit_add_s", "unit_cty", "unit_zip")) %>%
  mutate(X = ifelse(is.na(X.x), X.y, X.x),
         Y = ifelse(is.na(Y.x), Y.y, Y.x),
         source = ifelse(is.na(source.x), source.y, source.x)) %>%
  select(unit_add_s:unit_zip, X, Y, source) %>%
  rename(unit_add_strip = unit_add_s)


### Merge back with original distinct address data then original cleaned data
adds <- left_join(adds, adds_geo3, by = c("unit_add_strip", "unit_cty", "unit_zip"))
yt_cleanadd <- left_join(yt_cleanadd, adds, by = c("unit_add_new", "unit_cty", "unit_zip"))


##### Identify events #####
##############################################
# Goal is to categorize changes in addresses into one of the following categories:
# However, now using simplified system below

# ENTRY INTO SHA SYSTEM (CODE = E)
# E1) Entered into SHA system (YT address, old unit)
# E2) Entered into SHA system (YT address, new unit)
# E3) Entered into SHA system (non-YT SHA address)
# E4) Entered into SHA system (voucher user)
# Addtional K suffix if the person was previously in KCHA housing (e.g., code = E2K)

# MOVEMENT WITHIN SHA SYSTEM (CODE = M)
# M1) Moved from non-YT SHA address to YT (old unit)
# M2) Moved from non-YT SHA address to YT (new unit)
# M3) Moved from non-YT SHA address to another non-YT SHA address
# M4) Moved from voucher use address to YT (old unit)
# M5) Moved from voucher use address to YT (new unit)
# M6) Moved from voucher use address to another voucher use address
# M7) Moved from old YT unit to new YT unit
# M8) Moved from old YT unit to another old YT unit
# M9) Moved from new YT unit to old YT unit
# M10) Moved from new YT unit to another new YT unit
# M11) Moved from old YT unit to non-YT SHA address
# M12) Moved from new YT unit to non-YT SHA address
# M13) Moved from old YT unit to voucher use address
# M14) Moved from new YT unit to voucher use address

# EXIT FROM SHA SYSTEM (CODE = x)
# X1) Exited from SHA system (YT address, old unit)
# X2) Exited from SHA system (YT address, new unit)
# X3) Exited from SHA system (non-YT SHA address)
# X4) Exited from SHA system (voucher user)
# Addtional K suffix if the person moved to KCHA housing (e.g., code = X2K)
##########################################

# Use simplified system for describing movement

# First letter of start_type describes previous address,
# Second letter of start_type describes current address

# First letter of end_type describes current address,
# Second letter of end_type describes next address

#   K = KCHA
#   N = YT address (new unit)
#   S = non-YT SHA unit
#   U = unknown (i.e., new into SHA system)
#   V = voucher use address
#   Y = YT address (old unit)



# Notes:
# Currently we cannot differentiate between new and old YT units so all are assumed to be old units
# We also do not have SHA voucher data or KCHA data merged yet (as of 2017-03-31)



yt_cleanadd <- yt_cleanadd %>%
  mutate(
    start_type = NA,
    # Find when people first entered SHA system
    start_type = ifelse((pid != lag(pid, 1) | is.na(lag(pid, 1))) & yt == 1, "UY",
                        ifelse((pid != lag(pid, 1) | is.na(lag(pid, 1))) & yt == 0, "US", start_type)),
    # Find when people moved within YT
    start_type = ifelse(pid == lag(pid, 1) & !is.na(lag(pid, 1)) & yt == 1 & lag(yt, 1) == 1, "YY", start_type),
    # Find when people were previously at YT but are no longer
    start_type = ifelse(pid == lag(pid, 1) & !is.na(lag(pid, 1)) & yt == 0 & lag(yt, 1) == 1, "YS", start_type),
    # Find when people moved from non-YT into YT
    start_type = ifelse(pid == lag(pid, 1) & !is.na(lag(pid, 1)) & yt == 1 & lag(yt, 1) == 0, "SY", start_type),
    # Find when people moved from non-YT to another YT
    start_type = ifelse(pid == lag(pid, 1) & !is.na(lag(pid, 1)) & yt == 0 & lag(yt, 1) == 0, "SS", start_type),
    
    end_type = NA,
    # Find when people exited SHA system
    end_type = ifelse((pid != lead(pid, 1) | is.na(lead(pid, 1))) & yt == 1, "YU",
                      ifelse((pid != lead(pid, 1) | is.na(lead(pid, 1))) & yt == 0, "SU", end_type)),
    # Find when people's next unit is also within YT
    end_type = ifelse(pid == lead(pid, 1) & !is.na(lead(pid, 1)) & yt == lead(yt, 1) & yt == 1, "YY", end_type),
    # Find when people are at YT but not in next location
    end_type = ifelse(pid == lead(pid, 1) & !is.na(lead(pid, 1)) & yt == 1 & lead(yt, 1) == 0, "YS", end_type),
    # Find when people are not at YT but are at YT in next location
    end_type = ifelse(pid == lead(pid, 1) & !is.na(lead(pid, 1)) & yt == 0 & lead(yt, 1) == 1, "SY", end_type),
    # Find when people are not at YT and nor is their next SHa unit
    end_type = ifelse(pid == lead(pid, 1) & !is.na(lead(pid, 1)) & yt == 0 & lead(yt, 1) == 0, "SS", end_type)
    )


##### Plot events #####
# Bring in map of Seattle from Google
seattle.map = get_map(location = "Seattle", source = "google", maptype = "terrain", color = "bw")
map <- ggmap(seattle.map)
moveins <- map + geom_point(data = yt_cleanadd, aes(y = Y, x = X, color = start_type)) +
  scale_color_brewer(type = "qual", palette = "Dark2") +
  theme(axis.title = element_blank(), axis.text = element_blank(), axis.ticks = element_blank()) +
  ggtitle("Move types in SHA data")
moveins


  scale_color_brewer(type = "qual", palette = "Set2")
