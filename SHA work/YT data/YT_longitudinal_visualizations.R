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


#### Bring in combined PHA/Medicaid data with some demographics already run ####
pha_elig_demogs <- readRDS(file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/pha_elig_demogs.Rda")


#### Set up key variables ####
# Yesler Terrace and scattered sites indicators
yt_elig_final <- pha_elig_demogs %>%
  mutate(
    yt = ifelse((property_id %in% c("001", "1", "591", "738", "743") & !is.na(property_id)) |
                  ((str_detect(unit_add_new, "1105 E F") | str_detect(unit_add_new, "1305 E F") | 
                      str_detect(unit_add_new, "820[:space:]*[E]*[:space:]*YESLER")) & !is.na(unit_add_new)), 1, 0),
    yt_old = ifelse(property_id %in% c("1", "001") & !is.na(property_id), 1, 0),
    yt_new = ifelse((property_id %in% c("591", "738", "743") & !is.na(property_id)) |
                      (!is.na(unit_add_new) & (str_detect(unit_add_new, "1105 E F") | 
                                                 str_detect(unit_add_new, "1305 E F") | 
                                                 str_detect(unit_add_new, "820[:space:]*[E]*[:space:]*YESLER"))),
                    1, 0),
    ss = ifelse(property_id %in% c("050", "051", "052", "053", "054", "055", "056", "057", "A42", "A43", 
                                   "I43", "L42", "P42", "P43") & !is.na(property_id), 1, 0)
  )

# Find people who were ever at YT or SS
yt_elig_final <- yt_elig_final %>%
  group_by(pid2) %>%
  mutate_at(vars(yt, ss), funs(ever = sum(., na.rm = TRUE))) %>%
  ungroup() %>%
  mutate_at(vars(yt_ever, ss_ever), funs(replace(., which(. > 0), 1)))


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
#   O = non-YT, non-scattered site SHA unit
#   S = SHA scattered site
#   U = unknown (i.e., new into SHA system, mostly people who only had Medicaid but not PHA coverage)
#   Y = YT address (old unit)


yt_elig_final <- yt_elig_final %>%
  arrange(pid2, startdate_c, enddate_c) %>%
  mutate(
    # First ID the place for that row
    place = ifelse(is.na(agency_new), "U", 
                   ifelse(agency_new == "KCHA" & !is.na(agency_new), "K",
                          ifelse(agency_new == "SHA" & !is.na(agency_new) & yt == 0 & ss == 0, "O",
                                 ifelse(agency_new == "SHA" & !is.na(agency_new) & yt_old == 1, "Y",
                                        ifelse(agency_new == "SHA" & !is.na(agency_new) & yt_new == 1, "N",
                                               ifelse(agency_new == "SHA" & !is.na(agency_new) & yt == 0 & ss == 1, "S", NA)))))),
    start_type = NA,
    start_type = ifelse(pid2 != lag(pid2, 1) | is.na(lag(pid2, 1)), paste0("U", place),
                        ifelse(pid2 == lag(pid2, 1) & !is.na(lag(pid2, 1)), paste0(lag(place, 1), place), start_type)),
    end_type = NA,
    end_type = ifelse((pid2 != lead(pid2, 1) | is.na(lead(pid2, 1))) & enddate_c < as.Date("2017-09-15"), paste0(place, "U"),
                      ifelse(pid2 == lead(pid2, 1) & !is.na(lead(pid2, 1)), paste0(place, lead(place, 1)), end_type))
    )


yt_moves <- yt_elig_final %>%
  select(pid2, yt, yt_old, yt_new, X, Y, place, start_type, end_type) %>%
  filter(yt == 1 & !is.na(X) & !is.na(Y)) %>%
  mutate(northing = X, easting = Y)


#### Plot events ####
# Convert spatial coordinates to one that can go on Google maps (they are stored in 1983 HARN State Plane WA N FIPS 4601)
coordinates(yt_moves) <- ~ easting + northing
proj4string(yt_moves) <- CRS("+proj=lcc +lat_1=47.5 +lat_2=49.73333333333333 +lat_0=47 +lon_0=-120.8333333333333 +x_0=500000.0000000001 +y_0=0 +ellps=GRS80 +to_meter=0.3048006096012192 +no_defs")
yt_moves <- spTransform(yt_moves, CRS("+init=epsg:4326"))

# Add basemap
yt_move_map <- get_map(location = c(lon = mean(yt_moves$easting, na.rm = T), lat = mean(yt_moves$northing, na.rm = T)), zoom = 14, crop = F)

ggmap(yt_move_map) + 
  geom_point(data = as.data.frame(yt_moves), aes(x = easting, y = northing, color = start_type, shape = start_type)) +
#  geom_label(data = as.data.frame(yt_moves), aes(x = easting, y = northing, label = pid2)) +
  scale_color_manual(name = "Move-in type",
                     labels = c("KCHA to new YT", "KCHA to old YT", "New YT to new YT", "Other SHA to new YT", "Other SHA to old YT",
                                "Scattered sites to new YT", "Scattered sites to old YT", "Unknown to new YT", "Unknown to old YT",
                                "Old YT to new YT", "Old YT to old YT"),
                     values = c("#999999", "#999999", "#E69F00", "#56B4E9", "#56B4E9", "#F0E442", "#F0E442",
                                "#0072B2", "#0072B2", "#D55E00", "#D55E00")) +
  scale_shape_manual(name = "Move-in type",
                     labels = c("KCHA to new YT", "KCHA to old YT", "New YT to new YT", "Other SHA to new YT", "Other SHA to old YT",
                                "Scattered sites to new YT", "Scattered sites to old YT", "Unknown to new YT", "Unknown to old YT",
                                "Old YT to new YT", "Old YT to old YT"),
                     values = c(16, 17, 16, 16, 17, 16, 17, 16, 17, 16, 17)) +
  theme(axis.title = element_blank(), axis.text = element_blank(), axis.ticks = element_blank()) +
  ggtitle("Move types in SHA data")


# Bring in map of Seattle from Google
seattle.map = get_map(location = "Seattle", source = "google", maptype = "terrain", color = "bw")
map <- ggmap(seattle.map)
moveins <- map + geom_point(data = yt_moves, aes(y = Y, x = X, color = start_type, shape = place)) +
  scale_color_brewer(type = "qual", palette = "Dark2") +
  theme(axis.title = element_blank(), axis.text = element_blank(), axis.ticks = element_blank()) +
  ggtitle("Types of moves into Yesler Terrace")
moveins

