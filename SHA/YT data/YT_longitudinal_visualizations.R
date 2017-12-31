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


### Bring in combined PHA/Medicaid data with YT and move types already noted
yt_elig_final <- readRDS("//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/SHA cleaning/yt_elig_final.Rds")


yt_moves <- yt_elig_final %>%
  select(pid2, yt, yt_old, yt_new, x_h, y_h, place, start_type, end_type) %>%
  filter(yt == 1 & !is.na(x_h) & x_h < 0 & !is.na(y_h) & y_h > 0) %>%
  mutate(lon = x_h, lat = y_h)


#### Plot events ####
# NB. Coordinates are now stored in Google maps formet (ESRI 4326) so no need to convert
coordinates(yt_moves) <- ~ lon + lat
proj4string(yt_moves) <- CRS("+init=epsg:4326")

# Add basemap
yt_move_map <- get_map(location = c(lon = mean(yt_moves$lon, na.rm = T), lat = mean(yt_moves$lat, na.rm = T)), 
                       source = "google", maptype = "terrain", color = "bw", zoom = 16, crop = F)

ggmap(yt_move_map) + 
  geom_point(data = as.data.frame(yt_moves), aes(x = lon, y = lat, color = start_type, shape = start_type)) +
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

