###############################################################################
# Code to save data from the King County Housing Authority to SQL
#
# Alastair Matheson (PHSKC-APDE)
# 2017-05-31
###############################################################################

##### Set up global parameter and call in libraries #####
library(RODBC) # Used to connect to SQL server


##### Connect to the servers #####
db.apde51 <- odbcConnect("PH_APDEStore51")


##### Bring in raw data and combine #####
### Bring in data
kcha_old_p1 <- read.csv(file = "//phdata01/DROF_DATA/DOH DATA/Housing/KCHA/Original data/kcha_panel_01_2004-2015_received_2017-03-07.csv", stringsAsFactors = FALSE)
kcha_old_p2 <- read.csv(file = "//phdata01/DROF_DATA/DOH DATA/Housing/KCHA/Original data/kcha_panel_02_2004-2015_received_2017-03-07.csv", stringsAsFactors = FALSE)
kcha_old_p3 <- read.csv(file = "//phdata01/DROF_DATA/DOH DATA/Housing/KCHA/Original data/kcha_panel_03_2004-2015_received_2017-03-07.csv", stringsAsFactors = FALSE)

kcha_new_p1 <- read.csv(file = "//phdata01/DROF_DATA/DOH DATA/Housing/KCHA/Original data/kcha_panel_01_2016_received_2017-05-04.csv", stringsAsFactors = FALSE)
kcha_new_p2 <- read.csv(file = "//phdata01/DROF_DATA/DOH DATA/Housing/KCHA/Original data/kcha_panel_02_2016_received_2017-05-04.csv", stringsAsFactors = FALSE)
kcha_new_p3 <- read.csv(file = "//phdata01/DROF_DATA/DOH DATA/Housing/KCHA/Original data/kcha_panel_03_2016_received_2017-05-04.csv", stringsAsFactors = FALSE)


### Remove duplicates to reduce join issues (not needed with newer data)
kcha_old_p1 <- kcha_old_p1 %>% distinct()
kcha_old_p2 <- kcha_old_p2 %>% distinct()
kcha_old_p3 <- kcha_old_p3 %>% distinct()


### Join into a single file for each extract
kcha_old_full <- list(kcha_old_p1, kcha_old_p2, kcha_old_p3) %>%
  Reduce(function(dtf1, dtf2) full_join(dtf1, dtf2, by = c("subsidy_id", "h2a", "h2b")), .)

kcha_new_full <- list(kcha_new_p1, kcha_new_p2, kcha_new_p3) %>%
  Reduce(function(dtf1, dtf2) full_join(dtf1, dtf2, by = c("householdid", "certificationid", "h2a", "h2b")), .)


### Rename and reformat a few variables to make for easier appending
# Dates in older data come as integers with dropped leading zeros (and sometimes dropped second zero before the days)
kcha_old_full <- kcha_old_full %>%
  mutate_at(vars(h2b, h2h, starts_with("h3e")),
            funs(as.Date(ifelse(nchar(as.character(.)) == 7, paste0("0", as.character(.)),
                                ifelse(nchar(as.character(.)) == 6, 
                                       paste0("0", str_sub(., 1, 1), "0", str_sub(., 2, 6)),
                                       as.character(.))), "%m%d%Y")))

# Dates in newer data come as character
kcha_new_full <- kcha_new_full %>%
  mutate_at(vars(h2b, h2h, starts_with("h3e")),
            funs(as.Date(., format = "%m/%d/%Y")))


# Keep all SSNs in both data as characters for now
kcha_old_full <- kcha_old_full %>%
  mutate_at(vars(starts_with("h3n")),
            funs(as.character(.)))

kcha_new_full <- kcha_new_full %>%
  mutate_at(vars(starts_with("h3n")),
            funs(as.character(.)))


# A few other vars are character only in older data
kcha_old_full <- kcha_old_full %>%
  mutate_at(vars(h20b, h20d, h21b, h21e, h21i, h21j, h21k, h21n, h21p),
            funs(as.numeric(.)))


# The city variable seems misnamed in newer data
kcha_new_full <- kcha_new_full %>%
  rename(h5a3 = h5a2)


### Append latest extract
kcha <- bind_rows(kcha_old_full, kcha_new_full)


##### Load to SQL server #####
# May need to delete table first
#sqlDrop(db.apde51, "dbo.kcha_combined_raw")
sqlSave(db.apde51, kcha, tablename = "dbo.kcha_combined_raw",
        varTypes = c(
          h2b = "Date",
          h2h = "Date",
          h3e01 = "Date", h3e02 = "Date", h3e03 = "Date", h3e04 = "Date", h3e05 = "Date", h3e06 = "Date",
          h3e07 = "Date", h3e08 = "Date", h3e09 = "Date", h3e10 = "Date", h3e11 = "Date", h3e12 = "Date"
        ))


##### Remove temporary files #####
rm(list = ls(pattern = "kcha_old"))
rm(list = ls(pattern = "kcha_new"))
gc()
