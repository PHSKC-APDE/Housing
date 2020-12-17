###############################################################################
# OVERVIEW:
# Code to create a cleaned person table from the combined 
# King County Housing Authority and Seattle Housing Authority data sets
# Aim is to have a single row per contiguous time in a house per person
#
# STEPS:
# 01 - Process raw KCHA data and load to SQL database ### (THIS CODE) ###
# 02 - Process raw SHA data and load to SQL database
# 03 - Bring in individual PHA datasets and combine into a single file
# 04 - Deduplicate data and tidy up via matching process
# 05 - Recode race and other demographics
# 06 - Clean up addresses
# 06a - Geocode addresses
# 07 - Consolidate data rows
# 08 - Add in final data elements and set up analyses
# 09 - Join with Medicaid-Medicare eligibility & time varying data
# 10 - Set up joint housing/Medicaid analyses
#
# Alastair Matheson (PHSKC-APDE)
# alastair.matheson@kingcounty.gov
# 2017-05-17, split into separate files 2017-10
# 
###############################################################################

#### Set up global parameter and call in libraries ####
options(max.print = 600, tibble.print_max = 50, scipen = 999)

if(!require(housing)){
  devtools::install_github("PHSKC-APDE/Housing")
  require(housing) # contains many useful functions for cleaning
}

if(!require(odbc)){
  install.packages("odbc", repos='http://cran.us.r-project.org')
  require(odbc) # Used to connect to SQL server
}

if(!require(openxlsx)){
  install.packages("openxlsx", repos='http://cran.us.r-project.org')
  require(openxlsx) # Used to import/export Excel files
}

if(!require(data.table)){
  install.packages("data.table", repos='http://cran.us.r-project.org')
  require(data.table) # Used to read in csv files more efficiently
}

if(!require(tidyverse)){
  install.packages("tidyverse")
  require(tidyverse) # Used to manipulate data
}

if(!require(RJSONIO)){
  install.packages("RJSONIO", repos='http://cran.us.r-project.org')
  require(RJSONIO)
}

if(!require(RCurl)){
  install.packages("RCurl", repos='http://cran.us.r-project.org')
  require(RCurl)
}

if(!require(lubridate)){
  install.packages("lubridate")
  require(lubridate)
}

if(!require(RecordLinkage)){
  install.packages("RecordLinkage", repos='http://cran.us.r-project.org')
  require(RecordLinkage)
}

if(!require(phonics)){
  install.packages("phonics", repos='http://cran.us.r-project.org')
  require(phonics)
}

script <- RCurl::getURL("https://raw.githubusercontent.com/PHSKC-APDE/Housing/master/processing/metadata/set_data_env.r")
eval(parse(text = script))

housing_source_dir <- "local path"
METADATA = RJSONIO::fromJSON(paste0(housing_source_dir,"metadata/metadata.json"))
set_data_envr(METADATA, "kcha_data")



if (sql == TRUE) {
  db_apde51 <- dbConnect(odbc(), "PH_APDEStore51")
}

#####################################
#### PART 1: RAW DATA PROCESSING ####
#####################################

#### BRING IN DATA ####
# Some SSNs have temporary IDs in them so should be read in as characters
kcha_p1_2004_2015 <- fread(file = file.path(kcha_path, panel_1_2004_2015_fn), 
                           na.strings = c("NA", "", "NULL", "N/A", "."), 
                           stringsAsFactors = F)
kcha_p2_2004_2015 <- fread(file = file.path(kcha_path, panel_2_2004_2015_fn), 
                           na.strings = c("NA", "", "NULL", "N/A", "."), 
                           stringsAsFactors = F)
kcha_p3_2004_2015 <- fread(file = file.path(kcha_path, panel_3_2004_2015_fn), 
                           na.strings = c("NA", "", "NULL", "N/A", "."), 
                           stringsAsFactors = F)

kcha_p1_2016_2016 <- fread(file = file.path(kcha_path, panel_1_2016_2016_fn),
                      na.strings = c("NA", "", "NULL", "N/A", "."), 
                      stringsAsFactors = F,
                      colClasses = list(character = c("h3n08")))
kcha_p2_2016_2016 <- fread(file = file.path(kcha_path, panel_2_2016_2016_fn), 
                      na.strings = c("NA", "", "NULL", "N/A", "."), 
                      stringsAsFactors = F,
                      colClasses = list(character = c("h3n10", "h3n12")))
kcha_p3_2016_2016 <- fread(file = file.path(kcha_path, panel_3_2016_2016_fn), 
                      na.strings = c("NA", "", "NULL", "N/A", "."), 
                      stringsAsFactors = F)


kcha_p1_2017_2017 <- fread(file = file.path(kcha_path, panel_1_2017_2017_fn), 
                      na.strings = c("NA", "", "NULL", "N/A", "."), 
                      stringsAsFactors = F)
kcha_p2_2017_2017 <- fread(file = file.path(kcha_path, panel_2_2017_2017_fn), 
                      na.strings = c("NA", "", "NULL", "N/A", "."), 
                      stringsAsFactors = F)
kcha_p3_2017_2017 <- fread(file = file.path(kcha_path, panel_3_2017_2017_fn), 
                      na.strings = c("NA", "", "NULL", "N/A", "."), 
                      stringsAsFactors = F)

### Add 2018 data if available
if (add_2018 == TRUE) {
  kcha_p1_2018_2018 <- fread(file = file.path(kcha_path, panel_1_2018_2018_fn),
                        na.strings = c("NA", "", "NULL", "N/A", "."), 
                        stringsAsFactors = F)
  kcha_p2_2018_2018 <- fread(file = file.path(kcha_path, panel_2_2018_2018_fn), 
                        na.strings = c("NA", "", "NULL", "N/A", "."), 
                        stringsAsFactors = F)
  kcha_p3_2018_2018 <- fread(file = file.path(kcha_path, panel_3_2018_2018_fn), 
                        na.strings = c("NA", "", "NULL", "N/A", "."), 
                        stringsAsFactors = F)
  kcha_p1_2019_2019 <- fread(file = file.path(kcha_path, panel_1_2019_2019_fn),
                             na.strings = c("NA", "", "NULL", "N/A", "."), 
                             stringsAsFactors = F)
  kcha_p2_2019_2019 <- fread(file = file.path(kcha_path, panel_2_2019_2019_fn), 
                             na.strings = c("NA", "", "NULL", "N/A", "."), 
                             stringsAsFactors = F)
  kcha_p3_2019_2019 <- fread(file = file.path(kcha_path, panel_3_2019_2019_fn), 
                             na.strings = c("NA", "", "NULL", "N/A", "."), 
                             stringsAsFactors = F)
}


# Some of the KCHA end of participation data is missing from the original extract
if (UW == TRUE) {
  kcha_eop <- fread(file = file.path(kcha_path, kcha_eop_fn),
                  na.strings = c("NA", "", "NULL", "N/A", "."),  
                  stringsAsFactors = F) %>% 
    mutate(HOH.Birthdate = as.Date(HOH.Birthdate, origin = "1899-12-30"), 
           Effective.Date = as.Date(Effective.Date, origin = "1899-12-30"))
} else {
  kcha_eop <- fread(file = file.path(kcha_path, kcha_eop_fn),
                  na.strings = c("NA", "", "NULL", "N/A", "."), 
                  stringsAsFactors = F)
}

# Bring in variable name mapping table
fields <- read.csv(text = RCurl::getURL("https://raw.githubusercontent.com/PHSKC-APDE/Housing/master/processing/Field%20name%20mapping.csv"), 
                   header = T, stringsAsFactors = F)


#### DEDUPLICATE DATA ####
### First deduplicate data to avoid extra rows being made when joined
# Make list of data frames to deduplicate (not needed with newer data)
dfs <- list(kcha_p1_2004_2015 = kcha_p1_2004_2015, 
            kcha_p2_2004_2015 = kcha_p2_2004_2015, 
            kcha_p3_2004_2015 = kcha_p3_2004_2015)

# Clean up member numbers that are showing 0 and deduplicate data
df_dedups <- lapply(dfs, function(data) {
  data <- data %>% 
    mutate_at(vars(contains("h3a"), contains("h3e"), contains("h3m"),
                   contains("h5j"), contains("h19")), 
              list(~ifelse(. == 0, NA, .))) %>%
    distinct()
  return(data)
})

# Bring back data frames from list
list2env(df_dedups, .GlobalEnv)
rm(dfs)
rm(df_dedups)
gc()


### Remove duplicates in 2006-2015 panel 1
kcha_p1_2004_2015 <- kcha_p1_2004_2015 %>%
  arrange(subsidy_id, h2a, h2b) %>%
  mutate(drop = case_when(
    # Some rows are duplicated except one is missing gender and citizenship
    !is.na(lead(subsidy_id, 1)) & subsidy_id == lead(subsidy_id, 1) & 
      h2a == lead(h2a, 1) & h2b == lead(h2b, 1) &
      is.na(h3g01) & !is.na(lead(h3g01, 1)) & is.na(h3i01) & !is.na(lead(h3i01, 1)) ~ 1,
    !is.na(lag(subsidy_id, 1)) & subsidy_id == lag(subsidy_id, 1) & 
      h2a == lag(h2a, 1) & h2b == lag(h2b, 1) &
      is.na(h3g01) & !is.na(lag(h3g01, 1)) & is.na(h3i01) & !is.na(lag(h3i01, 1)) ~ 1,
    # Others are duplicated except field h2h (date admitted to program) is missing
    !is.na(lead(subsidy_id, 1)) & subsidy_id == lead(subsidy_id, 1) & 
      h2a == lead(h2a, 1) & h2b == lead(h2b, 1) & is.na(h2h) & !is.na(lead(h2h, 1)) ~ 1,
    !is.na(lag(subsidy_id, 1)) & subsidy_id == lag(subsidy_id, 1) & 
      h2a == lag(h2a, 1) & h2b == lag(h2b, 1) & is.na(h2h) & !is.na(lag(h2h, 1)) ~ 1,
    TRUE ~ 0
  )) %>%
  filter(drop == 0) %>%
  select(-drop)
  

### Remove duplicates in 2006-2015 panel 2
kcha_p2_2004_2015 <- kcha_p2_2004_2015 %>%
  # Remove white space in lines to further reduce duplicates
  mutate(h5a1a = str_squish(h5a1a),
         h5a1b = str_squish(h5a1b),
         # Some rows are identical except for missing addresses
         drop = case_when(
           !is.na(lead(subsidy_id, 1)) & subsidy_id == lead(subsidy_id, 1) & 
             h2a == lead(h2a, 1) & h2b == lead(h2b, 1) &
             is.na(h5a1a) & !is.na(lead(h5a1a, 1)) & h5d == 0 & lead(h5d, 1) == 1 ~ 1,
           !is.na(lag(subsidy_id, 1)) & subsidy_id == lag(subsidy_id, 1) & 
             h2a == lag(h2a, 1) & h2b == lag(h2b, 1) &
             is.na(h5a1a) & !is.na(lag(h5a1a, 1)) & h5d == 0 & lag(h5d, 1) == 1 ~ 1,
           TRUE ~ 0
         )) %>%
  filter(drop == 0) %>%
  select(-drop)


### Remove duplicates in 2006-2015 panel 3
# Some rows are identical except for missing total dollar columns (h19g, h19i)
dollar_dups <- kcha_p3_2004_2015 %>%
  filter(!is.na(h19g) & !is.na(h19i)) %>%
  distinct() %>% group_by(subsidy_id, h2a, h2b) %>%
  summarise(rows_new = n()) %>% ungroup()

kcha_p3_2004_2015 <- kcha_p3_2004_2015 %>%
  group_by(subsidy_id, h2a, h2b) %>% mutate(rows_old = n()) %>%
  ungroup() %>% 
  left_join(., dollar_dups, by = c("subsidy_id", "h2a", "h2b")) %>%
  # Keep original rows plus ones where dollar amounts are not missing
  filter(rows_old == rows_new | is.na(rows_new) |
            (rows_old != rows_new & !is.na(h19g) & !is.na(h19i))) %>%
  select(-rows_old, -rows_new)

rm(dollar_dups)

# Some rows are identical except for missing rent/utility info (h21i-h21k)
rent_dups <- kcha_p3_2004_2015 %>%
  group_by(subsidy_id, h2a, h2b) %>% mutate(rows = n()) %>% ungroup() %>%
  filter(rows > 1) %>% arrange(subsidy_id, h2a, h2b) %>%
  # Find rows where key vars are identical except for rent info
  mutate(drop = case_when(
    !is.na(lead(subsidy_id, 1)) & subsidy_id == lead(subsidy_id, 1) & 
      h2a == lead(h2a, 1) & h2b == lead(h2b, 1) &
      h19g == lead(h19g, 1) & h19i == lead(h19i, 1) &
      (h20b == lead(h20b, 1) | (is.na(h20b) & is.na(lead(h20b, 1)))) &
      is.na(h21i) & !is.na(lead(h21i, 1)) & h21n == 0 & lead(h21n, 1) > 0 ~ 1,
    !is.na(lag(subsidy_id, 1)) & subsidy_id == lag(subsidy_id, 1) & 
      h2a == lag(h2a, 1) & h2b == lag(h2b, 1) &
      h19g == lag(h19g, 1) & h19i == lag(h19i, 1) &
      (h20b == lag(h20b, 1) | (is.na(h20b) & is.na(lag(h20b, 1)))) &
      is.na(h21i) & !is.na(lag(h21i, 1)) & h21n == 0 & lag(h21n, 1) > 0 ~ 1,
    TRUE ~ 0
  )) %>%
  select(subsidy_id, h2a, h2b, h19g, h19i, h21i, h21j, h21k, h21n, drop) %>%
  filter(drop == 1)

kcha_p3_2004_2015 <- 
  left_join(kcha_p3_2004_2015, rent_dups, by = c("subsidy_id", "h2a", "h2b", "h19g", 
                                                 "h19i", "h21i", "h21j", "h21k", "h21n")) %>%
  filter(drop == 0 | is.na(drop))

rm(rent_dups)
  

#### COMBINE DATA ####
### Join into a single file for each extract and run simple error check
kcha_2004_2015_full <- list(kcha_p1_2004_2015, kcha_p2_2004_2015, 
                            kcha_p3_2004_2015) %>%
  Reduce(function(dtf1, dtf2) full_join(
    dtf1, dtf2, by = c("subsidy_id", "h2a", "h2b")), .)
# This fails due to multiple income/assets rows. Corrected below
nrow(kcha_2004_2015_full) == nrow(kcha_p1_2004_2015)

kcha_2016_2016_full <- list(kcha_p1_2016_2016, kcha_p2_2016_2016, kcha_p3_2016_2016) %>%
  Reduce(function(dtf1, dtf2) full_join(
    dtf1, dtf2, by = c("householdid", "certificationid", "h2a", "h2b")), .)
nrow(kcha_2016_2016_full) == nrow(kcha_p1_2016_2016)

kcha_2017_2017_full <- list(kcha_p1_2017_2017, kcha_p2_2017_2017, kcha_p3_2017_2017) %>%
  Reduce(function(dtf1, dtf2) full_join(
    dtf1, dtf2, by = c("householdid", "certificationid", "h2a", "h2b")), .)
nrow(kcha_2017_2017_full) == nrow(kcha_p1_2017_2017)

kcha_2018_2018_full <- list(kcha_p1_2018_2018, kcha_p2_2018_2018, kcha_p3_2018_2018) %>%
  Reduce(function(dtf1, dtf2) full_join(
    dtf1, dtf2, by = c("householdid", "certificationid", "vouchernumber", "h2a", "h2b")), .)
nrow(kcha_2018_2018_full) == nrow(kcha_p1_2018_2018)

kcha_2019_2019_full <- list(kcha_p1_2019_2019, kcha_p2_2019_2019, kcha_p3_2019_2019) %>%
  Reduce(function(dtf1, dtf2) full_join(
    dtf1, dtf2, by = c("householdid", "certificationid", "vouchernumber", "h2a", "h2b")), .)
nrow(kcha_2019_2019_full) == nrow(kcha_p1_2019_2019)


### Rename and reformat a few variables to make for easier appending
# Dates in older data come as integers with dropped leading zeros 
# (and sometimes dropped second zero before the days)
kcha_2004_2015_full <- kcha_2004_2015_full %>%
  mutate_at(vars(h2b, h2h, starts_with("h3e")),
            funs(as.Date(
              ifelse(nchar(as.character(.)) == 7, paste0("0", as.character(.)),
                     ifelse(nchar(as.character(.)) == 6, 
                            paste0("0", str_sub(., 1, 1), "0", str_sub(., 2, 6)),
                            as.character(.))), "%m%d%Y")))

# Dates in newer data come as character
kcha_2016_2016_full <- kcha_2016_2016_full %>%
  mutate_at(vars(h2b, h2h, starts_with("h3e")),
            funs(as.Date(., format = "%m/%d/%Y")))
            
kcha_2017_2017_full <- kcha_2017_2017_full %>%
  mutate_at(vars(h2b, h2h, starts_with("h3e")),
            funs(as.Date(., format = "%m/%d/%Y")))

kcha_2018_2018_full <- kcha_2018_2018_full %>%
  mutate_at(vars(h2b, h2h, starts_with("h3e")),
            funs(as.Date(., format = "%m/%d/%Y")))

kcha_2019_2019_full <- kcha_2019_2019_full %>%
  mutate_at(vars(h2b, h2h, starts_with("h3e")),
            funs(as.Date(., format = "%m/%d/%Y")))

# Keep all SSNs as characters for now 
# (2016/17 data already all character with newer version of fread)
kcha_2018_2018_full <- kcha_2018_2018_full %>%
  mutate_at(vars(starts_with("h3n")), funs(as.character(.)))

kcha_2019_2019_full <- kcha_2019_2019_full %>%
  mutate_at(vars(starts_with("h3n")), funs(as.character(.)))

# Fix up some inconsistent naming in income fields of <2015 data
kcha_2004_2015_full <- kcha_2004_2015_full %>%
  select(-h19a10a, -h19a11a, -h19a12a, -h19a13a, -h19a14a, 
         -h19a15a, -h19a16a, -h19a10b, -h19a11b, -h19a12b, 
         -h19a13b, -h19a14b, -h19a15b, -h19a16b) %>%
  rename(h19a10a = h1910a, h19a11a = h1911a, h19a12a = h1912a, h19a13a = h1913a,
         h19a14a = h1914a, h19a15a = h1915a, h19a16a = h1916a, 
         h19a10b = h1910b, h19a11b = h1911b, h19a12b = h1912b, h19a13b = h1913b,
         h19a14b = h1914b, h19a15b = h1915b, h19a16b = h1916b)

# The city variable seems misnamed in 2016 data (ok in 2017)
kcha_2016_2016_full <- kcha_2016_2016_full %>% rename(h5a3 = h5a2)

# Fix up an inconsitent name in the 2017 data
# Previously correcting types here but new version of fread obviates need for this
kcha_2017_2017_full <- kcha_2017_2017_full %>% rename(spec_vouch = spec_voucher)

# There are some 5+4 ZIPs in 2018 data, remove and make integer
kcha_2018_2018_full <- kcha_2018_2018_full %>%
  mutate(h5a5 = as.numeric(str_replace(h5a5, "-", "")))

kcha_2019_2019_full <- kcha_2019_2019_full %>%
  mutate(h5a5 = as.numeric(str_replace(h5a5, "-", "")))


# Renaming voucher number field here makes it easier to join with EOP file later
kcha_2018_2018_full <- kcha_2018_2018_full %>%
  rename(vouch_num = vouchernumber)

kcha_2019_2019_full <- kcha_2019_2019_full %>%
  rename(vouch_num = vouchernumber)

# Add source field to track where each row came from
kcha_2004_2015_full <- kcha_2004_2015_full %>% mutate(kcha_source = "kcha2015")
kcha_2016_2016_full <- kcha_2016_2016_full %>% mutate(kcha_source = "kcha2016")
kcha_2017_2017_full <- kcha_2017_2017_full %>% mutate(kcha_source = "kcha2017")
kcha_2018_2018_full <- kcha_2018_2018_full %>% mutate(kcha_source = "kcha2018")
kcha_2019_2019_full <- kcha_2019_2019_full %>% mutate(kcha_source = "kcha2019")

### Append latest extract
kcha <- bind_rows(kcha_2004_2015_full, kcha_2016_2016_full, kcha_2017_2017_full,
                  kcha_2018_2018_full, kcha_2019_2019_full)



#########################################
#### PART 2: RESHAPE AND REORGANIZE  ####
#########################################

### Drop columns with no information
kcha <- kcha %>% select(-(contains("15")), -(contains("16")))

#### Fix up program column (used for joining in this section)
kcha <- kcha %>%
  mutate(program_type = case_when(
    program_type == "P" ~ "PH",
    program_type == "PR" ~ "PBS8",
    program_type %in% c("T", "VO") ~ "TBS8",
    TRUE ~ program_type
  ))


#### REMOVE DUPLICATES ####
# There are duplicate rows where the only difference is the cert id (~426)
# Possibly a double data entry issue
# Remove via distinct, either list all columns or drop cert id (former for now)
kcha <- kcha %>%
  distinct(h1a, subsidy_id, h2a, h2b, program_type, h2c, h2d, h2h, h3a01, h3b01, 
           h3c01, h3d01, h3e01, h3g01, h3h01, h3i01, h3j01, h3k01a, h3k01b, 
           h3k01c, h3k01d, h3k01e, h3m01, h3n01, h3a02, h3b02, h3c02,  h3d02, 
           h3e02, h3g02, h3h02, h3i02, h3j02, h3k02a, h3k02b, h3k02c,  h3k02d, 
           h3k02e, h3m02, h3n02, h3a03, h3b03, h3c03, h3d03, h3e03,  h3g03, 
           h3h03, h3i03, h3j03, h3k03a, h3k03b, h3k03c, h3k03d, h3k03e,  h3m03, 
           h3n03, h3a04, h3b04, h3c04, h3d04, h3e04, h3g04, h3h04,  h3i04, 
           h3j04, h3k04a, h3k04b, h3k04c, h3k04d, h3k04e, h3m04, h3n04,  h3a05, 
           h3b05, h3c05, h3d05, h3e05, h3g05, h3h05, h3i05, h3j05,  h3k05a, 
           h3k05b, h3k05c, h3k05d, h3k05e, h3m05, h3n05, h3a06, h3b06,  h3c06, 
           h3d06, h3e06, h3g06, h3h06, h3i06, h3j06, h3k06a, h3k06b, h3k06c, 
           h3k06d, h3k06e, h3m06, h3n06, h3a07, h3b07, h3c07, h3d07, h3e07, 
           h3g07, h3h07, h3i07, h3j07, h3k07a, h3k07b, h3k07c, h3k07d, h3k07e, 
           h3m07, h3n07, h3a08, h3b08, h3c08, h3d08, h3e08, h3g08, h3h08, h3i08, 
           h3j08, h3k08a, h3k08b, h3k08c, h3k08d, h3k08e, h3m08, h3n08, h3a09, 
           h3b09, h3c09, h3d09, h3e09, h3g09, h3h09, h3i09, h3j09, h3k09a, 
           h3k09b, h3k09c, h3k09d, h3k09e, h3m09, h3n09, spec_vouch, h3a10, 
           h3b10, h3c10, h3d10, h3e10, h3g10, h3h10, h3i10, h3j10, h3k10a, 
           h3k10b, h3k10c, h3k10d, h3k10e, h3m10, h3n10, h3a11, h3b11, h3c11, 
           h3d11, h3e11, h3g11, h3h11, h3i11, h3j11, h3k11a, h3k11b, h3k11c, 
           h3k11d, h3k11e, h3m11, h3n11, h3a12, h3b12, h3c12, h3d12, h3e12, 
           h3g12, h3h12, h3i12, h3j12, h3k12a, h3k12b, h3k12c, h3k12d, h3k12e, 
           h3m12, h3n12, h5a1a, h5a1b, h5a2, h5a3, h5a4, h5a5, h5e, h5f, h5g, 
           h5j, h5k, h5d, h19a1a, h19a1b, h19b01, h19d01, h19f01, h19a2a, 
           h19a2b, h19b02, h19d02, h19f02, h19a3a, h19a3b, h19b03, h19d03, 
           h19f03, h19a4a, h19a4b, h19b04, h19d04, h19f04, h19a5a, h19a5b, 
           h19b05, h19d05, h19f05, h19a6a, h19a6b, h19b06, h19d06, h19f06, 
           h19a7a, h19a7b, h19b07, h19d07, h19f07, h19a8a, h19a8b, h19b08, 
           h19d08, h19f08, h19g, h19i, h19a9a, h19a9b, h19b09, h19d09, h19f09, 
           h19a10a, h19a10b, h19b10, h19d10, h19f10, h19a11a, h19a11b, h19b11, 
           h19d11, h19f11, h19a12a, h19a12b, h19b12, h19d12, h19f12, h19a13a, 
           h19a13b, h19b13, h19d13, h19f13, h19a14a, h19a14b, h19b14, h19d14, 
           h19f14, h20b, h20d, h21a, h21b, h21d, h21e, h21f, h21i, h21j, h21k, 
           h21n, h21p, kcha_source, householdid, developmentname, h19h, h19k, 
           h20a, h20c, h20e, h21m, h21q, propertytype, h3a13, h3a14, h3b13, 
           h3b14, h3c13, h3c14, h3d13, h3d14, h3e13, h3e14, h3g13, h3g14, h3h13, 
           h3h14, h3i13, h3i14, h3j13, h3j14, h3k13a, h3k13b, h3k13c, h3k13d, 
           h3k13e, h3k14a, h3k14b, h3k14c, h3k14d, h3k14e, h3m13, h3m14, h3n13, 
           h3n14, vouchernumber, box04b, box04c, .keep_all = T)


#### COMBINE HOUSEHOLD INCOME SOURCES BEFORE RESHAPING ####
# Much easier to do when the entire household is on a single row
# NB. There are many household/date combos repeated due to minor differences
# in rows, e.g., addresses formatted differently. This will mean household
# income is repeated until rows are cleaned up.

# Take most complete adjusted income data
# (sometimes h19h is missing when h19g is not, same is true for h19f and h19d, 
# which add up to h19h and h19g, respectively)
kcha <- kcha %>%
  mutate(
    h19f01 = ifelse(is.na(h19f01) & !is.na(h19d01), h19d01, h19f01),
    h19f02 = ifelse(is.na(h19f02) & !is.na(h19d02), h19d02, h19f02),
    h19f03 = ifelse(is.na(h19f03) & !is.na(h19d03), h19d03, h19f03),
    h19f04 = ifelse(is.na(h19f04) & !is.na(h19d04), h19d04, h19f04),
    h19f05 = ifelse(is.na(h19f05) & !is.na(h19d05), h19d05, h19f05),
    h19f06 = ifelse(is.na(h19f06) & !is.na(h19d06), h19d06, h19f06),
    h19f07 = ifelse(is.na(h19f07) & !is.na(h19d07), h19d07, h19f07),
    h19f08 = ifelse(is.na(h19f08) & !is.na(h19d08), h19d08, h19f08),
    h19f09 = ifelse(is.na(h19f09) & !is.na(h19d09), h19d09, h19f09),
    h19f10 = ifelse(is.na(h19f10) & !is.na(h19d10), h19d10, h19f10),
    h19f11 = ifelse(is.na(h19f11) & !is.na(h19d11), h19d11, h19f11),
    h19f12 = ifelse(is.na(h19f12) & !is.na(h19d12), h19d12, h19f12),
    h19f13 = ifelse(is.na(h19f13) & !is.na(h19d13), h19d13, h19f13),
    h19f14 = ifelse(is.na(h19f14) & !is.na(h19d14), h19d14, h19f14)
  )

# Make matrices of income codes and dollar amounts
inc_fixed <- kcha %>% select(contains("h19b")) %>%
  mutate_all(funs(ifelse(. %in% c("G", "P", "S", "SS"), 1, 0)))
inc_fixed <- as.matrix(inc_fixed)

inc_vary <- kcha %>% select(contains("h19b")) %>%
  mutate_all(funs(ifelse(. %in% c("G", "P", "S", "SS"), 0, 1)))
inc_vary <- as.matrix(inc_vary)

inc_amount <- kcha %>% select(contains("h19d"))
inc_amount <- as.matrix(inc_amount)

inc_adj_amount <- kcha %>% select(contains("h19f"))
inc_adj_amount <- as.matrix(inc_adj_amount)

# Calculate totals of fixed and varying incomes
# Using the adjusted amounts
inc_fixed_amt <- as.data.frame(inc_fixed * inc_amount)
inc_fixed_amt <- inc_fixed_amt %>%
  mutate(hh_inc_fixed = rowSums(., na.rm = TRUE)) %>%
  select(hh_inc_fixed)

inc_adj_fixed_amt <- as.data.frame(inc_fixed * inc_adj_amount)
inc_adj_fixed_amt <- inc_adj_fixed_amt %>%
  mutate(hh_inc_adj_fixed = rowSums(., na.rm = TRUE)) %>%
  select(hh_inc_adj_fixed)

inc_vary_amt <- as.data.frame(inc_vary * inc_amount)
inc_vary_amt <- inc_vary_amt %>%
  mutate(hh_inc_vary = rowSums(., na.rm = TRUE)) %>%
  select(hh_inc_vary)

inc_adj_vary_amt <- as.data.frame(inc_vary * inc_adj_amount)
inc_adj_vary_amt <- inc_adj_vary_amt %>%
  mutate(hh_inc_adj_vary = rowSums(., na.rm = TRUE)) %>%
  select(hh_inc_adj_vary)

# Join back to main data
kcha <- bind_cols(kcha, inc_fixed_amt, inc_adj_fixed_amt, 
                  inc_vary_amt, inc_adj_vary_amt)

# Remove temporary data
rm(list = ls(pattern = "inc_"))
gc()


#### SET UP HEAD OF HOUSEHOLD DATA ####
# Also set up data for joining to EOP data and reshaping
kcha <- kcha %>%
  mutate(
    hh_lname = h3b01,
    hh_fname = h3c01,
    hh_mname = h3d01,
    hh_ssn = h3n01,
    hh_dob = h3e01)


#### ADD MISSING END OF PARTICIPATION (EOP) CERTS ####
# Rename EOP fields to match KCHA
# NB. No longer using names from the fields csv so that the bind_rows command works
if (UW == TRUE) {
  kcha_eop <- kcha_eop %>%
    rename(householdid = `Household.ID`, vouch_num = `Voucher.Number`,
           hh_ssn = `HOH.SSN`, hh_dob = `HOH.Birthdate`, 
           hh_lname = `HOH.Full.Name`, program_type = `Program.Type`,
           h2a = `HUD-50058.2a.Type.of.Action`, h2b = `Effective.Date`)
} else {
kcha_eop <- kcha_eop %>%
  rename(householdid = `Household ID`, vouch_num = `Voucher Number`,
         hh_ssn = `HOH SSN`, hh_dob = `HOH Birthdate`, 
         hh_name = `HOH Full Name`, program_type = `Program Type`,
         h2a = `HUD-50058 2a Type of Action`, h2b = `Effective Date`)
}

# Pull out name components into separate fields
kcha_eop <- kcha_eop %>%
  mutate(
    hh_name = toupper(hh_name),
    # Extract middle initial if it exists
    m_init_chk = str_detect(hh_name, " \\w "),
    m_init_pos = str_locate(hh_name, " \\w ")[,1] + 1,
    hh_mname = ifelse(m_init_chk == T, str_sub(hh_name, m_init_pos, m_init_pos), NA),
    hh_name = ifelse(m_init_chk == T, str_replace(hh_name, " \\w ", " "), hh_name),
    # Extract first name (assume up until space is first name)
    hh_fname = str_sub(hh_name, 1, str_locate(hh_name, " ")[,1] - 1),
    # Put all remaining name parts in the last name field
    hh_lname = str_sub(hh_name, str_locate(hh_name, " ")[,1] + 1, nchar(hh_name))
    )
  

# Fix up variable types
kcha_eop <- kcha_eop %>%
  mutate(
    hh_ssn = str_replace_all(hh_ssn, "-", ""),
    hh_dob = as.Date(hh_dob, format = "%m/%d/%Y"),
    h2b = as.Date(h2b, format = "%m/%d/%Y"),
    prog_type = car::recode(program_type, "'MTW Tenant-Based Assistance' = 'TBS8';
                            'MTW Project-Based Assistance' = 'PBS8';
                            'MTW Public Housing' = 'PH'"),
    eop_source = "eop"
    ) %>%
  # Restrict to necessary columns
  select(householdid, vouch_num, hh_ssn, hh_dob, hh_lname, hh_fname, hh_mname,
         h2a, h2b, eop_source) %>%
  # Drop missing SSNs
  filter(!is.na(hh_ssn))

# Join EOP and HH info together
kcha <- bind_rows(kcha, kcha_eop) %>% arrange(hh_ssn, h2b, h2a, eop_source)

# Decide which row to keep when the EOP is already captured
kcha <- kcha %>%
  mutate(drop = case_when(
    # Keep EOP if names are missing from main data
    hh_ssn == lag(hh_ssn, 1) & h2b == lag(h2b, 1) & h2a == lag(h2a, 1) &
      is.na(hh_lname) & !is.na(lag(hh_lname, 1)) & lag(eop_source, 1) == "eop" ~ 1,
    # Keep main data in other circumstances
    hh_ssn == lead(hh_ssn, 1) & h2b == lead(h2b) & h2a == lead(h2a, 1) &
      !is.na(lead(hh_lname, 1)) & eop_source == "eop" ~ 1,
    TRUE ~ 0
  )) %>%
  # Remove records that should be dropped
  filter(drop == 0) %>%
  select(-drop)

  
# Transfer household data from row immediately prior to EOP row
kcha <- kcha %>%
  mutate_at(vars(h1a, h2h, contains("h3"), contains("h5"), contains("h19"),
                 contains("h20"), contains("h21"), hh_lname, hh_fname, 
                 hh_mname, contains("hh_inc"), kcha_source, propertytype,
                 developmentname, spec_vouch),
            funs(ifelse(hh_ssn == lag(hh_ssn, 1) & eop_source == "eop" &
                          !is.na(eop_source), 
                        lag(., 1), .))
            )

rm(list = ls(pattern = "kcha_eop"))


#### RESHAPE DATA ####
# The data initially has household members in wide format
# Need to reshape to give one hhold member per row but retain head of hhold info
# Make temporary record of the row each new row came from when reshaped
kcha <- kcha %>%
  arrange(hh_ssn, h2b, h2a) %>%
  mutate(hh_id_temp = row_number())

# Rename some variables to have consistent format
names <- as.data.frame(names(kcha), stringsAsFactors = FALSE)
colnames(names) <- c("varname")
names <- names %>%
  mutate(
    varname = ifelse(str_detect(varname, "h3k[:digit:]*a"), str_replace(varname, "h3k", "h3k1"), varname),
    varname = ifelse(str_detect(varname, "h3k[:digit:]*b"), str_replace(varname, "h3k", "h3k2"), varname),
    varname = ifelse(str_detect(varname, "h3k[:digit:]*c"), str_replace(varname, "h3k", "h3k3"), varname),
    varname = ifelse(str_detect(varname, "h3k[:digit:]*d"), str_replace(varname, "h3k", "h3k4"), varname),
    varname = ifelse(str_detect(varname, "h3k[:digit:]*e"), str_replace(varname, "h3k", "h3k5"), varname),
    varname = ifelse(str_detect(varname, "h19a[0-9]{1}[a]+"), str_replace(varname, "h19a", "h19a10"), varname),
    varname = ifelse(str_detect(varname, "h19a[0-9]{2}[a]+"), str_replace(varname, "h19a", "h19a1"), varname),
    varname = ifelse(str_detect(varname, "h19a[0-9]{1}[b]+"), str_replace(varname, "h19a", "h19a20"), varname),
    varname = ifelse(str_detect(varname, "h19a[0-9]{2}[b]+"), str_replace(varname, "h19a", "h19a2"), varname),
    # Trim the final letter
    varname = ifelse(str_detect(varname, "h3k") | str_detect(varname, "h19a"), str_sub(varname, 1, -2), varname)
  )
colnames(kcha) <- names[,1]

# Remove temporary data
rm(names)


# Using an apply process over reshape due to memory issues
# Make function to save space
reshape_f <- function(df, min = 1, max = 14) {
  
  # Set up number of people in the data, ensure leading zero with sprintf
  digits <- max(2, nchar(max))
  people <- as.list(sprintf(paste0("%0", digits, ".0f"), min:max))
  
  df_inner <- df
  
  #Make function to get a person's info
  person_f <- function(df_inner, x) {
    print(paste0("Working on person ", x))
    sublong <- df_inner %>%
      select(h1a, h2a, h2b, h2c:h2h,
             subsidy_id,
             vouch_num,
             certificationid,
             h3a = paste0("h3a", x), # not reliable so make own member number
             h3b = paste0("h3b", x),
             h3c = paste0("h3c", x),
             h3d = paste0("h3d", x),
             h3e = paste0("h3e", x),
             h3g = paste0("h3g", x),
             h3h = paste0("h3h", x),
             h3i = paste0("h3i", x),
             h3j = paste0("h3j", x),
             h3k1 = paste0("h3k1", x),
             h3k2 = paste0("h3k2", x),
             h3k3 = paste0("h3k3", x),
             h3k4 = paste0("h3k4", x),
             h3k5 = paste0("h3k5", x),
             h3m = paste0("h3m", x),
             h3n = paste0("h3n", x),
             h19a1 = paste0("h19a1", x),
             h19a2 = paste0("h19a2", x),
             h19b = paste0("h19b", x),
             h19d = paste0("h19d", x),
             h19f = paste0("h19f", x),
             h19h, h19k,
             program_type,
             spec_vouch,
             h5a1a:h5a5,
             h5d, h5e, h5f, h5g, h5j, h5k,
             developmentname,
             h20a, h20b, h20c, h20d, h20e,
             h21a, h21b, h21d, h21e, h21f, h21i, h21j, 
             h21k, h21m, h21n, h21p, h21q,
             box04b, box04c,
             hh_inc_fixed: hh_inc_adj_vary,
             householdid,
             hh_lname:hh_dob,
             kcha_source,
             eop_source,
             hh_id_temp) %>%
      mutate(mbr_num = x)
    
    return(sublong)
  }
  
  # Run function over everyone
  templist <- lapply(people, person_f, df = df_inner)
  
  # Turn back into a data frame
  long_full <- as.data.frame(rbindlist(templist))
  return(long_full)
}

kcha_long <- reshape_f(df = kcha, min = 1, max = 14)


# Get rid of white space (focus on variables used to filter first to save memory)
kcha_long <- kcha_long %>%
  mutate_at(vars(h3a, h3b, h3c, h3n, starts_with("h5a")), list(~str_trim(.)))


# Make mbr_num (both created and original data versions) and unit_zip a number
kcha_long <- mutate_at(kcha_long, vars(h3a, mbr_num, h19a2, h5a5), 
                       list(~as.numeric(.)))


#### REORGANIZE INCOME FIELDS ####
# Need to sum the income for a given time point to reduce duplicated rows
# No asset data provided
# Run this now because the number of rows with data exceed the number of 
# people in the household sometimes and data would be lost if run later
inc_temp <- kcha_long %>% select(hh_id_temp, mbr_num, contains("h19"))

# Filter out rows with no income or member number information
inc_temp <- inc_temp %>% filter(!(is.na(h19a2) | is.na(h19f)))

# Summarise fixed and varying income by person
inc_fixed <- inc_temp %>%
  filter(h19b %in% c("G", "P", "S", "SS")) %>%
  group_by(hh_id_temp, h19a2) %>%
  summarise(inc_fixed = sum(h19d, na.rm = T),
            inc_adj_fixed = sum(h19f, na.rm = T)) %>%
  ungroup()

inc_vary <- inc_temp %>%
  filter(!h19b %in% c("G", "P", "S", "SS")) %>%
  group_by(hh_id_temp, h19a2) %>%
  summarise(inc_vary = sum(h19d, na.rm = T),
            inc_adj_vary = sum(h19f, na.rm = T)) %>%
  ungroup()

# Join back to main data and make combined income data
# Join to KCHA-given mbr num (h3a) rather than income field mbr num (h19a2)
# because the latter can be propagated across multiple household members
kcha_long <- left_join(kcha_long, inc_fixed, by = c("hh_id_temp", "h3a" = "h19a2"))
kcha_long <- left_join(kcha_long, inc_vary, by = c("hh_id_temp", "h3a" = "h19a2"))

kcha_long <- kcha_long %>%
  mutate(inc = rowSums(select(., inc_fixed, inc_vary), na.rm = T),
         inc_adj = rowSums(select(., inc_adj_fixed, inc_adj_vary), na.rm = T),
         hh_inc = rowSums(select(., hh_inc_fixed, hh_inc_vary), na.rm = T),
         hh_inc_adj = rowSums(select(., hh_inc_adj_fixed, hh_inc_adj_vary), na.rm = T),
         # No asset income data so total adjusted income is the same as income source
         hh_inc_tot_adj = hh_inc_adj)

# Remove specific incomes and codes and deduplicate
kcha_long <- kcha_long %>%
  select(-h19a1, -h19a2, -h19b, -h19d, -h19f, -h19h, -h19k)

# Remove temporary data
rm(list = ls(pattern = "inc_"))

#### END INCOME REORGANIZATION ####


# Get rid of empty rows (note: some rows have a SSN but no name or address, 
# others have an address but no name or other details)
kcha_long <- kcha_long %>%
  filter(!((is.na(h3a) |  h3a == 0) & h3b == "" & h3c == "" & h3n == ""))
gc()

# Add number of household members at that time
# Use summarise and join because mutate crashes things
hh_size <- kcha_long %>%
  group_by(hh_id_temp) %>%
  summarise(hh_size = n_distinct(mbr_num)) %>%
  ungroup()

kcha_long <- left_join(kcha_long, hh_size, by = "hh_id_temp")


#### RENAME VARIABLES ####
kcha_long <- setnames(kcha_long, fields$common_name[match(names(kcha_long), fields$kcha_modified)])


#### CLEAN UP DATA AND MAKE VARIABLES FOR MERGING ####
kcha_long <- kcha_long %>%
  mutate(
    major_prog = ifelse(prog_type == "PH", "PH", "HCV"),
    property_id = as.numeric(ifelse(str_detect(subsidy_id, "^[0-9]-") == T, str_sub(subsidy_id, 3, 5), NA))
  )


#### JOIN WITH PROPERTY LISTS ####
### Public housing
# Bring in data and rename variables
kcha_portfolio_codes <- read.xlsx(file.path(kcha_path, kcha_portfolio_codes_fn))
kcha_portfolio_codes <- setnames(kcha_portfolio_codes, 
                                 fields$common_name[match(names(kcha_portfolio_codes), 
                                                    fields$kcha_modified)])

# Join and clean up duplicate variables
kcha_long <- left_join(kcha_long, kcha_portfolio_codes, by = c("property_id"))
kcha_long <- kcha_long %>% 
  # There shouldn't be any rows with values in both property_name columns 
  # (checked and seems to be the case)
  mutate(property_name = case_when(
    is.na(property_name.y) & !is.na(property_name.x) & 
      property_name.x != "" ~ property_name.x,
    !is.na(property_name.y) ~ property_name.y,
    TRUE ~ NA_character_
    )) %>%
  select(-property_name.x, -property_name.y)



### HCV (currently being done after join with SHA and address cleanup)
# Bring in data and rename variables
# kcha_dev_adds <- read.csv(file = paste0(path, "/Original data/Development Addresses_received_2017-07-21.csv"), stringsAsFactors = FALSE)
# kcha_dev_adds <- setnames(kcha_dev_adds, fields$common_name[match(names(kcha_dev_adds), fields$kcha_modified)])
# 
# # Drop spare rows and deduplicate
# # Note that only three rows (plus rows used for merging) are being kept for now.
# # If all rows are used later, deduplication is still required.
# kcha_dev_adds <- kcha_dev_adds %>% select(dev_add_apt, dev_city, property_name, portfolio, property_type)
# kcha_dev_adds <- kcha_dev_adds %>% distinct()
# 
# kcha_long <- left_join(kcha_long, kcha_dev_adds, by = c("dev_add_apt", "dev_city"))


#### Final clean up of formats in preparation for joining ####
kcha_long <- kcha_long %>%
  mutate_at(vars(admit_date, dob, hh_dob),
            funs(as.Date(., origin = "1970-01-01")))

kcha_long <- yesno_f(kcha_long, correction, ph_rent_ceiling, disability,
                     access_unit, access_req, tb_rent_ceiling, portability,
                     r_white, r_black, r_aian, r_asian, r_nhpi)
kcha_long <- char_f(kcha_long, property_id)

kcha_long <- kcha_long %>% distinct()


if (sql == TRUE) {
##### WRITE RESHAPED DATA TO SQL #####
# May need to delete table first if data structure and columns have changed
  
  tbl_id_meta <- DBI::Id(schema = "stage", table = "pha_kcha")
  
  if (dbExistsTable(db_apde51, tbl_id_meta)) {
    dbRemoveTable(db_apde51, tbl_id_meta)
  }
  
  dbWriteTable(db_apde51, tbl_id_meta, kcha_long, overwrite = T)
  rm(tbl_id_meta)
}

##### Remove temporary files #####
#### Remove temporary files ####
rm(list = ls(pattern = "kcha_2004"))
rm(list = ls(pattern = "kcha_201[6|7|8|9]"))
rm(list = ls(pattern = "panel_"))
rm(list = ls(pattern = "p[1|2|3]_"))
rm(list = c("fields", "reshape_f", "kcha_path"))
rm(hh_size)
rm(list = c("kcha_portfolio_codes", "kcha_portfolio_codes_fn"))
rm(kcha_eop_fn)
rm(kcha)
rm(kcha_long)
rm(METADATA)
rm(set_data_envr)
rm(add_2018)
rm(sql)
gc()
