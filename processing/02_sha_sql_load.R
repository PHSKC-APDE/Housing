###############################################################################
# OVERVIEW:
# Code to create a cleaned person table from the combined 
# King County Housing Authority and Seattle Housing Authority data sets
# Aim is to have a single row per contiguous time in a house per person
#
# STEPS:
# 01 - Process raw KCHA data and load to SQL database
# 02 - Process raw SHA data and load to SQL database ### (THIS CODE) ###
# 03 - Bring in individual PHA datasets and combine into a single file
# 04 - Deduplicate data and tidy up via matching process
# 05 - Recode race and other demographics
# 06 - Clean up addresses
# 06a - Geocode addresses
# 07 - Consolidate data rows
# 08 - Add in final data elements and set up analyses
# 09 - Join with Medicaid eligibility data
# 10 - Set up joint housing/Medicaid analyses
#
# Alastair Matheson (PHSKC-APDE)
# alastair.matheson@kingcounty.gov
# 2017-05-17, last updated 2018-04-30
# 
###############################################################################

#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, scipen = 999)

require(housing) # contains many useful functions for cleaning
require(odbc) # Used to connect to SQL server
require(openxlsx) # Used to import/export Excel files
require(data.table) # Used to read in csv files more efficiently
require(tidyverse) # Used to manipulate data
require(RJSONIO)
require(RCurl)
#devtools::install_github("hadley/dplyr")
#require(dplyr)

script <- RCurl::getURL("https://raw.githubusercontent.com/PHSKC-APDE/Housing/master/processing/metadata/set_data_env.r")
eval(parse(text = script))

METADATA = RJSONIO::fromJSON(paste0(housing_source_dir,"metadata/metadata.json"))
set_data_envr(METADATA, "sha_data")

if(sql == TRUE) {
  db.apde51 <- dbConnect(odbc(), "PH_APDEStore51")
}


#### Bring in data ####
sha3a_new <- fread(file = 
                     file.path(sha_path,
                               sha3a_new_fn), 
                   na.strings = c("NA", "", "NULL", "N/A"),
                   stringsAsFactors = F)
sha3b_new <- fread(file = file.path(sha_path,
                                       sha3b_new_fn), 
                   na.strings = c("NA", "", "NULL", "N/A"), 
                   stringsAsFactors = F)
sha5a_new <- fread(file = file.path(sha_path,
                                       sha5a_new_fn), 
                   na.strings = c("NA", "", "NULL", "N/A"), 
                   stringsAsFactors = F)
sha5b_new <- fread(file = file.path(sha_path,
                                       sha5b_new_fn),
                   na.strings = c("NA", "", "NULL", "N/A"), 
                   stringsAsFactors = F)


# Bring in suffix corrected SHA data
sha1a <- fread(file = file.path(sha_path,
                                   sha1a_fn), 
               na.strings = c("NA", "", "NULL", "N/A"), 
               stringsAsFactors = F)
sha1b <- fread(file = file.path(sha_path,
                                   sha1b_fn),
               na.strings = c("NA", "", "NULL", "N/A"), 
               stringsAsFactors = F)
sha1c <- fread(file = file.path(sha_path,
                                   sha1c_fn),
               na.strings = c("NA", "", "NULL", "N/A"), stringsAsFactors = F)
sha2a <- fread(file = file.path(sha_path,
                                   sha2a_fn),
               na.strings = c("NA", "", "NULL", "N/A"), 
               stringsAsFactors = F)
sha2b <- fread(file = file.path(sha_path,
                                   sha2b_fn),
               na.strings = c("NA", "", "NULL", "N/A"), 
               stringsAsFactors = F)
sha2c <- fread(file = file.path(sha_path,
                                   sha2c_fn),
               na.strings = c("NA", "", "NULL", "N/A"), 
               stringsAsFactors = F)
sha4a <- fread(file = file.path(sha_path,
                                   sha4a_fn),
               na.strings = c("NA", "", "NULL", "N/A"), 
               stringsAsFactors = F)

if (UW == TRUE) {
  sha6a_new <- fread(file = file.path(sha_path, sha6a_new_fn),
                     na.strings = c("NA", "", "NULL", "N/A"),
                     stringsAsFactors = F)
  
  sha6b_new <- fread(file = file.path(sha_path, sha6b_new_fn),
                     na.strings = c("NA", "", "NULL", "N/A"),
                     stringsAsFactors = F)
}

# Bring in voucher data

### UW DATA 
if (UW == TRUE){
sha_vouch_type <- read.xlsx(file.path(sha_path, sha_vouch_type_fn))
}
###
sha_prog_codes <- read.xlsx(file.path(
  sha_path, sha_prog_codes_fn), 2)

# Bring in portfolio codes
sha_portfolio_codes  <- read.xlsx(file.path(
  sha_path, sha_prog_codes_fn), 1)


#### PREP DATA SETS FOR JOINING ####

### First deduplicate data to avoid extra rows being made when joined
# Make list of data frames to deduplicate
if (UW == TRUE) {
  dfs <- list(sha1a = sha1a, sha1b = sha1b, sha1c = sha1c, 
              sha2a = sha2a, sha2b = sha2b, sha2c = sha2c, 
              sha3a_new = sha3a_new, sha3b_new = sha3b_new, 
              sha4a = sha4a, 
              sha5a_new = sha5a_new, sha5b_new = sha5b_new,
              sha6a_new = sha6a_new, sha6b_new = sha6b_new,
              sha_prog_codes = sha_prog_codes, 
              sha_portfolio_codes = sha_portfolio_codes)
} else {
  dfs <- list(sha1a = sha1a, sha1b = sha1b, sha1c = sha1c, 
              sha2a = sha2a, sha2b = sha2b, sha2c = sha2c, 
              sha3a_new = sha3a_new, sha3b_new = sha3b_new, 
              sha4a = sha4a, 
              sha5a_new = sha5a_new, sha5b_new = sha5b_new,
              sha_prog_codes = sha_prog_codes, 
              sha_portfolio_codes = sha_portfolio_codes)
}

# Deduplicate data
df_dedups <- lapply(dfs, function(data) {
  data <- data %>% distinct()
  return(data)
  })

# Bring back data frames from list
list2env(df_dedups, .GlobalEnv)
rm(df_dedups)


### Get field names to match
# Bring in variable name mapping table
fields <- read.csv(text = RCurl::getURL("https://raw.githubusercontent.com/PHSKC-APDE/Housing/master/processing/Field%20name%20mapping.csv"), 
         header = TRUE, stringsAsFactors = FALSE)
###
### UW DATA field names mappings are different or new ones don't have mappings for the voucher data?
if (UW == TRUE) {
  fields_uw <- read.xlsx(file.path(sha_path, field_name_mapping_fn), 1)
  
  fields_uw <- fields_uw %>%
    mutate_at(vars(SHA_old:SHA_new_ph), funs(gsub("[[:punct:]]|[[:space:]]","",.))) %>%
    mutate_at(vars(SHA_old:SHA_new_ph), funs(tolower(.)))
} 

# Get rid of spaces, characters, and capitals in existing names
# Makes it easier to accommodate changes in names provided by SHA
df_rename <- lapply(dfs, function(data) {
  colnames(data) <- str_replace_all(colnames(data), "[:punct:]|[:space:]", "")
  colnames(data) <- tolower(colnames(data))
  return(data)
})

# Bring back data frames from list
list2env(df_rename, .GlobalEnv)
rm(dfs)
rm(df_rename)
gc()

# Apply new names to columns
sha1a <- setnames(sha1a, fields$common_name[match(names(sha1a), fields$sha_old)])
sha1b <- setnames(sha1b, fields$common_name[match(names(sha1b), fields$sha_old)])
sha1c <- setnames(sha1c, fields$common_name[match(names(sha1c), fields$sha_old)])
sha2a <- setnames(sha2a, fields$common_name[match(names(sha2a), fields$sha_old)])
sha2b <- setnames(sha2b, fields$common_name[match(names(sha2b), fields$sha_old)])
sha2c <- setnames(sha2c, fields$common_name[match(names(sha2c), fields$sha_old)])
sha3a_new <- setnames(sha3a_new, fields$common_name[match(names(sha3a_new), 
                                                    fields$sha_new_ph)])

if (UW == TRUE) {
# Issue with the hh_names, they are reapeted accross both HH and housemember names same for ssn
  colnames(sha3a_new)[10] <- "hh_lname"
  colnames(sha3a_new)[11] <- "hh_fname"
  colnames(sha3a_new)[12] <- "hh_mname"
}

sha3b_new <- setnames(sha3b_new, fields$common_name[match(names(sha3b_new), 
                                                    fields$sha_new_ph)])
sha_portfolio_codes <- setnames(sha_portfolio_codes, 
                                fields$common_name[match(names(sha_portfolio_codes), 
                                                   fields$sha_prog_port_codes)])

sha4a <- setnames(sha4a, fields$common_name[match(names(sha4a), fields$sha_old)])
sha5a_new <- setnames(sha5a_new, fields$common_name[match(names(sha5a_new), 
                                                    fields$sha_new_hcv)])
sha5b_new <- setnames(sha5b_new, fields$common_name[match(names(sha5b_new), 
                                                    fields$sha_new_hcv)])

if (UW == TRUE) {
  sha6a_new <- setnames(sha6a_new, fields$common_name[match(names(sha6a_new), fields$sha_new_ph)])

  # Issue with the hh_names, they are reapeted accross both HH and housemember names same for ssn
  colnames(sha6a_new)[10] <- "hh_lname"
  colnames(sha6a_new)[11] <- "hh_fname"
  colnames(sha6a_new)[12] <- "hh_mname"

  sha6b_new <- setnames(sha6b_new, fields$common_name[match(names(sha6b_new), 
                                                    fields$sha_new_ph)])
  sha6a_new <- sha6a_new %>%
    mutate(property_id = as.character(property_id),
           act_type = as.numeric(ifelse(act_type == "E", 3, act_type)),
           mbr_num = as.numeric(ifelse(mbr_num == "NULL", NA, mbr_num)),
           r_hisp = as.numeric(ifelse(r_hisp == "NULL", NA, r_hisp))
  )
}

sha_prog_codes <- setnames(sha_prog_codes, 
                           fields$common_name[match(names(sha_prog_codes), 
                                              fields$sha_prog_port_codes)])

# UW DATA
if (UW == TRUE){
sha_vouch_type <- data.table::setnames(sha_vouch_type, fields_uw$PHSKC[match(names(sha_vouch_type), 
                                        fields_uw$SHA_new_hcv)])
sha_vouch_type <- sha_vouch_type %>%
  mutate(act_type = car::recode(act_type, c("'Annual HQS Inspection Only' = 13; 'Annual Reexamination' = 2; 'Annual Reexamination Searching' = 9;
                                       'End Participation' = 6; 'Expiration of Voucher' = 11; 'FSS/WtW Addendum Only' = 8;
                                       'Historical Adjustment' = 14; 'Interim Reexamination' = 3; 'Issuance of Voucher' = 10;
                                       'New Admission' = 1; 'Other Change of Unit' = 7; 'Port-Out Update (Not Submitted To MTCS)' = 16;
                                       'Portability Move-in' = 4; 'Portability Move-out' = 5; 'Portablity Move-out' = 5; 'Void' = 15;
                                       else = NA")),
          act_date=as.Date(act_date, origin = "1899-12-30")) # 1899 is needed because of an excel date bug
}
###

#### INCOME SECTIONS ####
# Need to do the following:
# 1) Tidy up and recode some fields
# 2) Identify people with income from a fixed source
# 3) Summarize income/assets for a given time point to reduce duplicated rows

if (UW == TRUE) {
### Function to do this across various income/asset data frames
inc_clean_f <- function(df) {
  ### Print message to show code is working
  message("Working on list item")
  
  ### Tidy up income fields and recode
  if("inc_code" %in% names(df)) {
    df <- df %>%
      mutate(inc_code = 
               car::recode(inc_code, 
                           "'Annual imputed welfare income' = 'IW'; 
                           'Child Support' = 'C';'Federal Wage' = 'F'; 
                           'General Assistance' = 'G'; 
                           'Indian Trust/Per Capita' = 'I'; 
                           'Medical reimbursement' = 'E'; 'Military Pay' = 'M'; 
                           'MTW Income' = 'X'; 'NULL' = NA; 
                           'Other NonWage Sources' = 'N'; 'Other Wage' = 'W'; 
                           'Own Business' = 'B'; 'Pension' = 'P'; 
                           'PHA Wage' = 'HA'; 'Social Security' = 'SS'; 
                           'SSI' = 'S'; 'TANF (formerly AFDC)' = 'T';
                           'Unemployment Benefits' = 'U'; '' = NA"),
             inc_fixed_temp = ifelse(
               inc_code %in% c("P", "S", "SS"), 1, 0))
  }
  
  ### Summarize income and assets differently depending on data format
  # Tested out summarise instead of mutate in first part. No faster.
  # Still need ways to optimize this code
  # Removed 'increment' not in UW data
  if ("mbr_id" %in% names(df)) {
    df_inc <- df %>%
      distinct(cert_id, mbr_id, inc_code, 
               inc, inc_excl, inc_adj, inc_fixed_temp) %>%
    group_by(cert_id, mbr_id) %>%
      summarise(
        inc = sum(inc, na.rm = T), 
        inc_excl = sum(inc_excl, na.rm = T),
        inc_adj = sum(inc_adj, na.rm = T),
        inc_fixed = min(inc_fixed_temp, na.rm = T)) %>%
      group_by(cert_id) %>%
      mutate(
        hh_inc = sum(inc, na.rm = T), 
        hh_inc_excl = sum(inc_excl, na.rm = T),
        hh_inc_adj = sum(inc_adj, na.rm = T)) %>%
      ungroup()

    df_ass <- df %>%
      distinct(cert_id, mbr_id, asset_type, asset_val, asset_inc) %>%
      group_by(cert_id, mbr_id) %>%
      summarise(
        asset_val = sum(as.numeric(asset_val), na.rm = T), 
        asset_inc = sum(asset_inc, na.rm = T)) %>%
      group_by(cert_id) %>%
      mutate(
        hh_asset_val = sum(as.numeric(asset_val), na.rm = T), 
        hh_asset_inc = sum(asset_inc, na.rm = T)) %>%
      ungroup()
    
    df <- left_join(df_inc, df_ass, by = c("cert_id", "mbr_id"))
    
  } else if ("incasset_id" %in% names(df) & "inc_mbr_num" %in% names(df)) {
    if ("inc" %in% names(df) & !("asset_val" %in% names(df))) {
      df <- df %>%
        group_by(incasset_id, inc_mbr_num) %>%
        mutate(
          inc = sum(inc, na.rm = T), 
          inc_excl = sum(inc_excl, na.rm = T),
          inc_adj = sum(inc_adj, na.rm = T),
          inc_fixed = min(inc_fixed_temp, na.rm = T)) %>%
        ungroup() %>%
        select(-inc_fixed_temp, -inc_code) %>%
        distinct() %>%
        group_by(incasset_id) %>%
        mutate(
          hh_inc = sum(inc, na.rm = T), 
          hh_inc_excl = sum(inc_excl, na.rm = T),
          hh_inc_adj = sum(inc_adj, na.rm = T)) %>%
        ungroup()
    }
    if (!("inc" %in% names(df)) & "asset_val" %in% names(df)) {
      df <- df %>%
        group_by(incasset_id, inc_mbr_num) %>%
        mutate(
          asset_val = sum(as.numeric(asset_val), na.rm = T), 
          asset_inc = sum(asset_inc, na.rm = T)
        ) %>%
        ungroup() %>%
        select(-asset_type) %>%
        distinct() %>%
        group_by(incasset_id) %>%
        mutate(
          hh_asset_val = sum(as.numeric(asset_val), na.rm = T), 
          hh_asset_inc = sum(asset_inc, na.rm = T)
        ) %>%
          ungroup()
    }
    if ("inc" %in% names(df) & "asset_val" %in% names(df)) {
      df <- df %>%
        group_by(incasset_id, inc_mbr_num) %>%
        mutate(
          inc = sum(inc, na.rm = T), 
          inc_excl = sum(inc_excl, na.rm = T),
          inc_adj = sum(inc_adj, na.rm = T),
          inc_fixed = min(inc_fixed_temp, na.rm = T),
          asset_val = sum(as.numeric(asset_val), na.rm = T), 
          asset_inc = sum(asset_inc, na.rm = T)
        ) %>%
        ungroup() %>%
        select(-inc_fixed_temp, -inc_code, -asset_type) %>%
        distinct() %>%
        group_by(incasset_id) %>%
        mutate(
          hh_inc = sum(inc, na.rm = T), 
          hh_inc_excl = sum(inc_excl, na.rm = T),
          hh_inc_adj = sum(inc_adj, na.rm = T),
          hh_asset_val = sum(as.numeric(asset_val), na.rm = T), 
          hh_asset_inc = sum(asset_inc, na.rm = T)
        ) %>%
        ungroup()
    }
  } else if ("incasset_id" %in% names(df) & !("inc_mbr_num" %in% names(df))) {
    df <- df %>%
      group_by(incasset_id) %>%
      mutate(hh_asset_val = sum(as.numeric(asset_val), na.rm = T),
             hh_asset_inc = sum(asset_inc, na.rm = T)) %>%
      ungroup() %>%
      select(-asset_type, -asset_val, -asset_inc) %>%
      distinct()
  } else {
    stop("No valid grouping variables")
  }
  
  return(df)
  
}
} else {
  ### Function to do this across various income/asset data frames
inc_clean_f <- function(df) {
  ### Print message to show code is working
  message("Working on list item")
  
  ### Tidy up income fields and recode
  if("inc_code" %in% names(df)) {
    df <- df %>%
      mutate(inc_code = 
               car::recode(inc_code, 
                           "'Annual imputed welfare income' = 'IW'; 
                           'Child Support' = 'C';'Federal Wage' = 'F'; 
                           'General Assistance' = 'G'; 
                           'Indian Trust/Per Capita' = 'I'; 
                           'Medical reimbursement' = 'E'; 'Military Pay' = 'M'; 
                           'MTW Income' = 'X'; 'NULL' = NA; 
                           'Other NonWage Sources' = 'N'; 'Other Wage' = 'W'; 
                           'Own Business' = 'B'; 'Pension' = 'P'; 
                           'PHA Wage' = 'HA'; 'Social Security' = 'SS'; 
                           'SSI' = 'S'; 'TANF (formerly AFDC)' = 'T';
                           'Unemployment Benefits' = 'U'; '' = NA"),
             inc_fixed_temp = ifelse(
               inc_code %in% c("P", "S", "SS"), 1, 0))
  }
  
  ### Summarize income and assets differently depending on data format
  # Tested out summarise instead of mutate in first part. No faster.
  # Still need ways to optimize this code
  if ("mbr_id" %in% names(df)) {
    df_inc <- df %>%
      distinct(cert_id, mbr_id, increment, inc_code, 
               inc, inc_excl, inc_adj, inc_fixed_temp) %>%
    group_by(cert_id, mbr_id, increment) %>%
      summarise(
        inc = sum(inc, na.rm = T), 
        inc_excl = sum(inc_excl, na.rm = T),
        inc_adj = sum(inc_adj, na.rm = T),
        inc_fixed = min(inc_fixed_temp, na.rm = T)) %>%
      group_by(cert_id) %>%
      mutate(
        hh_inc = sum(inc, na.rm = T), 
        hh_inc_excl = sum(inc_excl, na.rm = T),
        hh_inc_adj = sum(inc_adj, na.rm = T)) %>%
      ungroup()

    df_ass <- df %>%
      distinct(cert_id, mbr_id, increment, asset_type, asset_val, asset_inc) %>%
      group_by(cert_id, mbr_id, increment) %>%
      summarise(
        asset_val = sum(asset_val, na.rm = T), 
        asset_inc = sum(asset_inc, na.rm = T)) %>%
      group_by(cert_id) %>%
      mutate(
        hh_asset_val = sum(asset_val, na.rm = T), 
        hh_asset_inc = sum(asset_inc, na.rm = T)) %>%
      ungroup()
    
    df <- left_join(df_inc, df_ass, by = c("cert_id", "mbr_id", "increment"))
    
  } else if ("incasset_id" %in% names(df) & "inc_mbr_num" %in% names(df)) {
    if ("inc" %in% names(df) & !("asset_val" %in% names(df))) {
      df <- df %>%
        group_by(incasset_id, inc_mbr_num) %>%
        mutate(
          inc = sum(inc, na.rm = T), 
          inc_excl = sum(inc_excl, na.rm = T),
          inc_adj = sum(inc_adj, na.rm = T),
          inc_fixed = min(inc_fixed_temp, na.rm = T)) %>%
        ungroup() %>%
        select(-inc_fixed_temp, -inc_code) %>%
        distinct() %>%
        group_by(incasset_id) %>%
        mutate(
          hh_inc = sum(inc, na.rm = T), 
          hh_inc_excl = sum(inc_excl, na.rm = T),
          hh_inc_adj = sum(inc_adj, na.rm = T)) %>%
        ungroup()
    }
    if (!("inc" %in% names(df)) & "asset_val" %in% names(df)) {
      df <- df %>%
        group_by(incasset_id, inc_mbr_num) %>%
        mutate(
          asset_val = sum(asset_val, na.rm = T), 
          asset_inc = sum(asset_inc, na.rm = T)
        ) %>%
        ungroup() %>%
        select(-asset_type) %>%
        distinct() %>%
        group_by(incasset_id) %>%
        mutate(
          hh_asset_val = sum(asset_val, na.rm = T), 
          hh_asset_inc = sum(asset_inc, na.rm = T)
        ) %>%
          ungroup()
    }
    if ("inc" %in% names(df) & "asset_val" %in% names(df)) {
      df <- df %>%
        group_by(incasset_id, inc_mbr_num) %>%
        mutate(
          inc = sum(inc, na.rm = T), 
          inc_excl = sum(inc_excl, na.rm = T),
          inc_adj = sum(inc_adj, na.rm = T),
          inc_fixed = min(inc_fixed_temp, na.rm = T),
          asset_val = sum(asset_val, na.rm = T), 
          asset_inc = sum(asset_inc, na.rm = T)
        ) %>%
        ungroup() %>%
        select(-inc_fixed_temp, -inc_code, -asset_type) %>%
        distinct() %>%
        group_by(incasset_id) %>%
        mutate(
          hh_inc = sum(inc, na.rm = T), 
          hh_inc_excl = sum(inc_excl, na.rm = T),
          hh_inc_adj = sum(inc_adj, na.rm = T),
          hh_asset_val = sum(asset_val, na.rm = T), 
          hh_asset_inc = sum(asset_inc, na.rm = T)
        ) %>%
        ungroup()
    }
  } else if ("incasset_id" %in% names(df) & !("inc_mbr_num" %in% names(df))) {
    df <- df %>%
      group_by(incasset_id) %>%
      mutate(hh_asset_val = sum(asset_val, na.rm = T),
             hh_asset_inc = sum(asset_inc, na.rm = T)) %>%
      ungroup() %>%
      select(-asset_type, -asset_val, -asset_inc) %>%
      distinct()
  } else {
    stop("No valid grouping variables")
  }
  
  return(df)
  
}
}

# Make list of data frames with income or asset 
if (UW == TRUE) {
  dfs_inc <- list(sha1b = sha1b, sha1c = sha1c, sha2b = sha2b, sha2c = sha2c, 
                sha3b_new = sha3b_new, sha5b_new = sha5b_new, sha6b_new = sha6b_new)
} else {  
  dfs_inc <- list(sha1b = sha1b, sha1c = sha1c, sha2b = sha2b, sha2c = sha2c, 
                  sha3b_new = sha3b_new, sha5b_new = sha5b_new)
}

# Apply function to all relevant data frames (takes a few minutes to run)
income_assets <- lapply(dfs_inc, inc_clean_f)

 # Bring back data frames from list
list2env(income_assets, .GlobalEnv)
rm(dfs_inc)
rm(income_assets)


#### JOIN PUBLIC HOUSING FILES ####
# Clean up mismatching variables
sha2a <- yesno_f(sha2a, ph_rent_ceiling)
sha2a <- mutate(sha2a, fhh_ssn = as.character(fhh_ssn))
# Fix the variable mappings for this: SSN and fname, mname, lname,  
sha3a_new <- sha3a_new %>%
  mutate(property_id = as.character(property_id),
         act_type = as.numeric(ifelse(act_type == "E", 3, act_type)),
         mbr_num = as.numeric(ifelse(mbr_num == "NULL", NA, mbr_num)),
         r_hisp = as.numeric(ifelse(r_hisp == "NULL", NA, r_hisp))
  )

# UW DATA CODE
# Add suffix columns to sha1a
if(UW == TRUE) {
  sha1a.fix <- sha1a %>%
    filter(v56!="") %>%
    mutate(v57="",hh_lnamesuf=hh_fname, lnamesuf=mname) %>%
    select(-hh_fname,-mname)
    
  names(sha1a.fix) <- names(sha1a)
  names(sha1a.fix)[57] = "v57"
  
  sha1a.fix <- sha1a.fix %>%
    select(incasset_id:hh_lname, hh_lnamesuf = 56, hh_fname:lname, lnamesuf = 57, fname:55)

  sha1a.good <- sha1a %>% filter(is.na(v56)) %>%
                  mutate(hh_lnamesuf="", lnamesuf="") %>%
                  select(incasset_id:hh_lname,hh_lnamesuf,hh_fname:lname,lnamesuf,
                    fname:fhh_ssn, -v56)

  sha1a <- rbind(sha1a.good,sha1a.fix) %>%
    mutate(mbr_num=as.integer(mbr_num))

  # Add suffix columns sha2a
  sha2a.good <- sha2a %>%
    filter(is.na(v57)) %>%
    rename(hh_lnamesuf=v57, lnamesuf=v58) %>%
    mutate(lnamesuf="") %>%
    select(incasset_id:hh_lname,hh_lnamesuf,hh_fname:lname, 
      lnamesuf,fname:fhh_ssn)

  sha2a.fix1 <-
    sha2a %>%
    filter(v57!="", is.na(v58)) %>%
    mutate(lnamesuf=fname, hh_lnamesuf="") %>%
    select(incasset_id:hh_lname, hh_lnamesuf, hh_fname:lname, 
      lnamesuf, mname:v57)

  names(sha2a.fix1) <- names(sha2a.good)

  sha2a.fix2 <-
    sha2a %>% filter(!is.na(v58)) %>%
      mutate(lnamesuf=mname, hh_lnamesuf=hh_fname) %>%
      select(incasset_id:hh_lname, hh_lnamesuf, hh_mname:fname, 
        lnamesuf, dob:v58)

  names(sha2a.fix2) <- names(sha2a.good)

  sha2a <- rbind(sha2a.good, sha2a.fix1, sha2a.fix2) %>%
    mutate(mbr_num=as.integer(mbr_num))
}
###

# Join household, income, and asset tables
sha1 <- left_join(sha1a, sha1b, by = c("incasset_id", "mbr_num" = "inc_mbr_num"))
sha1 <- left_join(sha1, sha1c, by = c("incasset_id"))

sha2 <- left_join(sha2a, sha2b, by = c("incasset_id", "mbr_num" = "inc_mbr_num"))
sha2 <- left_join(sha2, sha2c, by = c("incasset_id"))

sha3 <- left_join(sha3a_new, sha3b_new, 
                  by = c("incasset_id", "mbr_num" = "inc_mbr_num"))

if (UW == TRUE){
  sha6 <- left_join(sha6a_new, sha6b_new, 
                  by = c("incasset_id", "mbr_num" = "inc_mbr_num"))
}

# Add source field to track where each row came from
sha1 <- sha1 %>% mutate(sha_source = "sha1")
sha2 <- sha2 %>% mutate(sha_source = "sha2")
sha3 <- sha3 %>% mutate(sha_source = "sha3")

if (UW == TRUE) {
  
  sha6 <- sha6 %>% mutate(sha_source = "sha6")
  
  ### Clean column types before append ### change to match new mappings  check other things
  sha1 <- sha1 %>%
          mutate(subs_type = as.character(subs_type),
                hhold_size = as.integer(hhold_size),
                rent_tenant = as.numeric(rent_tenant),
                r_hisp = as.numeric(r_hisp),
                hh_asset_val=as.numeric(hh_asset_val),
                hh_inc_tot_adj=as.numeric(hh_inc_tot_adj))

  sha2 <- sha2 %>%
            mutate(subs_type = as.character(subs_type),
                  rent_tenant = as.numeric(rent_tenant),
                  r_hisp = as.numeric(r_hisp),
                  hh_asset_val=as.numeric(hh_asset_val),
                  ph_rent_ceiling=as.integer(ph_rent_ceiling),
                  hh_inc_tot_adj=as.numeric(hh_inc_tot_adj))

  sha3 <- sha3 %>%
            mutate(subs_type = as.character(subs_type),
                  unit_zip = as.character(unit_zip),
                  rent_tenant = as.numeric(rent_tenant))
  
  sha6 <- sha6 %>%
    mutate(subs_type = as.character(subs_type),
           unit_zip = as.character(unit_zip),
           rent_tenant = as.numeric(rent_tenant))

  # Append data and drop data fields not being used (data from SHA are blank)
  sha_ph <- bind_rows(sha1, sha2, sha3, sha6)
} else {
  sha_ph <- bind_rows(sha1, sha2, sha3) %>%
  select(-fss_date, -emp_date, -fss_start_date, -fss_end_date, -fss_extend_date)
}

# Fix more formats
sha_ph <- sha_ph %>%
  mutate(property_id = ifelse(as.numeric(property_id) < 10 & 
                                !is.na(as.numeric(property_id)), 
                              paste0("00", property_id),
                              ifelse(as.numeric(property_id) >= 10 & 
                                       as.numeric(property_id) < 100 & 
                                       !is.na(as.numeric(property_id)), 
                                     paste0("0", property_id),
                              property_id))) %>%
  mutate_at(vars(contains("date"), dob), funs(as.Date(., format = "%m/%d/%Y")))

# Join with portfolio data
sha_ph <- left_join(sha_ph, sha_portfolio_codes, by = c("property_id"))

# Rename specific portfolio
sha_ph <- mutate(sha_ph, 
                 portfolio = ifelse(str_detect(portfolio, "Lake City Court"),
                                    "Lake City Court", portfolio))



#### JOIN HCV FILES ####
# Clean up mismatching variables
sha4a <- sha4a %>%
  mutate(mbr_num = as.numeric(ifelse(mbr_num == "NULL", NA, mbr_num)),
         # Truncate increment numbers to match the reference list when joined
         increment_old = increment,
         increment = str_sub(increment, 1, 5)) %>%
  mutate_at(vars(contains("date"), dob), funs(as.Date(., format = "%m/%d/%Y")))

sha4a <- yesno_f(sha4a, r_white, r_black, r_aian, r_asian, r_nhpi, 
                portability, disability)
sha4a <- sha4a %>% mutate(r_hisp = ifelse(r_hisp == 2 & !is.na(r_hisp), 
                                        0, r_hisp))

sha5a_new <- sha5a_new %>%
  mutate(
    act_type = car::recode(
      act_type, c("'Annual HQS Inspection Only' = 13; 
                  'Annual Reexamination' = 2;
                  'Annual Reexamination Searching' = 9; 'End Participation' = 6;
                  'Expiration of Voucher' = 11; 'FSS/WtW Addendum Only' = 8;
                  'Historical Adjustment' = 14; 'Interim Reexamination' = 3; 
                  'Issuance of Voucher' = 10; 'New Admission' = 1; 
                  'Other Change of Unit' = 7; 
                  'Port-Out Update (Not Submitted To MTCS)' = 16;
                  'Portability Move-in' = 4; 'Portability Move-out' = 5; 
                  'Portablity Move-out' = 5; 'Void' = 15; else = NA"))
    ) %>%
  mutate_at(vars(contains("date"), dob), funs(as.Date(., format = "%m/%d/%Y")))


sha5a_new <- yesno_f(sha5a_new, portability, disability, tb_rent_ceiling)
sha5a_new <- sha5a_new %>%
  mutate(r_hisp = as.numeric(case_when(
    r_hisp == "1 " ~ 1,
    r_hisp == "2 " ~ 0
  )))


# Join with income and asset files
sha4 <- left_join(sha4a, sha1b, 
                  by = c("incasset_id", "mbr_num" = "inc_mbr_num"))
sha4 <- left_join(sha4, sha1c, by = c("incasset_id"))
sha4 <- left_join(sha4, sha_prog_codes, by = c("increment"))

if (UW == TRUE) {
  sha5 <- left_join(sha5a_new, sha5b_new, 
                    by = c("cert_id", "mbr_id"))
  sha5 <- left_join(sha5, sha_vouch_type, by = c("cert_id", "hh_id", "mbr_id", "act_type", "act_date"))
  sha5 <- left_join(sha5, sha_prog_codes, by = c("increment"))
} else {
  sha5 <- left_join(sha5a_new, sha5b_new,
                    by = c("cert_id", "mbr_id", "increment"))
  sha5 <- left_join(sha5, sha_prog_codes, by = c("increment"))
}

# Add source field to track where each row came from
sha4 <- sha4 %>% mutate(sha_source = "sha4")
sha5 <- sha5 %>% mutate(sha_source = "sha5")

# Append data
# Fix column type mismatch before append
if (UW == TRUE) {
  sha4 <- sha4 %>%
            mutate(hh_asset_val = as.integer(hh_asset_val))
}

sha_hcv <- bind_rows(sha4, sha5)

#### JOIN PH AND HCV COMBINED FILES ####
# Clean up mismatching variables
if (UW == TRUE) {
  sha_ph <- yesno_f(sha_ph, r_white, r_black, r_aian, r_asian, r_nhpi, 
                    portability, disability)
                    
  # Cleanup before append
  sha_hcv <- sha_hcv %>%
              mutate(unit_zip = as.character(unit_zip))
} else {
  sha_ph <- yesno_f(sha_ph, r_white, r_black, r_aian, r_asian, r_nhpi, 
                  portability, disability, access_unit, access_req, 
                  assist_tanf, assist_gen, assist_food, assist_mcaid_chip,
                  assist_eitc)
}

#### Append data ####
sha <- bind_rows(sha_ph, sha_hcv)


### Fix up conflicting and missing income
# Some joined income data will show NA for HH fields. Use summarise to 
# fill in gaps (rather than mutate, which is slow)
# Data recorded in the HH fields do not add up to the calculated HH income
# Need to standardize, calculated data seems more accurate
hh_inc_y <- sha %>%
  filter(!is.na(hh_inc.y)) %>%
  group_by(incasset_id, cert_id, increment) %>%
  summarise(hh_inc.y = max(hh_inc.y, na.rm = T)) %>%
  ungroup()

hh_inc_adj_y <- sha %>%
  filter(!is.na(hh_inc.y)) %>%
  group_by(incasset_id, cert_id, increment) %>%
  summarise(hh_inc_adj.y = max(hh_inc_adj.y, na.rm = T)) %>%
  ungroup()


sha <- left_join(sha, hh_inc_y, by = c("incasset_id", "cert_id", "increment"))
sha <- left_join(sha, hh_inc_adj_y, by = c("incasset_id", "cert_id", "increment"))

# Now replace all NAs with 0 (came from joins where no income available)
sha <- sha %>%
  mutate_at(vars(contains("inc"), contains("asset")),
            funs(ifelse(is.na(.), 0, .)))

sha <- sha %>%
  mutate(
    hh_inc = case_when(
      sha_source %in% c("sha4", "sha5") ~ as.numeric(hh_inc),
      sha_source %in% c("sha1", "sha2", "sha3") ~ as.numeric(hh_inc.y.y)
      ),
    hh_inc_adj = hh_inc_adj.y.y,
    hh_asset_val = case_when(
      sha_source %in% c("sha1", "sha2", "sha4") ~ as.numeric(hh_asset_inc),
      sha_source %in% c("sha3", "sha5") ~ as.numeric(hh_asset_val.y)
    ),
    hh_asset_inc = case_when(
      sha_source %in% c("sha1", "sha2", "sha4") ~ as.numeric(hh_asset_inc),
      sha_source %in% c("sha3", "sha5") ~ as.numeric(hh_asset_inc.y)
    )
  ) %>%
  select(-(contains(".x")), -(contains(".y"))) %>%
  # Remake household totals to overwrite what was read in
  mutate(hh_asset_inc_final = max(hh_asset_inc, hh_asset_impute, na.rm = T),
         hh_inc_tot = hh_inc_adj + hh_asset_inc_final,
         hh_inc_tot_adj = case_when(
           is.na(hh_inc_deduct) ~ hh_inc_tot,
           !is.na(hh_inc_deduct) ~ hh_inc_tot - hh_inc_deduct
         ))

  

### Fix up a few more format issues
sha <- sha %>%
  mutate_at(vars(act_date, admit_date, dob), funs(as.Date(., format = "%m/%d/%Y")))


# Set up mbr_num head of households (will be important later when cleaning up names)
sha <- sha %>% mutate(mbr_num = ifelse(is.na(mbr_num) & ssn == hh_ssn & 
                                         lname == hh_lname & fname == hh_fname,
                        1, mbr_num))



#### Fix up SHA member numbers and head-of-household info ####
# ISSUE 1: Some households seem to have multiple HoHs recorded
# (hhold defined as the same address, action date, and PHA-generated hhold IDs)
# FIX 1: Overwrite HoH data to match mbr_num = 1
# ISSUE 2: The listed HoH isn't always member #1
# FIX 2: Switch member numbers around to make HoH member #1
# ISSUE 3: Not all households have member numbers or are missing #1
# FIX 3: Make sure the HoH has member number = 1

### Set up temp household ID  unique to a household and action date
sha$hh_id_temp <- group_indices(sha, hh_id, prog_type, unit_add, 
                                   unit_city, act_date, act_type, incasset_id)


#### FIX 1: Deal with households that have multiple HoHs listed ####
# Check for households with >1 people listed as HoH
multi_hoh <- sha %>%
  group_by(hh_id_temp) %>%
  summarise(people = n_distinct(hh_ssn, hh_lname, hh_lnamesuf, hh_fname, hh_mname)) %>%
  ungroup() %>%
  filter(people > 1) %>%
  mutate(rowcheck = row_number())

# Join to main data, restrict to member #1
multi_hoh_join <- left_join(multi_hoh, sha, by = "hh_id_temp") %>%
  filter(mbr_num == 1) %>%
  select(rowcheck, hh_id_temp, hh_ssn, hh_lname, hh_lnamesuf, hh_fname, hh_mname) %>%
  distinct()

# Add back to main data and bring over data into new columns
sha <- left_join(sha, multi_hoh_join, by = "hh_id_temp") %>%
  rename_at(vars(ends_with(".x")), funs(str_replace(., ".x", "_orig"))) %>%
  rename_at(vars(ends_with(".y")), funs(str_replace(., ".y", ""))) %>%
  mutate(
    hh_ssn = ifelse(is.na(hh_ssn), hh_ssn_orig, hh_ssn),
    hh_lname = ifelse(is.na(hh_lname), hh_lname_orig, hh_lname),
    hh_lnamesuf = ifelse(is.na(hh_lnamesuf), hh_lnamesuf_orig, hh_lnamesuf),
    hh_fname = ifelse(is.na(hh_fname), hh_fname_orig, hh_fname),
    hh_mname = ifelse(is.na(hh_mname), hh_mname_orig, hh_mname)
  )
rm(multi_hoh)
rm(multi_hoh_join)


#### FIX 2: Switch member numbers around to make HoH member #1 ####
# NB. Sometimes the original person names/SSN and HoH names/SSN don't match,
# even when the HOH is actually member #1.
# Overall, a small number of households have this general problem so skipping for
# now to avoid introducing other errors.

# Find when HoH != member number #1
# wrong_hoh <- pha_clean %>%
#   filter(mbr_num == 1 & ssn_new != hh_ssn_new & (lname_new != hh_lname | fname_new != hh_fname)) %>%
#   distinct(hh_id_temp)
# 
# # Bring in other housheold members
# wrong_hoh_join <- left_join(wrong_hoh, pha_clean, by = "hh_id_temp") %>%
#   select(hh_id_temp, ssn_new, lname_new, fname_new, mbr_num, 
#          hh_ssn_new, hh_lname, hh_fname, hh_dob) %>%
#   arrange(hh_id_temp, mbr_num) %>%
#   distinct()


#### FIX 3: Make sure the HoH has member number = 1 ####
# NB. Fixing this is also problematic because the original person-level and HoH data
# do not always match. 
# For now find households with completely missing member numbers and set the person
# whose data matches the HoH data to be member #1


### ID households that only has missing member numbers (SHA HCV data)
# First find smallest non-missing member number (almost all = 1)
# Exclude difficult temp HH IDs
min_mbr <- sha %>%
  filter(!is.na(mbr_num)) %>%
  group_by(hh_id_temp) %>%
  summarise(mbr_num_min = min(mbr_num)) %>%
  ungroup()


# Join with full list of temporary HH IDs to find which ones are missing member numbers
mbr_miss <- anti_join(sha, min_mbr, by = "hh_id_temp") %>%
  select(hh_id_temp, act_date, ssn, lname, fname, mname, lnamesuf, dob,
         mbr_num, hh_ssn, hh_lname, hh_fname, hh_mname, hh_lnamesuf) %>%
  arrange(hh_id_temp, ssn, lname, fname)

# Find the HoH and label them as member #1
mbr_miss <- mbr_miss %>%
  # Try matching on SSN
  mutate(mbr_num = ifelse(ssn == hh_ssn, 1, mbr_num)) %>%
  group_by(hh_id_temp) %>%
  mutate(done = max(mbr_num, na.rm = T)) %>%
  ungroup() %>%
  # Then try name combos
  mutate(mbr_num = ifelse(is.infinite(done) & 
                            toupper(lname) == toupper(hh_lname) & 
                            toupper(fname) == toupper(hh_fname), 
                          1, mbr_num)) %>%
  select(-done)

# If multiple people were flagged as #1, take the oldest
# Common when there are children and parents with the same name or DOB typos
# If same DOB, take row with middle inital, then no last name suffix
mbr_miss <- mbr_miss %>%
  arrange(hh_id_temp, mbr_num, dob, hh_mname, desc(hh_lnamesuf)) %>%
  group_by(hh_id_temp) %>%
  mutate(mbr_num = ifelse(row_number() > 1, NA, mbr_num)) %>%
  ungroup()


# Restrict to the newly identified HoHs and join back to main data
mbr_miss_join <- mbr_miss %>%
  filter(mbr_num == 1) %>%
  distinct(hh_id_temp, act_date, ssn, lname, fname, mname, 
           lnamesuf, dob, mbr_num)
sha <- left_join(sha, mbr_miss_join, 
                       by = c("hh_id_temp", "act_date", "ssn", 
                              "lname", "fname", "mname", "lnamesuf", "dob"))

# Bring over older member numbers and clean up columns
sha <- sha %>%
  mutate(mbr_num = ifelse(!is.na(mbr_num.y), mbr_num.y, mbr_num.x)) %>%
  select(-mbr_num.x, -mbr_num.y, -hh_id_temp, -rowcheck)

rm(min_mbr)
rm(mbr_miss)
rm(mbr_miss_join)

#### END SHA HEAD OF HOUSEHOLD FIX ####



### Transfer over data to rows with missing programs and vouchers
# (not all rows were joined earlier and it is easier to clean up at this point 
# once duplicate rows are deleted)
sha <- sha %>%
  arrange(ssn, lname, fname, dob, act_date) %>%
  group_by(ssn, lname, fname, dob) %>%
  mutate(prog_type = ifelse(is.na(prog_type) & !is.na(lag(prog_type, 1)) & 
                          unit_add == lag(unit_add, 1), 
                          lag(prog_type, 1), prog_type),
         vouch_type = ifelse(is.na(vouch_type) & !is.na(lag(vouch_type, 1)) & 
                              unit_add == lag(unit_add, 1), 
                            lag(vouch_type, 1), vouch_type)) %>%
  ungroup()


if(sql == TRUE) {
##### Load to SQL server #####
# May need to delete table first if data structure and columns have changed
  dbRemoveTable(db.apde51, name = "sha_combined")
  dbWriteTable(db.apde51, name = "sha_combined", 
              value = as.data.frame(sha), overwrite = T,
              field.types = c(
                act_date = "date",
                admit_date = "date",
                dob = "date"
              ))
}

##### Remove temporary files #####
rm(list = ls(pattern = "sha1"))
rm(list = ls(pattern = "sha2"))
rm(list = ls(pattern = "sha3"))
rm(list = ls(pattern = "sha4"))
rm(list = ls(pattern = "sha5"))
rm(list = ls(pattern = "sha6"))
rm(list = ls(pattern = "sha_"))
rm(list = ls(pattern = "hh_"))
rm(field_name_mapping_fn)
rm(inc_clean_f)
rm(script)
rm(fields_uw)
rm(fields)
rm(set_data_envr)
rm(METADATA)
rm(sql)
gc()
