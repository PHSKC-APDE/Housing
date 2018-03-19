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
# Clean up addresses and geocode
# Consolidate data rows ### (THIS CODE) ###
# Add in final data elements and set up analyses
# Join with Medicaid eligibility data and set up analyses
#
# Alastair Matheson (PHSKC-APDE)
# alastair.matheson@kingcounty.gov
# 2016-08-13, split into separate files 2017-10
#
###############################################################################


#### Set up global parameter and call in libraries ####
rm(list=ls()) #reset
options(max.print = 350, tibble.print_max = 50, scipen = 999, width = 100)
gc()
housing_path <- "//phdata01/DROF_DATA/DOH DATA/Housing"

library(colorout)
library(housing) # contains many useful functions for cleaning
library(openxlsx) # Used to import/export Excel files
library(stringr) # Used to manipulate string data
library(lubridate) # Used to manipulate dates
library(dplyr) # Used to manipulate data
library(tidyr) # More data manipulation


#### Bring in data and sort ####

# ==========================================================================
# TT Note: (180122) we are going around the geocoding for now
# ==========================================================================
# pha_cleanadd <- readRDS(file = paste0(housing_path, "/OrganizedData/pha_cleanadd_final.Rda"))
# load(file = "data/Housing/OrganizedData/pha_cleanadd_SANSGEOCODE_TTtemp.Rdata")
load(file = "data/Housing/OrganizedData/pha_cleanadd_final.RData")

pha_cleanadd_sort <- pha_cleanadd %>% # pha_cleanadd %>%
  arrange(ssn_id_m6, lname_new_m6, fname_new_m6, dob_m6, act_date, agency_new, prog_type)

#### Create key variables ####
### ID
pha_cleanadd_sort$pid <- group_indices(pha_cleanadd_sort, ssn_id_m6, lname_new_m6, fname_new_m6, dob_m6)


### Final agency and program fields
# Set up the groupings we want to use in analyses
pha_cleanadd_sort <- pha_cleanadd_sort %>%
  mutate(
    # Hard vs. TBS8 units (subsidy type)
    subsidy_type = ifelse(prog_type %in% c("TBS8", "TENANT BASED VOUCHER", "PORT"),
                          "TENANT BASED/SOFT UNIT", "HARD UNIT"),
    # Reorganize voucher types
    vouch_type_final =  ifelse(agency_new == "SHA" & is.na(vouch_type), "",
                               ifelse(subsidy_type == "TENANT BASED/SOFT UNIT", vouch_type,
                                      ifelse(agency_new == "SHA" & vouch_type == "MOD REHAB", "MOD REHAB",
                                             ifelse(agency_new == "SHA" & vouch_type == "PARTNER PROJECT-BASED VOUCHER",
                                                    "PARTNER PROJECT-BASED VOUCHER",
                                                    "")))),
    vouch_type_final = ifelse(agency_new == "KCHA" & prog_type == "PBS8",
                              "PARTNER PROJECT-BASED VOUCHER", vouch_type_final),
    vouch_type_final = ifelse(vouch_type_final %in% c("DOMESTIC VIOLENCE", "TERMINALLY ILL"),
                                  "OTHER (TI/DV)",
                                  ifelse(vouch_type_final %in% c("PERMANENT SUPPORTIVE HOUSING",
                                                                 "PH REDEVELOPMENT", "PROJECT-BASED - LOCAL",
                                                                 "PROJECT-BASED - REPLACEMENT HOUSING",
                                                                 "SOUND FAMILIES", "SUPPORTIVE HOUSING",
                                                                 "TENANT BASED VOUCHER"),
                                         "GENERAL TENANT-BASED VOUCHER", vouch_type_final)),
    vouch_type_final = ifelse(subsidy_type == "TENANT BASED/SOFT UNIT" & vouch_type_final == "",
                              "GENERAL TENANT-BASED VOUCHER", vouch_type_final),
    # Finalize portfolios
    portfolio_final = ifelse(agency_new == "SHA" & prog_type == "SHA OWNED/MANAGED",
                             portfolio,
                             ifelse(agency_new == "KCHA", property_type, "")),
    portfolio_final = ifelse(is.na(portfolio_final), "", portfolio_final),
    # Make operator variable
    operator_type = ifelse(subsidy_type == "HARD UNIT" &
                             (portfolio_final != "" |
                                prog_type %in% c("PH", "SHA OWNED/MANAGED") |
                                (vouch_type == "SHA OWNED PROJECT-BASED") & !is.na(vouch_type)),
                           "PHA OPERATED",
                           ifelse(
                             subsidy_type == "HARD UNIT" & !is.na(prog_type) &
                               (portfolio_final == "") &
                               ((agency_new == "SHA" & prog_type == "COLLABORATIVE HOUSING" &
                                   (vouch_type != "SHA OWNED PROJECT-BASED" | is.na(vouch_type))) |
                                  (agency_new == "KCHA" & prog_type != "PH")),
                             "NON-PHA OPERATED", ""))
  )


### Concatenated agency field
pha_cleanadd_sort <- pha_cleanadd_sort %>%
  mutate(agency_prog_concat = paste(agency_new, subsidy_type, operator_type, vouch_type_final, portfolio_final, sep = ", "))


#### Merge with KCHA EOP data - TEMPORARY MEASURE ####
# Some of the KCHA end of participation data is missing from the original extract
# Bring it in here and append. Eventually, move this step to the KCHA SQL load.
kcha_eop <- read.xlsx("~/data/KCHA/EOP Certifications_received_2017-10-05.xlsx")
# Bring in variable name mapping table
# (file is also here: https://github.com/PHSKC-APDE/Housing/blob/master/processing/Field%20name%20mapping.xlsx)
fields <- read.xlsx("data/KCHA/Field_name_mapping.xlsx")
kcha_eop <- data.table::setnames(kcha_eop, fields$PHSKC[match(names(kcha_eop), fields$KCHA_eop)])
# Fix up variable types
kcha_eop <- kcha_eop %>%
  mutate(
    hh_name = toupper(hh_name),
    hh_ssn_new = str_replace_all(hh_ssn, "-", ""),
    hh_dob = as.Date(hh_dob, origin = "1899-12-30"),
    act_date = as.Date(act_date, origin = "1899-12-30"),
    prog_type = car::recode(prog_type, "'MTW Tenant-Based Assistance' = 'TBS8';
                            'MTW Project-Based Assistance' = 'PBS8';
                            'MTW Public Housing' = 'PH'"),
    add_yr_eop = as.numeric(car::recode(add_yr_eop, "'WIN Rent' = '2';
                             'Manual Rent Override (WIN Rent)' = '2';
                             'EASY Rent' = '3';
                             'Manual Rent Override (Easy Rent)' = '3';
                             else = 2")),
    source_eop = "eop"
  ) %>%
  # Restrict to necessary columns
  select(hh_ssn_new, hh_dob:act_type, act_date, add_yr_eop, source_eop) %>%
  # Drop missing SSNs
  filter(!is.na(hh_ssn_new))

# Link to main data, match on SSN
kcha_eop_join <- left_join(kcha_eop, pha_cleanadd_sort, by = c("hh_ssn_new" = "ssn_id_m6")) %>%
  select(hh_ssn_new:source_eop, pid, hhold_id_new, dob_m6, lname_new_m6, fname_new_m6,
         hh_ssn_id_m6, hh_lname_m6, hh_fname_m6, act_date.y, act_type.y,
         subsidy_type, operator_type, vouch_type_final, portfolio_final)
# Drop rows where the names don't line up with the HoH or when no match found
kcha_eop_join <- kcha_eop_join %>%
  mutate(drop = if_else((is.na(act_date.y) & is.na(lname_new_m6)) | hh_ssn_new != hh_ssn_id_m6, 1, 0)) %>%
  filter(drop == 0)
kcha_eop_join <- kcha_eop_join %>%
  mutate(
    drop = if_else(str_detect(str_replace_all(hh_name, "[:punct:]|[:space:]", ""),
                                              str_replace_all(lname_new_m6, "[:punct:]|[:space:]", "")) == F, 1, 0),
    # Check first names to make sure there really isn't a match
    drop = if_else(drop == 1 & str_detect(hh_name, fname_new_m6), 0, drop)) %>%
  filter(drop == 0 | is.na(drop))
# Drop person if the EOP is already in the data
kcha_eop_join <- kcha_eop_join %>%
  mutate(drop = if_else(is.na(act_date.y), 1,
                        if_else(act_date.x == act_date.y, 1, 0))) %>%
  # Apply drop to all that person's rows
  group_by(hh_ssn_new) %>%
  mutate(drop = max(drop)) %>%
  ungroup() %>%
  filter(drop == 0)
# Reformat data so it is ready to apply to the whole dataset
kcha_eop_merge <- kcha_eop_join %>%
  mutate(hh_ssn_id_m6 = hh_ssn_new, act_type = act_type.x,
         act_date = act_date.x, prog_type = prog_type.x, agency_new = "KCHA",
         subsidy_type = ifelse(prog_type.x %in% c("PH", "PBS8"), "HARD UNIT",
                               ifelse(prog_type.x == "TBS8", "TENANT BASED/SOFT UNIT", "")),
         operator_type = NA, vouch_type_final = NA, portfolio_final = NA,
         agency_prog_concat = paste(agency_new, subsidy_type, operator_type,
                                    vouch_type_final, portfolio_final, sep = ", ")) %>%
  select(pid, hhold_id_new, hh_ssn_id_m6, act_type, act_date, agency_new, prog_type,
         subsidy_type, operator_type, vouch_type_final, portfolio_final, agency_prog_concat) %>%
  distinct()

# Join with all other people in the household
# NB. May end up adding rows for people who have already left the household
kcha_eop_full <- left_join(kcha_eop_merge, pha_cleanadd_sort, by = "hhold_id_new")
kcha_eop_full <- kcha_eop_full %>%
  mutate(pid = pid.y, hh_ssn_id_m6 = hh_ssn_id_m6.y, act_type = act_type.x,
         act_date = act_date.x,  agency_new = agency_new.x,
         prog_type = prog_type.x, subsidy_type = subsidy_type.x,
         operator_type = operator_type.x,
         vouch_type_final = vouch_type_final.x,
         portfolio_final = portfolio_final.x,
         agency_prog_concat = agency_prog_concat.x,
         source_eop = "eop") %>%
  select(pid, ssn_id_m6, lname_new_m6, fname_new_m6, dob_m6, hhold_id_new,
         hh_ssn_id_m6, hh_lname_m6, hh_fname_m6, hh_dob_m6,
         act_type, act_date, agency_new, prog_type, subsidy_type,
         operator_type, vouch_type_final, portfolio_final, agency_prog_concat,
         source_eop) %>%
  distinct()

# Append data and remove any duplicate rows
pha_cleanadd_sort <- bind_rows(pha_cleanadd_sort, kcha_eop_full)
pha_cleanadd_sort <- pha_cleanadd_sort %>%
  arrange(pid, agency_prog_concat, act_date) %>%
  distinct()


# Fill in any demographics that will be needed
pha_cleanadd_sort <- pha_cleanadd_sort %>%
  mutate_at(vars(gender_new_m6, r_hisp_new, race2, dob_m6, r_hisp_new, race2, disability,
                 citizen),
            funs(ifelse(source_eop == "eop" & pid == lag(pid, 1) & is.na(.), lag(., 1), .))) %>%
  mutate_at(vars(subsidy_type, operator_type, vouch_type_final, portfolio_final),
            funs(ifelse(source_eop == "eop" & is.na(.), "", .)))

# Remove temporary files
rm(list = ls(pattern = "^kcha_eop"))
rm(fields)
gc()




#### Begin consolidation ####
# New approach to tracking which rows are dropped
# Assign a different drop code for each instance and track in a list of all rows
# Drop types:
# 01 = missing action dates
# 02 = duplicate rows (due to varying cert IDs or income data)
# 03 = where address data are missing
# 04 = when a person is in both KCHA and SHA data due to port ins/outs
# 05 = blank addresses when there is an address for the same date (within a given program/subtype/spec voucher etc.)
# 06 = different programs with the same start date within the same agency
# 07 = different agencies with the same or similar action date (mostly remaining ports)
# 08 = auto-generated recertifications that are overwriting EOPs
# 09 = annual reexaminations/intermediate visits within a given address and program
# 10 = collapse rows to have a single line per person per address per time there
# 11 = delete rows where startdate = enddate and also startdate = the next row's startdate (often same program)
# 12 = delete one row of pairs with identical start and end dates

### Set up row numbers
pha_cleanadd_sort <- pha_cleanadd_sort %>% mutate(row = row_number())
# Set up list of all rows to track
drop_track <- pha_cleanadd_sort %>% select(row, pid, ssn_new:dob, agency_prog_concat, unit_concat, act_date, act_type, sha_source, cost_pha)

### Find the latest action date for a given program (will be useful later)
# Group by cost_pha too because some port outs move between agencies
# Ignore the warning produced, this will be addressed when rows with no action date are dropped
pha_cleanadd_sort <- pha_cleanadd_sort %>%
  group_by(pid, agency_prog_concat, unit_concat, cost_pha) %>%
  mutate(max_date = max(act_date, na.rm = T)) %>%
  ungroup()

### Count the number of unique address a person had
# (used to delete people who are only in KCHA data and had no address)
# Mutate was very slow so making new temp df
add_num_tmp <- pha_cleanadd_sort %>%
  group_by(pid) %>%
  summarise(add_num = n_distinct(unit_concat))

pha_cleanadd_sort <- left_join(pha_cleanadd_sort, add_num_tmp, by = "pid")
rm(add_num_tmp)


#### Make port in and out variables (will be refined further after additional row consolidation) ####
# Port in
pha_cleanadd_sort <- pha_cleanadd_sort %>%
  mutate(port_in = ifelse(
    # Need to avoid catching 'SUPPORTIVE HOUSING'
    prog_type == "PORT" | act_type == 4 |
      (agency_new == "KCHA" & cost_pha != "" & cost_pha != "WA002") |
      (agency_new == "SHA" & cost_pha != "" & cost_pha != "WA001" &
         # SHA seems to point to another billed PHA even when the person has ported out from SHA to another PHA, need to ignore this
         !((unit_concat == ",,,,NA" | str_detect(unit_concat, "PORT OUT")) & act_type %in% c(5, 16))),
      # The portability flag seems unreliable so ignoring for now
      #| (portability %in% c("Y", "Yes") & !is.na(portability)),
    1, 0))

# Port out
# Seems to be that when SHA is missing address data and the action code == Port-Out Update (Not Submitted To MTCS) (recoded as 16),
# the person is in another housing authority.
pha_cleanadd_sort <- pha_cleanadd_sort %>%
  mutate(
    # Find rows from the agency that indicate the person has already ported out
    port_out_kcha = ifelse(agency_new == "KCHA" & (str_detect(unit_concat, "PORTABLE") | act_type == 5), 1, 0),
    port_out_sha = ifelse(agency_new == "SHA" & (str_detect(unit_concat, "PORT OUT") | act_type == 5), 1, 0),
    port_out_sha = ifelse(agency_new == "SHA" & act_type == 16 & cost_pha != "", 1, port_out_sha),
    # Record the port on the other PHA row for that same or similar date (to track after rows are dropped)
    port_out_kcha = ifelse(
      ((pid == lead(pid, 1) & !is.na(lead(pid, 1)) & abs(act_date - lead(act_date, 1)) <= 31 &
         agency_new == "SHA" & lead(agency_new, 1) == "KCHA" & (lead(port_out_kcha, 1) == 1 | port_in == 1) &
         !lead(act_type, 1) %in% c(1, 4)) |
        (pid == lag(pid, 1) & !is.na(lag(pid, 1)) & abs(act_date - lag(act_date, 1)) < 31 &
           agency_new == "SHA" & lag(agency_new, 1) == "KCHA" & (lag(port_out_kcha, 1) == 1 | port_in == 1) &
           lag(act_type, 1) != 4)) &
        cost_pha %in% c("", "WA002") & act_type != 16,
      1, port_out_kcha),
    port_out_sha = ifelse(
      ((pid == lead(pid, 1) & !is.na(lead(pid, 1)) & abs(act_date - lead(act_date, 1)) < 31 &
         agency_new == "KCHA" & lead(agency_new, 1) == "SHA" & (lead(port_out_sha, 1) == 1 | port_in == 1) &
         lead(act_type, 1) != 4) |
        (pid == lag(pid, 1) & !is.na(lag(pid, 1)) & abs(act_date - lag(act_date, 1)) < 31 &
           agency_new == "KCHA" & lag(agency_new, 1) == "SHA" & (lag(port_out_sha, 1) == 1 | port_in == 1) &
           lag(act_type, 1) != 4)) &
        cost_pha %in% c("", "WA001"),
      1, port_out_sha),
    # Also use cost_pha field to find port_outs from the other PHA that weren't picked up due to data quality issues
    port_out_kcha = ifelse(
      agency_new == "SHA" & cost_pha == "WA002" &
        # SHA seems to point to another billed PHA even when the person has ported out from SHA to another PHA, need to ignore this
        !((unit_concat == ",,,,NA" | str_detect(unit_concat, "PORT OUT")) & act_type %in% c(5, 16)), 1, port_out_kcha),
    port_out_sha = ifelse(agency_new == "KCHA" & cost_pha == "WA001", 1, port_out_sha)
  )

#### Remove missing dates (droptype = 1) ####
dfsize_head <- nrow(pha_cleanadd_sort) # Keep track of size of data frame at each step
pha_cleanadd_sort <- pha_cleanadd_sort %>% mutate(drop = ifelse(is.na(act_date), 1, 0))
# Pull out drop tracking and merge
drop_temp <- pha_cleanadd_sort %>% select(row, drop)
drop_track <- left_join(drop_track, drop_temp, by = "row")
# Finish dropping rows
pha_cleanadd_sort <- pha_cleanadd_sort %>% filter(drop != 1)
dfsize_head - nrow(pha_cleanadd_sort)


#### Clean up duplicate rows (droptype == 2) ####
# Some duplicate rows are because of additional income/asset information in the SHA data (will be stripped out earlier eventually)
# Others are because of different subsidy/cert IDs in the KCHA data
# Taking the second row should work as that captures non-port subsidy IDs
dfsize_head <- nrow(pha_cleanadd_sort)
pha_cleanadd_sort <- pha_cleanadd_sort %>%
  arrange(pid, agency_prog_concat, act_date) %>%
  mutate(drop = if_else(
    pid == lead(pid, 1) & !is.na(lead(pid, 1)) &
      (unit_concat == lead(unit_concat, 1) | (is.na(unit_concat) & is.na(lead(unit_concat, 1)))) &
      (agency_prog_concat == lead(agency_prog_concat, 1)  |
         (is.na(agency_prog_concat) & is.na(lead(agency_prog_concat, 1)))) &
      (cost_pha == lead(cost_pha, 1)  | (is.na(cost_pha) & is.na(lead(cost_pha, 1)))) &
      (act_date == lead(act_date, 1)  | (is.na(act_date) & is.na(lead(act_date, 1)))) &
      (max_date == lead(max_date, 1)  | (is.na(max_date) & is.na(lead(max_date, 1)))) &
      act_type == lead(act_type, 1) &
      (sha_source == lead(sha_source, 1) | (is.na(sha_source) & is.na(lead(sha_source, 1)))) &
      (hhold_inc_fixed == lead(hhold_inc_fixed, 1) |
         (is.na(hhold_inc_fixed & is.na(lead(hhold_inc_fixed, 1))))) &
      (hhold_inc_vary == lead(hhold_inc_vary, 1) |
         (is.na(hhold_inc_vary & is.na(lead(hhold_inc_vary, 1))))) &
      (relcode == lead(relcode, 1)  | (is.na(relcode) & is.na(lead(relcode, 1)))) &
      (inc_fixed == lead(inc_fixed, 1) | (is.na(inc_fixed & is.na(lead(inc_fixed, 1))))),
    2, 0))
# Pull out drop tracking and merge
drop_temp <- pha_cleanadd_sort %>% select(row, drop)
drop_track <- left_join(drop_track, drop_temp, by = "row") %>%
  mutate(drop = ifelse(!is.na(drop.x) & drop.x > 0, drop.x, drop.y)) %>%
  select(-drop.x, -drop.y)
# Finish dropping rows
pha_cleanadd_sort <- pha_cleanadd_sort %>% filter(drop == 0 | is.na(drop))
dfsize_head - nrow(pha_cleanadd_sort) # Track how many rows were dropped


#### Drop rows with missing address data that are not port outs (droptype = 3) ####
dfsize_head <- nrow(pha_cleanadd_sort)
pha_cleanadd_sort <- pha_cleanadd_sort %>%
  mutate(
    # Start with non-port-out rows with act_type == 10 or 16)
    drop = ifelse(unit_concat %in% c(",,,,NA", ",,,,0") &
                    (act_type == 10 |
                       (act_type == 16 &
                          !(port_out_kcha == 1 | port_out_sha == 1))), 3, 0),
    # Also drop people who were only at KCHA and only had missing addresses
    drop = ifelse(agency_new == "KCHA" & add_num == 1 & unit_concat %in% c(",,,,NA", ",,,,0"),
                  3, drop)
    )
# Pull out drop tracking and merge
drop_temp <- pha_cleanadd_sort %>% select(row, drop)
drop_track <- left_join(drop_track, drop_temp, by = "row") %>%
  mutate(drop = ifelse(!is.na(drop.x) & drop.x > 0, drop.x, drop.y)) %>%
  select(-drop.x, -drop.y)
# Finish dropping rows
pha_cleanadd_sort <- pha_cleanadd_sort %>% filter(drop == 0 | is.na(drop))
dfsize_head - nrow(pha_cleanadd_sort) # Track how many rows were dropped


#### Find when a person is in both KCHA and SHA data due to port ins/outs (droptype = 4) ####
# First find rows with the same program/date but one row is missing the cost_pha field (makes the steps below work better)
dfsize_head <- nrow(pha_cleanadd_sort)
pha_cleanadd_sort <- pha_cleanadd_sort %>%
  arrange(pid, act_date, agency_prog_concat, unit_concat, cost_pha) %>%
  mutate(drop = if_else(pid == lead(pid, 1) & !is.na(lead(pid, 1)) & act_date == lead(act_date, 1) &
                        agency_prog_concat == lead(agency_prog_concat, 1) & unit_concat == lead(unit_concat, 1) &
                          cost_pha == "" & lead(cost_pha != ""), 4, 0))
# Pull out drop tracking and merge
drop_temp <- pha_cleanadd_sort %>% select(row, drop)
drop_track <- left_join(drop_track, drop_temp, by = "row") %>%
  mutate(drop = ifelse(!is.na(drop.x) & drop.x > 0, drop.x, drop.y)) %>%
  select(-drop.x, -drop.y)
# Finish dropping rows
pha_cleanadd_sort <- pha_cleanadd_sort %>% filter(drop == 0 | is.na(drop))


# Find instances when person is in both agencies on the same or similar date and delete the row for the agency
# not filling in the form (i.e., usually the one being billed for their port out)
# However, cost_pha is not consistently used so do not rely on solely that (also use action types and port flags)
repeat {
  dfsize <-  nrow(pha_cleanadd_sort)
  pha_cleanadd_sort <- pha_cleanadd_sort %>%
    arrange(pid, act_date, agency_prog_concat, unit_concat) %>%
    # Need to check to pairs in both directions because sometimes the SHA date is just before the KCHA one and
    # relying on an alphabetical sort will fail
    # NB. There are some differences between the agencies that require slightly different code
    mutate(drop = if_else(
      (pid == lead(pid, 1) & !is.na(lead(pid, 1)) & abs(act_date - lead(act_date, 1)) <= 31 &
         agency_new == "KCHA" & lead(agency_new, 1) == "SHA" &
         lead(port_out_kcha, 1) == 1 &
         !act_type %in% c(5, 6)) |
        (pid == lag(pid, 1) & !is.na(lag(pid, 1)) & abs(act_date - lag(act_date, 1)) <= 31 &
           agency_new == "KCHA" & lag(agency_new, 1) == "SHA" &
           (unit_concat == lag(unit_concat, 1) | unit_concat == ",,,,0" |
              is.na(unit_concat) | lag(port_out_kcha, 1) == 1) &
           ((act_type %in% c(5, 6) & lag(act_type, 1) %in% c(1, 4)) | !act_type %in% c(1, 4, 5, 6))) |
        (pid == lead(pid, 1) & !is.na(lead(pid, 1)) & abs(act_date - lead(act_date, 1)) <= 31 &
           agency_new == "SHA" & lead(agency_new, 1) == "KCHA" &
           (unit_concat == lead(unit_concat, 1) | unit_concat == ",,,,NA" | lead(port_out_sha, 1) == 1) &
           !act_type %in% c(5, 6)) |
        (pid == lag(pid, 1) & !is.na(lag(pid, 1)) & abs(act_date - lag(act_date, 1)) <= 31 &
           agency_new == "SHA" & lag(agency_new, 1) == "KCHA" &
           (unit_concat == lag(unit_concat, 1) | unit_concat == ",,,,NA" | lag(port_out_sha, 1) == 1) &
           ((act_type %in% c(5, 6) & lag(act_type, 1) %in% c(1, 4)) | !act_type %in% c(1, 4, 5, 6) |
              (act_type %in% c(5) & cost_pha == "WA002"))),
      4, 0))
  # Pull out drop tracking and merge
  drop_temp <- pha_cleanadd_sort %>% select(row, drop)
  drop_track <- left_join(drop_track, drop_temp, by = "row") %>%
    mutate(drop = ifelse(!is.na(drop.x) & drop.x > 0, drop.x, drop.y)) %>%
    select(-drop.x, -drop.y)
  # Finish dropping rows
  pha_cleanadd_sort <- pha_cleanadd_sort %>% filter(drop == 0 | is.na(drop))

  dfsize2 <-  nrow(pha_cleanadd_sort)
  if (dfsize2 == dfsize) {
    break
  }
}
dfsize_head - nrow(pha_cleanadd_sort)


#### Get rid of blank addresses when there is an address for the same date ####
# (within a given program/subtype/spec voucher etc.) (droptype = 5) #
dfsize_head <- nrow(pha_cleanadd_sort)
repeat {
  dfsize <-  nrow(pha_cleanadd_sort)
  pha_cleanadd_sort <- pha_cleanadd_sort %>%
    mutate(drop = if_else(
      (pid == lead(pid, 1) & act_date == lead(act_date, 1) &
         agency_prog_concat == lead(agency_prog_concat, 1) &
         unit_concat %in% c(",,,,NA", ",,,,0") & !(lead(unit_concat, 1) %in% c(",,,,NA", ",,,,0"))) |
        (pid == lag(pid, 1) & act_date == lag(act_date, 1) &
           agency_prog_concat == lag(agency_prog_concat, 1) &
           unit_concat %in% c(",,,,NA", ",,,,0") & !(lag(unit_concat, 1) %in% c(",,,,NA", ",,,,0"))),
      5, 0))
  # Pull out drop tracking and merge
  drop_temp <- pha_cleanadd_sort %>% select(row, drop)
  drop_track <- left_join(drop_track, drop_temp, by = "row") %>%
    mutate(drop = ifelse(!is.na(drop.x) & drop.x > 0, drop.x, drop.y)) %>%
    select(-drop.x, -drop.y)
  # Finish dropping rows
  pha_cleanadd_sort <- pha_cleanadd_sort %>% filter(drop == 0 | is.na(drop))

  dfsize2 <-  nrow(pha_cleanadd_sort)
  if (dfsize2 == dfsize) {
    break
  }
}
dfsize_head - nrow(pha_cleanadd_sort)


#### Different programs with the same or similar action date within the same agency (droptype = 6) ####
# Does not apply to address changes in the same program (these will mostly be artifacts with the start/end date being the same)
# SHA:
#    - take row from newer data source if the same type of major program
#    - prioritize HCV over PH (unless there is a single date when HCV switches to PH)
#    - drop rows where the the program type is PORT IN (assume the person was absorbed into SHA
#          (this will be mostly covered by taking the program that ran longest)
#    - sometimes different programs had slightly different action dates so use a diff of > 31 days for SHA)
# KCHA:
#    - generally prioritize HCV over PH
#    - however, if one program runs longer than the other, choose that
#    - sometimes different programs had slightly different action dates so use a diff of > 62 days for KCHA (had larger gaps than SHA))

# First wave of cleaning
dfsize_head <- nrow(pha_cleanadd_sort)
repeat {
  dfsize <-  nrow(pha_cleanadd_sort)
  pha_cleanadd_sort <- pha_cleanadd_sort %>%
  arrange(pid, act_date, agency_prog_concat) %>%
  mutate(
    # SHA
    drop = ifelse((pid == lag(pid, 1) & !is.na(lag(pid, 1)) & act_date - lag(act_date, 1) <= 31 &
                     # The act_type restriction avoids dropping rows that are genuine prog switches but may miss some that should be dropped
                     !((act_type %in% c(1, 4) & !lag(act_type, 1) %in% c(1, 4)) |
                         (!act_type %in% c(5, 6) & lag(act_type, 1) %in% c(5, 6))) &
                     agency_prog_concat != lag(agency_prog_concat, 1) & agency_new == "SHA" & lag(agency_new, 1) == "SHA" &
                    # The < line below works because PH sources are 1:3 and HCV are 4:5 (which prioritizes HCV over PH)
                    sha_source < lag(sha_source, 1)) |
                    (pid == lead(pid, 1) & !is.na(lead(pid, 1)) & act_date - lead(act_date, 1) >= -31 &
                       !((act_type %in% c(1, 4) & !lead(act_type, 1) %in% c(1, 4)) |
                           (act_type %in% c(5, 6) & !lead(act_type, 1) %in% c(5, 6))) &
                       agency_prog_concat != lead(agency_prog_concat, 1) & agency_new == "SHA" & lead(agency_new, 1) == "SHA" &
                       sha_source < lead(sha_source, 1)),
                  6, 0),
    # KCHA (generally prioritizing HCV)
    drop = ifelse((pid == lag(pid, 1) & !is.na(lag(pid, 1)) & act_date - lag(act_date, 1) <= 62 &
                     !((act_type %in% c(1, 4) & !lag(act_type, 1) %in% c(1, 4)) |
                         (!act_type %in% c(5, 6) & lag(act_type, 1) %in% c(5, 6))) &
                     agency_prog_concat != lag(agency_prog_concat, 1) & agency_new == "KCHA" & lag(agency_new, 1) == "KCHA" &
                     prog_type == "PH" & lag(prog_type, 1) %in% c("PBS8", "TBS8", "PORT") & max_date - lag(max_date, 1) <= 62) |
                    (pid == lead(pid, 1) & !is.na(lead(pid, 1)) & act_date - lead(act_date, 1) >= -62 &
                       !((act_type %in% c(1, 4) & !lead(act_type, 1) %in% c(1, 4)) |
                           (act_type %in% c(5, 6) & !lead(act_type, 1) %in% c(5, 6))) &
                       agency_prog_concat != lead(agency_prog_concat, 1) & agency_new == "KCHA" & lead(agency_new, 1) == "KCHA" &
                       prog_type == "PH" & lead(prog_type, 1) %in% c("PBS8", "TBS8", "PORT") & max_date - lead(max_date, 1) <= 62),
                  6, drop)
  )
  # Pull out drop tracking and merge
  drop_temp <- pha_cleanadd_sort %>% select(row, drop)
  drop_track <- left_join(drop_track, drop_temp, by = "row") %>%
    mutate(drop = ifelse(!is.na(drop.x) & drop.x > 0, drop.x, drop.y)) %>%
    select(-drop.x, -drop.y)
  # Finish dropping rows
  pha_cleanadd_sort <- pha_cleanadd_sort %>% filter(drop == 0 | is.na(drop))

  dfsize2 <-  nrow(pha_cleanadd_sort)
  if (dfsize2 == dfsize) {
    break
  }
}

# Second wave of cleaning
# Take program that ran until most recently (SHA and KCHA)
repeat {
  dfsize <-  nrow(pha_cleanadd_sort)
  pha_cleanadd_sort <- pha_cleanadd_sort %>%
    arrange(pid, act_date, agency_prog_concat) %>%
    mutate(
      drop = ifelse((pid == lag(pid, 1) & !is.na(lag(pid, 1)) & act_date - lag(act_date, 1) <= 30 &
                        !((act_type %in% c(1, 4) & !lag(act_type, 1) %in% c(1, 4)) |
                            (!act_type %in% c(5, 6) & lag(act_type, 1) %in% c(5, 6))) &
                        agency_prog_concat != lag(agency_prog_concat, 1) & agency_new == "SHA" & lag(agency_new, 1) == "SHA" &
                        max_date - lag(max_date, 1) <= -60) |
                       (pid == lead(pid, 1) & !is.na(lead(pid, 1)) & act_date == lead(act_date, 1) &
                          !((act_type %in% c(1, 4) & !lead(act_type, 1) %in% c(1, 4)) |
                              (act_type %in% c(5, 6) & !lead(act_type, 1) %in% c(5, 6))) &
                          agency_prog_concat != lead(agency_prog_concat, 1) & agency_new == "SHA" & lead(agency_new, 1) == "SHA" &
                          max_date - lead(max_date, 1) <= -60),
                     6, 0),
      drop = ifelse((pid == lag(pid, 1) & !is.na(lag(pid, 1)) & act_date - lag(act_date, 1) <= 60 &
                       !((act_type %in% c(1, 4) & !lag(act_type, 1) %in% c(1, 4)) |
                           (!act_type %in% c(5, 6) & lag(act_type, 1) %in% c(5, 6))) &
                       agency_prog_concat != lag(agency_prog_concat, 1) & agency_new == "KCHA" & lag(agency_new, 1) == "KCHA" &
                       max_date - lag(max_date, 1) <= -60) |
                      (pid == lead(pid, 1) & !is.na(lead(pid, 1)) & act_date == lead(act_date, 1) &
                         !((act_type %in% c(1, 4) & !lead(act_type, 1) %in% c(1, 4)) |
                             (act_type %in% c(5, 6) & !lead(act_type, 1) %in% c(5, 6))) &
                         agency_prog_concat != lead(agency_prog_concat, 1) & agency_new == "KCHA" & lead(agency_new, 1) == "KCHA" &
                         max_date - lead(max_date, 1) <= -60),
                    6, drop)
      )
  # Pull out drop tracking and merge
  drop_temp <- pha_cleanadd_sort %>% select(row, drop)
  drop_track <- left_join(drop_track, drop_temp, by = "row") %>%
    mutate(drop = ifelse(!is.na(drop.x) & drop.x > 0, drop.x, drop.y)) %>%
    select(-drop.x, -drop.y)
  # Finish dropping rows
  pha_cleanadd_sort <- pha_cleanadd_sort %>% filter(drop == 0 | is.na(drop))

  dfsize2 <-  nrow(pha_cleanadd_sort)
  if (dfsize2 == dfsize) {
    break
  }
}
dfsize_head - nrow(pha_cleanadd_sort)



#### Save point ####
save(pha_cleanadd_sort, file = "data/Housing/OrganizedData/pha_cleanadd_sort_mid-consolidation.Rdata")
save(drop_track, file = "data/Housing/OrganizedData/drop_track_mid-consolidation.Rda")
# pha_cleanadd_sort <- readRDS(file = paste0(housing_path, "/OrganizedData/pha_cleanadd_sort_mid-consolidation.Rda"))
# drop_track <- readRDS(file = paste0(housing_path, "/OrganizedData/drop_track_mid-consolidation.Rda"))

#### Set up KCHA move outs ####
# In the old KCHA system there was no record of when a household member moved
# out but the household remained on the same subsidy. Need to look at the next
# date in a program for each household and see if there is another certificate.
# If so, take the mid-point between that individual's last action date and the
# next date for the household as the EOP for that person. This section identifies
# the next household action date.
# Existing max_date is made from agency_prog_concat, unit_concat, and cost_pha.
# Need to just use agency_prog_concat here to avoid missing move outs

pha_cleanadd_sort <- pha_cleanadd_sort %>%
  group_by(pid, agency_prog_concat) %>%
  mutate(max_date2 = max(act_date, na.rm = T)) %>%
  ungroup()


# Pull out the maximum data for each person
max_date <- pha_cleanadd_sort %>%
  distinct(pid, hhold_id_new, agency_prog_concat, max_date2)
# ID which set of data is for the H-H
hh_act_dates <- pha_cleanadd_sort %>%
  filter(ssn_id_m6 == hh_ssn_id_m6) %>%
  distinct(hhold_id_new, agency_prog_concat, act_date) %>%
  arrange(hhold_id_new, act_date)
# Join together and do some filtering
act_dates_merge <- left_join(max_date, hh_act_dates,
                             by = c("hhold_id_new", "agency_prog_concat")) %>%
  arrange(hhold_id_new, pid, act_date) %>%
  filter(act_date > max_date2)
# The first row in each group is the next date for a household after this
# individual's last date. If missing, the individual was still in the household.
act_dates_merge <- act_dates_merge %>%
  group_by(pid, hhold_id_new, agency_prog_concat) %>%
  slice(., 1) %>%
  ungroup() %>%
  rename(next_hh_act = act_date)
# Join back with original data
pha_cleanadd_sort <- left_join(pha_cleanadd_sort, act_dates_merge,
                               by = c("hhold_id_new", "pid", "agency_prog_concat",
                                      "max_date2"))
# Remove temp files
rm(max_date)
rm(hh_act_dates)
rm(act_dates_merge)


#### Different agencies with the same or similar action date (droptype = 7) ####
# At this point in the row consolidation there are some residual rows recording port outs when they are also showing in the other PHA
# Removing them will improve precision when deciding whether to count someone in SHA or KCHA
dfsize_head <- nrow(pha_cleanadd_sort)
repeat {
  dfsize <-  nrow(pha_cleanadd_sort)
  pha_cleanadd_sort <- pha_cleanadd_sort %>%
    arrange(pid, act_date, agency_prog_concat) %>%
    mutate(
      # First find the obvious ports (don't use max_date because port outs likely to have longer max dates)
      drop =
        ifelse((pid == lag(pid, 1) & !is.na(lag(pid, 1)) & act_date - lag(act_date, 1) <= 62 &
                  agency_new != lag(agency_new, 1) &
                  !act_type %in% c(1, 4) &
                  ((agency_new == "SHA" & port_out_sha == 1 & lag(port_out_sha, 1) == 1) |
                     (agency_new == "KCHA" & port_out_kcha == 1 & lag(port_out_kcha, 1) == 1))
                     ) |
                 (pid == lead(pid, 1) & !is.na(lead(pid, 1)) & act_date - lead(act_date, 1) >= -62 &
                    agency_new != lead(agency_new, 1) &
                    !lead(act_type, 1) %in% c(1, 4) &
                    ((agency_new == "SHA" & port_out_sha == 1 & lead(port_out_sha, 1) == 1) |
                       (agency_new == "KCHA" & port_out_kcha == 1 & lead(port_out_kcha, 1) == 1))
                       ),
                    7, 0),
      # Then find other port out overlaps (sandwiched in the middle of port in rows)
      drop =
        ifelse((pid == lag(pid, 1) & pid == lead(pid, 1) & !is.na(lag(pid, 1)) & !is.na(lead(pid, 1)) &
                  agency_new != lag(agency_new, 1) & agency_new != lead(agency_new, 1) &
                  !act_type %in% c(1, 4) &
                  ((agency_new == "SHA" & port_out_sha == 1 & lag(port_out_sha, 1) == 1 & lead(port_out_sha, 1) == 1) |
                     (agency_new == "KCHA" & port_out_kcha == 1 & lag(port_out_kcha, 1) == 1) & lead(port_out_kcha, 1) == 1)),
        7, drop)
    )
  # Pull out drop tracking and merge
  drop_temp <- pha_cleanadd_sort %>% select(row, drop)
  drop_track <- left_join(drop_track, drop_temp, by = "row") %>%
    mutate(drop = ifelse(!is.na(drop.x) & drop.x > 0, drop.x, drop.y)) %>%
    select(-drop.x, -drop.y)
  # Finish dropping rows
  pha_cleanadd_sort <- pha_cleanadd_sort %>% filter(drop == 0 | is.na(drop))

  dfsize2 <-  nrow(pha_cleanadd_sort)
  if (dfsize2 == dfsize) {
    break
  }
}

# There are also some non-port agency switches that need fixing
pha_cleanadd_sort <- pha_cleanadd_sort %>%
  arrange(pid, act_date, agency_prog_concat) %>%
  mutate(drop = ifelse(pid == lag(pid, 1) & !is.na(lag(pid, 1)) & act_date - lag(act_date, 1) <= 62 &
                         agency_new != lag(agency_new, 1) & act_type %in% c(5, 6) & lag(act_type, 1) %in% c(1, 4),
                       7, 0))
# Pull out drop tracking and merge
drop_temp <- pha_cleanadd_sort %>% select(row, drop)
drop_track <- left_join(drop_track, drop_temp, by = "row") %>%
  mutate(drop = ifelse(!is.na(drop.x) & drop.x > 0, drop.x, drop.y)) %>%
  select(-drop.x, -drop.y)
# Finish dropping rows
pha_cleanadd_sort <- pha_cleanadd_sort %>% filter(drop == 0 | is.na(drop))

dfsize_head - nrow(pha_cleanadd_sort)


#### Remove auto-generated recertifications that are overwriting EOPs (droptype = 8) ####
# It looks like in some of the older KCHA data, recertifications were set to
# automatically generate on a given date, even if the household had already
# ended their participation at KCHA. Need to remove these.
# NB. Don't exclude if agency is KCHA + gap of >1 years + non-missing address +
#     address is NOT the same (these look like the person re-entered housing)
# For SHA there are a few bogus actions after an EOP but mostly the issue is
# an erroneous EOP. Need to set up second drop logic to catch those. Don't drop
# if agency is SHA + address is different but not missing
# (these look like the person re-entered housing/moved)

dfsize_head <- nrow(pha_cleanadd_sort)
pha_cleanadd_sort <- pha_cleanadd_sort %>%
  arrange(pid, act_date, agency_prog_concat) %>%
  mutate(
    drop = if_else(pid == lag(pid, 1) & !is.na(lag(pid, 1)) &
                     (pid != lead(pid, 1) | is.na(lead(pid, 1))) &
                     agency_new == lag(agency_new, 1) &
                     lag(act_type, 1) == 6 & !act_type %in% c(1, 4) &
                     # Slightly separate rules for SHA and KCHA
                     ((agency_new == "KCHA" &
                         (act_date - lag(act_date, 1) <= 365 |
                            (act_date - lag(act_date, 1) > 365 &
                               (unit_concat %in% c(NA, ",,,,NA", ",,,,0") |
                                  unit_concat == lag(unit_concat, 1))))) |
                        (agency_new == "SHA" & unit_concat %in% c(",,,,NA", ",,,,0"))),
                   8, 0),
    # For erroneous EOPs in SHA data, drop the row with the EOP in it
    drop = if_else(pid == lead(pid, 1) & !is.na(lead(pid, 1)) &
                     act_type == 6 & agency_new == "SHA" &
                     !unit_concat %in% c(",,,,NA", ",,,,0") &
                     unit_concat == lead(unit_concat, 1) & !is.na(lead(unit_concat)),
                   8, drop)
  )
# Pull out drop tracking and merge
drop_temp <- pha_cleanadd_sort %>% select(row, drop)
drop_track <- left_join(drop_track, drop_temp, by = "row") %>%
  mutate(drop = ifelse(!is.na(drop.x) & drop.x > 0, drop.x, drop.y)) %>%
  select(-drop.x, -drop.y)
# Finish dropping rows
pha_cleanadd_sort <- pha_cleanadd_sort %>% filter(drop == 0 | is.na(drop))
dfsize_head - nrow(pha_cleanadd_sort)


#### Remove annual reexaminations/intermediate visits within a given address + program (droptype = 9) ####
# Want to avoid capturing the first or last row for a person at a given address
# NB: now sorting by date before agency in order to reduce chances of erroneously treating two separate periods at an address as one
dfsize_head <- nrow(pha_cleanadd_sort)
pha_cleanadd_sort <- pha_cleanadd_sort %>%
  arrange(pid, act_date, agency_prog_concat) %>%
  mutate(drop = if_else(
    pid == lag(pid, 1) & pid == lead(pid, 1) & !is.na(lag(pid, 1)) & !is.na(lead(pid, 1)) &
      unit_concat == lag(unit_concat, 1) & unit_concat == lead(unit_concat, 1) &
      # Checking for prog_type and agency_new matches
      agency_prog_concat == lag(agency_prog_concat, 1) & agency_prog_concat == lead(agency_prog_concat, 1) &
      # Check that port outs are in the same place (or one is missing)
      # Don't drop if cost_pha = row above but row below is missing (indicates when voucher was absorbed by PHA)
      # unless the row after that also matches current cost_pha (indicates data entry issues)
      ((cost_pha == lag(cost_pha, 1) & cost_pha == lead(cost_pha, 1)) |
         (lag(cost_pha, 1) == "" & cost_pha == lead(cost_pha, 1)) |
         (cost_pha == "" & lag(cost_pha, 1) == lead(cost_pha, 1)) |
         (cost_pha == lag(cost_pha, 1) & lead(cost_pha, 1) == "" & cost_pha == lead(cost_pha, 2) &
            pid == lead(pid, 2) & unit_concat == lead(unit_concat, 2) & agency_prog_concat == lead(agency_prog_concat, 2))) &
      # Check that a person didn't exit the program then come in again at the same address
      !(act_type %in% c(1, 4, 5, 6)),
    9, 0))
# Pull out drop tracking and merge
drop_temp <- pha_cleanadd_sort %>% select(row, drop)
drop_track <- left_join(drop_track, drop_temp, by = "row") %>%
  mutate(drop = ifelse(!is.na(drop.x) & drop.x > 0, drop.x, drop.y)) %>%
  select(-drop.x, -drop.y)
# Finish dropping rows
pha_cleanadd_sort <- pha_cleanadd_sort %>% filter(drop == 0 | is.na(drop))
dfsize_head - nrow(pha_cleanadd_sort)




#### Set up the number of years between reexaminations ####
# Logic is somewhat complicated
# Note that a person is considered an adult only at the action date/start date
# (i.e., if they turn 18 between examinations, the new age is not considered until the next exam date)
#
# SHA low-income public housing (LIPH)
#   Annual review only (1-year gap) if ANY of the following are true:
#     - date < 2016
#     - any ADULT (18+ years) household member not elderly or disabled (excluding live-in attendants)
#     - homeworks properties with combo LIPH/tax credit funding (IDs: 10-12, 14, 16, 20 22, 24-30, 32-36, 40, 46)
#   3-year gap if ALL of the following are true:
#     - 100% adult household members are elderly and/or disabled
#     - date >= 2016
# SHA voucher holders (HCV)
#   Annual review (1-year gap) if ANY of the following are true:
#     - date < 2010
#     - household has a mod rehab voucher (increment starts with 'M')
#     - date > 2010 and < 2013 AND < 100% household income from a fixed source (pension, Social Security, SSI, veteran’s benefits)
#     - date > 2013 AND any ADULT (18+ years) household member not elderly or disabled (excluding live-in attendants)
#   3-year gap if ANY of the following are true:
#     - date >= 2010 and < 2013 AND 100% household income from a fixed source (pension, Social Security, SSI, veteran’s benefits) AND
#           not a mod rehab voucher
#     - date >= 2013 AND 100% adult (18+ years) household members are elderly and/or disabled (excluding live-in attendants)
#           AND not a mod rehab voucher
#
# KCHA LIPH
#   Annual review only (1-year gap) if ANY of the following are true:
#     - date < 2008-06-01
#   EASY status (3-year gap) if ALL of the following is true (NB: the reality is more complicated, this is a simplified approach):
#     - date >= 2008-06-01
# KCHA HCV
#   Annual review only (1-year gap) if ANY of the following are true:
#     - date < 2008-06-01
#     - date < 2011-06-01 AND < 100% adult household members are elderly and/or disabled (excluding live-in attendants)
#     - date < 2011-06-01 AND < 90% household income from a fixed source
#   WIN status (2-year-gap) if ALL of the following are true:
#     - date >= 2011-06-01
#     - < 100% adult household members are elderly and/or disabled (excluding live-in attendants) OR < 90% household income from a fixed source
#   EASY (3-year-gap) status if ALL of the following are true:
#     - date >= 2008-06-01
#     - 100% adult household members are elderly and/or disabled (excluding live-in attendants)
#     - no source of income OR at least 90% of household income from a fixed source

# NB. At this point there are people with the same address in the same program but with multiple income codes (~14k instances of this)
# This shouldn't matter once the number of years between inspections is applied to the household
# After that point, the income variable is not used so can be dropped


# Due to missing ages/action dates, need to make separate data frame and merge back
age_temp <- pha_cleanadd_sort %>%
  distinct(pid, dob_m6, act_date, mbr_num) %>%
  filter(!(is.na(dob_m6) | is.na(act_date))) %>%
  mutate(dob_m6 = as.Date(dob_m6, format = "%m%d%Y", origin = "1899-12-30")) %>%
  mutate(age = interval(start = dob_m6, end = act_date) / years(1), 1,
         # Fix up wonky birthdates and recalculate age
         # Negative ages mostly due to incorrect century
         dob_m6 = as.Date(ifelse(age < -10, format(dob_m6, "19%y-%m-%d"), format(dob_m6)), origin = "1970-01-01"),
         # Over 100 years and < 116 years treated as genuine if head of household
         # Over 100 years unlikely to be genuine if not head of household (some are typos, most century errors)
         dob_m6 = as.Date(ifelse((age > 115 & age < 117 & mbr_num == 1) | (age > 100 & age < 117 & (mbr_num > 1 | is.na(mbr_num))) |
                                   (age >= 1000 & format(dob_m6, "%y") < 18), format(dob_m6, "20%y-%m-%d"),
                                 ifelse(age >= 117 & age < 1000 | (age >= 1000 & format(dob_m6, "%y") > 17),  format(dob_m6, "19%y-%m-%d"),
                                        format(dob_m6))), origin = "1970-01-01"),
         age = round(interval(start = dob_m6, end = act_date) / years(1), 1),
         adult = ifelse(age >= 18, 1, 0),
         senior = ifelse(age >= 62, 1, 0)
  )

# Join back to main data
pha_cleanadd_sort <- pha_cleanadd_sort %>%
  left_join(., age_temp, by = c("pid", "act_date", "mbr_num")) %>%
  # Keep new DOBs if changed
  mutate(dob_m6 = ifelse(is.na(dob_m6.y), dob_m6.x, dob_m6.y))
# Count how many rows were affected
pha_cleanadd_sort %>% filter(dob_m6.x != dob_m6.y) %>% summarise(agediff = n())

# Set up Homeworks property identifier and income codes (SHA only)
pha_cleanadd_sort <- mutate(pha_cleanadd_sort,
                            homeworks = ifelse(as.numeric(property_id) %in% c(10:12, 14, 16, 20, 22, 24:30, 32:36, 40, 46), 1, 0))

# Set up income
pha_cleanadd_sort <- pha_cleanadd_sort %>%
  mutate(
    # SHA already done in SHA data merging process
    # KCHA
    inc_fixed = ifelse(agency_new == "KCHA" &
                         (((hhold_inc_fixed > 0 | hhold_inc_vary > 0) & hhold_inc_fixed / (hhold_inc_fixed + hhold_inc_vary) >= 0.9) |
                         (hhold_inc_fixed == 0 & hhold_inc_vary == 0)), 1, inc_fixed),
    inc_fixed = ifelse(agency_new == "KCHA" &
                         (hhold_inc_fixed > 0 | hhold_inc_vary > 0) & hhold_inc_fixed / (hhold_inc_fixed + hhold_inc_vary) < 0.9, 0, inc_fixed)
    )


# Look at number of years for each individual (NB. painfully slow, look for ways to optimize)
pha_cleanadd_sort <- pha_cleanadd_sort %>%
  mutate(
    # SHA public housing
    add_yr_temp = ifelse(agency_new == "SHA" & prog_type == "PH" & adult == 1 & relcode != "L" & act_date >= "2016-01-01" &
                           (senior == 1 | disability == 1) & homeworks == 0 & !is.na(homeworks),
                         3, NA),
    add_yr_temp = ifelse(agency_new == "SHA" & prog_type == "PH" & adult == 1 & relcode != "L" &
                           (act_date < "2016-01-01" | (senior == 0 & disability == 0) | homeworks == 1),
                         1, add_yr_temp),
    # SHA HCV
    add_yr_temp = ifelse(agency_new == "SHA" & prog_type != "PH" & vouch_type_final != "MOD REHAB" & adult == 1 & relcode != "L" &
                           ((act_date >= "2010-01-01" & act_date < "2013-01-01" & inc_fixed == 1) |
                              (act_date >= "2013-01-01" & (senior == 1 | disability == 1))),
                         3, add_yr_temp),
    add_yr_temp = ifelse(agency_new == "SHA" & prog_type != "PH" & adult == 1 & relcode != "L" &
                           (act_date < "2010-01-01" | vouch_type_final == "MOD REHAB" |
                              (act_date >= "2010-01-01" & act_date < "2013-01-01" & inc_fixed == 0) |
                              (act_date >= "2013-01-01" & senior == 0 & disability == 0)),
                         1, add_yr_temp),

    # KCHA default (will be modified below)
    add_yr_temp = ifelse(agency_new == "KCHA" & adult == 1 & relcode != "L" & act_date < "2011-06-01",
                         1, add_yr_temp),
    # KCHA public housing
    add_yr_temp = ifelse(agency_new == "KCHA" & prog_type == "PH" & adult == 1 & relcode != "L" & act_date >= "2008-06-01",
                         3, add_yr_temp),
    # KCHA HCV
    add_yr_temp = ifelse(agency_new == "KCHA" & prog_type != "PH" & adult == 1 & relcode != "L" & act_date >= "2008-06-01" &
                           (senior == 1 | disability == 1) & (inc_fixed == 1 | (hhold_inc_fixed == 0 & hhold_inc_vary == 0)),
                         3, add_yr_temp),
    add_yr_temp = ifelse(agency_new == "KCHA" & prog_type != "PH" & adult == 1 & relcode != "L" & act_date >= "2011-06-01" &
                           ((senior == 0 & disability == 0) | (inc_fixed == 0 & (hhold_inc_fixed > 0 | hhold_inc_vary > 0))),
                         2, add_yr_temp)
  )
pha_cleanadd_sort <- pha_cleanadd_sort %>%
  # Now apply to entire household (only NAs in add_yr_temp should be children or live-in attendants)
  group_by(hhold_id_new, act_date) %>%
  mutate(add_yr = min(add_yr_temp, na.rm = TRUE),
         # A few cleanup errors lead to some households with no add_yr (shows as infinity) so set to 1 for now
         add_yr = ifelse(add_yr > 3, 1, add_yr)
         ) %>%
  ungroup() %>%
  select(-add_yr_temp, -dob_m6.x, -dob_m6.y)

# Remove temporary data
rm(age_temp)


#### Create start and end dates for a person at that address/progam/agency ####
pha_cleanadd_sort <- pha_cleanadd_sort %>%
  arrange(pid, act_date, agency_prog_concat) %>%
  mutate(
    # First row for a person = act_date (admit_dates stretch back too far for port ins)
    # Any change in agency/program/address/PHA billed = act date
    startdate = as.Date(ifelse(pid != lag(pid, 1) | is.na(lag(pid, 1)) |
                                 agency_prog_concat != lag(agency_prog_concat, 1) |
                                 unit_concat != lag(unit_concat, 1) | cost_pha != lag(cost_pha, 1),
                               act_date, NA), origin = "1970-01-01"),
    # Last row for a person or change in agency/program =
    #   exit date or today's date or act_date + 1-3 years (depending on agency, age, and disability)
    # OR mid-point between last date and next household action date if it looks like
    #    the person has moved out (whichever is smallest of the two)
    # Other rows where that is the person's last row at that address/PHA billed but same agency/prog = act_date at next address - 1 day
    # Unless act_date is the same as startdate (e.g., because of different programs), then act_date
    enddate = as.Date(
      ifelse(act_type == 5 | act_type == 6, act_date,
             ifelse(pid != lead(pid, 1) | is.na(lead(pid, 1 )) |
                      agency_prog_concat != lead(agency_prog_concat, 1) |
                      cost_pha != lead(cost_pha, 1),
                    pmin(today(), act_date + dyears(add_yr),
                         act_date + ((next_hh_act - act_date) / 2), na.rm = TRUE),
                    # Treat a new address as simple date change
                    # (if the dates are the same avoid making the end date before the start date)
                    ifelse(unit_concat != lead(unit_concat, 1) & act_date != lead(act_date, 1),
                           lead(act_date, 1) - 1,
                           ifelse(unit_concat != lead(unit_concat, 1) & act_date == lead(act_date, 1),
                                  lead(act_date, 1),
                                  NA)))), origin = "1970-01-01")
  )


#### Collapse rows to have a single line per person per address per time there (droptype = 10) ####
# There are a few rows left with no start or end date (because of the logic around billed PHA and the data issues in that field)
dfsize_head <- nrow(pha_cleanadd_sort)
pha_cleanadd_sort <- pha_cleanadd_sort %>%
  arrange(pid, act_date, agency_prog_concat) %>%
  # Bring start and end dates onto a single line per address/program
  mutate(drop = ifelse(is.na(startdate) & is.na(enddate), 10, 0))
# Pull out drop tracking and merge
drop_temp <- pha_cleanadd_sort %>% select(row, drop)
drop_track <- left_join(drop_track, drop_temp, by = "row") %>%
  mutate(drop = ifelse(!is.na(drop.x) & drop.x > 0, drop.x, drop.y)) %>%
  select(-drop.x, -drop.y)
# Finish dropping rows
pha_cleanadd_sort <- pha_cleanadd_sort %>% filter(drop == 0 | is.na(drop))

# Then get start and end dates onto a single line
# NB. Important to retain the order from the code above
pha_cleanadd_sort <- pha_cleanadd_sort %>%
  arrange(pid, act_date, agency_prog_concat) %>%
  # Bring start and end dates onto a single line per address/program
  mutate(enddate = as.Date(ifelse(is.na(enddate), lead(enddate, 1), enddate), origin = "1970-01-01"),
         drop = ifelse(is.na(startdate) | is.na(enddate), 10, 0))
# Pull out drop tracking and merge (now add field for number of years between inspections)
drop_temp <- pha_cleanadd_sort %>% select(row, drop, add_yr)
drop_track <- left_join(drop_track, drop_temp, by = "row") %>%
  mutate(drop = ifelse(!is.na(drop.x) & drop.x > 0, drop.x, drop.y)) %>%
  select(-drop.x, -drop.y)
# Finish dropping rows
pha_cleanadd_sort <- pha_cleanadd_sort %>% filter(drop == 0 | is.na(drop))
dfsize_head - nrow(pha_cleanadd_sort) # Track how many rows were dropped


#### Refine port labels ####
### Infer ports by comparing agencies and enddates
# NB. Needs more inspection and checking
# pha_cleanadd_sort <- pha_cleanadd_sort %>%
#   arrange(pid, startdate, enddate, agency_prog_concat) %>%
#   mutate(
#     port_out_sha = ifelse(pid == lead(pid, 1) & !is.na(lead(pid, 1)) & agency_new == "SHA" & lead(agency_new, 1) == "KCHA" &
#                             enddate >= lead(startdate, 1) & lead(act_type, 1) != 6, 1, port_out_sha),
#     port_out_kcha = ifelse(pid == lead(pid, 1) & !is.na(lead(pid, 1)) & agency_new == "KCHA" & lead(agency_new, 1) == "SHA" &
#                             enddate >= lead(startdate, 1) & lead(act_type, 1) != 6, 1, port_out_kcha)
#   )

### Update port information across multiple rows within the same program
### Fill in any missing port out fields when the person switched between SHA and KCHA or vice versa
pha_cleanadd_sort <- pha_cleanadd_sort %>%
  mutate(port_out_kcha = ifelse(agency_new == "SHA" & port_in == 1 & cost_pha == "WA002", 1, port_out_kcha),
         port_out_sha = ifelse(agency_new == "KCHA" & port_in == 1 & cost_pha == "WA001", 1, port_out_sha))


#### Rows where startdate = enddate and also startdate = the next row's startdate (often same program) (droptype = 11) ####
# Use the same logic as droptype 6 to decide which row to keep
dfsize_head <- nrow(pha_cleanadd_sort)
pha_cleanadd_sort <- pha_cleanadd_sort %>%
  arrange(pid, startdate, enddate, agency_prog_concat) %>%
  mutate(
    # First capture pairs where only one row has the same start and end date
    drop = ifelse(pid == lead(pid, 1) & startdate == enddate & startdate == lead(startdate, 1) & startdate != lead(enddate, 1), 11, 0),
    drop = ifelse(pid == lag(pid, 1) & startdate == enddate & startdate == lag(startdate, 1) & startdate != lag(enddate, 1), 11, drop),
    # Then find pairs where both rows have the same start and end dates (use logic from droptype 05 to decide)
    # SHA
    drop = ifelse(
      (drop == 0 & lead(drop, 1) == 0 & pid == lead(pid, 1) &
         startdate == enddate & startdate == lead(startdate, 1) & startdate == lead(enddate, 1) &
         agency_new == "SHA" & lead(agency_new, 1) == "SHA" &
         ((sha_source < lead(sha_source, 1) & abs(max_date - lead(max_date, 1)) <= 31) |
            (sha_source == lead(sha_source, 1) & max_date - lead(max_date, 1) < -31))) |
        (drop == 0 & lag(drop, 1) == 0 & pid == lag(pid, 1) & startdate == enddate & startdate == lag(startdate, 1) & startdate == lag(enddate, 1) &
           agency_new == "SHA" & lag(agency_new, 1) == "SHA" &
           ((sha_source < lag(sha_source, 1) & abs(max_date - lag(max_date, 1)) <= 31) |
              (sha_source == lag(sha_source, 1) & max_date - lag(max_date, 1) < -31))), 11, drop),
    # KCHA
    drop = ifelse(
      (drop == 0 & lead(drop, 1) == 0 & pid == lead(pid, 1) &
         startdate == enddate & startdate == lead(startdate, 1) & startdate == lead(enddate, 1) &
         agency_new == "KCHA" & lead(agency_new, 1) == "KCHA" &
         ((prog_type == "PH" & lead(prog_type, 1) %in% c("PBS8", "TBS8", "PORT") &
             abs(max_date - lead(max_date, 1)) <= 62) |
            max_date - lead(max_date, 1) > 62)) |
        (drop == 0 & lag(drop, 1) == 0 & pid == lag(pid, 1) &
           startdate == enddate & startdate == lag(startdate, 1) & startdate == lag(enddate, 1) &
           agency_new == "KCHA" & lag(agency_new, 1) == "KCHA" &
           ((prog_type == "PH" & lag(prog_type, 1) %in% c("PBS8", "TBS8", "PORT") &
               abs(max_date - lag(max_date, 1) <= 62)) |
              max_date - lag(max_date, 1) > 62)), 11, drop),
    # If there are still pairs within a PHA, keep the program that immediately preceded this pair
    # SHA
    drop = ifelse(
      (drop == 0 & lead(drop, 1) == 0 & pid == lead(pid, 1) &
         startdate == enddate & startdate == lead(startdate, 1) & startdate == lead(enddate, 1) &
         agency_new == "SHA" & lead(agency_new, 1) == "SHA" & sha_source == lead(sha_source, 1) & abs(max_date - lead(max_date, 1)) <= 31 &
         agency_prog_concat != lag(agency_prog_concat, 1)) |
        (drop == 0 & pid == lag(pid, 1) & startdate == enddate & startdate == lag(startdate, 1) & startdate == lag(enddate, 1) &
           agency_new == "SHA" & lag(agency_new, 1) == "SHA" & sha_source == lag(sha_source, 1) & abs(max_date - lead(max_date, 1)) <= 31 &
           agency_prog_concat != lag(agency_prog_concat, 2)), 11, drop),
    # KCHA
    drop = ifelse(
      (drop == 0 & pid == lead(pid, 1) & startdate == enddate & startdate == lead(startdate, 1) & startdate == lead(enddate, 1) &
         agency_new == "KCHA" & lead(agency_new, 1) == "KCHA" & subsidy_type == lead(subsidy_type, 1) & abs(max_date - lead(max_date, 1)) <= 62 &
         agency_prog_concat != lag(agency_prog_concat, 1)) |
        (drop == 0 & lag(drop, 1) == 0 & pid == lag(pid, 1) &
           startdate == enddate & startdate == lag(startdate, 1) & startdate == lag(enddate, 1) &
           agency_new == "KCHA" & lag(agency_new, 1) == "KCHA" & subsidy_type == lag(subsidy_type, 1) & abs(max_date - lead(max_date, 1)) <= 62 &
           agency_prog_concat != lag(agency_prog_concat, 2)), 11, drop),
    # If the pairs come from different agencies, keep the PHA that immediately preceded this pair or the one with the latest max date
    drop = ifelse(
      (drop == 0 & lead(drop, 1) == 0 & pid == lead(pid, 1) &
         startdate == enddate & startdate == lead(startdate, 1) & startdate == lead(enddate, 1) &
         agency_new != lead(agency_new, 1) & agency_new != lag(agency_new, 1)) |
        (drop == 0 & lag(drop, 1) == 0 & pid == lag(pid, 1) & startdate == enddate &
           startdate == lag(startdate, 1) & startdate == lag(enddate, 1) &
           agency_new != lag(agency_new, 1) & agency_new != lag(agency_new, 2)), 11, drop),
    # There are ~50 pairs left that don't fit anything else, drop the second row
    drop = ifelse(drop == 0 & lag(drop, 1) == 0 & pid == lag(pid, 1) &
                    startdate == enddate & startdate == lag(startdate, 1) & startdate == lag(enddate, 1), 11, drop)
  )
# Pull out drop tracking and merge
drop_temp <- pha_cleanadd_sort %>% select(row, drop)
drop_track <- left_join(drop_track, drop_temp, by = "row") %>%
  mutate(drop = ifelse(!is.na(drop.x) & drop.x > 0, drop.x, drop.y)) %>%
  select(-drop.x, -drop.y)
# Finish dropping rows
pha_cleanadd_sort <- pha_cleanadd_sort %>% filter(drop == 0 | is.na(drop))
dfsize_head - nrow(pha_cleanadd_sort) # Track how many rows were dropped


#### Deal with overlapping program/agency dates (droptype == 12) ####
### First find rows with identical start and end dates
dfsize_head <- nrow(pha_cleanadd_sort)
pha_cleanadd_sort <- pha_cleanadd_sort %>%
  arrange(pid, startdate, enddate, agency_prog_concat) %>%
  mutate(
    # Keep port in row
    drop = ifelse(
      (pid == lead(pid, 1) & !is.na(lead(pid, 1)) & startdate == lead(startdate, 1) & enddate == lead(enddate, 1) &
         port_in == 0 & lead(port_in, 1) == 1) |
        (pid == lag(pid, 1) & !is.na(lag(pid, 1)) & startdate == lag(startdate, 1) & enddate == lag(enddate, 1) &
           port_in == 0 & lag(port_in, 1) == 1), 12, 0),
    # If both or neither rows are port in, keep the max date
    drop = ifelse(
      (drop == 0 & lead(drop, 1) == 0 & pid == lead(pid, 1) & !is.na(lead(pid, 1)) &
         startdate == lead(startdate, 1) & enddate == lead(enddate, 1) & max_date < lead(max_date, 1)) |
        (drop == 0 & lag(drop, 1) == 0 & pid == lag(pid, 1) & !is.na(lag(pid, 1)) &
           startdate == lag(startdate, 1) & enddate == lag(enddate, 1) & max_date < lag(max_date, 1)), 12, drop),
    # If max_dates and ports are the same, keep any special type programs
    drop = ifelse(
      (drop == 0 & lead(drop, 1) == 0 & pid == lead(pid, 1) & !is.na(lead(pid, 1)) &
         startdate == lead(startdate, 1) & enddate == lead(enddate, 1) &
         vouch_type_final != "" & !is.na(vouch_type_final) &
         (lead(vouch_type_final, 1) == "" | is.na(lead(vouch_type_final, 1)))) |
        (drop == 0 & lag(drop, 1) == 0 & pid == lag(pid, 1) & !is.na(lag(pid, 1)) &
           startdate == lag(startdate, 1) & enddate == lag(enddate, 1) &
           vouch_type_final != "" & !is.na(vouch_type_final) &
           (lag(vouch_type_final, 1) == "" | is.na(lag(vouch_type_final, 1)))), 12, drop),
    # For the remaining ~84 pairs, drop the second row
    drop = ifelse(drop == 0 & lag(drop, 1) == 0 & pid == lag(pid, 1) & !is.na(lag(pid, 1)) &
                    startdate == lag(startdate, 1) & enddate == lag(enddate, 1), 12, drop)
  )

### Then find rows with different start dates but the overlap is due to an exit (act_type = 5 or 6 and has the same start/end date)
# Most rows exist because sometimes a person starts in a new program/agency before they are closed out of the previous one
# Be sure not to capture legitimate exits where the program is the same but the address was missing in the exit row
# NB. Sometimes the addresses are the same and the programs are slightly different
# Can check up two rows to see
pha_cleanadd_sort <- pha_cleanadd_sort %>%
  arrange(pid, startdate, enddate, agency_prog_concat) %>%
  mutate(drop = ifelse(pid == lag(pid, 1) & !is.na(lag(pid, 1)) & lag(enddate, 1) >= startdate & startdate == enddate & act_type %in% c(5, 6) &
                         !((agency_prog_concat == lag(agency_prog_concat, 1) & unit_concat %in% c(",,,,NA", ",,,,0")) |
                            unit_concat == lag(unit_concat, 1) |
                             (unit_concat %in% c(",,,,NA", ",,,,0") & agency_new == "KCHA" &
                                (prog_type == "TBS8" & lag(prog_type, 1) == "PORT") | prog_type == lag(prog_type, 1))),
                       12, drop))

# Pull out drop tracking and merge
drop_temp <- pha_cleanadd_sort %>% select(row, drop)
drop_track <- left_join(drop_track, drop_temp, by = "row") %>%
  mutate(drop = ifelse(!is.na(drop.x) & drop.x > 0, drop.x, drop.y)) %>%
  select(-drop.x, -drop.y)
# Finish dropping rows
pha_cleanadd_sort <- pha_cleanadd_sort %>% filter(drop == 0 | is.na(drop))
dfsize_head - nrow(pha_cleanadd_sort) # Track how many rows were dropped


### Truncate overlapping dates
# Assume that most recent program/agency is the one to count
# (will be biased to the second one alphabetically when start dates are the same, if any remain)
pha_cleanadd_sort <- pha_cleanadd_sort %>%
  arrange(pid, startdate, enddate, agency_prog_concat) %>%
  mutate(
    # Make a note of which rows were truncated
    truncated = ifelse(pid == lead(pid, 1) & !is.na(lead(pid, 1)) &
                         (enddate >= lead(startdate, 1) | (enddate >= lead(startdate, 1) & startdate == lead(startdate, 1))),
                       1, 0),
    # Now truncate
    enddate = as.Date(ifelse(
      # If the start dates aren't the same, use next start date - 1 day
      pid == lead(pid, 1) & !is.na(lead(pid, 1)) & enddate >= lead(startdate, 1) &
        startdate != lead(startdate, 1),
      lead(startdate, 1) - 1,
      # If the start dates are the same, set the first end date equal to the start date
      # (avoids having negative time in housing but does lead to duplicate rows for a person on a given date)
      ifelse(pid == lead(pid, 1) & !is.na(lead(pid, 1)) & enddate >= lead(startdate, 1) & startdate == lead(startdate, 1),
             startdate, enddate))
      , origin = "1970-01-01"))
# See how many rows were affected
sum(pha_cleanadd_sort$truncated, na.rm = T)


#### Export drop tracking data ####
save(drop_track, file = "~/data/Housing/OrganizedData/drop_track.Rdata")
#drop_track <- readRDS(file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/drop_track.Rda")
rm(drop_temp)
rm(drop_track)

#### Save point ####
pha_cleanadd_sort_dedup <- pha_cleanadd_sort
save(pha_cleanadd_sort_dedup, file = "~/data/Housing/OrganizedData/pha_cleanadd_sort_dedup.Rdata")

### Clean up remaining data frames
rm(pha_cleanadd)
rm(pha_cleanadd_sort)
gc()
