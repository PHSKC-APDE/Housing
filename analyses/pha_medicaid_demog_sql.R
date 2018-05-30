###############################################################################
# Code to load summary housing and Medicaid enrollment data to SQL
#
# Alastair Matheson (PHSKC-APDE)
# alastair.matheson@kingcounty.gov
# 2018-01-22
#
#
###############################################################################


#### Set up global parameter and call in libraries ####
# Turn scientific notation off and other settings
options(max.print = 700, scipen = 100, digits = 5)

library(stringr) # Used to manipulate string data
library(lubridate) # Used to manipulate dates
library(dplyr) # Used to manipulate data
library(RODBC) # Used to load data to SQL


housing_path <- "//phdata01/DROF_DATA/DOH DATA/Housing"
db.apde51 <- odbcConnect("PH_APDEStore51")

#### Bring in data ####
pha_elig_final <- readRDS(file = paste0(housing_path, "/OrganizedData/pha_elig_final.Rda"))

### Restrict to necessary columns
pha_elig_sql <- pha_elig_final %>%
  select(mid, pid2, startdate_c, enddate_c, enroll_type, dual_elig_m, agency_new, 
         race_c, hisp_c, gender_c, dob_h, dob_m, disability_h,
         start_housing, vouch_type_final, portfolio_final, unit_zip_h, 
         subsidy_type, operator_type, pt12:pt16)


#### Set up variables ####
### Age
# Use Medicaid DOB unless missing
pha_elig_sql <- pha_elig_sql %>%
  mutate(dob_c = as.Date(ifelse(is.na(dob_m), dob_h, dob_m), origin = "1970-01-01"))

pha_elig_sql <- pha_elig_sql %>%
  mutate(age12 = round(lubridate::interval(start = dob_c, end = ymd(20121231)) / years(1), 1),
         age13 = round(lubridate::interval(start = dob_c, end = ymd(20131231)) / years(1), 1),
         age14 = round(lubridate::interval(start = dob_c, end = ymd(20141231)) / years(1), 1),
         age15 = round(lubridate::interval(start = dob_c, end = ymd(20151231)) / years(1), 1),
         age16 = round(lubridate::interval(start = dob_c, end = ymd(20161231)) / years(1), 1),
         age17 = round(lubridate::interval(start = dob_c, end = ymd(20171231)) / years(1), 1)
  ) %>%
  # Remove negative ages
  mutate_at(vars(age12:age17), funs(ifelse(. < 0, 0.01, .)))


### Time in housing
pha_elig_sql <- pha_elig_sql %>%
  mutate(length12 = round(interval(start = start_housing, end = ymd(20121231)) / years(1), 1),
         length13 = round(interval(start = start_housing, end = ymd(20131231)) / years(1), 1),
         length14 = round(interval(start = start_housing, end = ymd(20141231)) / years(1), 1),
         length15 = round(interval(start = start_housing, end = ymd(20151231)) / years(1), 1),
         length16 = round(interval(start = start_housing, end = ymd(20161231)) / years(1), 1),
         length17 = round(interval(start = start_housing, end = ymd(20171231)) / years(1), 1)
  ) %>%
  # Remove negative ages
  mutate_at(vars(length12:length17), funs(ifelse(. < 0, NA, .)))


#### Make numerical recodes ####
# Function to recode ages
# Remember that the unquoted column needs to be in parentheses
agecode_f <- function(df, x) {
  col <- enquo(x)
  varname <- paste(quo_name(col), "num", sep = "_")
  df %>%
    mutate(!!varname := case_when(
      (!!col) < 18 ~ 11,
      between((!!col), 18, 24.99) ~ 12,
      between((!!col), 25, 44.99) ~ 13,
      between((!!col), 45, 61.99) ~ 14,
      between((!!col), 62, 64.99) ~ 15,
      (!!col) >= 65 ~ 16,
      is.na((!!col)) ~ 99
    )
    )
}

pha_elig_sql <- agecode_f(pha_elig_sql, age12)
pha_elig_sql <- agecode_f(pha_elig_sql, age13)
pha_elig_sql <- agecode_f(pha_elig_sql, age14)
pha_elig_sql <- agecode_f(pha_elig_sql, age15)
pha_elig_sql <- agecode_f(pha_elig_sql, age16)
pha_elig_sql <- agecode_f(pha_elig_sql, age17)


# Function to recode length of stay
lencode_f <- function(df, x) {
  col <- enquo(x)
  varname <- paste(quo_name(col), "num", sep = "_")
  df %>%
    mutate(!!varname := case_when(
      (!!col) < 3 ~ 81,
      between((!!col), 3, 5.99) ~ 82,
      (!!col) >= 6 ~ 83,
      is.na((!!col)) ~ 99
    )
    )
}

pha_elig_sql <- lencode_f(pha_elig_sql, length12)
pha_elig_sql <- lencode_f(pha_elig_sql, length13)
pha_elig_sql <- lencode_f(pha_elig_sql, length14)
pha_elig_sql <- lencode_f(pha_elig_sql, length15)
pha_elig_sql <- lencode_f(pha_elig_sql, length16)
pha_elig_sql <- lencode_f(pha_elig_sql, length17)


pha_elig_sql <- pha_elig_sql %>%
  mutate(
    enroll_type_num = case_when(
      enroll_type == "m" ~ 1,
      enroll_type == "b" ~ 2,
      enroll_type == "h" ~ 3
    ),
    dual_elig_num = case_when(
      dual_elig_m == "Y" ~ 11,
      dual_elig_m == "N" ~ 12,
      is.na(dual_elig_m) ~ 99
    ),
    ethn_num = case_when(
      hisp_c == 1 ~ 44,
      str_detect(race_c, "AIAN") ~ 41,
      str_detect(race_c, "Asian") ~ 42,
      str_detect(race_c, "Black") ~ 43,
      str_detect(race_c, "Multiple") ~ 45,
      str_detect(race_c, "NHPI") ~ 46,
      str_detect(race_c, "White") ~ 47,
      str_detect(race_c, "Other") ~ 48,
      is.null(race_c) ~ 99
      ),
    disab_num = case_when(
      disability_h == 1 ~ 71,
      disability_h == 0 ~ 72,
      is.na(disability_h) ~ 99
    ),
    agency_num = case_when(
      is.na(agency_new) ~ 0,
      agency_new == "KCHA" ~ 1,
      agency_new == "SHA" ~ 2
    ),
    subsidy_num = case_when(
      str_detect(subsidy_type, "HARD") ~ 21,
      str_detect(subsidy_type, "TENANT") ~ 22,
      is.na(subsidy_type) ~ 99
    ),
    operator_num = case_when(
      operator_type == "NON-PHA OPERATED" ~ 31,
      operator_type == "PHA OPERATED" ~ 32,
      operator_type == "" ~ 99,
      is.na(operator_type) ~ 99
    ),
    portfolio_num = case_when(
      portfolio_final == "BIRCH CREEK" ~ 51,
      portfolio_final == "FAMILY" ~ 52,
      portfolio_final == "GREENBRIDGE" ~ 53,
      portfolio_final == "HIGH POINT" ~ 54,
      str_detect(portfolio_final, "HIGHRISE") ~ 55,
      portfolio_final == "LAKE CITY COURT" ~ 56,
      portfolio_final == "MIXED" ~ 60,
      portfolio_final == "NEWHOLLY" ~ 57,
      portfolio_final == "RAINIER VISTA" ~ 58,
      str_detect(portfolio_final, "SCATTERED SITES") ~ 59,
      portfolio_final == "SENIOR" ~ 60,
      portfolio_final == "SENIOR HOUSING" ~ 61,
      portfolio_final == "SEOLA GARDENS" ~ 62,
      portfolio_final == "VALLI KEE" ~ 63,
      portfolio_final == "YESLER TERRACE" ~ 64,
      str_detect(portfolio_final, "OTHER") ~ 98,
      is.na(portfolio_final) ~ 99
    ),
    voucher_num = case_when(
      vouch_type_final == "AGENCY VOUCHER" ~ 91,
      vouch_type_final == "FUP" ~ 92,
      vouch_type_final == "GENERAL TENANT-BASED VOUCHER" ~ 93,
      vouch_type_final == "HASP" ~ 94,
      vouch_type_final == "MOD REHAB" ~ 95,
      vouch_type_final == "PARTNER PROJECT-BASED VOUCHER" ~ 96,
      vouch_type_final == "VASH" ~ 97,
      str_detect(vouch_type_final, "OTHER") ~ 98,
      vouch_type_final == "" ~ 99,
      is.na(vouch_type_final) ~ 99
    )
  )



#### Reorganize data ####
### Restrict to just the columns needed
pha_elig_sql <- pha_elig_sql %>%
  select(mid, pid2, startdate_c, enddate_c, enroll_type, dual_elig_m, agency_new, 
         age12:age17, length12:length17, race_c, hisp_c,
         gender_c, dob_c, disability_h, 
         vouch_type_final, portfolio_final, subsidy_type, operator_type,
         unit_zip_h,
         enroll_type_num, dual_elig_num, agency_num,
         age12_num:age17_num, length12_num:length17_num, ethn_num, 
         disab_num, voucher_num, portfolio_num, subsidy_num, operator_num,
         pt12:pt16) %>%
  # Need to remove some rows that only have 2017 data (as pt17 not curerently calculated)
  # Remove this filter once pt17 is made
  filter(!(is.na(pt12) & is.na(pt13) & is.na(pt14) & is.na(pt15) & is.na(pt16))) %>%
  distinct()

### Recode a few things that need to be fixed up
pha_elig_sql <- pha_elig_sql %>%
  mutate(ethn_num = ifelse(is.na(ethn_num), 48, ethn_num),
         portfolio_num = ifelse(is.na(portfolio_num), 99, portfolio_num)) %>%
  mutate_at(vars(age12_num, age13_num, age14_num, age15_num, age16_num, age17_num,
                 length12_num, length13_num, length14_num, length15_num,
                 length16_num, length17_num),
            funs(ifelse(is.na(.), 99, .)))


#### Load to SQL ####
# May need to delete table first
sqlDrop(db.apde51, "dbo.pha_mcaid_demogs")
ptm01 <- proc.time() # Times how long this query takes
sqlSave(db.apde51, pha_elig_sql, tablename = "dbo.pha_mcaid_demogs",
        varTypes = c(startdate_c = "Date",
                     enddate_c = "Date",
                     dob_c = "Date"))
proc.time() - ptm01

