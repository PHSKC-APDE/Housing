###############################################################################
# OVERVIEW:
# Code to create a cleaned person table from the combined
# King County Housing Authority and Seattle Housing Authority data sets
# Aim is to have a single row per contiguous time in a house per person
#
# STEPS:
# Process raw KCHA data and load to SQL database
# Process raw SHA data and load to SQL database ### (THIS CODE) ###
# Bring in individual PHA datasets and combine into a single file
# Deduplicate data and tidy up via matching process
# Recode race and other demographics
# Clean up addresses and geocode
# Consolidate data rows
# Add in final data elements and set up analyses
# Join with Medicaid eligibility data and set up analyses
#
# Alastair Matheson (PHSKC-APDE)
# alastair.matheson@kingcounty.gov
# 2017-05-17, split into separate files 2017-10
#
###############################################################################

#### Set up global parameter and call in libraries ####
rm(list=ls()) #reset
options(max.print = 350, tibble.print_max = 50, scipen = 999)

library(housing) # contains many useful functions for cleaning
library(RODBC) # Used to connect to SQL server
library(openxlsx) # Used to import/export Excel files
library(stringr) # Used to manipulate string data
library(data.table)
library(dplyr) # Used to manipulate data

housing_path <- "~/data"
sha_path <- file.path(housing_path,"SHA")
db.apde51 <- odbcConnect("PH_APDEStore51")


#### Bring in data ####
sha3a_new <- fread(file = file.path(sha_path, "3.a_HH PublicHousing 2012 to Current- (Yardi) 50058 Data_2017-03-31.csv"), stringsAsFactors = FALSE)
sha3b_new <- fread(file = file.path(sha_path, "3.b_Income Assets PublicHousing 2012 to 2015- (Yardi) 50058 Data_2017-03-31.csv"), stringsAsFactors = FALSE)
sha5a_new <- fread(file = file.path(sha_path, "5.a_HH HCV 2006 to Current- (Elite) 50058 Data_2017-03-31.csv"), stringsAsFactors = FALSE)
sha5b_new <- fread(file = file.path(sha_path, "5.b_Income Assets HCV 2006 to Current- (Elite) 50058 Data_2017-03-31.csv"), stringsAsFactors = FALSE)

# Bring in suffix corrected SHA data
sha1a <- fread(file = file.path(sha_path, "1.a_HH PublicHousing 2004 to 2006 - (MLS) 50058 Data_2016-05-11.csv"), stringsAsFactors = FALSE)
sha1b <- fread(file = file.path(sha_path, "1.b_Income PublicHousing 2004 to 2006 - (MLS) 50058 Data_2016-02-16.csv"), stringsAsFactors = FALSE)
sha1c <- fread(file = file.path(sha_path, "1.c_Assets PublicHousing 2004 to 2006 - (MLS) 50058 Data_2016-02-16.csv"), stringsAsFactors = FALSE)
sha2a <- fread(file = file.path(sha_path, "2.a_HH PublicHousing 2007 to 2012 -(MLS) 50058 Data_2016-05-11.csv"), stringsAsFactors = FALSE)
sha2b <- fread(file = file.path(sha_path, "2.b_Income PublicHousing 2007 to 2012 - (MLS) 50058 Data_2016-02-16.csv"), stringsAsFactors = FALSE)
sha2c <- fread(file = file.path(sha_path, "2.c_Assets PublicHousing 2007 to 2012 - (MLS) 50058 Data_2016-02-16.csv"), stringsAsFactors = FALSE)
sha4a <- fread(file = file.path(sha_path, "4_HCV 2004 to 2006 - (MLS) 50058 Data_2016-05-25.csv"), stringsAsFactors = FALSE)

# Bring in voucher data
sha_vouch_type <- read.xlsx(file.path(sha_path, "HCV Voucher Type_2017-05-15.xlsx"))
sha_prog_codes <- read.xlsx(file.path(sha_path, "Program codes and portfolios_2017-11-02.xlsx"), 2)

# Bring in portfolio codes
sha_portfolio_codes  <- read.xlsx(file.path(sha_path, "Program codes and portfolios_2017-11-02.xlsx"), 1)


#### Join data sets together ####

### First deduplicate data to avoid extra rows being made when joined
# Make list of data frames to deduplicate
dfs <- list(sha1a = sha1a, sha1b = sha1b, sha1c = sha1c, sha2a = sha2a, sha2b = sha2b, sha2c = sha2c,
            sha3a_new = sha3a_new, sha3b_new = sha3b_new, sha4a = sha4a, sha5a_new = sha5a_new, sha5b_new = sha5b_new,
            sha_vouch_type = sha_vouch_type, sha_prog_codes = sha_prog_codes, sha_portfolio_codes = sha_portfolio_codes)

# Deduplicate data
df_dedups <- lapply(dfs, function(data) {
  data <- data %>% distinct()
  return(data)
  })

# Bring back data frames from list
list2env(df_dedups, .GlobalEnv)


#### Join PH files ####
# Get field names to match
# Bring in variable name mapping table
fields <- read.xlsx("~/data/OrganizedData/Field name mapping.xlsx")

sha1a <- data.table::setnames(sha1a, fields$PHSKC[match(names(sha1a), fields$SHA_old)])
sha1b <- data.table::setnames(sha1b, fields$PHSKC[match(names(sha1b), fields$SHA_old)])
sha1c <- data.table::setnames(sha1c, fields$PHSKC[match(names(sha1c), fields$SHA_old)])
sha2a <- data.table::setnames(sha2a, fields$PHSKC[match(names(sha2a), fields$SHA_old)])
sha2b <- data.table::setnames(sha2b, fields$PHSKC[match(names(sha2b), fields$SHA_old)])
sha2c <- data.table::setnames(sha2c, fields$PHSKC[match(names(sha2c), fields$SHA_old)])
sha3a_new <- data.table::setnames(sha3a_new, fields$PHSKC[match(names(sha3a_new), fields$SHA_new_ph)])
sha3b_new <- data.table::setnames(sha3b_new, fields$PHSKC[match(names(sha3b_new), fields$SHA_new_ph)])
sha_portfolio_codes <- data.table::setnames(sha_portfolio_codes, fields$PHSKC[match(names(sha_portfolio_codes), fields$SHA_prog_port_codes)])


# Clean up mismatching variables
sha2a <- yesno_f(sha2a, ph_rent_ceiling)
sha2a <- mutate(sha2a, fhh_ssn = as.character(fhh_ssn))
sha3a_new <- sha3a_new %>%
  mutate(property_id = as.character(property_id),
         act_type = as.numeric(ifelse(act_type == "E", 3, act_type)),
         mbr_num = as.numeric(ifelse(mbr_num == "NULL", NA, mbr_num)),
         r_hisp = as.numeric(ifelse(r_hisp == "NULL", NA, r_hisp))
  )


# Join household, income, and asset tables
sha1 <- left_join(sha1a, sha1b, by = c("incasset_id", "mbr_num" = "inc_mbr_num"))
sha1 <- left_join(sha1, sha1c, by = c("incasset_id"))

sha2 <- left_join(sha2a, sha2b, by = c("incasset_id", "mbr_num" = "inc_mbr_num"))
sha2 <- left_join(sha2, sha2c, by = c("incasset_id"))

sha3 <- left_join(sha3a_new, sha3b_new, by = c("incasset_id", "mbr_num" = "inc_mbr_num"))


# Add source field to track where each row came from
sha1 <- sha1 %>% mutate(sha_source = "sha1")
sha2 <- sha2 %>% mutate(sha_source = "sha2")
sha3 <- sha3 %>% mutate(sha_source = "sha3")

# Append data
sha_ph <- bind_rows(sha1, sha2, sha3)

# Fix more formats
sha_ph <- sha_ph %>%
  mutate(property_id = ifelse(as.numeric(property_id) < 10 & !is.na(as.numeric(property_id)), paste0("00", property_id),
                              ifelse(as.numeric(property_id) >= 10 & as.numeric(property_id) < 100 & !is.na(as.numeric(property_id)),
                                     paste0("0", property_id),
                              property_id)))

# Join with portfolio data
sha_ph <- left_join(sha_ph, sha_portfolio_codes, by = c("property_id"))

# Rename specific portfolio
sha_ph <- mutate(sha_ph,
                 portfolio = ifelse(str_detect(portfolio, "Lake City Court"),
                                    "Lake City Court", portfolio))


#### Join HCV files
# Fix up names
sha4a <- data.table::setnames(sha4a, fields$PHSKC[match(names(sha4a), fields$SHA_old)])
sha5a_new <- data.table::setnames(sha5a_new, fields$PHSKC[match(names(sha5a_new), fields$SHA_new_hcv)])
sha5b_new <- data.table::setnames(sha5b_new, fields$PHSKC[match(names(sha5b_new), fields$SHA_new_hcv)])
sha_vouch_type <- data.table::setnames(sha_vouch_type, fields$PHSKC[match(names(sha_vouch_type), fields$SHA_new_hcv)])
sha_prog_codes <- data.table::setnames(sha_prog_codes, fields$PHSKC[match(names(sha_prog_codes), fields$SHA_prog_port_codes)])


# Clean up mismatching variables
sha4a <- sha4a %>%
  mutate(mbr_num = as.numeric(ifelse(mbr_num == "NULL", NA, mbr_num)),
         # Truncate increment numbers so they match the reference list when joined
         increment_old = increment,
         increment = str_sub(increment, 1, 5)
         )


sha5a_new <- sha5a_new %>%
  mutate(
    act_type = car::recode(act_type, c("'Annual HQS Inspection Only' = 13; 'Annual Reexamination' = 2; 'Annual Reexamination Searching' = 9;
                                       'End Participation' = 6; 'Expiration of Voucher' = 11; 'FSS/WtW Addendum Only' = 8;
                                       'Historical Adjustment' = 14; 'Interim Reexamination' = 3; 'Issuance of Voucher' = 10;
                                       'New Admission' = 1; 'Other Change of Unit' = 7; 'Port-Out Update (Not Submitted To MTCS)' = 16;
                                       'Portability Move-in' = 4; 'Portability Move-out' = 5; 'Portablity Move-out' = 5; 'Void' = 15;
                                       else = NA"))
    ) %>%
  mutate_at(vars(unit_zip, bed_cnt, cost_month, rent_gross, rent_tenant_owner, rent_mixfam_owner),
            funs(as.numeric(ifelse(. == "NULL", NA, .))))

sha5b_new <- sha5b_new %>%
  mutate_at(vars(inc_year, inc_excl, inc_fin, antic_inc, asset_val), funs(as.numeric(ifelse(. == "NULL", NA, .))))


sha_vouch_type <- sha_vouch_type %>%
  mutate(act_type = car::recode(act_type, c("'Annual HQS Inspection Only' = 13; 'Annual Reexamination' = 2; 'Annual Reexamination Searching' = 9;
                                       'End Participation' = 6; 'Expiration of Voucher' = 11; 'FSS/WtW Addendum Only' = 8;
                                       'Historical Adjustment' = 14; 'Interim Reexamination' = 3; 'Issuance of Voucher' = 10;
                                       'New Admission' = 1; 'Other Change of Unit' = 7; 'Port-Out Update (Not Submitted To MTCS)' = 16;
                                       'Portability Move-in' = 4; 'Portability Move-out' = 5; 'Portablity Move-out' = 5; 'Void' = 15;
                                       else = NA")))


# Join with income and asset files
sha4 <- left_join(sha4a, sha1b, by = c("incasset_id", "mbr_num" = "inc_mbr_num"))
sha4 <- left_join(sha4, sha1c, by = c("incasset_id"))
sha4 <- left_join(sha4, sha_prog_codes, by = c("increment"))

sha5 <- left_join(sha5a_new, sha5b_new, by = c("cert_id", "mbr_id"))
sha5 <- left_join(sha5, sha_vouch_type, by = c("cert_id", "hh_id", "mbr_id", "act_type", "act_date"))
sha5 <- left_join(sha5, sha_prog_codes, by = c("increment"))

# Add source field to track where each row came from
sha4 <- sha4 %>% mutate(sha_source = "sha4")
sha5 <- sha5 %>% mutate(sha_source = "sha5")

# Append data
sha_hcv <- bind_rows(sha4, sha5)


### Join PH and HCV combined files
# Clean up mismatching variables
sha_hcv <- sha_hcv %>%
  mutate_at(vars(rent_tenant, rent_mixfam, ph_util_allow, ph_rent_ceiling, mbr_num, r_hisp),
            funs(as.numeric(ifelse(. == "NULL" | . == "N/A", NA, .)))) %>%
  mutate(tb_rent_ceiling = car::recode(ph_rent_ceiling, c("'Yes' = 1; 'No' = 0; else = NA")))

# Append data
sha <- bind_rows(sha_ph, sha_hcv)


### Fix up a few more format issues
sha <- sha %>%
  mutate_at(vars(act_date, admit_date, dob), funs(as.Date(., format = "%m/%d/%Y")))
sha <- yesno_f(sha, bdrm_voucher, rent_subs, disability)

# Set up mbr_num head of households (will be important later when cleaning up names)
sha <- sha %>% mutate(mbr_num = ifelse(is.na(mbr_num) & ssn == hh_ssn & lname == hh_lname & fname == hh_fname,
                        1, mbr_num))


### Tidy up income fields and consolidate
sha <- sha %>%
  mutate(inc_code = car::recode(inc_code, "'Annual imputed welfare income' = 'IW'; 'Child Support' = 'C';
                                'Federal Wage' = 'F'; 'General Assistance' = 'G'; 'Indian Trust/Per Capita' = 'I';
                                'Medical reimbursement' = 'E'; 'Military Pay' = 'M'; 'MTW Income' = 'X';
                                'NULL' = NA; 'Other NonWage Sources' = 'N'; 'Other Wage' = 'W'; 'Own Business' = 'B'; 'Pension' = 'P';
                                'PHA Wage' = 'HA'; 'Social Security' = 'SS'; 'SSI' = 'S'; 'TANF (formerly AFDC)' = 'T';
                                'Unemployment Benefits' = 'U'; '' = NA"),
         inc_fixed_temp = ifelse(inc_code %in% c("P", "PE", "Pension", "S", "SS", "SSI", "Social Security"), 1, 0))

# We are only interested in whether or not all income comes from a fixed source so taking the minimum tells us this
sha <- sha %>% group_by(ssn, lname, fname, dob, act_date) %>%
  mutate(inc_fixed = min(inc_fixed_temp, na.rm = T)) %>%
  ungroup() %>%
  select(-inc_fixed_temp)

# Restrict to relevant fields
# (can drop specific income and asset fields once fixed income flag is made)
sha <- sha %>%
  select(-inc_code, -inc_year, -inc_excl, -inc_fin, -inc_fin_tot,
         -inc_tot, -inc_adj, -inc_deduct, -inc_mbr_num, -incasset_id,
         -asset_type, -asset_val, -antic_inc,
         -antic_inc_tot, -asset_impute, -asset_final, -asset_tot) %>%
  distinct()


### Transfer over data to rows with missing programs and vouchers
# (not all rows were joined earlier and it is easier to clean up at this point once duplicate rows are deleted)
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



##### Load to SQL server #####
# May need to delete table first
sqlDrop(db.apde51, "dbo.sha_combined")
sqlSave(db.apde51, sha, tablename = "dbo.sha_combined",
        varTypes = c(
          act_date = "Date",
          admit_date = "Date",
          dob = "Date"
        ))


##### Remove temporary files #####
rm(list = ls(pattern = "sha1"))
rm(list = ls(pattern = "sha2"))
rm(list = ls(pattern = "sha3"))
rm(list = ls(pattern = "sha4"))
rm(list = ls(pattern = "sha5"))
rm(list = ls(pattern = "sha_"))
rm(dfs)
rm(df_dedups)
rm(fields)
gc()

