###############################################################################
# Code to save data from the Seattle Housing Authority to SQL
#
# Alastair Matheson (PHSKC-APDE)
# 2017-05-17
###############################################################################

##### Set up global parameter and call in libraries #####
library(RODBC) # Used to connect to SQL server
library(openxlsx) # Used to import/export Excel files
library(car) # used to recode variables
library(dplyr) # Used to manipulate data
library(data.table) # more data manipulation
library(dtplyr) # lets dplyr and data.table play nicely together

path <- "//phdata01/DROF_DATA/DOH DATA/Housing/SHA"


##### Connect to the servers #####
db.apde51 <- odbcConnect("PH_APDEStore51")


##### Bring in data #####
# Compare YT files
sha3a_old <- read.csv(file = paste0(path, "/SuffixCorrected/3.a_HH PublicHousing 2012 to Current- (Yardi) 50058 Data_2016-05-11.csv"), stringsAsFactors = FALSE)
sha3b_old <- read.csv(file = paste0(path, "/SuffixCorrected/3.b_Income Assets PublicHousing 2012 to 2015- (Yardi) 50058 Data_2016-02-16.csv"), stringsAsFactors = FALSE)

sha3a_new <- read.csv(file = paste0(path, "/Original/3.a_HH PublicHousing 2012 to Current- (Yardi) 50058 Data_2017-03-31.csv"), stringsAsFactors = FALSE)
sha3b_new <- read.csv(file = paste0(path, "/Original/3.b_Income Assets PublicHousing 2012 to 2015- (Yardi) 50058 Data_2017-03-31.csv"), stringsAsFactors = FALSE)

sha5a_old <- read.csv(file = paste0(path, "/SuffixCorrected/5.a_HH HCV 2006 to Current- (Elite) 50058 Data_2016-06-08.csv"), stringsAsFactors = FALSE)
sha5b_old <- read.csv(file = paste0(path, "/SuffixCorrected/5.b_Income Assets HCV 2006 to Current- (Elite) 50058 Data_2016-06-08.csv"), stringsAsFactors = FALSE)

sha5a_new <- read.csv(file = paste0(path, "/Original/5.a_HH HCV 2006 to Current- (Elite) 50058 Data_2017-03-31.csv"), stringsAsFactors = FALSE)
sha5b_new <- read.csv(file = paste0(path, "/Original/5.b_Income Assets HCV 2006 to Current- (Elite) 50058 Data_2017-03-31.csv"), stringsAsFactors = FALSE)


# Bring in suffix corrected SHA data
sha1a <- read.csv(file = paste0(path, "/SuffixCorrected/1.a_HH PublicHousing 2004 to 2006 - (MLS) 50058 Data_2016-05-11.csv"), stringsAsFactors = FALSE)
sha1b <- read.csv(file = paste0(path, "/SuffixCorrected/1.b_Income PublicHousing 2004 to 2006 - (MLS) 50058 Data_2016-02-16.csv"), stringsAsFactors = FALSE)
sha1c <- read.csv(file = paste0(path, "/SuffixCorrected/1.c_Assets PublicHousing 2004 to 2006 - (MLS) 50058 Data_2016-02-16.csv"), stringsAsFactors = FALSE)
sha2a <- read.csv(file = paste0(path, "/SuffixCorrected/2.a_HH PublicHousing 2007 to 2012 -(MLS) 50058 Data_2016-05-11.csv"), stringsAsFactors = FALSE)
sha2b <- read.csv(file = paste0(path, "/SuffixCorrected/2.b_Income PublicHousing 2007 to 2012 - (MLS) 50058 Data_2016-02-16.csv"), stringsAsFactors = FALSE)
sha2c <- read.csv(file = paste0(path, "/SuffixCorrected/2.c_Assets PublicHousing 2007 to 2012 - (MLS) 50058 Data_2016-02-16.csv"), stringsAsFactors = FALSE)
sha4 <- read.csv(file = paste0(path, "/SuffixCorrected/4_HCV 2004 to 2006 - (MLS) 50058 Data_2016-05-25.csv"), stringsAsFactors = FALSE)

# Bring in voucher data
sha_vouch_type <- read.xlsx(paste0(path, "/Original/HCV Voucher Type_2017-05-15.xlsx"))
sha_vouch_increment <- read.xlsx(paste0(path, "/Original/Voucher Increments_2017-05-15.xlsx"))



##### Join data sets together #####

### First deduplicate data to avoid extra rows being made when joined
# Make list of data frames to deduplicate
dfs <- list(sha1a = sha1a, sha1b = sha1b, sha1c = sha1c, sha2a = sha2a, sha2b = sha2b, sha2c = sha2c, 
            sha3a_new = sha3a_new, sha3b_new = sha3b_new, sha4 = sha4, sha5a_new = sha5a_new, sha5b_new = sha5b_new,
            sha_vouch_type = sha_vouch_type, sha_vouch_increment = sha_vouch_increment)

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
fields <- read.xlsx("//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/Field name mapping.xlsx")

sha1a <- setnames(sha1a, fields$PHSKC[match(names(sha1a), fields$SHA_old)])
sha1b <- setnames(sha1b, fields$PHSKC[match(names(sha1b), fields$SHA_old)])
sha1c <- setnames(sha1c, fields$PHSKC[match(names(sha1c), fields$SHA_old)])
sha2a <- setnames(sha2a, fields$PHSKC[match(names(sha2a), fields$SHA_old)])
sha2b <- setnames(sha2b, fields$PHSKC[match(names(sha2b), fields$SHA_old)])
sha2c <- setnames(sha2c, fields$PHSKC[match(names(sha2c), fields$SHA_old)])
sha3a_new <- setnames(sha3a_new, fields$PHSKC[match(names(sha3a_new), fields$SHA_new_ph)])
sha3b_new <- setnames(sha3b_new, fields$PHSKC[match(names(sha3b_new), fields$SHA_new_ph)])


# Clean up mismatching variables
sha2a <- mutate(sha2a, ph_rent_ceiling = car::recode(ph_rent_ceiling, c("'Y' = 1; 'N' = 0; else = NA")))
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



# Append data
sha_ph <- bind_rows(sha1, sha2, sha3)

# Add program type
sha_ph <- mutate(sha_ph, prog_type = "PH")


#### Join HCV files

# Fix up names
sha4 <- setnames(sha4, fields$PHSKC[match(names(sha4), fields$SHA_old)])
sha5a_new <- setnames(sha5a_new, fields$PHSKC[match(names(sha5a_new), fields$SHA_new_hcv)])
sha5b_new <- setnames(sha5b_new, fields$PHSKC[match(names(sha5b_new), fields$SHA_new_hcv)])
sha_vouch_increment <- setnames(sha_vouch_increment, fields$PHSKC[match(names(sha_vouch_increment), fields$SHA_new_hcv)])
sha_vouch_type <- setnames(sha_vouch_type, fields$PHSKC[match(names(sha_vouch_type), fields$SHA_new_hcv)])

# Clean up mismatching variables
sha4 <- sha4 %>%
  mutate(mbr_num = as.numeric(ifelse(mbr_num == "NULL", NA, mbr_num)))

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
  mutate(
    act_type = car::recode(act_type, c("'Annual HQS Inspection Only' = 13; 'Annual Reexamination' = 2; 'Annual Reexamination Searching' = 9;
                                       'End Participation' = 6; 'Expiration of Voucher' = 11; 'FSS/WtW Addendum Only' = 8;
                                       'Historical Adjustment' = 14; 'Interim Reexamination' = 3; 'Issuance of Voucher' = 10;
                                       'New Admission' = 1; 'Other Change of Unit' = 7; 'Port-Out Update (Not Submitted To MTCS)' = 16;
                                       'Portability Move-in' = 4; 'Portability Move-out' = 5; 'Portablity Move-out' = 5; 'Void' = 15;
                                       else = NA"))
  )

sha_vouch_increment <- select(sha_vouch_increment, -(drop_row_num_drop))

# Join with income and asset files
sha4 <- left_join(sha4, sha1b, by = c("incasset_id", "mbr_num" = "inc_mbr_num"))
sha4 <- left_join(sha4, sha1c, by = c("incasset_id"))

sha5 <- left_join(sha5a_new, sha5b_new, by = c("cert_id", "mbr_id"))
sha5 <- left_join(sha5, sha_vouch_type, by = c("cert_id", "hh_id", "mbr_id", "act_type", "act_date"))
sha5 <- left_join(sha5, sha_vouch_increment, by = c("increment"))

# Append data
sha_hcv <- bind_rows(sha4, sha5)


# Add program type
sha_hcv <- mutate(sha_hcv, prog_type = "HCV")


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
  mutate_at(vars(act_date, admit_date, dob),
            funs(as.Date(., format = "%m/%d/%Y"))) %>%
  mutate_at(vars(bdrm_voucher, rent_subs),
            funs(as.numeric(car::recode(., "'Y' = 1; 'N' = 0; 'N/A' = NA; 'SRO' = NA; 'NULL' = NA"))))


##### Load to SQL server #####
# May need to delete table first
sqlDrop(db.apde51, "dbo.sha_combined_raw")
sqlSave(db.apde51, sha, tablename = "dbo.sha_combined_raw",
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
rm(dfs)
rm(df_dedups)
gc()

