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
gc()
options(tibble.print_max = 50, scipen = 999, width = 150)
  # width adjusts terminal size output, change as needed

library(colorout) # for colorizing output in Mac terminal devtools::install_github("jalvesaq/colorout")
library(housing) # contains many useful functions for cleaning
library(RODBC) # Used to connect to SQL servercopy
library(openxlsx) # Used to import/export Excel files
library(stringr) # Used to manipulate string data
library(data.table)
library(tidyverse) # Used to manipulate data

housing_path <- "~/data"
sha_path <- file.path(housing_path,"SHA")
# db.apde51 <- odbcConnect("PH_APDEStore51")

#### Bring in data ####
sha3a_new <- fread(file = file.path(sha_path, "3.a_HH PublicHousing 2012 to Current- (Yardi) 50058 Data_2017-03-31.csv"), header=T,strip.white=T,na.strings= c("NA", " ", ""),stringsAsFactors=F)
sha3b_new <- fread(file = file.path(sha_path, "3.b_Income Assets PublicHousing 2012 to 2015- (Yardi) 50058 Data_2017-03-31.csv"), header=T,strip.white=T,na.strings= c("NA", " ", ""),stringsAsFactors=F)
sha5a_new <- fread(file = file.path(sha_path, "5.a_HH HCV 2006 to Current- (Elite) 50058 Data_2017-03-31.csv"), header=T,strip.white=T,na.strings= c("NA", " ", ""),stringsAsFactors=F)
sha5b_new <- fread(file = file.path(sha_path, "5.b_Income Assets HCV 2006 to Current- (Elite) 50058 Data_2017-03-31.csv"), header=T,strip.white=T,na.strings= c("NA", " ", ""),stringsAsFactors=F)

# Bring in suffix corrected SHA data
sha1a <- fread(file = file.path(sha_path, "1.a_HH PublicHousing 2004 to 2006 - (MLS) 50058 Data_2016-05-11.csv"), header=T,strip.white=T,na.strings= c("NA", " ", ""),stringsAsFactors=F)
sha1b <- fread(file = file.path(sha_path, "1.b_Income PublicHousing 2004 to 2006 - (MLS) 50058 Data_2016-02-16.csv"), header=T,strip.white=T,na.strings= c("NA", " ", ""),stringsAsFactors=F)
sha1c <- fread(file = file.path(sha_path, "1.c_Assets PublicHousing 2004 to 2006 - (MLS) 50058 Data_2016-02-16.csv"), header=T,strip.white=T,na.strings= c("NA", " ", ""),stringsAsFactors=F)
sha2a <- fread(file = file.path(sha_path, "2.a_HH PublicHousing 2007 to 2012 -(MLS) 50058 Data_2016-05-11.csv"), header=T,strip.white=T,na.strings= c("NA", " ", ""),stringsAsFactors=F)
sha2b <- fread(file = file.path(sha_path, "2.b_Income PublicHousing 2007 to 2012 - (MLS) 50058 Data_2016-02-16.csv"), header=T,strip.white=T,na.strings= c("NA", " ", ""),stringsAsFactors=F)
sha2c <- fread(file = file.path(sha_path, "2.c_Assets PublicHousing 2007 to 2012 - (MLS) 50058 Data_2016-02-16.csv"), header=T,strip.white=T,na.strings= c("NA", " ", ""),stringsAsFactors=F)
sha4a <- fread(file = file.path(sha_path, "4_HCV 2004 to 2006 - (MLS) 50058 Data_2016-05-25.csv"), header=T,strip.white=T,na.strings= c("NA", " ", ""),stringsAsFactors=F)

# Bring in voucher data
sha_vouch_type <- read.xlsx(file.path(sha_path, "HCV Voucher Type.xlsx"))

sha_prog_codes <- read.xlsx(file.path(sha_path, "Program Codes & Portfolios_November_Updates.xlsx"), 2)

# Bring in portfolio codes
sha_portfolio_codes  <- read.xlsx(file.path(sha_path, "Program Codes & Portfolios_November_Updates.xlsx"), 1)


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
fields <- read.xlsx(file.path(sha_path, "Field name mapping.xlsx"),1)

# Clean excel fields
fields <- fields %>%
        mutate_at(vars(SHA_old:SHA_new_ph), funs(gsub("\\.\\.\\."," - ",.))) %>%
        mutate_at(vars(SHA_old:SHA_new_ph), funs(gsub("\\."," ",.)))

fields <- fields %>% mutate(
    SHA_old=
    ifelse(grepl("Last Name - 3b  Head", SHA_old), "Last Name - 3b (Head)",
    ifelse(grepl("First Name - 3c  Head", SHA_old), "First Name - 3c (Head)",
    ifelse(grepl("Middle Initial - 3d  Head", SHA_old), "Middle Initial - 3d (Head)",
    ifelse(grepl("Unit Address Number and Street -  5a", SHA_old), "Unit Address(Number and Street) - 5a",
    ifelse(grepl("Utility Allowance monthly allowances - 20d", SHA_old), "Utility Allowance/monthly allowances - 20d",
    ifelse(grepl("Ceiling Rent Indicator  21q", SHA_old), "Ceiling Rent Indicator- 21q",
    ifelse(grepl("Race black african american indicator - 3k2", SHA_old), "Race black/african american indicator - 3k2",
    ifelse(grepl("Race american indian alaska native indicator - 3k3", SHA_old),"Race american indian/alaska native indicator - 3k3",
    ifelse(grepl("Race native hawaiin other pacific islander indicator - 3k5", SHA_old), "Race native hawaiin/other pacific islander indicator - 3k5",
    ifelse(grepl("Flat Subsidy or Inc  based sub - 21a", SHA_old), "Flat Subsidy or Inc. based sub - 21a",
    ifelse(grepl("Utility Allowance monthly allowances - 21j", SHA_old), "Utility Allowance/monthly allowances - 21j",
    ifelse(grepl("Projected Effective Date of Next Re Exam - 2i", SHA_old), "Projected Effective Date of Next Re-Exam - 2i",
    ifelse(grepl("Number of Household Members - 3t  Head ", SHA_old), "Number of Household Members - 3t (Head)",
    	SHA_old))))))))))))),
    SHA_new_ph=
    ifelse(grepl("Projected Effective Date of Next Re Exam - 2i", SHA_new_ph), "Projected Effective Date of Next Re-Exam - 2i",
    ifelse(grepl("Unit Address Number and Street -  5a", SHA_new_ph), "Unit Address(Number and Street) - 5a",
    ifelse(grepl("Utility Allowance monthly allowances - 20d", SHA_new_ph), "Utility Allowance/monthly allowances - 20d",
    ifelse(grepl("Ceiling Rent Indicator  21q", SHA_new_ph), "Ceiling Rent Indicator- 21q",
    ifelse(grepl("Race black african american indicator - 3k2", SHA_new_ph), "Race black/african american indicator - 3k2",
    ifelse(grepl("Race american indian alaska native indicator - 3k3", SHA_new_ph), "Race american indian/alaska native indicator - 3k3",
    ifelse(grepl("Race native hawaiin other pacific islander indicator - 3k5", SHA_new_ph), "Race native hawaiin/other pacific islander indicator - 3k5",
    ifelse(grepl("Utility Allowance monthly allowances - 21j", SHA_new_ph), "Utility Allowance/monthly allowances - 21j",
    ifelse(grepl("Flat Subsidy or Inc  based sub - 21a", SHA_new_ph), "Flat Subsidy or Inc. based sub - 21a",
    	SHA_new_ph)))))))))
    )

fields %>% select(SHA_old, SHA_new_ph, PHSKC)

#
# Change names
#

# Change names of variables in SHA
sha1a <- setnames(sha1a, fields$PHSKC[match(names(sha1a), fields$SHA_old)])
sha1b <- setnames(sha1b, fields$PHSKC[match(names(sha1b), fields$SHA_old)])
sha1c <- setnames(sha1c, fields$PHSKC[match(names(sha1c), fields$SHA_old)])
sha2a <- setnames(sha2a, fields$PHSKC[match(names(sha2a), fields$SHA_old)])
sha2b <- setnames(sha2b, fields$PHSKC[match(names(sha2b), fields$SHA_old)])
sha2c <- setnames(sha2c, fields$PHSKC[match(names(sha2c), fields$SHA_old)])
sha3a_new <- setnames(sha3a_new, fields$PHSKC[match(names(sha3a_new), fields$SHA_new_ph)])
sha3b_new <- setnames(sha3b_new, fields$PHSKC[match(names(sha3b_new), fields$SHA_new_ph)])
sha_portfolio_codes <- setnames(sha_portfolio_codes, fields$PHSKC[match(names(sha_portfolio_codes), fields$SHA_prog_port_codes)])

# Clean up mismatching variables
sha2a <- yesno_f(sha2a, ph_rent_ceiling)
sha2a <- mutate(sha2a, fhh_ssn = as.character(fhh_ssn))
sha3a_new <- sha3a_new %>%
  mutate(property_id = as.character(property_id),
         act_type = as.numeric(ifelse(act_type == "E", 3, act_type)),
         mbr_num = as.numeric(ifelse(mbr_num == "NULL", NA, mbr_num)),
         r_hisp = as.numeric(ifelse(r_hisp == "NULL", NA, r_hisp))
  )

### Clean shifted columns ###

# rename v9 to "Anticipated income - 18e"
	sha5b_new <- sha5b_new %>% rename(antic_inc=v9)

# Add suffix columns to sha1a
	sha1a.fix <- sha1a %>%
	  	filter(V56!="") %>%
	  	mutate(v57="", hh_lnamesuf=hh_fname, lnamesuf=mname) %>%
	  	select(-hh_fname, -mname)
	names(sha1a.fix) <- names(sha1a)
	sha1a.fix <- sha1a.fix %>%
		select(incasset_id:hh_lname,hh_lnamesuf=56,hh_fname:lname,lnamesuf=57, fname:55)

	sha1a.good <-
	  sha1a %>% filter(V56=="") %>%
	  			mutate(hh_lnamesuf="", lnamesuf="") %>%
	  			select(incasset_id:hh_lname,hh_lnamesuf,hh_fname:lname,lnamesuf, fname:fhh_ssn, -V56)

	sha1a <- rbind(sha1a.good,sha1a.fix) %>%
			 mutate(mbr_num=as.integer(mbr_num))

# Combine rogue names sha1c
	sha1c.good <- sha1c %>%
		filter(is.na(V5)) %>%
		select(-V5) %>%
		mutate(asset_val=as.numeric(asset_val))
	sha1c.fix <- sha1c %>%
		filter(!is.na(V5)) %>%
		mutate(asset_type=paste(asset_type,asset_val,sep=" ")) %>%
		select(-3)
	names(sha1c.fix) <- names(sha1c.good)

	sha1c <- rbind(sha1c.good,sha1c.fix)

# Add suffix columns sha2a
	sha2a.good <- sha2a %>%
		filter(V57=="") %>%
		rename(hh_lnamesuf=V57, lnamesuf=V58) %>%
		mutate(lnamesuf="") %>%
		select(incasset_id:hh_lname,hh_lnamesuf,hh_fname:lname, lnamesuf,fname:fhh_ssn)

	sha2a.fix1 <-
		sha2a %>%
		filter(V57!="", is.na(V58)) %>%
		mutate(lnamesuf=fname, hh_lnamesuf="") %>%
		select(incasset_id:hh_lname, hh_lnamesuf, hh_fname:lname, lnamesuf, mname:V57)
	names(sha2a.fix1) <- names(sha2a.good)

	sha2a.fix2 <-
		sha2a %>% filter(!is.na(V58)) %>%
		mutate(lnamesuf=mname, hh_lnamesuf=hh_fname) %>%
		select(incasset_id:hh_lname, hh_lnamesuf, hh_mname:fname, lnamesuf, dob:V58)
	names(sha2a.fix2) <- names(sha2a.good)

	sha2a <- rbind(sha2a.good, sha2a.fix1, sha2a.fix2) %>%
			 mutate(mbr_num=as.integer(mbr_num), ph_rent_ceiling=ifelse(ph_rent_ceiling=="N", NA, ph_rent_ceiling)) %>%
			 mutate(ph_rent_ceiling=as.integer(ph_rent_ceiling))

# Combine rogue names sha2c
	sha2c.good <-
		sha2c %>%
		filter(is.na(V5)) %>%
		select(-V5)

	sha2c.fix <-
		sha2c %>%
		filter(!is.na(V5)) %>%
		mutate(asset_type=paste(asset_type,asset_val,sep=" ")) %>%
		select(-asset_val)
	names(sha2c.fix) <- names(sha2c.good)

	sha2c <- rbind(sha2c.good,sha2c.fix)


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
# ==========================================================================
# Change above field types
# ==========================================================================
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
sha5b_new <- data.table::setnames(sha5b_new, fields$PHSKC[match(names(sha5b_new),
# ==========================================================================
# TT BEGIN FIX: CHECK HERE ABOUT v9 renaming to "Anticipated income - 18e"
# ==========================================================================
	fields$SHA_new_hcv)]) %>% rename(antic_inc=v9)

# ==========================================================================
# TT END FIX
# ==========================================================================

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

#### Fix up SHA member numbers and head-of-household info ####
# ISSUE 1: Some households seem to have multiple HoHs recorded
# (hhold defined as the same address, action date, and PHA-generated hhold IDs)
# FIX 1: Overwrite HoH data to match mbr_num = 1
# ISSUE 2: The listed HoH isn't always member #1
# FIX 2: Switch member numbers around to make HoH member #1
# ISSUE 3: Not all households have member numbers or are missing #1
# FIX 3: Make sure the HoH has member number = 1

### First set up temporary household ID that should be unique to a household and action date
sha$hhold_id_temp <- group_indices(sha, hh_id, prog_type, unit_add, unit_city, act_date, act_type, incasset_id)

#### FIX 1: Deal with households that have multiple HoHs listed ####
# Check for households with >1 people listed as HoH
multi_hoh <- sha %>%
  group_by(hhold_id_temp) %>%
  summarise(people = n_distinct(hh_ssn, hh_lname, hh_lnamesuf, hh_fname, hh_mname)) %>%
  ungroup() %>%
  filter(people > 1) %>%
  mutate(rowcheck = row_number())

# Join to main data, restrict to member #1
multi_hoh_join <- left_join(multi_hoh, sha, by = "hhold_id_temp") %>%
  filter(mbr_num == 1) %>%
  select(rowcheck, hhold_id_temp, hh_ssn, hh_lname, hh_lnamesuf, hh_fname, hh_mname) %>%
  distinct()

# Add back to main data and bring over data into new columns
sha <- left_join(sha, multi_hoh_join, by = "hhold_id_temp") %>%
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
#   distinct(hhold_id_temp)
#
# # Bring in other housheold members
# wrong_hoh_join <- left_join(wrong_hoh, pha_clean, by = "hhold_id_temp") %>%
#   select(hhold_id_temp, ssn_new, lname_new, fname_new, mbr_num,
#          hh_ssn_new, hh_lname, hh_fname, hh_dob) %>%
#   arrange(hhold_id_temp, mbr_num) %>%
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
  group_by(hhold_id_temp) %>%
  summarise(mbr_num_min = min(mbr_num)) %>%
  ungroup()


# Join with full list of temporary HH IDs to find which ones are missing member numbers
mbr_miss <- anti_join(sha, min_mbr, by = "hhold_id_temp") %>%
  select(hhold_id_temp, act_date, ssn, lname, fname, mname, lnamesuf, dob,
         mbr_num, hh_ssn, hh_lname, hh_fname, hh_mname, hh_lnamesuf) %>%
  arrange(hhold_id_temp, ssn, lname, fname)

# Find the HoH and label them as member #1
mbr_miss <- mbr_miss %>%
  # Try matching on SSN
  mutate(mbr_num = ifelse(ssn == hh_ssn, 1, mbr_num)) %>%
  group_by(hhold_id_temp) %>%
  mutate(done = max(mbr_num, na.rm = T)) %>%
  ungroup() %>%
  # Then try name combos
  mutate(mbr_num = ifelse(is.infinite(done) & lname == hh_lname & fname == hh_fname, 1, mbr_num)) %>%
  select(-done)

# If multiple people were flagged as #1, take the oldest
# Common when there are children and parents with the same name or DOB typos
# If same DOB, take row with middle inital, then last name suffix
# If still a clash, take newer SHA data
mbr_miss <- mbr_miss %>%
  arrange(hhold_id_temp, mbr_num, dob, hh_mname, hh_lnamesuf) %>%
  group_by(hhold_id_temp) %>%
  mutate(mbr_num = ifelse(row_number() > 1, NA, mbr_num)) %>%
  ungroup()


# Restrict to the newly identified HoHs and join back to main data
mbr_miss_join <- mbr_miss %>%
  filter(mbr_num == 1) %>%
  distinct(hhold_id_temp, act_date, ssn, lname, fname, mname, lnamesuf, dob, mbr_num)
sha <- left_join(sha, mbr_miss_join,
                       by = c("hhold_id_temp", "act_date", "ssn",
                              "lname", "fname", "mname", "lnamesuf", "dob"))

# Bring over older member numbers and clean up columns
sha <- sha %>%
  mutate(mbr_num = ifelse(!is.na(mbr_num.y), mbr_num.y, mbr_num.x)) %>%
  select(-mbr_num.x, -mbr_num.y, -hhold_id_temp, -rowcheck)

rm(min_mbr)
rm(mbr_miss)
rm(mbr_miss_join)

#### END SHA HEAD OF HOUSEHOLD FIX ####

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
