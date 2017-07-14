###############################################################################
# Code to reshape the King County Housing Authority data
# Aim is to have a single row per person rather than household
#
# Alastair Matheson (PHSKC-APDE)
# alastair.matheson@kingcounty.gov
# 2016-04-07
###############################################################################


##### Set up global parameter and call in libraries #####
options(max.print = 350, tibble.print_max = 30, scipen = 999)

library(RODBC) # Used to connect to SQL server
library(openxlsx) # Used to import/export Excel files
library(stringr) # Used to manipulate string data
library(lubridate) # Used to manipulate dates
library(dplyr) # Used to manipulate data
library(data.table) # more data manipulation
library(tidyr) # used to reorganize and reshape data


##### Connect to the servers #####
db.apde51 <- odbcConnect("PH_APDEStore51")

##### Bring in raw, joined KCHA data #####
ptm01 <- proc.time() # Times how long this query takes (~63 secs)
kcha <- sqlQuery(
  db.apde51,
  "SELECT *
  FROM dbo.kcha_combined_raw",
  stringsAsFactors = FALSE)
proc.time() - ptm01

# Make a copy of the dataset to avoid having to reread it
kcha_bk <- kcha


### Strip out some variables for now
# Remove panel 19 on income except for member name/number and source of income
kcha <- kcha %>%
  select(h1a, h2a, h2b, h2c, h2d, h2h, starts_with("h3"), starts_with("h5"),
         h19a1b, h19a2b, h19a3b, h19a4b, h19a5b, h19a6b,h19a7b, h19a8b, h19a9b, h1910b, h1911b, h1912b, h1913b,
         h1914b, h1915b, h1916b, starts_with("h19b"), starts_with("h19d"), starts_with("h19f"), h19g, h19h, 
         starts_with("h20"), starts_with("h21"), program_type, householdid:developmentname, spec_vouch, subsidy_id) %>%
  # Fix up some inconsistent naming
  rename(h19a10b = h1910b, h19a11b = h1911b, h19a12b = h1912b, h19a13b = h1913b, h19a14b = h1914b, h19a15b = h1915b, h19a16b = h1916b)


# Need to strip duplicates again now that some variables have been removed
kcha <- kcha %>% distinct()

### Look at household income sources before reshaping
# Much easier to do when the entire household is on a single row

# First clean up white space around income codes
kcha <- mutate_at(kcha, vars(starts_with("h19b")), funs(str_trim(.)))

# Next take most complete income data 
# (sometimes h19h is missing when h19g is not, unclear if this is always true for h19d and h19f, which add up to h19g and h19h, respectively)
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
    h19f14 = ifelse(is.na(h19f14) & !is.na(h19d14), h19d14, h19f14),
    h19f15 = ifelse(is.na(h19f15) & !is.na(h19d15), h19d15, h19f15),
    h19f16 = ifelse(is.na(h19f16) & !is.na(h19d16), h19d16, h19f16)
  )

# Make matrices of income codes and dollar amounts
inc_fixed <- kcha %>% select(h19b01:h19b16) %>%
  mutate_all(funs(ifelse(. %in% c("G", "P", "S", "SS"), 1, 0)))
inc_fixed <- as.matrix(inc_fixed)

inc_vary <- kcha %>% select(h19b01:h19b16) %>%
  mutate_all(funs(ifelse(. %in% c("G", "P", "S", "SS"), 0, 1)))
inc_vary <- as.matrix(inc_vary)

inc_amount <- kcha %>% select(h19f01:h19f16)
inc_amount <- as.matrix(inc_amount)

# Calculate totals of fixed and varying incomes
inc_fixed_amt <- as.data.frame(inc_fixed * inc_amount)
inc_fixed_amt <- inc_fixed_amt %>%
  mutate(hhold_inc_fixed = rowSums(., na.rm = TRUE)) %>%
  select(hhold_inc_fixed)

inc_vary_amt <- as.data.frame(inc_vary * inc_amount)
inc_vary_amt <- inc_vary_amt %>%
  mutate(hhold_inc_vary = rowSums(., na.rm = TRUE)) %>%
  select(hhold_inc_vary)

# Join back to main data
kcha <- bind_cols(kcha, inc_fixed_amt, inc_vary_amt)

# Remove temporary data
rm(list = ls(pattern = "inc_"))


##### Reshape data #####
# The data initially has household members in wide format
# Need to reshape to give one hhold member per row but retain head of hhold info

# Make HH vars first
kcha <- kcha %>%
  mutate(
    hh_lname = h3b01,
    hh_fname = h3c01,
    hh_mname = h3d01,
    hh_ssn = h3n01,
    hh_dob = h3e01,
    # Make temporary record of the row each new row came from when reshaped
    hhold_id_temp = row_number()
  )


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
    varname = ifelse(str_detect(varname, "h19a[1]{1}[ab]+"), str_replace(varname, "h19a", "h19a0"), varname),
    varname = ifelse(str_detect(varname, "h19a[2-9]+[ab]+"), str_replace(varname, "h19a", "h19a0"), varname),
    # Trim the final letter
    varname = ifelse(str_detect(varname, "h3k") | str_detect(varname, "h19a"), str_sub(varname, 1, -2), varname)
  )
colnames(kcha) <- names[,1]


# Using an apply process over reshape due to memory issues
# Make function to save space
reshape_f <- function(x) {
  print(x)
  sublong[[x]] <- kcha %>%
    select_('h1a:h2h',
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
            'program_type',
            'spec_vouch',
            'h5a1a:hhold_id_temp') %>%
    mutate(mbr_num = x)
}


# Make empty list and run reshaping function
sublong <- list()
members <- c("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12")
kcha_long <- lapply(members, reshape_f)
kcha_long <- bind_rows(kcha_long)

# Get rid of white space (focus on variables used to filter first to save memory)
kcha_long <- kcha_long %>%
  mutate_at(vars(h3a, h3b, h3c, h3n, starts_with("h5a")), funs(str_trim(.)))

# Get rid of empty rows (note: some rows have a SSN but no name or address, others have an address but no name or other details)
kcha_long <- kcha_long %>%
  filter(!((is.na(h3a) |  h3a == 0) & h3b == "" & h3c == "" & h3n == ""))

# Add number of household members at that time
kcha_long <- kcha_long %>%
  group_by(hhold_id_temp) %>%
  mutate(hhold_size = n()) %>%
  ungroup()

# Make mbr_num (created and from data) and unit_zip a number
kcha_long <- mutate_at(kcha_long, vars(h3a, mbr_num, h19a01:h19a16, h5a5), funs(as.numeric(.)))


# Convert dates to appropriate format
kcha_long <- mutate_at(kcha_long, vars(h2b, h2h, h3e, hh_dob),
                       funs(as.Date(., origin = "1970-01-01")))


##### REORGANIZE INCOME FIELDS #####
# Need to restrict the income source codes to only those that apply to that individual
# Code could be made more efficient but currently works

inc_temp <- kcha_long %>%
  select(hhold_id_temp, mbr_num, h19a01:h19b16)

# Reshape limited dataset
inc_temp <- inc_temp %>%
  gather(key, value, -hhold_id_temp, -mbr_num, -h19b01, -h19b02, -h19b03, -h19b04, -h19b05, -h19b06,
         -h19b07, -h19b08, -h19b09, -h19b10, -h19b11, -h19b12, -h19b13, -h19b14, -h19b15, -h19b16) %>%
  filter(!is.na(value))

inc_temp <- inc_temp %>%
  # Filter out NA rows and rows that don't match the member number
  filter(!is.na(value) & mbr_num == value)

inc_temp <- inc_temp %>%
  arrange(hhold_id_temp, mbr_num) %>%
  # Keep record of overall row number for merging back later
  mutate(rownum = row_number()) %>%
  # Find the row number for that person to know how many income sources they have
  group_by(hhold_id_temp, mbr_num) %>%
  mutate(grprownum = row_number()) %>%
  ungroup() %>%
  mutate(
    incomenum = as.numeric(str_sub(key, -2, -1))
  )

### Make a data frame to accommodate the new columns
# Establish parameters for size of new data
maxnum <- max(inc_temp$grprownum)
rowcount <- nrow(inc_temp)

# Make data frame and name columns appropriately
inc_df <- data.frame(matrix(ncol = maxnum, nrow = rowcount))
colnames(inc_df) <- paste0(rep("income", ncol(inc_df)), c(1:ncol(inc_df)))

# Join df back to main
inc_temp2 <- cbind(inc_temp, inc_df)

### Make a new variable that pulls the appropriate income code
# Set up column index
colnum <- data.frame(colnums = ifelse(inc_temp2$incomenum < 10, match(paste0("h19b0", inc_temp2$incomenum), colnames(inc_temp2)),
                            ifelse(inc_temp2$incomenum >= 10, match(paste0("h19b", inc_temp2$incomenum), colnames(inc_temp2)),
                                   NA)))
inc_temp2 <- cbind(inc_temp2, colnum)


### Extract out correct income code for that row
# Separate out rows by the column that should be looked up
inc_recode_f <- function(x) {
  inc_group[[x]] <- inc_temp2 %>%
    filter(colnums == x) %>%
    select(hhold_id_temp, mbr_num, grprownum, x) %>%
    mutate(income00 = .[[4]])
}

inc_group <- list()
inc_members <- seq(from = 3, to = 14)
inc_check <- lapply(inc_members, inc_recode_f)

# Bring back into a single data frame
inc_check <- bind_rows(inc_check)

# Clean out superfluous columns and trim white space
inc_check <- select(inc_check, hhold_id_temp, mbr_num, grprownum, income00)

### Merge back with main data
inc_temp2 <- left_join(inc_temp2, inc_check, by = c("hhold_id_temp", "mbr_num", "grprownum"))
# Set income up and restrict columns
inc_temp2 <- inc_temp2 %>%
  mutate(
    income1 = ifelse(grprownum == 1, income00, NA),
    income2 = ifelse(grprownum == 2, income00, NA),
    income3 = ifelse(grprownum == 3, income00, NA),
    income4 = ifelse(grprownum == 4, income00, NA),
    income5 = ifelse(grprownum == 5, income00, NA),
    income6 = ifelse(grprownum == 6, income00, NA)
    ) %>%
  select(hhold_id_temp, mbr_num, income1:income6) %>%
  # Consolidate to a single row per household member (slow, should find a more efficient approach)
  group_by(hhold_id_temp, mbr_num) %>%
  mutate(
    income1 = max(income1, na.rm = T),
    income2 = max(income2, na.rm = T),
    income3 = max(income3, na.rm = T),
    income4 = max(income4, na.rm = T),
    income5 = max(income5, na.rm = T),
    income6 = max(income6, na.rm = T)
  ) %>%
  slice(1) %>%
  ungroup()

### Merge back with longitudinal file
kcha_long <- left_join(kcha_long, inc_temp2, by = c("hhold_id_temp", "mbr_num"))

### Remove extraneous columns
kcha_long <- select(kcha_long, program_type, spec_vouch, householdid, certificationid, subsidy_id,
                    h1a:h2h, mbr_num, h3a:h3n, h5a1a:h5a5, developmentname, h5e:h5d, income1:income6, 
                    h20b:h21q, hh_lname:hh_dob, hhold_inc_fixed, hhold_inc_vary, hhold_size)


##### END INCOME REORGANIZATION #####




##### Rename variables #####
# Bring in variable name mapping table
fields <- read.xlsx("//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/Field name mapping.xlsx")

kcha_long <- setnames(kcha_long, fields$PHSKC[match(names(kcha_long), fields$KCHA_modified)])


##### WRITE RESHAPED DATA TO SQL #####
sqlDrop(db.apde51, "dbo.kcha_reshaped")
sqlSave(
  db.apde51,
  kcha_long,
  tablename = "dbo.kcha_reshaped",
  varTypes = c(
    act_date = "Date",
    admit_date = "Date",
    dob = "Date",
    hh_dob = "Date"
   )
 )


##### Remove temporary files #####
rm(list = ls(pattern = "inc_"))
rm(list = c('members', 'maxnum', 'rowcount', 'sublong', 'names', 'colnum', 'reshape_f'))
gc()
