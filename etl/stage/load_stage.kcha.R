#### CODE TO COMBINE KING COUNTY HOUSING AUTHORITY DATA
# Alastair Matheson, PHSKC (APDE)
#
# 2021-06

### Run from main_kcha_load script
# https://github.com/PHSKC-APDE/Housing/blob/master/claims_db/etl/db_loader/main_kcha_load.R
# Assumes relevant libraries are already loaded


# function to pick which years to load
# bring in relevant raw years
# do usual income work and reshaping
# truncate any data in stage table from selected years
# add selected years to stage table

# BRING IN DATA ----


# COMBINE HOUSEHOLD INCOME SOURCES BEFORE RESHAPING ----
# Much easier to do when the entire household is on a single row
# NB. There are many household/date combos repeated due to minor differences
# in rows, e.g., addresses formatted differently. This will mean household
# income is repeated until rows are cleaned up.

# Take most complete adjusted income data
# (sometimes h19h is missing when h19g is not, same is true for h19f and h19d, 
# which add up to h19h and h19g, respectively)
kcha_2004_2015_full <- kcha_2004_2015_full %>%
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
inc_fixed <- kcha_2004_2015_full %>% select(contains("h19b")) %>%
  mutate_all(list(~ ifelse(. %in% c("G", "P", "S", "SS"), 1L, 0L)))
inc_fixed <- as.matrix(inc_fixed)

inc_vary <- kcha_2004_2015_full %>% select(contains("h19b")) %>%
  mutate_all(list(~ ifelse(. %in% c("G", "P", "S", "SS"), 0L, 1L)))
inc_vary <- as.matrix(inc_vary)

inc_amount <- kcha_2004_2015_full %>% select(contains("h19d"))
inc_amount <- as.matrix(inc_amount)

inc_adj_amount <- kcha_2004_2015_full %>% select(contains("h19f"))
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


# RESHAPE AND REORGANIZE ----
