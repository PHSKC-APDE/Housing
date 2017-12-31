###############################################################################
# Code to join Yesler Terrace housing data with Medicaid claims data
#
# Alastair Matheson (PHSKC-APDE)
# 2017-06-30
#
# NOTE THAT THIS CODE IS A WORK IN PROGRESS
#
###############################################################################

##### Set up global parameter and call in libraries #####
options(max.print = 350, tibble.print_max = 30, scipen = 999)

library(RODBC) # Used to connect to SQL server
library(lubridate) # Used to manipulate dates
library(dplyr) # Used to manipulate data
library(reshape2) # Used to reshape data
library(epiR) # Used to calculate rates and other statistical measures


#### Connect to the SQL servers ####
db.claims51 <- odbcConnect("PHClaims51")


##### BRING IN DATA #####
### Bring in linked housing/Medicaid elig data with YT already designated
yt_elig_final <- readRDS("//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/SHA cleaning/yt_elig_final.Rds")




### Bring in ED visit data
ptm01 <- proc.time() # Times how long this query takes (~65 secs)
ed <- sqlQuery(db.claims51,
               "SELECT DISTINCT mid, FROM_SRVC_DATE AS ed_from, TO_SRVC_DATE AS ed_to
                     FROM (SELECT MEDICAID_RECIPIENT_ID AS mid, FROM_SRVC_DATE=convert(varchar(10), FROM_SRVC_DATE,1), 
                           TO_SRVC_DATE=convert(varchar(10), TO_SRVC_DATE,1),
                           REVENUE_CODE AS RCODE, PLACE_OF_SERVICE AS POS, PATIENT_STATUS_DESC AS STATUS, CLM_TYPE_CID AS CTYPE,
                           CASE WHEN PATIENT_STATUS_DESC LIKE '%hospital%' THEN 1 ELSE 0 END AS DTH
                           FROM dbo.NewClaims) a
                     WHERE (RCODE LIKE '045[01269]' OR RCODE LIKE '0981' OR POS LIKE '%23%') AND DTH=0
               ORDER BY mid, ed_from",
               stringsAsFactors = FALSE)
proc.time() - ptm01
ed <- ed %>%  mutate_at(vars(ed_from, ed_to), funs(as.Date(., origin = "1970-01-01")))


### Bring in asthma visit data
ptm01 <- proc.time() # Times how long this query takes (~38 secs)
asthma <- sqlQuery(db.claims51,
               "SELECT DISTINCT mid, FROM_SRVC_DATE AS ast_from, TO_SRVC_DATE AS ast_to
                FROM (SELECT MEDICAID_RECIPIENT_ID AS mid, FROM_SRVC_DATE, TO_SRVC_DATE,
                      PRIMARY_DIAGNOSIS_CODE AS DX1, DIAGNOSIS_CODE_2 AS DX2, DIAGNOSIS_CODE_3 AS DX3,
                      DIAGNOSIS_CODE_4 AS DX4,DIAGNOSIS_CODE_5 AS DX5, CLM_TYPE_CID
                      FROM PHClaims.dbo.NewClaims) a
                      WHERE (CLM_TYPE_CID = 31 OR CLM_TYPE_CID = 12 OR CLM_TYPE_CID = 23) AND
                      ((DX1 LIKE '493%' OR DX1 LIKE 'J45%' OR DX1 LIKE 'J44%') OR 
                        (DX2 LIKE '493%' OR DX2 LIKE 'J45%' OR DX2 LIKE 'J44%') OR 
                        (DX3 LIKE '493%' OR DX3 LIKE 'J45%' OR DX3 LIKE 'J44%') OR 
                        (DX4 LIKE '493%' OR DX4 LIKE 'J45%' OR DX4 LIKE 'J44%') OR 
                        (DX5 LIKE '493%' OR DX5 LIKE 'J45%' OR DX5 LIKE 'J44%'))
                ORDER BY mid, ast_from",
               stringsAsFactors = FALSE)
proc.time() - ptm01
asthma <- asthma %>%  mutate_at(vars(ast_from, ast_to), funs(as.Date(., origin = "1970-01-01")))


#### END BRING IN DATA SECITON ####

#### SET UP A YT/SCATTERED SITES ONLY FILE ####
# This section is used to created a specific dataset of people who have lived in Yesler Terrace or a scattered site
# Code for more general SHA linkage is beyong this section

yt_ss <- yt_elig_final %>%
  filter(yt_ever == 1 | ss_ever == 1)

# Make a list of unique Medicaid IDs in the data
yt_ss_ids <- yt_ss %>%
  filter(!is.na(MEDICAID_RECIPIENT_ID)) %>%
  distinct(MEDICAID_RECIPIENT_ID)
yt_ss_ids <- as.list(yt_ss_ids$MEDICAID_RECIPIENT_ID)
yt_ss_ids <- paste(yt_ss_ids, collapse = "', '")
yt_ss_ids <- paste0("'", yt_ss_ids, "'", collapse = NULL)

# Pull in claims from YT/SS people
ptm01 <- proc.time() # Times how long this query takes (~100 secs)
yt_ss_claims <- 
  sqlQuery(db.claims50,
           paste0("SELECT DISTINCT MEDICAID_RECIPIENT_ID, TCN, ORGNL_TCN, FROM_SRVC_DATE, TO_SRVC_DATE, BLNG_PRVDR_TYPE_CODE, CLM_TYPE_CID,
                  CLM_TYPE_NAME, CLAIM_STATUS, UNIT_SRVC_H, PAID_AMT_H, MEDICARE_COST_AVOIDANCE_AMT, 
                  TPL_COST_AVOIDANCE_AMT, PATIENT_PAY_AMT_L
                  FROM dbo.NewClaims
                  WHERE MEDICAID_RECIPIENT_ID IN (", yt_ss_ids, ")
                  ORDER BY MEDICAID_RECIPIENT_ID, FROM_SRVC_DATE, TCN"),
           as.is = TRUE)
proc.time() - ptm01


# Fix up formats
yt_ss_claims <- yt_ss_claims %>%
  mutate_at(vars(FROM_SRVC_DATE, TO_SRVC_DATE), funs(as.Date(., origin = "1970-01-01"))) %>%
  mutate_at(vars(UNIT_SRVC_H, PAID_AMT_H, MEDICARE_COST_AVOIDANCE_AMT, 
                 TPL_COST_AVOIDANCE_AMT, PATIENT_PAY_AMT_L), funs(as.numeric(.))) 


# Link back to the elig file
yt_ss_joined <- left_join(yt_ss, yt_ss_claims, by = c("MEDICAID_RECIPIENT_ID")) %>%
  # Remove Medicaid claims that fall outside of the time the person was covered by Medicaid (i.e., extra rows from the join)
  mutate(drop = ifelse(!is.na(FROM_SRVC_DATE) & (FROM_SRVC_DATE < startdate_c | FROM_SRVC_DATE > enddate_c), 1, 0)) %>%
  filter(drop == 0)

#Strip identifiers
yt_ss_joined <- select(yt_ss_joined, pid, gender_new_m6:agency_new, DUAL_ELIG, COVERAGE_TYPE_IND,
                       startdate_h:enddate_o, startdate_c:enroll_type, yt:PATIENT_PAY_AMT_L)


### Export data
saveRDS(yt_ss_joined, file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/SHA cleaning/yt_ss_joined_2017-08-17.Rda")
yt_ss_joined <- readRDS(file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/SHA cleaning/yt_ss_joined_2017-08-17.Rda")

#### END YT/SS SPECIFIC SECTION ####


#### JOIN PHA/ELIG TABLE WITH CLAIMS ####
yt_claims_ed <- left_join(yt_elig_final, ed, by = "mid") %>%
  # Make year of service var
  mutate(ed_year = year(ed_from))


yt_claims_asthma <- left_join(yt_elig_final, asthma, by = "mid") %>%
  # Make year of service var
  mutate(ast_year = year(ast_from))


# Do both in one go
yt_claims <- left_join(yt_elig_final, ed, by = "mid") %>%
  left_join(., asthma, by = "mid") %>%
  mutate(ed_year = year(ed_from), ast_year = year(ast_from))


##### ANALYZE DATA #####
### Combined visits
yt_claims <- yt_claims %>%
  # Remove Medicaid claims that fall outside of the time a person was in housing
  mutate_at(vars(ed_from, ast_from), 
            funs(ifelse(!is.na(.) & (. < startdate_c | . > enddate_c), NA, .))) %>%
  mutate(ed_to = ifelse(is.na(ed_from), NA, ed_to),
         ed_year = ifelse(is.na(ed_from), NA, ed_year),
         ast_to = ifelse(is.na(ast_from), NA, ast_to),
         ast_year = ifelse(is.na(ast_from), NA, ast_year)) %>%
  distinct() %>%
  # Redo date formats
  mutate_at(vars(ed_from, ed_to, ast_from, ast_to), funs(as.Date(., origin = "1970-01-01"))) %>%
  # Remove blanks rows when there are non-blank rows for that date range
  mutate(drop = ifelse(pid2 == lead(pid2, 1) & !is.na(lead(pid2, 1)) & 
                         startdate_c == lead(startdate_c, 1) &
                         ((is.na(ed_from) & !is.na(lead(ed_from, 1))) | 
                            (is.na(ast_from) & !is.na(lead(ast_from, 1)))), 1, 0),
         drop = ifelse(pid2 == lag(pid2, 1) & !is.na(lag(pid2, 1)) & 
                         startdate_c == lag(startdate_c, 1) &
                         ((is.na(ed_from) & !is.na(lag(ed_from, 1))) | 
                            (is.na(ast_from) & !is.na(lag(ast_from, 1)))), 1, drop)
         ) %>%
    filter(drop == 0)
    


# Collapse to count of the number of ED visits per period of overlapping housing/Medicaid coverage
ed_sum_tmp <- yt_claims %>%
  filter(!is.na(ed_from)) %>%
  distinct(pid2, startdate_c, ed_year, dual_elig_m, ed_from) %>%
  group_by(pid2, startdate_c, ed_year, dual_elig_m) %>%
  summarise(ed_count = n())

yt_claims <- left_join(yt_claims, ed_sum_tmp, by = c("pid2", "startdate_c", "ed_year", "dual_elig_m"))


### ED visits
# Look at rates by year
yt_claims_ed <- yt_claims %>%
  filter(agency_new == "SHA" & !is.na(dual_elig_m) & enroll_type == "b") %>%
  select(pid2, yt, agency_new, dob_h, gender2_h, agegrp_h, race_h, disability2_h, 
         dual_elig_m, pt12:pt16, ed_year, ed_count) %>%
  distinct()


yt_claims_ed %>%
  filter(dual_elig_m == "N") %>%
  group_by(yt) %>%
  summarise(
    ed_12 = sum(ed_count[ed_year == 2012 | is.na(ed_year)], na.rm = TRUE),
    ed_13 = sum(ed_count[ed_year == 2013 | is.na(ed_year)], na.rm = TRUE),
    ed_14 = sum(ed_count[ed_year == 2014 | is.na(ed_year)], na.rm = TRUE),
    ed_15 = sum(ed_count[ed_year == 2015 | is.na(ed_year)], na.rm = TRUE),
    ed_16 = sum(ed_count[ed_year == 2016 | is.na(ed_year)], na.rm = TRUE),
    pt_12_o = sum(pt12[ed_year == 2012 | is.na(ed_year)], na.rm = TRUE),
    pt_13_o = sum(pt13[ed_year == 2013 | is.na(ed_year)], na.rm = TRUE),
    pt_14_o = sum(pt14[ed_year == 2014 | is.na(ed_year)], na.rm = TRUE),
    pt_15_o = sum(pt15[ed_year == 2015 | is.na(ed_year)], na.rm = TRUE),
    pt_16_o = sum(pt16[ed_year == 2016 | is.na(ed_year)], na.rm = TRUE)
    ) %>%
  mutate(
    rate_12 = ed_12 / (pt_12_o/366) * 1000,
    rate_13 = ed_13 / (pt_13_o/365) * 1000,
    rate_14 = ed_14 / (pt_14_o/365) * 1000,
    rate_15 = ed_15 / (pt_15_o/365) * 1000,
    rate_16 = ed_16 / (pt_16_o/366) * 1000
    )


# Reshape to get a single person-time vector (issue: drops people with no claims)
# First make matrix of all years
yt_claims_frame <- yt_claims %>% 
  filter(enroll_type == 'b') %>%
  select(pid2, yt, ss, agency_new, dual_elig_m, gender2_h, age12_h, race_h, disability2_h, pt12:pt16) %>%
  distinct() %>%
  # Sum all overlapping person-time for a year
  group_by(pid2, yt, ss, agency_new, dual_elig_m, gender2_h, age12_h, race_h, disability2_h) %>%
  mutate(pt2012 = sum(pt12, na.rm = T),
         pt2013 = sum(pt13, na.rm = T),
         pt2014 = sum(pt14, na.rm = T),
         pt2015 = sum(pt15, na.rm = T),
         pt2016 = sum(pt16, na.rm = T)) %>%
  ungroup() %>%
  select(-pt12, -pt13, -pt14, -pt15, -pt16) %>%
  distinct() %>%
  melt(., id.vars = c("pid2", "yt", "ss", "agency_new", "dual_elig_m", "gender2_h",
                      "age12_h", "race_h", "disability2_h")) %>%
  rename(pt = value, year = variable) %>%
  mutate(year = as.numeric(str_sub(year, 3, 6)),
         pt = ifelse(is.na(pt), 0, pt)) %>%
  arrange(pid2, year)


yt_claims_long <- yt_claims %>%
  filter(enroll_type == "b" & !is.na(ed_count)) %>%
  select(pid2, yt, ss, agency_new, dual_elig_m, gender2_h, age12_h, race_h, 
         disability2_h, ed_year, ed_count) %>%
  distinct() %>%
  rename(year = ed_year) %>%
  group_by(pid2, yt, ss, agency_new, dual_elig_m, gender2_h, age12_h, race_h, disability2_h, year) %>%
  mutate(ed_count2 = sum(ed_count, na.rm = T)) %>%
  ungroup() %>%
  select(-ed_count) %>%
  distinct() %>%
  rename(ed_count = ed_count2)


yt_claims_ed2 <- left_join(yt_claims_frame, yt_claims_long, 
                           by = c("pid2", "yt", "ss", "agency_new", "dual_elig_m", "gender2_h", 
                                  "age12_h", "race_h", "disability2_h", "year")) %>%
  mutate(ed_count = ifelse(is.na(ed_count), 0 , ed_count))


ed_rate <- yt_claims_ed2 %>%
  filter(dual_elig_m == "N") %>%
  group_by(yt, ss, year) %>%
  summarise(ed = sum(ed_count, na.rm = TRUE), pt = sum(pt, na.rm = TRUE)) %>%
  ungroup() %>%
  mutate(pt_yr = ifelse(year %in% c(2012, 2016), pt/366, pt/365),
         rate = ed / pt_yr * 1000)

epi.conf(as.matrix(cbind(ed_rate$ed, ed_rate$pt_yr)), ctype = "inc.rate", method = "exact")





ed %>% distinct(mid, ed_from) %>% mutate(ed_year = year(ed_from)) %>% group_by(ed_year) %>% summarise(count = n())
ed_sum_tmp %>% distinct(pid2, ed_year, ed_count) %>% group_by(ed_year) %>% summarise(count = n())

yt_claims_ed %>% distinct(pid2, pt14_o, yt) %>% summarise(pt = sum(pt14_o, na.rm = T))
yt_claims_ed2 %>% distinct(pid2, ed_year, pt) %>% group_by(ed_year) %>% summarise(pt = sum(pt, na.rm = T))

temp14_a <- yt_claims_ed %>% filter(ed_year == 2014)
temp14_b <- yt_claims_ed2 %>% filter(ed_year == 2014)


# Output to Excel for use in Tableau
# Make unique ID to anonymize data
yt_claims_ed2$pid <- group_indices(yt_claims_ed2, MEDICAID_RECIPIENT_ID, SOCIAL_SECURITY_NMBR, ssn_new_pha, lname_new.1, fname_new.1, dob_h)
yt_claims_ed2 <- select(yt_claims_ed2, pid, gender_new_m6:enddate_o, yt:ed_count)

write.xlsx(yt_claims_ed2, file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/Summaries/pha_ed_visits.xlsx")





### Asthma visits
yt_claims_asthma <- yt_claims_asthma %>%
  # Remove Medicaid claims that fall outside of the time a person was in housing
  mutate(drop = ifelse(!is.na(ast_from) & (ast_from < startdate_o | ast_from > enddate_o), 1, 0)) %>%
  filter(drop == 0)

# Collapse to count of the number of ED visits per period of overlapping housing/Medicaid coverage
ast_sum_tmp <- yt_claims_asthma %>%
  filter(!is.na(ast_from)) %>%
  distinct(MEDICAID_RECIPIENT_ID, SOCIAL_SECURITY_NMBR, ssn_new_pha, lname_new.1, fname_new.1, dob_h, startdate_o, ast_from, ast_year) %>%
  group_by(MEDICAID_RECIPIENT_ID, SOCIAL_SECURITY_NMBR, ssn_new_pha, lname_new.1, fname_new.1, dob_h, startdate_o, ast_year) %>%
  summarise(ast_count = n())

yt_claims_asthma <- left_join(yt_claims_asthma, ast_sum_tmp, 
                          by = c("MEDICAID_RECIPIENT_ID", "SOCIAL_SECURITY_NMBR", "ssn_new_pha", "lname_new.1", 
                                 "fname_new.1", "dob_h", "startdate_o", "ast_year"))

# Look at rates by year
yt_claims_asthma2 <- yt_claims_asthma %>%
  select(MEDICAID_RECIPIENT_ID:agency_new, startdate_h:age16, ast_year, ast_count) %>%
  distinct()

yt_claims_asthma2 %>%
  filter(agency_new == "SHA") %>%
  group_by(yt, ss) %>%
  summarise(
    ast_12 = sum(ast_count[ast_year == 2012], na.rm = TRUE),
    ast_13 = sum(ast_count[ast_year == 2013], na.rm = TRUE),
    ast_14 = sum(ast_count[ast_year == 2014], na.rm = TRUE),
    ast_15 = sum(ast_count[ast_year == 2015], na.rm = TRUE),
    ast_16 = sum(ast_count[ast_year == 2016], na.rm = TRUE),
    pt_12_o = sum(pt12_o, na.rm = TRUE),
    pt_13_o = sum(pt13_o, na.rm = TRUE),
    pt_14_o = sum(pt14_o, na.rm = TRUE),
    pt_15_o = sum(pt15_o, na.rm = TRUE),
    pt_16_o = sum(pt16_o, na.rm = TRUE)
  ) %>%
  mutate(
    rate_12 = ast_12 / pt_12_o * 100000,
    rate_13 = ast_13 / pt_13_o * 100000,
    rate_14 = ast_14 / pt_14_o * 100000,
    rate_15 = ast_15 / pt_15_o * 100000,
    rate_16 = ast_16 / pt_16_o * 100000
  )


### Output to Excel for use in Tableau
write.xlsx(yt_claims_asthma2, file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/Summaries/pha_ast_visits.xlsx")





########## TESTING AREA #############
yt_claims %>% 
  filter(drop == 1 & !is.na(ast_from)) %>%
  select(ssn_new_pha, startdate_h:enddate_o, pt15_o, dob_h, ed_from, ast_from, drop) %>%
  head(.)


yt_claims_ed %>% 
  filter(!is.na(ed_from)) %>%
  select(ssn_new_pha, startdate_h:enddate_o, pt15_o, dob_h, ed_from, ed_to, drop, ed_count, ed_year) %>%
  head(.)


yt_claims_ed %>% 
  filter(ssn_new_pha == "100158522") %>%
  select(ssn_new_pha, startdate_h:enddate_o, pt15_o, dob_h, ed_from, ed_to, drop, ed_count, ed_year)


yt_claims_ed2 %>% 
  select(ssn_new_pha, dob_h, startdate_h:enddate_o, pt13_o, pt14_o, pt15_o, pt16_o, ed_year, ed_count) %>%
  head(.)


temp2 <- yt_claims_ed2 %>%
  filter(yt == 1 & ed_year == 2015 & !is.na(ed_count)) %>%
  select(ssn_new_pha, dob_h, startdate_h:enddate_o, pt13_o, pt14_o, pt15_o, pt16_o, ed_year, ed_count)
