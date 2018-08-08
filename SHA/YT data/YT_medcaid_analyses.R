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

library(odbc) # Used to connect to SQL server
library(openxlsx) # Used to import/export Excel files
library(lubridate) # Used to manipulate dates
library(tidyverse) # Used to manipulate data
library(reshape2) # Used to reshape data
library(epiR) # Used to calculate rates and other statistical measures


#### Connect to the SQL servers ####
db.apde51 <- dbConnect(odbc(), "PH_APDEStore51")
db.claims51 <- dbConnect(odbc(), "PHClaims51")


#### FUNCTIONS ####
# Function to count population and person-time by agency and YT/SS
# Person-time counted where the person was
# Population counted with the following priorities 
# (#1-5 assume simultaneous Medicaid enrollment and not dual eligible):
# 1) At least 30 days at YT that year = YT
# 2) No YT but 30+ days at SS that year = SS
# 3) No YT or SS but in SHA for 30+ days = SHA
# 4) Not YT/SS/SHA but in KCHA for 30+ days = KCHA
# 5) No 30+ days in any PHA = non-PHA Medicaid
# 6-10) Same as #1-5 but only dual eligible
# 11-15) Same as #1-5 but dual eligible is NA
# 16-18) Only time in SHA (+/- YT and SS) and no Medicaid
# 19) Only time in KCHA and no Medicaid

# This function applies a code to each housing/Medicaid period
popcode_yt_f <- function(df, year) {
  pt <- rlang::sym(paste0("pt", quo_name(year)))
  
  coded <- df %>%
    filter(!is.na((!!pt))) %>%
    mutate(
      pop_code = case_when(
        yt == 1 & enroll_type == "b" & !!pt >= 30 & dual_elig_m == "N" ~ 1,
        ss == 1 & enroll_type == "b" & !!pt >= 30 & dual_elig_m == "N" ~ 2,
        yt == 0 & ss == 0 & agency_new == "SHA" & enroll_type == "b" & !!pt >= 30 & dual_elig_m == "N" ~ 3,
        yt == 0 & ss == 0 & agency_new == "KCHA" & enroll_type == "b" & !!pt >= 30 & dual_elig_m == "N" ~ 4,
        yt == 0 & ss == 0 & is.na(agency_new) & enroll_type == "m" & dual_elig_m == "N" ~ 5,
        yt == 1 & enroll_type == "b" & !!pt >= 30 & dual_elig_m == "Y" ~ 6,
        ss == 1 & enroll_type == "b" & !!pt >= 30 & dual_elig_m == "Y" ~ 7,
        yt == 0 & ss == 0 & agency_new == "SHA" & enroll_type == "b" & !!pt >= 30 & dual_elig_m == "Y" ~ 8,
        yt == 0 & ss == 0 & agency_new == "KCHA" & enroll_type == "b" & !!pt >= 30 & dual_elig_m == "Y" ~ 9,
        yt == 0 & ss == 0 & is.na(agency_new) & enroll_type == "m" & dual_elig_m == "Y" ~ 10,
        yt == 1 & enroll_type == "b" & !!pt >= 30 & is.na(dual_elig_m) ~ 11,
        ss == 1 & enroll_type == "b" & !!pt >= 30 & is.na(dual_elig_m) ~ 12,
        yt == 0 & ss == 0 & agency_new == "SHA" & enroll_type == "b" & !!pt >= 30 & is.na(dual_elig_m) ~ 13,
        yt == 0 & ss == 0 & agency_new == "KCHA" & enroll_type == "b" & !!pt >= 30 & is.na(dual_elig_m) ~ 14,
        yt == 0 & ss == 0 & is.na(agency_new) & enroll_type == "m" & is.na(dual_elig_m) ~ 15,
        yt == 1 & agency_new == "SHA" & enroll_type == "h" ~ 16,
        ss == 1 & agency_new == "SHA" & enroll_type == "h" ~ 17,
        yt == 0 & ss == 0 & agency_new == "SHA" & enroll_type == "h" ~ 18,
        agency_new == "KCHA" & enroll_type == "h" ~ 19
      ),
      year_code = as.numeric(paste0(20, year)))
  
  return(coded)
}

# This function takes the minimum code for a given year, prioritizing YT residence
# Requires that popcode_yt_f has been run
popcode_min_f <- function(df, year) {
  
  mincode <- df %>%
    filter(year_code == as.numeric(paste0(20, year))) %>%
    group_by(pid2) %>%
    mutate(pop_type = min(pop_code)) %>%
    ungroup() %>%
    mutate(
      year = as.numeric(paste0(20, year)),
      agency = case_when(
        pop_type %in% c(1:3, 6:8, 11:13, 16:18) ~ "SHA",
        pop_type %in% c(4, 9, 14, 19) ~ "KCHA",
        pop_type %in% c(5, 10, 15) ~ "Non-PHA"
      ),
      enroll_type = case_when(
        pop_type %in% c(1:4, 6:9, 11:14) ~ "Both",
        pop_type %in% c(5, 10, 15) ~ "Medicaid only",
        pop_type %in% c(16:19) ~ "Housing only"
      ),
      dual = case_when(
        pop_type %in% c(1:5) ~ "N",
        pop_type %in% c(6:10) ~ "Y"
      ),
      yt = case_when(
        pop_type %in% c(1, 6, 11, 16) ~ 1,
        pop_type %in% c(2:5, 7:10, 12:15, 17:19) ~ 0
      ),
      ss = case_when(
        pop_type %in% c(2, 7, 12, 17) ~ 1,
        pop_type %in% c(1, 3:6, 8:11, 12:16, 18:19) ~ 0
      )
    )
  
    return(mincode)
}

# This function keeps one code per person per year
popcode_sum_f <- function(df, year, demogs = F) {
  pt <- rlang::sym(paste0("pt", quo_name(year)))  
  
  coded <- popcode_yt_f(df, year)
  output <- popcode_min_f(coded, year)
  
  if (demogs == F) {
    output <- output %>%
      distinct(year, mid, pid2, agency, enroll_type, dual, yt, ss) %>%
      # Adding select to get the right order
      select(year, mid, pid2, agency, enroll_type, dual, yt, ss)
  } else if (demogs == T) {
    output <- output %>%
      # Keep demographics associated with enrollment type
      filter(pop_type == pop_code) %>%
      distinct(year, mid, pid2, agency, enroll_type, dual, yt, ss,
               gender_c, race_c, hisp_c, 
               age12, age13, age14, age15, age16, age17,
               length12, length13, length14, length15, length16, length17) %>%
      # Adding select to get the right order
      select(year, mid, pid2, agency, enroll_type, dual, yt, ss,
             gender_c, race_c, hisp_c, 
             age12, age13, age14, age15, age16, age17,
             length12, length13, length14, length15, length16, length17) %>%
      # Keep first row if >1 different demographics for that year
      group_by(pid2, year) %>%
      slice(1) %>%
      ungroup()
  }
  
  return(output)
  
}

# This function counts the population in each PHA/Medicaid grouping
popcount_all_yt_f <- function(df, year) {
  pt <- rlang::sym(paste0("pt", quo_name(year)))                
  
  # Make person-time denominator for each grouping
  pt_output <- df %>%
    filter(!is.na((!!pt))) %>%
    group_by(agency_new, enroll_type, dual_elig_m, yt, ss) %>%
    summarise(pt = sum((!!pt))) %>%
    ungroup() %>%
    mutate(year = as.numeric(paste0(20, year)),
           pt = case_when(
             year %in% c(2012, 2016) ~ pt/366,
             year %in% c(2013:2015) ~ pt/365
           )
           ) %>%
    rename(agency = agency_new, dual = dual_elig_m) %>%
    select(year, agency, enroll_type, dual, yt, ss, pt) %>%
    mutate(
      agency = ifelse(is.na(agency), "Non-PHA", agency),
      enroll_type = case_when(
        enroll_type == "b" ~ "Both",
        enroll_type == "h" ~ "Housing only",
        enroll_type == "m" ~ "Medicaid only")
    )
  
  # 
  coded <- popcode_yt_f(df, year)
  pop_output <- popcode_min_f(coded, year)
  
  pop_output <- pop_output %>%
    distinct(year, pid2, agency, enroll_type, dual, yt, ss) %>%
    group_by(year, agency, enroll_type, dual, yt, ss) %>%
    summarise(pop = n_distinct(pid2)) %>%
    ungroup()

  output <- left_join(pt_output, pop_output, 
                      by = c("year", "agency", "enroll_type",
                             "dual", "yt", "ss"))
  
  return(output)
}


### Function to summarize acute events by YT/not YT
eventcount_yt_f <- function(df, event = NULL, year) {
  
  event_quo <- enquo(event)
  
  if (str_detect(quo_name(event_quo), "hosp")) {
    event_year <- quo(hosp_year)
  } else if (str_detect(quo_name(event_quo), "ed")) {
    event_year <- quo(ed_year)
  } else if (str_detect(quo_name(event_quo), "inj")) {
    event_year <- quo(inj_year)
  }

  output <- df %>%
    filter((!!event_year) == year | is.na((!!event_year)))  %>%
    group_by(agency_new, enroll_type, dual_elig_m, yt, ss) %>%
    summarise(count = sum(!!event_quo, na.rm = T)) %>%
    ungroup() %>%
    mutate(year = as.numeric(year)) %>%
    select(year, agency_new, enroll_type, dual_elig_m, yt, ss, count) %>%
    rename(agency = agency_new, dual = dual_elig_m) %>%
    mutate(agency = ifelse(is.na(agency), "Non-PHA", agency),
           enroll_type = case_when(
             enroll_type == "b" ~ "Both",
             enroll_type == "h" ~ "Housing only",
             enroll_type == "m" ~ "Medicaid only")
    )
  return(output)
}



#### BRING IN DATA ####
### Bring in linked housing/Medicaid elig data with YT already designated
yt_mcaid_final <- readRDS("//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/SHA cleaning/yt_mcaid_final.Rds")

# Filter to only include YT and SS residents
yt_ss <- yt_mcaid_final %>% filter(yt == 1 | ss == 1)



### Acute events
# ED visits (using broad definition)
system.time(
  ed <- dbGetQuery(db.claims51, 
                      "SELECT id, tcn, from_date, to_date, ed, ed_avoid_ca
                      FROM dbo.mcaid_claim_summary
                      WHERE ed = 1")
)


# Reformat and add column useful for counting up events
ed <- ed %>%
  mutate_at(vars(from_date, to_date), funs(as.Date(., origin = "1970-01-01"))) %>%
  distinct()



# Hospitalizations
system.time(
hosp <- dbGetQuery(db.claims51,
                   "SELECT id AS mid, from_date, to_date, inpatient
                   FROM PHClaims.dbo.mcaid_claim_summary
                   WHERE inpatient = 1")
)

# Reformat and add column useful for counting up events
hosp <- hosp %>% 
  mutate(from_date = as.Date(from_date, origin = "1970-01-01"),
         to_date = as.Date(to_date, origin = "1970-01-01"),
         hosp_year = year(from_date))


### Chronic conditions
# Bring in all predefined conditions
ptm01 <- proc.time() # Times how long this query takes (~115 secs)
conditions <- sqlQuery(db.apde51,
                       "SELECT * FROM PH_APDEStore.dbo.vcond_all",
                       stringsAsFactors = FALSE)
proc.time() - ptm01
conditions <- conditions %>% mutate(FR_SDT = as.Date(FR_SDT, origin = "1970-01-01"))

ptm01 <- proc.time() # Times how long this query takes (~125 secs)
conditions <- lapply(seq(2012, 2016), conditions_lin_nomerge_f)
proc.time() - ptm01
conditions <- as.data.frame(data.table::rbindlist(conditions))

# Just injuries
inj <- sqlQuery(db.apde51,
                "SELECT * FROM PH_APDEStore.dbo.vcond_all
                WHERE INJ = 1",
                stringsAsFactors = FALSE)
inj <- inj %>%
  select(ID, FR_SDT, INJ) %>%
  rename(mid = ID, fr_sdt = FR_SDT, inj = INJ) %>%
  mutate(fr_sdt = as.Date(fr_sdt, origin = "1970-01-01"))


#### END BRING IN DATA SECITON ####



#### POPULATION DATA ####
# Need to count up person time by agency, yt and ss
yt_pop_enroll <- lapply(seq(12, 17), popcount_all_yt_f, df = yt_mcaid_final)
yt_pop_enroll <- as.data.frame(data.table::rbindlist(yt_pop_enroll))


# Make simplified version of enrollment
yt_mcaid_simple <- lapply(seq(12, 17), popcode_sum_f, df = yt_mcaid_final, demogs = T)
yt_mcaid_simple <- as.data.frame(data.table::rbindlist(yt_mcaid_simple))


#### ACUTE EVENTS ####
### Join demographics and hospitalization events
yt_mcaid_hosp <- left_join(yt_ss, hosp, by = c("mid" = "ID")) %>%
  mutate(
    from_date = ifelse(from_date < startdate_c | from_date > enddate_c, NA, from_date),
    from_date = as.Date(from_date, origin = "1970-01-01")
  ) %>%
  filter(is.na(from_date) | (from_date >= startdate_c & from_date <= enddate_c)) %>%
  distinct()

# Run numbers for hospitalizations


yt_hosp_pers <- lapply(seq(12, 16), eventcount_yt_f, df = yt_mcaid_hosp, event = hosp_pers)
yt_hosp_pers <- as.data.frame(data.table::rbindlist(yt_hosp_pers)) %>%
  mutate(indicator = "Persons with hospitalization")
yt_hosp_cnt <- lapply(seq(12, 16), eventcount_yt_f, df = yt_mcaid_hosp, event = hosp_cnt)
yt_hosp_cnt <- as.data.frame(data.table::rbindlist(yt_hosp_cnt)) %>%
  mutate(indicator = "Hospitalizations")


### Join demographics and ED events
yt_mcaid_ed <- left_join(yt_ss, ed, by = c("mid" = "id")) %>%
  mutate(
    from_date = as.Date(ifelse(from_date < startdate_c | from_date > enddate_c, 
                               NA, from_date), origin = "1970-01-01")
    ) %>%
  filter(is.na(from_date) | (from_date >= startdate_c & from_date <= enddate_c)) %>%
  distinct()

yt_mcaid_ed <- yt_mcaid_ed %>% mutate(ed_year = year(from_date))


# The code above creates rows with NA in the same period when there are ED visits
# Need to strip out the NAs
yt_ed_cnt <- yt_mcaid_ed %>%
  filter(!is.na(ed_year)) %>%
  group_by(pid2, startdate_c, ed_year) %>%
  summarise(ed_cnt = n()) %>%
  ungroup()
yt_ed_avd_cnt <- yt_mcaid_ed %>%
  filter(ed_avoid_ca == 1 & !is.na(ed_year)) %>%
  group_by(pid2, ed_year) %>%
  summarise(ed_avoid = n()) %>%
  ungroup()

yt_mcaid_ed <- left_join(yt_mcaid_ed, yt_ed_cnt, 
                        by = c("pid2", "startdate_c", "ed_year")) %>%
  left_join(., yt_ed_avd_cnt, by = c("pid2", "ed_year")) %>%
  filter(!(is.na(ed_year) & ed_cnt > 0) | (is.na(ed_year) & is.na(ed_cnt))) %>%
  select(-from_date, -ed, -ed_avoid_ca) %>%
  distinct() %>%
  group_by(pid2, startdate_c) %>%
  mutate(rows = n()) %>%
  ungroup() %>%
  filter(!(is.na(ed_year) & rows > 1))

rm(yt_ed_cnt)
rm(yt_ed_avd_cnt)
gc()


# Run numbers for ED
yt_ed_pers <- lapply(seq(2012, 2017), eventcount_yt_f, df = yt_mcaid_ed, event = ed_pers)
yt_ed_pers <- as.data.frame(data.table::rbindlist(yt_ed_pers)) %>%
  mutate(indicator = "Persons with ED visits")
yt_ed_cnt <- lapply(seq(2012, 2017), eventcount_yt_f, df = yt_mcaid_ed, event = ed_cnt)
yt_ed_cnt <- as.data.frame(data.table::rbindlist(yt_ed_cnt)) %>%
  mutate(indicator = "ED visits")
yt_ed_avoid <- lapply(seq(12, 17), eventcount_yt_f, df = yt_mcaid_ed, event = ed_avoid)
yt_ed_avoid <- as.data.frame(data.table::rbindlist(yt_ed_avoid)) %>%
  mutate(indicator = "Avoidable ED visits")


### Join demographics and injury events
yt_mcaid_inj <- left_join(yt_mcaid_final, inj, by = "mid") %>%
  filter(is.na(fr_sdt) | (fr_sdt >= startdate_c & fr_sdt <= enddate_c))
# Set things up to count by demographics
yt_mcaid_inj <- yt_mcaid_inj %>%
  mutate(inj_year = year(fr_sdt),
         inj = ifelse(is.na(inj), 0, inj))
yt_mcaid_inj <- yt_mcaid_inj %>%
  group_by(mid, pid2, startdate_c, enddate_c, inj_year) %>%
  mutate(inj_cnt = sum(inj)) %>%
  ungroup() %>%
  select(-fr_sdt) %>%
  distinct()

# Run numbers for injuries
yt_inj_cnt <- lapply(seq(12, 16), eventcount_yt_f, df = yt_mcaid_inj, event = inj_cnt)
yt_inj_cnt <- as.data.frame(data.table::rbindlist(yt_inj_cnt)) %>%
  mutate(indicator = "Unintentional injuries")


### Clean up files to save memory
rm(yt_mcaid_hosp)
rm(yt_mcaid_ed)
rm(yt_mcaid_inj)
gc()


#### CHRONIC CONDITIONS ####
# Allocate each person for each year
yt_mcaid_pop <- lapply(seq(12, 16), popcode_yt_f, df = yt_mcaid_final)
yt_mcaid_pop <- as.data.frame(data.table::rbindlist(yt_mcaid_pop))

yt_conditions_elig <- left_join(yt_mcaid_pop, conditions, 
                             by = c("year" = "cond_year", "mid" = "ID")) %>%
  mutate_at(vars(HTN, DIA, AST, COP, IHD, DEP, MEN, INJ),
            funs(ifelse(is.na(.), 0, .)))

# Reshape to get indicator in a single column
yt_conditions_elig <- melt(yt_conditions_elig,
                       id.vars = c("year", "mid", "pid2", "agency", 
                                   "enroll_type", "dual", "yt", "ss"),
                       variable.name = "indicator", value.name = "count")

# Relabel with text descriptions
yt_conditions_elig <- yt_conditions_elig %>%
  mutate(
    indicator = case_when(
      indicator == "HTN" ~ "Hypertension",
      indicator == "AST" ~ "Asthma",
      indicator == "DIA" ~ "Diabetes",
      indicator == "DEP" ~ "Depression",
      indicator == "MEN" ~ "Mental health conditions",
      indicator == "COP" ~ "COPD",
      indicator == "IHD" ~ "IHD"
    )
  )

# Summarise data
yt_conditions <- conditions_elig %>%
  group_by(year, agency, enroll_type, dual, yt, ss, indicator) %>%
  summarise(count = sum(count)) %>%
  ungroup()


#### COMBINE DATA AND EXPORT ####
yt_health_events <- bind_rows(yt_hosp_pers, yt_hosp_cnt, yt_ed_pers, 
                              yt_ed_cnt, yt_ed_avoid, yt_inj_cnt, yt_conditions) %>%
  select(year, indicator, agency, enroll_type, dual, yt, ss, count)


list_dfs <- list("pop" = yt_pop_enroll, "tabdata" = yt_health_events)
write.xlsx(list_dfs, paste0(housing_path, "/OrganizedData/Summaries/YT/yt_claims_tableau_", Sys.Date(), ".xlsx"))

















#### DEFINE CONDITIONS ####
# Line level filter
ast <- conditions %>% filter(AST == 1) %>% 
  select(ID, FR_SDT, AST) %>%
  rename(mid = ID, fr_sdt = FR_SDT, ast = AST) %>%
  mutate(fr_sdt = as.Date(fr_sdt, origin = "1970-01-01"))

inj <- conditions %>% filter(INJ == 1) %>% 
  select(ID, FR_SDT, INJ) %>%
  rename(mid = ID, fr_sdt = FR_SDT, inj = INJ) %>%
  mutate(fr_sdt = as.Date(fr_sdt, origin = "1970-01-01"))

htn <- conditions %>% filter(HTN == 1) %>% 
  select(ID, FR_SDT, HTN) %>%
  rename(mid = ID, fr_sdt = FR_SDT, htn = HTN) %>%
  mutate(fr_sdt = as.Date(fr_sdt, origin = "1970-01-01"))




# Sum by year
ast_yr <- conditions %>% filter(AST == 1) %>%
  mutate(year = year(as.Date(FR_SDT, origin = "1970-01-01"))) %>%
  group_by(ID, year) %>%
  summarise(count = sum(AST, na.rm = T)) %>%
  ungroup()

inj_yr <- conditions %>% filter(INJ == 1) %>%
  mutate(year = year(as.Date(FR_SDT, origin = "1970-01-01"))) %>%
  group_by(ID, year) %>%
  summarise(count = sum(INJ, na.rm = T)) %>%
  ungroup()

htn_yr <- conditions %>% filter(HTN == 1) %>%
  mutate(year = year(as.Date(FR_SDT, origin = "1970-01-01"))) %>%
  group_by(ID, year) %>%
  summarise(count = sum(HTN, na.rm = T)) %>%
  ungroup()

#### SET UP A YT/SCATTERED SITES ONLY FILE ####
# This section is used to created a specific dataset of people who have lived in Yesler Terrace or a scattered site
# Code for more general SHA linkage is beyong this section

yt_ss <- yt_mcaid_final %>%
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
# Set up interval for enrollment
yt_mcaid_final <- yt_mcaid_final %>%
  mutate(int_c = interval(startdate_c, enddate_c))

pha_ast <- left_join(yt_mcaid_final, ast, by = "mid") %>%
  # Remove visits that fall outside of enrollment period
  filter((fr_sdt >= startdate_c & fr_sdt <= enddate_c) | is.na(fr_sdt)) %>%
  #select(pid2, pt12_o:pt16_o, yt, yt_old, age12_h, race_c, hisp_c, gender_c, start_housing, year, count) %>%
  distinct()

pha_inj <- left_join(yt_mcaid_final, inj, by = "mid") %>%
  # Remove visits that fall outside of enrollment period
  filter((fr_sdt >= startdate_c & fr_sdt <= enddate_c) | is.na(fr_sdt)) %>%
  #select(pid2, pt12_o:pt16_o, yt, yt_old, age12_h, race_c, hisp_c, gender_c, start_housing, year, count) %>%
  distinct()

pha_htn <- left_join(yt_mcaid_final, htn, by = "mid") %>%
  # Remove visits that fall outside of enrollment period
  filter((fr_sdt >= startdate_c & fr_sdt <= enddate_c) | is.na(fr_sdt)) %>%
  #select(pid2, pt12_o:pt16_o, yt, yt_old, age12_h, race_c, hisp_c, gender_c, start_housing, year, count) %>%
  distinct()

yt_claims_ed <- left_join(yt_mcaid_final, ed, by = "mid") %>%
  # Remove visits that fall outside of enrollment period
  mutate_at(vars(ed_from, ed), funs(ifelse(ed_from >= startdate_c & ed_from <= enddate_c, ., NA))) %>%
  # Make year of service var
  mutate(ed_year = year(ed_from)) %>%
  distinct()










yt_claims_asthma <- left_join(yt_mcaid_final, asthma, by = "mid") %>%
  # Make year of service var
  mutate(ast_year = year(ast_from))


# Do both in one go
yt_claims <- left_join(yt_mcaid_final, ed, by = "mid") %>%
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
ed_sum_tmp <- yt_claims_ed %>%
  filter(!is.na(ed_from)) %>%
  distinct(pid2, startdate_c, ed_year, dual_elig_m, ed_from) %>%
  group_by(pid2, startdate_c, ed_year, dual_elig_m) %>%
  summarise(ed_count = n())

yt_claims_ed <- left_join(yt_claims_ed, ed_sum_tmp, by = c("pid2", "startdate_c", "ed_year", "dual_elig_m"))
rm(ed_sum_tmp)


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
