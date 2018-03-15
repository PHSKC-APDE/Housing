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
library(openxlsx) # Used to import/export Excel files
library(lubridate) # Used to manipulate dates
library(dplyr) # Used to manipulate data
library(reshape2) # Used to reshape data
library(epiR) # Used to calculate rates and other statistical measures


#### Connect to the SQL servers ####
db.claims51 <- odbcConnect("PHClaims51")
db.apde51 <- odbcConnect("PH_APDEStore51")


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

popcode_yt_f <- function(df, year) {
  pt <- rlang::sym(paste0("pt", quo_name(year)))  
  
  output <- df %>%
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
      )) %>%
    group_by(mid, pid2) %>%
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
    ) %>%
    distinct(year, mid, pid2, agency, enroll_type, dual, yt, ss) %>%
    select(year, mid, pid2, agency, enroll_type, dual, yt, ss)
  
  return(output)
  
}

popcount_all_yt_f <- function(df, year) {
  pt <- rlang::sym(paste0("pt", quo_name(year)))                
  
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
  
  pop_output <- df %>%
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
      )) %>%
    group_by(pid2) %>%
    mutate(pop_type = min(pop_code)) %>%
    ungroup() %>%
    mutate(
      year = paste0(20, year),
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
    ) %>%
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
    filter((!!event_year) == as.numeric(paste0(20, year)) | is.na((!!event_year)))  %>%
    group_by(agency_new, enroll_type, dual_elig_m, yt, ss) %>%
    summarise(count = sum(!!event_quo)) %>%
    ungroup() %>%
    mutate(year = as.numeric(paste0(20, year))) %>%
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

### Lin's definition of chronic diseases
# Does not merge with pop data in SQL but brings everything in per ID
conditions_lin_nomerge_f <- function(year) {
  
  query <- paste0(
    "SELECT ID,
    CASE WHEN HTN1 > 0 OR HTN2 > 1 THEN 1 ELSE 0 END AS HTN,
    CASE WHEN DIA1 > 0 OR DIA2 > 1 THEN 1 ELSE 0 END AS DIA,
    CASE WHEN AST1 > 0 OR AST2 > 1 THEN 1 ELSE 0 END AS AST,
    CASE WHEN COP1 > 0 OR COP2 > 1 THEN 1 ELSE 0 END AS COP,
    CASE WHEN IHD1 > 0 OR IHD2 > 1 THEN 1 ELSE 0 END AS IHD,
    CASE WHEN DEP > 0 THEN 1 ELSE 0 END AS DEP,
    CASE WHEN MEN > 0 THEN 1 ELSE 0 END AS MEN
    FROM 
    (SELECT DISTINCT ID, SUM(HTN1) AS HTN1, SUM(HTN2) AS HTN2,
    SUM(DIA1) AS DIA1, SUM(DIA2) AS DIA2, SUM(DEP) AS DEP,
    SUM(AST1) AS AST1, SUM(AST2) AS AST2, SUM(MEN) AS MEN,
    SUM(COP1) AS COP1, SUM(COP2) AS COP2
    FROM
    (SELECT ID,
    CASE WHEN YEAR(FR_SDT) = ", year, 
    " AND HTN>0 AND CLMT IN(31,33,12,23) THEN 1 ELSE 0 END AS HTN1,
    CASE WHEN YEAR(FR_SDT) = ", year, 
    " AND HTN>0 AND CLMT IN(3,26,27,28,34) THEN 1 ELSE 0 END AS HTN2,
    CASE WHEN YEAR(FR_SDT) = ", year, 
    " AND AST>0 AND CLMT IN(31,33,12,23) THEN 1 ELSE 0 END AS AST1,
    CASE WHEN YEAR(FR_SDT) = ", year, 
    " AND AST>0 AND CLMT IN(3,26,27,28,34) THEN 1 ELSE 0 END AS AST2,
    CASE WHEN YEAR(FR_SDT) = ", year, 
    " AND COP>0 AND CLMT IN(31,33,12,23) THEN 1 ELSE 0 END AS COP1,
    CASE WHEN YEAR(FR_SDT) = ", year, 
    " AND COP>0 AND CLMT IN(3,26,27,28,34) THEN 1 ELSE 0 END AS COP2,
    CASE WHEN YEAR(FR_SDT) = ", year, " AND DEP>0 THEN 1 ELSE 0 END AS DEP,
    CASE WHEN YEAR(FR_SDT) = ", year, " AND MEN>0 THEN 1 ELSE 0 END AS MEN,
    CASE WHEN YEAR(FR_SDT) IN(", year-1, ",", year, ") 
    AND DIA > 0 AND CLMT IN(31,33,12,23) THEN 1 ELSE 0 END AS DIA1,
    CASE WHEN YEAR(FR_SDT) IN(", year-1, ",", year, ") 
    AND DIA > 0 AND CLMT IN(3,26,27,28,34)  THEN 1 ELSE 0 END AS DIA2,
    CASE WHEN YEAR(FR_SDT) IN(", year-1, ",", year, ") 
    AND IHD > 0 AND CLMT IN(31,33,12,23) THEN 1 ELSE 0 END AS IHD1,
    CASE WHEN YEAR(FR_SDT) IN(", year-1, ",", year, ") 
    AND IHD > 0 AND CLMT IN(3,26,27,28,34)  THEN 1 ELSE 0 END AS IHD2
    FROM [PH\\SONGL].[tCond_all]
    WHERE YEAR(FR_SDT) <= ", year, ") a
    GROUP BY ID) b
    ")
  
  output <- sqlQuery(db.apde51, query, stringsAsFactors = FALSE)
  output <- mutate(output, cond_year = as.numeric(year))
  return(output)
}


#### BRING IN DATA ####
### Bring in linked housing/Medicaid elig data with YT already designated
yt_elig_final <- readRDS("//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/SHA cleaning/yt_elig_final.Rds")

### Bring in all predefined conditions
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

### Just injuries
inj <- sqlQuery(db.apde51,
                "SELECT * FROM PH_APDEStore.dbo.vcond_all
                WHERE INJ = 1",
                stringsAsFactors = FALSE)
inj <- inj %>%
  select(ID, FR_SDT, INJ) %>%
  rename(mid = ID, fr_sdt = FR_SDT, inj = INJ) %>%
  mutate(fr_sdt = as.Date(fr_sdt, origin = "1970-01-01"))




#### OLD MEDICAID CODE ####
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
#### END OLD MEDICAID CODE ####

#### END BRING IN DATA SECITON ####


#### POPULATION DATA ####
# Need to count up person time by agency, yt and ss
yt_pop_enroll <- lapply(seq(12, 16), popcount_all_yt_f, df = yt_elig_final)
yt_pop_enroll <- as.data.frame(data.table::rbindlist(yt_pop_enroll))


#### ACUTE EVENTS ####
### Join demographics and hospitalization events
yt_elig_hosp <- left_join(yt_elig_final, hosp,
                           by = c("mid", "pid2", "startdate_c", "enddate_c")) %>%
  mutate_at(vars(hosp_cnt, hosp_pers), funs(ifelse(is.na(.), 0, .)))

# Run numbers for hospitalizations
yt_hosp_pers <- lapply(seq(12, 16), eventcount_yt_f, df = yt_elig_hosp, event = hosp_pers)
yt_hosp_pers <- as.data.frame(data.table::rbindlist(yt_hosp_pers)) %>%
  mutate(indicator = "Persons with hospitalization")
yt_hosp_cnt <- lapply(seq(12, 16), eventcount_yt_f, df = yt_elig_hosp, event = hosp_cnt)
yt_hosp_cnt <- as.data.frame(data.table::rbindlist(yt_hosp_cnt)) %>%
  mutate(indicator = "Hospitalizations")


### Join demographics and ED events
yt_elig_ed <- left_join(yt_elig_final, ed,
                         by = c("mid", "pid2", "startdate_c", "enddate_c")) %>%
  mutate_at(vars(ed_cnt, ed_pers, ed_avoid), funs(ifelse(is.na(.), 0, .)))

# Run numbers for ED
yt_ed_pers <- lapply(seq(12, 16), eventcount_yt_f, df = yt_elig_ed, event = ed_pers)
yt_ed_pers <- as.data.frame(data.table::rbindlist(yt_ed_pers)) %>%
  mutate(indicator = "Persons with ED visits")
yt_ed_cnt <- lapply(seq(12, 16), eventcount_yt_f, df = yt_elig_ed, event = ed_cnt)
yt_ed_cnt <- as.data.frame(data.table::rbindlist(yt_ed_cnt)) %>%
  mutate(indicator = "ED visits")
yt_ed_avoid <- lapply(seq(12, 16), eventcount_yt_f, df = yt_elig_ed, event = ed_avoid)
yt_ed_avoid <- as.data.frame(data.table::rbindlist(yt_ed_avoid)) %>%
  mutate(indicator = "Avoidable ED visits")


### Join demographics and injury events
yt_elig_inj <- left_join(yt_elig_final, inj, by = "mid") %>%
  filter(is.na(fr_sdt) | (fr_sdt >= startdate_c & fr_sdt <= enddate_c))
# Set things up to count by demographics
yt_elig_inj <- yt_elig_inj %>%
  mutate(inj_year = year(fr_sdt),
         inj = ifelse(is.na(inj), 0, inj))
yt_elig_inj <- yt_elig_inj %>%
  group_by(mid, pid2, startdate_c, enddate_c, inj_year) %>%
  mutate(inj_cnt = sum(inj)) %>%
  ungroup() %>%
  select(-fr_sdt) %>%
  distinct()

# Run numbers for injuries
yt_inj_cnt <- lapply(seq(12, 16), eventcount_yt_f, df = yt_elig_inj, event = inj_cnt)
yt_inj_cnt <- as.data.frame(data.table::rbindlist(yt_inj_cnt)) %>%
  mutate(indicator = "Unintentional injuries")


### Clean up files to save memory
rm(yt_elig_hosp)
rm(yt_elig_ed)
rm(yt_elig_inj)
gc()


#### CHRONIC CONDITIONS ####
# Allocate each person for each year
yt_elig_pop <- lapply(seq(12, 16), popcode_yt_f, df = yt_elig_final)
yt_elig_pop <- as.data.frame(data.table::rbindlist(yt_elig_pop))

yt_conditions_elig <- left_join(yt_elig_pop, conditions, 
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

ptm01 <- proc.time() # Times how long this query takes (~115 secs)
ed <- sqlQuery(db.claims51,
               "SELECT * FROM
               (SELECT DISTINCT a.ID, a.FR_SDT, a.DX1, SUM(DTH) AS DTH, SUM(MEN) AS MEN
                 FROM
                 (SELECT MEDICAID_RECIPIENT_ID AS ID 
                   ,CLM_TYPE_CID
                   ,CONVERT(DECIMAL(12,2), PAID_AMT_H) AS PAID_AMT
                   ,FR_SDT=convert(varchar(10), FROM_SRVC_DATE,1)
                   ,PRIMARY_DIAGNOSIS_CODE AS DX1
                   ,CASE WHEN PATIENT_STATUS_DESC LIKE '%hospital%' THEN 1 ELSE 0 END AS DTH
                   ,CASE WHEN PRIMARY_DIAGNOSIS_CODE BETWEEN '290' AND '316' 
                   OR PRIMARY_DIAGNOSIS_CODE LIKE 'F03' 
                   OR PRIMARY_DIAGNOSIS_CODE BETWEEN 'F10' AND 'F69'
                   OR PRIMARY_DIAGNOSIS_CODE BETWEEN 'F80' AND 'F99' THEN 1 ELSE 0 END AS MEN
                   FROM PHClaims.dbo.NewClaims
                   WHERE CLM_TYPE_CID IN(3,26,34) AND
                   (REVENUE_CODE LIKE '045[01269]' OR REVENUE_CODE LIKE '0981' OR LINE_PRCDR_CODE LIKE '9928[1-5]' 
                     OR (PLACE_OF_SERVICE LIKE '%23%' AND LINE_PRCDR_CODE BETWEEN '10021' AND '69990'))) a
                 GROUP BY ID, FR_SDT, DX1) b
               WHERE DTH=0 AND MEN=0
               GROUP BY ID, FR_SDT, DX1, DTH, MEN
               ORDER BY ID",
               stringsAsFactors = FALSE)
proc.time() - ptm01

ed <- ed %>%
  rename(mid = ID, ed_from = FR_SDT) %>%
  mutate(ed = 1) %>%
  select(mid, ed_from, ed)


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
# Set up interval for enrollment
yt_elig_final <- yt_elig_final %>%
  mutate(int_c = interval(startdate_c, enddate_c))

pha_ast <- left_join(yt_elig_final, ast, by = "mid") %>%
  # Remove visits that fall outside of enrollment period
  filter((fr_sdt >= startdate_c & fr_sdt <= enddate_c) | is.na(fr_sdt)) %>%
  #select(pid2, pt12_o:pt16_o, yt, yt_old, age12_h, race_c, hisp_c, gender_c, start_housing, year, count) %>%
  distinct()

pha_inj <- left_join(yt_elig_final, inj, by = "mid") %>%
  # Remove visits that fall outside of enrollment period
  filter((fr_sdt >= startdate_c & fr_sdt <= enddate_c) | is.na(fr_sdt)) %>%
  #select(pid2, pt12_o:pt16_o, yt, yt_old, age12_h, race_c, hisp_c, gender_c, start_housing, year, count) %>%
  distinct()

pha_htn <- left_join(yt_elig_final, htn, by = "mid") %>%
  # Remove visits that fall outside of enrollment period
  filter((fr_sdt >= startdate_c & fr_sdt <= enddate_c) | is.na(fr_sdt)) %>%
  #select(pid2, pt12_o:pt16_o, yt, yt_old, age12_h, race_c, hisp_c, gender_c, start_housing, year, count) %>%
  distinct()

yt_claims_ed <- left_join(yt_elig_final, ed, by = "mid") %>%
  # Remove visits that fall outside of enrollment period
  mutate_at(vars(ed_from, ed), funs(ifelse(ed_from >= startdate_c & ed_from <= enddate_c, ., NA))) %>%
  # Make year of service var
  mutate(ed_year = year(ed_from)) %>%
  distinct()










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
