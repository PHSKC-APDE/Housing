###############################################################################
# Code to generate data for the housing/Medicaid dashboard
#
# Alastair Matheson (PHSKC-APDE)
# 2018-01-24
#
#
###############################################################################

##### Set up global parameter and call in libraries #####
options(max.print = 350, tibble.print_max = 30, scipen = 999)

library(RODBC) # Used to connect to SQL server
library(openxlsx) # Used to import/export Excel files
library(housing) # contains many useful functions for analyzing housing/Medicaid data
library(lubridate) # Used to manipulate dates
library(tidyverse) # Used to manipulate data
library(reshape2) # Used to reshape data



##### Connect to the SQL servers #####
db.apde51 <- odbcConnect("PH_APDEStore51")
db.claims51 <- odbcConnect("PHClaims51")

housing_path <- "//phdata01/DROF_DATA/DOH DATA/Housing"



#### FUNCTIONS ####
# Relabel function
relabel_f <- function(data) {
  data$agegp <- code$agegp[match(data$agegp, code$Value)]
  data$gender <- code$gender[match(data$gender, code$Value)]
  data$ethn <- code$ethn[match(data$ethn, code$Value)]
  data$dual <- code$dual[match(data$dual, code$Value)]
  data$voucher <- code$voucher[match(data$voucher, code$Value)]
  data$subsidy <- code$subsidy[match(data$subsidy, code$Value)]
  data$operator <- code$operator[match(data$operator, code$Value)]
  data$portfolio <- code$portfolio[match(data$portfolio, code$Value)]
  data$length <- code$length[match(data$length, code$Value)]
  
  data <- data %>%
    mutate(
      enrtype = case_when(
        enrtype == "b" ~ "Both",
        enrtype == "h" ~ "Housing only",
        enrtype == "m" ~ "Medicaid only",
        TRUE ~ enrtype
      ),
      agency = ifelse(is.na(agency) | agency == "NULL", "Non-PHA", agency),
      voucher = ifelse(agency == "Non-PHA", "Non-PHA", voucher),
      subsidy = ifelse(agency == "Non-PHA", "Non-PHA", subsidy),
      operator = ifelse(agency == "Non-PHA", "Non-PHA", operator),
      portfolio = ifelse(agency == "Non-PHA", "Non-PHA", portfolio),
      length = ifelse(agency == "Non-PHA", "Non-PHA", length)
    )
  
  return(data)
}

### Population ###
# Function to count populations by all demographics (uses group_vars from housing package)
popcount_all_f <- function(df, year) {
  pt <- rlang::sym(paste0("pt", quo_name(year)))
  agex <- rlang::sym(paste0("age", quo_name(year), "_num"))
  lengthx <- rlang::sym(paste0("length", quo_name(year), "_num"))                   
  
  
  df <- df %>%
    filter(!is.na((!!pt))) %>%
    group_by(enroll_type, !!agex, gender_c, ethn_num, agency_new, dual_elig_num,
             voucher_num, subsidy_num, operator_num, portfolio_num, !!lengthx) %>%
    summarise(pop = n_distinct(pid2), pt = sum((!!pt))) %>%
    ungroup() %>%
    mutate(agegp = !!agex,
           gender_c = ifelse(is.na(gender_c), 9, gender_c),
           length = !!lengthx,
           year = paste0(20, year)) %>%
    select(year, enroll_type, agegp, gender_c, ethn_num, agency_new, dual_elig_num,
           voucher_num, subsidy_num, operator_num, portfolio_num, length,
           pop, pt)
  return(df)
}

# Function to count children aged 3–6 years for well child indicator
popcount_wc_f <- function(df, year) {
  pt <- rlang::sym(paste0("pt", quo_name(year)))
  agex <- rlang::sym(paste0("age", quo_name(year)))
  lengthx <- rlang::sym(paste0("length", quo_name(year), "_num"))                   
  
  df <- df %>%
    mutate(agegp = ifelse((!!agex) >= 3 & (!!agex) <= 6, 1, 0)) %>%
    filter(!is.na((!!pt)) & agegp == 1) %>%
    group_by(enroll_type, agegp, gender_c, ethn_num, agency_new, dual_elig_num,
             voucher_num, subsidy_num, operator_num, portfolio_num, !!lengthx) %>%
    summarise(pop = n_distinct(pid2), pt = sum((!!pt))) %>%
    ungroup() %>%
    mutate(agegp = 36,
           gender_c = ifelse(is.na(gender_c), 9, gender_c),
           length = !!lengthx,
           year = paste0(20, year)) %>%
    select(year, enroll_type, agegp, gender_c, ethn_num, agency_new, dual_elig_num,
           voucher_num, subsidy_num, operator_num, portfolio_num, length,
           pop, pt)
  return(df)
}

# Function to count up populations by a single demographic
popcount_f <- function(df, demog, year) {
  pt <- rlang::sym(paste0("pt", quo_name(year)))
  
  if (demog != "total" & demog != "unique") {
    
    if (demog == "gender") {
      demog <- quo(gender_c)
      cat <- "Gender"
    } else if (demog == "race") {
      demog <- quo(ethn_num)
      cat <- "Race/ethnicity"
    } else if (demog == "age") {
      demog <- rlang::sym(paste0("age", quo_name(year), "_num"))
      cat <- "Age"
    }
    
    df <- df %>%
      filter(!is.na((!!pt))) %>%
      group_by(agency_new, enroll_type, dual_elig_m, !!demog) %>%
      summarise(Population = n_distinct(pid2)) %>%
      mutate(Category = cat, Year = paste0(20, year), Group = !!demog) %>%
      ungroup() %>%
      select (Year, agency_new, enroll_type, dual_elig_m, 
              Category, Group, Population)
    return(df)
    
  } else if (demog == "total") {
    df <- df %>%
      filter(!is.na((!!pt))) %>%
      group_by(agency_new, enroll_type, dual_elig_m) %>%
      summarise(Total_population = n_distinct(pid2)) %>%
      mutate(Category = "Total", Year = paste0(20, year)) %>%
      select (Year, agency_new, enroll_type, dual_elig_m, Category, Total_population)
    return(df)
    
  } else if (demog == "unique") {
    df <- df %>%
      filter(!is.na((!!pt))) %>%
      group_by(agency_new) %>%
      summarise(Unique_population = n_distinct(pid2)) %>%
      mutate(Category = "Total", Year = paste0(20, year)) %>%
      select (Year, agency_new, Category, Unique_population)
    
    return(df)
  }
}

# Function to count up populations by ZIP code
# Currently only grouping on agency, non-PHA Medicaid ZIPs are not available
popcount_zip_f <- function(df, year, agency = TRUE) {
  pt <- rlang::sym(paste0("pt", quo_name(year)))
  
  if (agency == TRUE) {
    output <- df %>%
      filter(!is.na((!!pt)) & dual_elig_m == "N" & enroll_type == "b") %>%
      group_by(agency_new, unit_zip_h) %>%
      summarise(pop = n_distinct(pid2), pt = sum((!!pt))) %>%
      mutate(year = as.integer(paste0(20, year)),
             group = "General pop") %>%
      rename(zipcode = unit_zip_h,
             agency = agency_new) %>%
      select (year, agency, group, zipcode, pop, pt)
    }
  
  if (agency == FALSE) {
    output <- df %>%
      filter(!is.na((!!pt)) & dual_elig_m == "N" & enroll_type == "b") %>%
      group_by(unit_zip_h) %>%
      summarise(pop = n_distinct(pid2), pt = sum((!!pt))) %>%
      mutate(year = as.integer(paste0(20, year)),
             group = "General pop",
             agency = "KCHA and SHA combined") %>%
      rename(zipcode = unit_zip_h) %>%
      select (year, agency, group, zipcode, pop, pt)
    }
  
  return(output)
}

popcount_zip_wc_f <- function(df, year, agency = TRUE) {
  pt <- rlang::sym(paste0("pt", quo_name(year)))
  agex <- rlang::sym(paste0("age", quo_name(year)))
  
  if (agency == TRUE) {
    output <- df %>%
      mutate(agegp = ifelse((!!agex) >= 3 & (!!agex) <= 6, 1, 0)) %>%
      filter(!is.na((!!pt)) & agegp == 1 & dual_elig_m == "N" & enroll_type == "b") %>%
      group_by(agency_new, unit_zip_h) %>%
      summarise(pop = n_distinct(pid2), pt = sum((!!pt))) %>%
      mutate(year = as.integer(paste0(20, year)),
             group = "Well child") %>%
      rename(zipcode = unit_zip_h,
             agency = agency_new) %>%
      select (year, agency, group, zipcode, pop, pt)
    }
  
  if (agency == FALSE) {
    output <- df %>%
      mutate(agegp = ifelse((!!agex) >= 3 & (!!agex) <= 6, 1, 0)) %>%
      filter(!is.na((!!pt)) & agegp == 1 & dual_elig_m == "N" & enroll_type == "b") %>%
      group_by(unit_zip_h) %>%
      summarise(pop = n_distinct(pid2), pt = sum((!!pt))) %>%
      mutate(year = as.integer(paste0(20, year)),
             group = "Well child",
             agency_new = "KCHA and SHA combined") %>%
      rename(zipcode = unit_zip_h,
             agency = agency_new) %>%
      select (year, agency, group, zipcode, pop, pt)
    }
  
  return(output)
}

popcount_zip_lin_f <- function(year, agency = TRUE) {
  
  if (agency == TRUE) {
    query <- paste0(
      "SELECT HZIP, AGENCY, COUNT(*) AS pop_lin
      FROM dbo.tmp_hapop_", year, "
      WHERE DUAL = 12 AND ENRTYPE = 'b'
      GROUP BY HZIP, AGENCY"
      )
    
    output <- sqlQuery(db.apde51, query, stringsAsFactors = FALSE)
    output <- output %>%
      mutate(year = as.integer(year),
             group = "General pop") %>%
      rename(agency = AGENCY,
             zipcode = HZIP) %>%
      select(year, agency, group, zipcode, pop_lin)
  }
  
  if (agency == FALSE) {
    query <- paste0(
      "SELECT HZIP, COUNT(*) AS pop_lin
      FROM dbo.tmp_hapop_", year, "
      WHERE DUAL = 12 AND ENRTYPE = 'b'
      GROUP BY HZIP"
    )
    
  output <- sqlQuery(db.apde51, query, stringsAsFactors = FALSE)
  output <- output %>%
    mutate(year = as.integer(year),
           group = "General pop",
           agency = "KCHA and SHA combined") %>%
    rename(zipcode = HZIP) %>%
    select(year, agency, group, zipcode, pop_lin)
  }
 
  return(output) 
}

# Function to suppress combined population/numerator count if the one of the two
# groups is below the threshold and could be deduced
# Needs to be applied to df that has both combined and agency-specific data
pop_suppress_f <- function(df,
                           agency = agency,
                           combined = "KCHA and SHA combined",
                           group_var = c("year", "group", "zipcode"),
                           pop = pop,
                           threshold = 5) {
  
  # Convert the variable to quosures etc.
  pop_quo <- enquo(pop)
  pop_name <- quo_name(pop_quo)
  agency_quo <- enquo(agency)
  grouping_vars <- rlang::syms(group_var)

  # First flag any rows that should be suppressed
  output <- df %>%
    mutate(suppress = ifelse((!!pop_quo) > 0 & (!!pop_quo) < threshold, 1, 0)) %>%
    # Then apply to the entire group
    group_by(!!!grouping_vars) %>%
    mutate(suppress2 = max(suppress, na.rm = T)) %>%
    ungroup() %>%
    # Suppress agency and combined data if one agency is suppressed
    mutate(!!pop_name := case_when(
      (!!agency_quo) == combined & suppress2 == 1 ~ NA_integer_,
      suppress == 1 ~ NA_integer_,
      TRUE ~ as.integer(!!pop_quo)
    )) %>%
    select(-suppress, -suppress2)
  
  return(output)
}


### Lin's version of hospitalizations and ED visits ###
hosp_lin_f <- function(year) {
  query <- paste0(
    "SELECT ENRTYPE as enrtype, AGEGP as agegp, GENDER as gender, ETHN as ethn, 
    AGENCY as agency, DUAL as dual, VOUCHER as voucher, SUBSIDY as subsidy, 
    OPERATOR as operator, PORTFOLIO as portfolio, LENGTH as length, SUM(HOSP1) AS hosp_pers, SUM(HOSP2) AS hosp_cnt
    FROM
    (SELECT mid,ENRTYPE,AGEGP,GENDER,ETHN,DUAL,VOUCHER,AGENCY,SUBSIDY,OPERATOR,PORTFOLIO,LENGTH, HZIP,PT,
    CASE WHEN HOSP>0 THEN 1 ELSE 0 END AS HOSP1, ISNULL(HOSP,0) AS HOSP2
    FROM
    (SELECT * 
    FROM dbo.tmp_hapop_", year, " AS p
    LEFT JOIN
    (SELECT DISTINCT ID, COUNT(DISTINCT TO_SDT) AS HOSP 
    FROM
    (SELECT ID, TO_SRVC_DATE AS TO_SDT,
    CASE WHEN DX1 LIKE '29[0-9]%' OR DX1 LIKE 'F[012345689]%' THEN 1 ELSE 0 END AS EXCL1,
    CASE WHEN DX1 LIKE 'V3%' OR DX1 LIKE 'Z38%' THEN 1 ELSE 0 END AS EXCL2,
    CASE WHEN DX1 LIKE 'O%' OR (DX1 BETWEEN '63000' AND '67999') OR DX1 LIKE 'V240' THEN 1 ELSE 0 END AS EXCL3,
    CASE WHEN DRG_CODE IN ('765', '782', '789', '795') THEN 1 ELSE 0 END AS EXCL4,
    CASE WHEN A_SOURCE LIKE '%from a [HS]%' OR A_SOURCE LIKE '%from Another Health C%' THEN 1 ELSE 0 END AS EXCL5,
    CASE WHEN STATUS LIKE '%Expired%' THEN 1 ELSE 0 END AS EXCL6,
    CASE WHEN RCODE LIKE '002[24]' OR RCODE LIKE '01[1-5]8' OR RCODE LIKE '019%' OR RCODE LIKE '052[45]' OR
    RCODE LIKE '055[0129]' OR RCODE LIKE '066[01239]' OR RCODE LIKE '100[012]' THEN 1 ELSE 0 END AS EXCL7
    FROM
    (SELECT MEDICAID_RECIPIENT_ID AS ID, TO_SRVC_DATE=convert(varchar(10), TO_SRVC_DATE,1),
    PRIMARY_DIAGNOSIS_CODE AS DX1, DRG_CODE, ADMSN_SOURCE_NAME AS A_SOURCE, PATIENT_STATUS_DESC AS STATUS, REVENUE_CODE AS RCODE
    FROM PHClaims.dbo.NewClaims
    WHERE YEAR(TO_SRVC_DATE) =", year, "AND (CLM_TYPE_CID=31 OR CLM_TYPE_CID=33)) a) b
    WHERE EXCL1=0 AND EXCL2=0 AND EXCL3=0 AND EXCL4=0 AND EXCL5=0 AND EXCL6=0 AND EXCL7=0
    GROUP BY ID) x
    ON p.mid=x.ID) y) z
    WHERE ENRTYPE <> 'h'
    GROUP BY enrtype, agegp, gender, ethn, agency, dual, voucher, subsidy, operator, portfolio, length"
  )
  
  output <- sqlQuery(db.apde51, query, stringsAsFactors = FALSE)
  output <- mutate(output, year = year)
  return(output)
}
ed_lin_f <- function(year) {
  query <- paste0(
    "SELECT ENRTYPE as enrtype, AGEGP as agegp, GENDER as gender, ETHN as ethn, 
    AGENCY as agency, DUAL as dual, VOUCHER as voucher, SUBSIDY as subsidy, 
    OPERATOR as operator, PORTFOLIO as portfolio, LENGTH as length, 
    SUM(E1A) AS ed_pers, SUM(E1B) AS ed_cnt, SUM(E2) AS ed_avoid
    FROM 
    (SELECT mid, pid2, ENRTYPE, AGEGP, GENDER, ETHN, DUAL, VOUCHER, AGENCY,
    SUBSIDY, OPERATOR, PORTFOLIO, LENGTH, HZIP, PT,
    ISNULL(E1A,0) AS E1A, ISNULL(E1B,0) AS E1B, 
    CASE WHEN AGE < 1.0 THEN 0 
    WHEN E2 IS NULL THEN 0
    ELSE E2 END AS E2
    FROM
    (SELECT *
    FROM dbo.tmp_hapop_", year, " AS p
    LEFT JOIN
    (SELECT DISTINCT ID, SUM(E1) AS E1A, MAX(E1) AS E1B, SUM(E2) AS E2
    FROM
    (SELECT ID, FR_SDT, E1=1,
    CASE WHEN icd_code IS NOT NULL THEN 1 ELSE 0 END AS E2
    FROM
    (SELECT *
    FROM
    (SELECT DISTINCT ID, FR_SDT, DX1, SUM(DTH) AS DTH, SUM(MEN) AS MEN
    FROM
    (SELECT MEDICAID_RECIPIENT_ID AS ID, CLM_TYPE_CID,
    CONVERT(DECIMAL(12,2), PAID_AMT_H) AS PAID_AMT,
    FR_SDT = convert(varchar(10), FROM_SRVC_DATE, 1),
    PRIMARY_DIAGNOSIS_CODE AS DX1,
    CASE WHEN PATIENT_STATUS_DESC LIKE '%hospital%' THEN 1 ELSE 0 END AS DTH,
    CASE WHEN PRIMARY_DIAGNOSIS_CODE BETWEEN '290' AND '316' 
    OR PRIMARY_DIAGNOSIS_CODE LIKE 'F03' 
    OR PRIMARY_DIAGNOSIS_CODE BETWEEN 'F10' AND 'F69' 
    OR PRIMARY_DIAGNOSIS_CODE BETWEEN 'F80' AND 'F99' THEN 1 ELSE 0 END AS MEN
    FROM PHClaims.dbo.NewClaims
    WHERE YEAR(FROM_SRVC_DATE) = ", year, " AND CLM_TYPE_CID IN(3,26,34) AND
    (REVENUE_CODE LIKE '045[01269]' OR LINE_PRCDR_CODE LIKE '9928[1-5]' 
    OR (PLACE_OF_SERVICE LIKE '%23%' AND LINE_PRCDR_CODE BETWEEN '10021' AND '69990'))) a
    GROUP BY ID, FR_SDT, DX1) b
    WHERE DTH = 0) c
    LEFT JOIN [PH_APDEStore].[PH\\SONGL].cal_ed AS d
    ON c.DX1=d.icd_code) e1
    GROUP BY ID) x
    ON p.mid=x.ID) y) z
    GROUP BY enrtype, agegp, gender, ethn, agency, dual, voucher, subsidy, operator, portfolio, length"
  )
  
  output <- sqlQuery(db.apde51, query, stringsAsFactors = FALSE)
  output <- mutate(output, year = year)
  return(output)
}

### Function to summarize acute events across all demographics
eventcount_f <- function(df, event = NULL, number = TRUE, year) {
  
  event_quo <- enquo(event)
  
  if (str_detect(quo_name(event_quo), "hosp")) {
    event_year <- quo(hosp_year)
  } else if (str_detect(quo_name(event_quo), "ed")) {
    event_year <- quo(ed_year)
  } else if (str_detect(quo_name(event_quo), "inj")) {
    event_year <- quo(inj_year)
  }
  
  if (number == TRUE) {
    agex <- rlang::sym(paste0("age", quo_name(year), "_num"))
    lengthx <- rlang::sym(paste0("length", quo_name(year), "_num"))
  } else {
    agex <- rlang::sym(paste0("age", quo_name(year), "_grp"))
    lengthx <- rlang::sym(paste0("length", quo_name(year), "_grp"))
  }

  output <- df %>%
    filter((!!event_year) == as.numeric(paste0(20, year)) | is.na((!!event_year)))  %>%
    group_by(enroll_type, !!agex, gender_c, ethn_num, agency_new, dual_elig_num,
             voucher_num, subsidy_num, operator_num, portfolio_num, !!lengthx) %>%
    summarise(count = sum(!!event_quo)) %>%
    ungroup() %>%
    mutate(agegp = !!agex,
           length = !!lengthx,
           year = as.numeric(paste0(20, year))) %>%
    select(year, enroll_type, agegp, gender_c, ethn_num, agency_new, dual_elig_num,
           voucher_num, subsidy_num, operator_num, portfolio_num, length,
           count) %>%
    rename(enrtype = enroll_type, gender = gender_c, ethn = ethn_num,
           agency = agency_new, dual = dual_elig_num, voucher = voucher_num,
           subsidy = subsidy_num, operator = operator_num, portfolio = portfolio_num) %>%
    mutate(agency = ifelse(is.na(agency), "Non-PHA", agency),
           enrtype = case_when(
             enrtype == "b" ~ "Both",
             enrtype == "h" ~ "Housing only",
             enrtype == "m" ~ "Medicaid only"),
           ethn = ifelse(ethn == "0", "Other/unknown", ethn)
    ) %>%
    mutate_at(vars(agegp, gender, ethn, dual, voucher, subsidy, operator, portfolio, length),
              funs(if_else(is.na(.), 99, .)))
  return(output)
}

# Count number of events by ZIP code
# Currently only grouping on agency, non-PHA Medicaid ZIPs are not available
eventcount_zip_f <- function(df, event = NULL, year, agency = TRUE) {
  
  event_quo <- enquo(event)
  
  if (str_detect(quo_name(event_quo), "hosp")) {
    event_year <- quo(hosp_year)
  } else if (str_detect(quo_name(event_quo), "ed")) {
    event_year <- quo(ed_year)
  } else if (str_detect(quo_name(event_quo), "inj")) {
    event_year <- quo(inj_year)
  }
  
  
  if (agency == TRUE) {
    output <- df %>%
      filter(((!!event_year) == as.numeric(paste0(20, year)) | is.na((!!event_year))) &
               dual_elig_m == "N" & enroll_type == "b")  %>%
      group_by(agency_new, unit_zip_h) %>%
      summarise(count = sum(!!event_quo)) %>%
      ungroup() %>%
      mutate(year = as.integer(paste0(20, year))) %>%
      select(year, agency_new, unit_zip_h, count) %>%
      rename(zipcode = unit_zip_h,
             agency = agency_new)
  }
  
  if (agency == FALSE) {
    output <- df %>%
      filter(((!!event_year) == as.numeric(paste0(20, year)) | is.na((!!event_year))) &
               dual_elig_m == "N" & enroll_type == "b")  %>%
      group_by(unit_zip_h) %>%
      summarise(count = sum(!!event_quo)) %>%
      ungroup() %>%
      mutate(year = as.integer(paste0(20, year)),
             agency = "KCHA and SHA combined") %>%
      select(year, agency, unit_zip_h, count) %>%
      rename(zipcode = unit_zip_h)
  }
  
  return(output)
}

##### BRING IN DATA #####
pha_elig_final <- readRDS(file = paste0(housing_path, "/OrganizedData/pha_elig_final.Rda"))

### Bring in formatted housing and Medicaid demographics
pha_elig_sql <- sqlQuery(db.apde51,
                         "SELECT * FROM dbo.pha_mcaid_demogs",
                         stringsAsFactors = FALSE)
# Fix up date formats
pha_elig_sql <- pha_elig_sql %>%
  mutate(startdate_c = as.Date(startdate_c, origin = "1970-01-01"),
         enddate_c = as.Date(enddate_c, origin = "1970-01-01"))

# Bring in code that accompanies the demographics
code <- read.xlsx(paste0(housing_path, "/OrganizedData/Summaries/claims_Tableau_v9_recodes.xlsx"))


#### Bring in all chronic conditions ####
conditions <- sqlQuery(db.apde51,
                       "SELECT * FROM PH_APDEStore.dbo.vcond_all",
                       stringsAsFactors = FALSE)

### Just injuries
inj <- sqlQuery(db.apde51,
                       "SELECT * FROM PH_APDEStore.dbo.vcond_all
                        WHERE INJ = 1",
                       stringsAsFactors = FALSE)
inj <- inj %>%
  select(ID, FR_SDT, INJ) %>%
  rename(mid = ID, fr_sdt = FR_SDT, inj = INJ) %>%
  mutate(fr_sdt = as.Date(fr_sdt, origin = "1970-01-01"),
         inj_year = year(fr_sdt))


#### Bring in acute events (ED visits, hospitalizations) ####
#### Hospitalizations (takes ~90 secs to run all years at once) ####
# Pulling in all hospitalizations first then joining
hosp <- sqlQuery(db.apde51,
                  "SELECT y.mid, y.pid2, y.startdate_c, y.enddate_c, y.hosp_year, y.hosp
                  FROM
                  (SELECT x.mid, x.pid2, x.startdate_c, x.enddate_c, YEAR(x.to_sdt) AS hosp_year, x.to_sdt AS hosp
                    FROM
                    (SELECT * 
                        FROM pha_mcaid_demogs AS p
                      LEFT JOIN
                      (SELECT DISTINCT ID, to_sdt
                        FROM
                        (SELECT ID, TO_SRVC_DATE AS to_sdt,
                          CASE WHEN DX1 LIKE '29[0-9]%' OR DX1 LIKE 'F[012345689]%' THEN 1 ELSE 0 END AS EXCL1,
                          CASE WHEN DX1 LIKE 'V3%' OR DX1 LIKE 'Z38%' THEN 1 ELSE 0 END AS EXCL2,
                          CASE WHEN DX1 LIKE 'O%' OR (DX1 BETWEEN '63000' AND '67999') OR DX1 LIKE 'V240' THEN 1 ELSE 0 END AS EXCL3,
                          CASE WHEN DRG_CODE IN ('765', '782', '789', '795') THEN 1 ELSE 0 END AS EXCL4,
                          CASE WHEN A_SOURCE LIKE '%from a [HS]%' OR A_SOURCE LIKE '%from Another Health C%' THEN 1 ELSE 0 END AS EXCL5,
                          CASE WHEN STATUS LIKE '%Expired%' THEN 1 ELSE 0 END AS EXCL6,
                          CASE WHEN RCODE LIKE '002[24]' OR RCODE LIKE '01[1-5]8' OR RCODE LIKE '019%' OR RCODE LIKE '052[45]' OR
                          RCODE LIKE '055[0129]' OR RCODE LIKE '066[01239]' OR RCODE LIKE '100[012]' THEN 1 ELSE 0 END AS EXCL7
                          FROM
                          (SELECT MEDICAID_RECIPIENT_ID AS ID, TO_SRVC_DATE = convert(date, TO_SRVC_DATE),
                            PRIMARY_DIAGNOSIS_CODE AS DX1, DRG_CODE, ADMSN_SOURCE_NAME AS A_SOURCE, PATIENT_STATUS_DESC AS STATUS, REVENUE_CODE AS RCODE
                            FROM PHClaims.dbo.NewClaims
                            WHERE (CLM_TYPE_CID=31 OR CLM_TYPE_CID=33)
                          ) a
                        ) b
                      WHERE EXCL1=0 AND EXCL2=0 AND EXCL3=0 AND EXCL4=0 AND EXCL5=0 AND EXCL6=0 AND EXCL7=0) c
                      ON p.mid=c.ID) x
                    WHERE x.to_sdt >= x.startdate_c AND x.to_sdt <= x.enddate_c
                    GROUP BY x.mid, x.pid2, x.startdate_c, x.enddate_c, x.to_sdt
                  ) y
                  GROUP BY y.mid, y.pid2, y.startdate_c, y.enddate_c, y.hosp_year, y.hosp
                  ORDER BY y.pid2, y.startdate_c, y.enddate_c, y.hosp_year",
                  stringsAsFactors = FALSE)

# Collapse to count # events per year
hosp <- hosp %>%
  group_by(mid, pid2, startdate_c, enddate_c, hosp_year) %>%
  mutate(hosp_cnt = n(),
         hosp_pers = 1) %>%
  ungroup() %>%
  mutate(startdate_c = as.Date(startdate_c, origin = "1970-01-01"),
         enddate_c = as.Date(enddate_c, origin = "1970-01-01")) %>%
  distinct(mid, pid2, startdate_c, enddate_c, hosp_year, hosp_cnt, hosp_pers)


#### ED visits (takes ~120 secs to run all years at once) ####
ptm01 <- proc.time() # Times how long this query takes (~53 secs)
ed <- sqlQuery(db.apde51,
               "SELECT y.mid, y.pid2, y.startdate_c, y.enddate_c, 
                y.age12, y.age13, y.age14, y.age15, y.age16,
                y.ed_year, y.ed, y.ed_avoid
               FROM
               (SELECT x.mid, x.pid2, x.startdate_c, x.enddate_c, 
                x.age12, x.age13, x.age14, x.age15, x.age16,
                YEAR(x.fr_sdt) AS ed_year, x.fr_sdt AS ed,
                CASE WHEN x.icd_code IS NOT NULL THEN 1 ELSE 0 END AS ed_avoid
                 FROM
                 (SELECT * 
                     FROM pha_mcaid_demogs AS p
                   LEFT JOIN
                   (SELECT * 
                       FROM
                     (SELECT DISTINCT b.id, b.fr_sdt, b.dx1
                       FROM
                       (SELECT a.id, a.fr_sdt, a.dx1, max(a.dth) AS dth, max(a.men) AS men
                         FROM
                         (SELECT MEDICAID_RECIPIENT_ID AS id, CLM_TYPE_CID, 
                           CONVERT(DECIMAL(12,2), PAID_AMT_H) AS PAID_AMT,
                           fr_sdt = convert(date, FROM_SRVC_DATE), PRIMARY_DIAGNOSIS_CODE AS dx1,
                           CASE WHEN PATIENT_STATUS_DESC LIKE '%hospital%' THEN 1 ELSE 0 END AS dth,
                           CASE WHEN PRIMARY_DIAGNOSIS_CODE BETWEEN '290' AND '316' 
                           OR PRIMARY_DIAGNOSIS_CODE LIKE 'F03' 
                           OR PRIMARY_DIAGNOSIS_CODE BETWEEN 'F10' AND 'F69' 
                           OR PRIMARY_DIAGNOSIS_CODE BETWEEN 'F80' AND 'F99' THEN 1 ELSE 0 END AS men
                           FROM PHClaims.dbo.NewClaims
                           WHERE CLM_TYPE_CID IN(3,26,34) AND
                           (REVENUE_CODE LIKE '045[01269]' OR LINE_PRCDR_CODE LIKE '9928[1-5]' 
                             OR (PLACE_OF_SERVICE LIKE '%23%' AND LINE_PRCDR_CODE BETWEEN '10021' AND '69990'))
                         ) a
                         GROUP BY a.id, a.fr_sdt, a.dx1
                       ) b
                       WHERE b.dth = 0) c
                     LEFT JOIN dbo.v_cal_ed AS d
                     ON c.dx1 = d.icd_code
                   ) e
                   ON p.mid = e.id) x
                 WHERE x.fr_sdt >= x.startdate_c AND x.fr_sdt <= x.enddate_c
                 GROUP BY x.mid, x.pid2, x.startdate_c, x.enddate_c, 
                  x.age12, x.age13, x.age14, x.age15, x.age16,
                  x.fr_sdt, x.icd_code
               ) y
               GROUP BY y.mid, y.pid2, y.startdate_c, y.enddate_c, 
                y.age12, y.age13, y.age14, y.age15, y.age16,
                y.ed_year, y.ed, y.ed_avoid
               ORDER BY y.pid2, y.startdate_c, y.enddate_c, y.ed_year, y.ed",
               stringsAsFactors = FALSE)
proc.time() - ptm01

# Remove avoidable visits for children aged < 1 (need to reshape)
ed <- melt(ed,
           id.vars = c("mid", "pid2", "startdate_c", "enddate_c",
                       "ed_year", "ed", "ed_avoid"),
           variable.name = "age_year", value.name = "age")

ed <- ed %>%
  filter(paste0("age", str_sub(ed_year, -2, -1)) == age_year) %>%
  mutate(ed_avoid = ifelse(age < 1, 0, ed_avoid)) %>%
  select(-age_year, -age)


# Collapse to count # events per year
ed <- ed %>%
  group_by(mid, pid2, startdate_c, enddate_c, ed_year) %>%
  mutate(ed_cnt = n(),
         ed_avoid = sum(ed_avoid),
         ed_pers = 1) %>%
  ungroup() %>%
  mutate(startdate_c = as.Date(startdate_c, origin = "1970-01-01"),
         enddate_c = as.Date(enddate_c, origin = "1970-01-01")) %>%
  distinct(mid, pid2, startdate_c, enddate_c, ed_year, ed_cnt, ed_avoid, ed_pers)



#### POPULATION ####
### Enrollment population

### Run function for all demographics combined
pop_enroll_all <- lapply(seq(12, 16), popcount_all_f, df = pha_elig_sql)
pop_enroll_all <- as.data.frame(data.table::rbindlist(pop_enroll_all))
# Run again for well child checks
pop_enroll_wc <- lapply(seq(12, 16), popcount_wc_f, df = pha_elig_sql)
pop_enroll_wc <- as.data.frame(data.table::rbindlist(pop_enroll_wc))
# Combine into a single df
pop_enroll_all <- bind_rows(pop_enroll_all, pop_enroll_wc)



# Format for matching with Lin's numbers
pop_enroll_all <- pop_enroll_all %>%
  rename(enrtype = enroll_type,
         gender = gender_c,
         ethn = ethn_num,
         agency = agency_new,
         dual = dual_elig_num,
         voucher = voucher_num,
         subsidy = subsidy_num,
         operator = operator_num,
         portfolio = portfolio_num)

pop_enroll_all <- relabel_f(pop_enroll_all)


# Write out file
write.xlsx(pop_enroll_all, paste0(housing_path, "/OrganizedData/Summaries/PHA enrollment count for dashboard_", Sys.Date(), ".xlsx"))
rm(pop_enroll_all)
rm(pop_enroll_wc)


### Run function for demographics of interest
# Total across ALL groups (housing, dual, not dual)
pop_enroll_unique <- lapply(seq(12, 16), popcount_f, df = pha_elig_sql, demog = "unique")
pop_enroll_unique <- as.data.frame(data.table::rbindlist(pop_enroll_unique))

# Total in EACH Medicaid/housing group (housing, dual, not dual)
pop_enroll_total <- lapply(seq(12, 16), popcount_f, df = pha_elig_sql, demog = "total")
pop_enroll_total <- as.data.frame(data.table::rbindlist(pop_enroll_total))

pop_enroll_age <- lapply(seq(12, 16), popcount_f, df = pha_elig_sql, demog = "age")
pop_enroll_age <- as.data.frame(data.table::rbindlist(pop_enroll_age))
pop_enroll_age <- pop_enroll_age %>%
  mutate(Group = case_when(
    Group == 11 ~ "<18",
    Group == 12 ~ "18–24",
    Group == 13 ~ "25–44",
    Group == 14 ~ "45–61",
    Group == 15 ~ "62–64",
    Group == 16 ~ "65+",
    Group == 99 ~ "Unknown",
    is.na(Group) ~ "Unknown"
  ))

pop_enroll_gender <- lapply(seq(12, 16), popcount_f, df = pha_elig_sql, demog = "gender")
pop_enroll_gender <- as.data.frame(data.table::rbindlist(pop_enroll_gender))
pop_enroll_gender <- pop_enroll_gender %>%
  mutate(Group = case_when(
    Group == 1 ~ "Female",
    Group == 2 ~ "Male",
    is.na(Group) ~ "Unknown"
  ))

pop_enroll_race <- lapply(seq(12, 16), popcount_f, df = pha_elig_sql, demog = "race")
pop_enroll_race <- as.data.frame(data.table::rbindlist(pop_enroll_race))
pop_enroll_race <- pop_enroll_race %>%
  mutate(Group = case_when(
    Group == 41 ~ "AIAN",
    Group == 42 ~ "Asian",
    Group == 43 ~ "Black",
    Group == 44 ~ "Hispanic",
    Group == 45 ~ "Multiple",
    Group == 46 ~ "NHPI",
    Group == 47 ~ "White",
    Group == 48 ~ "Other/unknown",
    is.na(Group) ~ "Other/unknown"
  ))


# Bring together and fix formats
pop_enroll <- bind_rows(pop_enroll_total, pop_enroll_unique, pop_enroll_age, pop_enroll_gender, pop_enroll_race)
pop_enroll <- left_join(pop_enroll, pop_enroll_total, 
                        by = c("Year", "agency_new", "enroll_type", "dual_elig_m"))
pop_enroll <- left_join(pop_enroll, pop_enroll_unique, 
                        by = c("Year", "agency_new"))
pop_enroll <- pop_enroll %>%
  mutate(agency_new = ifelse(is.na(agency_new), "Non-PHA", agency_new),
         enroll_type = case_when(
           enroll_type == "b" ~ "Both",
           enroll_type == "h" ~ "Housing only",
           enroll_type == "m" ~ "Medicaid only"),
         dual_elig_m = case_when(
           dual_elig_m == "N" ~ "Not dual eligible",
           dual_elig_m == "Y" ~ "Dual eligible",
           is.na(dual_elig_m) ~ "Unknown")
         ) %>%
  rename(Agency = agency_new, `Enrollment type` = enroll_type,
         `Dual eligibility` = dual_elig_m)

# Write out file
write.xlsx(pop_enroll, paste0(housing_path, "/OrganizedData/Summaries/PHA enrollment count_", Sys.Date(), ".xlsx"))

rm(pop_enroll_total)
rm(pop_enroll_age)
rm(pop_enroll_gender)
rm(pop_enroll_race)
gc()


### Run function for ZIPs (with agency breakout (_a) and combined (_c))
pop_enroll_zip_a <- lapply(seq(12, 16), popcount_zip_f, df = pha_elig_sql, agency = TRUE)
pop_enroll_zip_a <- as.data.frame(data.table::rbindlist(pop_enroll_zip_a))
pop_enroll_zip_c <- lapply(seq(12, 16), popcount_zip_f, df = pha_elig_sql, agency = FALSE)
pop_enroll_zip_c <- as.data.frame(data.table::rbindlist(pop_enroll_zip_c))
# Then run for ZIP and well child pop (ages 3-6)
pop_enroll_zip_wc_a <- lapply(seq(12, 16), popcount_zip_wc_f, df = pha_elig_sql, agency = TRUE)
pop_enroll_zip_wc_a <- as.data.frame(data.table::rbindlist(pop_enroll_zip_wc_a))
pop_enroll_zip_wc_c <- lapply(seq(12, 16), popcount_zip_wc_f, df = pha_elig_sql, agency = FALSE)
pop_enroll_zip_wc_c <- as.data.frame(data.table::rbindlist(pop_enroll_zip_wc_c))

# Combine and add suppression
pop_enroll_zip <- bind_rows(pop_enroll_zip_a, pop_enroll_zip_c, pop_enroll_zip_wc_a, pop_enroll_zip_wc_c)
pop_enroll_zip <- pop_suppress_f(pop_enroll_zip)
# Optional: write out just first part
#write.xlsx(pop_enroll_zip, paste0(housing_path, "/OrganizedData/Summaries/PHA enrollment count - zip_", Sys.Date(), ".xlsx"))



# ZIP pop for Lin's counts
pop_enroll_zip_lin_a <- lapply(seq(2012, 2016), popcount_zip_lin_f, agency = TRUE)
pop_enroll_zip_lin_a <- as.data.frame(data.table::rbindlist(pop_enroll_zip_lin_a))
pop_enroll_zip_lin_c <- lapply(seq(2012, 2016), popcount_zip_lin_f, agency = FALSE)
pop_enroll_zip_lin_c <- as.data.frame(data.table::rbindlist(pop_enroll_zip_lin_c))

# Combine and add suppression
pop_enroll_zip_lin <- bind_rows(pop_enroll_zip_lin_a, pop_enroll_zip_lin_c)
pop_enroll_zip_lin <- pop_suppress_f(pop_enroll_zip_lin, pop = pop_lin)
# Optional: write out just Lin's pop
#write.xlsx(pop_enroll_zip_lin, paste0(housing_path, "/OrganizedData/Summaries/PHA enrollment count - zip - lin_", Sys.Date(), ".xlsx"))


# Combine into single file and write out
pop_enroll_combined <- left_join(pop_enroll_zip, pop_enroll_zip_lin,
                                 by = c("year", "agency", "group", "zipcode")) %>%
  rename(pop_am = pop, pt_am = pt)
write.xlsx(pop_enroll_combined, paste0(housing_path, "/OrganizedData/Summaries/PHA enrollment count - zip - combined_", Sys.Date(), ".xlsx"))


rm(pop_enroll_zip_a)
rm(pop_enroll_zip_c)
rm(pop_enroll_zip_wc_a)
rm(pop_enroll_zip_wc_c)
rm(pop_enroll_zip)

rm(pop_enroll_zip_lin_a)
rm(pop_enroll_zip_lin_c)
rm(pop_enroll_zip_lin)

rm(pop_enroll_combined)


#### TEMP #####
# Apply codes to Lin's data
tabdata_lin <- read.xlsx("M:/Analyses/Lin/Tableau/claims_Tableau_v9.xlsx", sheet = "TABDATA")
pop_lin <- read.xlsx("M:/Analyses/Lin/Tableau/claims_Tableau_v9.xlsx", sheet = "POP")
pop_lin <- pop_lin %>% mutate(gender = ifelse(is.na(gender) | gender == "NULL", 9, gender))

tabdata_lin <- relabel_f(tabdata_lin)
pop_lin <- relabel_f(pop_lin)

tabdata_lin <- tabdata_lin %>%
  mutate(
    indicator = case_when(
      indicator == "hosp_pers" ~ "Persons with hospitalization_lin",
      indicator == "hosp_cnt" ~ "Hospitalizations_lin",
      indicator == "ed_pers" ~ "Persons with ED visits_lin",
      indicator == "ed_cnt" ~ "ED visits_lin",
      indicator == "ed_avoid" ~ "Avoidable ED visits_lin",
      indicator == "htn" ~ "Hypertension",
      indicator == "ast" ~ "Asthma",
      indicator == "dia" ~ "Diabetes",
      indicator == "dep" ~ "Depression",
      indicator == "men" ~ "Mental health conditions",
      indicator == "cop" ~ "COPD",
      indicator == "ihd" ~ "IHD",
      indicator == "inj" ~ "Unintentional injuries_lin",
      indicator == 11 ~ "Persons with hospitalization_lin",
      indicator == 12 ~ "Hospitalizations_lin",
      indicator == 22 ~ "Persons with ED visits_lin",
      indicator == 21 ~ "ED visits_lin",
      indicator == 23 ~ "Avoidable ED visits_lin",
      indicator == 31 ~ "Hypertension",
      indicator == 32 ~ "Asthma",
      indicator == 33 ~ "Diabetes",
      indicator == 34 ~ "Depression",
      indicator == 35 ~ "Mental health conditions",
      indicator == 36 ~ "COPD",
      indicator == 37 ~ "IHD",
      indicator == 38 ~ "Unintentional injuries_lin",
      indicator == 41 ~ "1+ Well child check (ages 3–6)"
    )
  )


write.xlsx(tabdata_lin, paste0(housing_path, "/OrganizedData/Summaries/_a_lin temp tabdata_", Sys.Date(), ".xlsx"))
write.xlsx(pop_lin, paste0(housing_path, "/OrganizedData/Summaries/_a_lin temp pop_", Sys.Date(), ".xlsx"))

#### CHRONIC CONDITIONS ####
# Keep row with most person-time in housing 

#### Lin's definition of chronic diseases ####
conditions_lin_f <- function(year) {
  query <- paste0(
    "SELECT ENRTYPE as enrtype, AGEGP as agegp, GENDER as gender, 
    ETHN as ethn, AGENCY as agency, DUAL as dual, VOUCHER as voucher,
    SUBSIDY as subsidy, OPERATOR as operator, PORTFOLIO as portfolio, 
    LENGTH as length, HZIP as zip_h,
    SUM(HTN) AS htn, SUM(AST) AS ast, SUM(DIA) AS dia,
    SUM(DEP) AS dep, SUM(MEN) AS men, SUM(COP) AS cop,
    SUM(IHD) AS ihd, SUM(INJ) AS inj
    FROM (
    SELECT mid, pid2, ENRTYPE, AGEGP, GENDER, ETHN, DUAL, VOUCHER, 
    AGENCY, SUBSIDY, OPERATOR, PORTFOLIO, LENGTH, HZIP,
    ISNULL(HTN,0) AS HTN, ISNULL(AST, 0) AS AST, 
    ISNULL(DIA,0) AS DIA, ISNULL(DEP,0) AS DEP, ISNULL(MEN,0) AS MEN,
    ISNULL(COP,0) AS COP, ISNULL(IHD, 0) AS IHD, ISNULL(INJ,0) AS INJ
    FROM
    (SELECT * FROM dbo.tmp_hapop_", year, " AS p
    LEFT JOIN
    (SELECT ID,
    CASE WHEN HTN1 > 0 OR HTN2 > 1 THEN 1 ELSE 0 END AS HTN,
    CASE WHEN DIA1 > 0 OR DIA2 > 1 THEN 1 ELSE 0 END AS DIA,
    CASE WHEN AST1 > 0 OR AST2 > 1 THEN 1 ELSE 0 END AS AST,
    CASE WHEN COP1 > 0 OR COP2 > 1 THEN 1 ELSE 0 END AS COP,
    CASE WHEN IHD1 > 0 OR IHD2 > 1 THEN 1 ELSE 0 END AS IHD,
    CASE WHEN DEP > 0 THEN 1 ELSE 0 END AS DEP,
    CASE WHEN MEN > 0 THEN 1 ELSE 0 END AS MEN,
    CASE WHEN INJ > 0 THEN 1 ELSE 0 END AS INJ
    FROM 
    (SELECT DISTINCT ID, SUM(HTN1) AS HTN1, SUM(HTN2) AS HTN2,
    SUM(DIA1) AS DIA1, SUM(DIA2) AS DIA2, SUM(DEP) AS DEP,
    SUM(AST1) AS AST1, SUM(AST2) AS AST2, SUM(MEN) AS MEN,
    SUM(COP1) AS COP1, SUM(COP2) AS COP2,
    SUM(IHD1) AS IHD1, SUM(IHD2) AS IHD2,SUM(INJ) AS INJ
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
    CASE WHEN YEAR(FR_SDT) = ", year, 
    " AND DEP>0 THEN 1 ELSE 0 END AS DEP,
    CASE WHEN YEAR(FR_SDT) =", year, 
    " AND MEN>0 THEN 1 ELSE 0 END AS MEN,
    CASE WHEN YEAR(FR_SDT) IN(", year-1, ",", year, ") AND DIA > 0 AND CLMT IN(31,33,12,23) THEN 1 ELSE 0 END AS DIA1,
    CASE WHEN YEAR(FR_SDT) IN(", year-1, ",", year, ") AND DIA > 0 AND CLMT IN(3,26,27,28,34)  THEN 1 ELSE 0 END AS DIA2,
    CASE WHEN YEAR(FR_SDT) IN(", year-1, ",", year, ") AND IHD > 0 AND CLMT IN(31,33,12,23) THEN 1 ELSE 0 END AS IHD1,
    CASE WHEN YEAR(FR_SDT) IN(", year-1, ",", year, ") AND IHD > 0 AND CLMT IN(3,26,27,28,34)  THEN 1 ELSE 0 END AS IHD2,
    CASE WHEN YEAR(FR_SDT)= ", year, " AND INJ > 0 THEN 1 ELSE 0 END AS INJ
    FROM [PH\\SONGL].[tCond_all]
    WHERE YEAR(FR_SDT)<= ", year, ") a
    GROUP BY ID) b) c
    ON p. mid=c.ID)	d) e
    WHERE ENRTYPE <> 'h'
    GROUP BY enrtype, agegp, gender, ethn, agency, dual, voucher, 
    subsidy, operator, portfolio, length, HZIP")

  output <- sqlQuery(db.apde51, query, stringsAsFactors = FALSE)
  output <- mutate(output, year = as.numeric(year))
  return(output)
}

conditions_lin <- lapply(seq(2012,2016), conditions_lin_f)
conditions_lin <- as.data.frame(data.table::rbindlist(conditions_lin))

# Reshape to get indicator in a single column
conditions_lin <- melt(conditions_lin,
                       id.vars = c("enrtype", "agegp", "gender", "ethn",
                                   "agency", "dual", "voucher", "subsidy",
                                   "operator", "portfolio", "length", "zip_h", "year"),
                       variable.name = "indicator", value.name = "count")
conditions_lin <- conditions_lin %>%
  mutate(popsource = "lin") %>%
  select(popsource, year, indicator, enrtype:count)

# Relabel with text descriptions
conditions_lin <- relabel_f(conditions_lin)

conditions_lin <- conditions_lin %>%
  mutate(
    indicator = case_when(
      indicator == "htn" ~ "Hypertension",
      indicator == "ast" ~ "Asthma",
      indicator == "dia" ~ "Diabetes",
      indicator == "dep" ~ "Depression",
      indicator == "men" ~ "Mental health conditions",
      indicator == "cop" ~ "COPD",
      indicator == "ihd" ~ "IHD",
      indicator == "inj" ~ "Unintentional injuries_lin"
    )
  )


# Produce ZIP counts for chronic conditions (with agency breakout (_a) and combined (_c))
conditions_lin_zip_a <- conditions_lin %>%
  group_by(year, agency, zip_h, indicator) %>%
  summarise(count = sum(count)) %>%
  ungroup()

conditions_lin_zip_c <- conditions_lin %>%
  group_by(year, zip_h, indicator) %>%
  summarise(count = sum(count)) %>%
  ungroup() %>%
  mutate(agency = "KCHA and SHA combined")

conditions_lin_zip <- bind_rows(conditions_lin_zip_a, conditions_lin_zip_c) %>%
  rename(zipcode = zip_h) %>%
  mutate(popsource = "lin") %>%
  select(popsource, year:count) %>%
  arrange(indicator, year, zipcode, agency)

# Apply suppression and write out
conditions_lin_zip <- pop_suppress_f(conditions_lin_zip, 
                                     group_var = c("indicator", "year", "zipcode"),
                                     pop = count)
write.xlsx(conditions_lin_zip, paste0(housing_path, "/OrganizedData/Summaries/claims_Tableau_v9_health_events_lin zip_", Sys.Date(), ".xlsx"))



#### ACUTE EVENTS ####
### Join demographics and hospitalization events
pha_elig_hosp <- left_join(pha_elig_sql, hosp,
                           by = c("mid", "pid2", "startdate_c", "enddate_c")) %>%
  mutate_at(vars(hosp_cnt, hosp_pers), funs(ifelse(is.na(.), 0, .)))

# Run numbers for hospitalizations
hosp_pers <- lapply(seq(12, 16), eventcount_f, df = pha_elig_hosp, event = hosp_pers)
hosp_pers <- as.data.frame(data.table::rbindlist(hosp_pers)) %>%
  mutate(indicator = "Persons with hospitalization")
hosp_cnt <- lapply(seq(12, 16), eventcount_f, df = pha_elig_hosp, event = hosp_cnt)
hosp_cnt <- as.data.frame(data.table::rbindlist(hosp_cnt)) %>%
  mutate(indicator = "Hospitalizations")

# Run numbers by ZIP (with agency breakout (_a) and combined (_c))
hosp_pers_zip_a <- lapply(seq(12, 16), eventcount_zip_f, df = pha_elig_hosp, 
                        event = hosp_pers, agency = TRUE)
hosp_pers_zip_a <- as.data.frame(data.table::rbindlist(hosp_pers_zip_a)) %>%
  mutate(indicator = "Persons with hospitalization") %>%
  select(year, agency, zipcode, indicator, count)
hosp_pers_zip_c <- lapply(seq(12, 16), eventcount_zip_f, df = pha_elig_hosp, 
                          event = hosp_pers, agency = FALSE)
hosp_pers_zip_c <- as.data.frame(data.table::rbindlist(hosp_pers_zip_c)) %>%
  mutate(indicator = "Persons with hospitalization") %>%
  select(year, agency, zipcode, indicator, count)

hosp_cnt_zip_a <- lapply(seq(12, 16), eventcount_zip_f, df = pha_elig_hosp, 
                         event = hosp_cnt, agency = TRUE)
hosp_cnt_zip_a <- as.data.frame(data.table::rbindlist(hosp_cnt_zip_a)) %>%
  mutate(indicator = "Hospitalizations") %>%
  select(year, agency, zipcode, indicator, count)
hosp_cnt_zip_c <- lapply(seq(12, 16), eventcount_zip_f, df = pha_elig_hosp, 
                         event = hosp_cnt, agency = TRUE)
hosp_cnt_zip_c <- as.data.frame(data.table::rbindlist(hosp_cnt_zip_c)) %>%
  mutate(indicator = "Hospitalizations") %>%
  select(year, agency, zipcode, indicator, count)


### Join demographics and ED events
pha_elig_ed <- left_join(pha_elig_sql, ed,
                           by = c("mid", "pid2", "startdate_c", "enddate_c")) %>%
  mutate_at(vars(ed_cnt, ed_pers, ed_avoid), funs(ifelse(is.na(.), 0, .)))

# Run numbers for ED
ed_pers <- lapply(seq(12, 16), eventcount_f, df = pha_elig_ed, event = ed_pers)
ed_pers <- as.data.frame(data.table::rbindlist(ed_pers)) %>%
  mutate(indicator = "Persons with ED visits")
ed_cnt <- lapply(seq(12, 16), eventcount_f, df = pha_elig_ed, event = ed_cnt)
ed_cnt <- as.data.frame(data.table::rbindlist(ed_cnt)) %>%
  mutate(indicator = "ED visits")
ed_avoid <- lapply(seq(12, 16), eventcount_f, df = pha_elig_ed, event = ed_avoid)
ed_avoid <- as.data.frame(data.table::rbindlist(ed_avoid)) %>%
  mutate(indicator = "Avoidable ED visits")

# Run numbers by ZIP (with agency breakout (_a) and combined (_c))
ed_pers_zip_a <- lapply(seq(12, 16), eventcount_zip_f, df = pha_elig_ed, 
                        event = ed_pers, agency = TRUE)
ed_pers_zip_a <- as.data.frame(data.table::rbindlist(ed_pers_zip_a)) %>%
  mutate(indicator = "Persons with ED visits") %>%
  select(year, agency, zipcode, indicator, count)
ed_pers_zip_c <- lapply(seq(12, 16), eventcount_zip_f, df = pha_elig_ed, 
                        event = ed_pers, agency = FALSE)
ed_pers_zip_c <- as.data.frame(data.table::rbindlist(ed_pers_zip_c)) %>%
  mutate(indicator = "Persons with ED visits") %>%
  select(year, agency, zipcode, indicator, count)

ed_cnt_zip_a <- lapply(seq(12, 16), eventcount_zip_f, df = pha_elig_ed, 
                       event = ed_cnt, agency = TRUE)
ed_cnt_zip_a <- as.data.frame(data.table::rbindlist(ed_cnt_zip_a)) %>%
  mutate(indicator = "ED visits") %>%
  select(year, agency, zipcode, indicator, count)
ed_cnt_zip_c <- lapply(seq(12, 16), eventcount_zip_f, df = pha_elig_ed, 
                       event = ed_cnt, agency = FALSE)
ed_cnt_zip_c <- as.data.frame(data.table::rbindlist(ed_cnt_zip_c)) %>%
  mutate(indicator = "ED visits") %>%
  select(year, agency, zipcode, indicator, count)

ed_avoid_zip_a <- lapply(seq(12, 16), eventcount_zip_f, df = pha_elig_ed, 
                         event = ed_avoid, agency = TRUE)
ed_avoid_zip_a <- as.data.frame(data.table::rbindlist(ed_avoid_zip_a)) %>%
  mutate(indicator = "Avoidable ED visits") %>%
  select(year, agency, zipcode, indicator, count)
ed_avoid_zip_c <- lapply(seq(12, 16), eventcount_zip_f, df = pha_elig_ed, 
                         event = ed_avoid, agency = FALSE)
ed_avoid_zip_c <- as.data.frame(data.table::rbindlist(ed_avoid_zip_c)) %>%
  mutate(indicator = "Avoidable ED visits") %>%
  select(year, agency, zipcode, indicator, count)


### Join demographics and injury events
pha_elig_inj <- left_join(pha_elig_sql, inj, by = "mid") %>%
  filter(is.na(fr_sdt) | (fr_sdt >= startdate_c & fr_sdt <= enddate_c))

# Set things up to count by demographics
pha_elig_inj <- pha_elig_inj %>%
  mutate(inj = ifelse(is.na(inj), 0, inj))
pha_elig_inj <- pha_elig_inj %>%
  group_by(mid, pid2, startdate_c, enddate_c, inj_year) %>%
  mutate(inj_cnt = sum(inj)) %>%
  ungroup() %>%
  select(-fr_sdt) %>%
  distinct()

# Run numbers for injuries
inj_cnt <- lapply(seq(12, 16), eventcount_f, df = pha_elig_inj, event = inj_cnt)
inj_cnt <- as.data.frame(data.table::rbindlist(inj_cnt)) %>%
  mutate(indicator = "Unintentional injuries")

# Run numbers by ZIP (with agency breakout (_a) and combined (_c))
inj_cnt_zip_a <- lapply(seq(12, 16), eventcount_zip_f, df = pha_elig_inj, 
                        event = inj_cnt, agency = TRUE)
inj_cnt_zip_a <- as.data.frame(data.table::rbindlist(inj_cnt_zip_a)) %>%
  mutate(indicator = "Unintentional injuries") %>%
  select(year, agency, zipcode, indicator, count)
inj_cnt_zip_c <- lapply(seq(12, 16), eventcount_zip_f, df = pha_elig_inj, 
                        event = inj_cnt, agency = FALSE)
inj_cnt_zip_c <- as.data.frame(data.table::rbindlist(inj_cnt_zip_c)) %>%
  mutate(indicator = "Unintentional injuries") %>%
  select(year, agency, zipcode, indicator, count)


#### Lin's version of hospitalizations and ED visits ####
### Hospitalizations
ptm01 <- proc.time()
hosp_lin <- lapply(seq(2012, 2016), hosp_lin_f)
proc.time() - ptm01
hosp_lin <- as.data.frame(data.table::rbindlist(hosp_lin))

# Reshape to get indicator in a single column
hosp_lin <- melt(hosp_lin,
                 id.vars = c("enrtype", "agegp", "gender", "ethn",
                             "agency", "dual", "voucher", "subsidy",
                             "operator", "portfolio", "length", "year"),
                 variable.name = "indicator", value.name = "count")
hosp_lin <- hosp_lin %>%
  mutate(popsource = "lin") %>%
  select(popsource, year, indicator, enrtype:count)

# Relabel with text descriptions
hosp_lin <- relabel_f(hosp_lin)

hosp_lin <- hosp_lin %>%
  mutate(
    indicator = case_when(
      indicator == "hosp_pers" ~ "Persons with hospitalization_lin",
      indicator == "hosp_cnt" ~ "Hospitalizations_lin"
    )
  )

### ED visits
ptm01 <- proc.time()
ed_lin <- lapply(seq(2012, 2016), ed_lin_f)
proc.time() - ptm01
ed_lin <- as.data.frame(data.table::rbindlist(ed_lin))

# Reshape to get indicator in a single column
ed_lin <- melt(ed_lin,
               id.vars = c("enrtype", "agegp", "gender", "ethn",
                           "agency", "dual", "voucher", "subsidy",
                           "operator", "portfolio", "length", "year"),
               variable.name = "indicator", value.name = "count")
ed_lin <- ed_lin %>%
  mutate(popsource = "lin") %>%
  select(popsource, year, indicator, enrtype:count)

# Relabel with text descriptions
ed_lin <- relabel_f(ed_lin)

ed_lin <- ed_lin %>%
  mutate(
    indicator = case_when(
      indicator == "ed_pers" ~ "Persons with ED visits_lin",
      indicator == "ed_cnt" ~ "ED visits_lin",
      indicator == "ed_avoid" ~ "Avoidable ED visits_lin"
    )
  )




#### COMBINE DATA AND EXPORT ####
health_events_am <- bind_rows(hosp_pers, hosp_cnt, ed_pers, ed_cnt, ed_avoid, inj_cnt) %>%
  mutate(popsource = "am") %>%
  select(popsource, year, indicator, enrtype:count)

health_events_am <- relabel_f(health_events_am)
write.xlsx(health_events_am, paste0(housing_path, "/OrganizedData/Summaries/claims_Tableau_v9_health_events_", Sys.Date(), ".xlsx"))


health_events <- bind_rows(conditions_lin, hosp_lin, ed_lin, health_events_am)

write.xlsx(health_events, paste0(housing_path, "/OrganizedData/Summaries/claims_Tableau_v9_health_events_am and lin_", Sys.Date(), ".xlsx"))


# ZIP events
acute_events_zip <- bind_rows(hosp_pers_zip_a, hosp_pers_zip_c, 
                              hosp_cnt_zip_a, hosp_cnt_zip_c, 
                              ed_pers_zip_a, ed_pers_zip_c,
                              ed_cnt_zip_a, ed_cnt_zip_c,
                              ed_avoid_zip_a, ed_avoid_zip_c,
                              inj_cnt_zip_a, inj_cnt_zip_c) %>%
  mutate(popsource = "am") %>%
  select(popsource, year:count) %>%
  arrange(indicator, year, zipcode, agency)
# Apply suppression
acute_events_zip <- pop_suppress_f(acute_events_zip, agency = agency,
                                   group_var = c("indicator", "year", "zipcode"),
                                   pop = count)

write.xlsx(acute_events_zip, paste0(housing_path, "/OrganizedData/Summaries/claims_Tableau_v9_health_events zip_", Sys.Date(), ".xlsx"))

rm(conditions_lin)
rm(hosp_lin)
rm(ed_lin)
rm(hosp_pers)
rm(hosp_cnt)
rm(ed_pers)
rm(ed_cnt)
rm(ed_avoid)
rm(inj_cnt)
rm(health_events_am)
rm(health_events)
gc()

rm(hosp_pers_zip_a)
rm(hosp_pers_zip_c)
rm(hosp_cnt_zip_a)
rm(hosp_cnt_zip_c)
rm(ed_pers_zip_a)
rm(ed_pers_zip_c)
rm(ed_cnt_zip_a)
rm(ed_cnt_zip_c)
rm(ed_avoid_zip_a)
rm(ed_avoid_zip_c)
rm(inj_cnt_zip_a)
rm(inj_cnt_zip_c)
rm(acute_events_zip)
gc()
