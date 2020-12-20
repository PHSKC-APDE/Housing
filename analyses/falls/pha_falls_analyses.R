#### ANALYSES OF FALLS IN THE LINKED HOUSING/MEDICAID/MEDICARE DATA ####
#
# Alastair Matheson (PHSKC-APDE)
# 2016-02
#
###############################################################################

#### SET UP LIBRARIES ETC. ####
options(max.print = 350, tibble.print_max = 30, scipen = 999)

library(odbc) # Used to connect to SQL server
library(openxlsx) # Used to import/export Excel files
library(lubridate) # Used to manipulate dates
library(tidyverse) # Used to manipulate data
library(claims) # Used to aggregate data
library(glue) # Used to safely make SQL queries
library(injurymatrix)


db_apde <- dbConnect(odbc(), "PH_APDEStore51")
db_claims <- dbConnect(odbc(), "PHClaims51")

housing_path <- "//phdata01/DROF_DATA/DOH DATA/Housing/Organized_data"


#### FUNCTIONS ####
# Denom data function
acute_denom_f <- function(year = 2016) {
  output <- dbGetQuery(db_apde, 
                       glue::glue_sql("SELECT [year], id_apde, enroll_type, age_yr, gender_me, 
                             race_eth_me, mco_id, pha_agency, 
                             CASE WHEN pha_subsidy = 'TENANT BASED/SOFT UNIT' THEN 'HCV'
                               WHEN pha_subsidy = 'HARD UNIT' THEN 'PH'
                               ELSE 'Non-PHA' END AS pha_subsidy,
                             tpl, full_benefit, geo_zip, 
                             CASE WHEN dual = 1 OR apde_dual = 1 THEN 1 ELSE 0 END AS dual, 
                             pt
                             FROM stage.mcaid_mcare_pha_elig_calyear 
                             WHERE pop_ever = 1 AND [year] = {year} AND
                                      (
        (mcaid = 1 AND mcare = 0 AND full_benefit = 1 AND tpl = 0 AND dual = 0) OR 
        (mcare = 1 AND geo_kc = 1 AND mcaid = 0 AND part_a = 1 AND part_b = 1 AND [partial] = 0) OR
        (mcaid = 1 AND mcare = 1 AND ((full_benefit = 1 AND tpl = 0) OR (part_a = 1 AND part_b = 1)))
        )", 
                                      .con = db_Claims))
  
  output <- output %>%
    mutate(           age_yr_fall = cut(age_yr, 
                                        breaks = c(0, 9.999, 17.999, 29.999, 49.999, 64.999, 74.999, 180), 
                                        include.lowest = F, 
                                        labels = c("<10", "10-17", "18-29", "30-49", "50-64", "65-74", "75+"), 
                                        ordered_result = T),
           # Make pha_agency and pha_subsidy factors for better sorting
           pha_agency = fct_relevel(pha_agency, "Non-PHA", "KCHA", "SHA"),
           pha_subsidy = fct_relevel(pha_subsidy, "Non-PHA", "HCV", "PH"),
           # Make new year field with greater explanations
           year_text = case_when(
             year %in% c(2014:2016) ~ paste0(year, " (Medicaid and Medicare)"),
             year %in% c(2012:2013, 2017:2019) ~ paste0(year, " (Medicaid only, non-dual)"))
    )
}


# Header function to pull out all ED/inpatient and professional claims
header_query_f <- function(year = 2016, primary_dx = F) {

    if (primary_dx == T) {
    dx_where <- glue_sql("AND icdcm_number = '01'", .con = db_claims)
  } else {
    dx_where <- DBI::SQL('')
  }
  
  sql_query <- glue_sql(
    "SELECT DISTINCT a.id_apde, a.source_desc, a.claim_type_id,
    a.first_service_date, YEAR(a.first_service_date) AS year, 
    a.ed, a.inpatient, a.intent, a.mechanism, 
    b.fall, b.fall_unintent, b.fall_suicide, b.fall_homicide, b.fall_undetermined, 
    c.enroll_type, c.mco_id, c.pha_agency, c.pha_subsidy, c.dual, c.geo_zip, 
    d.dob, d.start_housing, d.gender_me, d.race_eth_me 
    FROM
    (SELECT id_apde, source_desc, claim_header_id, claim_type_id, 
      first_service_date, ed, inpatient, intent, mechanism  
      FROM PHClaims.final.mcaid_mcare_claim_header
      WHERE year(first_service_date) = {year}) a
    INNER JOIN
    (SELECT claim_header_id, icdcm_norm, icdcm_version,
    CASE wHEN icdcm_norm LIKE 'W0%' OR icdcm_norm LIKE 'W1%' OR icdcm_norm = 'X80' OR
      icdcm_norm = 'Y01' OR icdcm_norm = 'Y30' THEN 1 ELSE 0 END AS fall,
    CASE WHEN icdcm_norm LIKE 'W0%' OR icdcm_norm LIKE 'W1%' THEN 1 ELSE 0 END AS fall_unintent,
    CASE WHEN icdcm_norm = 'X80' THEN 1 ELSE 0 END AS fall_suicide,
    CASE WHEN icdcm_norm = 'Y01' THEN 1 ELSE 0 END AS fall_homicide,
    CASE WHEN icdcm_norm = 'Y30' THEN 1 ELSE 0 END AS fall_undetermined
     FROM [PHClaims].[final].[mcaid_mcare_claim_icdcm_header]
     WHERE (icdcm_norm LIKE 'W0%' OR icdcm_norm LIKE 'W1%' OR icdcm_norm = 'X80' OR
      icdcm_norm = 'Y01' OR icdcm_norm = 'Y30') 
     {dx_where}) b
    ON a.claim_header_id = b.claim_header_id
    INNER JOIN
    (SELECT id_apde, from_date, to_date, 
      CASE WHEN mcaid = 0 AND mcare = 0 AND pha = 1 THEN 'h'
      WHEN mcaid = 1 AND mcare = 0 AND pha = 1 THEN 'hmd'
      WHEN mcaid = 0 AND mcare = 1 AND pha = 1 THEN 'hme'
      WHEN mcaid = 1 AND mcare = 0 AND pha = 0 THEN 'md'
      WHEN mcaid = 0 AND mcare = 1 AND pha = 0 THEN 'me'
      WHEN mcaid = 1 AND mcare = 1 AND pha = 0 THEN 'mm'
      WHEN mcaid = 1 AND mcare = 1 AND pha = 1 THEN 'a' END AS enroll_type, 
      mco_id, 
      pha_agency, 
      CASE WHEN pha_subsidy = 'TENANT BASED/SOFT UNIT' THEN 'HCV'
                               WHEN pha_subsidy = 'HARD UNIT' THEN 'PH'
                               ELSE 'Non-PHA' END AS pha_subsidy,
      CASE WHEN dual = 1 OR apde_dual = 1 THEN 1 ELSE 0 END AS dual, 
      full_benefit, geo_zip
      FROM PH_APDEStore.final.mcaid_mcare_pha_elig_timevar
      WHERE (
        (mcaid = 1 AND mcare = 0 AND full_benefit = 1 AND tpl = 0 AND dual = 0) OR 
        (mcare = 1 AND geo_kc = 1 AND mcaid = 0 AND part_a = 1 AND part_b = 1 AND [partial] = 0) OR
        (mcaid = 1 AND mcare = 1 AND ((full_benefit = 1 AND tpl = 0) OR (part_a = 1 AND part_b = 1)))
        )
      ) c
    ON a.id_apde = c.id_apde AND
    a.first_service_date >= c.from_date AND a.first_service_date <= c.to_date
    LEFT JOIN
    (SELECT id_apde, dob, start_housing, gender_me, race_eth_me
      FROM PH_APDEStore.final.mcaid_mcare_pha_elig_demo) d
    ON COALESCE(a.id_apde, c.id_apde) = d.id_apde",
    .con = db_claims)
  
  output <- dbGetQuery(db_claims, sql_query) %>%
    # Format vars
    mutate_at(vars(first_service_date, dob), list( ~ as.Date(.))) %>%
    # Recode output
    mutate(dual = if_else(dual == 1, "Dual eligible", "Not dual eligible"),
           # Add in plain-text enrollment
           enroll_type_text = case_when(
             enroll_type == "a" ~ "Housing, Medicaid, and Medicare (dual)",
             enroll_type == "hmd" & dual == 1 & !is.na(dual) ~ "Housing, Medicaid, and Medicare (dual)",
             enroll_type == "hmd" & (dual != 1 | is.na(dual)) ~ "Housing and Medicaid (not dual)",
             enroll_type == "hme" ~ "Housing and Medicare (not dual)",
             enroll_type == "md" & (dual != 1 | is.na(dual)) ~ "Medicaid only (not dual)",
             enroll_type == "md" & dual == 1 & !is.na(dual) ~ "Medicaid and Medicare (dual)",
             enroll_type == "mm" ~ "Medicaid and Medicare (dual)",
             enroll_type == "me" ~ "Medicare only (not dual)",
             enroll_type == "h" ~ "Housing only"),
           # Redo age groups to be more relevant to asthma
           age_yr = floor(interval(start = dob, end = first_service_date) / years(1)),
           age_yr_fall = cut(age_yr, 
                               breaks = c(0, 9.999, 17.999, 29.999, 49.999, 64.999, 74.999, 180), 
                               include.lowest = F, 
                               labels = c("<10", "10-17", "18-29", "30-49", "50-64", "65-74", "75+"), 
                               ordered_result = T),
           # Make pha_agency and pha_subsidy factors for better sorting
           # pha_agency = fct_relevel(pha_agency, "Non-PHA", "KCHA", "SHA"),
           # pha_subsidy = fct_relevel(pha_subsidy, "Non-PHA", "HCV", "PH"),
           # Make new year field with greater explanations
           year_text = case_when(
             year %in% c(2014:2016) ~ paste0(year, " (Medicaid and Medicare)"),
             year %in% c(2012:2013, 2017:2019) ~ paste0(year, " (Medicaid only, non-dual)")),
           time_housing_yr = 
             round(interval(start = start_housing, end = first_service_date) / years(1), 1),
           time_housing = case_when(
             is.na(pha_agency) | pha_agency == "Non-PHA" ~ "Non-PHA",
             time_housing_yr < 3 ~ "<3 years",
             between(time_housing_yr, 3, 5.99) ~ "3 to <6 years",
             time_housing_yr >= 6 ~ "6+ years",
             TRUE ~ "Unknown"))
  
  return(output)
}


#### BRING IN DATA ####
# Denom
acute_denom_16 <- acute_denom_f(year = 2016)

# Numerator
system.time(header_16 <- header_query_f(year = 2016))

acute_16 <- header_16 %>%
  filter(ed == 1 | inpatient == 1) %>%
  mutate(type = case_when(
    ed == 1 ~ "ED visits",
    inpatient == 1 ~ "Hospitalizations")) %>%
  mutate(event = 1)


### Set up empty shell of all permutations for acute conditions
acute_denom_shell_pha <- expand.grid(type = c("ED visits", "Hospitalizations"),
                                     pha_agency = unique(acute_denom_16$pha_agency),
                                     age_yr_fall = unique(acute_denom_16$age_age_yr_fallyr_asthma)) %>%
  arrange(type, pha_agency, age_yr_fall) %>%
  filter(!is.na(age_yr_fall))

acute_denom_shell_sub <- expand.grid(type = c("ED visits", "Hospitalizations"),
                                     pha_subsidy = unique(acute_denom_16$pha_subsidy),
                                     age_yr_fall = unique(acute_denom_16$age_yr_fall)) %>%
  arrange(type, pha_subsidy, age_yr_fall) %>%
  filter(!is.na(age_yr_fall))

acute_denom_shell_all <- expand.grid(type = c("ED visits", "Hospitalizations"),
                                     pha_subsidy = unique(acute_denom_16$pha_subsidy),
                                     age_yr_fall = unique(acute_denom_16$age_yr_fall),
                                     race_eth_me = unique(acute_denom_16$race_eth_me),
                                     gender_me = unique(acute_denom_16$gender_me)) %>%
  arrange(type, pha_subsidy, age_yr_fall, race_eth_me, gender_me)


#### ACUTE EVENTs ####
#### Broken down by subsidy type ####
### Denominator
denom_16_age_sub <- acute_denom_16 %>%
  filter(!is.na(age_yr_fall) & year == 2016) %>%
  group_by(pha_subsidy, age_yr_fall) %>%
  summarise(denominator = sum(pt), denominator_yr = sum(pt) / 365,
            denominator_mth = denominator_yr * 12)

### Numerator
acute_16_age_sub <- acute_16 %>%
  filter(!is.na(age_yr_fall) & year == 2016) %>%
  group_by(type, pha_subsidy, age_yr_fall) %>%
  summarise(numerator = sum(event)) %>%
  ungroup()

acute_16_age_sub <- left_join(acute_denom_shell_sub, denom_16_age_sub, by = c("pha_subsidy", "age_yr_fall")) %>%
  left_join(., acute_16_age_sub, by = c("type", "pha_subsidy", "age_yr_fall")) %>%
  mutate(rate = numerator / denominator_mth * 1000) %>%
  # Suppress small numbers
  mutate_at(vars(numerator, rate), list(~ ifelse(numerator <= 10 | is.na(numerator), 0, .))) %>%
  # Add in CIs
  rowwise() %>%
  mutate(lower_bound = ifelse(numerator == 0, NA_real_, 
                              round(prop.test(numerator, denominator_mth, 
                                              correct = F)$conf.int[1] * 1000, 3)),
         upper_bound = ifelse(numerator == 0, NA_real_, 
                              round(prop.test(numerator, denominator_mth, 
                                              correct = F)$conf.int[2] * 1000, 3))) %>%
  ungroup()

# Make graph
acute_16_age_sub_g <- acute_16_age_sub %>%
  filter(type == "ED visits" & !is.na(age_yr_fall) & !age_yr_fall %in% c("65-74", "75+")) %>%
  ggplot(aes(fill = pha_subsidy, y = rate, x = age_yr_fall)) + 
  geom_bar(position = "dodge", stat = "identity", color = "#333333") +
  geom_text(aes(label = ifelse(numerator == 0, "*", "")), 
            position = position_dodge(width = 0.9), vjust = -0.05) + 
  scale_fill_manual(values = c("#79706e", "#2c7bb6", "#87d180"), name = "PHA subsidy type") + 
  xlab("Age (years)") + ylab("Rate per 1,000 member-months") + 
  ggtitle("ED visits from falls (primary dx) (2016, non-dual Medicaid members only)") +
  theme_ipsum_ps() +
  theme(panel.grid.major.y = element_line(color = "grey90"),
        panel.grid.major.x = element_blank(), 
        panel.grid.minor = element_blank())