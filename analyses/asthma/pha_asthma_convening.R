###############################################################################
# Data used for the DASHH asthma convening
#
# Alastair Matheson (PHSKC-APDE)
# 2020-02
#
###############################################################################

### Make sure the following tables are up to date:
# stage/final.perf_enroll_denom
# stage/final.mcaid_mcare_pha_elig_calyear
# 


#### SECTIONS:
# General demogs of PHA residents
# CCW asthma demogs
# CCW asthma spatial
# Asthma-related ED/hosp visits (among all CCW asthma?)
# HEDIS asthma (1 and 2 year)
# AMR


#### Set up global parameters etc. ---------------------------------------------
options(max.print = 350, tibble.print_max = 50, scipen = 999, warning.length = 8170,
        knitr.kable.NA = '')

pacman::p_load(odbc, housing, claims, lubridate, tidyverse, glue, aod, broom, readxl, 
               spdep, SpatialEpi, INLA, sf, ggrepel, knitr, kableExtra, rmarkdown,
               scales, ggplot2, hrbrthemes, treemapify)

db_apde <- dbConnect(odbc(), "PH_APDEStore51")
db_claims <- dbConnect(odbc(), "PHClaims51")

dashh_2_path <- "C:/Users/mathesal/King County/Laurent, Amy - DASHH-External/DASHH2.0"
spatial_dir <- paste0(dashh_2_path, "/Deep Dive/Asthma/spatial")
housing_dir <- "//phdata01/DROF_DATA/DOH DATA/Housing"


#### BRING IN RELEVANT DATA ----------------------------------------------------
#### DEMOGRAPHICS ####
### Linked housing/Medicaid data
# Only need the data where people are allocated into a single bucket
mcaid_mcare_pha_elig_demo <- dbGetQuery(db_apde, 
  "SELECT * FROM stage.mcaid_mcare_pha_elig_calyear WHERE pop = 1")

# Add in additional info/formating that can be useful
mcaid_mcare_pha_elig_demo <- mcaid_mcare_pha_elig_demo %>%
  mutate(
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
    age_yr_asthma = cut(age_yr, 
                        breaks = c(0, 4.999, 9.999, 17.999, 29.999, 49.999, 64.999, 74.999, 180), 
                        include.lowest = F, 
                        labels = c("<5", "5-9", "10-17", "18-29", "30-49", "50-64", "65-74", "75+"), 
                        ordered_result = T),
    # Make pha_agency a factor for better sorting
    pha_agency = fct_relevel(pha_agency, "Non-PHA", "KCHA", "SHA"),
    # Rename subsidy types and make a factor
    pha_subsidy = case_when(
      pha_subsidy == "TENANT BASED/SOFT UNIT" ~ "HCV",
      pha_subsidy == "HARD UNIT" ~ "PH",
      TRUE ~ pha_subsidy),
    pha_subsidy = fct_relevel(pha_subsidy, "Non-PHA", "HCV", "PH"),
    # Make new year field with greater explanations
    year_text = case_when(
      year %in% c(2014:2016) ~ paste0(year, " (Medicaid and Medicare)"),
      year %in% c(2012:2013, 2017:2019) ~ paste0(year, " (Medicaid only, non-dual)"))
  )


#### CHRONIC CONDITIONS ####
### CCW asthma
ccw_denom <- dbGetQuery(db_claims,
"SELECT  [year], [id_apde], SUM(pt) AS pt_inc
  FROM [PH_APDEStore].[stage].[mcaid_mcare_pha_elig_calyear]
  where pop_ever = 1 AND ((year >= 2017 AND age_yr < 65) OR year < 2017) AND
  ((enroll_type IN ('hmd', 'md') AND full_benefit = 1 AND tpl = 0 AND dual = 0) OR 
  (enroll_type IN ('hme', 'me') AND part_a = 1 AND part_b = 1 AND [partial] = 0) OR
  enroll_type IN ('a', 'mm') AND ((part_a = 1 AND part_b = 1 AND [partial] = 0) OR full_benefit = 1))
  group by [year], id_apde")

# Restrict to 11+ months of coverage
ccw_denom <- ccw_denom %>%
  filter(year %in% c(2012, 2016, 2020) & pt_inc/366 > 11/12 | 
           year %in% c(2012:2015, 2017:2019) & pt_inc/365 > 11/12)


ccw_asthma <- dbGetQuery(db_claims, "SELECT * FROM final.mcaid_mcare_claim_ccw
                         WHERE ccw_desc = 'ccw_asthma'")
ccw_asthma <- ccw_asthma %>% 
  mutate(id_apde = as.integer(id_apde)) %>%
  mutate_at(vars(from_date, to_date), list( ~ as.Date(.)))

# Reshape ccw_asthma to make it easier to join to demog table
ccw_asthma_long <- expand.grid(id_apde = unlist(distinct(ccw_asthma, id_apde)),
                               year = seq(2012, 2018)) %>%
  left_join(., ccw_asthma, by = "id_apde") %>%
  filter(year >= year(from_date) & year <= year(to_date)) %>%
  mutate(asthma = 1L) %>%
  select(-from_date, -to_date, -last_run) %>%
  distinct()


### HEDIS AMR
# Bring in denominator
amr_denom <- dbGetQuery(db_claims,
                        "SELECT b.id_apde, 'enroll_flag' = 1
                        FROM
                        (SELECT id_mcaid, year_month, end_month_age 
                        FROM [PHClaims].[stage].[mcaid_perf_enroll_denom]
                        WHERE full_benefit_t_12_m >= 11 AND dual_t_12_m = 0 AND 
                        end_month_age >= 5 AND end_month_age < 65 AND
                        year_month = 201812) a
                        LEFT JOIN
                        (SELECT id_apde, id_mcaid FROM final.xwalk_apde_mcaid_mcare_pha) b
                        ON a.id_mcaid = b.id_mcaid")


# Need to join to xwalk to get id_apde
amr_1_year <- dbGetQuery(db_claims,
                         "SELECT b.id_apde, a.end_year_month, a.numerator, a.denominator
                         FROM
                         (SELECT id_mcaid, end_year_month, numerator, denominator
                         FROM stage.mcaid_perf_measure
                         WHERE measure_id = 20 AND end_year_month = '201812') a
                         LEFT JOIN
                         (SELECT id_apde, id_mcaid FROM final.xwalk_apde_mcaid_mcare_pha) b
                         ON a.id_mcaid = b.id_mcaid")

amr <- dbGetQuery(db_claims,
                  "SELECT b.id_apde, a.end_year_month, a.numerator, a.denominator
                         FROM
                         (SELECT id_mcaid, end_year_month, numerator, denominator
                         FROM stage.mcaid_perf_measure
                         WHERE measure_id = 19 AND end_year_month = '201812') a
                         LEFT JOIN
                         (SELECT id_apde, id_mcaid FROM final.xwalk_apde_mcaid_mcare_pha) b
                         ON a.id_mcaid = b.id_mcaid")


#### ACUTE EVENTS ####
acute_query_f <- function(type = c("ed", "hosp"), year = 2018, primary_dx = T) {
  
  type <- match.arg(type)
  
  if (type == "ed") {
    top_vars <- glue_sql("a.ed ", .con = db_claims)
    vars <- glue_sql("ed ", .con = db_claims)
    where <- glue_sql("ed = 1 AND year(first_service_date) = {year}", .con = db_claims)
  } else if (type == "hosp") {
    top_vars <- glue_sql("a.inpatient ", .con = db_claims)
    vars <- glue_sql("inpatient ", .con = db_claims)
    where <- glue_sql("inpatient = 1 AND year(first_service_date) = {year}", .con = db_claims)
  }
  
  if (primary_dx == T) {
    dx_where <- glue_sql("WHERE icdcm_number = '01'", .con = db_claims)
  } else {
    dx_where <- DBI::SQL('')
  }
  
  sql_query <- glue_sql(
    "SELECT DISTINCT a.id_apde, a.source_desc, a.first_service_date, 
    YEAR(a.first_service_date) AS year, {top_vars}, c.ccw_asthma, 
    d.enroll_type, d.mco_id, d.pha_agency, d.pha_subsidy, d.dual, d.geo_zip, 
    e.dob, e.start_housing, e.gender_me, e.race_eth_me 
    FROM
    (SELECT id_apde, source_desc, claim_header_id, 
      first_service_date, {vars}
      FROM PHClaims.final.mcaid_mcare_claim_header
      WHERE {where}) a
    LEFT JOIN
    (SELECT claim_header_id, icdcm_norm, icdcm_version
     FROM [PHClaims].[final].[mcaid_mcare_claim_icdcm_header]
     {dx_where}) b
    ON a.claim_header_id = b.claim_header_id
    INNER JOIN
    (SELECT dx, dx_ver, ccw_asthma FROM [PHClaims].[ref].[dx_lookup]
     WHERE ccw_asthma = 1) c
    ON b.icdcm_norm = c.dx AND b.icdcm_version = c.dx_ver
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
      ) d
    ON a.id_apde = d.id_apde AND
    a.first_service_date >= d.from_date AND a.first_service_date <= d.to_date
    LEFT JOIN
    (SELECT id_apde, dob, start_housing, gender_me, race_eth_me
      FROM PH_APDEStore.final.mcaid_mcare_pha_elig_demo) e
    ON COALESCE(a.id_apde, d.id_apde) = e.id_apde",
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
           age_yr_asthma = cut(age_yr, 
                               breaks = c(0, 4.999, 9.999, 17.999, 29.999, 49.999, 64.999, 74.999, 180), 
                               include.lowest = F, 
                               labels = c("<5", "5-9", "10-17", "18-29", "30-49", "50-64", "65-74", "75+"), 
                               ordered_result = T),
           age_yr_asthma_u65 = ifelse(age_yr_asthma %in% c("65-74", "75+"), NA, age_yr_asthma),
           # Make pha_agency and pha_subsidy factors for better sorting
           pha_agency = fct_relevel(pha_agency, "Non-PHA", "KCHA", "SHA"),
           pha_subsidy = fct_relevel(pha_subsidy, "Non-PHA", "HCV", "PH"),
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

hosp_18 <- acute_query_f(type = "hosp", year = 2018)
ed_18 <- acute_query_f(type = "ed", year = 2018)
acute_18 <- bind_rows(mutate(ed_18, type = "ED visits"),
                      mutate(hosp_18, type = "Hospitalizations")) %>%
  mutate(event = 1)

hosp_18_all_dx <- acute_query_f(type = "hosp", year = 2018, primary_dx = F)
ed_18_all_dx <- acute_query_f(type = "ed", year = 2018, primary_dx = F)
acute_18_all_dx <- bind_rows(mutate(ed_18_all_dx, type = "ED visits"),
                             mutate(hosp_18_all_dx, type = "Hospitalizations")) %>%
  mutate(event = 1)


acute_denom_f <- function(year = 2018) {
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
    mutate(age_yr_asthma = cut(age_yr, 
                               breaks = c(0, 4.999, 9.999, 17.999, 29.999, 49.999, 64.999, 74.999, 180), 
                               include.lowest = F, 
                               labels = c("<5", "5-9", "10-17", "18-29", "30-49", "50-64", "65-74", "75+"), 
                               ordered_result = T),
           age_yr_asthma_u65 = ifelse(age_yr_asthma %in% c("65-74", "75+"), NA, age_yr_asthma),
           # Make pha_agency and pha_subsidy factors for better sorting
           pha_agency = fct_relevel(pha_agency, "Non-PHA", "KCHA", "SHA"),
           pha_subsidy = fct_relevel(pha_subsidy, "Non-PHA", "HCV", "PH"),
           # Make new year field with greater explanations
           year_text = case_when(
             year %in% c(2014:2016) ~ paste0(year, " (Medicaid and Medicare)"),
             year %in% c(2012:2013, 2017:2019) ~ paste0(year, " (Medicaid only, non-dual)"))
           )
}

acute_denom_18 <- acute_denom_f(year = 2018)


#### SPATIAL DATA ####
### King County ZIPs
zip <- dbGetQuery(db_claims,
                  "SELECT zip_code as zip_c, city 
                 FROM ref.apcd_zip WHERE county_name = 'King' and state = 'WA'")

### Shape file of ZIPs in King County 
# Bring in both one with and without water trim because we want to 
# identify neighboring ZIPs)
kc_zip <- read_sf("//gisdw/kclib/Plibrary2/admin/shapes/polygon/zipcode.shp")
kc_zip_trim <- read_sf("//gisdw/kclib/Plibrary2/admin/shapes/polygon/zipcode_shore.shp")
# Set CRS
kc_zip <- st_transform(kc_zip, 4326)
kc_zip_trim <- st_transform(kc_zip_trim, 4326)
# Restrict to KC ZIPs
kc_zip_trim <- kc_zip_trim %>% filter(COUNTY == "033")


### Portfolios
# Bring in lat/lon of major portfolios
portfolios <- read_xlsx(file.path(housing_dir, "Organized_data", "Portfolio addresses.xlsx"))
portfolios <- st_as_sf(x = portfolios, coords = c("Lon", "Lat"), remove = F, crs = 4326)


#### GENERAL SETTING UP OF DATA ------------------------------------------------
### Join long asthma to demogs
ccw_asthma_demog <- mcaid_mcare_pha_elig_demo %>% 
  inner_join(., ccw_denom, by = c("year", "id_apde")) %>%
  left_join(., ccw_asthma_long, by = c("id_apde", "year")) %>%
  mutate(asthma = replace_na(asthma, 0))

# Check results
ccw_asthma_demog %>% filter(year == 2016) %>%
  group_by(pha_agency) %>%
  summarise(count = sum(asthma), rows = n(), total = n_distinct(id_apde)) %>%
  ungroup() %>%
  mutate(prop = round(count / total * 1000, 4))

### Join AMR to demogs
housing_amr <- mcaid_mcare_pha_elig_demo %>%
  filter(year == 2018) %>%
  left_join(., amr_denom, by = "id_apde") %>%
  left_join(., dplyr::select(amr, -end_year_month), by = "id_apde") %>%
  left_join(., dplyr::select(amr_1_year, -end_year_month), by = "id_apde") %>%
  rename(denominator = denominator.x,
         numerator = numerator.x,
         denominator_1yr = denominator.y,
         numerator_1yr = numerator.y) %>%
  replace_na(., list(denominator = 0, numerator = 0, 
                     denominator_1yr = 0, numerator_1yr = 0,
                     enroll_flag = 0))


### Set up empty shell of all permutations for acute conditions
acute_denom_shell_pha <- expand.grid(type = c("ED visits", "Hospitalizations"),
                                 pha_agency = unique(acute_denom_18$pha_agency),
                                 age_yr_asthma = unique(acute_denom_18$age_yr_asthma)) %>%
  arrange(type, pha_agency, age_yr_asthma) %>%
  filter(!is.na(age_yr_asthma))

acute_denom_shell_sub <- expand.grid(type = c("ED visits", "Hospitalizations"),
                                     pha_subsidy = unique(acute_denom_18$pha_subsidy),
                                     age_yr_asthma = unique(acute_denom_18$age_yr_asthma)) %>%
  arrange(type, pha_subsidy, age_yr_asthma) %>%
  filter(!is.na(age_yr_asthma))

acute_denom_shell_all <- expand.grid(type = c("ED visits", "Hospitalizations"),
                                     pha_subsidy = unique(acute_denom_18$pha_subsidy),
                                     age_yr_asthma = unique(acute_denom_18$age_yr_asthma),
                                     race_eth_me = unique(acute_denom_18$race_eth_me),
                                     gender_me = unique(acute_denom_18$gender_me)) %>%
  arrange(type, pha_subsidy, age_yr_asthma, race_eth_me, gender_me)


#### GENERAL DEMOGS OF PHA RESIDENTS -------------------------------------------
# Use the Tableau dashboard for the PHA breakdown

### Breakdown of Mcaid/Mcare by PHA
mcaid_demog <- mcaid_mcare_pha_elig_demo %>% filter(mcaid == 1) %>% 
  group_by(year, enroll_type_text) %>% 
  summarise(count = n(), countd = n_distinct(id_apde)) %>%
  group_by(year) %>%
  mutate(total = sum(count), pct = round(count / total * 100, 3)) %>% 
  ungroup() %>%
  mutate(pha_mcaid = ifelse(enroll_type_text %in% 
                              c("Housing, Medicaid, and Medicare (dual)", 
                                "Housing and Medicaid (not dual)"), "PHA", "Non-PHA"),
         year_n = glue("{year} (N = {format(total, big.mark = ',')})"))

mcare_demog <- mcaid_mcare_pha_elig_demo %>% filter(mcare == 1) %>% 
  group_by(year, enroll_type_text) %>% 
  summarise(count = n(), countd = n_distinct(id_apde)) %>%
  group_by(year) %>%
  mutate(total = sum(count), pct = round(count / total * 100, 3)) %>% 
  ungroup() %>%
  mutate(pha_mcare = ifelse(enroll_type_text %in% 
                              c("Housing, Medicaid, and Medicare (dual)", 
                                "Housing and Medicare (not dual)"), "PHA", "Non-PHA"),
         year_n = glue("{year} (N = {format(total, big.mark = ',')})"))

dual_demog <- mcaid_mcare_pha_elig_demo %>% 
  filter((mcaid == 1 & mcare == 1 & year < 2017) |
           (mcaid == 1 & dual == 1)) %>% 
  group_by(year, enroll_type_text) %>% 
  summarise(count = n(), countd = n_distinct(id_apde)) %>%
  group_by(year) %>%
  mutate(total = sum(count), pct = round(count / total * 100, 3)) %>% 
  ungroup() %>%
  mutate(year_n = glue("{year} (N = {format(total, big.mark = ',')})"))


### Show graphs
mcaid_demog_plot <- ggplot(mcaid_demog[mcaid_demog$year %in% c(2012, 2014, 2016, 2018), ], 
                           aes(area = count,
                               label = enroll_type_text,
                               #fill = pct,
                               subgroup = pha_mcaid)) + 
  #scale_fill_continuous(high = "#132B43", low = "#56B1F7") + 
  geom_treemap() + 
  geom_treemap_subgroup_border(color = "blue", size = 2) + 
  geom_treemap_subgroup_text(place = "bottom", fontface = "italic", grow = FALSE) +
  geom_treemap_text(color = "black", place = "topleft", grow = FALSE, reflow = TRUE)

mcaid_demog_plot + facet_wrap(~ year_n, ncol = 2)


mcare_demog_plot <- ggplot(mcare_demog[mcare_demog$year %in% c(2012, 2014, 2016), ], 
                           aes(area = count,
                               label = enroll_type_text,
                               #fill = pct,
                               subgroup = pha_mcare)) + 
  #scale_fill_continuous(high = "#132B43", low = "#56B1F7") + 
  geom_treemap() + 
  geom_treemap_subgroup_border(color = "blue", size = 2) + 
  geom_treemap_subgroup_text(place = "bottom", fontface = "italic", grow = FALSE) +
  geom_treemap_text(color = "black", place = "topleft", grow = FALSE, reflow = TRUE)

mcare_demog_plot + facet_wrap(~ year_n, ncol = 2)


dual_demog_plot <- ggplot(dual_demog[dual_demog$year %in% c(2012, 2014, 2016, 2018), ], 
                          aes(area = count,
                              label = enroll_type_text)) + 
  geom_treemap() + 
  geom_treemap_text(color = "black", place = "topleft", grow = FALSE, reflow = TRUE)

dual_demog_plot + facet_wrap(~ year_n, ncol = 2)


#### CCW ASTHMA DEMOGS ---------------------------------------------------------
# Pull in Tableau images?

# Make a function to avoid repeating code (WIP)
test_f <- function(group = c(year, year_text, age_yr_asthma)) {
  # Quosures
  group_quo <- enquos(group)
  
  print(group_quo)
  
  print(quo_text(group_quo))
  
  # df <- lapply(group_quo, function(x) {
  #   
  # })
  
  print(dplyr::select(ccw_asthma_demog, !!!group_quo) %>% head())
  
  
}

test_f()

### Generic function to summarise CCW data
ccw_summary <- function(df = ccw_asthma_demog, yr = 2018, 
                        group = c("year", "year_text", "age_yr_asthma")) {
  
  # Quosures
  group_quo <- enquos(group)
  
  lapply()
  
  df <- filter()
  
}

### Now run by age and PHA
# Split up 2016 and 2018 to account for different populations
ccw_asthma_age_sub_16 <- ccw_asthma_demog %>%
  filter(!is.na(age_yr_asthma) & year == 2016) %>%
  group_by(year, year_text, age_yr_asthma, pha_subsidy) %>%
  summarise(count = sum(asthma), total = n_distinct(id_apde)) %>%
  ungroup() %>%
  mutate(prop = round(count / total * 1000, 4))
ccw_asthma_age_sub_18 <- ccw_asthma_demog %>%
  filter(!is.na(age_yr_asthma) & year == 2018 & age_yr < 65 & dual != 1) %>%
  group_by(year, year_text, age_yr_asthma, pha_subsidy) %>%
  summarise(count = sum(asthma), total = n_distinct(id_apde)) %>%
  ungroup() %>%
  mutate(prop = round(count / total * 1000, 4))

ccw_asthma_age_sub <- bind_rows(ccw_asthma_age_sub_16, ccw_asthma_age_sub_18) %>%
  # Add in CIs
  rowwise() %>%
  mutate(lower_bound = ifelse(count == 0, NA_real_, 
                              round(prop.test(count, total, 
                                              correct = F)$conf.int[1] * 1000, 3)),
         upper_bound = ifelse(count == 0, NA_real_, 
                              round(prop.test(count, total, 
                                              correct = F)$conf.int[2] * 1000, 3))) %>%
  ungroup()

ccw_asthma_age_sub_g <- ccw_asthma_age_sub %>%
  filter(!is.na(age_yr_asthma) & 
           (year == 2016 | (year == 2018 & age_yr_asthma != "65-74" & age_yr_asthma != "75+"))) %>%
  ggplot(aes(fill = pha_subsidy, y = prop, x = age_yr_asthma)) + 
  geom_bar(position = "dodge", stat = "identity", color = "#333333") +
  scale_fill_manual(values = c("#79706e", "#2c7bb6", "#87d180"), name = "PHA subsidy type") + 
  xlab("Age (years)") + ylab("Number per 1,000") + 
  theme_ipsum_ps() +
  theme(panel.grid.major.y = element_line(color = "grey90"),
        panel.grid.major.x = element_blank(), 
        panel.grid.minor = element_blank())

ccw_asthma_age_sub_g + 
  geom_errorbar(aes(ymin = lower_bound, ymax = upper_bound), width = 0.2,
                position = position_dodge(width = 0.9)) +
  facet_wrap( ~ year_text, ncol = 1)


### Now run by age and PHA
# Split up 2016 and 2018 to account for different populations
ccw_asthma_age_pha_16 <- ccw_asthma_demog %>%
  filter(!is.na(age_yr_asthma) & year == 2016) %>%
  group_by(year, year_text, age_yr_asthma, pha_agency) %>%
  summarise(count = sum(asthma), total = n_distinct(id_apde)) %>%
  ungroup() %>%
  mutate(prop = round(count / total * 1000, 4))
ccw_asthma_age_pha_18 <- ccw_asthma_demog %>%
  filter(!is.na(age_yr_asthma) & year == 2018 & age_yr < 65 & dual != 1) %>%
  group_by(year, year_text, age_yr_asthma, pha_agency) %>%
  summarise(count = sum(asthma), total = n_distinct(id_apde)) %>%
  ungroup() %>%
  mutate(prop = round(count / total * 1000, 4))

ccw_asthma_age_pha <- bind_rows(ccw_asthma_age_pha_16, ccw_asthma_age_pha_18) %>%
  # Add in CIs
  rowwise() %>%
  mutate(lower_bound = ifelse(count == 0, NA_real_, 
                              round(prop.test(count, total, 
                                              correct = F)$conf.int[1] * 1000, 3)),
         upper_bound = ifelse(count == 0, NA_real_, 
                              round(prop.test(count, total, 
                                              correct = F)$conf.int[2] * 1000, 3))) %>%
  ungroup()

ccw_asthma_age_pha_g <- ccw_asthma_age_pha %>%
  filter(!is.na(age_yr_asthma) & 
           (year == 2016 | (year == 2018 & age_yr_asthma != "65-74" & age_yr_asthma != "75+"))) %>%
  ggplot(aes(fill = pha_agency, y = prop, x = age_yr_asthma)) + 
  geom_bar(position = "dodge", stat = "identity", color = "#333333") +
  scale_fill_manual(values = c("#79706e", "#2c7bb6", "#87d180"), name = "Agency") + 
  xlab("Age (years)") + ylab("Number per 1,000") + 
  theme_ipsum_ps() +
  theme(panel.grid.major.y = element_line(color = "grey90"),
        panel.grid.major.x = element_blank(), 
        panel.grid.minor = element_blank())

ccw_asthma_age_pha_g + facet_wrap( ~ year_text, ncol = 1)


# Now run by race and subsidy
# Split up 2016 and 2018 to account for different populations
ccw_asthma_race_sub_16 <- ccw_asthma_demog %>%
  filter(!is.na(race_eth_me) & !race_eth_me %in% c("Asian_PI", "Other", "Unknown") & 
           year == 2016) %>%
  group_by(year, year_text, race_eth_me, pha_subsidy) %>%
  summarise(count = sum(asthma), total = n_distinct(id_apde)) %>%
  ungroup() %>%
  mutate(prop = round(count / total * 1000, 4))
ccw_asthma_race_sub_18 <- ccw_asthma_demog %>%
  filter(!is.na(race_eth_me) & !race_eth_me %in% c("Asian_PI", "Other", "Unknown") & 
           year == 2018 & age_yr < 65 & dual != 1) %>%
  group_by(year, year_text, race_eth_me, pha_subsidy) %>%
  summarise(count = sum(asthma), total = n_distinct(id_apde)) %>%
  ungroup() %>%
  mutate(prop = round(count / total * 1000, 4))

ccw_asthma_race_sub <- bind_rows(ccw_asthma_race_sub_16, ccw_asthma_race_sub_18) %>%
  # Add in CIs
  rowwise() %>%
  mutate(lower_bound = ifelse(count == 0, NA_real_, 
                              round(prop.test(count, total, 
                                              correct = F)$conf.int[1] * 1000, 3)),
         upper_bound = ifelse(count == 0, NA_real_, 
                              round(prop.test(count, total, 
                                              correct = F)$conf.int[2] * 1000, 3))) %>%
  ungroup()

ccw_asthma_race_sub_g <- ccw_asthma_race_sub_18 %>%
  ggplot(aes(fill = pha_subsidy, y = prop, x = race_eth_me)) + 
  geom_bar(position = "dodge", stat = "identity", color = "#333333") +
  scale_fill_manual(values = c("#79706e", "#2c7bb6", "#87d180"), name = "PHA subsidy type") + 
  xlab("Race/ethnicity") + ylab("Number per 1,000") + 
  theme_ipsum_ps() +
  theme(panel.grid.major.y = element_line(color = "grey90"),
        panel.grid.major.x = element_blank(), 
        panel.grid.minor = element_blank())

ccw_asthma_race_sub_g + 
  geom_errorbar(aes(ymin = lower_bound, ymax = upper_bound), width = 0.2,
                position = position_dodge(width = 0.9)) +
  facet_wrap( ~ year_text, ncol = 1)


#### CCW ASTHMA WHAT IF --------------------------------------------------------
ccw_asthma_all_demog_18 <- ccw_asthma_demog %>%
  filter(year == 2018 & age_yr < 65 & dual != 1) %>%
  group_by(pha_subsidy, age_yr_asthma, race_eth_me, gender_me) %>%
  summarise(count = sum(asthma), total = n_distinct(id_apde)) %>%
  ungroup() %>%
  mutate(prop = count / total)

ccw_asthma_all_demog_18_expected <- ccw_asthma_all_demog_18 %>%
  left_join(., filter(ccw_asthma_all_demog_18, pha_subsidy == "Non-PHA") %>% 
              dplyr::select(age_yr_asthma, race_eth_me, gender_me, prop) %>%
              rename(prop_pha = prop),
            by = c("age_yr_asthma", "race_eth_me", "gender_me")) %>%
  mutate(expected = prop_pha * total)

ccw_asthma_all_demog_18_avoidable <- ccw_asthma_all_demog_18_expected %>%
  group_by(pha_subsidy) %>%
  summarise(expected = round(sum(expected)), count = sum(count)) %>%
  ungroup() %>%
  mutate(difference = count - expected,
         pct = round(difference / count * 100, 3))


#### CCW ASTHMA SPATIAL --------------------------------------------------------
#### SET UP A FUNCTION THAT CAN WORK OVER DIFFERENT YEARS ####
geo_setup_f <- function(yr = 2016, pha = F) {
  
  #### Restrict to relevant year ####
  if (pha == F & yr <= 2016) {
    df <- ccw_asthma_demog %>% filter(year == yr)
  } else if (pha == F & yr >= 2017) {
    df <- ccw_asthma_demog %>% filter(year == yr & age_yr < 65 & dual != 1)
  } else if (pha == T & yr <= 2016) {
    df <- ccw_asthma_demog %>% filter(year == yr & pha_agency %in% c("SHA", "KCHA"))
  } else if (pha == T & yr >= 2017) {
    df <- ccw_asthma_demog %>% filter(year == yr & age_yr < 65 & dual != 1 & 
                                        pha_agency %in% c("SHA", "KCHA"))
  }
 
  print(nrow(df))
  print(distinct(df, year))
  
  df <- df %>%
    mutate_at(vars(gender_me, race_me),
              list(~ ifelse(. == "Unknown", NA_character_, .))) %>%
    group_by(geo_zip, gender_me, race_me, age_yr_asthma) %>%
    summarise(asthma = sum(asthma, na.rm = T), pop = n()) %>%
    ungroup()
  
  
  #### Set up ZIPs and neighborhood matrix ####
  # Restrict to ZIPs in shapefile and KC
  zips <- inner_join(distinct(df, geo_zip), 
                     kc_zip %>% as.data.frame() %>% distinct(ZIPCODE),
                     by = c("geo_zip" = "ZIPCODE")) %>%
    inner_join(., zip, by = c("geo_zip" = "zip_c"))
  
  kc_zip_year <- kc_zip %>% filter(ZIPCODE %in% unlist(zips))
  df <- df %>% filter(geo_zip %in% unlist(zips))
  
  # Convert to spatial object to make neighborhood matrix
  kc_zip_year_sp <- as(kc_zip_year, "Spatial")
  
  # Make neighborhood matrix
  nb <- poly2nb(kc_zip_year_sp)
  nb2INLA(file.path(spatial_dir, "zip_neighbors.adj"), nb)
  graph_nb <- inla.read.graph(filename = file.path(spatial_dir, "zip_neighbors.adj"))
  
  
  #### Set up expected numbers ####
  # Make a matrix of all possible combinations
  geo_zip_matrix <- expand.grid(geo_zip = unlist(distinct(df, geo_zip)), 
                                gender_me = unlist(distinct(df, gender_me)),
                                race_me = unlist(distinct(df, race_me)), 
                                age_yr_asthma = unlist(distinct(df, age_yr_asthma)))
  
  geo_zip_matrix <- geo_zip_matrix[order(geo_zip_matrix$geo_zip, 
                                         geo_zip_matrix$gender_me, 
                                         geo_zip_matrix$race_me, 
                                         geo_zip_matrix$age_yr_asthma), ]
  
  
  # Join with actual data and make NA = 0
  df <- left_join(geo_zip_matrix, df,
                  by = c("geo_zip", "gender_me", "race_me", "age_yr_asthma")) %>%
    mutate_at(vars(asthma, pop), list( ~ ifelse(is.na(.), 0, .)))
  
  # Sum counts for each geo_zip
  df_tot <- df %>% group_by(geo_zip) %>% 
    summarise(count = sum(asthma), pop = sum(pop)) %>% ungroup() %>%
    mutate(year = yr)
  
  # Remove ZIPs with only one person in them
  df_tot <- df_tot %>% filter(pop > 1)
  
  # Join back to remove excluded zips
  df <- inner_join(df, distinct(df_tot, geo_zip), by = "geo_zip")
  
  # Remove combinations with 0 people in them
  # Otherwise cannot calculate expected number of people below
  strata_pop <- df %>% group_by(gender_me, race_me, age_yr_asthma) %>%
    summarise(pop = sum(pop)) %>% ungroup() %>%
    filter(pop > 0)
  
  df <- inner_join(df, distinct(strata_pop, gender_me, race_me, age_yr_asthma),
                   by = c("gender_me", "race_me", "age_yr_asthma"))
  
  # Set up expected number of cases
  # First sort data
  df <- df %>% arrange(geo_zip, gender_me, race_me, age_yr_asthma)
  # Calculate the number of strata
  strata <- df %>% distinct(gender_me, race_me, age_yr_asthma) %>% summarise(n = n()) %>% as.numeric()
  # Calculate the expected number of cases in each geo_zip based on pop variables
  # (indirect standardization)
  expected <- expected(population = df$pop, cases = df$asthma, n.strata = strata)
  
  if (length(expected) != nrow(df_tot)) {
    stop(glue("There were {length(expected)} expected values calculated and {nrow(df_tot)} ZIPs"))  
  } else {
    message(glue("Number of expected values calculated matched number of ZIPs ({length(expected)})"))
  }
  
  # Add expected values for each geo_zip
  df_tot$expected <- expected
  
  if (pha == F) {
    ### Add covariate with proportion in PHA
    pha_prop <- ccw_asthma_demog %>%
      filter(year %in% yr) %>%
      mutate(pha = ifelse(pha_agency == "Non-PHA", 0, 1)) %>%
      group_by(geo_zip) %>%
      summarise(pha_prop = mean(pha, na.rm = T)) %>%
      ungroup()
    
    df_tot <- left_join(df_tot, pha_prop, by = c("geo_zip"))
  }

  # Add indices for spatial (re_s) and non-spatial (re_f) random effects
  df_tot$re_s <- 1:nrow(df_tot)
  df_tot$re_f <- 1:nrow(df_tot)
  
  
  #### SET UP SPATIAL MODEL ####
  message("Running INLA model")
  if (pha == F) {
    inla_model <- inla(formula = count ~ 1 + #pha_prop + 
                         # Spatial random effects
                         f(re_s, model = 'besag', graph = graph_nb) + 
                         # Non-spatial random effects
                         f(re_f, model = 'iid'),
                       family = "poisson", data = df_tot,
                       E = expected, 
                       control.predictor = list(compute = TRUE))
  } else {
    inla_model <- inla(formula = count ~ 1 + 
                         f(re_s, model = 'besag', graph = graph_nb) + 
                         f(re_f, model = 'iid'),
                       family = "poisson", data = df_tot,
                       E = expected, 
                       control.predictor = list(compute = TRUE))
  }
  
  message("Check INLA results below and plot on the right")
  print(summary(inla_model))
  plot(inla_model)
  
  #### EXTRACT RESULTS ####
  df_map <- df_tot
  df_map$rr <- inla_model$summary.fitted.values[, "mean"]
  df_map$lb <- inla_model$summary.fitted.values[, "0.025quant"]
  df_map$ub <- inla_model$summary.fitted.values[, "0.975quant"]
  df_map <- df_map %>%
    mutate(significant = case_when(ub < 1 | lb > 1 ~ 1L,
                                   ub > 1 & lb < 1 ~ 0L,
                                   TRUE ~ NA_integer_),
           alpha = case_when(significant == 1 ~ 1,
                             significant == 0 ~ 0.5,
                             TRUE ~ 0),
           direction = case_when(significant == 1 & rr < 1 ~ -1L,
                                 significant == 1 & rr > 1 ~ 1L,
                                 TRUE ~ 0L))
  
  # Join to map with water trim
  zip_trim_map <- left_join(kc_zip_trim, df_map, by = c("ZIPCODE" = "geo_zip"))
  
  # Pull out ZIP centroids
  zip_centroids <- st_centroid(zip_trim_map)
  zip_centroids <- cbind(zip_trim_map, st_coordinates(st_centroid(zip_trim_map$geometry)))
  # Replace missing alphas
  zip_centroids <- zip_centroids %>% mutate(alpha = ifelse(is.na(alpha), 0, alpha))
  
  #### OUTPUT RESULTS ###
  output <- list(df = df_map, zip_trim_map = zip_centroids)
  return(output)
}


#### SET UP FUNCTIONS TO MAKE MAPS ####  
geo_zip_map_prop <- function(df, yr = 2018, title = F, label = F) {
  
  df <- df %>% 
    filter(year == yr) %>%
    group_by(geo_zip) %>%
    summarise(asthma = sum(asthma, na.rm = T), 
              pop = n(),
              prop = round(asthma / pop * 100, 3)) %>%
    ungroup() %>%
    filter(asthma > 10)
  
  # Join to map with water trim
  zip_trim_map <- left_join(kc_zip_trim, df, by = c("ZIPCODE" = "geo_zip"))
  
  if (label == T) {
    # Pull out ZIP centroids
    suppressWarnings(zip_trim_map <- cbind(zip_trim_map, st_coordinates(st_centroid(zip_trim_map$geometry))))
  }
  
  # Pull out year and set up title
  if (title == F) {
    title <- ""
  } else {
    title <- glue("Percent with asthma among {pop} members in King County, by ZIP ({year})",
                  pop = ifelse(year < 2017, "Medicaid and Medicare", "Medicaid (non-dual)"))
  }

  map <- ggplot(data = zip_trim_map) + 
    geom_sf(aes(fill = prop)) + 
    scale_fill_viridis_c(option = "magma", trans = "sqrt", direction = -1) +
    labs(title = title,
         x = "Longitude", y = "Latitude", fill = "Percent with asthma")
  
  if (label == T) {
    
    map <- map + 
      geom_text(aes(x = X, y = Y, label = prop))
  }
  
  return(map)
}

geo_zip_map_inla <- function(df, title = F, adjust_caption = T, adjust_pha = F) {
  # Pull out year and set up title
  year <- unique(df$df$year)
  if (title == F) {
    title <- ""
  } else {
    title <- glue("Risk of having asthma among {pop} members in King County, by ZIP ({year})",
                  pop = ifelse(year < 2017, "Medicaid and Medicare", "Medicaid (non-dual)"))
  }
  
  if (adjust_caption == T) {
    if (adjust_pha == F) {
      adjust <- "Adjusted for age, gender, and race/ethnicity"
    } else {
      adjust <- "Adjusted for age, gender, race/ethnicity, and PHA agency"
    }
  } else {
    adjust <- ""
  }
  
  map <- ggplot(data = df$zip_trim_map) + 
    geom_sf(aes(fill = rr)) + 
    geom_text(aes(x = X, y = Y, 
                  label = ifelse(df$zip_trim_map$direction == 1L, sprintf('\u2191'), 
                                 ifelse(df$zip_trim_map$direction == -1L, sprintf('\u2193'), ""))),
              color = ifelse(df$zip_trim_map$direction == 1L, "grey70", 
                             ifelse(df$zip_trim_map$direction == -1L, "grey20", ""))) + 
    scale_fill_viridis_c(option = "magma", trans = "sqrt", direction = -1) +
    labs(title = title,
         #subtitle = "Adjusted for proportion of Medicaid members who are also PHA members",
         caption = paste0(sprintf('\u2191'), "/", sprintf('\u2193'), " = signficicantly different risk. ", 
                          adjust),
         x = "Longitude", y = "Latitude", fill = "Relative risk")
  
  return(map)
}


### RUN DATA
# Unadjusted proportions
geo_zip_map_16_prop <- geo_zip_map_prop(ccw_asthma_demog, yr = 2016, title = F)
geo_zip_map_18_prop <- geo_zip_map_prop(ccw_asthma_demog, yr = 2018, title = F)

# INLA-adjusted proportions
geo_zip_16 <- geo_setup_f(yr = 2016, pha = F)
geo_zip_18 <- geo_setup_f(yr = 2018, pha = F)

geo_zip_18_pha <- geo_setup_f(yr = 2018, pha = T)

geo_zip_map_16_inla <- geo_zip_map_inla(geo_zip_16, title = F, adjust_caption = T, adjust_pha = F)
geo_zip_map_18_inla <- geo_zip_map_inla(geo_zip_18, title = F, adjust_caption = T, adjust_pha = F)

# geo_zip_map_18 + geom_sf(data = portfolios) + 
#   geom_label_repel(data = portfolios, aes(x = Lon, y = Lat, label = Portfolio),
#                    nudge_x = c(-0.2, 0.2, -0.5, 0.3, -0.5, -0.3, -0.3, -0.5, 0.1),
#                    nudge_y = c(0, 0, 0, 0.05, 0, 0, 0.01, 0.03, -0.03))


#### HEDIS ASTHMA --------------------------------------------------------------
# Spot check to make sure no-one with housing-only, Medicare-only, or dual status 
#   is showing up with asthma (2018 is only Medicaid data, duals are excluded)
housing_amr %>% 
  filter(enroll_flag == 1) %>%
  group_by(enroll_type_text) %>% 
  # See how many people meet the definition for asthma
  summarise(numerator = sum(denominator),
            numerator_1yr = sum(denominator_1yr),
            denominator = n()) %>%
  ungroup() %>% mutate(total = sum(numerator), total_1yr = sum(numerator_1yr))

### Proportion with asthma
# By subsidy type
hedis_asthma_age_sub <- housing_amr %>%
  filter(enroll_flag == 1) %>%
  group_by(age_yr_asthma, pha_subsidy) %>%
  summarise(count = sum(denominator), total = n_distinct(id_apde)) %>%
  ungroup() %>%
  mutate(prop = round(count / total * 1000, 4),
         amr_type = "Persistent asthma")

hedis_asthma_age_sub_1yr <- housing_amr %>%
  filter(enroll_flag == 1) %>%
  group_by(age_yr_asthma, pha_subsidy) %>%
  summarise(count = sum(denominator_1yr), total = n_distinct(id_apde)) %>%
  ungroup() %>%
  mutate(prop = round(count / total * 1000, 4),
         amr_type = "One-year asthma")

hedis_asthma_all_age_sub <- bind_rows(hedis_asthma_age_sub, hedis_asthma_age_sub_1yr) %>%
  # Add in CIs
  rowwise() %>%
  mutate(lower_bound = ifelse(count == 0, NA_real_, 
                              round(prop.test(count, total, 
                                              correct = F)$conf.int[1] * 1000, 3)),
         upper_bound = ifelse(count == 0, NA_real_, 
                              round(prop.test(count, total, 
                                              correct = F)$conf.int[2] * 1000, 3))) %>%
  ungroup()

hedis_asthma_all_age_sub_g <- hedis_asthma_all_age_sub %>%
  ggplot(aes(fill = pha_subsidy, y = prop, x = age_yr_asthma)) + 
  geom_bar(position = "dodge", stat = "identity", color = "#333333") +
  scale_fill_manual(values = c("#79706e", "#2c7bb6", "#87d180"), name = "PHA subsidy type") + 
  xlab("Age (years)") + ylab("Number per 1,000") + 
  theme_ipsum_ps() +
  theme(panel.grid.major.y = element_line(color = "grey90"),
        panel.grid.major.x = element_blank(), 
        panel.grid.minor = element_blank())


# Now by PHA
hedis_asthma_age_pha <- housing_amr %>%
  filter(enroll_flag == 1) %>%
  group_by(age_yr_asthma, pha_agency) %>%
  summarise(count = sum(denominator), total = n_distinct(id_apde)) %>%
  ungroup() %>%
  mutate(prop = round(count / total * 1000, 4),
         amr_type = "Persistent asthma")

hedis_asthma_age_pha_1yr <- housing_amr %>%
  filter(enroll_flag == 1) %>%
  group_by(age_yr_asthma, pha_agency) %>%
  summarise(count = sum(denominator_1yr), total = n_distinct(id_apde)) %>%
  ungroup() %>%
  mutate(prop = round(count / total * 1000, 4),
         amr_type = "One-year asthma")

hedis_asthma_all_age_pha <- bind_rows(hedis_asthma_age_pha, hedis_asthma_age_pha_1yr) %>%
  # Add in CIs
  rowwise() %>%
  mutate(lower_bound = ifelse(count == 0, NA_real_, 
                              round(prop.test(count, total, 
                                              correct = F)$conf.int[1] * 1000, 3)),
         upper_bound = ifelse(count == 0, NA_real_, 
                              round(prop.test(count, total, 
                                              correct = F)$conf.int[2] * 1000, 3))) %>%
  ungroup()

hedis_asthma_all_age_pha_g <- hedis_asthma_all_age_pha %>%
  ggplot(aes(fill = pha_agency, y = prop, x = age_yr_asthma)) + 
  geom_bar(position = "dodge", stat = "identity", color = "#333333") +
  scale_fill_manual(values = c("#79706e", "#2c7bb6", "#87d180"), name = "Agency") + 
  xlab("Age (years)") + ylab("Number per 1,000") + 
  theme_ipsum_ps() +
  theme(panel.grid.major.y = element_line(color = "grey90"),
        panel.grid.major.x = element_blank(), 
        panel.grid.minor = element_blank())


### Regression analyses
# Set up a data frame
amr_regression_df <- housing_amr %>%
  filter(enroll_flag == 1) %>%
  mutate(pha = ifelse(pha_agency == "Non-PHA", 0, 1),
         age_grp = cut(age_yr, 
                       breaks = c(4.999, 11.999, 18.999, 30.999, 50.999, 64.999), 
                       include.lowest = F, 
                       labels = c("5-11", "12-18", "19-30", "31-50", "51-64"), 
                       ordered_result = F),
         length_grp = factor(time_housing,
                             levels = c("<3 years", "3-6 years", "6+ years"),
                             labels = c("<3 years", "3-6 years", "6+ years"))
  )

### Look at asthma overall by subsidy type
asthma_m_sub <- glm(denominator ~ pha_subsidy + age_yr_asthma + gender_me + race_eth_me,
                    data = amr_regression_df,
                    family = "binomial")
asthma_m_1yr_sub <- glm(denominator_1yr ~ pha_subsidy + age_grp + gender_me + race_eth_me,
                        data = amr_regression_df,
                        family = "binomial")

# Pull out coeficients
asthma_m_sub_coef <- tidy(asthma_m_sub, conf.int = T, exponentiate = T)
asthma_m_sub_coef_hcv <- asthma_m_sub_coef %>% filter(term == "pha_subsidyHCV")
asthma_m_sub_coef_ph <- asthma_m_sub_coef %>% filter(term == "pha_subsidyPH")
asthma_m_1yr_sub_coef <- tidy(asthma_m_1yr_sub, conf.int = T, exponentiate = T)
asthma_m_1yr_sub_coef_hcv <- asthma_m_1yr_sub_coef %>% filter(term == "pha_subsidyHCV")
asthma_m_1yr_sub_coef_ph <- asthma_m_1yr_sub_coef %>% filter(term == "pha_subsidyPH")


### Repeat but by PHA
asthma_m_pha <- glm(denominator ~ pha + age_yr_asthma + gender_me + race_eth_me,
                    data = amr_regression_df,
                    family = "binomial")
asthma_m_1yr_pha <- glm(denominator_1yr ~ pha + age_grp + gender_me + race_eth_me,
                        data = amr_regression_df,
                        family = "binomial")

# Pull out coeficients
asthma_m_pha_coef <- tidy(asthma_m_pha, conf.int = T, exponentiate = T)
asthma_m_pha_coef_pha <- asthma_m_pha_coef %>% filter(term == "pha")
asthma_m_1yr_pha_coef <- tidy(asthma_m_1yr_pha, conf.int = T, exponentiate = T)
asthma_m_1yr_pha_coef_pha <- asthma_m_1yr_coef %>% filter(term == "pha")

# summary(asthma_m_pha)
# exp(coef(asthma_m_pha))
# 
# summary(asthma_m_1yr_pha)
# exp(coef(asthma_m_1yr_pha))



#### HEDIS AMR -----------------------------------------------------------------
### AMR descriptive 
# Look by PHA subsidy
hedis_amr_age_sub <- housing_amr %>%
  filter(enroll_flag == 1) %>%
  group_by(age_yr_asthma, pha_subsidy) %>%
  summarise(count = sum(numerator), total = sum(denominator)) %>%
  ungroup() %>%
  mutate(prop = round(count / total * 100, 3),
         amr_type = "Met AMR (persistent asthma)")

hedis_amr_age_1yr_sub <- housing_amr %>%
  filter(enroll_flag == 1) %>%
  group_by(age_yr_asthma, pha_subsidy) %>%
  summarise(count = sum(numerator_1yr), total = sum(denominator_1yr)) %>%
  ungroup() %>%
  mutate(prop = round(count / total * 100, 3),
         amr_type = "Met AMR (one-year asthma)")

hedis_amr_all_age_sub <- bind_rows(hedis_amr_age_sub, hedis_amr_age_1yr_sub) %>%
  # Add in CIs
  rowwise() %>%
  mutate(lower_bound = ifelse(count == 0, NA_real_, 
                              round(prop.test(count, total, 
                                              correct = F)$conf.int[1] * 1000, 3)),
         upper_bound = ifelse(count == 0, NA_real_, 
                              round(prop.test(count, total, 
                                              correct = F)$conf.int[2] * 1000, 3))) %>%
  ungroup()

hedis_amr_all_age_sub_g <- hedis_amr_all_age_sub %>%
  ggplot(aes(fill = pha_subsidy, y = prop, x = age_yr_asthma)) + 
  geom_bar(position = "dodge", stat = "identity", color = "#333333") +
  scale_fill_manual(values = c("#79706e", "#2c7bb6", "#87d180"), name = "PHA subsidy type") + 
  xlab("Age (years)") + ylab("Percent") + 
  theme_ipsum_ps() +
  theme(panel.grid.major.y = element_line(color = "grey90"),
        panel.grid.major.x = element_blank(), 
        panel.grid.minor = element_blank())

# Rerun by PHA
hedis_amr_age_pha <- housing_amr %>%
  filter(enroll_flag == 1) %>%
  group_by(age_yr_asthma, pha_agency) %>%
  summarise(count = sum(numerator), total = sum(denominator)) %>%
  ungroup() %>%
  mutate(prop = round(count / total * 100, 3),
         amr_type = "Met AMR (persistent asthma)")

hedis_amr_age_1yr_pha <- housing_amr %>%
  filter(enroll_flag == 1) %>%
  group_by(age_yr_asthma, pha_agency) %>%
  summarise(count = sum(numerator_1yr), total = sum(denominator_1yr)) %>%
  ungroup() %>%
  mutate(prop = round(count / total * 100, 3),
         amr_type = "Met AMR (one-year asthma)")

hedis_amr_all_age_pha <- bind_rows(hedis_amr_age_pha, hedis_amr_age_1yr_pha) %>%
  # Add in CIs
  rowwise() %>%
  mutate(lower_bound = ifelse(count == 0, NA_real_, 
                              round(prop.test(count, total, 
                                              correct = F)$conf.int[1] * 1000, 3)),
         upper_bound = ifelse(count == 0, NA_real_, 
                              round(prop.test(count, total, 
                                              correct = F)$conf.int[2] * 1000, 3))) %>%
  ungroup()

hedis_amr_all_age_pha_g <- hedis_amr_all_age_pha %>%
  ggplot(aes(fill = pha_agency, y = prop, x = age_yr_asthma)) + 
  geom_bar(position = "dodge", stat = "identity", color = "#333333") +
  scale_fill_manual(values = c("#79706e", "#2c7bb6", "#87d180"), name = "Agency") + 
  xlab("Age (years)") + ylab("Percent") + 
  theme_ipsum_ps() +
  theme(panel.grid.major.y = element_line(color = "grey90"),
        panel.grid.major.x = element_blank(), 
        panel.grid.minor = element_blank())


### AMR regression
# Use data frame created in section above

# Look at AMR by subsidy type
amr_m_sub <- glm(numerator ~ pha_subsidy + age_grp + gender_me + race_eth_me,
                 data = amr_regression_df[amr_regression_df$denominator == 1, ],
                 family = "binomial")
amr_m_1yr_sub <- glm(numerator_1yr ~ pha_subsidy + age_grp + gender_me + race_eth_me,
                     data = amr_regression_df[amr_regression_df$denominator_1yr == 1, ],
                     family = "binomial")

# Pull out coeficients
amr_m_sub_coef <- tidy(amr_m_sub, conf.int = T, exponentiate = T)
amr_m_sub_coef_hcv <- amr_m_sub_coef %>% filter(term == "pha_subsidyHCV")
amr_m_sub_coef_ph <- amr_m_sub_coef %>% filter(term == "pha_subsidyPH")
amr_m_1yr_sub_coef <- tidy(amr_m_1yr_sub, conf.int = T, exponentiate = T)
amr_m_1yr_sub_coef_hcv <- amr_m_1yr_sub_coef %>% filter(term == "pha_subsidyHCV")
amr_m_1yr_sub_coef_ph <- amr_m_1yr_sub_coef %>% filter(term == "pha_subsidyPH")

# summary(amr_m_sub)
# exp(coef(amr_m_sub))
# 
# summary(amr_m_1yr_pha)
# exp(coef(amr_m_1yr_pha))


# Now look by PHA
amr_m_pha <- glm(numerator ~ pha + age_grp + gender_me + race_eth_me,
                      data = amr_regression_df[amr_regression_df$denominator == 1, ],
                      family = "binomial")
amr_m_1yr_pha <- glm(numerator_1yr ~ pha + age_grp + gender_me + race_eth_me,
                   data = amr_regression_df[amr_regression_df$denominator_1yr == 1, ],
                   family = "binomial")

# Pull out coeficients
amr_m_pha_coef <- tidy(amr_m_pha, conf.int = T, exponentiate = T)
amr_m_pha_coef_pha <- amr_m_pha_coef %>% filter(term == "pha")
amr_m_1yr_pha_coef <- tidy(amr_m_1yr_pha, conf.int = T, exponentiate = T)
amr_m_1yr_pha_coef_pha <- amr_m_1yr_pha_coef %>% filter(term == "pha")

# summary(amr_m_pha)
# exp(coef(amr_m_pha))
# 
# summary(amr_m_1yr_pha)
# exp(coef(amr_m_1yr_pha))


#### ACUTE EVENTS --------------------------------------------------------------

#### Broken down by subsidy type ####
### Denominator
denom_18_age_sub <- acute_denom_18 %>%
  filter(!is.na(age_yr_asthma) & year == 2018) %>%
  group_by(pha_subsidy, age_yr_asthma) %>%
  summarise(denominator = sum(pt), denominator_yr = sum(pt) / 365,
            denominator_mth = denominator_yr * 12)

### Primary dx
acute_18_age_sub <- acute_18 %>%
  filter(!is.na(age_yr_asthma) & year == 2018) %>%
  group_by(type, pha_subsidy, age_yr_asthma) %>%
  summarise(numerator = sum(event)) %>%
  ungroup()

acute_18_age_sub <- left_join(acute_denom_shell_sub, denom_18_age_sub, by = c("pha_subsidy", "age_yr_asthma")) %>%
  left_join(., acute_18_age_sub, by = c("type", "pha_subsidy", "age_yr_asthma")) %>%
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
acute_18_age_sub_g <- acute_18_age_sub %>%
  filter(type == "ED visits" & !is.na(age_yr_asthma) & !age_yr_asthma %in% c("65-74", "75+")) %>%
  ggplot(aes(fill = pha_subsidy, y = rate, x = age_yr_asthma)) + 
  geom_bar(position = "dodge", stat = "identity", color = "#333333") +
  geom_text(aes(label = ifelse(numerator == 0, "*", "")), 
            position = position_dodge(width = 0.9), vjust = -0.05) + 
  scale_fill_manual(values = c("#79706e", "#2c7bb6", "#87d180"), name = "PHA subsidy type") + 
  xlab("Age (years)") + ylab("Rate per 1,000 member-months") + 
  ggtitle("ED visits from asthma (primary dx) (2018, non-dual Medicaid members only)") +
  theme_ipsum_ps() +
  theme(panel.grid.major.y = element_line(color = "grey90"),
        panel.grid.major.x = element_blank(), 
        panel.grid.minor = element_blank())


### Any dx
acute_18_all_dx_age_sub <- acute_18_all_dx %>%
  filter(!is.na(age_yr_asthma) & year == 2018) %>%
  group_by(type, pha_subsidy, age_yr_asthma) %>%
  summarise(numerator = sum(event))

acute_18_all_dx_age_sub <- left_join(acute_denom_shell_sub, denom_18_age_sub, by = c("pha_subsidy", "age_yr_asthma")) %>%
  left_join(., acute_18_all_dx_age_sub, by = c("type", "pha_subsidy", "age_yr_asthma")) %>%
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
acute_18_all_dx_age_sub_g <- acute_18_all_dx_age_sub %>%
  filter(!is.na(age_yr_asthma) & !age_yr_asthma %in% c("65-74", "75+")) %>%
  ggplot(aes(fill = pha_subsidy, y = rate, x = age_yr_asthma)) + 
  geom_bar(position = "dodge", stat = "identity", color = "#333333") +
  geom_text(aes(label = ifelse(numerator == 0, "*", "")), 
            position = position_dodge(width = 0.9), vjust = -0.05) + 
  scale_fill_manual(values = c("#79706e", "#2c7bb6", "#87d180"), name = "PHA subsidy type") + 
  xlab("Age (years)") + ylab("Rate per 1,000 member-months") + 
  labs(caption = "* = numbers suppressed due to small size") + 
  ggtitle("ED visits/hospitalizations from asthma (any dx) (2018, non-dual Medicaid members only)") + 
  theme_ipsum_ps() +
  theme(panel.grid.major.y = element_line(color = "grey90"),
        panel.grid.major.x = element_blank(), 
        panel.grid.minor = element_blank())

acute_18_all_dx_age_sub_g + facet_wrap( ~ type, ncol = 1, scales = "free_y")


#### Broken down by PHA ####
### Denominator
denom_18_age_pha <- acute_denom_18 %>%
  filter(!is.na(age_yr_asthma) & year == 2018) %>%
  group_by(pha_agency, age_yr_asthma) %>%
  summarise(denominator = sum(pt), denominator_yr = sum(pt) / 365,
            denominator_mth = denominator_yr * 12)

### Primary dx
acute_18_age_pha <- acute_18 %>%
  filter(!is.na(age_yr_asthma) & year == 2018) %>%
  group_by(type, pha_agency, age_yr_asthma) %>%
  summarise(numerator = sum(event)) %>%
  ungroup()

acute_18_age_pha <- left_join(acute_denom_shell_pha, denom_18_age_pha, by = c("pha_agency", "age_yr_asthma")) %>%
  left_join(., acute_18_age_pha, by = c("type", "pha_agency", "age_yr_asthma")) %>%
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
acute_18_age_pha_g <- acute_18_age_pha %>%
  filter(type == "ED visits" & !is.na(age_yr_asthma) & !age_yr_asthma %in% c("65-74", "75+")) %>%
  ggplot(aes(fill = pha_agency, y = rate, x = age_yr_asthma)) + 
  geom_bar(position = "dodge", stat = "identity", color = "#333333") +
  geom_text(aes(label = ifelse(numerator == 0, "*", "")), 
            position = position_dodge(width = 0.9), vjust = -0.05) + 
  scale_fill_manual(values = c("#79706e", "#2c7bb6", "#87d180"), name = "Agency") + 
  xlab("Age (years)") + ylab("Rate per 1,000 member-months") + 
  ggtitle("ED visits from asthma (primary dx) (2018, non-dual Medicaid members only)") +
  theme_ipsum_ps() +
  theme(panel.grid.major.y = element_line(color = "grey90"),
        panel.grid.major.x = element_blank(), 
        panel.grid.minor = element_blank())


### Any dx
acute_18_all_dx_age_pha <- acute_18_all_dx %>%
  filter(!is.na(age_yr_asthma) & year == 2018) %>%
  group_by(type, pha_agency, age_yr_asthma) %>%
  summarise(numerator = sum(event))

acute_18_all_dx_age_pha <- left_join(acute_denom_shell, denom_18_age_pha, by = c("pha_agency", "age_yr_asthma")) %>%
  left_join(., acute_18_all_dx_age_pha, by = c("type", "pha_agency", "age_yr_asthma")) %>%
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
acute_18_all_dx_age_pha_g <- acute_18_all_dx_age_pha %>%
  filter(!is.na(age_yr_asthma) & !age_yr_asthma %in% c("65-74", "75+")) %>%
  ggplot(aes(fill = pha_agency, y = rate, x = age_yr_asthma)) + 
  geom_bar(position = "dodge", stat = "identity", color = "#333333") +
  geom_text(aes(label = ifelse(numerator == 0, "*", "")), 
            position = position_dodge(width = 0.9), vjust = -0.05) + 
  scale_fill_manual(values = c("#79706e", "#2c7bb6", "#87d180"), name = "Agency") + 
  xlab("Age (years)") + ylab("Rate per 1,000 member-months") + 
  labs(caption = "* = numbers suppressed due to small size") + 
  ggtitle("ED visits/hospitalizations from asthma (any dx) (2018, non-dual Medicaid members only)") + 
  theme_ipsum_ps() +
  theme(panel.grid.major.y = element_line(color = "grey90"),
        panel.grid.major.x = element_blank(), 
        panel.grid.minor = element_blank())

acute_18_all_dx_age_pha_g + facet_wrap( ~ type, ncol = 1, scales = "free_y")


#### Adjusted regression ####
# Set up a data frame

# Counts by ID
acute_18 %>% 
  filter(type == "ED visits") %>%
  group_by(id_apde) %>%
  summarise(ed_tot = sum(event)) %>%
  group_by(ed_tot) %>%
  summarise(count = n())
  


amr_regression_df <- housing_amr %>%
  filter(enroll_flag == 1) %>%
  mutate(pha = ifelse(pha_agency == "Non-PHA", 0, 1),
         age_grp = cut(age_yr, 
                       breaks = c(4.999, 11.999, 18.999, 30.999, 50.999, 64.999), 
                       include.lowest = F, 
                       labels = c("5-11", "12-18", "19-30", "31-50", "51-64"), 
                       ordered_result = F),
         length_grp = factor(time_housing,
                             levels = c("<3 years", "3-6 years", "6+ years"),
                             labels = c("<3 years", "3-6 years", "6+ years"))
  )

### Look at asthma overall by subsidy type
asthma_m_sub <- glm(denominator ~ pha_subsidy + age_yr_asthma + gender_me + race_eth_me,
                    data = amr_regression_df,
                    family = "binomial")

#### ED VISITS WHAT IF ---------------------------------------------------------
### Denominator
denom_18_all <- acute_denom_18 %>%
  filter(!is.na(age_yr_asthma) & year == 2018 & !age_yr_asthma %in% c("65-74", "75+")) %>%
  group_by(pha_subsidy, age_yr_asthma, race_eth_me, gender_me) %>%
  summarise(denominator = sum(pt), denominator_yr = sum(pt) / 365,
            denominator_mth = denominator_yr * 12)

### Primary dx
acute_18_all <- acute_18 %>%
  filter(!is.na(age_yr_asthma) & year == 2018 & !age_yr_asthma %in% c("65-74", "75+")) %>%
  group_by(type, pha_subsidy, age_yr_asthma, race_eth_me, gender_me) %>%
  summarise(numerator = sum(event)) %>%
  ungroup()

acute_18_all <- left_join(acute_denom_shell_all, denom_18_all, 
                          by = c("pha_subsidy", "age_yr_asthma", "race_eth_me", "gender_me")) %>%
  left_join(., acute_18_all, by = c("type", "pha_subsidy", "age_yr_asthma", "race_eth_me", "gender_me")) %>%
  filter(!age_yr_asthma %in% c("65-74", "75+") & !is.na(denominator)) %>%
  mutate(numerator = replace_na(numerator, 0), rate = numerator / denominator_mth)

ed_18_all_expected <- acute_18_all %>% filter(type == "ED visits") %>%
  left_join(., filter(acute_18_all, type == "ED visits" & pha_subsidy == "Non-PHA") %>%
              dplyr::select(age_yr_asthma, race_eth_me, gender_me, rate) %>%
              rename(rate_pha = rate),
            by = c("age_yr_asthma", "race_eth_me", "gender_me")) %>%
  mutate(expected = rate_pha * denominator_mth)
  

ed_18_all_avoidable <- ed_18_all_expected %>%
  mutate(expected_round = round(expected)) %>%
  group_by(pha_subsidy) %>%
  summarise(expected = round(sum(expected)), 
            expected_round = sum(expected_round),
            count = sum(numerator)) %>%
  ungroup() %>%
  mutate(difference = count - expected,
         pct = round(difference / count * 100, 3))


#### MCO ANALYSES --------------------------------------------------------------
# Bring in MCO ref table
mco_ref <- dbGetQuery(db_claims, "SELECT * FROM ref.mco")

# Find people who were in an MCO in 2018
mco_18 <- mcaid_mcare_pha_elig_demo %>%
  filter(!is.na(mco_id) & year == 2018 & age_yr < 65 & dual != 1) %>%
  left_join(., dplyr::select(mco_ref, product_identifier, mco), 
            by = c("mco_id" = "product_identifier")) %>%
  filter(!is.na(mco))


#### Enrollment in MCO and PHA ####
mco_enrollment <- mco_18 %>% group_by(mco, dual, pha_subsidy) %>%
  summarise(enrolled = n_distinct(id_apde)) %>%
  group_by(mco) %>%
  mutate(total = sum(enrolled)) %>% ungroup() %>%
  mutate(pct = round(enrolled / total * 100, 1))

# Add overall to show comparison
mco_enrollment <- bind_rows(mco_enrollment,
                            mco_enrollment %>% group_by(pha_subsidy) %>%
                              summarise(enrolled = sum(enrolled), total = sum(total),
                                        pct = round(enrolled / total * 100, 1),
                                        mco = "MCOs overall") %>% ungroup(),
                            filter(mcaid_mcare_pha_elig_demo, year == 2018 & age_yr < 65 & dual != 1) %>%
                              group_by(pha_subsidy) %>%
                              summarise(enrolled = n_distinct(id_apde)) %>%
                              ungroup() %>% mutate(total = sum(enrolled),
                                                   pct = round(enrolled / total * 100, 1),
                                                   mco = "Overall"))

mco_enrollment <- mco_enrollment %>%
  group_by(mco) %>% 
  mutate(ymax = cumsum(pct),
         ymin = ifelse(is.na(lag(ymax, 1)), 0, lag(ymax, 1)))

# Test graph
mco_enrollment %>%
  filter(mco == "Coordinated Care of Washington") %>%
  ggplot(aes(ymax = ymax, ymin = ymin, xmax = 4, xmin = 3, fill = pha_subsidy)) +
  geom_rect(color = "grey20") + 
  coord_polar(theta = "y") + 
  xlim(c(2, 4)) + 
  theme_void() + 
  scale_fill_manual(values = c("#79706e", "#2c7bb6", "#87d180"), name = "PHA subsidy") +
  geom_label(x = 3.5, aes(y = (ymax + ymin) / 2, label = paste0(pct, "%")), size = 3,
             show.legend = F)


#### Proportion with asthma ####
# Summarise by MCO
ccw_asthma_mco_age <- mco_18 %>%
  filter(!is.na(age_yr_asthma) & !is.na(mco) & year == 2018 & age_yr < 65 & dual != 1) %>%
  group_by(mco, year, year_text, age_yr_asthma, pha_subsidy) %>%
  summarise(count = sum(asthma), total = n_distinct(id_apde)) %>%
  ungroup() %>%
  mutate(prop = round(count / total * 1000, 3))

# Add overall to show comparison
ccw_asthma_mco_age <- bind_rows(mutate(ccw_asthma_mco_age),
                                   mutate(ccw_asthma_age_sub_18, mco = "Overall"),
                                ccw_asthma_mco_age %>% 
                                     group_by(year, year_text, age_yr_asthma, pha_subsidy) %>%
                                     summarise(count = sum(count), total = sum(total)) %>%
                                     ungroup() %>%
                                     mutate(prop = round(count / total * 1000, 3),
                                            mco = "MCOs overall"))

# Also make non-age version
ccw_asthma_mco <- ccw_asthma_mco_age %>% 
  group_by(mco, pha_subsidy) %>%
  summarise(count = sum(count), total = sum(total)) %>%
  ungroup() %>%
  mutate(prop = round(count / total * 1000, 0))

# Test graph
ccw_asthma_mco %>%
  filter(mco == "Amerigroup Washington Inc." | mco == "MCOs overall") %>%
  ggplot(aes(fill = pha_subsidy, y = prop, x = mco)) + 
  geom_bar(position = "dodge", stat = "identity", color = "#333333") +
  scale_fill_manual(values = c("#79706e", "#2c7bb6", "#87d180"), name = "PHA subsidy") + 
  xlab("") + ylab("Number per 1,000") + 
  theme_ipsum_ps() +
  theme(panel.grid.major.y = element_line(color = "grey90"),
        panel.grid.major.x = element_blank(), 
        panel.grid.minor = element_blank())


#### ED visits ####
# Make a denominator
denom_18_sub_mco <- acute_denom_18 %>%
  left_join(., dplyr::select(mco_ref, product_identifier, mco), 
            by = c("mco_id" = "product_identifier")) %>%
  filter(!is.na(mco) & age_yr < 65 & dual != 1) %>%
  group_by(mco, pha_subsidy) %>%
  summarise(denominator = sum(pt), denominator_yr = sum(pt) / 365,
            denominator_mth = denominator_yr * 12)


acute_18_sub_mco <- acute_18 %>%
  left_join(., dplyr::select(mco_ref, product_identifier, mco), 
            by = c("mco_id" = "product_identifier")) %>%
  filter(!is.na(mco) & age_yr < 65 & dual != 1 & type == "ED visits") %>%
  group_by(mco, pha_subsidy) %>%
  summarise(numerator = sum(event)) %>%
  ungroup() %>%
  left_join(., denom_18_sub_mco, by = c("mco", "pha_subsidy")) %>%
  mutate(rate = numerator / denominator_mth * 1000)


# Add in overall
acute_18_sub_mco <- bind_rows(acute_18_sub_mco,
                              acute_18_sub_mco %>% group_by(pha_subsidy) %>%
                                summarise(numerator = sum(numerator), denominator = sum(denominator),
                                          denominator_yr = sum(denominator_yr), denominator_mth = sum(denominator_mth)) %>%
                                ungroup() %>%
                                mutate(rate = numerator / denominator_mth * 1000,
                                       mco = "MCOs overall"),
                              acute_18_age_sub %>% filter(type == "ED visits" & 
                                                            !age_yr_asthma %in% c("65-74", "75+")) %>%
                                group_by(pha_subsidy) %>%
                                summarise(numerator = sum(numerator), denominator = sum(denominator),
                                          denominator_yr = sum(denominator_yr), denominator_mth = sum(denominator_mth)) %>%
                                ungroup() %>%
                                mutate(rate = numerator / denominator_mth * 1000,
                                       mco = "Overall"))

# Test graph
acute_18_sub_mco %>%
  filter(mco == "Amerigroup Washington Inc." | mco == "MCOs overall") %>%
  ggplot(aes(fill = pha_subsidy, y = rate, x = mco)) + 
  geom_bar(position = "dodge", stat = "identity", color = "#333333") +
  geom_text(aes(label = ifelse(numerator == 0, "*", "")), 
            position = position_dodge(width = 0.9), vjust = -0.05) + 
  scale_fill_manual(values = c("#79706e", "#2c7bb6", "#87d180"), name = "PHA subsidy") + 
  xlab("") + ylab("Rate per 1,000 member-months") + 
  ggtitle("ED visits from asthma (primary dx) (2018, non-dual Medicaid members only)") +
  theme_ipsum_ps() +
  theme(panel.grid.major.y = element_line(color = "grey90"),
        panel.grid.major.x = element_blank(), 
        panel.grid.minor = element_blank())



#### Make list of MCOs ####
mcos <- as.list(unique(ccw_asthma_mco_age_18$mco))
mcos <- mcos[!mcos %in% c("Overall", "MCOs overall")]

#### Write out custom reports ####
lapply(mcos, function(x) {
  message(paste0("Working on ", x))

  render(file.path(dashh_2_path, "/Convening/DASHH convening 2019-02 - MCO report.Rmd"),
  output_file = file.path(dashh_2_path, "Convening", paste0("DASHH convening 2020-02 - MCO report - ", x, ".html")))
})



#### WRITE OUT PPT SLIDES ------------------------------------------------------
render(file.path(getwd(), "analyses/asthma/pha_asthma_convening.Rmd"), "powerpoint_presentation",
       output_file = file.path(dashh_2_path, "Convening", "DASHH convening 2020-02 - data slides.pptx"))
