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

pacman::p_load(odbc, housing, claims, lubridate, tidyverse, glue, aod, broom, 
               spdep, SpatialEpi, INLA, sf, knitr, kableExtra, rmarkdown,
               scales, ggplot2, hrbrthemes, treemapify)

db_apde <- dbConnect(odbc(), "PH_APDEStore51")
db_claims <- dbConnect(odbc(), "PHClaims51")

dashh_2_path <- "C:/Users/mathesal/King County/Laurent, Amy - DASHH-Medicare/Deep Dive"
spatial_dir <- "C:/Users/mathesal/King County/Laurent, Amy - DASHH-External/DASHH2.0/Deep Dive/Asthma/spatial"


#### BRING IN RELEVANT DATA ----------------------------------------------------
# Find the most recent month we have enrollment summaries for
# Comes in as year-month
max_month <- unlist(dbGetQuery(db_claims, "SELECT MAX(year_month) FROM stage.perf_enroll_denom"))
# Now find last day of the month by going forward a month then back a day
max_month <- as.Date(parse_date_time(max_month, "Ym") %m+% months(1) - days(1))

# Set up quarters to run over
months_list <- as.list(seq(as.Date("2013-01-01"), as.Date(max_month) + 1, by = "year") - 1)


### Linked housing/Medicaid data
mcaid_mcare_pha_elig_demo <- DBI::dbReadTable(
  db_apde, DBI::Id(schema = "stage", table = "mcaid_mcare_pha_elig_calyear"))

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
    # Make new year field with greater explanations
    year_text = case_when(
      year %in% c(2012:2016) ~ paste0(year, " (Medicaid and Medicare)"),
      year %in% c(2017:2019) ~ paste0(year, " (Medicaid only, non-dual)"))
  )


### CCW asthma
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


### King County ZIPs
zip <- dbGetQuery(db_claims,
                  "SELECT zip_code as zip_c, city 
                 FROM ref.apcd_zip WHERE county_name = 'King' and state = 'WA'")

### Shape file of ZIPs in King County 
# Bring in both one with and without water trim because we want to 
# identify neighboring ZIPs)
kc_zip <- read_sf("//gisdw/kclib/Plibrary2/admin/shapes/polygon/zipcode.shp")
kc_zip_trim <- read_sf("//gisdw/kclib/Plibrary2/admin/shapes/polygon/zipcode_shore.shp")


#### GENERAL SETTING UP OF DATA ------------------------------------------------
### Join long asthma to demogs
ccw_asthma_demog <- mcaid_mcare_pha_elig_demo %>% 
  filter(!is.na(pop) & enroll_type != "h") %>%
  left_join(., ccw_asthma_long, by = c("id_apde", "year")) %>%
  mutate(asthma = replace_na(asthma, 0))

# Check results
ccw_asthma_demog %>% filter(year == 2016) %>%
  group_by(pha_agency) %>%
  summarise(count = sum(asthma), rows = n(), total = n_distinct(id_apde)) %>%
  ungroup() %>%
  mutate(prop = round(count / total * 1000, 4))


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

### Show new age groups in bar charts
# Split up 2016 and 2018 to account for different populations
ccw_asthma_age_16 <- ccw_asthma_demog %>%
  filter(!is.na(age_yr_asthma) & year == 2016) %>%
  group_by(year, year_text, age_yr_asthma, pha_agency) %>%
  summarise(count = sum(asthma), total = n_distinct(id_apde)) %>%
  ungroup() %>%
  mutate(prop = round(count / total * 1000, 4))
ccw_asthma_age_18 <- ccw_asthma_demog %>%
  filter(!is.na(age_yr_asthma) & year == 2018 & age_yr < 65 & dual != 1) %>%
  group_by(year, year_text, age_yr_asthma, pha_agency) %>%
  summarise(count = sum(asthma), total = n_distinct(id_apde)) %>%
  ungroup() %>%
  mutate(prop = round(count / total * 1000, 4))

ccw_asthma_age <- bind_rows(ccw_asthma_age_16, ccw_asthma_age_18)

ccw_asthma_age_g <- ccw_asthma_age %>%
  filter(!is.na(age_yr_asthma) & 
           (year == 2016 | (year == 2018 & age_yr_asthma != "65-74" & age_yr_asthma != "75+"))) %>%
  ggplot(aes(fill = pha_agency, y = prop, x = age_yr_asthma)) + 
  geom_bar(position = "dodge", stat = "identity", color = "#333333") +
  scale_fill_manual(values = c("#79706e", "#2c7bb6", "#87d180"), name = "Agency") + 
  xlab("Age (years)") + ylab("Number per 1,000") + 
  theme_ipsum_ps()

ccw_asthma_age_g + facet_wrap( ~ year_text, ncol = 1)



#### CCW ASTHMA SPATIAL --------------------------------------------------------
#### SET UP DATA FOR 2016 ####
asthma_geo_zip <- ccw_asthma_demog %>%
  filter(year %in% c(2016, 2018)) %>%
  mutate_at(vars(gender_me, race_me),
            list(~ ifelse(. == "Unknown", NA_character_, .))) %>%
  group_by(geo_zip, gender_me, race_me, age_yr_asthma) %>%
  summarise(asthma = sum(asthma, na.rm = T), pop = n()) %>%
  ungroup()

asthma_geo_zip_pha <- ccw_asthma_demog %>%
  filter(year %in% c(2016, 2018) & pha_agency %in% c("SHA", "KCHA")) %>%
  mutate_at(vars(gender_me, race_me),
            list(~ ifelse(. == "Unknown", NA_character_, .))) %>%
  group_by(geo_zip, gender_me, race_me, age_yr_asthma) %>%
  summarise(asthma = sum(asthma, na.rm = T), pop = n()) %>%
  ungroup()

# Make a matrix of all possible combinations
geo_zip_matrix <- expand.grid(year = unlist(distinct(asthma_geo_zip, year)),
                              geo_zip = unlist(distinct(asthma_geo_zip, geo_zip)), 
                              gender_me = unlist(distinct(asthma_geo_zip, gender_me)),
                              race_me = unlist(distinct(asthma_geo_zip, race_me)), 
                              age_yr_asthma = unlist(distinct(asthma_geo_zip, age_yr_asthma)))

geo_zip_matrix <- geo_zip_matrix[order(geo_zip_matrix$year, 
                                       geo_zip_matrix$geo_zip, 
                                       geo_zip_matrix$gender_me, 
                                       geo_zip_matrix$race_me, 
                                       geo_zip_matrix$age_yr_asthma), ]


# Join with actual data and make NA = 0
asthma_geo_zip <- left_join(geo_zip_matrix, asthma_geo_zip,
                            by = c("year", "geo_zip", "gender_me", "race_me", "age_yr_asthma")) %>%
  mutate_at(vars(asthma, pop), list( ~ ifelse(is.na(.), 0, .)))

asthma_geo_zip_pha <- left_join(geo_zip_matrix, asthma_geo_zip_pha,
                                by = c("year", "geo_zip", "gender_me", "race_me", "age_yr_asthma")) %>%
  mutate_at(vars(asthma, pop), list( ~ ifelse(is.na(.), 0, .)))

# Sum counts for each geo_zip
asthma_geo_zip_tot <- asthma_geo_zip %>% group_by(year, geo_zip) %>%
  summarise(count = sum(asthma), pop = sum(pop)) %>% ungroup()

asthma_geo_zip_tot_pha <- asthma_geo_zip_pha %>% group_by(year, geo_zip) %>%
  summarise(count = sum(asthma), pop = sum(pop)) %>% ungroup()

# Remove geo_zips with only one person in them
asthma_geo_zip_tot <- asthma_geo_zip_tot %>% filter(pop > 1)
asthma_geo_zip_tot_pha <- asthma_geo_zip_tot_pha %>% filter(pop > 1)

asthma_geo_zip <- inner_join(asthma_geo_zip, 
                             distinct(asthma_geo_zip_tot, year, geo_zip), 
                             by = c("year", "geo_zip"))
asthma_geo_zip_pha <- inner_join(asthma_geo_zip_pha, 
                                 distinct(asthma_geo_zip_tot_pha, year, geo_zip), 
                                 by = c("year", "geo_zip"))

# Remove combinations with 0 people in them
# Otherwise cannot calculate expected number of people below
strata_pop <- asthma_geo_zip %>% group_by(year, gender_me, race_me, age_yr_asthma) %>%
  summarise(pop = sum(pop)) %>% ungroup() %>%
  filter(pop > 0)

strata_pop_pha <- asthma_geo_zip_pha %>% group_by(year, gender_me, race_me, age_yr_asthma) %>%
  summarise(pop = sum(pop)) %>% ungroup() %>%
  filter(pop > 0)

asthma_geo_zip <- inner_join(asthma_geo_zip, 
                             distinct(strata_pop, year, gender_me, race_me, age_yr_asthma),
                             by = c("year", "gender_me", "race_me", "age_yr_asthma"))

asthma_geo_zip_pha <- inner_join(asthma_geo_zip_pha, 
                                 distinct(strata_pop_pha, year, gender_me, race_me, age_yr_asthma),
                                 by = c("year", "gender_me", "race_me", "age_yr_asthma"))

# Set up expected number of cases
# First sort data
asthma_geo_zip <- asthma_geo_zip %>% arrange( year, geo_zip, gender_me, race_me, age_yr_asthma)
asthma_geo_zip_pha <- asthma_geo_zip_pha %>% arrange(year, geo_zip, gender_me, race_me, age_yr_asthma)
# Calculate the number of strata
strata <- asthma_geo_zip %>% distinct(year, gender_me, race_me, age_yr_asthma) %>% summarise(n = n()) %>%
  as.numeric()
strata_pha <- asthma_geo_zip_pha %>% distinct(year, gender_me, race_me, age_yr_asthma) %>% summarise(n = n()) %>%
  as.numeric()
# Calculate the expected number of cases in each geo_zip based on pop variables
# (indirect standardization)
expected <- expected(population = asthma_geo_zip$pop,
                     cases = asthma_geo_zip$asthma,
                     n.strata = strata)
expected_pha <- expected(population = asthma_geo_zip_pha$pop,
                         cases = asthma_geo_zip_pha$asthma,
                         n.strata = strata_pha)

# Add expected values for each geo_zip
asthma_geo_zip_tot$expected <- expected
asthma_geo_zip_tot_pha$expected <- expected_pha


### Add covariate with proportion in PHA
pha_prop <- ccw_asthma_demog %>%
  filter(year %in% c(2016, 2018)) %>%
  mutate(pha = ifelse(pha_agency == "Non-PHA", 0, 1)) %>%
  group_by(year, geo_zip) %>%
  summarise(pha_prop = mean(pha, na.rm = T)) %>%
  ungroup()

asthma_geo_zip_tot <- left_join(asthma_geo_zip_tot, pha_prop, 
                                by = c("year", "geo_zip"))


### Restrict to ZIPs in data, shape file, and KC
zips <- inner_join(distinct(asthma_geo_zip_tot, geo_zip), 
                   kc_zip %>% as.data.frame() %>% distinct(ZIPCODE),
                   by = c("geo_zip" = "ZIPCODE")) %>%
  inner_join(., zip, by = c("geo_zip" = "zip_c"))
zips_pha <- inner_join(distinct(asthma_geo_zip_pha, geo_zip), 
                       kc_zip %>% as.data.frame() %>% distinct(ZIPCODE),
                       by = c("geo_zip" = "ZIPCODE")) %>%
  inner_join(., zip, by = c("geo_zip" = "zip_c"))

kc_zip_pha <- kc_zip %>% filter(ZIPCODE %in% unlist(zips_pha))
asthma_geo_zip_pha <- asthma_geo_zip_pha %>% filter(geo_zip %in% unlist(zips_pha))

kc_zip <- kc_zip %>% filter(ZIPCODE %in% unlist(zips))
asthma_geo_zip_tot <- asthma_geo_zip_tot %>% filter(geo_zip %in% unlist(zips))


# Restrict to KC ZIPs
kc_zip_trim <- kc_zip_trim %>% filter(COUNTY == "033")

# Convert to spatial object to make neighborhood matrix
kc_zip_sp <- as(kc_zip, "Spatial")
kc_zip_sp_pha <- as(kc_zip_pha, "Spatial")


### Make neighborhood matrix
nb <- poly2nb(kc_zip_sp)
nb2INLA(file.path(spatial_dir, "zip_neighbors.adj"), nb)

nb_pha <- poly2nb(kc_zip_sp_pha)
nb2INLA(file.path(spatial_dir, "zip_neighbors_pha.adj"), nb_pha)

graph_nb <- inla.read.graph(filename = file.path(spatial_dir, "zip_neighbors.adj"))
graph_nb_pha <- inla.read.graph(filename = file.path(spatial_dir, "zip_neighbors_pha.adj"))


# Add indices for spatial (re_s) and non-spatial (re_f) random effects
asthma_geo_zip_tot$re_s <- 1:nrow(asthma_geo_zip_tot)
asthma_geo_zip_tot$re_f <- 1:nrow(asthma_geo_zip_tot)

asthma_geo_zip_pha$re_s <- 1:nrow(asthma_geo_zip_pha)
asthma_geo_zip_pha$re_f <- 1:nrow(asthma_geo_zip_pha)


#### SET UP SPATIAL MODEL ####
inla_model <- inla(count ~ 1 + year + pha_prop + 
                     # Spatial random effects
                     f(re_s, model = "besag", graph.file = graph_nb) + 
                     # Non-spatial random effects
                     f(re_f, model = "iid"),
                   family = "poisson", data = asthma_geo_zip_tot,
                   E = expected, 
                   control.predictor = list(compute = TRUE))

summary(inla_model)
plot(inla_model)


inla_model_pha <- inla(count ~ 1 + year
                       # Spatial random effects
                       + f(re_s, model = "besag", graph.file = graph_nb_pha) 
                       # Non-spatial random effects
                       + f(re_f, model = "iid")
                       ,family = "poisson", data = asthma_geo_zip_pha,
                       E = expected, 
                       control.predictor = list(compute = TRUE))

summary(inla_model_pha)
plot(inla_model_pha)


#### EXTRACT RESULTS ####
asthma_zip_tot_map <- asthma_zip_tot
asthma_zip_tot_map$rr <- inla_model$summary.fitted.values[, "mean"]
asthma_zip_tot_map$lb <- inla_model$summary.fitted.values[, "0.025quant"]
asthma_zip_tot_map$ub <- inla_model$summary.fitted.values[, "0.975quant"]
asthma_zip_tot_map <- asthma_zip_tot_map %>%
  mutate(significant = case_when(
    ub < 1 | lb > 1 ~ 1L,
    ub > 1 & lb < 1 ~ 0L,
    TRUE ~ NA_integer_))

asthma_zip_tot_map_pha <- asthma_zip_tot_pha
asthma_zip_tot_map_pha$rr <- inla_model_pha$summary.fitted.values[, "mean"]
asthma_zip_tot_map_pha$lb <- inla_model_pha$summary.fitted.values[, "0.025quant"]
asthma_zip_tot_map_pha$ub <- inla_model_pha$summary.fitted.values[, "0.975quant"]


kc_zip_trim_pha <- left_join(kc_zip_trim, asthma_zip_tot_map_pha, by = c("ZIPCODE" = "zip"))
kc_zip_trim_map <- left_join(kc_zip_trim, asthma_zip_tot_map, by = c("ZIPCODE" = "zip"))


### Pull out ZIP centroids
zip_centroids <- st_centroid(kc_zip_trim_map)
zip_centroids <- cbind(kc_zip_trim_map, st_coordinates(st_centroid(kc_zip_trim_map$geometry)))











### People who met the inclusion criteria (11+ months of Medicaid, ages 5-64, non-dual)
elig_pop <- bind_rows(lapply(months_list, function(x) {
  date_month <- paste0(str_sub(x, 1, 4), str_sub(x, 6, 7))
  
  sql_temp <- glue::glue_sql("SELECT a.id, a.year_month, b.end_month, a.end_month_age, 
                             c.dobnew, c.gender_mx, c.race_eth_mx, c.maxlang, 'enroll_flag' = 1  
                             FROM
                             (SELECT id, year_month, end_month_age 
                               FROM [PHClaims].[stage].[perf_enroll_denom]
                               WHERE full_benefit_t_12_m >= 11 AND dual_t_12_m = 0 AND 
                               end_month_age >= 5 AND end_month_age < 65 AND 
                               year_month = {date_month}) a 
                             LEFT JOIN
                             (SELECT year_month, end_month, beg_measure_year_month 
                               FROM [ref].[perf_year_month]) b 
                             ON a.year_month = b.year_month
                             LEFT JOIN
                             (SELECT id, dobnew, gender_mx, race_eth_mx, maxlang
                               FROM dbo.mcaid_elig_demoever) c
                             ON a.id = c.id",
                             .con = db_claims)
  
  print(paste0("Collecting data for the year through to ", date_month))
  dbGetQuery(db_claims, sql_temp)
}))

# Single year version for 2017 only
# elig_pop <- dbGetQuery(db_claims,
#                        "SELECT a.id, a.year_month, b.end_month, a.end_month_age, 
#                        c.dobnew, c.gender_mx, c.race_eth_mx, c.maxlang, 'enroll_flag' = 1  
#                        FROM
#                         (SELECT id, year_month, end_month_age 
#                           FROM [PHClaims].[stage].[perf_enroll_denom]
#                           WHERE full_benefit_t_12_m >= 11 AND dual_t_12_m = 0 AND 
#                             end_month_age >= 5 AND end_month_age < 65 AND 
#                               year_month = '201712') a 
#                         LEFT JOIN
#                         (SELECT year_month, end_month, beg_measure_year_month 
#                           FROM [ref].[perf_year_month]) b 
#                         ON a.year_month = b.year_month
#                         LEFT JOIN
#                         (SELECT id, dobnew, gender_mx, race_eth_mx, maxlang
#                           FROM dbo.mcaid_elig_demoever) c
#                         ON a.id = c.id")


### Asthma medication ratio data
# NOTE THAT THIS CODE WILL NEED TO BE UPDATED ONCE THE NEW ETL PROCESS IS RUN
amr_1_year <- dbGetQuery(db_claims,
                         "SELECT end_year_month, id, numerator, denominator
                         FROM stage.mcaid_perf_measure
                         WHERE measure_id = 20 --AND end_year_month = '201712'")


amr <- dbGetQuery(db_claims,
                  "SELECT end_year_month, id, numerator, denominator
                  FROM stage.mcaid_perf_measure
                  WHERE measure_id = 19 --AND end_year_month = '201712'")


#### BRING DATA TOGETHER AND PROCESS ####
housing_amr <- inner_join(housing, elig_pop, 
                          by = c("mid" = "id", 
                                 "end_period" = "end_month")) %>%
  left_join(., amr, by = c("mid" = "id",
                           "year_month" = "end_year_month")) %>%
  left_join(., amr_1_year, by = c("mid" = "id",
                                  "year_month" = "end_year_month")) %>%
  rename(denominator = denominator.x,
         numerator = numerator.x,
         denominator_1yr = denominator.y,
         numerator_1yr = numerator.y) %>%
  replace_na(., list(denominator = 0, numerator = 0, 
                     denominator_1yr = 0, numerator_1yr = 0))


### Allocate people to a housing category based on pt in that year
# Can then drop columns not currently being used
housing_amr_cat <- bind_rows(lapply(months_list, function(x) {
  
  pt_var <- quo_name(paste0("pt", str_sub(x, 3, 4)))
  age_var <- quo_name(paste0("age", str_sub(x, 3, 4)))
  length_var <- quo_name(paste0("length", str_sub(x, 3, 4)))
  
  result <- housing_amr %>%
    filter(end_period == x) %>%
    arrange(pid2, desc(!!sym(pt_var))) %>%
    group_by(pid2) %>%
    slice(1) %>%
    ungroup() %>%
    mutate(
      age_grp = case_when(
        !!sym(age_var) >= 5 & !!sym(age_var) <= 11.999 ~ "5-11",
        !!sym(age_var) >= 12 & !!sym(age_var) <= 18.999 ~ "12-18",
        !!sym(age_var) >= 19 & !!sym(age_var) <= 30.999 ~ "19-30",
        !!sym(age_var) >= 31 & !!sym(age_var) <= 50.999 ~ "31-50",
        !!sym(age_var) >= 51 & !!sym(age_var) <= 64.999 ~ "51-64",
        TRUE ~ NA_character_
      ),
      length_grp = case_when(
        is.na(!!sym(length_var)) ~ NA_character_,
        !!sym(length_var) >= 0 & !!sym(length_var) < 2.999 ~ "<3 years",
        !!sym(length_var) >= 3 & !!sym(length_var) < 5.999 ~ "3-6 years",
        !!sym(length_var) >= 6 ~ "6+ years",
        TRUE ~ NA_character_
      ),
      lang_grp = case_when(
        is.na(maxlang) ~ NA_character_,
        maxlang == "ENGLISH" ~ "ENGLISH",
        maxlang == "SPANISH; CASTILIAN" ~ "SPANISH",
        maxlang == "VIETNAMESE" ~ "VIETNAMESE",
        maxlang == "CHINESE" ~ "CHINESE",
        maxlang == "SOMALI" ~ "SOMALI",
        maxlang == "RUSSIAN" ~ "RUSSIAN",
        maxlang == "ARABIC" ~ "ARABIC",
        maxlang == "KOREAN" ~ "KOREAN",
        maxlang == "UKRAINIAN" ~ "UKRAINIAN",
        maxlang == "AMHARIC" ~ "AMHARIC",
        maxlang == "BURMESE" ~ "BURMESE",
        maxlang == "TIGRINYA" ~ "TIGRINYA",
        TRUE ~ "OTHER"
      )
    ) %>%
    select(end_period, pid2, age_grp, gender_c, 
           ethn_c, lang_grp, length_grp, 
           agency_new, subsidy_type, operator_type, vouch_type_final, 
           zip_c, portfolio_final,
           numerator, denominator, denominator_1yr, numerator_1yr)
  return(result)
}))


#### ANALYZE DATA ####
#### Tableau-ready output ####
### Look at all groups combined
asthma_cnt_combined <- tabloop_f(df = housing_amr_cat,
                                 dcount = list_var(pid2),
                                 sum = list_var(denominator, numerator,
                                                denominator_1yr, numerator_1yr),
                                 fixed = list_var(end_period),
                                 loop = list_var(age_grp, gender_c, 
                                                 ethn_c, length_grp, lang_grp,
                                                 agency_new, subsidy_type,
                                                 operator_type,
                                                 vouch_type_final,
                                                 zip_c,
                                                 portfolio_final)) %>%
  mutate(agency = "Combined")


### Look at all groups combined
asthma_cnt_agency <- tabloop_f(df = housing_amr_cat,
                               dcount = list_var(pid2),
                               sum = list_var(denominator, numerator,
                                              denominator_1yr, numerator_1yr),
                               fixed = list_var(end_period, agency_new),
                               loop = list_var(age_grp, gender_c, 
                                               ethn_c, length_grp, lang_grp,
                                               subsidy_type,
                                               operator_type,
                                               vouch_type_final,
                                               zip_c,
                                               portfolio_final)) %>%
  rename(agency = agency_new)


### Bring together and calculate proportions
asthma_cnt <- bind_rows(asthma_cnt_combined, asthma_cnt_agency) %>%
  mutate(# See what proportion of the pop had persistent asthma
    asthma_pct = round(denominator_sum / pid2_dcount, 4),
    asthma_pct_lb = purrr::map2_dbl(denominator_sum, pid2_dcount,
                                    .f = function (a, b)
                                      if (b > 0) {round(binom.test(a, b)$conf.int[1], 4)} 
                                    else {NA}),
    asthma_pct_ub = purrr::map2_dbl(denominator_sum, pid2_dcount,
                                    .f = function (a, b)
                                      if (b > 0) {round(binom.test(a, b)$conf.int[2], 4)} 
                                    else {NA}),
    # See what proportion with persistent asthma managed it
    amr_pct = ifelse(denominator_sum > 0, round(numerator_sum / denominator_sum, 4), NA),
    amr_pct_lb = purrr::map2_dbl(numerator_sum, denominator_sum,
                                 .f = function (a, b)
                                   if (b > 0) {round(binom.test(a, b)$conf.int[1], 4)} 
                                 else {NA}),
    amr_pct_ub = purrr::map2_dbl(numerator_sum, denominator_sum,
                                 .f = function (a, b)
                                   if (b > 0) {round(binom.test(a, b)$conf.int[2], 4)} 
                                 else {NA}),
    # See what proportion of the pop had asthma in a single year
    asthma_1yr_pct = round(denominator_1yr_sum / pid2_dcount, 4),
    asthma_1yr_pct_lb = purrr::map2_dbl(denominator_1yr_sum, pid2_dcount,
                                        .f = function (a, b)
                                          if (b > 0) {round(binom.test(a, b)$conf.int[1], 4)} 
                                        else {NA}),
    asthma_1yr_pct_ub = purrr::map2_dbl(denominator_1yr_sum, pid2_dcount,
                                        .f = function (a, b)
                                          if (b > 0) {round(binom.test(a, b)$conf.int[2], 4)} 
                                        else {NA}),
    # See what proportion with asthma in a single year managed it
    amr_1yr_pct = ifelse(denominator_1yr_sum > 0, round(numerator_1yr_sum / denominator_1yr_sum, 4), NA),
    amr_1yr_pct_lb = purrr::map2_dbl(numerator_1yr_sum, denominator_1yr_sum,
                                     .f = function (a, b)
                                       if (b > 0) {round(binom.test(a, b)$conf.int[1], 4)} 
                                     else {NA}),
    amr_1yr_pct_ub = purrr::map2_dbl(numerator_1yr_sum, denominator_1yr_sum,
                                     .f = function (a, b)
                                       if (b > 0) {round(binom.test(a, b)$conf.int[2], 4)} 
                                     else {NA})
  ) %>%
  rename(cat1 = group_cat, cat1_group = group, population = pid2_dcount,
         asthma = denominator_sum, amr_50 = numerator_sum,
         asthma_1yr = denominator_1yr_sum, amr_50_1yr = numerator_1yr_sum) %>%
  select(end_period, agency, cat1, cat1_group, population, asthma, amr_50,
         asthma_pct, asthma_pct_lb, asthma_pct_ub, 
         amr_pct, amr_pct_lb, amr_pct_ub,
         asthma_1yr, amr_50_1yr,
         asthma_1yr_pct, asthma_1yr_pct_lb, asthma_1yr_pct_ub,
         amr_1yr_pct, amr_1yr_pct_lb, amr_1yr_pct_ub) %>%
  # Fix up category names
  mutate(cat1 = case_when(
    cat1 == "age_grp" ~ "Age group",
    cat1 == "agency_new" ~ "Agency",
    cat1 == "ethn_c" ~ "Race/ethnicity",
    cat1 == "gender_c" ~ "Gender",
    cat1 == "lang_grp" ~ "Language",
    cat1 == "length_grp" ~ "Length of time in housing",
    cat1 == "operator_type" ~ "Operator",
    cat1 == "portfolio_final" ~ "Portfolio",
    cat1 == "subsidy_type" ~ "Subsidy type",
    cat1 == "vouch_type_final" ~ "Voucher type",
    cat1 == "zip_c" ~ "ZIP",
    TRUE ~ cat1)) %>%
  # Remove annoying rows
  filter(!(cat1 %in% c("Age group", "Language") & is.na(cat1_group)) &
           !(cat1 == "ZIP" & cat1_group %in% c("0", "8261")) &
           !(cat1 %in% c("Operator", "Portfolio", "Voucher type") & cat1_group == ""))


### Add suppression flags
asthma_cnt_supp <- asthma_cnt %>%
  mutate(population_supp = ifelse(population < 11, 1, 0),
         asthma_supp = ifelse(asthma < 11, 1, 0),
         amr_50_supp = ifelse(amr_50 < 11, 1, 0),
         asthma_1yr_supp = ifelse(asthma_1yr < 11, 1, 0),
         amr_50_1yr_supp = ifelse(amr_50_1yr < 11, 1, 0))

### Check to see if the overall/combined group needs to be suppressed
# This is to prevent back calculating a suppressed number
supp_check_pop <- asthma_cnt_supp %>%
  filter(population_supp == 1) %>%
  group_by(end_period, cat1, cat1_group) %>%
  summarise(count = n()) %>% ungroup() %>%
  mutate(population_supp2 = ifelse(count == 1, 1, 0)) %>%
  select(end_period, cat1, cat1_group, population_supp2)

supp_check_asthma <- asthma_cnt_supp %>%
  filter(asthma_supp == 1) %>%
  group_by(end_period, cat1, cat1_group) %>%
  summarise(count = n()) %>% ungroup() %>%
  mutate(asthma_supp2 = ifelse(count == 1, 1, 0)) %>%
  select(end_period, cat1, cat1_group, asthma_supp2)

supp_check_amr <- asthma_cnt_supp %>%
  filter(amr_50_supp == 1) %>%
  group_by(end_period, cat1, cat1_group) %>%
  summarise(count = n()) %>% ungroup() %>%
  mutate(amr_50_supp2 = ifelse(count == 1, 1, 0)) %>%
  select(end_period, cat1, cat1_group, amr_50_supp2)

supp_check_asthma_1yr <- asthma_cnt_supp %>%
  filter(asthma_1yr_supp == 1) %>%
  group_by(end_period, cat1, cat1_group) %>%
  summarise(count = n()) %>% ungroup() %>%
  mutate(asthma_1yr_supp2 = ifelse(count == 1, 1, 0)) %>%
  select(end_period, cat1, cat1_group, asthma_1yr_supp2)

supp_check_amr_1yr <- asthma_cnt_supp %>%
  filter(amr_50_1yr_supp == 1) %>%
  group_by(end_period, cat1, cat1_group) %>%
  summarise(count = n()) %>% ungroup() %>%
  mutate(amr_50_1yr_supp2 = ifelse(count == 1, 1, 0)) %>%
  select(end_period, cat1, cat1_group, amr_50_1yr_supp2)


# Join back and suppress rows
asthma_cnt_supp <- list(asthma_cnt_supp, supp_check_pop, supp_check_asthma, supp_check_amr,
                        supp_check_asthma_1yr, supp_check_amr_1yr) %>%
  reduce(left_join, by = c("end_period" = "end_period", "cat1" = "cat1", 
                           "cat1_group" = "cat1_group")) %>%
  mutate_at(vars(population_supp2, asthma_supp2, amr_50_supp2,
                 asthma_1yr_supp2, amr_50_1yr_supp2),
            funs(ifelse(is.na(.), 0, .)))


asthma_cnt_supp <- asthma_cnt_supp %>%
  mutate(
    population = case_when(
      population_supp == 1 ~ NA_real_,
      population_supp2 == 1 & agency == "Combined" ~ NA_real_,
      TRUE ~ population)) %>%
  mutate_at(vars(asthma, amr_50, asthma_pct,
                 asthma_pct_lb, asthma_pct_ub, amr_pct,
                 amr_pct_lb, amr_pct_ub),
            funs(case_when(
              asthma_supp == 1 | is.na(population) ~ NA_real_,
              (asthma_supp2 == 1 | is.na(population)) & agency == "Combined" ~ NA_real_,
              TRUE ~ .))) %>%
  mutate_at(vars(asthma_1yr, amr_50_1yr,
                 asthma_1yr_pct, asthma_1yr_pct_lb, asthma_1yr_pct_ub,
                 amr_1yr_pct, amr_1yr_pct_lb, amr_1yr_pct_ub),
            funs(case_when(
              asthma_1yr_supp == 1 | is.na(population) ~ NA_real_,
              (asthma_1yr_supp2 == 1 | is.na(population)) & agency == "Combined" ~ NA_real_,
              TRUE ~ .))) %>%
  mutate_at(vars(amr_50, amr_pct, amr_pct_lb, amr_pct_ub),
            funs(case_when(
              amr_50_supp == 1 | is.na(asthma) ~ NA_real_,
              (amr_50_supp2 == 1 | is.na(asthma)) & agency == "Combined" ~ NA_real_,
              TRUE ~ .))) %>%
  mutate_at(vars(amr_50_1yr, amr_1yr_pct, amr_1yr_pct_lb, amr_1yr_pct_ub),
            funs(case_when(
              amr_50_1yr_supp == 1 | is.na(asthma_1yr) ~ NA_real_,
              (amr_50_1yr_supp2 == 1 | is.na(asthma_1yr)) & agency == "Combined" ~ NA_real_,
              TRUE ~ .)))


### Save for Tableau
write.csv(asthma_cnt_supp, paste0(dashh_2_path, 
                                  "/persistent_asthma_", Sys.Date(), ".csv"),
          row.names = F)







#### Regression ####
# Restrict to 2017 and recode
amr_regression_df <- housing_amr_cat %>%
  filter(end_period == "2017-12-31") %>%
  mutate(pha = ifelse(agency_new == "Non-PHA", 0, 1),
         age_grp = factor(age_grp,
                          levels = c("5-11", "12-18", "19-30", "31-50", "51-64"),
                          labels = c("5-11", "12-18", "19-30", "31-50", "51-64")),
         length_grp = factor(length_grp,
                             levels = c("<3 years", "3-6 years", "6+ years"),
                             labels = c("<3 years", "3-6 years", "6+ years"))
  )

### Compare asthma among PHA to non-PHA
asthma_overall <- glm(denominator_1yr ~ pha + age_grp + gender_c + ethn_c,
                      data = amr_regression_df,
                      family = "binomial")

summary(asthma_overall)

# Summarise output using broom package
asthma_overall_output <- tidy(asthma_overall, conf.int = T, exponentiate = T)
asthma_overall_output <- asthma_overall_output %>%
  mutate(model = rep("Asthma comparing PHA/non-PHA", nrow(asthma_overall_output))) %>%
  select(model, term, estimate, std.error, p.value, conf.low, conf.high)

asthma_overall_output_wald <- data.frame(
  model = rep("Asthma comparing PHA/non-PHA", 3),
  term = c("Age", "Gender", "Race/ethnicity"),
  chi2 = c(
    wald.test(b = coef(asthma_overall), Sigma = vcov(asthma_overall), Terms = 3:6)$result$chi2[3],
    wald.test(b = coef(asthma_overall), Sigma = vcov(asthma_overall), Terms = 7:8)$result$chi2[3],
    wald.test(b = coef(asthma_overall), Sigma = vcov(asthma_overall), Terms = 9:14)$result$chi2[3]
  )
)

### Compare asthma within PHAs in public housing
asthma_pha_hard <- glm(denominator_1yr ~ agency_new + age_grp + gender_c + ethn_c + length_grp + operator_type,
                       data = filter(amr_regression_df, agency_new != "Non-PHA" & subsidy_type == "HARD UNIT" &
                                       operator_type != ""),
                       family = "binomial")

summary(asthma_pha_hard)
asthma_pha_hard_output <- tidy(asthma_pha_hard, conf.int = T, exponentiate = T)
asthma_pha_hard_output <- asthma_pha_hard_output %>%
  mutate(model = rep("Asthma in PHA hard units", nrow(asthma_pha_hard_output))) %>%
  select(model, term, estimate, std.error, p.value, conf.low, conf.high)

asthma_pha_hard_output_wald <- data.frame(
  model = rep("Asthma in PHA hard units", 4),
  term = c("Age", "Gender", "Race/ethnicity", "Length of time"),
  chi2 = c(
    wald.test(b = coef(asthma_pha_hard), Sigma = vcov(asthma_pha_hard), Terms = 3:6)$result$chi2[3],
    wald.test(b = coef(asthma_pha_hard), Sigma = vcov(asthma_pha_hard), Terms = 7:8)$result$chi2[3],
    wald.test(b = coef(asthma_pha_hard), Sigma = vcov(asthma_pha_hard), Terms = 9:14)$result$chi2[3],
    wald.test(b = coef(asthma_pha_hard), Sigma = vcov(asthma_pha_hard), Terms = 15:16)$result$chi2[3]
  )
)


### Compare asthma within PHAs in public housing
asthma_pha_soft <- glm(denominator_1yr ~ agency_new + age_grp + gender_c + ethn_c + length_grp + vouch_type_final,
                       data = filter(amr_regression_df, agency_new != "Non-PHA" & subsidy_type == "TENANT BASED/SOFT UNIT"),
                       family = "binomial")

summary(asthma_pha_soft)

asthma_pha_soft_output <- tidy(asthma_pha_soft, conf.int = T, exponentiate = T)
asthma_pha_soft_output <- asthma_pha_soft_output %>%
  mutate(model = rep("Asthma in PHA soft units", nrow(asthma_pha_soft_output))) %>%
  select(model, term, estimate, std.error, p.value, conf.low, conf.high)

asthma_pha_soft_output_wald <- data.frame(
  model = rep("Asthma in PHA soft units", 5),
  term = c("Age", "Gender", "Race/ethnicity", "Time in housing", "Voucher type"),
  chi2 = c(
    wald.test(b = coef(asthma_pha_soft), Sigma = vcov(asthma_pha_soft), Terms = 3:6)$result$chi2[3],
    wald.test(b = coef(asthma_pha_soft), Sigma = vcov(asthma_pha_soft), Terms = 7:8)$result$chi2[3],
    wald.test(b = coef(asthma_pha_soft), Sigma = vcov(asthma_pha_soft), Terms = 9:14)$result$chi2[3],
    wald.test(b = coef(asthma_pha_soft), Sigma = vcov(asthma_pha_soft), Terms = 15:16)$result$chi2[3],
    wald.test(b = coef(asthma_pha_soft), Sigma = vcov(asthma_pha_soft), Terms = 17:21)$result$chi2[3]
  )
)


### Compare AMR among PHA to non-PHA
amr_overall <- glm(numerator_1yr ~ pha + age_grp + gender_c + ethn_c,
                   data = filter(amr_regression_df, denominator_1yr == 1),
                   family = "binomial")

summary(amr_overall)

# Summarise output using broom package
amr_overall_output <- tidy(amr_overall, conf.int = T, exponentiate = T)
amr_overall_output <- amr_overall_output %>%
  mutate(model = rep("AMR comparing PHA/non-PHA", nrow(amr_overall_output))) %>%
  select(model, term, estimate, std.error, p.value, conf.low, conf.high)

amr_overall_output_wald <- data.frame(
  model = rep("AMR comparing PHA/non-PHA", 3),
  term = c("Age", "Gender", "Race/ethnicity"),
  chi2 = c(
    wald.test(b = coef(amr_overall), Sigma = vcov(amr_overall), Terms = 3:6)$result$chi2[3],
    wald.test(b = coef(amr_overall), Sigma = vcov(amr_overall), Terms = 7:8)$result$chi2[3],
    wald.test(b = coef(amr_overall), Sigma = vcov(amr_overall), Terms = 9:14)$result$chi2[3]
  )
)

### Compare AMR within PHAs in public housing
amr_pha_hard <- glm(numerator_1yr ~ agency_new + age_grp + gender_c + ethn_c + length_grp + operator_type,
                    data = filter(amr_regression_df, agency_new != "Non-PHA" & subsidy_type == "HARD UNIT" &
                                    operator_type != "" & denominator_1yr == 1),
                    family = "binomial")

summary(amr_pha_hard)
amr_pha_hard_output <- tidy(amr_pha_hard, conf.int = T, exponentiate = T)
amr_pha_hard_output <- amr_pha_hard_output %>%
  mutate(model = rep("AMR in PHA hard units", nrow(amr_pha_hard_output))) %>%
  select(model, term, estimate, std.error, p.value, conf.low, conf.high)

amr_pha_hard_output_wald <- data.frame(
  model = rep("AMR in PHA hard units", 4),
  term = c("Age", "Gender", "Race/ethnicity", "Time in housing"),
  chi2 = c(
    wald.test(b = coef(amr_pha_hard), Sigma = vcov(amr_pha_hard), Terms = 3:6)$result$chi2[3],
    wald.test(b = coef(amr_pha_hard), Sigma = vcov(amr_pha_hard), Terms = 7:8)$result$chi2[3],
    wald.test(b = coef(amr_pha_hard), Sigma = vcov(amr_pha_hard), Terms = 9:14)$result$chi2[3],
    wald.test(b = coef(amr_pha_hard), Sigma = vcov(amr_pha_hard), Terms = 15:16)$result$chi2[3]
  )
)


### Compare asthma within PHAs in public housing
# Exclude small number with multiple gender as all fall in same AMR category
amr_pha_soft <- glm(numerator_1yr ~ agency_new + age_grp + gender_c + ethn_c + length_grp + vouch_type_final,
                    data = filter(amr_regression_df, agency_new != "Non-PHA" & 
                                    subsidy_type == "TENANT BASED/SOFT UNIT" & denominator_1yr == 1 & 
                                    gender_c != "Multiple"),
                    family = "binomial")

summary(amr_pha_soft)

amr_pha_soft_output <- tidy(amr_pha_soft, conf.int = T, exponentiate = T)
amr_pha_soft_output <- amr_pha_soft_output %>%
  mutate(model = rep("AMR in PHA soft units", nrow(amr_pha_soft_output))) %>%
  select(model, term, estimate, std.error, p.value, conf.low, conf.high)

amr_pha_soft_output_wald <- data.frame(
  model = rep("AMR in PHA soft units", 5),
  term = c("Age", "Gender", "Race/ethnicity", "Time in housing", "Voucher type"),
  chi2 = c(
    wald.test(b = coef(amr_pha_soft), Sigma = vcov(amr_pha_soft), Terms = 3:6)$result$chi2[3],
    wald.test(b = coef(amr_pha_soft), Sigma = vcov(amr_pha_soft), Terms = 7:7)$result$chi2[3],
    wald.test(b = coef(amr_pha_soft), Sigma = vcov(amr_pha_soft), Terms = 8:15)$result$chi2[3],
    wald.test(b = coef(amr_pha_soft), Sigma = vcov(amr_pha_soft), Terms = 16:17)$result$chi2[3],
    wald.test(b = coef(amr_pha_soft), Sigma = vcov(amr_pha_soft), Terms = 17:20)$result$chi2[3]
  )
)


### Combine together
model_output <- bind_rows(asthma_overall_output, asthma_pha_hard_output, asthma_pha_soft_output,
                          amr_overall_output, amr_pha_hard_output, amr_pha_soft_output) %>%
  mutate_if(is.numeric, round, 4)

model_output_wald <- bind_rows(asthma_overall_output_wald, asthma_pha_hard_output_wald, 
                               asthma_pha_soft_output_wald,
                               amr_overall_output_wald, amr_pha_hard_output_wald, 
                               amr_pha_soft_output_wald) %>%
  mutate(chi2 = round(chi2, 4))

model_output_overall <- bind_rows(model_output, model_output_wald) %>%
  mutate(term = str_replace(term, "ethn_c", "Race/ethnicity: "),
         term = str_replace(term, "gender_c", "Gender: "),
         term = str_replace(term, "age_grp", "Age: "),
         term = str_replace(term, "length_grp", "Time in housing: "),
         term = str_replace(term, "agency_new", "PHA: "),
         term = str_replace(term, "operator_type", "Operator type: "),
         term = str_replace(term, "vouch_type_final", "Voucher type: ")) %>%
  arrange(model, term) %>%
  rename(odds_ratio = estimate, SE = std.error, p = p.value,
         ci_lb = conf.low, ci_ub = conf.high) %>%
  mutate_at(vars(p, chi2),
            funs(case_when(
              . == 0 ~ as.character("<0.0001"),
              TRUE ~ as.character(.)
            )
            ))


### Write out table
options(knitr.kable.NA = '')
temp <- model_output_overall %>% 
  filter(model == "Asthma comparing PHA/non-PHA") %>%
  select(-model) %>%
  mutate(p = case_when(
    is.na(p) ~ NA_character_,
    TRUE ~ cell_spec(p, "latex", bold = ifelse(as.numeric(str_replace(p, "<", "")) < 0.05, T, F))
  ),
  chi2 = case_when(
    is.na(chi2) ~ NA_character_,
    TRUE ~ cell_spec(chi2, "latex", bold = ifelse(as.numeric(str_replace(chi2, "<", "")) < 0.05, T, F))
  )) %>%
  kable(format = "latex", booktabs = T, escape = F,
        caption = "Binomial regression model outputs for 1-yr asthma and  AMR") %>%
  collapse_rows(columns = 1, valign = "top") %>%
  footnote(general = "chi2 column shows the output of the Wald test for the entire group")

temp <- gsub("(_)", "\\\\\\1", temp)

render(file.path(getwd(), "analyses/pha_asthma_med_ratio_output.Rmd"), "pdf_document",
       output_file = file.path(dashh_2_path, "Asthma", "pha_asthma_med_ratio_output.pdf"))



