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

dashh_2_path <- "C:/Users/mathesal/King County/Laurent, Amy - DASHH-External/DASHH2.0"
spatial_dir <- paste0(dashh_2_path, "/Deep Dive/Asthma/spatial")


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
# Restrict to KC ZIPs
kc_zip_trim <- kc_zip_trim %>% filter(COUNTY == "033")


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
#### SET UP A FUNCTION THAT CAN WORK OVER DIFFERENT YEARS ####
geo_setup_f <- function(year = 2016, pha = F) {
  
  #### Restrict to relevant year ####
  if (pha == F & year <= 2016) {
    df <- ccw_asthma_demog %>% filter(year == year)
  } else if (pha == F & year >= 2017) {
    df <- ccw_asthma_demog %>% filter(year == year & age_yr < 65 & dual != 1)
  } else if (pha == T & year <= 2016) {
    df <- ccw_asthma_demog %>% filter(year == year & pha_agency %in% c("SHA", "KCHA"))
  } else if (pha == T & year >= 2017) {
    df <- ccw_asthma_demog %>% filter(year == year & age_yr < 65 & dual != 1 & 
                                        pha_agency %in% c("SHA", "KCHA"))
  }
 
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
    mutate(year = year)
  
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
      filter(year %in% year) %>%
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


#### SET UP A FUNCTION TO MAKE MAPS ####  
geo_zip_map <- function(df, title = F) {
  # Pull out year and set up title
  year <- unique(df$df$year)
  if (title == F) {
    title <- ""
  } else {
    title <- glue("Risk of having asthma among {pop} members in King County, by ZIP ({year})",
                  pop = ifelse(year < 2017, "Medicaid and Medicare", "Medicaid (non-dual)"))
  }
  
  map <- ggplot(data = df$zip_trim_map) + 
    geom_sf(aes(fill = rr)) + 
    geom_text(aes(x = X, y = Y, 
                  label = ifelse(df$zip_trim_map$direction == 1L, sprintf('\u2191'), 
                                 ifelse(df$zip_trim_map$direction == -1L, sprintf('\u2193'), ""))),
              color = ifelse(df$zip_trim_map$direction == 1L, "grey20", 
                             ifelse(df$zip_trim_map$direction == -1L, "grey70", ""))) + 
    scale_fill_viridis_c(option = "magma", trans = "sqrt") +
    labs(title = title,
         #subtitle = "Adjusted for proportion of Medicaid members who are also PHA members",
         caption = paste0(sprintf('\u2191'), "/", sprintf('\u2193'), " = signficicantly different risk"),
         x = "Longitude", y = "Latitude", fill = "Relative risk")
  
  return(map)
}


### RUN DATA
geo_zip_16 <- geo_setup_f(year = 2016, pha = F)
geo_zip_18 <- geo_setup_f(year = 2018, pha = F)

geo_zip_18_pha <- geo_setup_f(year = 2018, pha = T)

geo_zip_map(geo_zip_18)


### Write out PPT slides
render(file.path(getwd(), "analyses/asthma/pha_asthma_convening.Rmd"), "powerpoint_presentation",
       output_file = file.path(dashh_2_path, "Convening", "DASHH convening 2019-02 - data slides.pptx"))

