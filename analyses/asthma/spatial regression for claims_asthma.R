### SPATIAL REGRESSION MODELS FOR CLAIMS DATA
# ALASTAIR MATHESON (PHSKC)
# 2019-09

#### NOTES ####
# This approach makes use of integrated nested LaPlace approximation (INLA)
# More resources are here: http://www.r-inla.org/home
# And some more info about INLA is here: https://www.precision-analytics.ca/blog/a-gentle-inla-tutorial/
# A worked example: https://journal.r-project.org/archive/2018/RJ-2018-036/RJ-2018-036.pdf


#### LOAD REQURIED PACKAGES AND SET OPTIONS ####
# If installing INLA for the first time:
# install.packages("INLA", repos=c(getOption("repos"), INLA="https://inla.r-inla-download.org/R/stable"), dep=TRUE)

library(spdep) # Use this to identify neighbors
library(SpatialEpi) # Calculate expected number of cases
library(INLA)
library(sf) # Use this to read in shape files
library(odbc) # Connect to SQL
library(tidyverse) # Data manipulation
library(data.table) # Data manipulation

working_dir <- "<set where you want files to live>"
working_dir <- "C:/Users/mathesal/King County/Laurent, Amy - DASHH-External/DASHH2.0/Deep Dive/Asthma/spatial"


db_phclaims <- dbConnect(odbc(), "PHClaims51")
db_apde51 <- dbConnect(odbc(), "PH_APDEStore51")

#### BRING IN DATA AND SET UP ####
### Bring in claims data
asthma <- dbGetQuery(db_phclaims,"SELECT distinct id_mcaid as id, 1 as asthma,
                       from_date,to_date
                       FROM stage.mcaid_claim_ccw
                       Where from_date <= '2017-12-31'AND
                       to_date >= '2017-01-01' AND
                       ccw_code = 6")


housing_temp <- dbGetQuery(db_apde51,
                           "SELECT pid2, 
                       overlap_type, startdate_c, enddate_c, 
                     enroll_type, id_mcaid, dob_c, age17, race_c, hisp_c, ethn_c, gender_c, 
                      agency_new, subsidy_type, operator_type, vouch_type_final, 
                     zip_c,kc_area_h, portfolio_final,
                      port_in, port_out_kcha, port_out_sha, dual_elig_m,
                      pt17_h, pt17_m, pt17, length17
                      FROM stage.mcaid_pha
                        WHERE startdate_c <= '2017-12-31' AND enddate_c >= '2017-01-01'")

elig_pop <- dbGetQuery(db_phclaims,
                       "SELECT a.id_mcaid, a.year_month, b.end_month, a.end_month_age,
                       c.dob, c.gender_me, c.race_eth_me, c.lang_max, 'enroll_flag' = 1
                       FROM
                        (SELECT id_mcaid, year_month, end_month_age
                          FROM [PHClaims].[stage].[perf_enroll_denom]
                          WHERE full_benefit_t_12_m >= 11 AND dual_t_12_m = 0 AND
                            end_month_age >= 5 AND end_month_age < 65 AND
                              year_month = '201712') a
                        LEFT JOIN
                        (SELECT year_month, end_month, beg_measure_year_month
                          FROM [ref].[perf_year_month]) b
                        ON a.year_month = b.year_month
                        LEFT JOIN
                        (SELECT id_mcaid, dob, gender_me, race_eth_me, lang_max
                          FROM [final].[mcaid_elig_demo]) c
                        ON a.id_mcaid = c. id_mcaid")

zip <- dbGetQuery(db_phclaims,
                 "SELECT zip_code as zip_c, city 
                 FROM ref.apcd_zip WHERE county_name = 'King' and state = 'WA'")

housing <- left_join(elig_pop, housing_temp, by = "id_mcaid")
housing <- right_join(housing, zip, by = "zip_c")



#### FUNCTIONS ####
chronic_pop_f <- function(df, year = 17, test = F) {
  
  agex_quo <- rlang::sym(paste0("age", quo_name(year), "_grp"))
  lengthx_quo <- rlang::sym(paste0("length", quo_name(year), "_grp"))
  ptx_quo <- rlang::sym(paste0("pt", quo_name(year)))
  
  year_full <- as.numeric(paste0(20, year))
  
  
  # Make new generic vars so data table works
  # Currently only using ptx, expand to others if more code converted
  df <- df %>% 
    mutate(ptx = !!ptx_quo,
           agex = !!agex_quo,
           lengthx = !!lengthx_quo)
  
  
  # Count a person if they were in that group at any point in the period
  # Also count person time accrued in each group (in days)
  df <- setDT(df)
  
  pt <- df[!is.na(ptx), .(pid2, agency_new, enroll_type, agex, gender_c,
                          ethn_c, dual_elig_m, vouch_type_final, subsidy_type,
                          operator_type, portfolio_final, lengthx, zip_c, ptx)]
  pt <- pt[, pt_days := sum(ptx),
           by = .(pid2, agency_new, enroll_type, agex, gender_c,
                  ethn_c, dual_elig_m, vouch_type_final, subsidy_type,
                  operator_type, portfolio_final, lengthx, zip_c)]
  
  pt[, ptx := NULL]
  
  # Allocate an individual to a PHA/program based on rules:
  # 1) Medicaid only and PHA only = Medicaid row with most time
  #   (rationale is we can look at the health data for Medicaid portion at least)
  # 2) Medicaid only and PHA/Medicaid = PHA group with most person-time where
  #    person was enrolled in both housing and Medicaid
  # 3) Multiple PHAs = PHA group with most person-time for EACH PHA where
  #    person was enrolled in both housing and Medicaid
  # 4) PHA only = group with most person-time (for one or more PHAs)
  # Note that this only allocates individuals, not person-time, which should
  # be allocated to each group in which it is accrued
  
  # Join back to a single df
  df_pop <- merge(df, pt, by = c("pid2", "agency_new", "enroll_type", "agex", "gender_c", 
                                 "ethn_c", "dual_elig_m", "vouch_type_final", "subsidy_type", 
                                 "operator_type", "portfolio_final", "lengthx", "zip_c"))
  setorder(df_pop, pid2, agency_new, enroll_type, -ptx)
  
  # Find the row with the most person-time in each group
  # (ties will be broken by whatever other ordering exists)
  pop <- copy(df_pop)
  pop <- pop[!is.na(ptx)]
  # pop <- df_pop[ptx > 0 & !is.na(ptx)]
  pop <- pop[pop[, .I[1], by = .(pid2, agency_new, enroll_type)]$V1]
  # pop <- pop[order(pid2, agency_new, enroll_type, -ptx)]
  # pop[pop[, .I[1], by = .(pid2, agency_new, enroll_type)]$V1]
  
  # Number of agencies, should only be one row per possibility below
  pop[, agency_count := NA_integer_]
  pop[, agency_count := ifelse(agency_new == "KCHA" & enroll_type == "h", 0L, agency_count)]
  pop[, agency_count := ifelse(agency_new == "SHA" & enroll_type == "h", 0L, agency_count)]
  pop[, agency_count := ifelse(agency_new == "KCHA" & enroll_type == "b", 1L, agency_count)]
  pop[, agency_count := ifelse(agency_new == "SHA" & enroll_type == "b", 2L, agency_count)]
  pop[, agency_count := ifelse(agency_new == "Non-PHA", 4L, agency_count)]
  
  
  # Count up permutations of agency and enrollment
  pop[, agency_sum := sum(agency_count, na.rm = T), by = pid2]
  
  # Throw up a list of IDs to check out
  if (test == T) {
    temp <- pop %>% sample_n(5)
    print(temp)
  }
  
  # Filter so only rows meeting the rules above are kept
  pop <- pop[(agency_sum == 4 & agency_count == 4) | 
               (agency_sum == 5 & agency_count == 1) |
               (agency_sum == 6 & agency_count == 2) |
               (agency_sum == 7 & agency_count == 1) | 
               (agency_sum == 7 & agency_count == 2) |
               (agency_sum == 1 & agency_count == 1) |
               (agency_sum == 2 & agency_count == 2) |
               agency_sum == 3 |
               agency_sum == 0]
  
  pop <- pop[, .(pid2, id_mcaid, agency_new, enroll_type, dual_elig_m, agex, 
                 gender_c, ethn_c, vouch_type_final, subsidy_type, operator_type, 
                 portfolio_final, lengthx, zip_c, age17, city)]
  setnames(pop, old = c("agex", "lengthx"), new = c("age", "length"))
  pop[, year := as.numeric(paste0("20", year))]
  
  return(pop)
} 

agecode_f <- function(df, x = age17) {
  col <- enquo(x)
  varname <- paste(quo_name(col), "grp", sep = "_")
  df %>%
    mutate(!!varname := case_when(
      (!!col) >= 5 & (!!col) <= 11.999 ~ "5-11",
      (!!col) >= 12 & (!!col) <= 18.999 ~ "12-18",
      (!!col) >= 19 & (!!col) <= 30.999 ~ "19-30",
      (!!col) >= 31 & (!!col) <= 64.999 ~ "31-64",
      is.na((!!col)) ~ "Unknown")
    )
}
lencode_f <- function(df, x = length17, agency = agency_new) {
  col <- enquo(x)
  agency <- enquo(agency)
  
  varname <- paste(quo_name(col), "grp", sep = "_")
  df %>%
    mutate(!!varname := case_when(
      (!!col) < 3 ~ "<3 years",
      between((!!col), 3, 5.99) ~ "3-<6 years",
      (!!col) >= 6 ~ "6+ years",
      is.na((!!agency)) | (!!agency) == "Non-PHA" | (!!agency) == 0 ~ "Non-PHA",
      is.na((!!col)) ~ "Unknown")
    )
}

# Allocate people into one bucket for making chronic disease denominators
eventcount_chronic_f <- function(df_chronic = chronic, df_pop = chronic_pop,
                                 condition = NULL, year = 17) {
  
  yr_chk <- as.numeric(paste0("20", year))
  year_start = as.Date(paste0("20", year, "-01-01"), origin = "1970-01-01")
  year_end = as.Date(paste0("20", year, "-12-31"), origin = "1970-01-01")
  
  condition_quo <- enquo(condition)
  
  # Filter to only include people with the condition in that year
  cond <- df_chronic %>%
    filter(!!condition_quo == 1 & from_date <= year_end & to_date >= year_start) %>%
    distinct(id) %>%
    mutate(condition = 1)
  
  
  df_pop <- df_pop %>% filter(year == yr_chk)
  
  ### Join pop and condition data to summarise
  output <- left_join(df_pop, cond, by =  c("id_mcaid"="id"))
  output <- output %>% mutate(condition = if_else(is.na(condition), 0, 1))
  
  output <- output %>%
    group_by( id_mcaid,agency_new, enroll_type, dual_elig_m, age,
              gender_c, ethn_c, vouch_type_final, subsidy_type, operator_type,
              portfolio_final, length, zip_c) %>%
    summarise(count := sum(condition)) %>%
    ungroup() %>%
    select(id_mcaid, agency_new, enroll_type, dual_elig_m, age,
           gender_c, ethn_c, vouch_type_final, subsidy_type, operator_type,
           portfolio_final, length, zip_c,
           count) %>%
    rename(agency = agency_new,
           dual = dual_elig_m,
           gender = gender_c,
           ethn = ethn_c,
           portfolio = portfolio_final,
           zip = zip_c)
  
  return(output)
  
}


#### SUMMARIZE ASTHMA ####
# Recode age and length of time into groups
housing <- agecode_f(housing, age17)
housing <- lencode_f(housing,length17)

# Allocate people with asthma, after putting into buckets
chronic_pop <- chronic_pop_f(housing, year = 17, test = F)
asthma_pop <- eventcount_chronic_f(df_chronic = asthma, df_pop = chronic_pop, condition = asthma, year = 17)

# Make zip summary
asthma_zip <- asthma_pop %>%
  mutate(ethn = ifelse(ethn == "", NA_character_, ethn)) %>%
  group_by(zip, gender, ethn, age) %>%
  summarise(asthma = sum(count, na.rm = T), pop = n()) %>%
  ungroup()

asthma_zip_pha <- asthma_pop %>%
  filter(agency %in% c("SHA", "KCHA")) %>%
  mutate(ethn = ifelse(ethn == "", NA_character_, ethn)) %>%
  group_by(zip, gender, ethn, age) %>%
  summarise(asthma = sum(count, na.rm = T), pop = n()) %>%
  ungroup()


# Make a matrix of all possible combinations
zip_matrix <- expand.grid(zip = unlist(distinct(asthma_zip, zip)), 
                          gender = unlist(distinct(asthma_zip, gender)),
                          ethn = unlist(distinct(asthma_zip, ethn)), 
                          age = unlist(distinct(asthma_zip, age)))

zip_matrix <- zip_matrix[order(zip_matrix$zip, zip_matrix$gender, 
                               zip_matrix$ethn, zip_matrix$age), ]


# Join with actual data and make NA = 0
asthma_zip <- left_join(zip_matrix, asthma_zip,
                      by = c("zip", "gender", "ethn", "age")) %>%
  mutate_at(vars(asthma, pop), list( ~ ifelse(is.na(.), 0, .)))

asthma_zip_pha <- left_join(zip_matrix, asthma_zip_pha,
                          by = c("zip", "gender", "ethn", "age")) %>%
  mutate_at(vars(asthma, pop), list( ~ ifelse(is.na(.), 0, .)))

# Sum counts for each ZIP
asthma_zip_tot <- asthma_zip %>% group_by(zip) %>%
  summarise(count = sum(asthma), pop = sum(pop)) %>% ungroup()

asthma_zip_tot_pha <- asthma_zip_pha %>% group_by(zip) %>%
  summarise(count = sum(asthma), pop = sum(pop)) %>% ungroup()

# Remove ZIPs with only one person in them
asthma_zip_tot <- asthma_zip_tot %>% filter(pop > 1)
asthma_zip_tot_pha <- asthma_zip_tot_pha %>% filter(pop > 1)

asthma_zip <- inner_join(asthma_zip, distinct(asthma_zip_tot, zip), by = "zip")
asthma_zip_pha <- inner_join(asthma_zip_pha, distinct(asthma_zip_tot_pha, zip), by = "zip")

# Remove combinations with 0 people in them
# Otherwise cannot calculate expected number of people below
strata_pop <- asthma_zip %>% group_by(gender, ethn, age) %>%
  summarise(pop = sum(pop)) %>% ungroup() %>%
  filter(pop > 0)

strata_pop_pha <- asthma_zip_pha %>% group_by(gender, ethn, age) %>%
  summarise(pop = sum(pop)) %>% ungroup() %>%
  filter(pop > 0)

asthma_zip <- inner_join(asthma_zip, distinct(strata_pop, gender, ethn, age),
                       by = c("gender", "ethn", "age"))

asthma_zip_pha <- inner_join(asthma_zip_pha, distinct(strata_pop_pha, gender, ethn, age),
                       by = c("gender", "ethn", "age"))

# Set up expected number of cases
# First sort data
asthma_zip <- asthma_zip %>% arrange(zip, gender, ethn, age)
asthma_zip_pha <- asthma_zip_pha %>% arrange(zip, gender, ethn, age)
# Calculate the number of strata
strata <- asthma_zip %>% distinct(gender, ethn, age) %>% summarise(n = n()) %>%
  as.numeric()
strata_pha <- asthma_zip_pha %>% distinct(gender, ethn, age) %>% summarise(n = n()) %>%
  as.numeric()
# Calculate the expected number of cases in each ZIP based on pop variables
# (indirect standardization)
expected <- expected(population = asthma_zip$pop,
                     cases = asthma_zip$asthma,
                     n.strata = strata)
expected_pha <- expected(population = asthma_zip_pha$pop,
                     cases = asthma_zip_pha$asthma,
                     n.strata = strata_pha)

# Add expected values for each ZIP
asthma_zip_tot$expected <- expected
asthma_zip_tot_pha$expected <- expected_pha


### Add covariate with proportion in PHA
pha_prop <- asthma_pop %>%
  mutate(pha = ifelse(agency == "Non-PHA", 0, 1)) %>%
  group_by(zip) %>%
  summarise(pha_prop = mean(pha, na.rm = T)) %>%
  ungroup()

asthma_zip_tot <- left_join(asthma_zip_tot, pha_prop, by = "zip")


### Shape file of ZIPs in King County 
# Bring in both one with and without water trim because we want to 
# identify neighboring ZIPs)
kc_zip <- read_sf("//gisdw/kclib/Plibrary2/admin/shapes/polygon/zipcode.shp")
kc_zip_trim <- read_sf("//gisdw/kclib/Plibrary2/admin/shapes/polygon/zipcode_shore.shp")

# Restrict to ZIPs both with data and in shape file
zips <- inner_join(distinct(asthma_zip_tot, zip), kc_zip %>% as.data.frame() %>% distinct(ZIPCODE),
                   by = c("zip" = "ZIPCODE"))
zips_pha <- inner_join(distinct(asthma_zip_tot_pha, zip), kc_zip %>% as.data.frame() %>% distinct(ZIPCODE),
                       by = c("zip" = "ZIPCODE"))

kc_zip_pha <- kc_zip %>% filter(ZIPCODE %in% unlist(zips_pha))
asthma_zip_tot_pha <- asthma_zip_tot_pha %>% filter(zip %in% unlist(zips_pha))

kc_zip <- kc_zip %>% filter(ZIPCODE %in% unlist(zips))
asthma_zip_tot <- asthma_zip_tot %>% filter(zip %in% unlist(zips))


# Restrict to KC ZIPs
kc_zip_trim <- kc_zip_trim %>% filter(COUNTY == "033")

# Convert to spatial object to make neighborhood matrix
kc_zip_sp <- as(kc_zip, "Spatial")
kc_zip_sp_pha <- as(kc_zip_pha, "Spatial")


### Make neighborhood matrix
nb <- poly2nb(kc_zip_sp)
nb2INLA(file.path(working_dir, "zip_neighbors.adj"), nb)

nb_pha <- poly2nb(kc_zip_sp_pha)
nb2INLA(file.path(working_dir, "zip_neighbors_pha.adj"), nb_pha)

graph_nb <- inla.read.graph(filename = file.path(working_dir, "zip_neighbors.adj"))
graph_nb_pha <- inla.read.graph(filename = file.path(working_dir, "zip_neighbors_pha.adj"))


# Add indices for spatial (re_s) and non-spatial (re_f) random effects
asthma_zip_tot$re_s <- 1:nrow(asthma_zip_tot)
asthma_zip_tot$re_f <- 1:nrow(asthma_zip_tot)

asthma_zip_tot_pha$re_s <- 1:nrow(asthma_zip_tot_pha)
asthma_zip_tot_pha$re_f <- 1:nrow(asthma_zip_tot_pha)


#### SET UP SPATIAL MODEL ####
inla_model <- inla(count ~ 1 + pha_prop + 
                     # Spatial random effects
                     f(re_s, model = "besag", graph.file = graph_nb) + 
                     # Non-spatial random effects
                     f(re_f, model = "iid"),
                   family = "poisson", data = asthma_zip_tot,
                   E = expected, 
                   control.predictor = list(compute = TRUE))
                   
summary(inla_model)
plot(inla_model)


inla_model_pha <- inla(count ~ 1  
                     # Spatial random effects
                     + f(re_s, model = "besag", graph.file = graph_nb_pha) 
                     # Non-spatial random effects
                     + f(re_f, model = "iid")
                     ,family = "poisson", data = asthma_zip_tot_pha,
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

### Plot map
ggplot(data = kc_zip_trim_map) + 
  geom_sf(aes(fill = rr)) + 
  geom_label(data = zip_centroids, aes(x = X, y = Y,
                                      label = ifelse(kc_zip_trim_map$significant == 1L, "*", NA)),
             label.padding = unit(0.1, "lines")) +
  # ggrepel::geom_label_repel(data = zip_centroids, 
  #                           aes(x = X, y = Y, 
  #                               label = ifelse(kc_zip_trim_map$significant == 1L, "*", NA)),
  #                           label.padding = unit(0.07, "lines"),
  #                           segment.color = "white") + 
  scale_fill_viridis_c(option = "magma", trans = "sqrt") +
  labs(title = "Risk of having asthma among Medicaid members in King County, by ZIP",
       subtitle = "Adjusted for proportion of Medicaid members who are also PHA members",
       caption = "* = signficicantly different risk",
       x = "Longitude", y = "Latitude", fill = "Relative risk")
  

ggplot(data = kc_zip_trim_pha) + 
  geom_sf(aes(fill = rr)) + 
  scale_fill_viridis_c(option = "magma", trans = "sqrt")
