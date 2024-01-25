# Header ----
# Author: Danny Colombara
# Date: September 15, 2023
# R version: 4.3.1
# Purpose: to calculate the leading causes of death and age standardized death rates by year
# Notes: Ensured consistency with PHA<>decedent ID pairings from main .Rmd script
#        Must be run after `pha-matching.R`
#

# Set up connections ----
  # Connect to HHSAW
    db_hhsaw <- DBI::dbConnect(odbc::odbc(),
                               driver = "ODBC Driver 17 for SQL Server",
                               server = "tcp:kcitazrhpasqlprp16.azds.kingcounty.gov,1433",
                               database = "hhs_analytics_workspace",
                               uid = keyring::key_list("hhsaw")[["username"]],
                               pwd = keyring::key_get("hhsaw", keyring::key_list("hhsaw")[["username"]]),
                               Encrypt = "yes",
                               TrustServerCertificate = "yes",
                               Authentication = "ActiveDirectoryPassword")
    
# Get data ---- 
  # Get calyear data for population denominator
    cod.calyear <- setDT(
      odbc::dbGetQuery(conn = db_hhsaw, 
                       statement = paste0("SELECT DISTINCT year, agency, prog_type, chi_age = age_yr, 
                                           chi_sex = gender_recent, days = pt_total 
                                           FROM pha.final_calyear
                                           WHERE year IN (2017, 2018, 2019, 2020, 2021)")))
    
  # Get KCMASTER_ID <> state_file_number xwalk
    ids.sfn <- setDT(copy(match))[, .(KCMASTER_ID, state_file_number)]

  # Get observations by month (same concept as what is under `PHA monthly counts` in .Rmd, except way faster)
    by_month <- final_timevar[, .(start = min(from_date), end = max(to_date)), by = .(KCMASTER_ID, period_start, agency, prog_type)][
      , `:=`(start_v2 = ifelse(day(start) <= 15, floor_date(start, "month"), ceiling_date(start, "month")) %>% as.Date(.,origin="1970-01-01"),
             end_v2 = ifelse(day(end) < 15, floor_date(end, "month") %m-% months(1), floor_date(end, "month")) %>% as.Date(.,origin="1970-01-01"))
    ][end_v2 >= start_v2]
    by_month[, month_year := .(list(format(seq(start_v2, end_v2, by = "month"), "%Y-%m"))), by = .(KCMASTER_ID, period_start, agency, prog_type)]
    setorder(by_month, KCMASTER_ID, period_start, agency, prog_type)
    
  # Ascribe each KCMASTER_ID to a single agency/prog combo (follow code in .Rmd to make sure have same KCMASTER_ID <> agency pairings)
    decedent_pha_prog <- by_month %>% 
      ungroup() %>%
      select(KCMASTER_ID,agency,prog_type, end) %>%
      # keep most recent date
      group_by(KCMASTER_ID) %>% 
      slice(which.max(as.Date(end, '%m/%d/%Y'))) %>%
      select(-end) %>% setDT()
    
  # Get subset of important death data
    cod <- kc_deaths0[, .(state_file_number, chi_sex, chi_age, age6, date_of_death, date_of_death2 = date_of_death, underlying_cod_code)]


# Merge data ----
  # merge KCMASTER_ID onto COD data
    cod <- merge(ids.sfn,
                 cod,
                 by = 'state_file_number', 
                 all.x = T, all.y = F)
    
  # merge on the PHA data
    cod <- merge(cod, 
                 decedent_pha_prog, 
                 by = 'KCMASTER_ID', 
                 all.x = T, all.y = F)

# Get the CDC 130 top COD ----
  # calculate
    cod[, year := year(date_of_death)]
    cod <- death_113_count(ph.data = cod, 
                          kingco = F, # false because the dataset is already a subset of KC and not KC identifier in table
                          group_by = c('year', 'agency', 'prog_type', 'chi_sex', 'chi_age'))[, causeid := NULL]
    
  # Ensure that every age group is present
    cod.template = setDT(tidyr::crossing(data.table(chi_age = 0:100), 
                                         unique(cod[, .(cause.of.death, year, agency, prog_type, chi_sex)])))
    cod <- merge(cod.template, 
                 cod, 
                 by = c('year', 'cause.of.death', 'agency', 'prog_type', 'chi_sex', 'chi_age'), 
                 all = T)
    cod[is.na(deaths), deaths := 0] # rows added through merger with template will have deaths = NA
    
# Merge on aggregated COD definitions ----
    extra.cod <- rads.data::icd_nchs113causes_raw[, .(cause.of.death, cause.group = leading.cause.group.alias, level1, level2)]
    cod <- merge(cod, 
                 extra.cod, 
                 by = 'cause.of.death', 
                 all.x = T, all.y = F)
    cod[is.na(cause.group), cause.group := cause.of.death] # to provid All causes, COVID, and Missing/Unknown

# Get the corresponding population from calyear ----
  # tidy calyear to align categoricals with cod
    cod.calyear <- cod.calyear[chi_sex != 'Unknown']
    cod.calyear[chi_age > 100, chi_age := 100]
    cod.calyear <- cod.calyear[!is.na(chi_age)]
    
  # sum person-time to get population at risk
    pop <- cod.calyear[, .(pop = sum(days)/365.25), .(year, agency, prog_type, chi_sex, chi_age) ] # 1 person over a calendar year contributes 365.25 days
  
# Merge population onto 130 COD ----
    cod <- merge(cod, 
                 pop, 
                 by = intersect(names(pop), names(cod)), 
                 all.x = T, all.y = T)
    cod[is.na(pop), pop := 0]

# Calc age standardized rates ----
  # need to change standardize name of age variable
    setnames(cod, 'chi_age', 'age')
    
  
  # create list of all combinations of groupings of interest
    mygroups <- list(c('year', 'cause.of.death'), 
                     c('year', 'cause.of.death', 'agency'), 
                     c('year', 'cause.of.death', 'prog_type'), 
                     c('year', 'cause.of.death', 'chi_sex'), 
                     c('year', 'cause.of.death', 'agency', 'prog_type'),
                     c('year', 'cause.of.death', 'agency', 'chi_sex'),
                     c('year', 'cause.of.death', 'agency', 'prog_type', 'chi_sex'), 
                     c('year', 'cause.group'), 
                     c('year', 'cause.group', 'agency'), 
                     c('year', 'cause.group', 'prog_type'), 
                     c('year', 'cause.group', 'chi_sex'), 
                     c('year', 'cause.group', 'agency', 'prog_type'),
                     c('year', 'cause.group', 'agency', 'chi_sex'),
                     c('year', 'cause.group', 'agency', 'prog_type', 'chi_sex'))
    
  # calculate age standardized death rate for grouping in the list above
    myrates = rbindlist(lapply(X = 1:length(mygroups), 
                               FUN = function(X){suppressWarnings(
                                 tempy <- age_standardize(ph.data = cod, 
                                                          ref.popname = "2000 U.S. Std Population (11 age groups)", 
                                                          collapse = T, # because I am passing single ages
                                                          my.count = 'deaths', 
                                                          my.pop = 'pop', 
                                                          per = 10000, # typically 100,000, but following 10K due to standards in main .Rmd report
                                                          conf.level = 0.95, 
                                                          group_by = mygroups[[X]]))
                                 tempy[, group := X]
                                 if('chi_sex' %in% mygroups[[X]]){tempy <- tempy[!is.na(chi_sex)]}else{tempy}
                                 }), fill = T)
    
    # tidy
    myrates <- myrates[!(is.na(cause.group) & is.na(cause.of.death))]
    myrates <- myrates[is.na(cause.group) | cause.group != '999'] # useless codes
    myrates <- myrates[, .(group, cause.group, cause.of.death, year, agency, prog_type, chi_sex, deaths = count, pop, crude.rate, crude.lci, crude.uci, adj.rate, adj.lci, adj.uci)]
    setorder(myrates, group, -year, -deaths)
    
  # Sanity check
    message("Total of 'All causes' in each group should be the same as the number of unique KCMASTER_ID <> state_file_number pairs: ", nrow(ids.sfn), ".\nMinor exceptions might be due to missing sex identifiers.")
    myrates[cause.of.death == 'All causes', .(total_deaths = sum(deaths)), group]

# Identify the top 10 causes of death by year ----
  # Follow CHI guidelines ... top 10 causes are ranked by COUNT (not rates), with All Cause being ranked 0.
  # Rather than use ties.method == 'dense' to keep all ties <= 10, will use 'first' so that will have a max of 10 COD per demographic (aside from All causes)
    # First time is for 113 COD
      top10_v1 <- copy(myrates[!is.na(cause.of.death)])
      top10_v1[, ranking := frank(-deaths, ties.method = 'first') - 1, .(year, agency, prog_type, chi_sex)]
      top10_v1 <- top10_v1[ranking <= 10]
      setorder(top10_v1, group, year, agency, prog_type, chi_sex, ranking, na.last = F)
      top10_v1 <- top10_v1[, .(group, year, agency, prog_type, chi_sex, ranking, Cause.Type = 'CDC 113 COD', cause = cause.of.death, deaths, pop, crude.rate, crude.lci, crude.uci, adj.rate, adj.lci, adj.uci)]
      top10_v1 <- top10_v1[deaths != 0] # meaningless if there are no deaths
      print(top10_v1[group == 1 & ranking == 0])

    # Second time is for aggregated COD
      top10_v2 <- copy(myrates[!is.na(cause.group)])
      top10_v2[, ranking := frank(-deaths, ties.method = 'first') - 1, .(year, agency, prog_type, chi_sex)]
      top10_v2 <- top10_v2[ranking <= 10]
      setorder(top10_v2, group, year, agency, prog_type, chi_sex, ranking, na.last = F)
      top10_v2 <- top10_v2[, .(group, year, agency, prog_type, chi_sex, ranking, Cause.Type = 'Cause Groups', cause = cause.group, deaths, pop, crude.rate, crude.lci, crude.uci, adj.rate, adj.lci, adj.uci)]
      top10_v2 <- top10_v2[deaths != 0] # meaningless if there are no deaths
      print(top10_v2[group == 8 & ranking == 0])
      
    # Combine
      top10 <- rbind(top10_v1, top10_v2)
      top10[, strata := paste(agency, prog_type, chi_sex, sep = '_')]
      top10[, Groupings := factor(group, 
                                  levels = 1:14, 
                                  labels = c("Overall", "PHA", "Prog", "Sex", "PHA + Prog", "PHA + Sex", "PHA + Prog + Sex", 
                                             "Overall", "PHA", "Prog", "Sex", "PHA + Prog", "PHA + Sex", "PHA + Prog + Sex"))]
      
# Save as CSV ----
    write.csv(top10, "C:/temp/top10.csv")
    
# The end!----