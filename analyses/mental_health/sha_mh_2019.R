## ---------------------------
##
## Script name: Seattle Housing Authority mental health use
##
## Purpose of script: Classify mental health use in the Medicaid data by race/ethnicity
##
## Author: Alastair Matheson, Public Health - Seattle & King County
## Date Created: 2021-04-19
## Email: alastair.matheson@kingcounty.gov
##
## ---------------------------
##
## Notes: Only covers non-dual Medicaid members in 2019
##   
##
## ---------------------------

#### SET OPTIONS AND BRING IN PACKAGES ####
options(scipen = 6, digits = 4, warning.length = 8170)

if (!require("pacman")) {install.packages("pacman")}
pacman::p_load(tidyverse, odbc, glue, knitr, kableExtra, rmarkdown)


#### CONNECT TO DATABASE AND BRING IN DATA ####
db_apde <- dbConnect(odbc(), "PH_APDEStore51")

# Bring in people in SHA in 2019 (non-dual Medicaid only) and join to their claims
sha_19 <- dbGetQuery(db_apde,
                     "SELECT * FROM PH_APDEStore.stage.mcaid_mcare_pha_elig_calyear
                          WHERE year = 2019 AND pha = 1 AND pha_agency = 'SHA'")


sha_mh_19 <- dbGetQuery(db_apde,
                     "SELECT a.*, b.claim_header_id, b.first_service_date,
                        b.mental_dx_rda_any, b.sud_dx_rda_any, b.ed_pophealth_id
                       FROM
                        (SELECT id_apde, pha, pha_agency, dual, pt, pop, pop_yr, pt_tot 
                          FROM PH_APDEStore.stage.mcaid_mcare_pha_elig_calyear
                          WHERE year = 2019 AND pha = 1 AND pha_agency = 'SHA') a
                        LEFT JOIN
                        (SELECT y.*, z.id_apde
                          FROM
                          (SELECT id_mcaid, claim_header_id, first_service_date, 
                            mental_dx_rda_any, sud_dx_rda_any, ed_pophealth_id
                            FROM PHClaims.final.mcaid_claim_header
                            WHERE year(first_service_date) = 2019) y
                          LEFT JOIN
                          (SELECT id_apde, id_mcaid FROM PHClaims.final.xwalk_apde_mcaid_mcare_pha) z
                          ON y.id_mcaid = z.id_mcaid) b
                     ON a.id_apde = b.id_apde"
)


#### PROCESS DATA ####
### Find out who had 1+ MH or 1+ SUD claims in 2019
mh <- sha_mh_19 %>%
  filter(mental_dx_rda_any >= 1 & dual == 0) %>%
  group_by(id_apde) %>%
  summarise(mh = 1L,
         mh_num = n_distinct(claim_header_id),
         mh_ed = n_distinct(ed_pophealth_id) - 1) %>% # Account for NAs
  ungroup() %>%
  mutate(mh_ed_any = ifelse(mh_ed > 0, 1L, 0L),
         mh_ed_prop = round(mh_ed / mh_num * 100, 1))

sud <- sha_mh_19 %>%
  filter(sud_dx_rda_any >= 1 & dual == 0) %>%
  group_by(id_apde) %>%
  summarise(sud = 1L,
         sud_num = n_distinct(claim_header_id),
         sud_ed = n_distinct(ed_pophealth_id) - 1) %>% # Account for NAs
  ungroup() %>%
  mutate(sud_ed_any = ifelse(sud_ed > 0, 1L, 0L),
         sud_ed_prop = round(sud_ed / sud_num * 100, 1))


### Bring back to main data
sha_19_mh_sud <- sha_19 %>%
  filter(mcaid == 1 & dual == 0 & pop == 1 & age_yr < 65) %>%
  left_join(., mh, by = "id_apde") %>%
  left_join(., sud, by = "id_apde") %>%
  mutate_at(vars(mh, mh_num, mh_ed, mh_ed_any, mh_ed_prop, 
                 sud, sud_num, sud_ed, sud_ed_any, sud_ed_prop),
            list (~ replace_na(., 0)))


### Summarize data
# Any mental health claim by demographics
any_mh <- bind_rows(
  sha_19_mh_sud %>% group_by(race_eth_me) %>% summarise(MH = sum(mh), Total = n()) %>% ungroup() %>%
    mutate(Percent = paste0(round(MH / Total * 100, 1), "%"), Category = "Race/ethnicity") %>% rename(Group = race_eth_me),
  sha_19_mh_sud %>% group_by(gender_me) %>% summarise(MH = sum(mh), Total = n()) %>% ungroup() %>%
    mutate(Percent = paste0(round(MH / Total * 100, 1), "%"), Category = "Gender") %>% rename(Group = gender_me),
  sha_19_mh_sud %>% group_by(agegrp_expanded) %>% summarise(MH = sum(mh), Total = n()) %>% ungroup() %>%
    mutate(Percent = paste0(round(MH / Total * 100, 1), "%"), Category = "Age Group") %>% rename(Group = agegrp_expanded),
  sha_19_mh_sud %>% group_by(time_housing) %>% summarise(MH = sum(mh), Total = n()) %>% ungroup() %>%
    mutate(Percent = paste0(round(MH / Total * 100, 1), "%"), Category = "Time in housing") %>% rename(Group = time_housing),
  sha_19_mh_sud %>% group_by(pha_subsidy) %>% summarise(MH = sum(mh), Total = n()) %>% ungroup() %>%
    mutate(Percent = paste0(round(MH / Total * 100, 1), "%"), Category = "Subsidy type") %>% rename(Group = pha_subsidy)
) %>%
  select(Category, Group:Percent) %>%
  # Suppress small numbers
  mutate(MH = ifelse(MH > 0 & MH < 10, "<10", MH),
         Total = ifelse(Total > 0 & Total < 10, "<10", Total),
         Percent = ifelse(MH == "<10" | Total == "<10", NA, Percent))


# Any mental health-related ED claim by demographics
any_mh_ed <- bind_rows(
  sha_19_mh_sud %>% group_by(race_eth_me) %>% summarise(MH_ED = sum(mh_ed_any), Total = n()) %>% ungroup() %>%
    mutate(Percent = paste0(round(MH_ED / Total * 100, 1), "%"), Category = "Race/ethnicity") %>% rename(Group = race_eth_me),
  sha_19_mh_sud %>% group_by(gender_me) %>% summarise(MH_ED = sum(mh_ed_any), Total = n()) %>% ungroup() %>%
    mutate(Percent = paste0(round(MH_ED / Total * 100, 1), "%"), Category = "Gender") %>% rename(Group = gender_me),
  sha_19_mh_sud %>% group_by(agegrp_expanded) %>% summarise(MH_ED = sum(mh_ed_any), Total = n()) %>% ungroup() %>%
    mutate(Percent = paste0(round(MH_ED / Total * 100, 1), "%"), Category = "Age Group") %>% rename(Group = agegrp_expanded),
  sha_19_mh_sud %>% group_by(time_housing) %>% summarise(MH_ED = sum(mh_ed_any), Total = n()) %>% ungroup() %>%
    mutate(Percent = paste0(round(MH_ED / Total * 100, 1), "%"), Category = "Time in housing") %>% rename(Group = time_housing),
  sha_19_mh_sud %>% group_by(pha_subsidy) %>% summarise(MH_ED = sum(mh_ed_any), Total = n()) %>% ungroup() %>%
    mutate(Percent = paste0(round(MH_ED / Total * 100, 1), "%"), Category = "Subsidy type") %>% rename(Group = pha_subsidy)
) %>%
  select(Category, Group:Percent) %>%
  # Suppress small numbers
  mutate(MH_ED = ifelse(MH_ED > 0 & MH_ED < 10, "<10", MH_ED),
         Total = ifelse(Total > 0 & Total < 10, "<10", Total),
         Percent = ifelse(MH_ED == "<10" | Total == "<10", NA, Percent))

# Any substance use claim by demographics
any_sud <- bind_rows(
  sha_19_mh_sud %>% group_by(race_eth_me) %>% summarise(SUD = sum(sud), Total = n()) %>% ungroup() %>%
    mutate(Percent = paste0(round(SUD / Total * 100, 1), "%"), Category = "Race/ethnicity") %>% rename(Group = race_eth_me),
  sha_19_mh_sud %>% group_by(gender_me) %>% summarise(SUD = sum(sud), Total = n()) %>% ungroup() %>%
    mutate(Percent = paste0(round(SUD / Total * 100, 1), "%"), Category = "Gender") %>% rename(Group = gender_me),
  sha_19_mh_sud %>% group_by(agegrp_expanded) %>% summarise(SUD = sum(sud), Total = n()) %>% ungroup() %>%
    mutate(Percent = paste0(round(SUD / Total * 100, 1), "%"), Category = "Age Group") %>% rename(Group = agegrp_expanded),
  sha_19_mh_sud %>% group_by(time_housing) %>% summarise(SUD = sum(sud), Total = n()) %>% ungroup() %>%
    mutate(Percent = paste0(round(SUD / Total * 100, 1), "%"), Category = "Time in housing") %>% rename(Group = time_housing),
  sha_19_mh_sud %>% group_by(pha_subsidy) %>% summarise(SUD = sum(sud), Total = n()) %>% ungroup() %>%
    mutate(Percent = paste0(round(SUD / Total * 100, 1), "%"), Category = "Subsidy type") %>% rename(Group = pha_subsidy)
) %>%
  select(Category, Group:Percent) %>%
  # Suppress small numbers
  mutate(SUD = ifelse(SUD > 0 & SUD < 10, "<10", SUD),
         Total = ifelse(Total > 0 & Total < 10, "<10", Total),
         Percent = ifelse(SUD == "<10" | Total == "<10", NA, Percent))


# Any substance use-related ED claim by demographics
any_sud_ed <- bind_rows(
  sha_19_mh_sud %>% group_by(race_eth_me) %>% summarise(SUD_ED = sum(sud_ed_any), Total = n()) %>% ungroup() %>%
    mutate(Percent = paste0(round(SUD_ED / Total * 100, 1), "%"), Category = "Race/ethnicity") %>% rename(Group = race_eth_me),
  sha_19_mh_sud %>% group_by(gender_me) %>% summarise(SUD_ED = sum(sud_ed_any), Total = n()) %>% ungroup() %>%
    mutate(Percent = paste0(round(SUD_ED / Total * 100, 1), "%"), Category = "Gender") %>% rename(Group = gender_me),
  sha_19_mh_sud %>% group_by(agegrp_expanded) %>% summarise(SUD_ED = sum(sud_ed_any), Total = n()) %>% ungroup() %>%
    mutate(Percent = paste0(round(SUD_ED / Total * 100, 1), "%"), Category = "Age Group") %>% rename(Group = agegrp_expanded),
  sha_19_mh_sud %>% group_by(time_housing) %>% summarise(SUD_ED = sum(sud_ed_any), Total = n()) %>% ungroup() %>%
    mutate(Percent = paste0(round(SUD_ED / Total * 100, 1), "%"), Category = "Time in housing") %>% rename(Group = time_housing),
  sha_19_mh_sud %>% group_by(pha_subsidy) %>% summarise(SUD_ED = sum(sud_ed_any), Total = n()) %>% ungroup() %>%
    mutate(Percent = paste0(round(SUD_ED / Total * 100, 1), "%"), Category = "Subsidy type") %>% rename(Group = pha_subsidy)
) %>%
  select(Category, Group:Percent) %>%
  # Suppress small numbers
  mutate(SUD_ED = ifelse(SUD_ED > 0 & SUD_ED < 10, "<10", SUD_ED),
         Total = ifelse(Total > 0 & Total < 10, "<10", Total),
         Percent = ifelse(SUD_ED == "<10" | Total == "<10", NA, Percent))

#### RUN MARKDOWN AND PRODUCE OUTPUT -------------------------------------------
render(file.path(here::here(), "analyses/mental_health/sha_mh_2019.Rmd"),
       "html_document")
