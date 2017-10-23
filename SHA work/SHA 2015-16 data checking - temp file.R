# run numbers similar to SHA MTW reports



h_portfolio <- counts(pha_longitudinal, group_var = c("agency_new", "major_prog", "portfolio_final"), period = "date", 
                          yearmin = 2015, yearmax = 2016, agency = "sha", unit = hhold_id_new, filter = quo(port_out_sha == 0 & major_prog == "PH")) %>%
  mutate(category = "Households by portfolio", group = portfolio_final)
h_prop <- counts(pha_longitudinal, group_var = c("agency_new", "major_prog", "prog_final"), period = "date", 
                       yearmin = 2015, yearmax = 2016, agency = "sha", unit = hhold_id_new, filter = quo(port_out_sha == 0 & major_prog == "HCV")) %>%
  mutate(category = "Households by HCV program", group = prog_final)
h_race <- counts(pha_longitudinal, group_var = c("agency_new", "major_prog", "race2"), period = "date", 
                  yearmin = 2015, yearmax = 2016, agency = "sha", unit = hhold_id_new, filter = quo(port_out_sha == 0)) %>%
  mutate(category = "Households by race", group = race2)

i_age <- counts(pha_longitudinal, group_var = c("agency_new", "major_prog", "agegrp"), period = "date", 
                  yearmin = 2015, yearmax = 2016, agency = "sha", unit = pid, filter = quo(port_out_sha == 0)) %>%
  mutate(category = "Age groups by major program", group = major_prog)
i_age_portfolio <- counts(pha_longitudinal, group_var = c("agency_new", "major_prog", "portfolio_final", "agegrp"), period = "date", 
                  yearmin = 2015, yearmax = 2016, agency = "sha", unit = pid, filter = quo(port_out_sha == 0 & major_prog == "PH")) %>%
  mutate(category = "Age groups by portfolio", group = portfolio_final)
i_age_prop <- counts(pha_longitudinal, group_var = c("agency_new", "major_prog", "prog_final", "agegrp"), period = "date", 
                        yearmin = 2015, yearmax = 2016, agency = "sha", unit = pid, filter = quo(port_out_sha == 0 & major_prog == "HCV")) %>%
  mutate(category = "Age groups by HCV program", group = prog_final)

i_disab_portfolio <- counts(pha_longitudinal, group_var = c("agency_new", "major_prog", "portfolio_final", "disability"), period = "date", 
                          yearmin = 2015, yearmax = 2016, agency = "sha", unit = pid, filter = quo(port_out_sha == 0 & major_prog == "PH")) %>%
  mutate(category = "Disability by portfolio", group = portfolio_final)
i_disab_prop <- counts(pha_longitudinal, group_var = c("agency_new", "major_prog", "prog_final", "disability"), period = "date", 
                     yearmin = 2015, yearmax = 2016, agency = "sha", unit = pid, filter = quo(port_out_sha == 0 & major_prog == "HCV")) %>%
  mutate(category = "Disability by HCV program", group = prog_final)



sha_count <- bind_rows(h_portfolio, h_prop, h_race, i_age, i_age_portfolio, i_age_prop, i_disab_portfolio, i_disab_prop) %>%
  mutate(unit = car::recode(unit, "'hhold_id_new' = 'Households'; 'pid' = 'Individuals'")) %>%
  select(agency_new, unit, category, major_prog, group, agegrp, disability, count:period) %>%
  arrange(date, unit, category, major_prog, group, agegrp, disability)

write.xlsx(sha_count, file = paste0("//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/Summaries/SHA enrollment count - non-matched_", 
                                             Sys.Date(), ".xlsx"))

rm(list = ls(pattern = "^h_"))
rm(list = ls(pattern = "^i_"))