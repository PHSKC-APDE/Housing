

library(pastecs) # Used for summary statistics

# Header, libraries etc

#pha_elig_final <- readRDS(file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/pha_elig_final.Rda")

##### Look at demographics of overlapping PHA and Medicaid #####



# Quick count of people enrolled at any point in 2015
temp <- pha_elig_final %>% 
  filter(pt15_h > 0) %>% 
  group_by(ssn_new_pha, lname_new.1, fname_new.1, dob_h) %>% 
  summarise(pt15_h = sum(pt15_h, na.rm = T), pt15_o = sum(pt15_o, na.rm = T)) %>% 
  ungroup() 
temp %>% 
  mutate(
    pt15_h_ever = ifelse(is.na(pt15_h), 0, ifelse(pt15_h > 0, 1, 0)), 
    pt15_o_ever = ifelse(is.na(pt15_o), 0, ifelse(pt15_o > 0, 1, 0))) %>% 
  summarise(pha = sum(pt15_h_ever, na.rm = T), overlap = sum(pt15_o_ever, na.rm = T))


### Set up a particular year (2015)
# Enrolled in housing as of December 31, 2015
pha_elig_demogs <- pha_elig_final %>%
  mutate(
    h2015 = ifelse(startdate_h <= as.Date("2015-12-31", origin = "1970-01-01") & 
                     enddate_h >= as.Date("2015-12-31", origin = "1970-01-01"),
                   1, 0),
    m2015 = ifelse(startdate_m <= as.Date("2015-12-31", origin = "1970-01-01") & 
                     enddate_m >= as.Date("2015-12-31", origin = "1970-01-01"),
                   1, 0),
    o2015 = ifelse(startdate_o <= as.Date("2015-12-31", origin = "1970-01-01") & 
                     enddate_o >= as.Date("2015-12-31", origin = "1970-01-01"),
                   1, 0)
  )

# Enrolled in Medicaid as of December 31, 2015
elig <- elig %>%
  mutate(
    m2015 = ifelse(startdate <= as.Date("2015-12-31", origin = "1970-01-01") & 
                     enddate >= as.Date("2015-12-31", origin = "1970-01-01"),
                   1, 0)
  )


# Age at each date
pha_elig_demogs <- pha_elig_demogs %>%
  mutate(age15 = round(interval(start = dob.2, end = ymd(20151231)) / years(1), 1)
  )



### Count per year
# Housing
pha_elig_demogs %>% filter(h2015 == 1) %>% distinct(ssn_new_pha, lname_new.1, fname_new.1, dob_h) %>% summarise(count = n())
# Mediciad
elig %>% filter(m2015 == 1) %>% distinct(MEDICAID_RECIPIENT_ID, SOCIAL_SECURITY_NMBR) %>% summarise(count = n())
pha_elig_demogs %>% filter(m2015 == 1) %>% distinct(ssn_new_pha, lname_new.1, fname_new.1, dob_h) %>% summarise(count = n())
# Overlapping
pha_elig_demogs %>% filter(o2015 == 1) %>% distinct(ssn_new_pha, lname_new.1, fname_new.1, dob_h) %>% summarise(count = n())


### Output to Excel for use in Tableau
temp <- pha_elig_demogs %>%
  filter(h2015 == 1) %>%
  distinct(ssn_new_pha, lname_new.1, fname_new.1, dob_h, .keep_all = TRUE)

write.xlsx(temp, file = "//phdata01/DROF_DATA/DOH DATA/Housing/OrganizedData/pha_elig_demogs.xlsx")



### Gender per year
pha_elig_demogs %>%
  filter(h2015 == 1) %>%
  distinct(ssn_new_pha, lname_new.1, fname_new.1, dob_h, .keep_all = TRUE) %>%
  group_by(agency_new, prog_type_new, o2015) %>%
  summarise(female = sum(gender_new_m6[gender_new_m6 == 1], na.rm = TRUE),
            male = sum(gender_new_m6[gender_new_m6 == 2] - 1, na.rm = TRUE),
            unknown = sum(is.na(gender_new_m6)),
            total = n()) %>%
  mutate(femaleper = female / total, maleper = male / total, unkper = unknown / total)


temp <- pha_elig_demogs %>%
  filter(h2015 == 1) %>%
  distinct(ssn_new_pha, lname_new.1, fname_new.1, dob_h, .keep_all = TRUE)
stat.desc(temp$age15[temp$agency_new == "KCHA" & temp$prog_type_new == "PBS8" & temp$o2015 == 0], basic = F)
stat.desc(temp$age15[temp$agency_new == "KCHA" & temp$prog_type_new == "PBS8" & temp$o2015 == 1], basic = F)
stat.desc(temp$age15[temp$agency_new == "KCHA" & temp$prog_type_new == "PH" & temp$o2015 == 0], basic = F)
stat.desc(temp$age15[temp$agency_new == "KCHA" & temp$prog_type_new == "PH" & temp$o2015 == 1], basic = F)
stat.desc(temp$age15[temp$agency_new == "KCHA" & temp$prog_type_new == "PORT" & temp$o2015 == 0], basic = F)
stat.desc(temp$age15[temp$agency_new == "KCHA" & temp$prog_type_new == "PORT" & temp$o2015 == 1], basic = F)
stat.desc(temp$age15[temp$agency_new == "KCHA" & temp$prog_type_new == "TBS8" & temp$o2015 == 0], basic = F)
stat.desc(temp$age15[temp$agency_new == "KCHA" & temp$prog_type_new == "TBS8" & temp$o2015 == 1], basic = F)

stat.desc(temp$age15[temp$agency_new == "SHA" & temp$prog_type_new == "HCV" & temp$o2015 == 0], basic = F)
stat.desc(temp$age15[temp$agency_new == "SHA" & temp$prog_type_new == "HCV" & temp$o2015 == 1], basic = F)
stat.desc(temp$age15[temp$agency_new == "SHA" & temp$prog_type_new == "PBS8" & temp$o2015 == 0], basic = F)
stat.desc(temp$age15[temp$agency_new == "SHA" & temp$prog_type_new == "PBS8" & temp$o2015 == 1], basic = F)
stat.desc(temp$age15[temp$agency_new == "SHA" & temp$prog_type_new == "PH" & temp$o2015 == 0], basic = F)
stat.desc(temp$age15[temp$agency_new == "SHA" & temp$prog_type_new == "PH" & temp$o2015 == 1], basic = F)
stat.desc(temp$age15[temp$agency_new == "SHA" & temp$prog_type_new == "PORT" & temp$o2015 == 0], basic = F)
stat.desc(temp$age15[temp$agency_new == "SHA" & temp$prog_type_new == "PORT" & temp$o2015 == 1], basic = F)
stat.desc(temp$age15[temp$agency_new == "SHA" & temp$prog_type_new == "TBS8" & temp$o2015 == 0], basic = F)
stat.desc(temp$age15[temp$agency_new == "SHA" & temp$prog_type_new == "TBS8" & temp$o2015 == 1], basic = F)





