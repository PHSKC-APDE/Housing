# Temp file to look at PHA/Medicaid enrollment
# Uses numbers to figure out who was enrolled where

## Code
# 1 = Medicaid only
# 2 = SHA only
# 4 = KCHA only
# 8 = SHA and Medicaid together
# 17 = KCHA and Medicaid together

### Permutations
# 1 = Medicaid only
# 2 = SHA only
# 3 = Medicaid only and SHA only
# 4 = KCHA only
# 5 = Medicaid only and KCHA only
# 6 = SHA only and KCHA only
# 7 = Medicaid only, SHA only, and KCHA only
# 8 = SHA and Medicaid together
# 9 = Medicaid only and SHA and Medicaid together
# 10 = SHA only and SHA and Medicaid together
# 11 = Medicaid only and SHA only and SHA and Medicaid together
# 12 = KCHA only and SHA and Medicaid together
# 13 = Medicaid only and KCHA only and SHA and Medicaid together
# 14 = SHA only and KCHA only and SHA and Medicaid together
# 15 = Medicaid only and SHA only and KCHA only and SHA and Medicaid together
# 16 = NOT POSSIBLE
# 17 = KCHA and Medicaid together
# 18 = Medicaid only and KCHA and Medicaid together
# 19 = SHA only and KCHA and Medicaid together
# 20 = Medicaid only and SHA only and KCHA and Medicaid together
# 21 = KCHA only and KCHA and Medicaid together
# 22 = Medicaid only and KCHA only and KCHA and Medicaid together
# 23 = SHA only and KCHA only and KCHA and Medicaid together
# 24 = Medicaid only and SHA only and KCHA only and KCHA and Medicaid together
# 25 = SHA and Medicaid together and KCHA and Medicaid together
# 26 = Medicaid only and SHA and Medicaid together and KCHA and Medicaid together
# 27 = SHA only and SHA and Medicaid together and KCHA and Medicaid together
# 28 = Medicaid only and SHA only and SHA and Medicaid together and KCHA and Medicaid together
# 29 = KCHA only and SHA and Medicaid together and KCHA and Medicaid together
# 30 = Medicaid only and SHA and Medicaid together and KCHA and Medicaid together
# 31 = SHA only and KCHA only and SHA and Medicaid together and KCHA and Medicaid together
# 32 = Medicaid only and SHA only and KCHA only and SHA and Medicaid together and KCHA and Medicaid together

### Groups
# Medicaid only, never in housing (1)
# SHA only, not with Medicaid at the same time (2, 3, 6, 7, 19, 20, 23, 24)
# SHA and Medicaid any time (8:15, 25:32)
# KCHA only, not with Medicaid at the same time (4:7, 12:15)
# KCHA and Medicaid any time (17:32)
# SHA and Medicaid AND KCHA and Medicaid (25:32)


temp <- pha_elig_sql %>%
  select(pid2, startdate_c, enddate_c, agency_new, enroll_type, age12:age16, gender_c, race_c, pt12:pt16) %>%
  mutate(
    enrollment = case_when(
      enroll_type == "m" ~ 1,
      enroll_type == "h" & agency_new == "SHA" ~ 2,
      enroll_type == "h" & agency_new == "KCHA" ~ 4,
      enroll_type == "b" & agency_new == "SHA" ~ 8,
      enroll_type == "b" & agency_new == "KCHA" ~ 17
      ),
    gender_c = case_when(
      gender_c == 1 ~ "Female",
      gender_c == 2 ~ "Male",
      is.na(gender_c) ~ "Unknown"
      )
    ) %>%
  mutate_at(
    vars(age12, age13, age14, age15, age16),
    funs(case_when(
      . < 18 ~ "<18",
      between(., 18, 24.99) ~ "18–24",
      between(., 25, 44.99) ~ "25–44",
      between(., 45, 61.99) ~ "45–61",
      between(., 62, 64.99) ~ "62–64",
      . >= 65 ~ "65+",
      is.na(.) ~ "Unknown"
      )
      )
    )


enrollment_f <- function(df, demog, year) {
  pt <- rlang::sym(paste0("pt", quo_name(year)))
  agex <- rlang::sym(paste0("age", quo_name(year)))
  
  if (demog != "total") {
    
    if (demog == "gender") {
      demog <- quo(gender_c)
      cat <- "Gender"
    } else if (demog == "race") {
      demog <- quo(race_c)
      cat <- "Race/ethnicity"
    } else if (demog == "age") {
      demog <- rlang::sym(paste0("age", quo_name(year)))
      cat <- "Age"
    }

    output <- df %>%
      filter(!is.na((!!pt))) %>%
      distinct(pid2, !!demog, enrollment) %>%
      group_by(pid2, !!demog) %>%
      summarise(enrollment = sum(enrollment)) %>%
      ungroup() %>%
      mutate(
        year = paste0(20, year),
        category = cat,
        group = !!demog,
        group_sha = case_when(
          #enrollment == 1 ~ "Only enrolled in Medicaid",
          enrollment %in% c(2, 3, 6, 7, 19, 20, 23, 24) ~ "PHA resident but not simultanesouly enrolled in Medicaid",
          enrollment %in% c(8:15, 25:32) ~ "PHA resident and simultanesouly enrolled in Medicaid at some point"
        ),
        group_kcha = case_when(
          #enrollment == 1 ~ "Only enrolled in Medicaid",
          enrollment %in% c(4:7, 12:15) ~ "PHA resident but not simultanesouly enrolled in Medicaid",
          enrollment %in% c(17:32) ~ "PHA resident and simultanesouly enrolled in Medicaid at some point"
        )
      ) %>%
      distinct(year, category, group, pid2, group_sha, group_kcha)
    
    return(output)
    
  } else if (demog == "total") {
    output <- df %>%
      filter(!is.na((!!pt))) %>%
      distinct(pid2, enrollment) %>%
      group_by(pid2) %>%
      summarise(enrollment = sum(enrollment)) %>%
      ungroup() %>%
      mutate(
        year = paste0(20, year),
        category = "Total",
        group = "Total",
        group_sha = case_when(
          #enrollment == 1 ~ "Only enrolled in Medicaid",
          enrollment %in% c(2, 3, 6, 7, 19, 20, 23, 24) ~ "PHA resident but not simultanesouly enrolled in Medicaid",
          enrollment %in% c(8:15, 25:32) ~ "PHA resident and simultanesouly enrolled in Medicaid at some point"
        ),
        group_kcha = case_when(
          #enrollment == 1 ~ "Only enrolled in Medicaid",
          enrollment %in% c(4:7, 12:15) ~ "PHA resident but not simultanesouly enrolled in Medicaid",
          enrollment %in% c(17:32) ~ "PHA resident and simultanesouly enrolled in Medicaid at some point"
        )
      ) %>%
      distinct(year, category, group, pid2, group_sha, group_kcha)
    return(output)
    
  }
}

### Overall enrollment
enroll_tot <- lapply(seq(12, 16), enrollment_f, df = temp, demog = "total")
enroll_tot <- as.data.frame(data.table::rbindlist(enroll_tot))

### Enrollment by gender
enroll_gender <- lapply(seq(12, 16), enrollment_f, df = temp, demog = "gender")
enroll_gender <- as.data.frame(data.table::rbindlist(enroll_gender))

### Enrollment by age
enroll_age <- lapply(seq(12, 16), enrollment_f, df = temp, demog = "age")
enroll_age <- as.data.frame(data.table::rbindlist(enroll_age))

### Enrollment by race/ethnicity
enroll_race <- lapply(seq(12, 16), enrollment_f, df = temp, demog = "race")
enroll_race <- as.data.frame(data.table::rbindlist(enroll_race))


### Combine into a single df
enroll_overall <- bind_rows(enroll_tot, enroll_gender, enroll_age, enroll_race) %>%
  filter(!(is.na(group_sha) & is.na(group_kcha)))


### Specific views
enroll_tot %>% group_by(year, group_sha) %>% summarise(count = n_distinct(pid2)) %>% 
  filter(!is.na(group_sha)) %>% group_by(year) %>%
  mutate(total = sum(count), percent = round(count / total * 100, 0))

enroll_tot %>% group_by(year, group_kcha) %>% summarise(count = n_distinct(pid2)) %>% 
  filter(!is.na(group_kcha)) %>% group_by(year) %>%
  mutate(total = sum(count), percent = round(count / total * 100, 0))

enroll_gender %>% group_by(year, category, group, group_sha) %>% summarise(count = n_distinct(pid2)) %>% 
  filter(!is.na(group_sha)) %>% group_by(year, group) %>%
  mutate(total = sum(count), percent = round(count / total * 100, 0)) %>%
  arrange(year, group, group_sha)

enroll_gender %>% group_by(year, category, group, group_kcha) %>% summarise(count = n_distinct(pid2)) %>% 
  filter(!is.na(group_kcha)) %>% group_by(year, group) %>%
  mutate(total = sum(count), percent = round(count / total * 100, 0)) %>%
  arrange(year, group, group_kcha)

enroll_age %>% group_by(year, category, group, group_sha) %>% summarise(count = n_distinct(pid2)) %>% 
  filter(!is.na(group_sha)) %>% group_by(year, group) %>%
  mutate(total = sum(count), percent = round(count / total * 100, 0)) %>%
  arrange(year, group, group_sha)

enroll_gender %>% group_by(year, category, group, group_kcha) %>% summarise(count = n_distinct(pid2)) %>% 
  filter(!is.na(group_kcha)) %>% group_by(year, group) %>%
  mutate(total = sum(count), percent = round(count / total * 100, 0)) %>%
  arrange(year, group, group_kcha)


# All combined
enroll_sha <- enroll_overall %>% group_by(year, category, group, group_sha) %>% summarise(count = n_distinct(pid2)) %>% 
  filter(!is.na(group_sha)) %>% group_by(year, group) %>%
  mutate(total = sum(count), percent = round(count / total * 100, 0),
         agency = "SHA") %>%
  arrange(year, category, group, group_sha)

enroll_kcha <- enroll_overall %>% group_by(year, category, group, group_kcha) %>% summarise(count = n_distinct(pid2)) %>% 
  filter(!is.na(group_kcha)) %>% group_by(year, group) %>%
  mutate(total = sum(count), percent = round(count / total * 100, 0),
         agency = "KCHA") %>%
  arrange(year, category, group, group_kcha)

enroll_combined <- bind_rows(enroll_sha, enroll_kcha)

enroll_combined %>% filter(year == 2016)
