#### CODE TO CREATE CONSOLIDATED TIME-VARYING TABLE
# Alastair Matheson, PHSKC (APDE)
#
# 2021-06

### Run from main_pha_load script
# https://github.com/PHSKC-APDE/Housing/blob/master/claims_db/etl/db_loader/main_pha_load.R
# Assumes relevant libraries are already loaded

# For now, sticking to the bare minimum needed to make the DASHH dashboard
# Eventually add in income and other elements


# conn = ODBC connection to use
# to_schema = name of the schema to load data to
# to_table = name of the table to load data to
# from_schema = name of the schema the input data are in
# from_table = common prefix of the table the input data are in
# id_schema = name of the schema the identity table is in
# id_table = name of the identity table

load_stage_timevar <- function(conn = NULL,
                               to_schema = "pha",
                               to_table = "stage_timevar",
                               from_schema = "pha",
                               from_table = "stage_",
                               demo_schema = "pha",
                               demo_table = "final_demo",
                               id_schema = "pha",
                               id_table = "final_identities") {
  
  
  # BRING IN DATA AND COMBINE ----
  kcha <- dbGetQuery(
    conn,
    glue_sql(
      "SELECT DISTINCT b.id_kc_pha, a.*, c.dob
      FROM 
      (SELECT agency, act_type, act_date, pha_source, 
      id_hash, ssn, lname, fname, relcode, disability, 
      hh_ssn, hh_lname, hh_fname, mbr_num, hh_inc_fixed, hh_inc_vary, 
      hh_id, subsidy_id, cert_id, vouch_num, 
      port_in, port_out_kcha, portability, cost_pha, 
      subs_type, major_prog, prog_type, vouch_type,
      geo_add1_clean, geo_add2_clean, geo_city_clean, geo_state_clean, geo_zip_clean,
      geo_hash_clean, geo_blank, portfolio, portfolio_type
        FROM {`from_schema`}.{DBI::SQL(paste0(from_table, 'kcha'))}) a
      LEFT JOIN
      (SELECT id_hash, id_kc_pha FROM {`id_schema`}.{`id_table`}) b
      ON a.id_hash = b.id_hash
      LEFT JOIN
      (SELECT id_kc_pha, dob FROM {`demo_schema`}.{`demo_table`}) c
      ON b.id_kc_pha = c.id_kc_pha",
      .con = conn))
  
  
  # Once upstream stage files are changes, change the following:
  sha <- dbGetQuery(
    conn,
    glue_sql(
      "SELECT DISTINCT b.id_kc_pha, a.*, c.dob
      FROM 
      (SELECT agency, act_type, act_date, pha_source, 
      id_hash, ssn, lname, fname, relcode, disability, 
      hh_ssn, hh_lname, hh_fname, mbr_num, hh_inc_fixed, 
      cert_id, incasset_id, 
      port_in, port_out_sha, portability, cost_pha, 
      recert_sched,
      subs_type, major_prog, prog_type, vouch_type,
      geo_add1_clean, geo_add2_clean, geo_city_clean, geo_state_clean, geo_zip_clean,
      geo_hash_clean, geo_blank, portfolio, property_id
        FROM {`from_schema`}.{DBI::SQL(paste0(from_table, 'sha'))}) a
      LEFT JOIN
      (SELECT id_hash, id_kc_pha FROM {`id_schema`}.{`id_table`}) b
      ON a.id_hash = b.id_hash
      LEFT JOIN
      (SELECT id_kc_pha, dob FROM {`demo_schema`}.{`demo_table`}) c
      ON b.id_kc_pha = c.id_kc_pha",
      .con = conn))
  
  
  pha <- setDT(bind_rows(kcha, sha))
  
  # Remove the rows with missing id_kc_pha since we won't be able to join on this
  pha <- pha[!is.na(id_kc_pha)]
  # Remove Sedro-Woolley HA
  pha <- pha[agency != "SWHA"]
  
  
  # KEY VARIABLES ----
  ## Household ID ----
  # Make a temp hh_id that can be used to find the following:
  # 1) Multiple rows per ID/date/etc but one is missing an address
  # 2) There are two heads of households listed so multiple rows
  pha[, hh_id_tmp := .GRP, by = c("agency", "act_date", "act_type", "hh_id", "subsidy_id", 
                                  "cert_id", "vouch_num", "incasset_id")]
  # Also flag when there is >1 row per ID/HH_ID combo
  pha[, hh_id_cnt := .N, by = c("id_kc_pha", "hh_id_tmp")]
  
  # Also make a head of household ID to track households across time
  pha[, hh_id_long := .GRP, by = c("hh_ssn", "hh_lname", "hh_fname")]
  
  
  ## Subsidy type ----
  # Make program type upper case (in case it isn't)
  pha[, prog_type := toupper(prog_type)]
  
  # Hard vs. TBS8 units (subsidy type)
  pha[, subsidy_type := case_when(
    str_detect(prog_type, "TBS8|PORT|TENANT BASED") ~ "TENANT BASED/SOFT UNIT",
    prog_type %in% c("PH", "SHA OWNED AND MANAGED", "SHA OWNED/MANAGED", "PBS8") ~ "HARD UNIT",
    prog_type == "COLLABORATIVE HOUSING" & (subs_type != "HCV" | is.na(subs_type)) ~ "HARD UNIT",
    prog_type == "COLLABORATIVE HOUSING" & subs_type == "HCV" ~ "TENANT BASED/SOFT UNIT",
    is.na(prog_type) & subs_type == "HCV" ~ "TENANT BASED/SOFT UNIT",
    TRUE ~ NA_character_)]
  
  # Check output
  pha %>% count(agency, prog_type, subs_type, subsidy_type)
  
  
  ## Portfolio ----
  # Finalize portfolios
  pha[, portfolio_final := case_when(
    agency == "SHA" ~ toupper(portfolio),
    agency == "KCHA" & !is.na(portfolio_type) & portfolio_type != "" ~ portfolio_type,
    TRUE ~ NA_character_
  )]
  
  # Check output
  pha %>% count(agency, subsidy_type, portfolio_type, portfolio_final)
  
  
  ## Operator type ----
  pha[, operator_type := case_when(
    prog_type %in% c("PH", "SHA OWNED AND MANAGED") ~ "PHA OPERATED",
    toupper(vouch_type) == "SHA OWNED PROJECT-BASED" & !is.na(vouch_type) ~ "PHA OPERATED",
    !is.na(portfolio_final) & str_detect(portfolio_final, "COLLABORATIVE") == F ~ "PHA OPERATED",
    prog_type %in% c("PBS8") & is.na(portfolio_final) ~ "NON-PHA OPERATED",
    subsidy_type == "HARD UNIT" & agency == "SHA" & prog_type == "COLLABORATIVE HOUSING" & 
      (vouch_type != "SHA OWNED PROJECT-BASED" | is.na(vouch_type)) ~ "NON-PHA OPERATED",
    TRUE ~ NA_character_)]
  
  # Check output
  pha %>% count(agency, subsidy_type, operator_type)
  
  
  ## Voucher type ----
  pha[, vouch_type := toupper(vouch_type)]
  pha[, vouch_type_final := case_when(
    # Special types (some in hard units also)
    vouch_type == "AGENCY VOUCHER" ~ "AGENCY TENANT-BASED VOUCHER",
    vouch_type == "FUP" ~ "FUP",
    vouch_type == "HASP" ~ "HASP",
    vouch_type == "MOD REHAB" ~ "MOD REHAB",
    vouch_type == "VASH" ~ "VASH",
    vouch_type %in% c("DOMESTIC VIOLENCE", "TERMINALLY ILL", "OTHER - DV/TI") ~ "OTHER (TI/DV)",
    # Other soft units
    subsidy_type == "TENANT BASED/SOFT UNIT" & 
      vouch_type %in% c("PERMANENT SUPPORTIVE HOUSING",
                        "PH REDEVELOPMENT", 
                        "PROJECT-BASED - LOCAL",
                        "PROJECT-BASED - REPLACEMENT HOUSING",
                        "SOUND FAMILIES", 
                        "SUPPORTIVE HOUSING",
                        "TENANT BASED VOUCHER") ~ "GENERAL TENANT-BASED VOUCHER",
    subsidy_type == "TENANT BASED/SOFT UNIT" & 
      is.na(vouch_type) ~ "GENERAL TENANT-BASED VOUCHER",
    # Partner vouchers
    subsidy_type == "HARD UNIT" & operator_type == "NON-PHA OPERATED" &
      vouch_type %in% c("PERMANENT SUPPORTIVE HOUSING",
                        "PH REDEVELOPMENT", 
                        "PROJECT-BASED - LOCAL",
                        "PROJECT-BASED - REPLACEMENT HOUSING",
                        "SOUND FAMILIES", 
                        "SUPPORTIVE HOUSING",
                        "TENANT BASED VOUCHER") ~ "PARTNER PROJECT-BASED VOUCHER",
    subsidy_type == "HARD UNIT" & operator_type == "NON-PHA OPERATED" & 
      is.na(vouch_type) ~ "PARTNER PROJECT-BASED VOUCHER",
    # PHA operated vouchers
    subsidy_type == "HARD UNIT" & operator_type == "PHA OPERATED" &
      vouch_type %in% c("PERMANENT SUPPORTIVE HOUSING",
                        "PH REDEVELOPMENT", 
                        "PROJECT-BASED - LOCAL",
                        "PROJECT-BASED - REPLACEMENT HOUSING",
                        "SOUND FAMILIES", 
                        "SUPPORTIVE HOUSING",
                        "TENANT BASED VOUCHER") ~ "PHA OPERATED VOUCHER",
    subsidy_type == "HARD UNIT" & operator_type == "PHA OPERATED" & 
      is.na(vouch_type) ~ NA_character_,
    # The rest
    TRUE ~ NA_character_
  )]
  
  # Check output
  pha %>% count(agency, subsidy_type, vouch_type, vouch_type_final)
  
  
  ## Age ----
  # Use this to determine time between recertifications
  pha[, age := round(interval(start = dob, end = act_date) / years(1), 1)]
  
  # See how many extreme/wrong ages there are and decide if more investigate is needed
  pha %>% filter(is.na(age)) %>% summarise(cnt = n())
  pha %>% filter(age < -1) %>% summarise(cnt = n())
  pha %>% filter(age > 110) %>% summarise(cnt = n())
  
  # Set up age-related variables
  pha[, ':=' (adult = ifelse(age >= 18, 1, 0),
              senior = ifelse(age >= 62, 1, 0))]
  
  pha %>% count(adult, senior)
  
  
  ## Concatenated agency field ----
  pha[, agency_prog_concat := paste(agency, subsidy_type, operator_type, 
                                    vouch_type_final, portfolio_final, 
                                    sep = ", ")]
  
  
  # HEAD OF HOUSEHOLD ----
  ## Make a new HH ID to track households over time ----
  hh <- pha[, c("id_kc_pha", "agency", "act_date", "ssn", "lname", "fname", "dob",
                "hh_ssn", "hh_lname", "hh_fname", "mbr_num", "hh_id_tmp")]
  hh <- unique(hh)
  
  # Attach id_kc_pha to each HH to make tracking easier
  hh_id <- hh[, c("id_kc_pha", "act_date", "ssn", "lname", "fname", "dob", "hh_ssn", "hh_lname", "hh_fname")]
  hh_id <- unique(hh_id)
  
  hh_id[, hh_id_kc_pha := case_when(
    hh_ssn == ssn & !is.na(ssn) & (hh_lname == lname | hh_fname == fname) ~ id_kc_pha,
    hh_lname == lname & hh_fname == fname & (is.na(hh_ssn) | is.na(ssn)) ~ id_kc_pha,
    hh_ssn != ssn & !is.na(ssn) ~ NA_character_
  )]
  hh_id <- hh_id[!is.na(hh_id_kc_pha), c("id_kc_pha", "act_date", "hh_ssn", "hh_lname", "hh_fname", "hh_id_kc_pha")]
  hh_id <- unique(hh_id)
  
  # Join back to HH data
  hh <- merge(hh, hh_id, by = c("id_kc_pha", "act_date", "hh_ssn", "hh_lname", "hh_fname"), all.x = T)
  
  hh[, hh_flag := case_when(
    mbr_num == 1 ~ 1L,
    mbr_num > 1 ~ 0L,
    hh_ssn == ssn & !is.na(ssn) ~ 1L,
    hh_lname == lname & hh_fname == fname ~ 1L,
    TRUE ~ 0L)]
  
  # Check how many households don't have a HH or have multiple HHs
  # Also count how many rows have any ID on it (used for tiebreakers later)
  hh[, has_hh := sum(hh_flag, na.rm = T), by = "hh_id_tmp"]
  hh[, hh_id_cnt := uniqueN(hh_id_kc_pha, na.rm = T), by = "hh_id_tmp"]
  hh[!is.na(hh_id_kc_pha), hh_id_row_cnt := .N, by = "hh_id_tmp"]
  hh[, hh_id_row_cnt := max(hh_id_row_cnt, na.rm = T), by = "hh_id_tmp"]
  
  hh %>% count(agency, has_hh)
  hh %>% count(agency, has_hh, hh_id_cnt)
  hh %>% count(agency, hh_id_cnt)
  
  # For households with no hh_id_kc_pha, add some possibilities
  hh[hh_id_cnt == 0 & has_hh != 1, hh_id_poss := case_when(
    RecordLinkage::jarowinkler(hh_ssn, ssn) > 0.9 ~ 1L,
    (ssn == hh_ssn | (is.na(hh_ssn) | is.na(ssn))) & fname == hh_fname ~ 1L,
    hh_lname == lname & hh_fname == fname & (!is.na(hh_ssn) | !is.na(ssn)) ~ 1L,
    TRUE ~ 0L
  )]
  hh[, hh_id_poss_cnt := sum(hh_id_poss, na.rm = T), by = "hh_id_tmp"]
  hh %>% count(hh_id_poss_cnt)
  
  
  ## Assign head of household as needed ----
  ## Use the following to allocate HH for a given act_date/hh combo when 2+ candidates
  # 1) If 2+ hh_flags but only one row with a hh_id_value, take the one with a hh_id_pha_value 
  # 2) If 1+ hh_id_kc_pha value but competing details, take mbr_num = 1
  # 3) If no mbr_num or 2+ with mbr_num = 1, take oldest of those with a hh_id_kc_pha value (regardless of mbr_num)
  # 4) If no DOB for #3, take the row with a hh_ssn value (regardless of mbr_num)
  # 5) If still undecided, randomly assign one of the people with a hh_id_kc_pha value
  # 6) If 2+ hh_id_kc_pha values, take mbr_num = 1
  # 7) If no mbr_num or 2+ with mbr_num = 1, take oldest of those with a hh_id_kc_pha value (regardless of mbr_num)
  # 8) If no DOB for #7, randomly assign one of the people with a hh_id_kc_pha value
  # 9) If no hh_id_kc_pha values and 2+ with mbr_num = 1, take the oldest person with mbr_num = 1
  # 10) If no DOB for #9, randomly take one of the mbr_num = 1 people
  # 11) If no hh_id_kc_pha and no mbr_num, take the possible hh_id person
  # 12) If multiple possible hh_id people, take the eldest
  # 13) If no DOBs for #12, take a random hh_id contender
  # 14) If no possible hh_id contenders, take the oldest person in the house
  # 15) If no DOB for #14 or oldest is a tie, randomly take one person in the house
  hh_decider <- hh[has_hh != 1]
  hh_decider[mbr_num == 1, mbr1_cnt := .N, by = c("hh_id_tmp")]
  hh_decider[mbr_num == 1, mbr1_id_cnt := uniqueN(hh_id_kc_pha, na.rm = T), by = c("hh_id_tmp")]
  hh_decider[mbr_num == 1 & !is.na(hh_id_kc_pha), mbr1_id_row_cnt := .N, by = c("hh_id_tmp")]
  hh_decider[!is.na(hh_id_kc_pha), has_ssn_id := !is.na(hh_ssn)]
  hh_decider[!is.na(hh_id_kc_pha), oldest_hh_id := dob == min(dob, na.rm = T), by = "hh_id_tmp"]
  hh_decider[mbr_num == 1 & !is.na(hh_id_kc_pha), oldest_hh_id_mbr1 := dob == min(dob, na.rm = T), by = "hh_id_tmp"]
  hh_decider[mbr_num == 1, oldest_mbr1 := dob == min(dob, na.rm = T), by = "hh_id_tmp"]
  hh_decider[hh_id_poss == 1, oldest_hh_poss := dob == min(dob, na.rm = T), by = "hh_id_tmp"]
  hh_decider[, oldest_any := dob == min(dob, na.rm = T), by = "hh_id_tmp"]
  # Apply key numbers to whole household
  hh_decider[, hh_id_row_cnt := max(hh_id_row_cnt, na.rm = T), by = c("hh_id_tmp")]
  hh_decider[, mbr1_cnt := max(mbr1_cnt, na.rm = T), by = c("hh_id_tmp")]
  hh_decider[, mbr1_id_cnt := max(mbr1_id_cnt, na.rm = T), by = c("hh_id_tmp")]
  hh_decider[, mbr1_id_row_cnt := max(mbr1_id_row_cnt, na.rm = T), by = c("hh_id_tmp")]
  hh_decider[, has_ssn_id_cnt := sum(has_ssn_id, na.rm = T), by = c("hh_id_tmp")]
  hh_decider[, oldest_hh_id_cnt := sum(oldest_hh_id, na.rm = T), by = c("hh_id_tmp")]
  hh_decider[, oldest_hh_id_mbr1_cnt := sum(oldest_hh_id_mbr1, na.rm = T), by = c("hh_id_tmp")]
  hh_decider[, oldest_mbr1_cnt := sum(oldest_mbr1, na.rm = T), by = c("hh_id_tmp")]
  hh_decider[, oldest_hh_poss_cnt := sum(oldest_hh_poss, na.rm = T), by = c("hh_id_tmp")]
  hh_decider[, oldest_any_cnt := sum(oldest_any, na.rm = T), by = c("hh_id_tmp")]
  # Set up random numbers for each scenario
  set.seed(98104)
  hh_decider[!is.na(hh_id_kc_pha), decider_id := runif(.N, 0, 1)]
  hh_decider[!is.na(hh_id_kc_pha) & mbr_num == 1, decider_id_mbr1 := runif(.N, 0, 1)]
  hh_decider[mbr_num == 1, decider_mbr1 := runif(.N, 0, 1)]
  hh_decider[hh_id_poss == 1, decider_hh_poss := runif(.N, 0, 1)]
  hh_decider[oldest_hh_id == T, decider_oldest_hh_id := runif(.N, 0, 1)]
  hh_decider[oldest_any == T, decider_oldest := runif(.N, 0, 1)]
  hh_decider[, decider_any := runif(.N, 0, 1)]
  # Apply max random numbers to whole household
  hh_decider[, decider_id_max := max(decider_id, na.rm = T), by = c("hh_id_tmp")]
  hh_decider[, decider_id_mbr1_max := max(decider_id_mbr1, na.rm = T), by = c("hh_id_tmp")]
  hh_decider[, decider_mbr1_max := max(decider_mbr1, na.rm = T), by = c("hh_id_tmp")]
  hh_decider[, decider_hh_poss_max := max(decider_hh_poss, na.rm = T), by = c("hh_id_tmp")]
  hh_decider[, decider_oldest_hh_id_max := max(decider_oldest_hh_id, na.rm = T), by = c("hh_id_tmp")]
  hh_decider[, decider_oldest_max := max(decider_oldest, na.rm = T), by = c("hh_id_tmp")]
  hh_decider[, decider_any_max := max(decider_any, na.rm = T), by = c("hh_id_tmp")]
  
  
  ### Reset hh_flag ----
  hh_decider[, hh_flag := NA_integer_]
  
  ### One hh_id_kc_pha value with multiple rows ----
  hh_decider[, hh_flag := case_when(
    # One row with an ID
    hh_id_cnt == 1 & hh_id_row_cnt == 1 & !is.na(hh_id_kc_pha) ~ 1L,
    hh_id_cnt == 1 & hh_id_row_cnt == 1 & is.na(hh_id_kc_pha) ~ 0L,
    # One row with mbr_num = 1
    hh_id_cnt == 1 & mbr1_cnt >= 1 & mbr1_id_row_cnt == 1 & mbr_num == 1 & !is.na(hh_id_kc_pha) ~ 1L,
    hh_id_cnt == 1 & mbr1_cnt >= 1 & mbr1_id_row_cnt == 1 & mbr_num == 1 & is.na(hh_id_kc_pha) ~ 0L,
    hh_id_cnt == 1 & mbr1_cnt >= 1 & mbr1_id_row_cnt == 1 & mbr_num != 1 ~ 0L,
    # 2+ rows with mbr_num = 1 (take oldest person, most likely actually non-missing DOB)
    hh_id_cnt == 1 & mbr1_id_row_cnt > 1 & mbr_num == 1 & 
      oldest_hh_id_mbr1 == T & oldest_hh_id_mbr1_cnt == 1 ~ 1L,
    hh_id_cnt == 1 & mbr1_id_row_cnt > 1 & mbr_num == 1 & 
      (oldest_hh_id_mbr1 == F | (is.na(oldest_hh_id_mbr1) & oldest_hh_id_mbr1_cnt == 1)) ~ 0L,
    hh_id_cnt == 1 & mbr1_id_row_cnt > 1 & mbr_num != 1 ~ 0L,
    # 2+ rows with mbr_num = 1 and no DOB or tied DOB, take SSN (regardless of mbr_num)
    hh_id_cnt == 1 & mbr1_id_row_cnt > 1 & has_ssn_id == T & has_ssn_id_cnt == 1 ~ 1L,
    hh_id_cnt == 1 & mbr1_id_row_cnt > 1 & has_ssn_id == F & has_ssn_id_cnt == 1 ~ 0L,
    # 2+ rows with mbr_num = 1 and no DOB or tied DOB, and no SSN
    hh_id_cnt == 1 & mbr1_id_cnt > 1 & mbr_num == 1 & oldest_hh_id_mbr1_cnt != 1 & 
      has_ssn_id_cnt != 1 & decider_id_mbr1 == decider_id_mbr1_max ~ 1L,
    hh_id_cnt == 1 & mbr1_id_cnt > 1 & mbr_num == 1 & oldest_hh_id_mbr1_cnt != 1 & 
      has_ssn_id_cnt != 1 & decider_id_mbr1 != decider_id_mbr1_max ~ 0L,
    # No member numbers, but DOBs
    hh_id_cnt == 1 & is.na(mbr1_cnt) & oldest_hh_id == T & oldest_hh_id_cnt == 1 ~ 1L,
    hh_id_cnt == 1 & is.na(mbr1_cnt) & (oldest_hh_id == F | (is.na(oldest_hh_id) & oldest_hh_id_cnt == 1)) ~ 0L,
    # No member numbers, and tied DOB
    hh_id_cnt == 1 & is.na(mbr1_cnt) & oldest_hh_id_cnt > 1 & 
      decider_oldest_hh_id == decider_oldest_hh_id_max ~ 1L,
    hh_id_cnt == 1 & is.na(mbr1_cnt) & oldest_hh_id_cnt > 1 & 
      decider_oldest_hh_id != decider_oldest_hh_id_max ~ 0L,
    # No member numbers and no DOBs
    hh_id_cnt == 1 & decider_id == decider_id_max ~ 1L,
    hh_id_cnt == 1 & decider_id != decider_id_max ~ 0L,
    hh_id_cnt == 1 & is.na(decider_id) ~ 0L,
    TRUE ~ hh_flag)]
  
  
  ### Multiple hh_id_kc_pha values ----
  hh_decider[, hh_flag := case_when(
    # One hh_id_kc_pha with mbr_num = 1
    hh_id_cnt > 1 & mbr1_cnt >= 1 & mbr1_id_cnt == 1 & mbr_num == 1 & !is.na(hh_id_kc_pha) ~ 1L,
    hh_id_cnt > 1 & mbr1_cnt >= 1 & mbr1_id_cnt == 1 & mbr_num == 1 & is.na(hh_id_kc_pha) ~ 0L,
    hh_id_cnt > 1 & mbr1_cnt >= 1 & mbr1_id_cnt == 1 & mbr_num != 1 ~ 0L,
    # 2+ hh_id_kc_pha with mbr_num = 1 (take oldest person)
    hh_id_cnt > 1 & mbr1_id_cnt > 1 & mbr_num == 1 & 
      oldest_hh_id_mbr1 == T & oldest_hh_id_mbr1_cnt == 1 ~ 1L,
    hh_id_cnt > 1 & mbr1_id_cnt > 1 & mbr_num == 1 & 
      (oldest_hh_id_mbr1 == F | (is.na(oldest_hh_id_mbr1) & oldest_hh_id_mbr1_cnt == 1)) ~ 0L,
    hh_id_cnt > 1 & mbr1_id_cnt > 1 & mbr_num != 1 ~ 0L,
    # 2+ hh_id_kc_pha with mbr_num = 1 and no DOB or tied DOB
    hh_id_cnt > 1 & mbr1_id_cnt > 1 & mbr_num == 1 & oldest_hh_id_mbr1_cnt != 1 & decider_id_mbr1 == decider_id_mbr1_max ~ 1L,
    hh_id_cnt > 1 & mbr1_id_cnt > 1 & mbr_num == 1 & oldest_hh_id_mbr1_cnt != 1 & decider_id_mbr1 != decider_id_mbr1_max ~ 0L,
    # No member numbers, but DOBs
    hh_id_cnt > 1 & is.na(mbr1_cnt) & oldest_hh_id == T & oldest_hh_id_cnt == 1 ~ 1L,
    hh_id_cnt > 1 & is.na(mbr1_cnt) & (oldest_hh_id == F | (is.na(oldest_hh_id) & oldest_hh_id_cnt == 1)) ~ 0L,
    # No member numbers, and tied DOB
    hh_id_cnt > 1 & is.na(mbr1_cnt) & oldest_any_cnt > 1 & 
      decider_oldest_hh_id == decider_oldest_hh_id_max ~ 1L,
    hh_id_cnt > 1 & is.na(mbr1_cnt) & oldest_any_cnt > 1 & 
      decider_oldest_hh_id != decider_oldest_hh_id_max ~ 0L,
    # No member numbers and no DOBs
    hh_id_cnt > 1 & decider_id == decider_id_max ~ 1L,
    hh_id_cnt > 1 & decider_id != decider_id_max ~ 0L,
    hh_id_cnt > 1 & is.na(decider_id) ~ 0L,
    TRUE ~ hh_flag)]
  
  
  ### No hh_id_kc_pha values ----
  hh_decider[, hh_flag := case_when(
    # 1+ mbr_num = 1 and DOBs
    hh_id_cnt == 0 & mbr1_cnt > 1 & mbr_num == 1 & oldest_mbr1 == T & oldest_mbr1_cnt == 1 ~ 1L,
    hh_id_cnt == 0 & mbr1_cnt > 1 & mbr_num == 1 & (oldest_mbr1 == F | (is.na(oldest_mbr1) & oldest_mbr1_cnt == 1)) ~ 0L,
    # 1+ mbr_num = 1 and no DOB or tied DOB
    hh_id_cnt == 0 & mbr1_cnt > 1 & mbr_num == 1 & oldest_mbr1_cnt != 1 & decider_mbr1 == decider_mbr1_max ~ 1L,
    hh_id_cnt == 0 & mbr1_cnt > 1 & mbr_num == 1 & oldest_mbr1_cnt != 1 & decider_mbr1 != decider_mbr1_max ~ 0L,
    hh_id_cnt == 0 & mbr1_cnt >= 1 & mbr_num != 1 ~ 0L,
    # No member numbers, one possible hh_id_kc_pha contenders
    hh_id_cnt == 0 & is.na(mbr1_cnt) & hh_id_poss == 1 & hh_id_poss_cnt == 1 ~ 1L,
    hh_id_cnt == 0 & is.na(mbr1_cnt) & hh_id_poss == 0 & hh_id_poss_cnt == 1 ~ 0L,
    # No member numbers, 2+ possible hh_id_kc_pha contenders and DOBs
    hh_id_cnt == 0 & is.na(mbr1_cnt) & hh_id_poss == 1 & hh_id_poss_cnt > 1 & 
      oldest_hh_poss_cnt == 1 & oldest_hh_poss == T ~ 1L,
    hh_id_cnt == 0 & is.na(mbr1_cnt) & hh_id_poss_cnt > 1 & 
      (oldest_hh_poss == F | (is.na(oldest_hh_poss) & oldest_hh_poss_cnt == 1)) ~ 0L,
    hh_id_cnt == 0 & is.na(mbr1_cnt) & hh_id_poss_cnt >= 1 & hh_id_poss == 0 ~ 0L,
    # No member numbers, 2+ possible hh_id_kc_pha contenders and no DOB or tied DOB
    hh_id_cnt == 0 & is.na(mbr1_cnt) & hh_id_poss == 1 & hh_id_poss_cnt > 1 & oldest_hh_poss_cnt != 1 & 
      decider_hh_poss == decider_hh_poss_max ~ 1L,
    hh_id_cnt == 0 & is.na(mbr1_cnt) & hh_id_poss == 1 & hh_id_poss_cnt > 1 & oldest_hh_poss_cnt != 1 & 
      decider_hh_poss != decider_hh_poss_max ~ 0L,
    # No member numbers or possible hh_id_kc_pha contenders, but DOBs
    hh_id_cnt == 0 & is.na(mbr1_cnt) & hh_id_poss_cnt == 0 & oldest_any == T & oldest_any_cnt == 1 ~ 1L,
    hh_id_cnt == 0 & is.na(mbr1_cnt) & hh_id_poss_cnt == 0 & 
      (oldest_any == F | (is.na(oldest_any) & oldest_any_cnt == 1)) ~ 0L,
    # No member numbers or possible hh_id_kc_pha contenders, and tied DOB
    hh_id_cnt == 0 & is.na(mbr1_cnt) & hh_id_poss_cnt == 0 & oldest_any_cnt > 1 & 
      decider_oldest == decider_oldest_max ~ 1L,
    hh_id_cnt == 0 & is.na(mbr1_cnt) & hh_id_poss_cnt == 0 & oldest_any_cnt > 1 & 
      decider_oldest != decider_oldest_max ~ 0L,
    # No member numbers or possible hh_id_kc_pha contenders, and no DOBs
    hh_id_cnt == 0 & is.na(mbr1_cnt) & is.na(oldest_any) & oldest_any_cnt == 0 & 
      decider_any == decider_any_max ~ 1L,
    hh_id_cnt == 0 & is.na(mbr1_cnt) & is.na(oldest_any) & oldest_any_cnt == 0 &  
      decider_any != decider_any_max ~ 0L,
    TRUE ~ hh_flag)]
  
  hh_decider %>% count(hh_flag)
  
  # Make sure each hh_id_tmp has at least 1 hh_flag
  if (nrow(hh_decider[is.na(hh_flag)]) > 0) {
    stop("Check head of household decider code to see why some people were not classified")
  }
  
  # Transfer name and SSN details over to HH fields
  hh_decider[hh_flag == 1, `:=` (hh_id_kc_pha = id_kc_pha,
                                 hh_ssn = ssn,
                                 hh_lname = lname,
                                 hh_fname = fname)]
  
  unique_hh_ids <- length(unique(hh_decider$hh_id_tmp))
  
  hh_decider <- hh_decider[hh_flag == 1, .(agency, act_date, hh_id_tmp,
                                           hh_id_kc_pha, hh_ssn, hh_lname, hh_fname, hh_flag)]
  hh_decider <- unique(hh_decider)
  
  # Check all households are still present
  if (length(unique(hh_decider$hh_id_tmp)) != unique_hh_ids) {
    stop("Some hh_id_tmp values disappeared from hh_decider. Check why.")
  }
  if (nrow(hh_decider) != unique_hh_ids) {
    stop ("The number of rows in hh_decider doesn't match the number of hh_id_values. Check why.")
  }
  rm(unique_hh_ids)
  
  
  ## Set up data without head of household details ----
  hh_final <- copy(hh)
  hh_final[, `:=` (hh_id_kc_pha = NULL, hh_ssn = NULL, hh_lname = NULL, hh_fname = NULL)]
  hh_final <- unique(hh_final)
  
  ## Member numbers ----
  # People appear twice with different member numbers
  # Collapse to lowest observed member number to reduce row duplication
  # Best to do here before regenerating the hh_flag
  hh_final[, mbr_num_min := min(mbr_num, na.rm = T), by = c("hh_id_tmp", "id_kc_pha", "ssn", "lname", "fname", "dob")]
  
  # Select final columns for joining back to main data
  hh_final <- hh_final[, mbr_num := NULL]
  hh_final <- unique(hh_final)
  
  
  ## Join HH decider back to main DF ----
  # Pull out all the head of households that didn't need to be decided
  hh_no_decider <- hh[hh_flag == 1 & has_hh == 1, 
                      .(agency, act_date, hh_id_tmp, id_kc_pha, ssn, lname, fname, hh_flag)]
  setnames(hh_no_decider, c("id_kc_pha", "ssn", "lname", "fname"), c("hh_id_kc_pha", "hh_ssn", "hh_lname", "hh_fname"))
  
  
  hh_final <- merge(hh_final, 
                    bind_rows(hh_decider, hh_no_decider), 
                    by = c("agency", "act_date", "hh_id_tmp"),
                    all.x = T)
  
  
  hh_final[, `:=` (
    hh_flag = case_when(has_hh == 1 ~ hh_flag.x,
                        hh_id_kc_pha == id_kc_pha & 
                          (hh_ssn == ssn | (is.na(hh_ssn) & is.na(ssn))) & 
                          (hh_lname == lname | (is.na(hh_lname) & is.na(lname))) & 
                          (hh_fname == fname | (is.na(hh_fname) & is.na(fname))) ~ 1L,
                        TRUE ~ 0L)
  )]
  
  
  ## Check how many households don't have a HH or have multiple HHs ----
  hh_final[, has_hh2 := sum(hh_flag, na.rm = T), by = "hh_id_tmp"]
  hh_final %>% count(agency, has_hh2)
  
  
  # Some households still have multiple HHs because of duplicate HH data
  # Use initial HH flag and details to weed these out
  hh_final[has_hh2 > 1 & hh_flag == 1, hh_flag := 
             case_when(hh_flag.x == 1 & hh_flag == 1 & hh_id_kc_pha == id_kc_pha ~ 1L,
                       hh_flag.x == 0 & hh_flag == 1 & hh_id_kc_pha == id_kc_pha ~ 0L,
                       hh_flag.x == 1 & hh_flag == 1 & hh_id_kc_pha != id_kc_pha ~ 0L,
                       TRUE ~ hh_flag)]
  
  

  
  # Check how many households don't have a HH or have multiple HHs
  hh_final[, has_hh3 := sum(hh_flag, na.rm = T), by = "hh_id_tmp"]
  hh_final %>% count(agency, has_hh2, has_hh3)
  
  if (max(hh_final$has_hh3) > 1) {
    stop(" There are still some hh_id_tmp values with multiple HHs")
  }
  
  # Apply hh_flag to any remaining duplicate rows and collapse
  hh_final[, hh_flag := max(hh_flag), by = c("hh_id_tmp", "id_kc_pha", "ssn", "lname", "fname", "dob")]
  
  # Select final columns for merging
  hh_final <- hh_final[, .(agency, act_date, hh_id_tmp, 
                           id_kc_pha, ssn, lname, fname, dob, mbr_num_min, 
                           hh_id_kc_pha, hh_ssn, hh_lname, hh_fname, hh_flag)]
  hh_final <- unique(hh_final)
  
  # One last check for duplicate HHs
  hh_final[, has_hh4 := sum(hh_flag, na.rm = T), by = "hh_id_tmp"]
  hh_final %>% count(agency, has_hh4)
  
  if (max(hh_final$has_hh4) > 1) {
    stop(" There are still some hh_id_tmp values with multiple HHs")
  } else {
    hh_final[, has_hh4 := NULL]
  }
  
  # There are now some duplicate rows where everything is the same except hh_id_tmp
  # Since we will join hh_final later to the consolidated data, we no longer need
  # most columns.
  # However, keep hh_final for now in case this process is revised.
  hh_final2 <- copy(hh_final)
  hh_final2 <- unique(hh_final2[, .(agency, id_kc_pha, act_date, hh_id_kc_pha, hh_ssn, hh_lname, hh_fname, hh_flag)])
  
  
  # BRING HEAD OF HOUSEHOLD AND MEMBER NUMBER INFO BACK TO MAIN DATA ----
  # Remove the columns that will be re-added later
  pha[, `:=` (mbr_num = NULL, hh_ssn = NULL, hh_lname = NULL, hh_fname = NULL)]
  pha <- unique(pha)
  
  # Currently not adding this info until after consolidation
  # pha <- merge(pha, hh_final, by = c("agency", "act_date", "hh_id_tmp", "id_kc_pha", 
  #                                    "ssn", "lname", "fname", "dob"),
  #               all.x = T)
  
  
  # BEGIN CONSOLIDATION ----
  # New approach to tracking which rows are dropped
  # Assign a different drop code for each instance and track in a list of all rows
  # Note that a blank address has a geo_hash of 8926262F06508A0E264BC13D340FD8FAB9291001FC06341D2E687BD9C3AF6104
  #   because the zip code is recorded as 0
  
  # Drop types:
  # 01 = missing action dates
  # 02 = duplicate rows due to multiple EOP types (6 and 11)
  # 03 = duplicate rows due to varying cert IDs or income data
  # 04 = where address data are missing
  # 05 = when a person is in both KCHA and SHA data due to port ins/outs
  # 06 = blank addresses when there is an address for the same date (within a given program/subtype/spec voucher etc.)
  # 07 = different programs with the same start date within the same agency
  # 08 = different agencies with the same or similar action date (mostly remaining ports)
  # 09 = auto-generated recertifications that are overwriting EOPs
  # 10 = annual reexaminations/intermediate visits within a given address and program
  # 11 = collapse rows to have a single line per person per address per time there
  # 12 = delete rows where from_date = to_date and also from_date = the next row's from_date (often same program)
  # 13 = delete one row of pairs with identical start and end dates
  
  ## Sort data ----
  pha_sort <- setorder(pha, id_kc_pha, act_date, agency, prog_type)
  
  
  ## Set up row numbers and a tracking ----
  pha_sort[, row := .I]
  
  
  # Set up list of all rows to track
  drop_track <- pha_sort %>% 
    select(row, id_kc_pha, id_hash, agency_prog_concat, 
           geo_add1_clean, geo_add2_clean, geo_city_clean, geo_state_clean, geo_zip_clean,
           geo_hash_clean, geo_blank, act_date, 
           act_type, pha_source, cost_pha)
  
  
  ## Find the latest action date for a given program (will be useful later) ----
  # Group by cost_pha too because some port outs move between agencies
  # Ignore the warning produced, this will be addressed when rows with no action date are dropped
  pha_sort[, max_date := max(act_date, na.rm = T),
           by = .(id_kc_pha, agency_prog_concat, geo_hash_clean, cost_pha)]
  
  
  ## Count the number of unique address a person had  ----
  pha_sort[, add_num := uniqueN(geo_hash_clean), by = "id_kc_pha"]
  
  
  ## Make port in and out variables ----
  ### Port out
  pha_sort[, port_out_kcha := 
             case_when(
               id_kc_pha == lead(id_kc_pha, 1) & !is.na(lead(id_kc_pha, 1)) & 
                 abs(act_date - lead(act_date, 1)) <= 31 & 
                 agency == "SHA" & lead(agency, 1) == "KCHA" & 
                 (lead(port_out_kcha, 1) == 1 | port_in == 1) &
                 !lead(act_type, 1) %in% c(1, 4) & 
                 cost_pha %in% c("", "WA002") & act_type != 16 ~ 1L, 
               id_kc_pha == lag(id_kc_pha, 1) & !is.na(lag(id_kc_pha, 1)) & 
                 abs(act_date - lag(act_date, 1)) < 31 & 
                 agency == "SHA" & lag(agency, 1) == "KCHA" & 
                 (lag(port_out_kcha, 1) == 1 | port_in == 1) &
                 lag(act_type, 1) != 4 & 
                 cost_pha %in% c("", "WA002") & act_type != 16 ~ 1L,
               TRUE ~ port_out_kcha)]
  
  pha_sort[, port_out_sha := 
             case_when(
               id_kc_pha == lead(id_kc_pha, 1) & !is.na(lead(id_kc_pha, 1)) &
                 abs(act_date - lead(act_date, 1)) < 31 & 
                 agency == "KCHA" & lead(agency, 1) == "SHA" & 
                 (lead(port_out_sha, 1) == 1 | port_in == 1) &
                 lead(act_type, 1) != 4 & cost_pha %in% c("", "WA001") ~ 1,
               id_kc_pha == lag(id_kc_pha, 1) & !is.na(lag(id_kc_pha, 1)) &
                 abs(act_date - lag(act_date, 1)) < 31 & 
                 agency == "KCHA" & lag(agency, 1) == "SHA" & 
                 (lag(port_out_sha, 1) == 1 | port_in == 1) &
                 lag(act_type, 1) != 4 &
                 cost_pha %in% c("", "WA001") ~ 1,
               TRUE ~ port_out_sha)]
  
  pha_sort[, port_out_kcha := ifelse(agency == "SHA" & cost_pha == "WA002" & 
                                      port_out_sha == 0L & !is.na(port_out_sha), 1, port_out_kcha)]
  pha_sort[, port_out_sha := ifelse(agency == "KCHA" & cost_pha == "WA001", 1, port_out_sha)]
  
  
  
  ## Remove missing dates (droptype = 1) ----
  time_start <- Sys.time()
  dfsize_head <- nrow(pha_sort) # Keep track of size of data frame at each step
  pha_sort[, drop := ifelse(is.na(act_date), 1, 0)]
  # Pull out drop tracking and merge
  drop_temp <- pha_sort %>% select(row, drop)
  drop_track <- left_join(drop_track, drop_temp, by = "row")
  # Finish dropping rows
  pha_sort <- pha_sort[drop != 1]
  dfsize_head - nrow(pha_sort)
  time_end <- Sys.time()
  print(paste0("Drop #1 took ", round(difftime(time_end, time_start, units = "secs"), 2), " secs"))
  
  
  ## Clean up duplicate rows - multiple EOP types (droptype == 2) ----
  # Some duplicate rows where there is both an EOP action (#6) and an
  # expiration of voucher equivalent (#11) (same program)
  # Keep type #6 since code below is set up for that as EOP (and better reflects
  # when people are moving from hard to soft units)
  # Ignore income since these are not consistently recorded, use action #6 inc
  time_start <- Sys.time()
  dfsize_head <- nrow(pha_sort)
  repeat {
    dfsize <- nrow(pha_sort)
    setorder(pha_sort, id_kc_pha, agency_prog_concat, act_date, act_type)
    
    pha_sort[, drop := 
               if_else(id_kc_pha == lag(id_kc_pha, 1) & 
                         geo_hash_clean == lag(geo_hash_clean, 1) &
                         agency_prog_concat == lag(agency_prog_concat, 1) &
                         cost_pha == lag(cost_pha, 1) &
                         act_date == lag(act_date, 1) &
                         max_date == lag(max_date, 1) &
                         act_type == 11 & lag(act_type, 1) == 6 &
                         (pha_source == lag(pha_source, 1) | 
                            (is.na(pha_source) & is.na(lag(pha_source, 1)))),
                       2, 0)]
    # Pull out drop tracking and merge
    drop_temp <- pha_sort %>% select(row, drop)
    drop_track <- left_join(drop_track, drop_temp, by = "row") %>%
      mutate(drop = ifelse(!is.na(drop.x) & drop.x > 0, drop.x, drop.y)) %>%
      select(-drop.x, -drop.y)
    # Finish dropping rows
    pha_sort <- pha_sort[drop == 0 | is.na(drop)]
    
    dfsize2 <-  nrow(pha_sort)
    if (dfsize2 == dfsize) {
      break
    }
  }
  dfsize_head - nrow(pha_sort) # Track how many rows were dropped
  time_end <- Sys.time()
  print(paste0("Drop #2 took ", round(difftime(time_end, time_start, units = "secs"), 2), " secs"))
  
  
  ## Clean up duplicate rows - cert IDs etc. (droptype == 3) ----
  # Some duplicate rows are because of multiple incomes/assets information 
  # reported at the same action date 
  # (i.e., different subsidy/cert IDs/increment (both SHA and KCHA))
  # Taking the final row should work as that captures port subsidy IDs
  # and a random income level
  time_start <- Sys.time()
  dfsize_head <- nrow(pha_sort)
  setorder(pha_sort, id_kc_pha, agency_prog_concat, act_date, cost_pha)
  pha_sort[, drop := 
             if_else(id_kc_pha == lead(id_kc_pha, 1) & 
                       geo_hash_clean == lead(geo_hash_clean, 1) &
                       agency_prog_concat == lead(agency_prog_concat, 1) &
                       act_date == lead(act_date, 1) &
                       act_type == lead(act_type, 1) &
                       (pha_source == lead(pha_source, 1) | 
                          (is.na(pha_source) & is.na(lead(pha_source, 1)))),
                     3, 0)]
  # Pull out drop tracking and merge
  drop_temp <- pha_sort %>% select(row, drop)
  drop_track <- left_join(drop_track, drop_temp, by = "row") %>%
    mutate(drop = ifelse(!is.na(drop.x) & drop.x > 0, drop.x, drop.y)) %>%
    select(-drop.x, -drop.y)
  # Finish dropping rows
  pha_sort <- pha_sort[drop == 0 | is.na(drop)]
  dfsize_head - nrow(pha_sort) # Track how many rows were dropped
  time_end <- Sys.time()
  print(paste0("Drop #3 took ", round(difftime(time_end, time_start, units = "secs"), 2), " secs"))
  
  
  ## Drop rows with missing address data that are not port outs (droptype = 4) ----
  time_start <- Sys.time()
  dfsize_head <- nrow(pha_sort)
  pha_sort[, drop := case_when(
    # Start with non-port-out rows with act_type == 10 or 16)
    geo_blank == 1 & 
      (act_type == 10 | (act_type == 16 & !(port_out_kcha == 1 | port_out_sha == 1))) ~ 4,
    # Also drop people who were only at KCHA and only had missing addresses
    agency == "KCHA" & add_num == 1 & geo_blank == 1 ~ 4,
    TRUE ~ 0
  )]
  # Pull out drop tracking and merge
  drop_temp <- pha_sort %>% select(row, drop)
  drop_track <- left_join(drop_track, drop_temp, by = "row") %>%
    mutate(drop = ifelse(!is.na(drop.x) & drop.x > 0, drop.x, drop.y)) %>%
    select(-drop.x, -drop.y)
  # Finish dropping rows
  pha_sort <- pha_sort[drop == 0 | is.na(drop)]
  dfsize_head - nrow(pha_sort) # Track how many rows were dropped
  time_end <- Sys.time()
  print(paste0("Drop #4 took ", round(difftime(time_end, time_start, units = "secs"), 2), " secs"))
  
  
  ## Find when a person is in both KCHA and SHA data due to port ins/outs (droptype = 5) ----
  # First find rows with the same program/date but one row is missing the 
  # cost_pha field (makes the steps below work better)
  time_start <- Sys.time()
  dfsize_head <- nrow(pha_sort)
  setorder(pha_sort, id_kc_pha, act_date, agency_prog_concat, geo_hash_clean, cost_pha)
  pha_sort[, drop := if_else(id_kc_pha == lead(id_kc_pha, 1) & !is.na(lead(id_kc_pha, 1)) & 
                               act_date == lead(act_date, 1) &
                               agency_prog_concat == lead(agency_prog_concat, 1) & 
                               geo_hash_clean == lead(geo_hash_clean, 1) &
                               cost_pha == "" & lead(cost_pha != ""), 5, 0)]
  # Pull out drop tracking and merge
  drop_temp <- pha_sort %>% select(row, drop)
  drop_track <- left_join(drop_track, drop_temp, by = "row") %>%
    mutate(drop = ifelse(!is.na(drop.x) & drop.x > 0, drop.x, drop.y)) %>%
    select(-drop.x, -drop.y)
  # Finish dropping rows
  pha_sort <- pha_sort[drop == 0 | is.na(drop)]
  
  # Find instances when person is in both agencies on the same or similar date and delete the row for the agency
  # not filling in the form (i.e., usually the one being billed for their port out)
  # However, cost_pha is not consistently used so do not rely on solely that (also use action types and port flags)
  repeat {
    dfsize <-  nrow(pha_sort)
    setorder(pha_sort, id_kc_pha, act_date, agency_prog_concat, geo_hash_clean)
    # Need to check to pairs in both directions because sometimes the SHA date is just before the KCHA one and
    # relying on an alphabetical sort will fail
    # NB. There are some differences between the agencies that require slightly different code
    pha_sort[, drop := case_when(
      id_kc_pha == lead(id_kc_pha, 1) & !is.na(lead(id_kc_pha, 1)) & 
        abs(act_date - lead(act_date, 1)) <= 31 &
        agency == "KCHA" & lead(agency, 1) == "SHA" &
        lead(port_out_kcha, 1) == 1 &
        !act_type %in% c(5, 6) ~ 5,
      id_kc_pha == lag(id_kc_pha, 1) & !is.na(lag(id_kc_pha, 1)) &
        abs(act_date - lag(act_date, 1)) <= 31 &
        agency == "KCHA" & lag(agency, 1) == "SHA" &
        (geo_hash_clean == lag(geo_hash_clean, 1) | geo_blank == 1 | lag(port_out_kcha, 1) == 1) &
        ((act_type %in% c(5, 6) & lag(act_type, 1) %in% c(1, 4)) | 
           !act_type %in% c(1, 4, 5, 6)) ~ 5,
      id_kc_pha == lead(id_kc_pha, 1) & !is.na(lead(id_kc_pha, 1)) &
        abs(act_date - lead(act_date, 1)) <= 31 &
        agency == "SHA" & lead(agency, 1) == "KCHA" &
        (geo_hash_clean == lead(geo_hash_clean, 1) | geo_blank == 1 | lead(port_out_sha, 1) == 1) & 
        !act_type %in% c(5, 6) ~ 5,
      id_kc_pha == lag(id_kc_pha, 1) & !is.na(lag(id_kc_pha, 1)) &
        abs(act_date - lag(act_date, 1)) <= 31 &
        agency == "SHA" & lag(agency, 1) == "KCHA" &
        (geo_hash_clean == lag(geo_hash_clean, 1) | geo_blank == 1 | lag(port_out_sha, 1) == 1) &
        ((act_type %in% c(5, 6) & lag(act_type, 1) %in% c(1, 4)) |
           !act_type %in% c(1, 4, 5, 6) |
           (act_type %in% c(5) & cost_pha == "WA002")) ~ 5,
      TRUE ~ 0)]
    # Pull out drop tracking and merge
    drop_temp <- pha_sort %>% select(row, drop)
    drop_track <- left_join(drop_track, drop_temp, by = "row") %>%
      mutate(drop = ifelse(!is.na(drop.x) & drop.x > 0, drop.x, drop.y)) %>%
      select(-drop.x, -drop.y)
    # Finish dropping rows
    pha_sort <- pha_sort[drop == 0 | is.na(drop)]
    
    dfsize2 <-  nrow(pha_sort)
    if (dfsize2 == dfsize) {
      break
    }
  }
  
  dfsize_head - nrow(pha_sort) # Track how many rows were dropped
  time_end <- Sys.time()
  print(paste0("Drop #5 took ", round(difftime(time_end, time_start, units = "secs"), 2), " secs"))
  
  
  ## Get rid of blank addresses when there is an address for the same date (droptype = 6) ----
  # NO LONGER within a given program/subtype/spec voucher etc.
  # Requiring an exact program match was leaving too many that should be dropped
  time_start <- Sys.time()
  dfsize_head <- nrow(pha_sort)
  repeat {
    dfsize <-  nrow(pha_sort)
    pha_sort[, drop := if_else(
      (id_kc_pha == lead(id_kc_pha, 1) & act_date == lead(act_date, 1) & geo_blank == 1 & lead(geo_blank, 1) != 1) |
        (id_kc_pha == lag(id_kc_pha, 1) & act_date == lag(act_date, 1) & geo_blank == 1 & lag(geo_blank, 1) != 1),
      6, 0)]
    # Pull out drop tracking and merge
    drop_temp <- pha_sort %>% select(row, drop)
    drop_track <- left_join(drop_track, drop_temp, by = "row") %>%
      mutate(drop = ifelse(!is.na(drop.x) & drop.x > 0, drop.x, drop.y)) %>%
      select(-drop.x, -drop.y)
    # Finish dropping rows
    pha_sort <- pha_sort[drop == 0 | is.na(drop)]
    
    dfsize2 <-  nrow(pha_sort)
    if (dfsize2 == dfsize) {
      break
    }
  }
  dfsize_head - nrow(pha_sort)
  time_end <- Sys.time()
  print(paste0("Drop #6 took ", round(difftime(time_end, time_start, units = "secs"), 2), " secs"))
  
  
  ## Different programs with the same or similar action date within the same agency (droptype = 7) ----
  # Does not apply to address changes in the same program (these will mostly be artifacts with the start/end date being the same)
  # SHA:
  #    - take row from newer data source if the same type of major program
  #    - prioritize HCV over PH (unless there is a single date when HCV switches to PH)
  #    - prioritizes tenant-based vouchers over collaborative housing
  #    - drop rows where the the program type is PORT IN (assume the person was absorbed into SHA
  #          (this will be mostly covered by taking the program that ran longest)
  #    - sometimes different programs had slightly different action dates so use a diff of > 31 days for SHA)
  #    - If slightly different prog, same max date, take last row
  # KCHA:
  #    - generally prioritize HCV over PH
  #    - however, if one program runs longer than the other, choose that 
  #    - sometimes different programs had slightly different action dates so use 
  #      diff of > 62 days for KCHA (had larger gaps than SHA))
  #    - If slightly different prog, same max date, take last row
  
  # First wave of cleaning
  time_start <- Sys.time()
  dfsize_head <- nrow(pha_sort)
  repeat {
    dfsize <-  nrow(pha_sort)
    setorder(pha_sort, id_kc_pha, act_date, agency_prog_concat)
    pha_sort[, drop := case_when(
      # SHA
      (id_kc_pha == lag(id_kc_pha, 1) & act_date - lag(act_date, 1) <= 31 &
         # The act_type restriction avoids dropping rows that are genuine prog 
         # switches but may miss some that should be dropped
         !((act_type %in% c(1, 4) & !lag(act_type, 1) %in% c(1, 4)) | 
             (!act_type %in% c(5, 6) & lag(act_type, 1) %in% c(5, 6))) &
         agency_prog_concat != lag(agency_prog_concat, 1) &
         agency == "SHA" & lag(agency, 1) == "SHA" &
         str_detect(pha_source, "ph") & str_detect(lag(pha_source, 1), "hcv")) |
        (id_kc_pha == lead(id_kc_pha, 1) & act_date - lead(act_date, 1) >= -31 &
           !((act_type %in% c(1, 4) & !lead(act_type, 1) %in% c(1, 4)) |
               (act_type %in% c(5, 6) & !lead(act_type, 1) %in% c(5, 6))) &
           agency_prog_concat != lead(agency_prog_concat, 1) &
           agency == "SHA" & lead(agency, 1) == "SHA" &
           str_detect(pha_source, "ph") < str_detect(lead(pha_source, 1), "hcv")) ~ 7,
      # KCHA (generally prioritizing HCV)
      (id_kc_pha == lag(id_kc_pha, 1) & act_date - lag(act_date, 1) <= 62 &
         !((act_type %in% c(1, 4) & !lag(act_type, 1) %in% c(1, 4)) |
             (!act_type %in% c(5, 6) & lag(act_type, 1) %in% c(5, 6))) &
         agency_prog_concat != lag(agency_prog_concat, 1) &
         agency == "KCHA" & lag(agency, 1) == "KCHA" &
         prog_type == "PH" & lag(prog_type, 1) %in% c("PBS8", "TBS8", "PORT") &
         max_date - lag(max_date, 1) <= 62) |
        (id_kc_pha == lead(id_kc_pha, 1) & act_date - lead(act_date, 1) >= -62 &
           !((act_type %in% c(1, 4) & !lead(act_type, 1) %in% c(1, 4)) |
               (act_type %in% c(5, 6) & !lead(act_type, 1) %in% c(5, 6))) &
           agency_prog_concat != lead(agency_prog_concat, 1) &
           agency == "KCHA" & lead(agency, 1) == "KCHA" &
           prog_type == "PH" &
           lead(prog_type, 1) %in% c("PBS8", "TBS8", "PORT") &
           max_date - lead(max_date, 1) <= 62) ~ 7,
      TRUE ~ 0
    )]
    # Pull out drop tracking and merge
    drop_temp <- pha_sort %>% select(row, drop)
    drop_track <- left_join(drop_track, drop_temp, by = "row") %>%
      mutate(drop = ifelse(!is.na(drop.x) & drop.x > 0, drop.x, drop.y)) %>%
      select(-drop.x, -drop.y)
    # Finish dropping rows
    pha_sort <- pha_sort[drop == 0 | is.na(drop)]
    
    dfsize2 <-  nrow(pha_sort)
    if (dfsize2 == dfsize) {
      break
    }
  }
  
  # Second wave of cleaning
  # Take program that ran until most recently (SHA and KCHA)
  # Max date should be 60+ days greater to count because the PHAs didn't
  #  always record the same exit date for the same actual exit
  repeat {
    dfsize <-  nrow(pha_sort)
    setorder(pha_sort, id_kc_pha, act_date, agency_prog_concat, max_date)
    pha_sort[, drop := case_when(
      (id_kc_pha == lag(id_kc_pha, 1) & act_date - lag(act_date, 1) <= 30 &
         !((act_type %in% c(1, 4) & !lag(act_type, 1) %in% c(1, 4)) |
             (!act_type %in% c(5, 6) & lag(act_type, 1) %in% c(5, 6))) &
         agency_prog_concat != lag(agency_prog_concat, 1) &
         agency == "SHA" & lag(agency, 1) == "SHA" &
         max_date - lag(max_date, 1) <= 0) |
        (id_kc_pha == lead(id_kc_pha, 1) & act_date == lead(act_date, 1) &
           !((act_type %in% c(1, 4) & !lead(act_type, 1) %in% c(1, 4)) |
               (act_type %in% c(5, 6) & !lead(act_type, 1) %in% c(5, 6))) &
           agency_prog_concat != lead(agency_prog_concat, 1) &
           agency == "SHA" & lead(agency, 1) == "SHA" &
           max_date - lead(max_date, 1) < 0) ~ 7,
      (id_kc_pha == lag(id_kc_pha, 1) & act_date - lag(act_date, 1) <= 62 &
         !((act_type %in% c(1, 4) & !lag(act_type, 1) %in% c(1, 4)) |
             (!act_type %in% c(5, 6) & lag(act_type, 1) %in% c(5, 6))) &
         agency_prog_concat != lag(agency_prog_concat, 1) &
         agency == "KCHA" & lag(agency, 1) == "KCHA" &
         max_date - lag(max_date, 1) <= 0) |
        (id_kc_pha == lead(id_kc_pha, 1) & act_date == lead(act_date, 1) &
           !((act_type %in% c(1, 4) & !lead(act_type, 1) %in% c(1, 4)) |
               (act_type %in% c(5, 61) & !lead(act_type, 1) %in% c(5, 6))) &
           agency_prog_concat != lead(agency_prog_concat, 1) &
           agency == "KCHA" & lead(agency, 1) == "KCHA" &
           max_date - lead(max_date, 1) < 0) ~ 7,
      TRUE ~ 0
    )]
    # Pull out drop tracking and merge
    drop_temp <- pha_sort %>% select(row, drop)
    drop_track <- left_join(drop_track, drop_temp, by = "row") %>%
      mutate(drop = ifelse(!is.na(drop.x) & drop.x > 0, drop.x, drop.y)) %>%
      select(-drop.x, -drop.y)
    # Finish dropping rows
    pha_sort <- pha_sort[drop == 0 | is.na(drop)]
    
    dfsize2 <-  nrow(pha_sort)
    if (dfsize2 == dfsize) {
      break
    }
  }
  dfsize_head - nrow(pha_sort)
  time_end <- Sys.time()
  print(paste0("Drop #7 took ", round(difftime(time_end, time_start, units = "secs"), 2), " secs"))
  
  
  ## Set up KCHA move outs ----
  # In the old KCHA system there was no record of when a household member moved
  # out but the household remained on the same subsidy. Need to look at the next
  # date in a program for each household and see if there is another certificate.
  # If so, take the mid-point between that individual's last action date and the
  # next date for the household as the EOP for that person. This section identifies
  # the next household action date.
  # Existing max_date is made from agency_prog_concat, geo_hash_clean, and cost_pha.
  # Need to just use agency_prog_concat here to avoid missing move outs
  pha_sort[, max_date2 := max(act_date, na.rm = T), by = .(id_kc_pha, agency_prog_concat)]
  
  # Pull out the maximum data for each person
  max_date <- pha_sort %>% distinct(id_kc_pha, hh_id_long, agency_prog_concat, max_date2)
  # ID which set of data is for the H-H
  hh_act_dates <- pha_sort %>%
    filter(ssn == hh_ssn | (is.na(hh_ssn) & lname == hh_lname & fname == hh_fname)) %>%
    distinct(hh_id_long, agency_prog_concat, act_date) %>%
    arrange(hh_id_long, act_date)
  # Join together and do some filtering
  act_dates_merge <- left_join(max_date, hh_act_dates, 
                               by = c("hh_id_long", "agency_prog_concat")) %>%
    arrange(hh_id_long, id_kc_pha, act_date) %>%
    filter(act_date > max_date2)
  # The first row in each group is the next date for a household after this 
  # individual's last date. If missing, the individual was still in the household.
  act_dates_merge <- act_dates_merge %>% 
    group_by(id_kc_pha, hh_id_long, agency_prog_concat) %>% 
    slice(., 1) %>%
    ungroup() %>%
    rename(next_hh_act = act_date)
  act_dates_merge <- setDT(act_dates_merge)
  # Join back with original data
  pha_sort <- merge(pha_sort, act_dates_merge,
                    by = c("hh_id_long", "id_kc_pha", "agency_prog_concat", "max_date2"), all.x = T)
  
  # Remove temp files
  rm(max_date)
  rm(hh_act_dates)
  rm(act_dates_merge)
  
  
  ## Different agencies with the same or similar action date (droptype = 8) ----
  # At this point in the row consolidation there are some residual rows recording port outs when they are also showing in the other PHA
  # Removing them will improve precision when deciding whether to count someone in SHA or KCHA
  time_start <- Sys.time()
  dfsize_head <- nrow(pha_sort)
  repeat {
    dfsize <-  nrow(pha_sort)
    setorder(pha_sort, id_kc_pha, act_date, agency_prog_concat)
    
    pha_sort[, drop := case_when(
      (id_kc_pha == lag(id_kc_pha, 1) & act_date - lag(act_date, 1) <= 62 &
         agency != lag(agency, 1) & !act_type %in% c(1, 4) &
         ((agency == "SHA" & port_out_sha == 1 & lag(port_out_sha, 1) == 1) |
            (agency == "KCHA" & port_out_kcha == 1 &  lag(port_out_kcha, 1) == 1))) |
        (id_kc_pha == lead(id_kc_pha, 1) & !is.na(lead(id_kc_pha, 1)) & act_date - lead(act_date, 1) >= -62 &
           agency != lead(agency, 1) & !lead(act_type, 1) %in% c(1, 4) &
           ((agency == "SHA" & port_out_sha == 1 & lead(port_out_sha, 1) == 1) |
              (agency == "KCHA" & port_out_kcha == 1 & lead(port_out_kcha, 1) == 1))) ~ 8,
      (id_kc_pha == lag(id_kc_pha, 1) & id_kc_pha == lead(id_kc_pha, 1) & !is.na(lag(id_kc_pha, 1)) & !is.na(lead(id_kc_pha, 1)) &
         agency != lag(agency, 1) & agency != lead(agency, 1) &
         !act_type %in% c(1, 4) & 
         ((agency == "SHA" & port_out_sha == 1 & 
             lag(port_out_sha, 1) == 1 & lead(port_out_sha, 1) == 1) |
            (agency == "KCHA" & port_out_kcha == 1 & 
               lag(port_out_kcha, 1) == 1) & lead(port_out_kcha, 1) == 1)) ~ 8,
      TRUE ~ 0
    )]
    # Pull out drop tracking and merge
    drop_temp <- pha_sort %>% select(row, drop)
    drop_track <- left_join(drop_track, drop_temp, by = "row") %>%
      mutate(drop = ifelse(!is.na(drop.x) & drop.x > 0, drop.x, drop.y)) %>%
      select(-drop.x, -drop.y)
    # Finish dropping rows
    pha_sort <- pha_sort[drop == 0 | is.na(drop)]
    
    dfsize2 <-  nrow(pha_sort)
    if (dfsize2 == dfsize) {
      break
    }
  }
  
  # There are also some non-port agency switches that need fixing
  setorder(pha_sort, id_kc_pha, act_date, agency_prog_concat)
  pha_sort[, drop := ifelse(
    id_kc_pha == lag(id_kc_pha, 1) & act_date - lag(act_date, 1) <= 62 &
      agency != lag(agency, 1) &
      act_type %in% c(5, 6) & lag(act_type, 1) %in% c(1, 4),
    8, 0)]
  # Pull out drop tracking and merge
  drop_temp <- pha_sort %>% select(row, drop)
  drop_track <- left_join(drop_track, drop_temp, by = "row") %>%
    mutate(drop = ifelse(!is.na(drop.x) & drop.x > 0, drop.x, drop.y)) %>%
    select(-drop.x, -drop.y)
  # Finish dropping rows
  pha_sort <- pha_sort[drop == 0 | is.na(drop)]
  
  dfsize_head - nrow(pha_sort)
  time_end <- Sys.time()
  print(paste0("Drop #8 took ", round(difftime(time_end, time_start, units = "secs"), 2), " secs"))
  
  
  ## Remove auto-generated recertifications that are overwriting EOPs (droptype = 9) ----
  # It looks like in some of the older KCHA data, recertifications were set to 
  # automatically generate on a given date, even if the household had already 
  # ended their participation at KCHA. Need to remove these.
  # NB. Don't exclude if agency is KCHA + gap of >1 years + non-missing address +
  #     address is NOT the same (these look like the person re-entered housing)
  # For SHA there are a few bogus actions after an EOP but mostly the issue is
  # an erroneous EOP. Need to set up second drop logic to catch those. Don't drop 
  # if agency is SHA + address is different but not missing 
  # (these look like the person re-entered housing/moved)
  time_start <- Sys.time()
  dfsize_head <- nrow(pha_sort)
  setorder(pha_sort, id_kc_pha, act_date, agency_prog_concat)
  pha_sort[, drop := case_when(
    id_kc_pha == lag(id_kc_pha, 1) & (id_kc_pha != lead(id_kc_pha, 1) | is.na(lead(id_kc_pha, 1))) &
      agency == lag(agency, 1) & 
      lag(act_type, 1) == 6 & !act_type %in% c(1, 4) &
      # Slightly separate rules for SHA and KCHA
      ((agency == "KCHA" & 
          (act_date - lag(act_date, 1) <= 365 |
             (act_date - lag(act_date, 1) > 365 &
                (geo_blank == 1 | geo_hash_clean == lag(geo_hash_clean, 1))))) |
         (agency == "SHA" & geo_blank == 1)) ~ 9,
    # For erroneous EOPs in SHA data, drop the row with the EOP in it
    id_kc_pha == lead(id_kc_pha, 1) & act_type == 6 & agency == "SHA" &
      geo_blank != 1 &
      geo_hash_clean == lead(geo_hash_clean, 1) & 
      lead(geo_blank, 1) != 1 ~ 9,
    TRUE ~ 0
  )]
  # Pull out drop tracking and merge
  drop_temp <- pha_sort %>% select(row, drop)
  drop_track <- left_join(drop_track, drop_temp, by = "row") %>%
    mutate(drop = ifelse(!is.na(drop.x) & drop.x > 0, drop.x, drop.y)) %>%
    select(-drop.x, -drop.y)
  # Finish dropping rows
  pha_sort <- pha_sort[drop == 0 | is.na(drop)]
  dfsize_head - nrow(pha_sort)
  time_end <- Sys.time()
  print(paste0("Drop #9 took ", round(difftime(time_end, time_start, units = "secs"), 2), " secs"))
  
  
  ## Remove annual reexaminations/intermediate visits within a given address + program (droptype = 10) ----
  # Want to avoid capturing the first or last row for a person at a given address
  # NB: now sorting by date before agency in order to reduce chances of 
  # erroneously treating two separate periods at an address as one
  time_start <- Sys.time()
  dfsize_head <- nrow(pha_sort)
  setorder(pha_sort, id_kc_pha, act_date, agency_prog_concat)
  pha_sort[, drop := if_else(
    id_kc_pha == lag(id_kc_pha, 1) & id_kc_pha == lead(id_kc_pha, 1) & !is.na(lag(id_kc_pha, 1)) & !is.na(lead(id_kc_pha, 1)) &
      geo_hash_clean == lag(geo_hash_clean, 1) & geo_hash_clean == lead(geo_hash_clean, 1) &
      # Checking for prog_type and agency matches
      agency_prog_concat == lag(agency_prog_concat, 1) & agency_prog_concat == lead(agency_prog_concat, 1) &
      # Check that port outs are in the same place (or one is missing)
      # Don't drop if cost_pha = row above but row below is missing 
      # (indicates when voucher was absorbed by PHA)
      # unless the row after that also matches current cost_pha (indicates data entry issues)
      ((cost_pha == lag(cost_pha, 1) & cost_pha == lead(cost_pha, 1)) |
         (lag(cost_pha, 1) == "" & cost_pha == lead(cost_pha, 1)) |
         (cost_pha == "" & lag(cost_pha, 1) == lead(cost_pha, 1)) |
         (cost_pha == lag(cost_pha, 1) & lead(cost_pha, 1) == "" &
            cost_pha == lead(cost_pha, 2) &
            id_kc_pha == lead(id_kc_pha, 2) & geo_hash_clean == lead(geo_hash_clean, 2) &
            agency_prog_concat == lead(agency_prog_concat, 2))) &
      # Check that a person didn't exit the program then come in again at the same address
      !(act_type %in% c(1, 4, 5, 6)),
    10, 0)]
  # Pull out drop tracking and merge
  drop_temp <- pha_sort %>% select(row, drop)
  drop_track <- left_join(drop_track, drop_temp, by = "row") %>%
    mutate(drop = ifelse(!is.na(drop.x) & drop.x > 0, drop.x, drop.y)) %>%
    select(-drop.x, -drop.y)
  # Finish dropping rows
  pha_sort <- pha_sort[drop == 0 | is.na(drop)]
  dfsize_head - nrow(pha_sort)
  time_end <- Sys.time()
  print(paste0("Drop #10 took ", round(difftime(time_end, time_start, units = "secs"), 2), " secs"))
  
  
  
  ## Set up the number of years between reexaminations ----
  # Logic is somewhat complicated
  # Note that a person is considered an adult only at the action date/start date
  # (i.e., if they turn 18 between examinations, the new age is not considered until the next exam date)
  #
  # SHA low-income public housing (LIPH)
  #   Annual review only (1-year gap) if ANY of the following are true:
  #     - date < 2016
  #     - any ADULT (18+ years) household member not elderly or disabled (excluding live-in attendants)
  #     - homeworks properties with combo LIPH/tax credit funding (IDs: 10-12, 14, 16, 20 22, 24-30, 32-36, 40, 46)
  #   3-year gap if ALL of the following are true:
  #     - 100% adult household members are elderly and/or disabled
  #     - date >= 2016
  # SHA voucher holders (HCV)
  #   Annual review (1-year gap) if ANY of the following are true:
  #     - date < 2010
  #     - household has a mod rehab voucher (increment starts with 'M')
  #     - date > 2010 and < 2013 AND < 100% household income from a fixed source (pension, Social Security, SSI, veteran’s benefits)
  #     - date > 2013 AND any ADULT (18+ years) household member not elderly or disabled (excluding live-in attendants)
  #   3-year gap if ANY of the following are true:
  #     - date >= 2010 and < 2013 AND 100% household income from a fixed source (pension, Social Security, SSI, veteran’s benefits) AND
  #           not a mod rehab voucher
  #     - date >= 2013 AND 100% adult (18+ years) household members are elderly and/or disabled (excluding live-in attendants) 
  #           AND not a mod rehab voucher
  #
  # KCHA LIPH
  #   Annual review only (1-year gap) if ANY of the following are true:
  #     - date < 2008-06-01
  #   EASY status (3-year gap) if ALL of the following is true (NB: the reality is more complicated, this is a simplified approach):
  #     - date >= 2008-06-01
  # KCHA HCV
  #   Annual review only (1-year gap) if ANY of the following are true:
  #     - date < 2008-06-01
  #     - date < 2011-06-01 AND < 100% adult household members are elderly and/or disabled (excluding live-in attendants)
  #     - date < 2011-06-01 AND < 90% household income from a fixed source
  #   WIN status (2-year-gap) if ALL of the following are true:
  #     - date >= 2011-06-01
  #     - < 100% adult household members are elderly and/or disabled (excluding live-in attendants) OR < 90% household income from a fixed source
  #   EASY (3-year-gap) status if ALL of the following are true:
  #     - date >= 2008-06-01
  #     - 100% adult household members are elderly and/or disabled (excluding live-in attendants)
  #     - no source of income OR at least 90% of household income from a fixed source
  
  # NB. At this point there are people with the same address in the same program but with multiple income codes (~14k instances of this)
  # This shouldn't matter once the number of years between inspections is applied to the household
  # After that point, the income variable is not used so can be dropped
  
  
  # Join back to main data and make relevant variables
  pha_sort[, ':=' (
    # Set up Homeworks property identifier and income codes (only applies to SHA)
    homeworks = ifelse(agency == "SHA" & as.numeric(property_id) %in% 
                         c(10:12, 14, 16, 20, 22, 24:30, 32:36, 40, 46), 1, 0),
    # Set up income (SHA already done in SHA data merging process so only KCHA)
    inc_fixed = case_when(agency == "KCHA" & 
                            (((hh_inc_fixed > 0 | hh_inc_vary > 0) & 
                                hh_inc_fixed / (hh_inc_fixed + hh_inc_vary) >= 0.9) | 
                               (hh_inc_fixed == 0 & hh_inc_vary == 0)) ~ 1,
                          agency == "KCHA" & 
                            (hh_inc_fixed > 0 | hh_inc_vary > 0) & 
                            hh_inc_fixed / (hh_inc_fixed + hh_inc_vary) < 0.9 ~ 0,
                          TRUE ~ hh_inc_fixed)
  )]
  
  
  # Look at number of years for each individual
  pha_sort[, add_yr_temp := case_when(
    # SHA public housing
    agency == "SHA" & prog_type == "PH" & adult == 1 & relcode != "L" & 
      act_date >= "2016-01-01" & (senior == 1 | disability == 1) & homeworks == 0 ~ 3,
    agency == "SHA" & prog_type == "PH" & adult == 1 & relcode != "L" &
      (act_date < "2016-01-01" | (senior == 0 & disability == 0) | homeworks == 1) ~ 1,
    # SHA HCV
    agency == "SHA" & prog_type != "PH" & 
      vouch_type_final != "MOD REHAB" & adult == 1 & relcode != "L" &
      ((act_date >= "2010-01-01" & act_date < "2013-01-01" & inc_fixed == 1) |
         (act_date >= "2013-01-01" & (senior == 1 | disability == 1))) ~ 3,
    agency == "SHA" & prog_type != "PH" & adult == 1 & relcode != "L" &
      (act_date < "2010-01-01" | vouch_type_final == "MOD REHAB" |
         (act_date >= "2010-01-01" & act_date < "2013-01-01" & inc_fixed == 0) |
         (act_date >= "2013-01-01" & senior == 0 & disability == 0)) ~ 1,
    # KCHA public housing
    agency == "KCHA" & prog_type == "PH" & adult == 1 & relcode != "L" & 
      act_date >= "2008-06-01" ~ 3,
    # KCHA HCV
    agency == "KCHA" & prog_type != "PH" & adult == 1 & 
      relcode != "L" & act_date >= "2008-06-01" & (senior == 1 | disability == 1) & 
      (inc_fixed == 1 | (hh_inc_fixed == 0 & hh_inc_vary == 0)) ~ 3,
    agency == "KCHA" & prog_type != "PH" & adult == 1 & relcode != "L" & 
      act_date >= "2011-06-01" & ((senior == 0 & disability == 0) | 
                                    (inc_fixed == 0 & (hh_inc_fixed > 0 | 
                                                         hh_inc_vary > 0))) ~ 2,
    # KCHA default
    agency == "KCHA" & adult == 1 & relcode != "L" & act_date < "2011-06-01" ~ 1,
    TRUE ~ NA_real_
  )]
  
  # Now apply to entire household (only NAs in add_yr_temp should be children or live-in attendants)
  pha_sort[, add_yr := min(add_yr_temp, na.rm = TRUE), by = .(hh_id_tmp, act_date)]
  # A few cleanup errors lead to some households with no add_yr (shows as infinity) so set to 1 for now
  pha_sort[, add_yr := ifelse(add_yr > 3, 1, add_yr)]
  
  
  
  ## Create start and end dates for a person at that address/progam/agency ----
  setorder(pha_sort, id_kc_pha, act_date, agency_prog_concat)
  pha_sort[, ':=' (
    # First row for a person = act_date (admit_dates stretch back too far for port ins)
    # Any change in agency/program/address/PHA billed = act date
    from_date = as.Date(ifelse(id_kc_pha != lag(id_kc_pha, 1) | is.na(lag(id_kc_pha, 1)) | 
                                 agency_prog_concat != lag(agency_prog_concat, 1) |
                                 geo_hash_clean != lag(geo_hash_clean, 1) | cost_pha != lag(cost_pha, 1),
                               act_date, NA), origin = "1970-01-01"),
    # Last row for a person or change in agency/program = 
    #   exit date or today's date or act_date + 1-3 years (depending on agency, age, and disability)
    # OR mid-point between last date and next household action date if it looks like
    #    the person has moved out (whichever is smallest of the two)
    # Other rows where that is the person's last row at that address/PHA billed but same agency/prog = act_date at next address - 1 day
    # Unless act_date is the same as from_date (e.g., because of different programs), then act_date 
    to_date = as.Date(
      case_when(
        act_type == 5 | act_type == 6 ~  act_date,
        id_kc_pha != lead(id_kc_pha, 1) | is.na(lead(id_kc_pha, 1 )) |
          agency_prog_concat != lead(agency_prog_concat, 1) |
          cost_pha != lead(cost_pha, 1) ~ pmin(today(), act_date + dyears(add_yr),
                                               act_date + ((next_hh_act - act_date) / 2), na.rm = TRUE),
        geo_hash_clean != lead(geo_hash_clean, 1) & act_date != lead(act_date, 1) ~ lead(act_date, 1) - 1,
        geo_hash_clean != lead(geo_hash_clean, 1) & act_date == lead(act_date, 1) ~ lead(act_date, 1))
      , origin = "1970-01-01")
  )]
  
  time_end <- Sys.time()
  print(paste0("Setting up years between recertifications took ", 
               round(difftime(time_end, time_start, units = "secs"), 2), " secs"))
  
  
  ## Collapse rows to have a single line per person per address per time there (droptype = 11) ----
  # There are a few rows left with no start or end date 
  # (because of the logic around billed PHA and the data issues in that field)
  time_start <- Sys.time()
  dfsize_head <- nrow(pha_sort)
  
  setorder(pha_sort, id_kc_pha, act_date, agency_prog_concat)
  # Bring start and end dates onto a single line per address/program
  pha_sort[, drop := ifelse(is.na(from_date) & is.na(to_date), 11, 0)]
  
  # Pull out drop tracking and merge
  drop_temp <- pha_sort %>% select(row, drop)
  drop_track <- left_join(drop_track, drop_temp, by = "row") %>%
    mutate(drop = ifelse(!is.na(drop.x) & drop.x > 0, drop.x, drop.y)) %>%
    select(-drop.x, -drop.y)
  # Finish dropping rows
  pha_sort <- pha_sort[drop == 0 | is.na(drop)]
  
  
  # Then get start and end dates onto a single line
  # NB. Important to retain the order from the code above
  setorder(pha_sort, id_kc_pha, act_date, agency_prog_concat)
  pha_sort[, to_date := as.Date(ifelse(is.na(to_date), lead(to_date, 1), to_date), 
                                origin = "1970-01-01")]
  pha_sort[, drop := ifelse(is.na(from_date) | is.na(to_date), 11, 0)]
  
  # Pull out drop tracking and merge
  drop_temp <- pha_sort %>% select(row, drop)
  drop_track <- left_join(drop_track, drop_temp, by = "row") %>%
    mutate(drop = ifelse(!is.na(drop.x) & drop.x > 0, drop.x, drop.y)) %>%
    select(-drop.x, -drop.y)
  # Finish dropping rows
  pha_sort <- pha_sort[drop == 0 | is.na(drop)]
  dfsize_head - nrow(pha_sort)
  time_end <- Sys.time()
  print(paste0("Drop #11 took ", round(difftime(time_end, time_start, units = "secs"), 2), " secs"))
  
  
  ## Refine port labels ----
  ### Infer ports by comparing agencies and to_dates
  # NB. Needs more inspection and checking
  # pha_sort <- pha_sort %>%
  #   arrange(id_kc_pha, from_date, to_date, agency_prog_concat) %>%
  #   mutate(
  #     port_out_sha = ifelse(id_kc_pha == lead(id_kc_pha, 1) & !is.na(lead(id_kc_pha, 1)) & agency == "SHA" & lead(agency, 1) == "KCHA" &
  #                             to_date >= lead(from_date, 1) & lead(act_type, 1) != 6, 1, port_out_sha),
  #     port_out_kcha = ifelse(id_kc_pha == lead(id_kc_pha, 1) & !is.na(lead(id_kc_pha, 1)) & agency == "KCHA" & lead(agency, 1) == "SHA" &
  #                             to_date >= lead(from_date, 1) & lead(act_type, 1) != 6, 1, port_out_kcha)
  #   )
  
  ### Update port information across multiple rows within the same program
  ### Fill in any missing port out fields when the person switched between SHA and KCHA or vice versa
  pha_sort[, ':=' (
    port_out_kcha = ifelse(agency == "SHA" & port_in == 1 & cost_pha == "WA002", 1, port_out_kcha),
    port_out_sha = ifelse(agency == "KCHA" & port_in == 1 & cost_pha == "WA001", 1, port_out_sha)
  )]
  
  ## Rows where from_date = to_date and also from_date = the next row's from_date (often same program) (droptype = 12) ----
  # Use the same logic as droptype 6 to decide which row to keep
  time_start <- Sys.time()
  dfsize_head <- nrow(pha_sort)
  setorder(pha_sort, id_kc_pha, from_date, to_date, agency_prog_concat)
  # First capture pairs where only one row has the same start and end date
  pha_sort[, drop := ifelse(id_kc_pha == lead(id_kc_pha, 1) & from_date == to_date &
                              from_date == lead(from_date, 1) &
                              from_date != lead(to_date, 1), 12, 0)]
  pha_sort[, drop := ifelse(id_kc_pha == lag(id_kc_pha, 1) & from_date == to_date &
                              from_date == lag(from_date, 1) &
                              from_date != lag(to_date, 1), 12, drop)]
  # Then find pairs where both rows have the same start and end dates 
  # (use logic from droptype 05 to decide)
  # SHA
  pha_sort[, drop := ifelse(
    (drop == 0 & lead(drop, 1) == 0 & id_kc_pha == lead(id_kc_pha, 1) & 
       from_date == to_date & from_date == lead(from_date, 1) &
       from_date == lead(to_date, 1) &
       agency == "SHA" & lead(agency, 1) == "SHA" &
       ((pha_source < lead(pha_source, 1) & abs(max_date - lead(max_date, 1)) <= 31) | 
          (pha_source == lead(pha_source, 1) & max_date - lead(max_date, 1) < -31))) |
      (drop == 0 & lag(drop, 1) == 0 & id_kc_pha == lag(id_kc_pha, 1) &
         from_date == to_date & from_date == lag(from_date, 1) &
         from_date == lag(to_date, 1) &
         agency == "SHA" & lag(agency, 1) == "SHA" & 
         ((pha_source < lag(pha_source, 1) & abs(max_date - lag(max_date, 1)) <= 31) | 
            (pha_source == lag(pha_source, 1) & max_date - lag(max_date, 1) < -31))),
    12, drop)]
  # KCHA
  pha_sort[, drop := ifelse(
    (drop == 0 & lead(drop, 1) == 0 & id_kc_pha == lead(id_kc_pha, 1) & 
       from_date == to_date & from_date == lead(from_date, 1) &
       from_date == lead(to_date, 1) &
       agency == "KCHA" & lead(agency, 1) == "KCHA" & 
       ((prog_type == "PH" & lead(prog_type, 1) %in% c("PBS8", "TBS8", "PORT") & 
           abs(max_date - lead(max_date, 1)) <= 62) |
          max_date - lead(max_date, 1) > 62)) | (drop == 0 & lag(drop, 1) == 0 & id_kc_pha == lag(id_kc_pha, 1) & 
                                                   from_date == to_date & from_date == lag(from_date, 1) & from_date == lag(to_date, 1) &
                                                   agency == "KCHA" & lag(agency, 1) == "KCHA" & 
                                                   ((prog_type == "PH" & lag(prog_type, 1) %in% c("PBS8", "TBS8", "PORT") & 
                                                       abs(max_date - lag(max_date, 1) <= 62)) | max_date - lag(max_date, 1) > 62)),
    12, drop)]
  # If there are still pairs within a PHA, keep the program that immediately preceded this pair
  # SHA
  pha_sort[, drop := ifelse(
    (drop == 0 & lead(drop, 1) == 0 & id_kc_pha == lead(id_kc_pha, 1) & 
       from_date == to_date & from_date == lead(from_date, 1) & from_date == lead(to_date, 1) &
       agency == "SHA" & lead(agency, 1) == "SHA" & pha_source == lead(pha_source, 1) &
       abs(max_date - lead(max_date, 1)) <= 31 & agency_prog_concat != lag(agency_prog_concat, 1)) |
      (drop == 0 & id_kc_pha == lag(id_kc_pha, 1) & from_date == to_date & 
         from_date == lag(from_date, 1) & from_date == lag(to_date, 1) &
         agency == "SHA" & lag(agency, 1) == "SHA" & pha_source == lag(pha_source, 1) &
         abs(max_date - lead(max_date, 1)) <= 31 & agency_prog_concat != lag(agency_prog_concat, 2)),
    12, drop)]
  # KCHA
  pha_sort[, drop := ifelse(
    (drop == 0 & id_kc_pha == lead(id_kc_pha, 1) & from_date == to_date &
       from_date == lead(from_date, 1) & from_date == lead(to_date, 1) &
       agency == "KCHA" & lead(agency, 1) == "KCHA" & subsidy_type == lead(subsidy_type, 1) &
       abs(max_date - lead(max_date, 1)) <= 62 & agency_prog_concat != lag(agency_prog_concat, 1)) |
      (drop == 0 & lag(drop, 1) == 0 & id_kc_pha == lag(id_kc_pha, 1) & 
         from_date == to_date & from_date == lag(from_date, 1) & from_date == lag(to_date, 1) & 
         agency == "KCHA" & lag(agency, 1) == "KCHA" &
         subsidy_type == lag(subsidy_type, 1) & abs(max_date - lead(max_date, 1)) <= 62 &
         agency_prog_concat != lag(agency_prog_concat, 2)),
    12, drop)]
  # If the pairs come from different agencies, keep the PHA that immediately 
  #   preceded this pair or the one with the latest max date
  pha_sort[, drop := ifelse(
    (drop == 0 & lead(drop, 1) == 0 & id_kc_pha == lead(id_kc_pha, 1) & 
       from_date == to_date & from_date == lead(from_date, 1) & from_date == lead(to_date, 1) &
       agency != lead(agency, 1) & agency != lag(agency, 1)) |
      (drop == 0 & lag(drop, 1) == 0 & id_kc_pha == lag(id_kc_pha, 1) &
         from_date == to_date & from_date == lag(from_date, 1) & from_date == lag(to_date, 1) &
         agency != lag(agency, 1) & agency != lag(agency, 2)),
    12, drop)]
  # There are ~50 pairs left that don't fit anything else, drop the second row
  pha_sort[, drop := ifelse(
    drop == 0 & lag(drop, 1) == 0 & id_kc_pha == lag(id_kc_pha, 1) & 
      from_date == to_date & from_date == lag(from_date, 1) & from_date == lag(to_date, 1),
    12, drop)]
  
  # Pull out drop tracking and merge
  drop_temp <- pha_sort %>% select(row, drop)
  drop_track <- left_join(drop_track, drop_temp, by = "row") %>%
    mutate(drop = ifelse(!is.na(drop.x) & drop.x > 0, drop.x, drop.y)) %>%
    select(-drop.x, -drop.y)
  # Finish dropping rows
  pha_sort <- pha_sort[drop == 0 | is.na(drop)]
  dfsize_head - nrow(pha_sort)
  time_end <- Sys.time()
  print(paste0("Drop #12 took ", round(difftime(time_end, time_start, units = "secs"), 2), " secs"))
  
  
  ## Deal with overlapping program/agency dates (droptype == 13) ----
  time_start <- Sys.time()
  ### First find rows with identical start and end dates
  dfsize_head <- nrow(pha_sort)
  setorder(pha_sort, id_kc_pha, from_date, to_date, agency_prog_concat)
  # Keep port in row
  pha_sort[, drop := ifelse(
    (id_kc_pha == lead(id_kc_pha, 1) & !is.na(lead(id_kc_pha, 1)) &
       from_date == lead(from_date, 1) & to_date == lead(to_date, 1) &
       port_in == 0 & lead(port_in, 1) == 1) |
      (id_kc_pha == lag(id_kc_pha, 1) & !is.na(lag(id_kc_pha, 1)) &
         from_date == lag(from_date, 1) & to_date == lag(to_date, 1) &
         port_in == 0 & lag(port_in, 1) == 1),
    13, 0)]
  # If both or neither rows are port in, keep the max date
  pha_sort[, drop := ifelse(
    (drop == 0 & lead(drop, 1) == 0 & id_kc_pha == lead(id_kc_pha, 1) & !is.na(lead(id_kc_pha, 1)) &
       from_date == lead(from_date, 1) & to_date == lead(to_date, 1) &
       max_date < lead(max_date, 1)) |
      (drop == 0 & lag(drop, 1) == 0 & id_kc_pha == lag(id_kc_pha, 1) & !is.na(lag(id_kc_pha, 1)) &
         from_date == lag(from_date, 1) & to_date == lag(to_date, 1) &
         max_date < lag(max_date, 1)),
    13, drop)]
  # If max_dates and ports are the same, keep any special type programs
  pha_sort[, drop := ifelse(
    (drop == 0 & lead(drop, 1) == 0 & id_kc_pha == lead(id_kc_pha, 1) & !is.na(lead(id_kc_pha, 1)) &
       from_date == lead(from_date, 1) & to_date == lead(to_date, 1) &
       vouch_type_final != "" & !is.na(vouch_type_final) &
       (lead(vouch_type_final, 1) == "" | is.na(lead(vouch_type_final, 1)))) |
      (drop == 0 & lag(drop, 1) == 0 & id_kc_pha == lag(id_kc_pha, 1) & !is.na(lag(id_kc_pha, 1)) &
         from_date == lag(from_date, 1) & to_date == lag(to_date, 1) &
         vouch_type_final != "" & !is.na(vouch_type_final) &
         (lag(vouch_type_final, 1) == "" | is.na(lag(vouch_type_final, 1)))),
    13, drop)]
  # For the remaining ~84 pairs, drop the second row
  pha_sort[, drop := ifelse(
    drop == 0 & lag(drop, 1) == 0 & id_kc_pha == lag(id_kc_pha, 1) & !is.na(lag(id_kc_pha, 1)) &
      from_date == lag(from_date, 1) & to_date == lag(to_date, 1), 13, drop)]
  
  
  ### Then find rows with different start dates but the overlap is due to an exit (act_type = 5 or 6 and has the same start/end date)
  # Most rows exist because sometimes a person starts in a new program/agency before they are closed out of the previous one
  # Be sure not to capture legitimate exits where the program is the same but the address was missing in the exit row
  # NB. Sometimes the addresses are the same and the programs are slightly different
  # Can check up two rows to see
  setorder(pha_sort, id_kc_pha, from_date, to_date, agency_prog_concat)
  pha_sort[, drop := ifelse(
    id_kc_pha == lag(id_kc_pha, 1) & !is.na(lag(id_kc_pha, 1)) &
      lag(to_date, 1) >= from_date & from_date == to_date & act_type %in% c(5, 6) &
      !((agency_prog_concat == lag(agency_prog_concat, 1) & geo_blank == 1) | 
          geo_hash_clean == lag(geo_hash_clean, 1) |
          (geo_blank == 1 & agency == "KCHA" & 
             ((prog_type == "TBS8" & lag(prog_type, 1) == "PORT") | prog_type == lag(prog_type, 1)))
      ),
    13, drop)]
  
  
  # Pull out drop tracking and merge
  drop_temp <- pha_sort %>% select(row, drop)
  drop_track <- left_join(drop_track, drop_temp, by = "row") %>%
    mutate(drop = ifelse(!is.na(drop.x) & drop.x > 0, drop.x, drop.y)) %>%
    select(-drop.x, -drop.y)
  # Finish dropping rows
  pha_sort <- pha_sort[drop == 0 | is.na(drop)]
  dfsize_head - nrow(pha_sort)
  time_end <- Sys.time()
  print(paste0("Drop #13 took ", round(difftime(time_end, time_start, units = "secs"), 2), " secs"))
  
  
  ### Truncate overlapping dates
  # Assume that most recent program/agency is the one to count 
  # (will be biased to the second one alphabetically when start dates are the same, if any remain)
  setorder(pha_sort, id_kc_pha, from_date, to_date, agency_prog_concat)
  # Make a note of which rows were truncated
  pha_sort[, truncated := ifelse(
    id_kc_pha == lead(id_kc_pha, 1) & !is.na(lead(id_kc_pha, 1)) & to_date >= lead(from_date, 1), 1, 0)]
  # Now truncate
  pha_sort[, to_date := as.Date(
    case_when(
      # If the start dates aren't the same, use next start date - 1 day
      id_kc_pha == lead(id_kc_pha, 1) & !is.na(lead(id_kc_pha, 1)) & to_date >= lead(from_date, 1) & 
        from_date != lead(from_date, 1) ~ lead(from_date, 1) - 1,
      #  If the start dates are the same, set the first end date equal to the start date 
      # (avoids having negative time in housing but does lead to duplicate rows for a person on a given date)
      id_kc_pha == lead(id_kc_pha, 1) & !is.na(lead(id_kc_pha, 1)) & to_date >= lead(from_date, 1) & 
        from_date == lead(from_date, 1) ~ from_date,
      TRUE ~ to_date
    ), origin = "1970-01-01")]
  
  
  # See how many rows were affected
  sum(pha_sort$truncated, na.rm = T)
  
  
  
  # REGENERATE HEAD OF HOUSEHOLD ----
  # Use the HH most recently associated with that time period
  
  
  test <- copy(pha_sort)
  test[, `:=` (
    mbr_num_min = NULL, hh_id_kc_pha = NULL, hh_ssn = NULL, hh_lname = NULL,
    hh_fname = NULL, hh_flag = NULL, act_date = NULL
  )]
  
  test_hh <- copy(hh_final2)
  test_hh[, hh_cnt := uniqueN(hh_id_kc_pha), by = .(id_kc_pha)]
  
  
  test2 <- test[test_hh[!is.na(act_date), .(agency, id_kc_pha, act_date, mbr_num_min, hh_id_kc_pha, hh_ssn, hh_lname, hh_fname, hh_flag)],
                on = .(id_kc_pha = id_kc_pha, from_date <= act_date, to_date >= act_date),
                mult = "last", nomatch = 0]
  
  
  test2 <- hh_final2[test, 
                   .(agency, id_kc_pha, x.act_date, from_date, to_date, 
                     hh_id_kc_pha, hh_ssn, hh_lname, hh_fname, hh_flag),
                   on = .(agency, id_kc_pha, 
                          act_date >= from_date, act_date <= to_date),
                   mult = "all"]
  setnames(test2, "x.act_date", "act_date")
  test2[, max_act_date := max(act_date), by = .(id_kc_pha, from_date)]
  test2 <- test2[act_date == max_act_date]
  
  test2[, from_date_cnt := .N, by = .(id_kc_pha, from_date)]
  test2 %>% count(from_date_cnt)
  test2 %>% filter(from_date_cnt > 1) %>% head()
  
  
  
  
  test[, c("hh_id_kc_pha", "hh_ssn", "hh_lname", "hh_fname", "hh_flag") := # Assign the below result to new columns in dtgrouped2
         test[test_hh, # join
                   .(hh_id_kc_pha, hh_ssn, hh_lname, hh_fname, hh_flag), # get the column you need
                   on = .(id_kc_pha = id_kc_pha, # join conditions
                          from_date <= act_date, 
                          to_date >= act_date), 
                   mult = "last"]] # get always the latest EndDate
  
  
  test2 <- test_hh[!is.na(act_date), c("from_date", "to_date") := # Assign the below result to new columns in dtgrouped2
            test[test_hh, # join
                      .(from_date, to_date), # get the column you need
              on = .(id_kc_pha = id_kc_pha, # join conditions
                     from_date <= act_date, 
                     to_date >= act_date), 
              mult = "last"]] # get always the latest EndDate
  
  
  test3 <- test[test_hh, # join
                # .(from_date, to_date), # get the column you need
                on = .(id_kc_pha, # join conditions
                       from_date <= act_date, 
                       to_date >= act_date), 
                # mult = "last", 
                nomatch = 0]
  
  test2 %>% filter(id_kc_pha == "6ms5k28qis") %>% select(id_kc_pha, act_date, hh_id_kc_pha, from_date, to_date)
  test2 %>% filter(id_kc_pha == "mlgjp9urj7") %>% select(id_kc_pha, act_date, hh_id_kc_pha, from_date, to_date)
  
  
  test2[, max_act_date := max(act_date, na.rm = T), by = c("id_kc_pha", "from_date", "to_date")]
  
  
  # FINALIZE TABLE ----
  pha_timevar <- pha_sort
  
  ## Coverage time ----
  setorder(pha_timevar, id_kc_pha, from_date, to_date)
  pha_timevar[, ':=' (
    cov_time = interval(start = from_date, end = to_date) / ddays(1) + 1,
    gap = case_when(is.na(lag(id_kc_pha, 1)) | id_kc_pha != lag(id_kc_pha, 1) ~ 0L,
                    id_kc_pha == lag(id_kc_pha, 1) & from_date - lag(to_date, 1) <= 62 ~ 0L,
                    from_date - lag(to_date, 1) > 62 ~ 1L,
                    TRUE ~ NA_integer_))]
  # Find the number of unique periods a person was in housing
  pha_timevar[, period := rowid(id_kc_pha, gap) * gap + 1]
  
  
  
  ## ZIPs ----
  #### ZIPs to restrict to KC and surrounds (helps make maps that drop far-flung ports) ####
  zips <- read.csv(text = httr::content(httr::GET(
    "https://raw.githubusercontent.com/PHSKC-APDE/reference-data/master/spatial_data/zip_hca.csv")), 
    header = TRUE) %>% 
    select(geo_zip) %>% 
    mutate(geo_zip = as.character(geo_zip), geo_kc_area = 1)
  zips <- setDT(zips)
  
  
  pha_timevar <- merge(pha_timevar, zips, 
                            by.x = c("geo_zip_clean"), by.y = c("geo_zip"),
                            all.x = TRUE, sort = F)
  
  rm(zips)
  
  
  ## Add in last_run date ----
  pha_timevar[, last_run := Sys.time()]
  
  
  ## Select and arrange columns
  cols_select <- c(
    # ID variables
    "id_kc_pha", "id_hash", "hh_id_long", 
    # Time-varying demog variables
    "disability",
    # Program info
    "agency", "major_prog", "subsidy_type", "prog_type", "operator_type", 
    "vouch_type_final", "agency_prog_concat",
    # Address and portfolio info
    "geo_add1_clean", "geo_add2_clean", "geo_city_clean", "geo_state_clean", 
    "geo_zip_clean", "geo_hash_clean", "geo_blank", "geo_kc_area", "portfolio_final",
    # Date info
    "from_date", "to_date", "cov_time", "gap", "period", 
    # Port info
    "port_in", "port_out_kcha", "port_out_sha", "cost_pha",
    # Other info
    "last_run")
  
  pha_timevar <- pha_timevar[, ..cols_select]
  
  
  # LOAD TO SQL SERVER ----
  message("Loading to SQL")
  
  # Set up cols
  col_types <- c("id_kc_pha" = "char(10)")
  
  # Write data
  odbc::dbWriteTable(conn, 
                     name = DBI::Id(schema = to_schema, table = to_table), 
                     value = as.data.frame(pha_timevar),
                     overwrite = T, append = F,
                     field.types = col_types,
                     batch_rows = 10000)
}
