#### CODE TO COMBINE SEATTLE HOUSING AUTHORITY DATA
# Alastair Matheson, PHSKC (APDE)
#
# 2021-06

### Run from main_sha_load script
# https://github.com/PHSKC-APDE/Housing/blob/master/claims_db/etl/db_loader/main_sha_load.R
# Assumes relevant libraries are already loaded


# function to pick which years to load
# bring in relevant raw years
# do usual income work and reshaping
# truncate any data in stage table from selected years
# add selected years to stage table

# conn = ODBC connection to use
# to_schema = name of the schema to load data to
# to_table = name of the table to load data to
# from_schema = name of the schema the raw data are in
# from_table = name of the table the raw data are in (assumes a common table name prefix)
# qa_schema = name of the schema the QA lives in (likely the same as the to_schema if working in HHSAW)
# qa_table = name of the table that holds QA outcomes
# file_path = where the sha data files live (note that the file names themselves are hard coded for now)
# hcv_years = which years of HCV data to bring in (when a file covers multiple years, the latest year is used)
# ph_years = which years of PH data to bring in (when a file covers multiple years, the latest year is used)
# truncate = whether to remove existing stage data from selected years first (default is TRUE).
#    NB. any existing data from other years will remain intact regardless

load_stage_sha <- function(conn = NULL,
                           to_schema = NULL,
                           to_table = NULL,
                           from_schema = NULL,
                           from_table = NULL,
                           qa_schema = NULL,
                           qa_table = NULL,
                           hcv_years = c(2006, 2017, 2018:2020),
                           ph_years = c(2006, 2012, 2017, 2018:2020),
                           truncate = T) {
  
  # CHECK INPUTS ----
  # If 2019 is listed in both HCV and PH, ensure it is only pulled in once
  if (2019 %in% hcv_years & 2019 %in% ph_years) {
    ph_years <- ph_years[ph_years != 2019]
  }
  
  
  # BRING IN DATA AND COMBINE ----
  ## HCV ----
  if (!is.null(hcv_years)) {
    sha_raw_hcv <- map(hcv_years, function(x) {
      message("Working on ", x, " data")
      
      # Need special consideration for table that spans multiple years
      if (x == 2006) {
        table_to_fetch <- paste0(from_table, "_", "hcv_2004_2006")
      } else if (x == 2017) {
        table_to_fetch <- paste0(from_table, "_", "hcv_2006_2017")
      } else if (x == 2019) {
        table_to_fetch <- paste0(from_table, "_", "2019")
      } else {
        table_to_fetch <- paste0(from_table, "_hcv_", x)
      }
      
      table_yr <- dbGetQuery(conn,
                             glue_sql("SELECT * FROM {`from_schema`}.{`table_to_fetch`}",
                                      .con = conn))
      
      return(table_yr)
    })
    # Name each year
    names(sha_raw_hcv) <- paste0("sha_hcv_", hcv_years)
  } else {
    sha_raw_hcv <- NULL
  }
  
  ## PH ----
  if (!is.null(ph_years)) {
    sha_raw_ph <- map(ph_years, function(x) {
      message("Working on ", x, " data")
      
      # Need special consideration for table that spans multiple years
      if (x == 2006) {
        table_to_fetch <- paste0(from_table, "_", "ph_2004_2006")
      } else if (x == 2012) {
        table_to_fetch <- paste0(from_table, "_", "ph_2007_2012")
      } else if (x == 2017) {
        table_to_fetch <- paste0(from_table, "_", "ph_2012_2017")
      } else if (x == 2019) {
        table_to_fetch <- paste0(from_table, "_", "2019")
      } else {
        table_to_fetch <- paste0(from_table, "_ph_", x)
      }
      
      table_yr <- dbGetQuery(conn,
                             glue_sql("SELECT * FROM {`from_schema`}.{`table_to_fetch`}",
                                      .con = conn))
      
      return(table_yr)
    })
    # Name each year
    names(sha_raw_ph) <- paste0("sha_ph_", ph_years)
  } else {
    sha_raw_ph <- NULL
  }
  

  ## Combine ----
  sha_raw <- c(sha_raw_hcv, sha_raw_ph)
  
  
  # CLEAN UP ----
  ## Agency ----
  sha_raw <- sha_raw %>%
    map(~ .x %>% mutate(agency = "SHA"))
  
  
  ## Action codes/types ----
  sha_raw <- sha_raw %>%
    map(~ .x %>% mutate(act_type = case_when(
      act_type %in% c("New Admission", "Move In", "MI") ~ 1L,
      act_type %in% c("Annual Reexamination", "Annual Recertification", "AR") ~ 2L,
      act_type == "Interim Reexamination" ~ 3L,
      act_type == "Portability Move-in" ~ 4L,
      act_type %in% c("Portability Move-out", "Portablity Move-out") ~ 5L,
      act_type %in% c("End Participation", "Termination", "Move Out") ~ 6L,
      act_type %in% c("Other Change of Unit", "Unit Transfer") ~ 7L,
      act_type %in% c("FSS/WtW Addendum Only") ~ 8L,
      act_type == "Annual Reexamination Searching" ~ 9L,
      act_type == "Issuance of Voucher" ~ 10L,
      act_type == "Expiration of Voucher" ~ 11L,
      act_type == "Annual HQS Inspection Only" ~ 13L,
      act_type == "Historical Adjustment" ~ 14L,
      act_type == "Void" ~ 15L,
      act_type == "Port-Out Update (Not Submitted To MTCS)" ~ 16L,
      act_type == "Gross Rent Change" ~ 17L,
      as.integer(act_type) == 99 ~ NA_integer_,
      TRUE ~ as.integer(act_type)))
    )
  
  
  ## Binary recodes ----
  sha_raw <- sha_raw %>%
    map(~ .x %>%
          mutate(across(any_of(c("r_white", "r_black", "r_aian", "r_asian", "r_nhpi", 
                                 "portability", "disability", "tb_rent_ceiling",
                                 "ph_rent_ceiling")) &
                          where(is.character),
                 ~ case_when(tolower(.) %in% c("y", "yes") ~ 1L,
                             tolower(.) %in% c("n", "no", "n0") ~ 0L,
                             is.na(.) ~ NA_integer_,
                             TRUE ~ 99L)),
                 r_hisp = case_when(r_hisp == "2" ~ 0L,
                                    r_hisp == "1" ~ 1L,
                                    r_hisp == "0" ~ 0L,
                                    TRUE ~ as.integer(r_hisp)),
                 female = case_when(tolower(gender) %in% c("f", "female") ~ 1L,
                                    tolower(gender) %in% c("m", "male") ~ 0L,
                                    TRUE ~ NA_integer_)))
  
  
  ## Field types ----
  # Need to get some fields to have the same type so they union properly
  sha_raw <- sha_raw %>%
    map(~ .x %>%
          mutate(across(any_of(c("bdrm_voucher", "building_id", "cost_pha", "fhh_ssn", 
                                 "property_id", "recert_sched", "rent_type", "subs_type",
                                 "unit_type")), ~ as.character(.))))
  

  ## Clean up white space ----
  sha_raw <- sha_raw %>%
    map(~ .x %>% mutate(across(where(is_character), ~ str_squish(.x))))
  
  
  ## Names ----
  # Set up suffixes to remove (they make matching harder)
  suffix <- c(" SR", " JR", "-JR", "JR", "JR I", "JR II", "JR III", " II", " III", " IV")
  
  sha_raw <- sha_raw %>%
    map(~ .x %>%
          mutate(across(any_of(c("hh_lname", "hh_fname", "hh_mname", "lname", "fname", "mname")), 
                        ~ toupper(.)),
                 across(any_of(c("hh_lname", "hh_fname", "hh_mname", "lname", "fname", "mname")), 
                        ~ str_replace_all(., "_|-", " ")),
                 across(any_of(c("hh_lname", "hh_fname", "hh_mname", "lname", "fname", "mname")), 
                        ~ str_replace_all(., "NULL|\\.|\"|\\\\|'|`|[0-9]|\\*", "")),
                 across(any_of(c("hh_lname", "hh_fname", "hh_mname", "lname", "fname", "mname")), 
                        ~ str_replace_all(., paste(suffix, "$", collapse="|", sep = ""), "")),
                 across(any_of(c("hh_lname", "hh_fname", "hh_mname", "lname", "fname", "mname")), 
                        ~ str_squish(.)),
                 across(any_of(c("hh_lname", "hh_fname", "hh_mname", "lname", "fname", "mname")), 
                        ~ ifelse(. == "", NA_character_, .))) %>%
          # Clean up where middle initial seems to be in first name field
          # NOTE: There are many rows with a middle initial in the fname field AND 
          # the mname field (and the initials are not always the same).
          # In those situations, keep existing middle initial and drop from fname if
          # the initial is the same
          mutate(f_init = str_sub(fname, -2, -1),
                 mname = case_when(str_detect(f_init, "[:space:][A-Z]") & is.na(mname) ~ str_sub(fname, -1),
                                       TRUE ~ mname),
                 fname = case_when(str_detect(f_init, "[:space:][A-Z]") & str_sub(fname, -1) == mname ~ 
                                         str_sub(fname, 1, -3),
                                       TRUE ~ fname),
                 # Remove any first name unknown values
                 across(any_of(c("fname", "hh_fname")), ~ ifelse(. == "FNU", NA_character_, .)),
                 # Flag baby and institutional names for cleaning
                 drop_name = case_when(fname %in% c("UNBORN", "BABY", "MIRACLE", "CHILD") & 
                                         lname %in% c("UNBORN", "BABY", "CHILD") ~ 1L,
                                       lname == "YWCA" ~ 1L,
                                       lname == "ELDER" & fname == "PLACE" ~ 1L,
                                       lname == "ELDER PLACE" ~ 1L,
                                       str_detect(lname, "LIVE IN") ~ 1L,
                                       str_detect(fname, "LIVE IN") ~ 1L,
                                       TRUE ~ 0L),
                 # Clean baby and other names
                 across(any_of(c("lname", "fname", "mname")), ~ ifelse(drop_name == 1, NA, .)),
                 fname = ifelse(fname %in% c("UNBORN", "BABY"), NA, fname)) %>%
          select(-f_init, -drop_name)
        ) %>%
    map(~ if ("hh_fname" %in% names(.x)) {
      .x %>% mutate(f_init_hh = str_sub(hh_fname, -2, -1),
                    hh_mname = case_when(str_detect(f_init_hh, "[:space:][A-Z]") & is.na(hh_mname) ~ str_sub(hh_fname, -1),
                                         TRUE ~ hh_mname),
                    hh_fname = case_when(str_detect(f_init_hh, "[:space:][A-Z]") & str_sub(hh_fname, -1) == hh_mname ~ 
                                           str_sub(hh_fname, 1, -3),
                                         TRUE ~ hh_fname),
                    drop_name_hh = case_when(hh_fname %in% c("UNBORN", "BABY", "MIRACLE", "CHILD") & 
                                               hh_lname %in% c("UNBORN", "BABY", "CHILD") ~ 1L,
                                             hh_lname == "YWCA" ~ 1L,
                                             hh_lname == "ELDER" & hh_fname == "PLACE" ~ 1L,
                                             hh_lname == "ELDER PLACE" ~ 1L,
                                             str_detect(hh_lname, "LIVE IN") ~ 1L,
                                             str_detect(hh_fname, "LIVE IN") ~ 1L,
                                             TRUE ~ 0L),
                    across(any_of(c("hh_lname", "hh_fname", "hh_mname")), ~ ifelse(drop_name_hh == 1, NA, .))) %>%
        select(-f_init_hh, -drop_name_hh)} 
      else {.x})
    
  
  
  ## SSNs ----
  # Some SSNs are HUD/PHA-generated IDs so separate into new field
  # (note that conversion of legitimate SSN to numeric strips out leading zeros and removes rows with characters)
  # Need to restore leading zeros
  # Remove dashes first
  sha_raw <- sha_raw %>%
    map(~ .x %>%
          mutate(across(any_of(c("ssn", "hh_ssn", "fhh_ssn")), ~ str_replace_all(., "-", "")),
                 across(any_of(c("ssn", "hh_ssn", "fhh_ssn")), 
                        ~ case_when(. %in% c("010010101", "011111111", "011223333", 
                                             "111111111", "112234455", "111119999", "123121234", "123123123", "123456789", 
                                             "222111212", "222332222",
                                             "333333333", "444444444", 
                                             "555112222", "555115555", "555555555", "555555566",
                                             "699999999",  
                                             "888888888", "898989898", "898888899") ~ NA_character_,
                                    as.numeric(.) < 1000000 | as.numeric(.) >= 900000000 ~ NA_character_,
                                    between(as.numeric(.), 666000000, 666999999) ~ NA_character_,
                                    str_sub(., -4, -1) == "0000" | str_sub(., 1, 3) == "999" |
                                      str_detect(., "XXX|A00-0|A000") ~ NA_character_,
                                    TRUE ~ .)),
                 across(any_of(c("ssn", "hh_ssn", "fhh_ssn")), ~ ifelse(str_detect(., "[:alpha:]"), ., NA_character_),
                        .names = "{.col}_new"),
                 across(any_of(c("ssn", "hh_ssn", "fhh_ssn")),
                        ~ str_pad(round(as.numeric(.), digits = 0), width = 9, side = "left", pad = "0")))) %>%
    # Change new column names
    map(~ .x %>% rename_with(., ~ str_replace(., "ssn_new", "pha_id"), .cols = matches("ssn_new")))
  
  
  ## Program types ----
  sha_raw <- sha_raw %>%
    map(~ .x %>%
          mutate(prog_type = 
                   case_when(str_detect(tolower(prog_type), "owned") ~ "SHA owned and managed",
                             str_detect(tolower(prog_type), "collaborative") ~ "Collaborative housing",
                             str_detect(tolower(prog_type), "partnership") ~ "Collaborative housing",
                             str_detect(tolower(prog_type), "tenant") ~ "Tenant based")))
  
  
  ## Income fields ----
  # Don't want to keep any specific income sources, just totals (avoids duplicate rows)
  sha_raw <- sha_raw %>%
    map(~ .x %>%
          select(-any_of(c("inc_b", "inc_c", "inc_e", "inc_f", "inc_g", "inc_ha", 
                           "inc_i", "inc_iw", "inc_m", "inc_n","inc_p", "inc_s", 
                           "inc_ss", "inc_t", "inc_u", "inc_w", "inc_x"))) %>%
          distinct())
  
  
  ## Port ins/outs ----
  sha_raw <- sha_raw %>%
    map(~ if ("cost_pha" %in% names(.x)) {
      .x %>%
        mutate(
          # Port in
          port_in = case_when(
            prog_type == "PORT" ~ 1L,
            act_type == 4 ~ 1L,
            cost_pha != "" & cost_pha != "WA001" & 
              # SHA seems to point to another billed PHA even when the person has ported out from SHA to another PHA, need to ignore this
              !(str_detect(geo_add1_raw, "PORT OUT") & act_type %in% c(5, 16)) ~ 1L,
            TRUE ~ 0L),
          # Port out
          # Seems to be that when SHA is missing address data and the action code == 
          # Port-Out Update (Not Submitted To MTCS) (recoded as 16), the person is in another housing authority.
          port_out_sha = case_when(
            str_detect(geo_add1_raw, "PORT OUT") | act_type == 5 ~ 1,
            act_type == 16 & cost_pha != "" ~ 1,
            TRUE ~ 0)
        )
    } else {
      .x %>%
        mutate(
          # Port in
          port_in = case_when(
            act_type == 4 ~ 1L,
            TRUE ~ 0L),
          # Port out
          # Seems to be that when SHA is missing address data and the action code == 
          # Port-Out Update (Not Submitted To MTCS) (recoded as 16), the person is in another housing authority.
          port_out_sha = case_when(
            str_detect(geo_add1_raw, "PORT OUT") | act_type == 5 ~ 1,
            act_type == 16 ~ 1,
            TRUE ~ 0)
        )
    })
  

  # ADDRESS CLEANING ----
  ## Make a geo_hash_raw field for easier joining
  sha_raw <- sha_raw %>%
    map(~ .x %>% 
          mutate(across(starts_with("geo_"), toupper)) %>%
          mutate(geo_hash_raw = as.character(toupper(openssl::sha256(
            paste(str_replace_na(geo_add1_raw, ''),
                  if ("geo_add2_raw" %in% names(.x)) {str_replace_na(geo_add2_raw, '')} else {''},
                  if ("geo_add3_raw" %in% names(.x)) {str_replace_na(geo_add3_raw, '')} else {''},
                  str_replace_na(geo_city_raw, ''),
                  str_replace_na(geo_state_raw, ''),
                  str_replace_na(geo_zip_raw, ''),
                  sep = "|")))))
    )
  
  ## Pull out all unique addresses ----
  adds_distinct <- sha_raw %>%
    map(~ .x %>% distinct(across(matches("^geo_")))) %>%
    bind_rows() %>%
    distinct() %>%
    select(geo_add1_raw, geo_add2_raw, geo_add3_raw, geo_city_raw, 
           geo_state_raw, geo_zip_raw, geo_hash_raw)
  
  
  ## Load to a temp SQL table for cleaning (have to use prod HHSAW here) ----
  db_hhsaw_prod <- dbConnect(odbc(), "hhsaw_prod", uid = keyring::key_list("hhsaw")[["username"]])
  
  try(dbRemoveTable(db_hhsaw_prod, "##sha_adds"))
  odbc::dbWriteTable(db_hhsaw_prod,
                     name = "##sha_adds",
                     value = adds_distinct,
                     overwrite = T)
  
  ## Pull in clean addresses ----
  adds_already_clean <- DBI::dbGetQuery(db_hhsaw_prod,
                                        "SELECT b.* FROM 
                                 (SELECT geo_hash_raw FROM ##sha_adds) a
                                 INNER JOIN
                                 (SELECT * FROM ref.address_clean) b
                                 ON a.geo_hash_raw = b.geo_hash_raw")
  
  ## Pull in addresses that need cleaning ----
  adds_to_clean <- DBI::dbGetQuery(db_hhsaw_prod,
                                   "SELECT a.* FROM 
                                 (SELECT * FROM ##sha_adds) a
                                 LEFT JOIN
                                 (SELECT geo_hash_raw, 1 AS clean
                                 FROM ref.address_clean) b
                                 ON a.geo_hash_raw = b.geo_hash_raw
                                 WHERE b.clean IS NULL")
  
  ## Load to Informatica for cleaning ----
  if (nrow(adds_to_clean) > 0) {
    # Add new addresses that need cleaning into the Informatica table
    timestamp <- Sys.time()
    
    adds_to_clean <- adds_to_clean %>% mutate(geo_source = NA, timestamp = timestamp)
    
    DBI::dbWriteTable(db_hhsaw_prod, 
                      name = DBI::Id(schema = "ref", table = "informatica_address_input"),
                      value = adds_to_clean,
                      overwrite = F, append = T)
  }
  
  ## Retrieve cleaned addresses ----
  if (nrow(adds_to_clean) > 0) {
    adds_clean <- dbGetQuery(db_hhsaw_prod,
                             glue::glue_sql("SELECT * FROM ref.informatica_address_output
                           WHERE convert(varchar, timestamp, 20) = {lubridate::with_tz(timestamp, 'utc')}",
                                            .con = db_hhsaw_prod))
    
    # Keep checking each hour until the addresses have been cleaned
    while (nrow(adds_clean) == 0) {
      message("Waiting on Informatica to clean addresses. Will check again in 1 hour.")
      Sys.sleep(3600)
      
      # Will need to reconnect to the DB since it will have timed out
      db_hhsaw_prod <- dbConnect(odbc(), "hhsaw_prod", uid = keyring::key_list("hhsaw")[["username"]])
      
      adds_clean <- dbGetQuery(db_hhsaw_prod,
                               glue::glue_sql("SELECT * FROM ref.informatica_address_output
                           WHERE convert(varchar, timestamp, 20) = {lubridate::with_tz(timestamp, 'utc')}",
                                              .con = db_hhsaw_prod))
    }
    
    # Informatica seems to drop secondary designators when they start with #
    # Move over from old address
    adds_clean <- adds_clean %>%
      mutate(geo_add2_clean = ifelse(is.na(geo_add2_clean) & str_detect(geo_add1_raw, "^#"),
                                     geo_add1_raw, geo_add2_clean))
    
    # Tidy up some PO box messiness
    adds_clean <- adds_clean %>%
      mutate(geo_add1_clean = case_when((is.na(geo_add1_clean) | geo_add1_clean == "") & 
                                          !is.na(geo_po_box_clean) ~ geo_po_box_clean,
                                        TRUE ~ geo_add1_clean),
             geo_add2_clean = case_when(
               geo_add1_clean == geo_po_box_clean ~ geo_add2_clean,
               (is.na(geo_add2_clean) | geo_add2_clean == "") & !is.na(geo_po_box_clean) & 
                 !is.na(geo_add1_clean) ~ geo_po_box_clean,
               !is.na(geo_add2_clean) & !is.na(geo_po_box_clean) & 
                 !is.na(geo_add1_clean) ~ paste(geo_add2_clean, geo_po_box_clean, sep = " "),
               TRUE ~ geo_add2_clean)
      )
    
    # Set up variables of interest
    adds_clean <- adds_clean %>%
      mutate(geo_geocode_skip = 0L,
             across(where(is.character) & contains("clean"), str_squish),
             geo_hash_clean = as.character(toupper(openssl::sha256(paste(stringr::str_replace_na(geo_add1_clean, ''), 
                                                                         stringr::str_replace_na(geo_add2_clean, ''), 
                                                                         stringr::str_replace_na(geo_city_clean, ''), 
                                                                         stringr::str_replace_na(geo_state_clean, ''), 
                                                                         stringr::str_replace_na(geo_zip_clean, ''), 
                                                                         sep = "|")))),
             geo_hash_geocode = as.character(toupper(openssl::sha256(paste(stringr::str_replace_na(geo_add1_clean, ''),  
                                                                           stringr::str_replace_na(geo_city_clean, ''), 
                                                                           stringr::str_replace_na(geo_state_clean, ''), 
                                                                           stringr::str_replace_na(geo_zip_clean, ''), 
                                                                           sep = "|")))),
             last_run = Sys.time()) %>%
      select(geo_add1_raw, geo_add2_raw, geo_add3_raw, geo_city_raw, 
             geo_state_raw, geo_zip_raw, geo_hash_raw,
             geo_add1_clean, geo_add2_clean, geo_city_clean, 
             geo_state_clean, geo_zip_clean, geo_hash_clean, geo_hash_geocode,
             geo_geocode_skip, last_run) %>%
      # Convert all blank fields to be NA
      mutate_if(is.character, list(~ ifelse(. == "", NA_character_, .)))
    
    
    # Add to ref table
    dbWriteTable(db_hhsaw_prod, 
                 name = DBI::Id(schema = "ref",  table = "stage_address_clean"),
                 adds_clean,
                 overwrite = F, append = T)
    
    # Do some basic QA
    rows_stage <- as.integer(dbGetQuery(db_hhsaw_prod, "SELECT COUNT (*) AS row_cnt FROM ref.stage_address_clean"))
    rows_ref <- as.integer(dbGetQuery(db_hhsaw_prod, "SELECT COUNT (*) AS row_cnt FROM ref.address_clean"))
    
    if (rows_stage > rows_ref) {
      dbWriteTable(db_hhsaw_prod, 
                   name = DBI::Id(schema = "ref",  table = "address_clean"),
                   adds_clean,
                   overwrite = F, append = T)
    }
    
    # Don't need to geocode at this point so skip that part
  }
  
  ## Bring it all together ----
  if (nrow(adds_to_clean) > 0) {
    adds_final <- bind_rows(adds_already_clean, adds_clean)
  } else {
    adds_final <- adds_already_clean
  }
  
  ## Join back to original data ----
  sha_raw <- sha_raw %>%
    map(~ .x %>% left_join(., 
                           select(adds_final, geo_hash_raw:geo_hash_geocode),
                           by = "geo_hash_raw") %>%
          select(-matches("geo_(.)*_raw")))
  
  # Add in geo_blank
  sha_raw <- sha_raw %>%
    map(~ .x %>% mutate(geo_blank = ifelse(
      geo_hash_clean %in% c("45CA31C3315A5978F40438AAB46040D75E99C9B125C2FD01DB6E10AC80BEF906",
                            "8926262F06508A0E264BC13D340FD8FAB9291001FC06341D2E687BD9C3AF6104"),
      1L, 0L)))
  
  
  # HEAD OF HOUSEHOLD ----
  # Previously there had been multiple people listed as head of household for 
  # some rows, mostly from the overlapping 2006-2016 and 2006-2017 HCV files.
  # However, with the new approach to processing those files, where only rows from
  # the older files with distinct cert_id, hh_id, mbr_id, and act_date and carried over,
  # there are no longer any multiple heads of household.
  # However, there are only hh_ssn fields in later files, not names, so want to fix that
  
  # First keep track of the number of rows in case the join adds some
  nrow_sha <- sha_raw %>% map(~ .x %>% nrow()) %>% bind_rows()
  
  sha_raw <- sha_raw %>%
    map(~ if ("hh_lname" %in% names(.x) == F) {
      hh_names <- .x %>%
        select(cert_id, act_date, hh_ssn, ssn, lname, fname, mname) %>%
        filter(hh_ssn == ssn) %>%
        rename(hh_lname = lname,
               hh_fname = fname,
               hh_mname = mname) %>%
        select(-ssn) %>%
        distinct()
      
      .x <- left_join(.x, hh_names, by = c("cert_id", "act_date", "hh_ssn"))
    } else {.x})
  
  if (min(sha_raw %>% map(~ .x %>% nrow()) %>% bind_rows() == nrow_sha) == 0) {
    stop ("Filling in the head of household details created extra rows. Check for duplicate rows in each year.")
  }
  
  
  # FILL IN GAPS ----
  sha_raw <- sha_raw %>%
    map(~ .x %>%
          mutate(major_prog = case_when(str_detect(pha_source, "hcv") ~ "HCV",
                                        str_detect(pha_source, "ph") ~ "PH",
                                        str_detect(tolower(prog_type), "owned") ~ "PH",
                                        str_detect(tolower(prog_type), "tenant") ~ "HCV",
                                        str_detect(tolower(prog_type), "collaborative") ~ "HCV",
                                                   TRUE ~ NA_character_)))
  
  
  # MAKE FINAL DATA FRAME AND ADD USEFUL VARIABLES ----
  last_run <- Sys.time()
  
  sha_long <- sha_raw %>%
    bind_rows() %>%
    # Make a hash of variables that are used for identity matching
    mutate(id_hash = as.character(toupper(openssl::sha256(paste(str_replace_na(ssn, ''),
                                                           str_replace_na(pha_id, ''),
                                                           str_replace_na(lname, ''),
                                                           str_replace_na(fname, ''),
                                                           str_replace_na(mname, ''),
                                                           str_replace_na(dob, ''),
                                                           str_replace_na(female, ''),
                                                           sep = "|")))),
           last_run = last_run)
  
  
  # QA FINAL DATA ----
  ## Row counts compared to last time ----
  rows_existing <- as.integer(dbGetQuery(conn,
                                         glue_sql("SELECT qa_result FROM {`qa_schema`}.{`qa_table`}
                                       WHERE qa_type = 'value' AND qa_item = 'row_count' AND
                                       table_name = '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}'
                                       ORDER BY qa_date desc",
                                                  .con = conn))[1,])
  
  if (is.na(rows_existing)) {
    qa_row_diff_result <- "PASS"
    qa_row_diff_note <- glue("There was no existing row data to compare to last time")
    message(qa_row_diff_note)
  } else if (!is.na(rows_existing) & nrow(sha_long) >= rows_existing) {
    qa_row_diff_result <- "PASS"
    qa_row_diff_note <- glue("There were {format(nrow(sha_long) - rows_existing, big.mark = ',')}", 
                             " more rows in the lastest stage table")
    message(qa_row_diff_note)
  } else if (!is.na(rows_existing) & nrow(sha_long) < rows_existing) {
    qa_row_diff_result <- "FAIL"
    qa_row_diff_note <- glue("There were {format(rows_existing - nrow(sha_long), big.mark = ',')}", 
                             " fewer rows in the lastest stage table. See why this is.")
    warning(qa_row_diff_note)
  } 
  
  DBI::dbExecute(conn,
                 glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`} 
                          (etl_batch_id, last_run, table_name, qa_type, qa_item, qa_result, qa_date, note) 
                          VALUES (NULL, {last_run}, '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}', 'result', 
                          'row_count_vs_previous', {qa_row_diff_result}, {Sys.time()}, {qa_row_diff_note})",
                          .con = conn))
  
  
  ## Columns match existing table ----
  cols_current <- try(dbGetQuery(conn,
                                 glue_sql("SELECT TOP 0 * FROM {`to_schema`}.{`to_table`}",
                                          .con = conn)))
  
  if (str_detect(cols_current[1], "Error", negate = T)) {
    if (length(names(cols_current)[names(cols_current) %in% names(sha_long) == F]) > 1) {
      qa_names_result <- "FAIL"
      qa_names_note <- glue("The existing stage table has columns not found in the new data")
      warning(qa_names_note)
    } else if (length(names(sha_long)[names(sha_long) %in% names(cols_current) == F]) > 1) {
      qa_names_result <- "FAIL"
      qa_names_note <- glue("The new stage table has columns not found in the current data")
      warning(qa_names_note)
    } else {
      qa_names_result <- "PASS"
      qa_names_note <- glue("The existing and new stage tables have matching columns")
      message(qa_names_note)
    }
  } else {
    qa_names_result <- "PASS"
    qa_names_note <- glue("There was no existing column data to compare to last time")
    message(qa_names_note)
  }
  
  
  DBI::dbExecute(conn,
                 glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`} 
                          (etl_batch_id, last_run, table_name, qa_type, qa_item, qa_result, qa_date, note) 
                          VALUES (NULL, {last_run}, '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}', 'result', 
                          'row_count_vs_previous', {qa_names_result}, {Sys.time()}, {qa_names_note})",
                          .con = conn))
  
  
  # ADD VALUES TO METADATA ----
  # Row counts
  DBI::dbExecute(conn,
                 glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`} 
                          (etl_batch_id, last_run, table_name, 
                            qa_type, qa_item, qa_result, qa_date, note) 
                          VALUES (NULL, {last_run}, '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                                  'value', 'row_count', {nrow(sha_long)},
                                  {Sys.time()}, NULL)",
                          .con = conn))
  
  
  # CHECK QA PASSED ----
  # Stop processing if one or more QA check failed
  if (min(qa_row_diff_result, qa_names_result) == "FAIL") {
    stop(glue("One or more QA checks failed on {to_schema}.{to_table}. See {`qa_schema`}.{`qa_table`} for more details."))
  } else {
    # Clean up QA objects if everything passed
    rm(list = ls(pattern = "^qa_"))
  }
  
  
  # LOAD DATA TO SQL ----
  ## Remove any rows from selected years ----
  # Need to figure out how this works
  # - use etl batch ID? won't work if a year is updated
  # - use action dates? won't work since data cover multiple years
  if (truncate == T) {
    message("Have not worked out how to implement this yet")
  }
  
  
  ## Load data ----
  # Split into smaller tables to avoid SQL connection issues
  start <- 1L
  max_rows <- 50000L
  cycles <- ceiling(nrow(sha_long)/max_rows)
  
  lapply(seq(start, cycles), function(i) {
    start_row <- ifelse(i == 1, 1L, max_rows * (i-1) + 1)
    end_row <- min(nrow(sha_long), max_rows * i)
    
    message("Loading cycle ", i, " of ", cycles)
    if (i == 1) {
      dbWriteTable(conn,
                   name = DBI::Id(schema = to_schema, table = to_table),
                   value = as.data.frame(sha_long[start_row:end_row, ]),
                   overwrite = T, append = F)
    } else {
      dbWriteTable(conn,
                   name = DBI::Id(schema = to_schema, table = to_table),
                   value = as.data.frame(sha_long[start_row:end_row ,]),
                   overwrite = F, append = T)
    }
  })
  
}
