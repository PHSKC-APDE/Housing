#### CODE TO COMBINE SEATTLE HOUSING AUTHORITY DATA ----
# Alastair Matheson, PHSKC (APDE)
#
# 2021-06

### Run from main_sha_load script (description) ----
# https://github.com/PHSKC-APDE/Housing/blob/main/claims_db/etl/db_loader/main_sha_load.R
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
# hcv_ph_years = which years of joint HCV-PH data to bring in (when a file covers multiple years, the latest year is used)
# truncate = whether to remove existing stage data from selected years first (default is TRUE).
#    NB. any existing data from other years will remain intact regardless

### Function ----
load_stage_sha <- function(conn = NULL,
                           to_schema = NULL,
                           to_table = NULL,
                           from_schema = NULL,
                           from_table = NULL,
                           qa_schema = NULL,
                           qa_table = NULL,
                           hcv_years = c(2006, 2017, 2018),
                           ph_years = c(2006, 2012, 2017, 2018),
                           hcv_ph_years = c(2019:2021),
                           truncate = T) {
  
  # CHECK INPUTS ----
    # Only process years if they have not been processed before 
      db_hhsaw_prod <- create_db_connection(server = 'hhsaw', interactive = F, prod = T)
      
      already <- tryCatch(dbGetQuery(conn = db_hhsaw_prod, "SELECT DISTINCT pha_source from [pha].[stage_sha]")[]$pha_source, 
                                error = function(e)
                                  message("The [pha].[stage_sha] table does not exist so all years of data requested in arguments will be loaded"))
      if(exists("already")){
        # identify years of data already in stage by major program ----
          already.hcv <- gsub("[a-z]|_", "", grep("hcv", already, value = T))
          already.ph <- gsub("[a-z]|_", "", grep("ph", already, value = T))
          already.hcv_ph <- gsub("[a-z]|_", "", grep("ph|hcv", already, value = T, invert = T))
        
        # check hcv_years ----
          if(length(intersect(hcv_years, already.hcv) > 0)){
            message(paste0("\n\U00026A0 The raw HCV data for the following years already exists and will not be reloaded: ", 
                           paste(intersect(hcv_years, already.hcv), collapse = ", "), ".\n", 
                           "If you want to update these data in the stage table, first delete from the SQL table. For example,\n", 
                           'dbGetQuery(conn = db_hhsaw_prod, "DELETE FROM [pha].[stage_sha] WHERE pha_source = "sha2017_hcv")'))
            hcv_years <- setdiff(hcv_years, intersect(hcv_years, already.hcv))
            if(length(hcv_years) == 0){hcv_years = NULL}
          }
          
        # check ph_years ----
          if(length(intersect(ph_years, already.ph) > 0)){
            message(paste0("\n\U00026A0 The raw ph data for the following years already exists and will not be reloaded: ", 
                           paste(intersect(ph_years, already.ph), collapse = ", "), ".\n", 
                           "If you want to update these data in the stage table, first delete from the SQL table. For example,\n", 
                           'dbGetQuery(conn = db_hhsaw_prod, "DELETE FROM [pha].[stage_sha] WHERE pha_source = "sha2017_ph")'))
            ph_years <- setdiff(ph_years, intersect(ph_years, already.ph))
            if(length(ph_years) == 0){ph_years = NULL}
          }
          
        # check hcv_ph_years ----
          if(length(intersect(hcv_ph_years, already.hcv_ph) > 0)){
            message(paste0("\n\U00026A0 The raw HCV data for the following years already exists and will not be reloaded: ", 
                           paste(intersect(hcv_ph_years, already.hcv_ph), collapse = ", "), ".\n", 
                           "If you want to update these data in the stage table, first delete from the SQL table. For example,\n", 
                           'dbGetQuery(conn = db_hhsaw_prod, "DELETE FROM [pha].[stage_sha] WHERE pha_source = "sha2019")'))
            hcv_ph_years <- setdiff(hcv_ph_years, intersect(hcv_ph_years, already.hcv_ph))
            if(length(hcv_ph_years) == 0){hcv_ph_years = NULL}
          }
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
        
        new.hcv_source <- unlist(lapply(sha_raw_hcv, FUN = function(X){unique(X$pha_source)}))
        if(length(intersect(already, new.hcv_source)) > 0){
          stop(paste0("\nYou imported the following pha_source(s) that already exists in the stage table: ", 
                      paste0(intersect(already, new.hcv_source), collapse = ','), ".\n", 
                      "If you want to replace this data in the stage table, manually the rows. For example, \n", 
                      'dbGetQuery(conn = db_hhsaw_prod, "DELETE FROM from [pha].[stage_sha] WHERE pha_source = "sha2017_hcv"")'))
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
    
        new.ph_source <- unlist(lapply(sha_raw_ph, FUN = function(X){unique(X$pha_source)}))
        if(length(intersect(already, new.ph_source)) > 0){
          stop(paste0("\nYou imported the following pha_source(s) that already exists in the stage table: ", 
                      paste0(intersect(already, new.ph_source), collapse = ','), ".\n", 
                      "If you want to replace this data in the stage table, manually the rows. For example, \n", 
                      'dbGetQuery(conn = db_hhsaw_prod, "DELETE FROM from [pha].[stage_sha] WHERE pha_source = "sha2017_ph"")'))
        }    
        
    ## HCV-PH (joint)----
        if (!is.null(hcv_ph_years)) {
          sha_raw_hcv_ph <- map(hcv_ph_years, function(x) {
            message("Working on ", x, " HCV & PH data")
            
            table_to_fetch <- paste0(from_table, "_", x)
        
            table_yr <- setDT(dbGetQuery(conn,
                                   glue_sql("SELECT * FROM {`from_schema`}.{`table_to_fetch`}",
                                            .con = conn)))
            rads::sql_clean(table_yr)
            
            return(table_yr)
          })
          # Name each year
          names(sha_raw_hcv_ph) <- paste0("sha_hcv_ph_", hcv_ph_years)
        } else {
          sha_raw_hcv_ph <- NULL
        }
        
        new.hcv_ph_source <- unlist(lapply(sha_raw_hcv_ph, FUN = function(X){unique(X$pha_source)}))
        if(length(intersect(already, new.hcv_ph_source)) > 0){
          stop(paste0("\nYou imported the following pha_source(s) that already exists in the stage table: ", 
                      paste0(intersect(already, new.hcv_ph_source), collapse = ','), ".\n", 
                      "If you want to replace this data in the stage table, manually the rows. For example, \n", 
                      'dbGetQuery(conn = db_hhsaw_prod, "DELETE FROM from [pha].[stage_sha] WHERE pha_source = "sha2019"")'))
        }    
        
    ## Combine ----
    sha_raw <- c(sha_raw_hcv, sha_raw_ph, sha_raw_hcv_ph)
    
  # CLEAN UP ----
    ## Clean up white space ----
        sha_raw <- sha_raw %>%
          map(~ .x %>% mutate(across(where(is_character), ~ str_squish(.x)))) 
        
    ## Data types ----
        sha_raw <- sha_raw %>%
          map(~ .x %>% 
                mutate(hh_size = as.integer(hh_size), 
                       bed_cnt = as.integer(bed_cnt), 
                       rent_tenant = as.integer(rent_tenant), 
                       rent_mixfam = as.integer(rent_mixfam), 
                       mbr_num = as.integer(mbr_num))) 
        
    ## Agency ----
      sha_raw <- sha_raw %>%
        map(~ .x %>% mutate(agency = "SHA"))
      
    
    ## Action codes/types ----
      sha_raw <- sha_raw %>%
        map(~ .x %>% mutate(act_type = case_when(
          act_type %in% c("New Admission", "Move In", "MI", "Initial Certificaton") ~ 1L,
          act_type %in% c("Annual Reexamination", "Annual Recertification", "AR") ~ 2L,
          act_type %in% c("Interim Reexamination", "Interim Recertification") ~ 3L,
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
        
        sha_raw <- sha_raw %>%
          map(~ .x %>%
                mutate(across(any_of(c("hh_inc_tot", "hh_inc_tot_adj", "rent_owner")), ~ as.integer(.))))
  
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
                                                 "078051120", # woolworth wallet SSN 
                                                 "111111111", "112234455", "111119999", "123121234", "123123123", "123456789", 
                                                 "222111212", "222332222",
                                                 "219099999", # SS administration advertisement SSN
                                                 "333333333", "444444444", 
                                                 "457555462", # Lifelock CEO Todd Davis (at least 13 cases of identity theft)
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
      map(~ if ("port_status" %in% names(.x)) {
        .x %>%
          mutate(
            # Port in
            port_in = case_when(
              port_status == 'Port-In' ~ 1L,
              TRUE ~ 0L),
            # Port out
            port_out_sha = case_when(
              port_status == 'Port-Out' ~ 1,
              TRUE ~ 0)
          ) %>% 
          select(-port_status)
      } else {.x})
    
    sha_raw <- sha_raw %>%
      map(~ if (!"port_status" %in% names(.x) & "cost_pha" %in% names(.x)) {
        .x %>%
          mutate(
            # Port in
            port_in = case_when(
              prog_type == "PORT" ~ 1L,
              act_type == 4 ~ 1L, # 4 >> Port move in 
              cost_pha != "" & cost_pha != "WA001" & # WA001 == SHA
                # SHA seems to point to another billed PHA even when the person has ported out from SHA to another PHA, need to ignore this
                !(str_detect(geo_add1_raw, "PORT OUT") & act_type %in% c(5, 16)) ~ 1L,
              TRUE ~ 0L),
            # Port out
            # Seems to be that when SHA is missing address data and the action code == 
            # Port-Out Update (Not Submitted To MTCS) (recoded as 16), the person is in another housing authority.
            port_out_sha = case_when(
              str_detect(geo_add1_raw, "PORT OUT") | act_type == 5 ~ 1, # 5 >> Port move out
              act_type == 16 & cost_pha != "" ~ 1, # 16 >> Port-Out Update (Not Submitted To MTCS)
              TRUE ~ 0)
          )
      } else {.x})
    
    sha_raw <- sha_raw %>%
      map(~ if (!"port_status" %in% names(.x) & !"cost_pha" %in% names(.x)) {
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
      } else {.x})
  
  # ADDRESS CLEANING ----
    ## Make a geo_hash_raw field for easier joining ----
      # create geo_add2_raw if needed
        sha_raw <- sha_raw %>%
          map(~ if (!"geo_add2_raw" %in% names(.x)) {
            .x %>% mutate(geo_add2_raw = NA_character_)
          } else {.x} )
      
      # create geo_add3_raw if needed
        sha_raw <- sha_raw %>%
          map(~ if (!"geo_add3_raw" %in% names(.x)) {
            .x %>% mutate(geo_add3_raw = NA_character_)
          } else {.x} )
      
      # create geo_hash_raw with kcgeocode package
        sha_raw <- sha_raw %>%
          map(~ .x %>%
                mutate(geo_hash_raw = kcgeocode::hash_address(add1 = geo_add1_raw, 
                                                              add2 = geo_add2_raw, 
                                                              add3 = geo_add3_raw, 
                                                              city = geo_city_raw, 
                                                              state = geo_state_raw, 
                                                              zip = as.integer(geo_zip_raw), 
                                                              type = 'raw'))
          )
  
    ## Pull out all unique addresses ----
        adds_distinct <- unique(setDT(sha_raw %>%
          map(~ .x %>% distinct(across(matches("^geo_")))) %>%
          bind_rows()))
    
        adds_distinct <- adds_distinct[!(geo_add1_raw == 'NULL' & geo_add2_raw == 'NULL')]

    ## Identify adds_already_clean & adds_to_clean ----
      db_hhsaw_prod <- create_db_connection(server = 'hhsaw', interactive = F, prod = T) # server 16
      address_check = kcgeocode::fetch_addresses(ads = adds_distinct, 
                                                       input_type = "raw", 
                                                       con = db_hhsaw_prod, 
                                                       geocode = FALSE)
      rads::sql_clean(address_check)
      
      adds_already_clean <- address_check[!is.na(geo_add1_clean)] # only keep coded addresses
      adds_already_clean[, "geo_po_box_clean" := NULL] # Want street address, not PO BOX
      
      adds_to_clean <- adds_distinct[!geo_hash_raw %in% adds_already_clean$geo_hash_raw]

    ## Actual geocode using kcgeocode (SLOW!!!)----
      message("Geocoding ... be patient!")
        if(nrow(adds_to_clean) > 0){
          kcgeocode::submit_ads_for_cleaning(
            ads = setDF(copy(adds_to_clean)), 
            con = db_hhsaw_prod
          )
        }
      
    ## Check geocoding status ----
      message("To check the geocoding status, update and uncomment the following line ... ")
      # kcgeocode::check_status('2023-03-23 16:21:50', type = 'timestamp', con = db_hhsaw_prod)
    
    ## Get newly cleaned/geocoded addresses ----
      if(nrow(adds_to_clean) > 0){
          adds_clean = setDT(kcgeocode::fetch_addresses(ads = adds_to_clean, 
                                                              input_type = "raw", 
                                                              con = db_hhsaw_prod, 
                                                              geocode = FALSE))
          sql_clean(adds_clean)
          adds_clean <- adds_clean[!is.na(geo_add1_clean)] # only keep coded addresses
          adds_clean[, "geo_po_box_clean" := NULL] # Want street address, not PO BOX
          
          message(paste0(rads::round2(100*nrow(adds_clean)/nrow(adds_to_clean), 1), 
                         "% of the addresses that were not previously geocoded (", 
                         format(nrow(adds_clean), big.mark = ','), 
                         " of ", 
                         format(nrow(adds_to_clean), big.mark = ','), 
                         ") were successfully geocoded"))
      } else {message("All addresses were previously geocoded.")}
    
    ## Append all clean addresses together ----
        if (nrow(adds_to_clean) > 0) {
          adds_final <- bind_rows(adds_already_clean, adds_clean)
        } else {
          adds_final <- adds_already_clean
        }
        
        rads::sql_clean(adds_final)
      
    ## Join clean addresses back to original data ----
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
    
    # Create hh_names based on when hh_ssn == ssn
      # change to lapply / data.table because too much time wasted messing with tidy equivalents
      sha_raw <- lapply(X = sha_raw, 
                         FUN = function(X){
                           if('hh_lname' %in% names(X) & 'cert_id' %in% names(X)){
                             hh_names <- unique(copy(X)[hh_ssn == ssn, 
                                                 .(cert_id, act_date, hh_ssn, hh_lname.y = lname, hh_fname.y = fname, hh_mname.y = mname)])
                             X <- merge(X, hh_names, by = c("cert_id", "act_date", "hh_ssn"), all.x = T, all.y = F)
                             X[is.na(hh_lname) & !is.na(hh_lname.y), `:=` (hh_lname = hh_lname.y, hh_fname = hh_fname.y, hh_mname = hh_mname.y)]
                             X[, c("hh_lname.y", "hh_fname.y", "hh_mname.y") := NULL]
                           } else {X}
                           return(X)
                         })  

  # FILL IN GAPS ----
    sha_raw <- sha_raw %>%
      map(~ if ("source" %in% names(.x)){
        .x %>%
            mutate(major_prog = case_when(str_detect(pha_source, "hcv") ~ "HCV",
                                          str_detect(pha_source, "ph") ~ "PH",
                                          tolower(source) == "public housing" ~ "PH", # when lacks hcv & ph, then ascribe value of 'source' column 
                                          tolower(source) == "hcv" ~ "HCV", # when lacks hcv & ph, then ascribe value of 'source' column 
                                          str_detect(tolower(prog_type), "owned") ~ "PH",
                                          str_detect(tolower(prog_type), "tenant") ~ "HCV",
                                          str_detect(tolower(prog_type), "collaborative") ~ "HCV",
                                                     TRUE ~ NA_character_)) %>%
          select(-source)
      } else {
        .x %>%
          mutate(major_prog = case_when(str_detect(pha_source, "hcv") ~ "HCV",
                                        str_detect(pha_source, "ph") ~ "PH",
                                        str_detect(tolower(prog_type), "owned") ~ "PH",
                                        str_detect(tolower(prog_type), "tenant") ~ "HCV",
                                        str_detect(tolower(prog_type), "collaborative") ~ "HCV",
                                        TRUE ~ NA_character_))
      }
      )
  
  
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
    setDT(sha_long)
    
    sha_long[mtw_program == 'Y', mtw_program := 'Yes']
    sha_long[mtw_program == 'N', mtw_program := 'No']  
  
  # QA FINAL DATA ----
    ## Row counts compared to last time ----
      # refreshed table will be total of rows in sha_long plus whatever exists right now in SQL
      rows_refresh <- nrow(sha_long) + 
        as.integer(dbGetQuery(conn, glue_sql("SELECT count(*) FROM {`to_schema`}.{`to_table`}", .con = conn)))
      
      # previous rows from last refresh is stored in metadata table
      rows_previous <- as.integer(dbGetQuery(conn,
                                             glue_sql("SELECT qa_result FROM {`qa_schema`}.{`qa_table`}
                                           WHERE qa_type = 'value' AND qa_item = 'row_count' AND
                                           table_name = '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}'
                                           ORDER BY qa_date desc",
                                                      .con = conn))[1,])
      
      if (is.na(rows_previous)) {
        qa_row_diff_result <- "PASS"
        qa_row_diff_note <- glue("There was no existing row data to compare to last time")
        message(paste("\U0001f642", qa_row_diff_note))
      } else if (!is.na(rows_previous) & rows_refresh >= rows_previous) {
        qa_row_diff_result <- "PASS"
        qa_row_diff_note <- glue("There are {format(rows_refresh - rows_previous, big.mark = ',')}", 
                                 " more rows in the latest stage table")
        message(paste("\U0001f642", qa_row_diff_note))
      } else if (!is.na(rows_previous) & rows_refresh < rows_previous) {
        qa_row_diff_result <- "FAIL"
        qa_row_diff_note <- glue("There were {format(rows_previous - rows_refresh, big.mark = ',')}", 
                                 " fewer rows in the latest stage table. See why this is.")
        warning(paste("\U00026A0", qa_row_diff_note))
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
        if (length(setdiff(names(cols_current), names(sha_long))) > 1) {
          qa_names_result <- "WARNING"
          qa_names_note <- glue("The existing stage table has columns not found in the new data")
          warning(paste("\U00026A0", qa_names_note))
        } else if (length(setdiff(names(sha_long), names(cols_current))) > 1) {
          qa_names_result <- "FAIL"
          qa_names_note <- glue("The new stage table has columns not found in the current data")
          warning(paste("\U00026A0", qa_names_note))
        } else {
          qa_names_result <- "PASS"
          qa_names_note <- glue("The existing and new stage tables have matching columns")
          message(paste("\U0001f642", qa_names_note))
        }
      } else {
        qa_names_result <- "PASS"
        qa_names_note <- glue("There was no existing column data to compare to last time")
        message(paste("\U0001f642", qa_names_note))
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
  ## Load data ----
  # Split into smaller tables to avoid SQL connection issues
  message("As of 3/2/2023, this code only processes and loads data that is not already in stage.")
  message("For this reason, we are appending (not overwriting) the data in the lapply below.")
      
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
                   overwrite = F, append = T)
    } else {
      dbWriteTable(conn,
                   name = DBI::Id(schema = to_schema, table = to_table),
                   value = as.data.frame(sha_long[start_row:end_row ,]),
                   overwrite = F, append = T)
    }
  })
  
}
