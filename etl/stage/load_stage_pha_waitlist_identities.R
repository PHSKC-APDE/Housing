#### CODE TO COMBINE KING COUNTY HOUSING AUTHORITY AND SEATTLE HOUSING AUTHORITY IDENTITIES
# Alastair Matheson, PHSKC (APDE)
#
# 2021-07
#
# Run from main_pha_waitlist_load script
# https://github.com/PHSKC-APDE/Housing/blob/master/claims_db/etl/db_loader/main_pha_waitlist_load.R
# Assumes relevant libraries are already loaded
#
#
# conn = ODBC connection to use
# to_schema = name of the schema to load data to
# to_table = name of the table to load data to
# from_schema = name of the schema the input data are in
# from_table = common prefix of the table the input data are in
# qa_schema = name of the schema the QA lives in (likely the same as the to_schema if working in HHSAW)
# qa_table = name of the table that holds QA outcomes
# file_path = where the KCHA data files live (note that the file names themselves are hard coded for now)
# years = which years to load to the stage table
# truncate = whether to remove existing stage data from selected years first (default is TRUE).
#    NB. any existing data from other years will remain intact regardless


# Set these up manually for now but possibly use a function later
qa_schema <- "pha"
qa_table <- "metadata_qa"
from_schema <- "pha"
from_table <- "stage_"
to_schema <- "pha"
to_table <- "stage_identities"
final_schema <- "pha"
final_table <- "final_identities"


# BRING IN DATA ----
# If an identity table already exists, bring this in too
if (dbExistsTable(conn, name = DBI::Id(schema = final_schema, table = final_table))) {
  names_existing <- dbGetQuery(conn, 
                               glue_sql("SELECT * FROM {`final_schema`}.{`final_table`}",
                                        .con = conn))
  
  # Only bring in names that don't already exist in the final table
  names <- dbGetQuery(
    conn,
    glue_sql("SELECT a.*, b.present FROM 
               (SELECT DISTINCT ssn, lname, fname, mname, dob, 
               CASE WHEN gender = 2 THEN 0 ELSE gender END AS female 
               FROM {`from_schema`}.{`from_table`}) a
               LEFT JOIN
               (SELECT ssn, lname, fname, mname, dob, female, 1 AS present
               FROM {`final_schema`}.{`final_table`}) b
               ON (a.ssn = b.ssn OR (a.ssn IS NULL AND b.ssn IS NULL)) AND
               (a.lname = b.lname OR (a.lname IS NULL AND b.lname IS NULL)) AND 
               (a.fname = b.fname OR (a.fname IS NULL AND b.fname IS NULL)) AND
               (a.mname = b.mname OR (a.mname IS NULL AND b.mname IS NULL)) AND
               (a.dob = b.dob OR (a.dob IS NULL AND b.dob IS NULL)) AND
               (a.female = b.female OR (a.female IS NULL AND b.female IS NULL))
               WHERE b.present IS NULL",
             .con = conn)) %>%
    select(-present)
} else {
  names <- dbGetQuery(
    conn,
    glue_sql("SELECT DISTINCT ssn, lname, fname, mname, dob, 
               CASE WHEN gender = 2 THEN 0 ELSE gender END AS female 
               FROM {`from_schema`}.{`from_table`}",
             .con = conn))
}


# FUNCTIONS ----
## Adaptation of Carolina's code ----
# From here: https://github.com/DCHS-PME/PMEtools/blob/master/R/idm_dedup.R
# pairs_input = Output from a RecordLinkage getPairs function
# df = The data frame that was fed into the matching process. 
#      Must have rowid and id_hash fields
# iteration = What match cycle this is (affects cluster ID suffix)

match_process <- function(pairs_input, df, iteration) {
  ### Attach ids for each individual pairwise link found ----
  pairs <- pairs_input %>%
    distinct(id1, id2) %>%
    left_join(df, ., by = c(rowid = "id1"))
  # In case the input was a data table, convert to data frame
  # (otherwise the recursion breaks after 1 round)
  pairs <- data.table::setDF(pairs)
  
  ### Roll up pair combinations ----
  # self-join to consolidate all pair combinations for clusters with > 2 identities linked 
  # roll up cluster id correctly with coalesce
  # formula for how many other_pair2 records should exist for n number of matching records: 
  #   (n*(n-1)/2) + 1 - e.g. 3 carolina johnsons will have 4  records (3*2/2+1)
  remaining_dupes <- sum(!is.na(pairs$id2))
  
  # while loop self-joining pairs until no more open pairs remain
  recursion_level <- 0
  recursion_df <- pairs %>% rename(id2_recur0 = id2)
  while (remaining_dupes > 0) {
    recursion_level <- recursion_level + 1
    print(paste0(remaining_dupes, " remaining duplicated rows. Starting recursion iteration ", recursion_level))
    recursion_df <- pairs %>%
      self_join_dups(base_df = ., iterate_df = recursion_df, iteration = recursion_level)
    remaining_dupes <- sum(!is.na(recursion_df[ , paste0("id2_recur", recursion_level)]))
  }
  
  # identify full list of id columns to coalesce after recursion
  recurcols <- tidyselect::vars_select(names(recursion_df), matches("_recur\\d")) %>%
    sort(decreasing = T)
  coalesce_cols <- c(recurcols, "rowid")
  coalesce_cols <- rlang::syms(coalesce_cols)
  
  # coalesce recursive id columns in sequence to generate single common cluster ID
  pairsend <- recursion_df %>%
    mutate(clusterid = coalesce(!!!coalesce_cols)) %>%
    rename(id2 = id2_recur0) %>%
    select(-contains("_recur")) %>%
    distinct()
  
  # identify any unclosed cluster groups (open triangle problem), resulting in duplicated cluster
  double_dups <- pairsend %>%
    select(rowid, clusterid) %>%
    distinct() %>%
    group_by(rowid) %>%
    filter(n() > 1) %>%
    group_by(clusterid) %>%
    mutate(row_min = min(rowid)) %>%
    ungroup() %>%
    rename(back_join_id = row_min) %>%
    select(-rowid) %>%
    distinct()
  
  
  # collapse duplicate partial clusters to one cluster
  # error checking to make sure that correct total clusters are maintained
  if (nrow(double_dups) > 0) {
    pairsend <- left_join(pairsend, double_dups, by = c(clusterid = "clusterid")) %>%
      mutate(clusterid2 = coalesce(back_join_id, clusterid))
    
    message("There are ", sum(pairsend$clusterid != pairsend$clusterid2), 
            " mismatched clusterid/clusterid2 combos and at least ",
            nrow(double_dups)*2, " expected")
    
    pairsend <- pairsend %>%
      mutate(clusterid = clusterid2) %>%
      select(-clusterid2, -back_join_id)
  }
  
  ### Add identifiers/unique ids for paired records ----
  # overwrite the original pairs with the consolidated & informed dataframe
  pairs_final <- df %>%
    rename_all(~ paste0(., "_b")) %>%
    left_join(pairsend, ., by = c(id2 = "rowid_b"))
  
  ### Take the union of all unique ids with their cluster ids ----
  # (swinging links from _b cols to unioned rows, and taking distinct)
  # create cluster index
  cluster_index <- select(pairs_final, clusterid, id_hash = id_hash_b) %>%
    drop_na() %>%
    bind_rows(select(pairs_final, clusterid, id_hash)) %>%
    distinct()
  
  ### Check that each personal id only in one cluster ----
  n_pi_split <- pairs_final %>% 
    group_by(id_hash) %>% 
    filter(n_distinct(clusterid) > 1) %>%
    n_distinct()
  
  if (n_pi_split) {
    stop(glue::glue("Deduplication processing error: {nrow(n_pi_split)} ",
                    "clients sorted into more than one cluster. ", 
                    "This is an internal failure in the function and will require debugging. ", 
                    "Talk to package maintainer)"))
  }
  
  ### Report results ----
  n_orig_ids <- df %>% select(id_hash) %>% n_distinct()
  n_cluster_ids <- n_distinct(cluster_index$clusterid)
  
  message("Number of unique clients prior to deduplication: ", n_orig_ids, 
          ". Number of deduplicated clients: ", n_cluster_ids)
  
  
  ### Attach cluster IDS back to base file ----
  output <- left_join(df, 
                      # Set up iteration name
                      rename(cluster_index, 
                             !!quo_name(paste0("clusterid_", iteration)) := clusterid), 
                      by = "id_hash")
  output
}


## Helper functions specifically for client deduplication
#' Function for joining duplicated records to base pair, used in recursive deduplication
#' @param base_df The starting dataframe with initial duplicated pair ids
#' @param iterate_df The df with iterated rowid joins - what is continually updated during recursive pair closing
#' @param iteration Numeric counter indicating which recursion iteration the self-joining loop is on. Used for column name suffixes
self_join_dups <- function(base_df, iterate_df, iteration) {
  joinby <- paste0("rowid_recur", iteration)
  names(joinby) <- paste0("id2_recur", iteration-1)
  
  base_df %>%
    select(rowid, id2) %>%
    rename_all(~paste0(., "_recur", iteration)) %>%
    left_join(iterate_df, ., by = joinby)
}


# This function creates a vector of unique IDs of any length
# id_n = how many unique IDs you want generated
# id_length = how long do you want the ID to get (too short and you'll be stuck in a loop)
id_nodups <- function(id_n, id_length, seed = 98104) {
  set.seed(seed)
  id_list <- stringi::stri_rand_strings(n = id_n, length = id_length, pattern = "[a-z0-9]")
  
  # If any IDs were duplicated (very unlikely), overwrite them with new IDs
  iteration <- 1
  while(any(duplicated(id_list)) & iteration <= 50) {
    id_list[which(duplicated(id_list))] <- stringi::stri_rand_strings(n = sum(duplicated(id_list), na.rm = TRUE),
                                                                      length = id_length,
                                                                      pattern = "[a-z0-9]")
    iteration <<- iteration + 1
  }
  
  if (iteration == 50) {
    stop("After 50 iterations there are still duplicate IDs. ",
         "Either decrease id_n or increase id_length")
  } else {
    return(id_list)
  }
}


# SET UP DATA ----
## Final pool of identities
if (exists("names_existing")) {
  names_list <- bind_rows(mutate(names_existing, source = "final"), 
                          mutate(names, source = "waitlist"))
} else {
  names_list <- names %>% mutate(source = "waitlist")
}


## Add in variables for matching ----
names_list <- names_list %>%
  mutate(ssn_id = case_when(!is.na(ssn) ~ ssn,
                            !is.na(pha_id) ~ pha_id,
                            TRUE ~ NA_character_),
         lname_phon = RecordLinkage::soundex(lname),
         fname_phon = RecordLinkage::soundex(fname),
         dob_y = lubridate::year(dob),
         dob_m = lubridate::month(dob),
         dob_d = as.numeric(lubridate::day(dob)),
         # Make a hash of variables that are used for identity matching
         id_hash = ifelse(!is.na(id_hash), id_hash,
                          as.character(toupper(openssl::sha256(paste(str_replace_na(ssn, ''),
                                                              str_replace_na(pha_id, ''),
                                                              str_replace_na(lname, ''),
                                                              str_replace_na(fname, ''),
                                                              str_replace_na(mname, ''),
                                                              str_replace_na(dob, ''),
                                                              str_replace_na(female, ''),
                                                              sep = "|"))))),
         rowid = row_number()) %>%
  select(-last_run)


# FIRST PASS: BLOCK ON SSN OR PHA ID ----
## Run deduplication ----
# Blocking on SSN or PHA ID and string compare names
st <- Sys.time()
match_01 <- RecordLinkage::compare.dedup(
  names_list, 
  blockfld = "ssn_id", 
  strcmp = c("lname", "fname", "mname", "dob_y", "dob_m", "dob_d", "female"), 
  exclude = c("ssn", "pha_id", "lname_phon", "fname_phon", "dob", "rowid", 
              "id_hash", "id_kc_pha", "source"))
message("Pairwise comparisons complete. Total run time: ", round(Sys.time() - st, 2), " ", units(Sys.time()-st))

summary(match_01)


## Add weights and extract pairs ----
# Using EpiLink approach
match_01 <- epiWeights(match_01)
classify_01 <- epiClassify(match_01, threshold.upper = 0.6)
summary(classify_01)
pairs_01 <- getPairs(classify_01, single.rows = TRUE) %>%
  mutate(across(contains("dob_"), ~ str_squish(.)))


## Review output and select cutoff point(s) ----
pairs_01 %>% 
  # Only keep matches with waitlist data in it
  filter(!(source.1 == "final" & source.2 == "final")) %>%
  filter(Weight >= 0.67 & !((dob_m.1 == "1" & dob_d.1 == "1") | (dob_m.2 == "1" & dob_d.2 == "1")) &
           dob_y.1 != dob_y.2 & dob_m.1 == dob_m.2 & dob_d.1 == dob_d.2) %>%
  # filter(Weight >= 0.68 & !((dob_m.1 == "1" & dob_d.1 == "1") | (dob_m.2 == "1" & dob_d.2 == "1")) &
  #          dob_y.1 == dob_y.2 & (dob_m.1 != dob_m.2 | dob_d.1 != dob_d.2)) %>%
  # filter(Weight >= 0.58 & !((dob_m.1 == "1" & dob_d.1 == "1") | (dob_m.2 == "1" & dob_d.2 == "1")) &
  #          dob_y.1 == dob_y.2 & dob_m.1 == dob_m.2 & dob_d.1 == dob_d.2) %>%
  # filter(Weight >= 0.71 & ((dob_m.1 == "1" & dob_d.1 == "1") | (dob_m.2 == "1" & dob_d.2 == "1")) &
  #          dob_y.1 == dob_y.2) %>%
  # filter(Weight >= 0.729 & ((dob_m.1 == "1" & dob_d.1 == "1") | (dob_m.2 == "1" & dob_d.2 == "1")) &
  #          dob_y.1 != dob_y.2) %>%
  select(id1, ssn_id.1, lname.1, fname.1, mname.1, dob.1, female.1, id_kc_pha.1, source.1,
         id2, ssn_id.2, lname.2, fname.2, mname.2, dob.2, female.2, id_kc_pha.2, source.2,
         Weight) %>%
  tail()



pairs_01_trunc <- pairs_01 %>%
  # Only keep matches with waitlist data in it
  filter(!(source.1 == "final" & source.2 == "final")) %>%
  filter(
    # Non-Jan 1 birth dates
    (!((dob_m.1 == "1" & dob_d.1 == "1") | (dob_m.2 == "1" & dob_d.2 == "1")) &
       (
         # Full DOB match
         (Weight >= 0.59 & dob_y.1 == dob_y.2 & dob_m.1 == dob_m.2 & dob_d.1 == dob_d.2) |
           # Year mismatch but month and day match
           (Weight >= 0.7 & dob_y.1 != dob_y.2 & dob_m.1 == dob_m.2 & dob_d.1 == dob_d.2) |
           # Year match but month or day mismatch
           (Weight >= 0.67 & dob_y.1 == dob_y.2 & (dob_m.1 != dob_m.2 | dob_d.1 != dob_d.2))
       )) |
      # Jan 1 birth dates
      (((dob_m.1 == "1" & dob_d.1 == "1") | (dob_m.2 == "1" & dob_d.2 == "1")) & 
         (
           # Year match
           (Weight >= 0.71 & dob_y.1 == dob_y.2) |
             # Year non-match (actually works out with same Weight)
             (Weight >= 0.729 & dob_y.1 != dob_y.2)
         ))
  )


## Collapse IDs ----
match_01_dedup <- match_process(pairs_input = pairs_01_trunc, 
                                df = names_list, iteration = 1)


## Error check ----
match_01_chk <- setDT(match_01_dedup %>% distinct(id_hash, clusterid_1))
match_01_chk[, cnt := .N, by = "id_hash"]

if (max(match_01_chk$cnt) > 1) {
  stop("Some id_hash values are associated with multiple clusterid_1 values. ",
       "Check what went wrong.")
}


# SECOND PASS: BLOCK ON PHONETIC LNAME, FNAME AND DOB ----
## Run deduplication ----
st <- Sys.time()
match_02 <- RecordLinkage::compare.dedup(
  names_list, 
  blockfld = c("lname_phon", "fname_phon", "dob_y", "dob_m", "dob_d"), 
  strcmp = c("ssn_id", "lname", "fname", "mname", "female"), 
  exclude = c("ssn", "pha_id", "dob", "rowid", "id_hash", "id_kc_pha", "source"))
message("Pairwise comparisons complete. Total run time: ", round(Sys.time() - st, 2), " ", units(Sys.time()-st))

summary(match_02)


## Add weights and extract pairs ----
# Using EpiLink approach
match_02 <- epiWeights(match_02)
classify_02 <- epiClassify(match_02, threshold.upper = 0.6)
summary(classify_02)
pairs_02 <- getPairs(classify_02, single.rows = TRUE) %>%
  mutate(across(contains("dob_"), ~ str_squish(.)))

## Review output and select cutoff point(s) ----
pairs_02 %>% filter(Weight <= 0.8 & ssn_id.1 != ssn_id.2) %>% select(-contains("id_hash")) %>% head()

pairs_02 %>% 
  # Only keep matches with waitlist data in it
  filter(!(source.1 == "final" & source.2 == "final")) %>%
  # select(-contains("id_hash")) %>% 
  filter(Weight >= 0.8748) %>% 
  filter(ssn_id.1 != ssn_id.2) %>%
  # filter(is.na(ssn.1) | is.na(ssn.2)) %>%
  # filter(!is.na(ssn.1) & !is.na(ssn.2)) %>%
  # filter(!(dob_m.1 == "1" & dob_d.1 == "1")) %>%
  filter(dob_m.1 == "1" & dob_d.1 == "1") %>%
  select(id1, ssn_id.1, lname.1, fname.1, mname.1, dob.1, female.1, id_kc_pha.1, source.1,
         id2, ssn_id.2, lname.2, fname.2, mname.2, dob.2, female.2, id_kc_pha.2, source.2,
         Weight) %>%
  tail()


pairs_02_trunc <- pairs_02 %>%
  # Only keep matches with waitlist data in it
  filter(!(source.1 == "final" & source.2 == "final")) %>%
  filter((
    (ssn_id.1 == ssn_id.2) |
      (Weight >= 0.87 & ssn_id.1 != ssn_id.2 & !(dob_m.1 == "1" & dob_d.1 == "1")) |
      (Weight >= 0.875 & ssn_id.1 != ssn_id.2 & dob_m.1 == "1" & dob_d.1 == "1") |
      (Weight >= 0.72 & ssn_id.1 != ssn_id.2 & (is.na(ssn.1) | is.na(ssn.2)))
  ))


## Collapse IDs ----
match_02_dedup <- match_process(pairs_input = pairs_02_trunc, df = names_list, iteration = 2) %>%
  mutate(clusterid_2 = clusterid_2 + max(match_01_dedup$clusterid_1))

## Error check ----
match_02_chk <- setDT(match_02_dedup %>% distinct(id_hash, clusterid_2))
match_02_chk[, cnt := .N, by = "id_hash"]

if (max(match_02_chk$cnt) > 1) {
  stop("Some id_hash values are associated with multiple clusterid_2 values. ",
       "Check what went wrong.")
}



# BRING MATCHING ROUNDS TOGETHER ----
# Use clusterid_1 as the starting point, find where one clusterid_2 value
# is associated with multiple clusterid_1 values, then take the min of the latter.
# This would need to made iterative if there is more than two matching processes.

ids_dedup <- setDT(full_join(select(match_01_dedup, id_hash, clusterid_1), 
                             select(match_02_dedup, id_hash, clusterid_2),
                             by = "id_hash"))

ids_dedup[, clusterid := min(clusterid_1), by = "clusterid_2"]


## Error check ----
ids_dedup_chk <- unique(ids_dedup[, c("id_hash", "clusterid")])
ids_dedup_chk[, cnt_id := .N, by = "id_hash"]
ids_dedup_chk[, cnt_hash := .N, by = "clusterid"]
# cnt_id should = 1 and cnt_hash should be >= 1
ids_dedup_chk %>% count(cnt_id, cnt_hash)
if (max(ids_dedup_chk$cnt_id) > 1) {
  stop("There is more than one cluster ID for a given id_has. Investigate why.")
}


## Make an alpha-numeric ID that will be stored in a table ----
new_ids_needed <- n_distinct(ids_dedup$clusterid)
if (exists("names_existing")) {
  # Pull in existing IDs to make sure they are not repeated
  ids_existing <- unique(names_existing$id_kc_pha)
  
  # If using the same seed as was used to create other IDs, can just tack on
  # the number of new IDs needed
  ids_final <- id_nodups(id_n = length(ids_existing) + new_ids_needed,
                         id_length = 10)
  
  ids_final_dedup <- ids_final[!ids_final %in% ids_existing]
  
  # Make sure there are enough new IDs
  if (length(ids_final_dedup) < new_ids_needed) {
    # Run again but make it longer
    ids_final <- id_nodups(id_n = length(ids_existing) + new_ids_needed * 3,
                           id_length = 10)
    
    ids_final_dedup <- ids_final[!ids_final %in% ids_existing]
    rm(ids_final_dedup)
  }
  # Check again
  if (length(ids_final_dedup) < new_ids_needed) {
    stop("Could not generate enough new IDs")
  }
  
  # Trim to correct size
  ids_final <- ids_final_dedup[1:new_ids_needed]
} else {
  ids_final <- id_nodups(id_n = new_ids_needed, id_length = 10)
}

# Join to cluster IDs
ids_final <- ids_dedup %>%
  distinct(clusterid) %>%
  arrange(clusterid) %>%
  bind_cols(., id_kc_pha = ids_final)

# Join to make final list of names and IDs
names_final <- names_list %>%
  select(ssn, pha_id, lname, fname, mname, dob, female, id_hash) %>%
  left_join(., select(ids_dedup, id_hash, clusterid), by = "id_hash") %>%
  left_join(., ids_final, by = "clusterid") %>%
  select(-clusterid) %>%
  distinct() %>%
  mutate(last_run = Sys.time())


## Consolidate IDs ----
if (exists("names_existing")) {
  # Easiest to assign a number to each ID so taking the min works to consolidate groups
  # Quicker to use data table to set these up
  names_existing <- setDT(names_existing)
  setnames(names_existing, "id_kc_pha", "id_kc_pha_old")
  names_existing[, `:=` (hash_cnt_old = .N, id_num_old = .GRP), by = "id_kc_pha_old"]
  
  names_final <- setDT(rename(names_final, id_kc_pha_new = id_kc_pha))
  names_final[, `:=` (hash_cnt_new = .N, id_num_new = .GRP), by = "id_kc_pha_new"]
  names_final[, id_num_new := id_num_new + length(ids_existing)]
  
  # Bring old and new together
  names_dedup <- merge(names_existing[, .(id_hash, id_kc_pha_old, hash_cnt_old, id_num_old)],
                       names_final[, .(id_hash, id_kc_pha_new, hash_cnt_new, id_num_new)],
                       by = "id_hash", all = T)
  
  # Check for unclosed groups in either direction
  names_dedup[!is.na(id_kc_pha_old), id_min_new := min(id_num_new, na.rm = T), by = "id_kc_pha_old"]
  names_dedup[!is.na(id_kc_pha_new), id_min_old := min(id_num_old, na.rm = T), by = "id_kc_pha_new"]
  
  # Replace infinite values created above
  names_dedup[is.infinite(id_min_old), id_min_old := NA]
  names_dedup[is.infinite(id_min_new), id_min_new := NA]
  
  # Any rows with blank id_min_new and id_min_old are those that didn't match at all. Just bring over id_num_new
  names_dedup[is.na(id_min_new) & is.na(id_min_old), id_min_new := id_num_new]
  
  # Consolidate so old number is preferentially kept
  names_dedup[, id_num_final := coalesce(id_min_old, id_min_new)]
  
  # Make a reshaped list of IDs and ID numbers to get final ID
  id_num_list <- bind_rows(distinct(names_dedup, id_kc_pha_old, id_num_old) %>% 
                             filter(!is.na(id_kc_pha_old)) %>%
                             rename(id_kc_pha_final = id_kc_pha_old,
                                    id_num_final = id_num_old),
                           distinct(names_dedup, id_kc_pha_new, id_num_new) %>% 
                             rename(id_kc_pha_final = id_kc_pha_new, 
                                    id_num_final = id_num_new))
  
  # Check the number of IDs matches what is expected
  if (nrow(id_num_list) != length(ids_existing) + new_ids_needed) {
    stop("Mismatched number of rows in id_num_list (too many or too few)")
  }
  
  # Join to get the final ID
  names_dedup <- left_join(select(names_dedup, id_hash, id_kc_pha_old, id_kc_pha_new, id_num_final),
                           id_num_list,
                           by = "id_num_final") %>%
    select(-id_num_final)
  
  
  ## Make final names table ----
  # Set up last_run time
  run_time <- Sys.time()
  names_final <- left_join(names_final, select(names_dedup, id_hash, id_kc_pha_final),
                           by = "id_hash") %>%
    select(ssn, pha_id, lname, fname, mname, dob, female, id_hash, id_kc_pha_final) %>%
    rename(id_kc_pha = id_kc_pha_final) %>%
    mutate(last_run = run_time)
  
  
  # UPDATE ID TABLE ----
  # Want to keep a record of a person's ID over time
  # Assumes a table called [final_schema].[final_table]_history exists
  # Could make a dynamic part of a function later
  
  # Bring in history table
  id_history <- dbGetQuery(
    conn, glue_sql("SELECT * FROM {`final_schema`}.{DBI::SQL(paste0(final_table, '_history'))}",
                   .con = conn))
  # Fix any format issues
  id_history <- id_history %>%
    mutate(across(c("from_date", "to_date", "last_run"), ~ as.POSIXct(.)))
  
  
  # Find IDs that changed and update their to_date
  id_changed <- names_dedup %>% 
    filter(!is.na(id_kc_pha_old) & id_kc_pha_old != id_kc_pha_final) %>%
    select(id_hash) %>%
    mutate(changed = 1L)
  
  id_history_updated <- left_join(id_history, id_changed, by = "id_hash") %>%
    mutate(to_date = as.POSIXct(ifelse(changed == 1, run_time, to_date), origin = "1970-01-01", tz = "utc")) %>%
    select(-changed)
  
  # Add in new or changed IDs and set from date
  id_new <- names_dedup %>% 
    filter(is.na(id_kc_pha_old) | id_kc_pha_old != id_kc_pha_final) %>%
    select(id_hash, id_kc_pha_final) %>%
    rename(id_kc_pha = id_kc_pha_final) %>%
    mutate(from_date = run_time, to_date = as.POSIXct(NA))
  
  id_history_updated <- bind_rows(id_history_updated, id_new)
  
  # Replace last_run date
  id_history_updated <- id_history_updated %>%
    mutate(last_run = lubridate::with_tz(run_time, tzone = "utc"))
  
}



# QA FINAL DATA ----
# Number of id_hashes compared to the number of id_kc_phas
message("There are ", n_distinct(names_final$id_hash), " IDs and ", 
        n_distinct(names_final$id_kc_pha), " id_kc_phas")


## New clusters compared to last time ----



## Check combined table is longer than the original ----


## ID history table rows compared to last time ----



# ADD VALUES TO METADATA ----
# Row counts
DBI::dbExecute(conn,
               glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`} 
                          (etl_batch_id, last_run, table_name, 
                            qa_type, qa_item, qa_result, qa_date, note) 
                          VALUES (NULL, {min(names_final$last_run)}, 
                                  '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                                  'value', 'row_count', {nrow(names_final)},
                                  {Sys.time()}, NULL)",
                        .con = conn))

# Number of IDs
DBI::dbExecute(conn,
               glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`} 
                          (etl_batch_id, last_run, table_name, 
                            qa_type, qa_item, qa_result, qa_date, note) 
                          VALUES (NULL, {min(names_final$last_run)}, 
                                  '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                                  'value', 'id_count', {n_distinct(names_final$id_kc_pha)},
                                  {Sys.time()}, NULL)",
                        .con = conn))


# CHECK QA PASSED ----
# Stop processing if one or more QA check failed
if (min(qa_clusters_result, qa_row_diff_result) == "FAIL") {
  stop("One or more QA checks failed on {to_schema}.{to_table}. See {qa_schema}.{qa_table} for more details.")
} else {
  # Clean up QA objects if everything passed
  rm(list = ls(pattern = "^qa_"))
}


# LOAD DATA TO SQL ----
## Identities ----
# Split into smaller tables to avoid SQL connection issues
start <- 1L
max_rows <- 50000L
cycles <- ceiling(nrow(names_final)/max_rows)

lapply(seq(start, cycles), function(i) {
  start_row <- ifelse(i == 1, 1L, max_rows * (i-1) + 1)
  end_row <- min(nrow(names_final), max_rows * i)
  
  message("Loading cycle ", i, " of ", cycles)
  if (i == 1) {
    dbWriteTable(conn,
                 name = DBI::Id(schema = to_schema, table = to_table),
                 value = as.data.frame(names_final[start_row:end_row, ]),
                 overwrite = T, append = F)
  } else {
    dbWriteTable(conn,
                 name = DBI::Id(schema = to_schema, table = to_table),
                 value = as.data.frame(names_final[start_row:end_row ,]),
                 overwrite = F, append = T)
  }
})

## Identity history ----
# Split into smaller tables to avoid SQL connection issues
start <- 1L
max_rows <- 50000L
cycles <- ceiling(nrow(id_history_updated)/max_rows)

lapply(seq(start, cycles), function(i) {
  start_row <- ifelse(i == 1, 1L, max_rows * (i-1) + 1)
  end_row <- min(nrow(id_history_updated), max_rows * i)
  
  message("Loading cycle ", i, " of ", cycles)
  if (i == 1) {
    dbWriteTable(conn,
                 name = DBI::Id(schema = to_schema, table = paste0(to_table, "_history")),
                 value = as.data.frame(id_history_updated[start_row:end_row, ]),
                 overwrite = T, append = F)
  } else {
    dbWriteTable(conn,
                 name = DBI::Id(schema = to_schema, table = paste0(to_table, "_history")),
                 value = as.data.frame(id_history_updated[start_row:end_row ,]),
                 overwrite = F, append = T)
  }
})
