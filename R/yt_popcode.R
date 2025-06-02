#' Assign people to a demographic group for a given year, prioritize YT.
#' 
#' \code{yt_popcode} assigns people in the SHA data to a demographic group.
#' 
#' This function is specific to Seattle Housing Authority's data. It assigns a 
#' numeric code to each row in the data where person-time has accumulated for 
#' that calendar year. Codes are assigned as follows:
#' (#1-5 assume simultaneous Medicaid enrollment and not dual eligible):
#' 1) At least 30 days at YT that year = YT, 
#' 2) No YT but 30+ days at SS that year = SS, 
#' 3) No YT or SS but in SHA for 30+ days = SHA, 
#' 4) Not YT/SS/SHA but in KCHA for 30+ days = KCHA, 
#' 5) No 30+ days in any PHA = non-PHA Medicaid, 
#' 6-10) Same as #1-5 but only dual eligible, 
#' 11-15) Same as #1-5 but dual eligible is NA, 
#' 16-18) Only time in SHA (+/- YT and SS) and no Medicaid, 
#' 19) Only time in KCHA and no Medicaid
#' 
#' If the min flag is true, only takes the smallest numeric code produced is 
#' kept for that year and new demographic variables are calculated. 
#' Taking the smallest code prioritizes a person's time at Yesler Terrace when 
#' they were also enrolled in Medicaid.
#' 
#' It is assumed that the df has variables for housing authority, enrollment 
#' type, dual eligibility, and person-time for that calendar year. Also assumes
#' that \code{\link{yt_flag}} has been run to identify people living at YT
#' or scattered sites.
#' 
#' Eventually could expand to accommodate non-calendar years using the 
#' \code{\link{time_range}} function.
#' 
#' @param df A data frame
#' @param year An integer that describes the calendar year being examined. 
#' Used to identify the person-time variable.
#' @param year_pre A string that prefixes the year in the person-time variable.
#' @param year_suf A string that suffixes the year in the person-time variable.
#' @param agency A named variable that specifies the agency.
#' @param enroll_type A named variable that specifies the enrollment type.
#' @param dual A named variable that specifies a person's dual eligibility.
#' @param yt A named variable that specifies the Yesler Terrace flag. Must be 
#' binary (0/1).
#' @param ss A named variable that specifies the scattered sites flag. Must be
#' binary (0/1).
#' @param pt_cut The threshold in person-days a person should meet in order to 
#' be considered at YT/SS in that year
#' @param min Flag to keep the minimum pop_code.
#' 
#' 
#' @examples 
#' \dontrun{
#' yt_popcode(yt_mcaid_final, year_pre = "pt", year = 12, min = T)
#' }
#' 
#' @importFrom data.table setDT `:=`
#' @import dplyr
#' @export


yt_popcode <- function(df, year = 12, year_pre = "pt", year_suf = NULL,
                       agency = NULL, enroll_type = NULL, dual = NULL,
                       yt = NULL, ss = NULL, pt_cut = 30, min = T) {
  
  # Set up variables
  pt <- rlang::sym(paste0(year_pre, year, year_suf))

  if (nchar(year) == 2) {
    year_num = as.numeric(paste0(20, year))
  } else if (nchar(year) == 4) {
    year_num = as.numeric(year)
  } else {
    stop("Invalid year format. Should be 2 or 4 digits")
  }
  
  # Figure out which agency field to use
  if(!missing(agency)) {
    agency <- enquo(agency)
  } else if("agency_new" %in% names(df)) {
    agency <- quo(agency_new)
  } else {
    stop("No valid agency variable")
  }
  
  # Figure out which enrollment type field to use
  if(!missing(enroll_type)) {
    enroll_type <- enquo(enroll_type)
  } else if("enroll_type" %in% names(df)) {
    enroll_type <- quo(enroll_type)
  } else {
    stop("No valid enroll type variable")
  }
  
  # Figure out which dual eligibility field to use
  if(!missing(dual)) {
    dual <- enquo(dual)
  } else if("dual_elig_m" %in% names(df)) {
    dual <- quo(dual_elig_m)
  } else {
    stop("No valid dual eligiblity variable")
  }
  
  # Figure out which YT field to use
  if(!missing(yt)) {
    yt <- enquo(yt)
  } else if("yt" %in% names(df)) {
    yt <- quo(yt)
  } else {
    stop("No valid Yesler Terrace variable")
  }
  
  # Figure out which SS field to use
  if(!missing(ss)) {
    ss <- enquo(ss)
  } else if("ss" %in% names(df)) {
    ss <- quo(ss)
  } else {
    stop("No valid Scattered Sites variable")
  }
  
  
  # Standardize vars to work with base R (temp crutch)
  df <- df %>%
    mutate(yt_tmp = !!yt,
           ss_tmp = !!ss,
           enroll_tmp = !!enroll_type,
           pt_tmp = !!pt,
           dual_tmp = !!dual,
           agency_tmp = !!agency)
  

  coded <- df %>% filter(!is.na(!!pt))

  # Pre-allocate pop_code with a higher number 
  # (so taking the minimum doesn't keep rows that don't meet any of the criteria)
  coded$pop_code <- rep(99, nrow(coded))
  
  # Use base R vs mutate (ugly but faster)
  coded$pop_code <- replace(coded$pop_code,
                            coded$yt_tmp == 1 & coded$enroll_tmp == "b" & 
                              coded$pt_tmp >= pt_cut & ((coded$dual_tmp == "N" | coded$dual_tmp == 0) | coded$dual_tmp == 0), 1)
  coded$pop_code <- replace(coded$pop_code,
                            coded$ss_tmp == 1 & coded$enroll_tmp == "b" & 
                              coded$pt_tmp >= pt_cut & ((coded$dual_tmp == "N" | coded$dual_tmp == 0) | coded$dual_tmp == 0), 2)
  coded$pop_code <- replace(coded$pop_code,
                            coded$yt_tmp == 0 & coded$ss_tmp == 0 & 
                              coded$agency_tmp == "SHA" & coded$enroll_tmp == "b" & 
                              coded$pt_tmp >= pt_cut & ((coded$dual_tmp == "N" | coded$dual_tmp == 0) | coded$dual_tmp == 0), 3)
  coded$pop_code <- replace(coded$pop_code,
                            coded$yt_tmp == 0 & coded$ss_tmp == 0 & 
                              coded$agency_tmp == "KCHA" & coded$enroll_tmp == "b" & 
                              coded$pt_tmp >= pt_cut & (coded$dual_tmp == "N" | coded$dual_tmp == 0), 4)
  coded$pop_code <- replace(coded$pop_code,
                            coded$yt_tmp == 0 & coded$ss_tmp == 0 & 
                              (is.na(coded$agency_tmp) | coded$agency_tmp == "Non-PHA") & 
                              coded$enroll_tmp == "m" & 
                              (coded$dual_tmp == "N" | coded$dual_tmp == 0), 5)
  coded$pop_code <- replace(coded$pop_code,
                            coded$yt_tmp == 1 & coded$enroll_tmp == "b" & 
                              coded$pt_tmp >= pt_cut & (coded$dual_tmp == "Y" | coded$dual_tmp == 1), 6)
  coded$pop_code <- replace(coded$pop_code,
                            coded$ss_tmp == 1 & coded$enroll_tmp == "b" & 
                              coded$pt_tmp >= pt_cut & (coded$dual_tmp == "Y" | coded$dual_tmp == 1), 7)
  coded$pop_code <- replace(coded$pop_code,
                            coded$yt_tmp == 0 & coded$ss_tmp == 0 & 
                              coded$agency_tmp == "SHA" & coded$enroll_tmp == "b" & 
                              coded$pt_tmp >= pt_cut & (coded$dual_tmp == "Y" | coded$dual_tmp == 1), 8)
  coded$pop_code <- replace(coded$pop_code,
                            coded$yt_tmp == 0 & coded$ss_tmp == 0 & 
                              coded$agency_tmp == "KCHA" & coded$enroll_tmp == "b" & 
                              coded$pt_tmp >= pt_cut & (coded$dual_tmp == "Y" | coded$dual_tmp == 1), 9)
  coded$pop_code <- replace(coded$pop_code,
                            coded$yt_tmp == 0 & coded$ss_tmp == 0 & 
                              (is.na(coded$agency_tmp) | coded$agency_tmp == "Non-PHA") & 
                              coded$enroll_tmp == "m" & 
                              (coded$dual_tmp == "Y" | coded$dual_tmp == 1), 10)
  coded$pop_code <- replace(coded$pop_code,
                            coded$yt_tmp == 1 & coded$enroll_tmp == "b" & 
                              coded$pt_tmp >= pt_cut & is.na(coded$dual_tmp), 11)
  coded$pop_code <- replace(coded$pop_code,
                            coded$ss_tmp == 1 & coded$enroll_tmp == "b" & 
                              coded$pt_tmp >= pt_cut & is.na(coded$dual_tmp), 12)
  coded$pop_code <- replace(coded$pop_code,
                            coded$yt_tmp == 0 & coded$ss_tmp == 0 & 
                              coded$agency_tmp == "SHA" & coded$enroll_tmp == "b" & 
                              coded$pt_tmp >= pt_cut & is.na(coded$dual_tmp), 13)
  coded$pop_code <- replace(coded$pop_code,
                            coded$yt_tmp == 0 & coded$ss_tmp == 0 & 
                              coded$agency_tmp == "KCHA" & coded$enroll_tmp == "b" & 
                              coded$pt_tmp >= pt_cut & is.na(coded$dual_tmp), 14)
  coded$pop_code <- replace(coded$pop_code,
                            coded$yt_tmp == 0 & coded$ss_tmp == 0 & 
                              (is.na(coded$agency_tmp) | coded$agency_tmp == "Non-PHA") & 
                              coded$enroll_tmp == "m" & 
                              is.na(coded$dual_tmp), 15)
  coded$pop_code <- replace(coded$pop_code,
                            coded$yt_tmp == 1 & coded$agency_tmp == "SHA" & 
                              coded$enroll_tmp == "h", 16)
  coded$pop_code <- replace(coded$pop_code,
                            coded$ss_tmp == 1 & coded$agency_tmp == "SHA" & 
                              coded$enroll_tmp == "h", 17)
  coded$pop_code <- replace(coded$pop_code,
                            coded$yt_tmp == 0 & coded$ss_tmp == 0 & 
                              coded$agency_tmp == "SHA" & coded$enroll_tmp == "h", 18)
  coded$pop_code <- replace(coded$pop_code,
                            coded$agency_tmp == "KCHA" & coded$enroll_tmp == "h", 19)
  
  coded$year_code <- year_num
  
  ### This runs more slowly than the uglier base code above but keeping here for now
  #   in case improvements can be made
  # coded <- df %>%
  #   filter(!is.na((!!pt))) %>%
  #   mutate(
  #     pop_code = case_when(
  #       !!yt == 1 & !!enroll_type == "b" & !!pt >= pt_cut & !!dual == "N" ~ 1,
  #       !!ss == 1 & !!enroll_type == "b" & !!pt >= pt_cut & !!dual == "N" ~ 2,
  #       !!yt == 0 & !!ss == 0 & !!agency == "SHA" & !!enroll_type == "b" & 
  #         !!pt >= pt_cut & !!dual == "N" ~ 3,
  #       !!yt == 0 & !!ss == 0 & !!agency == "KCHA" & !!enroll_type == "b" & 
  #         !!pt >= pt_cut & !!dual == "N" ~ 4,
  #       !!yt == 0 & !!ss == 0 & is.na(!!agency) & !!enroll_type == "m" & 
  #         !!dual == "N" ~ 5,
  #       !!yt == 1 & !!enroll_type == "b" & !!pt >= pt_cut & !!dual == "Y" ~ 6,
  #       !!ss == 1 & !!enroll_type == "b" & !!pt >= pt_cut & !!dual == "Y" ~ 7,
  #       !!yt == 0 & !!ss == 0 & !!agency == "SHA" & !!enroll_type == "b" & 
  #         !!pt >= pt_cut & !!dual == "Y" ~ 8,
  #       !!yt == 0 & !!ss == 0 & !!agency == "KCHA" & !!enroll_type == "b" & 
  #         !!pt >= pt_cut & !!dual == "Y" ~ 9,
  #       !!yt == 0 & !!ss == 0 & is.na(!!agency) & !!enroll_type == "m" & 
  #         !!dual == "Y" ~ 10,
  #       !!yt == 1 & !!enroll_type == "b" & !!pt >= pt_cut & is.na(!!dual) ~ 11,
  #       !!ss == 1 & !!enroll_type == "b" & !!pt >= pt_cut & is.na(!!dual) ~ 12,
  #       !!yt == 0 & !!ss == 0 & !!agency == "SHA" & !!enroll_type == "b" & 
  #         !!pt >= pt_cut & is.na(!!dual) ~ 13,
  #       !!yt == 0 & !!ss == 0 & !!agency == "KCHA" & !!enroll_type == "b" & 
  #         !!pt >= pt_cut & is.na(!!dual) ~ 14,
  #       !!yt == 0 & !!ss == 0 & is.na(!!agency) & !!enroll_type == "m" & 
  #         is.na(!!dual) ~ 15,
  #       !!yt == 1 & !!agency == "SHA" & !!enroll_type == "h" ~ 16,
  #       !!ss == 1 & !!agency == "SHA" & !!enroll_type == "h" ~ 17,
  #       !!yt == 0 & !!ss == 0 & !!agency == "SHA" & !!enroll_type == "h" ~ 18,
  #       !!agency == "KCHA" & !!enroll_type == "h" ~ 19
  #     ),
  #     year_code = year_num)
  
  if (min == F) {
    coded <- coded %>% select(-contains("_tmp"))
    return(coded)
  } else {
    
    # Switch to a data table for faster group processing
    setDT(coded)
    
    # Find the minimum pop_code for that year
    coded_min <- coded[, pop_type := min(pop_code), by = pid2]
    # Add up total person time before filtering and sort by descending pt
    coded_min[, pt_tot := sum(pt_tmp, na.rm = T), by = pid2][order(pid2, -(pt_tmp))]
    # Just keep the row with the min pop code and most pt
    coded_min <- coded_min[pop_type == pop_code, .SD[1], by = pid2]
    
    coded_min <- as.data.frame(coded_min)
    coded_min <- coded_min %>%
      mutate(
        agency_min = case_when(
          pop_type %in% c(1:3, 6:8, 11:13, 16:18) ~ "SHA",
          pop_type %in% c(4, 9, 14, 19) ~ "KCHA",
          pop_type %in% c(5, 10, 15) ~ "Non-PHA"
        ),
        enroll_type_min = case_when(
          pop_type %in% c(1:4, 6:9, 11:14) ~ "Both",
          pop_type %in% c(5, 10, 15) ~ "Medicaid only",
          pop_type %in% c(16:19) ~ "Housing only"
        ),
        dual_min = case_when(
          pop_type %in% c(1:5) ~ "N",
          pop_type %in% c(6:10) ~ "Y"
        ),
        yt_min = case_when(
          pop_type %in% c(1, 6, 11, 16) ~ 1,
          pop_type %in% c(2:5, 7:10, 12:15, 17:19) ~ 0
        ),
        ss_min = case_when(
          pop_type %in% c(2, 7, 12, 17) ~ 1,
          pop_type %in% c(1, 3:6, 8:11, 12:16, 18:19) ~ 0
        ),
        !!pt := pt_tot
      ) %>%
      select(-contains("_tmp"), -pt_tot)

    
    return(coded_min)
    
  }
}