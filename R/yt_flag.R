#' Flags people who live at Yesler Terrace.
#' 
#' \code{yt_flag} creates a set of flags specific to SHA's Yesler Terrace.
#' 
#' This function creates a set of flags that are specific to Seattle Housing
#' Authority's Yesler Terrace site. It is a convenient way to quickly add the 
#' flags to a given data set. Also adds a flag for scattered sites.
#' Once each person's addresses have been flagged as Yesler Terrace/scattered 
#' sites or not, a new set of variables is made to indicate whether or not the 
#' person has ever lived at Yesler Terrace/scattered sites.
#' It is assumed that the df has variables for the property ID, property name, 
#' address, and individual identifier (pid or pid2).
#' 
#' @param df A data frame
#' @param unit A named variable that determines the unit to group by. Default is 
#' pid2 (individuals) but pid should be used with housing-only data that has not
#' been matched to Medicaid data.
#' @param prop_id A named variable that specifies the property IDs used in 
#' housing data. Default is property_id.
#' @param prop_id A named variable that specifies the property name used in 
#' housing data. Default is property_name.
#' @param address A named varaible that specifies the unit address. 
#' Default is unit_add_h.
#' 
#' @examples 
#' \dontrun{
#' yt_flag(pha_elig_demogs, unit = pid)
#' }
#' 
#' @export

yt_flag <- function(df, unit = NULL, prop_id = NULL, prop_name = NULL, 
                    address = NULL){
  if (missing(unit)) {
    print("Attempting to use default grouping (pid2 then pid).")
  }
  
  # Figure out which unit of analysis to use
  if(!missing(unit)) {
    unit <- enquo(unit)
  } else if("pid2" %in% names(df)) {
    unit <- quo(pid2)
  } else if("pid" %in% names(df)) {
    unit <- quo(pid)
  } else {
    stop("No valid unit of analysis found")
  }
  
  # Figure out which property ID field to use
  if(!missing(prop_id)) {
    prop_id <- enquo(prop_id)
  } else if("property_id" %in% names(df)) {
    prop_id <- quo(property_id)
  } else {
    stop("No valid property ID variable")
  }
  
  # Figure out which property ID field to use
  if(!missing(prop_name)) {
    prop_name <- enquo(prop_name)
  } else if("property_name" %in% names(df)) {
    prop_name <- quo(property_name)
  } else {
    stop("No valid property name variable")
  }
  
  # Figure out which address field to use
  if(!missing(address)) {
    address <- enquo(address)
  } else if("unit_add_h" %in% names(df)) {
    address <- quo(unit_add_h)
  } else if("unit_add_new" %in% names(df)) {
    address <- quo(unit_add_new)
  } else {
    stop("No valid address variable")
  }
  
  ### Yesler Terrace and scattered sites indicators
  df <- df %>%
    mutate(
      yt = ifelse(
        (!!prop_id %in% c("001", "1", "591", "738", "743") & !is.na(!!prop_id)) |
          (!is.na(!!address) &
             # Kebero Court
             (str_detect(!!address, "^1105[:space:]*[E]*[:space:]*F") |
                str_detect(!!address, "^1[123]0[:space:]*[E]*[:space:]*BOREN") |
                # The Baldwin
                str_detect(!!address, "^1305[:space:]*[E]*[:space:]*F") |
                # Hoa Mai
                str_detect(!!address, "^221[:space:]*10TH[:space:]*AVE") |
                # General new YT
                str_detect(!!address, "^820[:space:]*[E]*[:space:]*YESLER"))),
        1, 0),
      yt_old = ifelse(!!prop_id %in% c("1", "001") & !is.na(!!prop_id), 
                      1, 0),
      yt_new = ifelse(yt == 1 & yt_old == 0, 1, 0),
      ss = ifelse(
        (!!prop_id %in% 
           c("050", "051", "052", "053", "054", "055", "056", "057",
             "A42", "A43", "I42", "I43", "L42", "L43", "P42", "P43") &
           !is.na(!!prop_id)) |
          (str_detect(!!prop_name, "SCATTERED")),
        1, 0)
    )
  
  ### Find people who were ever at YT or SS
  df <- df %>%
    group_by(!!unit) %>%
    mutate_at(vars(yt, ss), funs(ever = sum(., na.rm = TRUE))) %>%
    ungroup() %>%
    mutate_at(vars(yt_ever, ss_ever), funs(replace(., which(. > 0), 1)))
  
  
  return(df)
}