#' Flags people who live at Yesler Terrace.
#' 
#' \code{yesler} creates a set of flags specific to SHA's Yesler Terrace.
#' 
#' This function creates a set of flags that are specific to Seattle Housing
#' Authority's Yesler Terrace site. It is a convenient way to quickly add the 
#' flags to a given data set. Also adds a flag for scattered sites.
#' Once each person's addresses have been flagged as Yesler Terrace/scattered 
#' sites or not, a new set of variables is made to indicate whether or not the 
#' person has ever lived at Yesler Terrace/scattered sites.
#' For now, it is assumed that the df has variables called property_id, 
#' unit_add_new or unit_add_h, and a variable to group people on (pid or pid2).
#' 
#' @param df A data frame
#' @param unit A named variable that determines the unit to group by. Default is 
#' pid2 (individuals) but pid should be used with housing-only data that has not
#' been matched to Medicaid data.
#' 
#' @examples 
#' \dontrun{
#' yesler(pha_elig_demogs, unit = pid)
#' }
#' 
#' @export

yesler <- function(df, unit = NULL){
  if (missing(unit)) {
    print("Attempting to use default grouping (pid2 then pid).")
  }
  
  if(!missing(unit)) {
    unit <- enquo(unit)
  } else if("pid2" %in% names(df)) {
    unit <- quo(pid2)
  } else if("pid" %in% names(df)) {
    unit <- quo(pid)
  } else {
    stop("No valid unit of analysis found")
  }
  
  # Figure out which address field to use
  if("unit_add_h" %in% names(df)) {
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
        (property_id %in% c("001", "1", "591", "738", "743") & !is.na(property_id)) |
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
      yt_old = ifelse(property_id %in% c("1", "001") & !is.na(property_id), 
                      1, 0),
      yt_new = ifelse(
        (property_id %in% c("591", "738", "743") & !is.na(property_id)) |
          (!is.na(!!address) & 
             # Kebero Court
             (str_detect(!!address, "^1105[:space:]*[E]*[:space:]*F") |
                # The Baldin
                str_detect(!!address, "^1305[:space:]*[E]*[:space:]*F") |
                # Hoa Mai
                str_detect(!!address, "^221[:space:]*10TH[:space:]*AVE") |
                # General new YT
                str_detect(!!address, "^820[:space:]*[E]*[:space:]*YESLER"))),
        1, 0),
      ss = ifelse(
        (property_id %in% 
           c("050", "051", "052", "053", "054", "055", "056", "057",
             "A42", "A43", "I42", "I43", "L42", "L43", "P42", "P43") &
           !is.na(property_id)) |
          (str_detect(property_name, "SCATTERED")),
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