#' Count people with chronic conditions by desired grouping
#' 
#' This function counts the number of people with chronic conditions for a given 
#' calendar year. It is currently specific to the joined Medicaid/housing data 
#' sets but could be generalized to other data. The function joins condition 
#' data with population data to create comprehensive counts across demographic 
#' and housing characteristics.
#' 
#' @param df_chronic A data frame containing chronic condition data with 
#' person IDs, condition indicators, and date ranges (from_date, to_date).
#' @param df_pop A data frame containing population data with demographic and 
#' housing characteristics. Default is chronic_pop.
#' @param condition The chronic condition of interest. Should match a field name 
#' in df_chronic (typically a binary indicator).
#' @param year The calendar year (2 digits) of interest. Default is 22 (for 2022).
#' 
#' @return A data frame with counts of people with the chronic condition, 
#' grouped by year and various demographic/housing characteristics including 
#' agency, enrollment type, dual eligibility, age group, gender, ethnicity, 
#' voucher status, subsidy type, operator, portfolio, length of housing, and ZIP code.
#' 
#' @examples
#' \dontrun{
#' count_chronic(condition = diabetes, year = 15)
#' count_chronic(df_chronic = my_conditions, df_pop = my_population,
#'               condition = hypertension, year = 16)
#' count_chronic(condition = mental_health, year = 14)
#' }
#' 
#' @import dplyr
#' 
#' @export
count_chronic <- function(df_chronic = chronic, 
                          df_pop = chronic_pop,
                          condition = NULL, 
                          year = 22) {
  
  yr_chk <- as.numeric(paste0("20", year))
  condition_quo <- enquo(condition)
  
  agex_quo <- rlang::sym(paste0("age", quo_name(year), "_num"))
  lengthx_quo <- rlang::sym(paste0("length", quo_name(year), "_num"))
  
  year_start = as.Date(paste0("20", year, "-01-01"), origin = "1970-01-01")
  year_end = as.Date(paste0("20", year, "-12-31"), origin = "1970-01-01")
  
  # Filter to only include people with the condition in that year
  cond <- df_chronic %>%
    filter(!!condition_quo == 1 & from_date <= year_end & to_date >= year_start) %>%
    distinct(id, !!condition_quo)
  
  df_pop <- df_pop %>% filter(year == yr_chk)
  
  ### Join pop and condition data to summarise
  output <- left_join(df_pop, cond, by = c("mid" = "id")) %>%
    mutate(condition = if_else(is.na(!!condition_quo), 0, as.numeric(!!condition_quo))) %>%
    group_by(year, agency_num, enroll_type_num, dual_elig_num, age_group,
             gender_num, ethn_num, voucher_num, subsidy_num, operator_num,
             portfolio_num, length, zip_c) %>%
    summarise(count := sum(condition)) %>%
    ungroup() %>%
    select(year, agency_num, enroll_type_num, dual_elig_num, age_group, 
           gender_num, ethn_num, voucher_num, subsidy_num, operator_num, 
           portfolio_num, length, zip_c,
           count) %>%
    rename(agency = agency_num,
           enroll_type = enroll_type_num,
           dual = dual_elig_num,
           gender = gender_num,
           ethn = ethn_num,
           voucher = voucher_num,
           subsidy = subsidy_num,
           operator = operator_num,
           portfolio = portfolio_num,
           zip = zip_c)
  
  return(output)
}