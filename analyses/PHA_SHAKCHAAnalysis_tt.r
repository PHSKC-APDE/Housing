# ==========================================================================
# Analysis of SHA and KCHA data
# Tim Thomas (UW eScience)
# t77@uw.edu
# 2018.01.10
# ==========================================================================

# ==========================================================================
#  setup
# ==========================================================================
	rm(list=ls()) #reset
	gc()
	options(tibble.print_max = 50, scipen = 999, width = 150)

# ==========================================================================
# Libraries
# ==========================================================================

	library(colorout)
	library(data.table)
	library(texreg)
	library(tidyverse)

# ==========================================================================
# Data
# ==========================================================================
	sha <- readRDS("data/Housing/OrganizedData/SHA.Rda")
	kcha <- readRDS("data/Housing/OrganizedData/kcha_long.Rda")

# ==========================================================================
# Data Processing
# ==========================================================================
# Find disparities in data
	glimpse(sha)
	glimpse(kcha)

	data.frame(table(sha$hh_ssn_orig)) %>% tail
	data.frame(table(kcha$hh_ssn_orig)) %>% tail

# Clean data
	kcha <- kcha %>%
			mutate(disability=ifelse(disability=="", NA,
					ifelse(disability==0, NA,
						ifelse(disability=="N", 0, 1))),
					ph_rent_ceiling=ifelse(ph_rent_ceiling=="",NA,
						ifelse(ph_rent_ceiling=="N",0,1)),
					tb_rent_ceiling=ifelse(tb_rent_ceiling=="",NA,
						ifelse(tb_rent_ceiling=="N",0,1)),
					HH_ID_FIN=hhold_id) # create common households ID
	sha <- sha %>%
			mutate(property_id=as.numeric(property_id),
					HH_ID_FIN=hh_id) # create common household ID

# Merge household IDs across sha and kcha

# bind rows
	df1 <- bind_rows(sha,kcha) %>% distinct()
	gc()

	df1 <- df1 %>%
			mutate(AGE=year(act_date)-year(dob),
					IS_CHILD=ifelse(AGE<=18,1,0))
	glimpse(df1)

# ==========================================================================
# Analysis
# ==========================================================================
# number of unique individuals
	dem <-
	df1 %>%