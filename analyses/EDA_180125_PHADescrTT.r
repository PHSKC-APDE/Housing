# ==========================================================================
# Exploratory Data Analysis
# Descriptives of full PHA dataset
# Tim Thomas - t77@uw.edu
# 2018.01.25
# ==========================================================================

	rm(list=ls()) #reset
	options(max.print = 350, tibble.print_max = 50, scipen = 999, width = 100)
	gc()

# ==========================================================================
# Libraries and Data
# ==========================================================================

	library(colorout)
	library(tidyverse)

	load("~/data/Housing/OrganizedData/pha_longitudinal_kc.Rdata")
	pha <- pha_longitudinal_kc

# ==========================================================================
# Descriptives
# ==========================================================================

	####
	# Notes:
	# Individuals are distinct by ssn and
	#
	glimpse(pha)