# ==========================================================================
# Analysis of SHA and KCHA data
# Tim Thomas (UW eScience)
# t77@uw.edu
# 2018.01.10
# ==========================================================================

### Setup ###
	rm(list=ls()) #reset
	gc()
	options(tibble.print_max = 50, scipen = 999, width = 150)

### Libraries ###
	library(colorout)
	library(data.table)
	library(tidyverse)

### Data ###
	sha <- load("data/Housing/OrganizedData/SHA.rdata")
	kcha <- load("data/Housing/OrganizedData/kcha_long.Rda")

### Process ###
# Find disparities in data
	glimpse(sha)