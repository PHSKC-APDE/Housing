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
	pha <- pha_longitudinal_kc %>%
			mutate(age = )

# ==========================================================================
# Descriptives
# ==========================================================================

	####
	# Notes:
	# Individuals are distinct by ssn and
	#
	glimpse(pha)

	### Number of persons, households
	pha %>% summarise(individuals = n_distinct(pid),
					Households = n_distinct(hhold_id_new))

	## by org
	pha %>% group_by(agency_new) %>% summarise(individuals = n_distinct(pid),
					Households = n_distinct(hhold_id_new))

			# 		  Agency individuals Households
			#        <chr>       <int>      <int>
			# 1       KCHA       84253      30207
			# 2        SHA       64783      30634
			# 3		 Total		138654      57207
			# 4		  Both       10382		 3634

	### Mean number of persons in households
	pha %>% group_by(hhold_id_new) %>% summarise(meanHHsize = mean(hhold_size)) %>% ungroup() %>% summarise(mean(meanHHsize))
		# 2.3

	###
	pha %>% select(max_date) %>% arrange(max_date)

	pha %>%
		ggplot(aes(x="time_pha")) +
		geom_histogram(stat = "count")

		84253 + 64783 - 138654
		30207 + 30634 - 57207

	# sex and race
		pha %>% group_by(gender2) %>% summarise(n_distinct(pid))

			# 		  gender2 `n_distinct(pid)`
			#     <chr>             <int>
			# 1  Female             77225
			# 2    Male             49192
			# 3    <NA>             14224

		pha %>% group_by(race2) %>% summarise(n_distinct(pid))

			# 		          race2 `n_distinct(pid)`
			#           <chr>             <int>
			# 2    Asian only             15171
			# 3    Black only             59557
			# 4  Multi-racial              8087
			# 5     NHPI only              2508
			# 6    White only             53116
			# 7          <NA>               215

	# Number of children in 2016
		pha %>% filter(age12<18) %>% summarise(n_distinct(pid)) #48483
		pha %>% filter(age13<18) %>% summarise(n_distinct(pid)) #46027
		pha %>% filter(age14<18) %>% summarise(n_distinct(pid)) #43495
		pha %>% filter(age15<18) %>% summarise(n_distinct(pid)) #40929
		pha %>% group_by(agency_new) %>% filter(age16<18) %>% summarise(n_distinct(pid)) #38190
		# 			Agency 			Count
		# 1       	KCHA             24902
		# 2           SHA             16597
		# 3			Total			 38190
		# 4			Both			  3309

sum(2111,
13060,
59557,
8087,
2508,
53116,
215)

sum(2111,13060)