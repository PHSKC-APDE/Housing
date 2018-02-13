# ==========================================================================
# Exploratory Data Analysis
# Descriptives of full PHA dataset
# Tim Thomas - t77@uw.edu
# 2018.01.25
# ==========================================================================

	rm(list=ls()) #reset
	options(max.print = 500, tibble.print_max = 50, scipen = 999, width = 170)
	gc()

# ==========================================================================
# Libraries and Data
# ==========================================================================

	library(colorout)
	library(rgdal)
	library(tidyverse)

	load("data/Housing/OrganizedData/pha_longitudinal_kc.Rdata")
	load("data/Housing/OrganizedData/pha_longitudinal.Rdata")
	kc <- readOGR("data/Housing/OrganizedData/Shapefiles", "KingCounty2010_4601")

	pha_kc <- pha_longitudinal_kc
	pha <- pha_longitudinal
	# %>%
	# 		mutate(age = )

# ==========================================================================
# Geocoding problems
# ==========================================================================

	  zip <- c(98001,98002,98003,98004,98005,98006,98007,98008,98010,98011,98014,98019,98022,98023,98024,98027,98028,98029,98030,98031,98032,98033,98034,98038,98039,98040,98042,98045,98047,98050,98051,98052,98053,98055,98056,98057,98058,98059,98065,98068,98070,98072,98074,98075,98077,98092,98101,98102,98103,98104,98105,98106,98107,98108,98109,98112,98115,98116,98117,98118,98119,98121,98224,98122,98125,98126,98133,98134,98136,98144,98146,98148,98154,98288,98155,98158,98164,98166,98168,98174,98177,98178,98188,98195,98198,98199)

	### Show the problems with subsetting by spatial location
	# Prep data
	pha_sp <- pha %>% filter(!is.na(X)) %>% mutate(ID=str_pad(rep(1:nrow(.)),6,pad='0')) # subset data removing NA in coords, add an ID field
 	coordinates(pha_sp) <- ~X+Y # make object spatial
 	proj4string(pha_sp) <- "+proj=longlat +datum=WGS84" # define the projection
 	kc <- spTransform(kc, proj4string(pha_sp)) # define King County boundary projection
 	pha_kc_sp <- pha_sp[kc,] # select point that fall inside King County.
 	pha_kcout_sp <- pha_sp[!(pha_sp$ID %in% pha_kc_sp$ID),]# select points that fall outside of king county
 	pha_kcout_spSea <- pha_kcout_sp[pha_kcout_sp$unit_city=="SEATTLE",]# select points that say Seattle and outside of KC
 	pha_kcout_spzip <- pha_kcout_sp[pha_kcout_sp$unit_zip %in% zip,]

 	### plot ###
 	plot(pha_sp)
 	plot(pha_kcout_sp)
 	plot(pha_kcout_spzip, add=T, col="red", pch=20)

 	plot(pha_kc_sp, col="grey80")
 	plot(kc, add=T)
 	plot(pha_kcout_sp, add=T, col="lightblue")
 	plot(pha_kcout_spzip, add=T, col="red", pch=20)
 	plot(pha_kcout_spSea, add=T, col="purple", pch=20)

 	### table of errors ###
	pha_kcout_spzip@data %>% select(unit_concat, formatted_address, score_esri, source) %>% data.frame()

# ==========================================================================
# Codebook
# ==========================================================================
	# ssn_c = ssn that has alpha numeric

# ==========================================================================
# Descriptives
# ==========================================================================

dim(pha_kc)
dim(pha)

glimpse(pha)

pha %>% summarize(sum(port_in, na.rm=T), sum(port_out_kcha, na.rm=T), sum(port_out_sha, na.rm=T))

pha %>% filter(port_in==1, port_out_sha==1) %>% dim

summarize(sum(port_in, na.rm=T), sum(port_out_kcha, na.rm=T), sum(port_out_sha, na.rm=T))

####
	# Notes:
	# Individuals are distinct by ssn and
	#
	glimpse(pha_kc)

	### Number of persons, households
	pha_kc %>% summarise(individuals = n_distinct(pid),
					Households = n_distinct(hhold_id_new))

	## by org
	pha_kc %>% group_by(agency_new) %>% summarise(individuals = n_distinct(pid),
					Households = n_distinct(hhold_id_new))

			# 		  Agency individuals Households
			#        <chr>       <int>      <int>
			# 1       KCHA       84253      30207
			# 2        SHA       64783      30634
			# 3		 Total		138654      57207
			# 4		  Both       10382		 3634

	### Mean number of persons in households
	pha_kc %>% group_by(hhold_id_new) %>% summarise(meanHHsize = mean(hhold_size)) %>% ungroup() %>% summarise(mean(meanHHsize))
		# 2.3

	###
	pha_kc %>% select(max_date) %>% arrange(max_date)

	pha_kc %>%
		ggplot(aes(x="time_pha")) +
		geom_histogram(stat = "count")



		84253 + 64783 - 138654
		30207 + 30634 - 57207

	# sex and race
		pha_kc %>% group_by(gender2) %>% summarise(n_distinct(pid))

			# 		  gender2 `n_distinct(pid)`
			#     <chr>             <int>
			# 1  Female             77225
			# 2    Male             49192
			# 3    <NA>             14224

		pha_kc %>% group_by(race2) %>% summarise(n_distinct(pid))

			# 		          race2 `n_distinct(pid)`
			#           <chr>             <int>
			# 2    Asian only             15171
			# 3    Black only             59557
			# 4  Multi-racial              8087
			# 5     NHPI only              2508
			# 6    White only             53116
			# 7          <NA>               215

	# Number of children in 2016
		pha_kc %>% filter(age12<18) %>% summarise(n_distinct(pid)) #48483
		pha_kc %>% filter(age13<18) %>% summarise(n_distinct(pid)) #46027
		pha_kc %>% filter(age14<18) %>% summarise(n_distinct(pid)) #43495
		pha_kc %>% filter(age15<18) %>% summarise(n_distinct(pid)) #40929
		pha_kc %>% group_by(agency_new) %>% filter(age16<18) %>% summarise(n_distinct(pid)) #38190
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