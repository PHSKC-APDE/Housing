#! /usr/bin/Rscript --vanilla --default-packages=utils

args <- commandArgs(trailingOnly = TRUE)

hild_dir <- args[1]  
housing_source <- args[2] 
local_metadata_path <- args[3]
UW <- TRUE
aws <- FALSE

start_time <- Sys.time()
source(paste0(housing_source,'01_kcha_sql_load.R'))
source(paste0(housing_source,'02_sha_sql_load.R'))
source(paste0(housing_source,'03_pha_combining.R'))
source(paste0(housing_source,'04_pha_matching.R'))
source(paste0(housing_source,'05_pha_recodes.R'))
source(paste0(housing_source,'06_pha_address_cleaning.R'))
source(paste0(housing_source,'06a_pha_geocoding.R'))
source(paste0(housing_source,'07_pha_consolidation.R'))
source(paste0(housing_source,'08_pha_analyses_prep.R'))
end_time <- Sys.time()
print(end_time - start_time)

