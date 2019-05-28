#! /usr/bin/Rscript --vanilla --default-packages=utils

args <- commandArgs(trailingOnly = TRUE)

hild_dir <- args[1]
#hild_dir <- "/home/ubuntu/data/HILD/" #temp, will pass through python script
housing_source_dir <- args[2] 
#housing_source_dir <- "/home/joseh/source/Housing/processing/" # temp, will pass through python script
# local_metadata_path <- args[3]

housing_scripts <- c('01_kcha_sql_load.R',
                     '02_sha_sql_load.R',
                     '03_pha_combining.R',
                     '04_pha_matching.R',
                     '05_pha_recodes.R',
                     '06_pha_address_cleaning.R',
                     '06a_pha_geocoding.R',
                     '07_pha_consolidation.R',
                     '08_pha_analyses_prep.R')


for (rscripts in housing_scripts) {
  source(paste0(housing_source_dir, rscripts))
}


