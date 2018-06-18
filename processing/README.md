# Housing
Code specific to joining together KCHA and SHA data for analysis
Also includes code for joining housing and Medicaid data
Relies on functions created in the housing package

# STEPS:
1) Process raw KCHA data and load to SQL database (01 - kcha_sql_load.R)
2) Process raw SHA data and load to SQL database (02 - sha_sql_load.R)
3) Bring in individual PHA datasets and combine into a single file (03 - pha_combining.R)
4) Deduplicate data and tidy up via matching process (04 - pha_matching.R)
5) Recode race and other demographics (05 - pha_recodes.R)
6) Clean up addresses (06 - pha_address cleaning.R)
6a) Geocode addresses (06a - pha_geocoding.R)
7) Consolidate data rows (07 - pha_consolidation.R)
8) Add in final data elements and set up analyses (pha_analyses_prep.R)
9) Join with Medicaid eligibility data (09 - pha_mcaid join.R)
10) Set up joint housing/Medicaid analyses (10 - pha_mcaid analysis prep.R)
