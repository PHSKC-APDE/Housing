# Housing
Code specific to joining together KCHA and SHA data for analysis
Relies on functions created in the housing package

# STEPS:
- Process raw KCHA data and load to SQL database (kcha_sql_load.R)
- Process raw SHA data and load to SQL database (sha_sql_load.R)
- Bring in individual PHA datasets and combine into a single file (pha_combining.R)
- Deduplicate data and tidy up via matching process (pha_matching.R)
- Recode race and other demographics (pha_recodes.R)
- Clean up addresses and geocode (pha_address_cleaning.R)
- Consolidate data rows (pha_consolidation.R)
- Add in final data elements and set up analyses (pha_analyses_prep.R)
- Join with Medicaid eligibility data and set up analyses (pha_medicaid_join.R)
