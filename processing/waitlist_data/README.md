# Housing waitlist data
Code in this section processes SHA and KCHA waitlist data from 2017

# STEPS:
1) Load KCHA and SHA csv files, process, and load to SQL (waitlist_01_pha_sql_load.R)
2) Join to to existing linked PHA data (waitlist_02_join_data.R)
