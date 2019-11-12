/****** COPY FROM STAGE to FINAL  ******/
	IF OBJECT_ID('[PH_APDEStore].[final].[mcaid_mcare_pha_elig_demo]', 'U') IS NOT NULL 
		DROP TABLE [PH_APDEStore].[final].[mcaid_mcare_pha_elig_demo]

	SELECT *
		INTO [PH_APDEStore].[final].[mcaid_mcare_pha_elig_demo]	
		FROM [PH_APDEStore].[stage].[mcaid_mcare_pha_elig_demo]


/****** ADD COLUMSTORE CLUSTERED INDEX ******/
	CREATE CLUSTERED COLUMNSTORE INDEX idx_final_mcaid_mcare_pha_elig_demo
	ON [PH_APDEStore].[final].[mcaid_mcare_pha_elig_demo]
	WITH (DROP_EXISTING = OFF)


/****** BASIC ERROR CHECKING COMPARING STAGE & FINAL ******/
	SELECT COUNT(*) FROM [PH_APDEStore].[stage].[mcaid_mcare_pha_elig_demo]
	SELECT COUNT(*) FROM [PH_APDEStore].[final].[mcaid_mcare_pha_elig_demo]

	SELECT geo_kc_ever, 
	count(*) FROM [PH_APDEStore].[stage].[mcaid_mcare_pha_elig_demo]
	  GROUP BY geo_kc_ever
	  ORDER BY -count(*)

	 SELECT geo_kc_ever, 
	count(*) FROM [PH_APDEStore].[final].[mcaid_mcare_pha_elig_demo]
	  GROUP BY geo_kc_ever
	  ORDER BY -count(*)