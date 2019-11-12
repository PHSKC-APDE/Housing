/****** COPY FROM STAGE to FINAL  ******/
	IF OBJECT_ID('[PH_APDEStore].[final].[mcaid_mcare_pha_elig_timevar]', 'U') IS NOT NULL 
		DROP TABLE [PH_APDEStore].[final].[mcaid_mcare_pha_elig_timevar]

	SELECT *
		INTO [PH_APDEStore].[final].[mcaid_mcare_pha_elig_timevar]	
		FROM [PH_APDEStore].[stage].[mcaid_mcare_pha_elig_timevar]


/****** ADD COLUMSTORE CLUSTERED INDEX ******/
	CREATE CLUSTERED COLUMNSTORE INDEX idx_final_mcaid_mcare_pha_elig_timevar
	ON [PH_APDEStore].[final].[mcaid_mcare_pha_elig_timevar]
	WITH (DROP_EXISTING = OFF)


/****** BASIC ERROR CHECKING COMPARING STAGE & FINAL ******/
	SELECT COUNT(*) FROM [PH_APDEStore].[stage].[mcaid_mcare_pha_elig_timevar]
	SELECT COUNT(*) FROM [PH_APDEStore].[final].[mcaid_mcare_pha_elig_timevar]

	SELECT contiguous, 
	count(*) FROM [PH_APDEStore].[stage].[mcaid_mcare_pha_elig_timevar]
	  GROUP BY contiguous
	  ORDER BY -count(*)

	 SELECT contiguous, 
	count(*) FROM [PH_APDEStore].[final].[mcaid_mcare_pha_elig_timevar]
	  GROUP BY contiguous
	  ORDER BY -count(*)