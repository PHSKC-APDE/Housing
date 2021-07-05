/****** COPY FROM STAGE to FINAL  ******/
	IF OBJECT_ID('[PH_APDEStore].[final].[mcaid_mcare_pha_elig_calyear]', 'U') IS NOT NULL 
		DROP TABLE [PH_APDEStore].[final].[mcaid_mcare_pha_elig_calyear]

	SELECT *
		INTO [PH_APDEStore].[final].[mcaid_mcare_pha_elig_calyear]	
		FROM [PH_APDEStore].[stage].[mcaid_mcare_pha_elig_calyear]


/****** ADD COLUMSTORE CLUSTERED INDEX ******/
	CREATE CLUSTERED COLUMNSTORE INDEX idx_final_mcaid_mcare_pha_elig_calyear
	ON [PH_APDEStore].[final].[mcaid_mcare_pha_elig_calyear]
	WITH (DROP_EXISTING = OFF)


/****** BASIC ERROR CHECKING COMPARING STAGE & FINAL ******/
	SELECT COUNT(*) FROM [PH_APDEStore].[stage].[mcaid_mcare_pha_elig_calyear]
	SELECT COUNT(*) FROM [PH_APDEStore].[final].[mcaid_mcare_pha_elig_calyear]

	SELECT [year], count(*) as cnt FROM final.mcaid_mcare_pha_elig_calyear 
	GROUP BY [year] ORDER BY [year]
	SELECT [year], count(*) as cnt FROM stage.mcaid_mcare_pha_elig_calyear 
	GROUP BY [year] ORDER BY [year]

	SELECT mcaid, mcare, pha, count(*) as cnt FROM final.mcaid_mcare_pha_elig_calyear 
	WHERE [year] = 2017
	GROUP BY mcaid, mcare, pha ORDER BY mcaid, mcare, pha 
	SELECT mcaid, mcare, pha, count(*) as cnt FROM stage.mcaid_mcare_pha_elig_calyear 
	WHERE [year] = 2017
	GROUP BY mcaid, mcare, pha ORDER BY mcaid, mcare, pha 