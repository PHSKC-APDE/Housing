-- PHA and Medicaid health events data load QA
-- 2018-10-31
--
-- Alastair Matheson
-- PHSKC


-- Check counts of conditions and time in each data set
SELECT
	SUM(CAST(ISNULL([count], 0) AS BIGINT)) AS 'count',
	SUM(CAST(ISNULL([pop], 0) AS BIGINT)) AS 'pop',
	SUM(CAST(ISNULL([pt_days], 0) AS BIGINT)) AS 'pt'
FROM [PH_APDEStore].[dbo].[pha_mcaid_events_suppressed]

SELECT
	SUM(CAST(ISNULL([count], 0) AS BIGINT)) AS 'count',
	SUM(CAST(ISNULL([pop], 0) AS BIGINT)) AS 'pop',
	SUM(CAST(ISNULL([pt_days], 0) AS BIGINT)) AS 'pt'
FROM [PH_APDEStore].[dbo].[pha_mcaid_events_suppressed_load]