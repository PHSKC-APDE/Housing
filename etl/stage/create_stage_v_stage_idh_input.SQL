CREATE VIEW pha.v_stage_idh_identities AS
SELECT 
ssn, pha_id, lname, fname, mname, dob, female, r_aian, r_asian, r_black, r_hisp, r_nhpi, r_white, id_hash, 'kcha_50058' AS source
FROM pha.stage_kcha
WHERE ssn IS NOT NULL OR pha_id IS NOT NULL OR lname IS NOT NULL OR fname IS NOT NULL OR mname IS NOT NULL OR dob IS NOT NULL OR female IS NOT NULL
UNION

/*
  -- Right now there are no female or race field equivalents for HH
-- This means there is a lot of unncessary repetition all in the name of finding a few corner cases where the
-- HH identity is not found in the main data
-- Do not bring in for now but if hh_female and hh_r_<race> fields are set up then reconsider
SELECT 
TOP 100
hh_ssn AS ssn, hh_pha_id AS pha_id, hh_lname AS lname, hh_fname AS fname, hh_mname AS mname, 
hh_dob AS dob, NULL AS gender, 
NULL as r_aian, NULL as r_asian, NULL as r_black, 
NULL as r_hisp, NULL as r_nhpi, NULL as r_white,
-- Add hash with placeholder for gender
CONVERT(CHAR(64), HASHBYTES('SHA2_256', concat(hh_ssn, '|', hh_pha_id, '|', hh_lname, '|', hh_fname, '|', hh_mname, '|', hh_dob, '|')), 2) AS id_hash
FROM pha.stage_kcha
*/

SELECT
ssn, pha_id, lname, fname, mname, dob, female, r_aian, r_asian, r_black, r_hisp, r_nhpi, r_white, id_hash, 'sha_50058' AS source
FROM pha.stage_sha
WHERE ssn IS NOT NULL OR pha_id IS NOT NULL OR lname IS NOT NULL OR fname IS NOT NULL OR mname IS NOT NULL OR dob IS NOT NULL OR female IS NOT NULL
UNION

SELECT
ssn, pha_id, lname, fname, mname, dob, NULL AS female, 
NULL as r_aian, NULL as r_asian, NULL as r_black, 
NULL as r_hisp, NULL as r_nhpi, NULL as r_white, 
CONVERT(CHAR(64), HASHBYTES('SHA2_256', concat(ssn, '|', pha_id, '|', lname, '|', fname, '|', mname, '|', dob, '|')), 2) AS id_hash,
'pha_exit' AS source
FROM pha.stage_pha_exit
WHERE ssn IS NOT NULL OR pha_id IS NOT NULL OR lname IS NOT NULL OR fname IS NOT NULL OR mname IS NOT NULL OR dob IS NOT NULL

UNION
SELECT
ssn, NULL AS pha_id, lname, fname, mname, dob, female, r_aian, r_asian, r_black, r_hisp, r_nhpi, r_white, id_hash,
'pha_waitlist' AS source
FROM pha.stage_pha_waitlist
WHERE ssn IS NOT NULL OR lname IS NOT NULL OR fname IS NOT NULL OR mname IS NOT NULL OR dob IS NOT NULL