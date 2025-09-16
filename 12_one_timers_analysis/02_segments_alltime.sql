SELECT
macro_segment,
COUNT(distinct id) as distinct_ids
FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
GROUP BY 1
ORDER BY 2 DESC

-- Prospect:118K
-- One Timer:150K
-- Loyal:19K
-- VIC:2K
-- VVIC:1K