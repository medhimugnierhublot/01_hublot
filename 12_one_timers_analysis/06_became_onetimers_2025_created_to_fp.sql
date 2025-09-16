SELECT
DATE_DIFF(DATE(first_dos_purchase_date_c),DATE(created_date), DAY) AS days_diff,
count(distinct account_id)
FROM
`hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_first_purchase_last_purchase.DATA_ANALYSIS_ONE_TIMERS_05_became_onetimers_2025_active_fp_in_DOS`
GROUP BY 1
ORDER BY 1

-- 0.8K same day
-- +0.2K one week
-- +0.1K one month