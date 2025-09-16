-- CREATE OR REPLACE VIEW
-- `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_first_purchase_last_purchase.DATA_ANALYSIS_ONE_TIMERS_03_became_onetimers_2025` 
-- AS
WITH 
one_timers_jan25 AS 
  (
  SELECT
    DISTINCT ah.account_id
  FROM
    `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah
  WHERE
    photo_date = "2025-01-01 00:00:00 UTC" AND
    life_time_segment = 'One Timer'
  )

SELECT
    DISTINCT ah.account_id
  FROM
    `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah
  LEFT JOIN one_timers_jan25 USING(account_id)
  WHERE
    photo_date = "2025-05-01 00:00:00 UTC" AND
    life_time_segment = 'One Timer' AND
    one_timers_jan25.account_id IS NULL

-- 7K

-- e.g.:
-- 001J500000926VSIAY
-- 001J500000930QLIAY
-- 001J5000009XPJPIA4
-- 001J5000009VjsOIAS
-- 001J50000090xBnIAI