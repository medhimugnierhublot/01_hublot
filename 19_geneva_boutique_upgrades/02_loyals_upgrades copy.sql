    -- ...converted to Loyal/VIC/VVIC clients ​in 2024
WITH 
one_timers_dos_prev_period AS 
    (
    SELECT
        DISTINCT ap.id
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap 
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah ON ah.account_id = ap.id
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_primary_boutiques` bout
    ON ap.primary_boutique_c = bout.primary_boutique_c
    WHERE 
        ah.photo_date = '2025-01-01 00:00:00 UTC' AND
        ah.life_time_segment = 'Loyal' AND 
        bout.name = "Hublot Geneva Boutique"
    )
SELECT
    COUNT(DISTINCT ap.id)
FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap 
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah
    ON ah.account_id = ap.id
LEFT JOIN one_timers_dos_prev_period USING(id)
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_primary_boutiques` bout
ON ap.primary_boutique_c = bout.primary_boutique_c
WHERE
    one_timers_dos_prev_period.id IS NOT NULL AND
    ah.photo_date = "2025-07-01 00:00:00 UTC" AND
    ah.life_time_segment in ('VIC','VVIC') AND 
    bout.name = "Hublot Geneva Boutique"