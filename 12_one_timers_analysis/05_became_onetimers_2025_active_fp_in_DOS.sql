CREATE OR REPLACE VIEW
`hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_first_purchase_last_purchase.DATA_ANALYSIS_ONE_TIMERS_05_became_onetimers_2025_active_fp_in_DOS` 
AS
SELECT 
    bec_ot_2025.*,
    ap.*
FROM
    `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_first_purchase_last_purchase.DATA_ANALYSIS_ONE_TIMERS_03_became_onetimers_2025` bec_ot_2025
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap ON bec_ot_2025.account_id = ap.id
WHERE
    status = 'Active' AND
    DATE(first_dos_purchase_date_c) >= DATE'2025-01-01'

-- 1.3K