WITH
filtered_belongings AS (
  SELECT *
  FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_c_filtered`
  WHERE
    is_watch_c = TRUE
    AND active_c = TRUE
    AND is_deleted = FALSE
    AND sav_active_c = FALSE
),

belonging_summary AS (
  SELECT
    account_c,
    COALESCE(warranty_activation_date_c, purchase_date_c) AS purchase_date,
    retail_price_chf_c,
    pb.market,
    ap.macro_segment
  FROM filtered_belongings fb
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap ON fb.account_c = ap.id
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah ON ap.id = ah.account_id
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_primary_boutiques` pb ON ah.primary_boutique_c = pb.id
  WHERE 
    DATE(COALESCE(warranty_activation_date_c, purchase_date_c)) >= DATE'2025-01-01' AND
    DATE(ah.photo_date) = DATE'2025-05-01' AND 
    pb.market = 'Japan'
)

SELECT
macro_segment,
count(account_c) as sales,
sum(retail_price_chf_c) AS retail_price_chf_c
FROM belonging_summary
GROUP BY 1 
ORDER BY 2 DESC