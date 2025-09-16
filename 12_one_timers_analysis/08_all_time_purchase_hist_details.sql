WITH ordered_data AS (
    SELECT
        bel.account_c AS account_id,
        COALESCE(bel.warranty_activation_date_c, bel.purchase_date_c) AS purchase_date,
        bel.retail_price_chf_c,
        bel.product_reference_c,
        ROW_NUMBER() OVER (
            PARTITION BY bel.account_c 
            ORDER BY COALESCE(bel.warranty_activation_date_c, bel.purchase_date_c) ASC
        ) AS rn
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_first_purchase_last_purchase.DATA_ANALYSIS_ONE_TIMERS_05_became_onetimers_2025_active_fp_in_DOS` ot
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_c_filtered` bel
        ON ot.account_id = bel.account_c
    WHERE bel.is_watch_c
)

SELECT
    account_id,
    purchase_date,
    retail_price_chf_c,
    product_reference_c
FROM ordered_data
WHERE 
    rn = 2
    -- rn > 1 (switch if you want not only the second one, but all the next ones)
