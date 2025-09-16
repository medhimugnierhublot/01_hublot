WITH 

belonging_summary AS (
  SELECT
    account_c,
    MAX(COALESCE(warranty_activation_date_c, purchase_date_c)) AS last_purchase_date,
    COUNT(*) AS nb_purchase,
    SAFE_CAST(SUM(retail_price_chf_c) AS INT64) AS total_purchase_chf
  FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_c_filtered`
  WHERE 
    is_watch_c = TRUE AND
    active_c
  GROUP BY account_c
)

SELECT
  acc.id,
  acc.billing_country,
  acc.billing_state,
  acc.billing_city,
  acc.first_name,
  acc.last_name,
  acc.created_date,
  acc.person_email,
  acc.permission_to_contact_pc,
  acc.macro_segment,
  acc.status,
  acc.market,
  acc.last_purchase_date_c,
  acc.number_of_belongings_c,
  acc.number_of_watches_c,
  acc.number_of_wishlists_c,
  bout.id as primary_boutique_id,
  pos.name as primary_external_pos
  -- COUNT(DISTINCT acc.id)
    -- acc.id AS acc_id,
    -- acc.macro_segment AS acc_macro_segment,

    -- CASE
    --     WHEN
    --         (acc.email_syntax_quality = 1 AND acc.marketing_consent = 1)
    --         OR
    --         (acc.phone_syntax_quality = 1 AND acc.marketing_consent = 1)
    --     THEN 1
    --     ELSE 0
    -- END AS is_contactable_LVMH,

    -- CASE
    --     WHEN acc.permission_to_contact_pc THEN 1
    --     ELSE 0
    -- END AS is_permission_to_contact_pc,

    -- acc.billing_country_code AS acc_country_code,
    -- acc.billing_country AS acc_country,
    -- acc.billing_state AS acc_state,
    -- bout.billing_country_code AS bout_country_code,
    -- bout.billing_country AS bout_country,
    -- bout.billing_state AS bout_state,
    -- bout.name AS bout_name,

    -- CAST(acc.last_dos_purchase_date_c AS DATE) AS acc_last_dos_purchase_date_c,

    -- COALESCE(CAST(acc.last_dos_purchase_date_c AS DATE), CAST(sellout_max.max_sellout_date AS DATE)) AS last_purchase_date_c,

    -- acc.market AS acc_market,

    -- COALESCE(task_count.nb_offline_actions, 0) AS nb_offline_actions_last_12m,
    -- bout.market AS bout_market,

    -- acc.billing_city AS acc_city,
    -- acc.first_name AS acc_first_name,
    -- acc.last_name AS acc_last_name,

    -- -- New fields from belonging_summary
    -- bs.last_purchase_date,
    -- bs.nb_purchase,
    -- bs.total_purchase_chf

FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` acc

LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_primary_boutiques` bout
    ON acc.primary_boutique_c = bout.primary_boutique_c
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_external_pos` pos
  ON acc.primary_external_pos_c = pos.id

LEFT JOIN belonging_summary bs
    ON acc.id = bs.account_c

WHERE 
    acc.permission_to_contact_pc AND
    bout.id IS NULL AND
    (
    acc.billing_country_code IN 
        (
        'GB',
        'FR',
        'CH',
        'JP' 
        ) 
      OR 
    acc.billing_state IN 
    (
    'New York',
    'Nevada'
    )
    )
    -- <> bout.billing_country_code

-- GROUP BY 1
-- ORDER BY 2 desc