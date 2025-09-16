WITH filtered_belongings AS (
  SELECT *
  FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_c_filtered`
  WHERE
    is_watch_c = TRUE AND
    active_c = TRUE AND
    is_deleted = FALSE AND
    sav_active_c = FALSE AND
    LEFT(name,3) NOT IN ('400','440','450') -- Exclude Big Bang E
),

belonging_summary AS (
  SELECT
    account_c,
    MAX(COALESCE(warranty_activation_date_c, purchase_date_c)) AS last_purchase_date,
    COUNT(*) AS nb_purchase,
    SAFE_CAST(SUM(retail_price_chf_c) AS INT64) AS total_purchase_chf
  FROM filtered_belongings
  GROUP BY account_c
),

belongings_jsonified AS (
  SELECT
    account_c,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(
      name AS product_name,
      COALESCE(warranty_activation_date_c, purchase_date_c) AS purchase_date,
      SAFE_CAST(retail_price_chf_c AS INT64) AS price_chf,
      is_watch_c,
      active_c,
      is_deleted,
      sav_active_c
    ))) AS belongings_json
  FROM filtered_belongings
  GROUP BY account_c
)

SELECT
    acc.id AS account_id,
    acc.macro_segment AS segment,
    acc.permission_to_contact_pc AS permission_to_contact,
    acc.billing_country AS account_country,
    bout.name AS boutique_name,
    CAST(COALESCE(acc.last_dos_purchase_date_c, bs.last_purchase_date) AS DATE) AS last_purchase,
    bs.nb_purchase,
    bs.total_purchase_chf,
    bj.belongings_json
FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` acc
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_primary_boutiques` bout
    ON acc.primary_boutique_c = bout.primary_boutique_c
LEFT JOIN belonging_summary bs
    ON acc.id = bs.account_c
LEFT JOIN belongings_jsonified bj
    ON acc.id = bj.account_c
WHERE
    acc.permission_to_contact_pc = TRUE
    AND acc.market IN (
        'Central Europe', 'Eastern Europe & Scandinavia', 'France & BeLux',
        'Iberia', 'Italy', 'Switzerland', 'UK'
    )
    AND bout.name IN ('Hublot Geneva Boutique')
    AND acc.macro_segment NOT IN ('Prospect','Inactive', 'VIC/VVIC')
    AND CAST(COALESCE(acc.last_dos_purchase_date_c, bs.last_purchase_date) AS DATE) <= DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH)
    AND CAST(COALESCE(acc.last_dos_purchase_date_c, bs.last_purchase_date) AS DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 72 MONTH)