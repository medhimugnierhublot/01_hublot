-- Subquery to generate ranked purchases per account
WITH ranked_purchases AS (
  SELECT
    account_c,
    COALESCE(warranty_activation_date_c, purchase_date_c) AS purchase_date,
    product_reference_c,
    SAFE_CAST(retail_price_chf_c AS INT64) AS retail_price_chf,
    ROW_NUMBER() OVER (
      PARTITION BY account_c
      ORDER BY COALESCE(warranty_activation_date_c, purchase_date_c) DESC
    ) AS rn
  FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_c_filtered`
  WHERE is_watch_c = TRUE AND active_c
),


-- Aggregate all purchases per account
belonging_summary AS (
  SELECT
    account_c,
    MAX(purchase_date) AS last_purchase_date,
    COUNT(*) AS nb_purchase,
    SAFE_CAST(SUM(retail_price_chf) AS INT64) AS total_purchase_chf
  FROM ranked_purchases
  GROUP BY account_c
),


-- Aggregate only last 5 purchases as text
purchase_details_5 AS (
  SELECT
    account_c,
    ARRAY_TO_STRING(
      ARRAY_AGG(
        FORMAT_DATE('%Y-%m-%d', purchase_date) || ': (' || product_reference_c || ', ' || FORMAT('%\'d', retail_price_chf) || ' CHF)'
        ORDER BY purchase_date DESC
      ),
      '\n'
    ) AS purchase_details_last_5
  FROM ranked_purchases
  WHERE rn <= 5
  GROUP BY account_c
)


-- Final select
SELECT
  acc.id,
  acc.billing_country AS country,
  acc.first_name,
  acc.last_name,
  -- acc.status_pc AS status,
  bout.name AS primary_dos_boutique,
  DATE(bs.last_purchase_date) AS last_purchase,
  bs.nb_purchase,
  FORMAT('%\'d', bs.total_purchase_chf) AS total_purchase_chf,
  pd.purchase_details_last_5


FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` acc


LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_primary_boutiques` bout
  ON acc.primary_boutique_c = bout.primary_boutique_c


LEFT JOIN belonging_summary bs
  ON acc.id = bs.account_c


LEFT JOIN purchase_details_5 pd
  ON acc.id = pd.account_c


WHERE
  acc.status_pc = 'Active'
  AND acc.macro_segment = 'VVIC'
  AND acc.permission_to_contact_pc


ORDER BY bs.total_purchase_chf DESC
LIMIT 60