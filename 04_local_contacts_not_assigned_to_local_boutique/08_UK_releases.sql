WITH task_count AS (
  SELECT
    task.account_id,
    COUNT(task.id) AS nb_offline_actions
  FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_task_filtered` task
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_boutiques` ab
    ON task.account_id = ab.id
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_user_with_markets` user
    ON task.owner_id = user.id
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_user_role_filtered` role
    ON task.owner_id = role.id
  WHERE
    task.channel_c = 'Action'
    AND task.type_c IS NOT NULL
    AND (
      (ab.boutique_type_2_c = 'DOS' AND STRPOS(ab.name, 'eBoutique') = 0)
      OR ab.id = '0010X00004snyN3QAI'
    )
    AND (
      STRPOS(role.name, 'Boutique Manager') > 0
      OR STRPOS(role.name, 'Sales Associate') > 0
      OR user.id IN (
        '0050X000007hoVsQAI',
        '0056700000CeROxAAN',
        '0056700000EOFDeAAP'
      )
    )
    AND DATE(task.activity_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
  GROUP BY task.account_id
),

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
    MAX(COALESCE(warranty_activation_date_c, purchase_date_c)) AS last_purchase_date,
    COUNT(*) AS nb_purchase,
    SAFE_CAST(SUM(retail_price_chf_c) AS INT64) AS total_purchase_chf
  FROM filtered_belongings
  GROUP BY account_c
),

belongings_json_summary AS (
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
  acc.first_name AS first_name,
  acc.last_name AS last_name,
  acc.macro_segment AS segment,
  acc.billing_country AS account_country,
  acc.billing_state AS account_state,
  acc.billing_city AS account_city,
  acc.person_email,
  bout.name AS boutique_name,
  CAST(COALESCE(acc.last_dos_purchase_date_c, bs.last_purchase_date) AS DATE) AS last_purchase,
  bs.nb_purchase,
  bs.total_purchase_chf,
  belongings.belongings_json

FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` acc

LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_primary_boutiques` bout
  ON acc.primary_boutique_c = bout.primary_boutique_c

LEFT JOIN task_count
  ON acc.id = task_count.account_id

LEFT JOIN belonging_summary bs
  ON acc.id = bs.account_c

LEFT JOIN belongings_json_summary belongings
  ON acc.id = belongings.account_c

WHERE
  acc.permission_to_contact_pc = TRUE
  AND COALESCE(task_count.nb_offline_actions, 0) < 2
  -- AND bout.billing_country_code = 'UK'
  AND bout.market = 'UK'
  AND acc.billing_country_code IN (
    'AT', 'AU', 'CA', 'CH', 'CN', 'DE', 'ES', 'FR', 'GB',
    'HK', 'IT', 'JP', 'KR', 'MC', 'PT', 'US'
  )
  AND acc.billing_country_code <> bout.billing_country_code
  AND (
    (acc.macro_segment NOT IN ('Prospect','Loyal','VIC','VVIC')
     AND CAST(COALESCE(acc.last_dos_purchase_date_c, bs.last_purchase_date) AS DATE) < DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH))
    OR (acc.macro_segment = 'Prospect'
        AND CAST(acc.created_date AS DATE) < DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH))
  );


