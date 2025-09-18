WITH region_mapping AS (
  SELECT
    ap.id AS account_id,
    CASE
      WHEN ah.market IN (
        'Central Europe','Eastern Europe & Scandinavia','France & BeLux',
        'Iberia','Italy','Switzerland','UK'
      ) THEN 'Europe'
      WHEN ah.market IN ('North America','Mexico') THEN 'North America'
      WHEN ah.market IN ('Eastern Mediterranean','MEA') THEN 'Middle East'
      WHEN ah.market IN ('Australia','India','SEA') THEN 'APAC'
      WHEN ah.market IN ('Greater China') THEN 'China'
      WHEN ah.market = 'Japan' THEN 'Japan'
      WHEN ah.market = 'South Korea' THEN 'Korea'
      ELSE 'Other'
    END AS region,
    ah.sales_person_new_c
  FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
    ON ah.account_id = ap.id
  WHERE ah.photo_date = TIMESTAMP("2025-08-01 00:00:00+00")
    AND ah.life_time_segment != 'Prospect'
    AND ah.primary_boutique_c IS NOT NULL       -- ✅ exclude accounts with no primary boutique
),

-- Flag clients with a dedicated Client Advisor
clients_with_ca AS (
  SELECT DISTINCT
    rm.account_id
  FROM region_mapping rm
  JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_distinct_sa_joined_users` sa
    ON rm.sales_person_new_c = sa.sales_person_new_c
),

-- Regional rollup
regional_rollup AS (
  SELECT
    rm.region,
    COUNT(DISTINCT CASE WHEN ca.account_id IS NOT NULL THEN rm.account_id END)
      AS nb_with_advisor,
    COUNT(DISTINCT rm.account_id) AS nb_total
  FROM region_mapping rm
  LEFT JOIN clients_with_ca ca
    ON rm.account_id = ca.account_id
  GROUP BY rm.region
),

-- Global rollup
global_rollup AS (
  SELECT
    'Global' AS region,
    COUNT(DISTINCT CASE WHEN ca.account_id IS NOT NULL THEN rm.account_id END)
      AS nb_with_advisor,
    COUNT(DISTINCT rm.account_id) AS nb_total
  FROM region_mapping rm
  LEFT JOIN clients_with_ca ca
    ON rm.account_id = ca.account_id
)

-- ======================
-- Final Pivot
-- ======================
SELECT
  -- % KPI
  SAFE_DIVIDE(MAX(CASE WHEN region = 'Global' THEN nb_with_advisor END),
              MAX(CASE WHEN region = 'Global' THEN nb_total END)) AS Global_Figures_Input,
  SAFE_DIVIDE(MAX(CASE WHEN region = 'Europe' THEN nb_with_advisor END),
              MAX(CASE WHEN region = 'Europe' THEN nb_total END)) AS Europe,
  SAFE_DIVIDE(MAX(CASE WHEN region = 'North America' THEN nb_with_advisor END),
              MAX(CASE WHEN region = 'North America' THEN nb_total END)) AS North_America,
  SAFE_DIVIDE(MAX(CASE WHEN region = 'Middle East' THEN nb_with_advisor END),
              MAX(CASE WHEN region = 'Middle East' THEN nb_total END)) AS Middle_East,
  SAFE_DIVIDE(MAX(CASE WHEN region = 'APAC' THEN nb_with_advisor END),
              MAX(CASE WHEN region = 'APAC' THEN nb_total END)) AS APAC,
  SAFE_DIVIDE(MAX(CASE WHEN region = 'China' THEN nb_with_advisor END),
              MAX(CASE WHEN region = 'China' THEN nb_total END)) AS China,
  SAFE_DIVIDE(MAX(CASE WHEN region = 'Japan' THEN nb_with_advisor END),
              MAX(CASE WHEN region = 'Japan' THEN nb_total END)) AS Japan,
  SAFE_DIVIDE(MAX(CASE WHEN region = 'Korea' THEN nb_with_advisor END),
              MAX(CASE WHEN region = 'Korea' THEN nb_total END)) AS Korea,

  -- Absolute counts (with advisor)
  MAX(CASE WHEN region = 'Global' THEN nb_with_advisor END) AS Global_nb_with_advisor,
  MAX(CASE WHEN region = 'Europe' THEN nb_with_advisor END) AS Europe_nb_with_advisor,
  MAX(CASE WHEN region = 'North America' THEN nb_with_advisor END) AS North_America_nb_with_advisor,
  MAX(CASE WHEN region = 'Middle East' THEN nb_with_advisor END) AS Middle_East_nb_with_advisor,
  MAX(CASE WHEN region = 'APAC' THEN nb_with_advisor END) AS APAC_nb_with_advisor,
  MAX(CASE WHEN region = 'China' THEN nb_with_advisor END) AS China_nb_with_advisor,
  MAX(CASE WHEN region = 'Japan' THEN nb_with_advisor END) AS Japan_nb_with_advisor,
  MAX(CASE WHEN region = 'Korea' THEN nb_with_advisor END) AS Korea_nb_with_advisor,

  -- Absolute counts (total clients, denominator)
  MAX(CASE WHEN region = 'Global' THEN nb_total END) AS Global_nb_total,
  MAX(CASE WHEN region = 'Europe' THEN nb_total END) AS Europe_nb_total,
  MAX(CASE WHEN region = 'North America' THEN nb_total END) AS North_America_nb_total,
  MAX(CASE WHEN region = 'Middle East' THEN nb_total END) AS Middle_East_nb_total,
  MAX(CASE WHEN region = 'APAC' THEN nb_total END) AS APAC_nb_total,
  MAX(CASE WHEN region = 'China' THEN nb_total END) AS China_nb_total,
  MAX(CASE WHEN region = 'Japan' THEN nb_total END) AS Japan_nb_total,
  MAX(CASE WHEN region = 'Korea' THEN nb_total END) AS Korea_nb_total

FROM (
  SELECT * FROM regional_rollup
  UNION ALL
  SELECT * FROM global_rollup
);
