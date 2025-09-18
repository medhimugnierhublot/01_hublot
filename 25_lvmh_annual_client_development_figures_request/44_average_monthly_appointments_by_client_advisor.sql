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
  WHERE ah.photo_date = TIMESTAMP("2025-09-01 00:00:00+00")
    AND ah.primary_boutique_c IS NOT NULL
),

-- Clients linked to an active SA
portfolio AS (
  SELECT
    rm.region,
    rm.sales_person_new_c,
    COUNT(DISTINCT rm.account_id) AS nb_users
  FROM region_mapping rm
  JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_distinct_sa_joined_users` sa
    ON rm.sales_person_new_c = sa.sales_person_new_c
  JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_user_with_markets` usr
    ON sa.sales_person_new_c = usr.id
  WHERE usr.is_active
  GROUP BY rm.region, rm.sales_person_new_c
),

-- Regional rollup
regional_rollup AS (
  SELECT
    region,
    SUM(nb_users) AS total_users,
    COUNT(DISTINCT sales_person_new_c) AS total_advisors,
    SAFE_DIVIDE(SUM(nb_users), COUNT(DISTINCT sales_person_new_c)) AS avg_users_per_advisor
  FROM portfolio
  GROUP BY region
),

-- Global rollup
global_rollup AS (
  SELECT
    'Global' AS region,
    SUM(nb_users) AS total_users,
    COUNT(DISTINCT sales_person_new_c) AS total_advisors,
    SAFE_DIVIDE(SUM(nb_users), COUNT(DISTINCT sales_person_new_c)) AS avg_users_per_advisor
  FROM portfolio
)

-- ======================
-- Final Pivot
-- ======================
SELECT
  -- KPI
  MAX(CASE WHEN region = 'Global' THEN avg_users_per_advisor END) AS Global,
  MAX(CASE WHEN region = 'Europe' THEN avg_users_per_advisor END) AS Europe,
  MAX(CASE WHEN region = 'North America' THEN avg_users_per_advisor END) AS North_America,
  MAX(CASE WHEN region = 'Middle East' THEN avg_users_per_advisor END) AS Middle_East,
  MAX(CASE WHEN region = 'APAC' THEN avg_users_per_advisor END) AS APAC,
  MAX(CASE WHEN region = 'China' THEN avg_users_per_advisor END) AS China,
  MAX(CASE WHEN region = 'Japan' THEN avg_users_per_advisor END) AS Japan,
  MAX(CASE WHEN region = 'Korea' THEN avg_users_per_advisor END) AS Korea,

  -- Numerator (total users)
  MAX(CASE WHEN region = 'Global' THEN total_users END) AS Global_total_users,
  MAX(CASE WHEN region = 'Europe' THEN total_users END) AS Europe_total_users,
  MAX(CASE WHEN region = 'North America' THEN total_users END) AS North_America_total_users,
  MAX(CASE WHEN region = 'Middle East' THEN total_users END) AS Middle_East_total_users,
  MAX(CASE WHEN region = 'APAC' THEN total_users END) AS APAC_total_users,
  MAX(CASE WHEN region = 'China' THEN total_users END) AS China_total_users,
  MAX(CASE WHEN region = 'Japan' THEN total_users END) AS Japan_total_users,
  MAX(CASE WHEN region = 'Korea' THEN total_users END) AS Korea_total_users,

  -- Denominator (total advisors)
  MAX(CASE WHEN region = 'Global' THEN total_advisors END) AS Global_total_advisors,
  MAX(CASE WHEN region = 'Europe' THEN total_advisors END) AS Europe_total_advisors,
  MAX(CASE WHEN region = 'North America' THEN total_advisors END) AS North_America_total_advisors,
  MAX(CASE WHEN region = 'Middle East' THEN total_advisors END) AS Middle_East_total_advisors,
  MAX(CASE WHEN region = 'APAC' THEN total_advisors END) AS APAC_total_advisors,
  MAX(CASE WHEN region = 'China' THEN total_advisors END) AS China_total_advisors,
  MAX(CASE WHEN region = 'Japan' THEN total_advisors END) AS Japan_total_advisors,
  MAX(CASE WHEN region = 'Korea' THEN total_advisors END) AS Korea_total_advisors
FROM (
  SELECT * FROM regional_rollup
  UNION ALL
  SELECT * FROM global_rollup
);
