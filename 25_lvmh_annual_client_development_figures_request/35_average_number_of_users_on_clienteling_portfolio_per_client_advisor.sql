WITH region_mapping AS (
  SELECT
    ap.id AS account_id,
    CASE
      WHEN pbout.market IN ('Central Europe','Eastern Europe & Scandinavia','France & BeLux','Iberia','Italy','Switzerland','UK') THEN 'Europe'
      WHEN pbout.market IN ('North America','Mexico') THEN 'North America'
      WHEN pbout.market IN ('Eastern Mediterranean','MEA') THEN 'Middle East'
      WHEN pbout.market IN ('Australia','India','SEA') THEN 'APAC'
      WHEN pbout.market = 'Greater China' THEN 'China'
      WHEN pbout.market = 'Japan' THEN 'Japan'
      WHEN pbout.market = 'South Korea' THEN 'Korea'
      ELSE 'Other'
    END AS region,
    ah.sales_person_new_c,
    ah.life_time_segment,
    ah.status
  FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
    ON ah.account_id = ap.id
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_primary_boutiques` pbout
    ON ah.primary_boutique_c = pbout.id
  WHERE ah.photo_date = TIMESTAMP("2025-08-01 00:00:00+00")
    AND ah.primary_boutique_c IS NOT NULL
),

-- keep only advisors that use the Clienteling app
classified AS (
  SELECT
    rm.region,
    rm.sales_person_new_c,
    rm.account_id,
    CASE
      WHEN rm.life_time_segment = 'Prospect' THEN 'Prospects'
      WHEN rm.status = 'Active' THEN 'Active Clients'
      WHEN rm.status IN ('Sleeping','Inactive') THEN 'Inactive Clients'
      ELSE 'Inactive Clients'  -- catch-all so types sum to All Users
    END AS user_type
  FROM region_mapping rm
  JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_distinct_sa_joined_users` sa
    ON rm.sales_person_new_c = sa.sales_person_new_c
),

-- per region & advisor & type
portfolio_by_type AS (
  SELECT
    region,
    sales_person_new_c,
    user_type,
    COUNT(DISTINCT account_id) AS nb_users
  FROM classified
  GROUP BY region, sales_person_new_c, user_type
),

-- per region & advisor (all users)
portfolio_all AS (
  SELECT
    region,
    sales_person_new_c,
    COUNT(DISTINCT account_id) AS nb_users
  FROM classified
  GROUP BY region, sales_person_new_c
),

-- denominator per region: distinct advisors using clienteling
advisors_per_region AS (
  SELECT
    region,
    COUNT(DISTINCT sales_person_new_c) AS advisors_all
  FROM portfolio_all
  GROUP BY region
),

-- totals per region & type (numerators), using the SAME denominator per region
regional_breakdown AS (
  SELECT
    p.region,
    p.user_type,
    SUM(p.nb_users) AS total_users,
    a.advisors_all AS total_advisors,
    SAFE_DIVIDE(SUM(p.nb_users), a.advisors_all) AS avg_users_per_advisor
  FROM portfolio_by_type p
  JOIN advisors_per_region a USING (region)
  GROUP BY p.region, p.user_type, a.advisors_all
),

-- "All Users" row per region from portfolio_all with same denominator
regional_all AS (
  SELECT
    pa.region,
    'All Users' AS user_type,
    SUM(pa.nb_users) AS total_users,
    ar.advisors_all AS total_advisors,
    SAFE_DIVIDE(SUM(pa.nb_users), ar.advisors_all) AS avg_users_per_advisor
  FROM portfolio_all pa
  JOIN advisors_per_region ar USING (region)
  GROUP BY pa.region, ar.advisors_all
),

-- Global denominator: distinct advisors across all regions (deduped)
global_den AS (
  SELECT COUNT(DISTINCT sales_person_new_c) AS advisors_global
  FROM portfolio_all
),

-- Global numerators by type and All Users (deduped advisors denominator)
global_by_type AS (
  SELECT
    'Global' AS region,
    user_type,
    SUM(total_users) AS total_users
  FROM regional_breakdown
  GROUP BY user_type
),
global_all AS (
  SELECT
    'Global' AS region,
    'All Users' AS user_type,
    SUM(total_users) AS total_users
  FROM regional_all
)

SELECT region, user_type, total_users, total_advisors, avg_users_per_advisor
FROM (
  -- regional 3 rows (Prospects / Active / Inactive)
  SELECT * FROM regional_breakdown
  UNION ALL
  -- regional All Users row
  SELECT * FROM regional_all
  UNION ALL
  -- global 3 rows + All Users
  SELECT
    gbt.region,
    gbt.user_type,
    gbt.total_users,
    gd.advisors_global AS total_advisors,
    SAFE_DIVIDE(gbt.total_users, gd.advisors_global) AS avg_users_per_advisor
  FROM global_by_type gbt CROSS JOIN global_den gd
  UNION ALL
  SELECT
    ga.region,
    ga.user_type,
    ga.total_users,
    gd.advisors_global AS total_advisors,
    SAFE_DIVIDE(ga.total_users, gd.advisors_global) AS avg_users_per_advisor
  FROM global_all ga CROSS JOIN global_den gd
)
ORDER BY
  CASE region
    WHEN 'Global' THEN 0
    WHEN 'Europe' THEN 1
    WHEN 'North America' THEN 2
    WHEN 'Middle East' THEN 3
    WHEN 'APAC' THEN 4
    WHEN 'China' THEN 5
    WHEN 'Japan' THEN 6
    WHEN 'Korea' THEN 7
    ELSE 99
  END,
  CASE user_type
    WHEN 'All Users' THEN 1
    WHEN 'Prospects' THEN 2
    WHEN 'Active Clients' THEN 3
    WHEN 'Inactive Clients' THEN 4
    ELSE 5
  END;
