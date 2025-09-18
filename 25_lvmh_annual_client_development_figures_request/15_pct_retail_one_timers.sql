WITH active_clients AS (
  SELECT DISTINCT
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
    END AS region
  FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
    ON ah.account_id = ap.id
  WHERE ah.photo_date = TIMESTAMP("2025-08-01 00:00:00+00")
    AND ah.life_time_segment != 'Prospect'
),

transactions_per_client AS (
  SELECT
    bel.account_c AS account_id,
    COUNT(DISTINCT bel.belonging_id_c) AS nb_transactions,
    ARRAY_AGG(DISTINCT LOWER(COALESCE(bout.name, 'unknown_boutique'))) AS boutiques
  FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_c_filtered` bel
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_boutiques` bout
    ON bel.boutique_of_purchase_c = bout.id
  GROUP BY bel.account_c
),

one_timers AS (
  SELECT
    tpc.account_id,
    CASE
      WHEN ARRAY_LENGTH(tpc.boutiques) = 1 AND boutiques[OFFSET(0)] LIKE '%eboutique%' THEN 'online'
      WHEN ARRAY_LENGTH(tpc.boutiques) = 1 AND boutiques[OFFSET(0)] NOT LIKE '%eboutique%' THEN 'retail'
      ELSE 'unknown'
    END AS one_timer_type
  FROM transactions_per_client tpc
  WHERE nb_transactions = 1
),

regional_rollup AS (
  SELECT
    ac.region,
    SAFE_DIVIDE(
      COUNT(DISTINCT CASE WHEN ot.one_timer_type = 'retail' THEN ot.account_id END),
      COUNT(DISTINCT ot.account_id)
    ) AS pct_retail_one_timers
  FROM active_clients ac
  JOIN one_timers ot
    ON ac.account_id = ot.account_id
  GROUP BY ac.region
),

global_rollup AS (
  SELECT
    'Global' AS region,
    SAFE_DIVIDE(
      COUNT(DISTINCT CASE WHEN ot.one_timer_type = 'retail' THEN ot.account_id END),
      COUNT(DISTINCT ot.account_id)
    ) AS pct_retail_one_timers
  FROM active_clients ac
  JOIN one_timers ot
    ON ac.account_id = ot.account_id
)

-- ============================
-- Final Pivot Output
-- ============================
SELECT
  MAX(CASE WHEN region = 'Global' THEN pct_retail_one_timers END) AS Global_Figures_Input,
  MAX(CASE WHEN region = 'Europe' THEN pct_retail_one_timers END) AS Europe,
  MAX(CASE WHEN region = 'North America' THEN pct_retail_one_timers END) AS North_America,
  MAX(CASE WHEN region = 'Middle East' THEN pct_retail_one_timers END) AS Middle_East,
  MAX(CASE WHEN region = 'APAC' THEN pct_retail_one_timers END) AS APAC,
  MAX(CASE WHEN region = 'China' THEN pct_retail_one_timers END) AS China,
  MAX(CASE WHEN region = 'Japan' THEN pct_retail_one_timers END) AS Japan,
  MAX(CASE WHEN region = 'Korea' THEN pct_retail_one_timers END) AS Korea
FROM (
  SELECT * FROM global_rollup
  UNION ALL
  SELECT * FROM regional_rollup
);
