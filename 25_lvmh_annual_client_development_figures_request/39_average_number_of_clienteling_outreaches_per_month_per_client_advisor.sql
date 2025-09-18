WITH
-- Parameters: tracking window
params AS (
  SELECT DATE('2025-01-01') AS start_date, DATE('2025-08-31') AS end_date
),

-- Outreaches during tracking period
outreaches AS (
  SELECT
    t.owner_id AS advisor_id,
    DATE(t.created_date) AS outreach_date,
    t.channel_c
  FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_task_filtered` t
  JOIN params p
    ON DATE(t.created_date) BETWEEN p.start_date AND p.end_date
  WHERE t.channel_c IN ('Call','Email','Kakao','Line','SMS','WeChat','WhatsApp')
),

-- Advisors using clienteling
advisors AS (
  SELECT DISTINCT sales_person_new_c AS advisor_id
  FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_distinct_sa_joined_users`
),

-- Map advisors to regions using account_history
advisor_regions AS (
  SELECT DISTINCT
    ah.sales_person_new_c AS advisor_id,
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
  WHERE ah.photo_date = TIMESTAMP("2025-08-01 00:00:00+00")
),

-- Outreaches per advisor (filtered to clienteling advisors)
outreaches_per_advisor AS (
  SELECT
    o.advisor_id,
    COUNT(*) AS nb_outreaches
  FROM outreaches o
  JOIN advisors a ON o.advisor_id = a.advisor_id
  GROUP BY o.advisor_id
),

-- Months in tracking period
months_in_period AS (
  SELECT DATE_DIFF(MAX(end_date), MIN(start_date), MONTH) + 1 AS nb_months
  FROM params
),

-- KPI per advisor with region
kpi_per_advisor AS (
  SELECT
    ar.region,
    oa.advisor_id,
    SAFE_DIVIDE(oa.nb_outreaches, m.nb_months) AS monthly_outreaches
  FROM outreaches_per_advisor oa
  JOIN advisor_regions ar ON oa.advisor_id = ar.advisor_id
  CROSS JOIN months_in_period m
),

-- Regional averages
regional_summary AS (
  SELECT
    region,
    AVG(monthly_outreaches) AS avg_monthly_outreaches,
    COUNT(DISTINCT advisor_id) AS total_advisors
  FROM kpi_per_advisor
  GROUP BY region
),

-- Global average
global_summary AS (
  SELECT
    'Global' AS region,
    AVG(monthly_outreaches) AS avg_monthly_outreaches,
    COUNT(DISTINCT advisor_id) AS total_advisors
  FROM kpi_per_advisor
)

-- Final pivot
SELECT
  -- KPI
  MAX(CASE WHEN region = 'Global' THEN avg_monthly_outreaches END) AS Global_Figures_Input,
  MAX(CASE WHEN region = 'Europe' THEN avg_monthly_outreaches END) AS Europe,
  MAX(CASE WHEN region = 'North America' THEN avg_monthly_outreaches END) AS North_America,
  MAX(CASE WHEN region = 'Middle East' THEN avg_monthly_outreaches END) AS Middle_East,
  MAX(CASE WHEN region = 'APAC' THEN avg_monthly_outreaches END) AS APAC,
  MAX(CASE WHEN region = 'China' THEN avg_monthly_outreaches END) AS China,
  MAX(CASE WHEN region = 'Japan' THEN avg_monthly_outreaches END) AS Japan,
  MAX(CASE WHEN region = 'Korea' THEN avg_monthly_outreaches END) AS Korea,

  -- Denominator: advisors
  MAX(CASE WHEN region = 'Global' THEN total_advisors END) AS Global_total_advisors,
  MAX(CASE WHEN region = 'Europe' THEN total_advisors END) AS Europe_total_advisors,
  MAX(CASE WHEN region = 'North America' THEN total_advisors END) AS North_America_total_advisors,
  MAX(CASE WHEN region = 'Middle East' THEN total_advisors END) AS Middle_East_total_advisors,
  MAX(CASE WHEN region = 'APAC' THEN total_advisors END) AS APAC_total_advisors,
  MAX(CASE WHEN region = 'China' THEN total_advisors END) AS China_total_advisors,
  MAX(CASE WHEN region = 'Japan' THEN total_advisors END) AS Japan_total_advisors,
  MAX(CASE WHEN region = 'Korea' THEN total_advisors END) AS Korea_total_advisors

FROM (
  SELECT * FROM regional_summary
  UNION ALL
  SELECT * FROM global_summary
);
