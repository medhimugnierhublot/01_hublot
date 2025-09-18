WITH
-- ================
-- Base: Lists & Clients
-- ================
list_base AS (
  SELECT DISTINCT
    bout.name AS bout_name,
    bout.market AS bout_market,
    cl.id AS list_id,
    DATE(cl.created_date) AS created_date
  FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_client_list_joined` cl
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_boutiques` bout
    ON cl.business_entity_c = bout.id
  WHERE cl.list_type_c = 'store'
),

client_list_members AS (
  SELECT DISTINCT
    cl.client_c,
    cl.client_list_c,
    ap.id AS account_id
  FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_client_list_joined` cl
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
    ON cl.client_c = ap.id
  WHERE NOT cl.is_deleted AND cl.list_type_c = 'store'
),

-- ================
-- Outreaches (denominator)
-- ================
outreaches_attributed AS (
  SELECT
    cm.client_c,
    cb.list_id,
    ap.id AS account_id,
    tsks.channel_c,
    DATE(tsks.created_date) AS outreach_date,
    cb.bout_market AS market
  FROM client_list_members cm
  JOIN list_base cb ON cb.list_id = cm.client_list_c
  JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
    ON cm.client_c = ap.id
  JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_task_filtered` tsks
    ON ap.id = tsks.account_id
  WHERE DATE(tsks.created_date) BETWEEN cb.created_date AND DATE_ADD(cb.created_date, INTERVAL 90 DAY)
    AND tsks.channel_c IN ('Call','Email','Kakao','Line','SMS','WeChat','WhatsApp')
),

-- Offline actions (optional for "touch")
offline_actions_attributed AS (
  SELECT
    cm.client_c,
    cb.list_id,
    ap.id AS account_id,
    DATE(tsks.activity_date) AS activity_date
  FROM client_list_members cm
  JOIN list_base cb ON cb.list_id = cm.client_list_c
  JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
    ON cm.client_c = ap.id
  JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_task_filtered` tsks
    ON ap.id = tsks.account_id
  WHERE tsks.type IS NOT NULL
    AND tsks.record_type_id = "01267000000V6gLAAS"   -- offline actions
    AND DATE(tsks.activity_date) BETWEEN cb.created_date AND DATE_ADD(cb.created_date, INTERVAL 90 DAY)
),

-- First touch (outreach OR offline)
touch_events AS (
  SELECT client_c, list_id, outreach_date AS event_date
  FROM outreaches_attributed
  UNION ALL
  SELECT client_c, list_id, activity_date AS event_date
  FROM offline_actions_attributed
),

first_touch AS (
  SELECT
    te.client_c,
    te.list_id,
    MIN(te.event_date) AS first_touch_3m
  FROM touch_events te
  GROUP BY te.client_c, te.list_id
),

-- Sales aggregated AFTER first touch (numerator)
sales_attributed AS (
  SELECT
    cm.client_c,
    cb.list_id,
    DATE(COALESCE(b.warranty_activation_date_c, b.purchase_date_c)) AS purchase_date,
    b.id AS sale_id
  FROM client_list_members cm
  JOIN list_base cb ON cb.list_id = cm.client_list_c
  JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_c_filtered` b
    ON cm.account_id = b.account_c
  WHERE b.is_watch_c = TRUE
),

sales_aggregated AS (
  SELECT
    sa.client_c,
    sa.list_id,
    COUNT(DISTINCT CASE
      WHEN sa.purchase_date BETWEEN lb.created_date AND DATE_ADD(lb.created_date, INTERVAL 90 DAY)
           AND ft.first_touch_3m IS NOT NULL
           AND sa.purchase_date >= ft.first_touch_3m
      THEN sa.sale_id END) AS converted_sales
  FROM sales_attributed sa
  JOIN list_base lb USING(list_id)
  LEFT JOIN first_touch ft
    ON sa.client_c = ft.client_c
   AND sa.list_id = ft.list_id
  GROUP BY sa.client_c, sa.list_id
),

-- Roll-up by region (⚡ only oa.market used here)
regional_rollup AS (
  SELECT
    CASE
      WHEN oa.market IN ('Central Europe','Eastern Europe & Scandinavia','France & BeLux',
                         'Iberia','Italy','Switzerland','UK') THEN 'Europe'
      WHEN oa.market IN ('North America','Mexico') THEN 'North America'
      WHEN oa.market IN ('Eastern Mediterranean','MEA') THEN 'Middle East'
      WHEN oa.market IN ('Australia','India','SEA') THEN 'APAC'
      WHEN oa.market IN ('Greater China') THEN 'China'
      WHEN oa.market = 'Japan' THEN 'Japan'
      WHEN oa.market = 'South Korea' THEN 'Korea'
      ELSE 'Other'
    END AS region,
    COUNT(DISTINCT oa.client_c || '-' || oa.list_id || '-' || oa.outreach_date || '-' || oa.channel_c) AS total_outreaches,
    SUM(sa.converted_sales) AS total_converted_sales
  FROM outreaches_attributed oa
  LEFT JOIN sales_aggregated sa
    ON oa.client_c = sa.client_c AND oa.list_id = sa.list_id
  GROUP BY region
),

-- Global rollup
global_rollup AS (
  SELECT
    'Global' AS region,
    SUM(total_outreaches) AS total_outreaches,
    SUM(total_converted_sales) AS total_converted_sales
  FROM regional_rollup
)

-- Final pivot
SELECT
  -- KPI
  SAFE_DIVIDE(SUM(CASE WHEN region='Global' THEN total_converted_sales END),
              SUM(CASE WHEN region='Global' THEN total_outreaches END)) AS Global_Figures_Input,
  SAFE_DIVIDE(SUM(CASE WHEN region='Europe' THEN total_converted_sales END),
              SUM(CASE WHEN region='Europe' THEN total_outreaches END)) AS Europe,
  SAFE_DIVIDE(SUM(CASE WHEN region='North America' THEN total_converted_sales END),
              SUM(CASE WHEN region='North America' THEN total_outreaches END)) AS North_America,
  SAFE_DIVIDE(SUM(CASE WHEN region='Middle East' THEN total_converted_sales END),
              SUM(CASE WHEN region='Middle East' THEN total_outreaches END)) AS Middle_East,
  SAFE_DIVIDE(SUM(CASE WHEN region='APAC' THEN total_converted_sales END),
              SUM(CASE WHEN region='APAC' THEN total_outreaches END)) AS APAC,
  SAFE_DIVIDE(SUM(CASE WHEN region='China' THEN total_converted_sales END),
              SUM(CASE WHEN region='China' THEN total_outreaches END)) AS China,
  SAFE_DIVIDE(SUM(CASE WHEN region='Japan' THEN total_converted_sales END),
              SUM(CASE WHEN region='Japan' THEN total_outreaches END)) AS Japan,
  SAFE_DIVIDE(SUM(CASE WHEN region='Korea' THEN total_converted_sales END),
              SUM(CASE WHEN region='Korea' THEN total_outreaches END)) AS Korea
FROM (
  SELECT * FROM regional_rollup
  UNION ALL
  SELECT * FROM global_rollup
);
