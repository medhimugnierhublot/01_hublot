WITH region_mapping AS (
  SELECT
    ap.id AS account_id,
    ap.person_email,
    CASE
      WHEN ap.market IN ('Central Europe','Eastern Europe & Scandinavia','France & BeLux',
                         'Iberia','Italy','Switzerland','UK') THEN 'Europe'
      WHEN ap.market IN ('North America','Mexico') THEN 'North America'
      WHEN ap.market IN ('Eastern Mediterranean','MEA') THEN 'Middle East'
      WHEN ap.market IN ('Australia','India','SEA') THEN 'APAC'
      WHEN ap.market = 'Greater China' THEN 'China'
      WHEN ap.market = 'Japan' THEN 'Japan'
      WHEN ap.market = 'South Korea' THEN 'Korea'
      ELSE 'Other'
    END AS region
  FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
),

regional_rollup AS (
  SELECT
    region,
    COUNT(DISTINCT account_id) AS total_clients,
    COUNT(DISTINCT CASE WHEN person_email IS NOT NULL THEN account_id END) AS clients_with_email
  FROM region_mapping
  GROUP BY region
),

global_rollup AS (
  SELECT
    'Global' AS region,
    SUM(total_clients) AS total_clients,
    SUM(clients_with_email) AS clients_with_email
  FROM regional_rollup
)

SELECT
  SAFE_DIVIDE(SUM(CASE WHEN region='Global' THEN clients_with_email END),
              SUM(CASE WHEN region='Global' THEN total_clients END)) AS Global_Figures_Input,
  SAFE_DIVIDE(SUM(CASE WHEN region='Europe' THEN clients_with_email END),
              SUM(CASE WHEN region='Europe' THEN total_clients END)) AS Europe,
  SAFE_DIVIDE(SUM(CASE WHEN region='North America' THEN clients_with_email END),
              SUM(CASE WHEN region='North America' THEN total_clients END)) AS North_America,
  SAFE_DIVIDE(SUM(CASE WHEN region='Middle East' THEN clients_with_email END),
              SUM(CASE WHEN region='Middle East' THEN total_clients END)) AS Middle_East,
  SAFE_DIVIDE(SUM(CASE WHEN region='APAC' THEN clients_with_email END),
              SUM(CASE WHEN region='APAC' THEN total_clients END)) AS APAC,
  SAFE_DIVIDE(SUM(CASE WHEN region='China' THEN clients_with_email END),
              SUM(CASE WHEN region='China' THEN total_clients END)) AS China,
  SAFE_DIVIDE(SUM(CASE WHEN region='Japan' THEN clients_with_email END),
              SUM(CASE WHEN region='Japan' THEN total_clients END)) AS Japan,
  SAFE_DIVIDE(SUM(CASE WHEN region='Korea' THEN clients_with_email END),
              SUM(CASE WHEN region='Korea' THEN total_clients END)) AS Korea
FROM (
  SELECT * FROM regional_rollup
  UNION ALL
  SELECT * FROM global_rollup
);
