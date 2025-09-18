WITH region_mapping AS (
  SELECT
    ap.id AS account_id,
    ap.person_birthdate,
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
    COUNT(DISTINCT CASE WHEN person_birthdate IS NOT NULL THEN account_id END) AS clients_with_birthday
  FROM region_mapping
  GROUP BY region
),

global_rollup AS (
  SELECT
    'Global Figures Input' AS region,
    SUM(total_clients) AS total_clients,
    SUM(clients_with_birthday) AS clients_with_birthday
  FROM regional_rollup
),

final AS (
  SELECT * FROM regional_rollup
  UNION ALL
  SELECT * FROM global_rollup
)

SELECT
  MAX(CASE WHEN region = 'Global Figures Input' THEN SAFE_DIVIDE(clients_with_birthday, total_clients) END) AS Global_Figures_Input,
  MAX(CASE WHEN region = 'Europe' THEN SAFE_DIVIDE(clients_with_birthday, total_clients) END) AS Europe,
  MAX(CASE WHEN region = 'North America' THEN SAFE_DIVIDE(clients_with_birthday, total_clients) END) AS North_America,
  MAX(CASE WHEN region = 'Middle East' THEN SAFE_DIVIDE(clients_with_birthday, total_clients) END) AS Middle_East,
  MAX(CASE WHEN region = 'APAC' THEN SAFE_DIVIDE(clients_with_birthday, total_clients) END) AS APAC,
  MAX(CASE WHEN region = 'China' THEN SAFE_DIVIDE(clients_with_birthday, total_clients) END) AS China,
  MAX(CASE WHEN region = 'Japan' THEN SAFE_DIVIDE(clients_with_birthday, total_clients) END) AS Japan,
  MAX(CASE WHEN region = 'Korea' THEN SAFE_DIVIDE(clients_with_birthday, total_clients) END) AS Korea
FROM final;
