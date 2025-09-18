WITH region_mapping AS (
  SELECT
    ap.id AS account_id,
    ap.gender_pc,
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
    COUNT(DISTINCT CASE WHEN gender_pc = 'Male' THEN account_id END) AS male_clients,
    COUNT(DISTINCT CASE WHEN gender_pc = 'Female' THEN account_id END) AS female_clients,
    COUNT(DISTINCT CASE WHEN gender_pc IS NULL OR gender_pc NOT IN ('Male','Female') THEN account_id END) AS nonbinary_prefnot_clients
  FROM region_mapping
  GROUP BY region
),

global_rollup AS (
  SELECT
    'Global Figures Input' AS region,
    SUM(total_clients) AS total_clients,
    SUM(male_clients) AS male_clients,
    SUM(female_clients) AS female_clients,
    SUM(nonbinary_prefnot_clients) AS nonbinary_prefnot_clients
  FROM regional_rollup
),

final AS (
  SELECT * FROM regional_rollup
  UNION ALL
  SELECT * FROM global_rollup
)

SELECT
  MAX(CASE WHEN region = 'Global Figures Input' THEN SAFE_DIVIDE(male_clients, total_clients) END) AS Global_Figures_Input_male,
  MAX(CASE WHEN region = 'Europe' THEN SAFE_DIVIDE(male_clients, total_clients) END) AS Europe_male,
  MAX(CASE WHEN region = 'North America' THEN SAFE_DIVIDE(male_clients, total_clients) END) AS North_America_male,
  MAX(CASE WHEN region = 'Middle East' THEN SAFE_DIVIDE(male_clients, total_clients) END) AS Middle_East_male,
  MAX(CASE WHEN region = 'APAC' THEN SAFE_DIVIDE(male_clients, total_clients) END) AS APAC_male,
  MAX(CASE WHEN region = 'China' THEN SAFE_DIVIDE(male_clients, total_clients) END) AS China_male,
  MAX(CASE WHEN region = 'Japan' THEN SAFE_DIVIDE(male_clients, total_clients) END) AS Japan_male,
  MAX(CASE WHEN region = 'Korea' THEN SAFE_DIVIDE(male_clients, total_clients) END) AS Korea_male,

  MAX(CASE WHEN region = 'Global Figures Input' THEN SAFE_DIVIDE(female_clients, total_clients) END) AS Global_Figures_Input_female,
  MAX(CASE WHEN region = 'Europe' THEN SAFE_DIVIDE(female_clients, total_clients) END) AS Europe_female,
  MAX(CASE WHEN region = 'North America' THEN SAFE_DIVIDE(female_clients, total_clients) END) AS North_America_female,
  MAX(CASE WHEN region = 'Middle East' THEN SAFE_DIVIDE(female_clients, total_clients) END) AS Middle_East_female,
  MAX(CASE WHEN region = 'APAC' THEN SAFE_DIVIDE(female_clients, total_clients) END) AS APAC_female,
  MAX(CASE WHEN region = 'China' THEN SAFE_DIVIDE(female_clients, total_clients) END) AS China_female,
  MAX(CASE WHEN region = 'Japan' THEN SAFE_DIVIDE(female_clients, total_clients) END) AS Japan_female,
  MAX(CASE WHEN region = 'Korea' THEN SAFE_DIVIDE(female_clients, total_clients) END) AS Korea_female,

  MAX(CASE WHEN region = 'Global Figures Input' THEN SAFE_DIVIDE(nonbinary_prefnot_clients, total_clients) END) AS Global_Figures_Input_nonbinary_prefnot,
  MAX(CASE WHEN region = 'Europe' THEN SAFE_DIVIDE(nonbinary_prefnot_clients, total_clients) END) AS Europe_nonbinary_prefnot,
  MAX(CASE WHEN region = 'North America' THEN SAFE_DIVIDE(nonbinary_prefnot_clients, total_clients) END) AS North_America_nonbinary_prefnot,
  MAX(CASE WHEN region = 'Middle East' THEN SAFE_DIVIDE(nonbinary_prefnot_clients, total_clients) END) AS Middle_East_nonbinary_prefnot,
  MAX(CASE WHEN region = 'APAC' THEN SAFE_DIVIDE(nonbinary_prefnot_clients, total_clients) END) AS APAC_nonbinary_prefnot,
  MAX(CASE WHEN region = 'China' THEN SAFE_DIVIDE(nonbinary_prefnot_clients, total_clients) END) AS China_nonbinary_prefnot,
  MAX(CASE WHEN region = 'Japan' THEN SAFE_DIVIDE(nonbinary_prefnot_clients, total_clients) END) AS Japan_nonbinary_prefnot,
  MAX(CASE WHEN region = 'Korea' THEN SAFE_DIVIDE(nonbinary_prefnot_clients, total_clients) END) AS Korea_nonbinary_prefnot
FROM final;
