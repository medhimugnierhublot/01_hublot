WITH region_mapping AS (
  SELECT
    ap.id AS account_id,
    CASE
      WHEN ah.market IN (
        'Central Europe', 'Eastern Europe & Scandinavia', 'France & BeLux',
        'Iberia', 'Italy', 'Switzerland', 'UK'
      ) THEN 'Europe'
      WHEN ah.market IN ('North America', 'Mexico') THEN 'North America'
      WHEN ah.market IN ('Eastern Mediterranean', 'MEA') THEN 'Middle East'
      WHEN ah.market IN ('Australia', 'India', 'SEA') THEN 'APAC'
      WHEN ah.market = 'Greater China' THEN 'China'
      WHEN ah.market = 'Japan' THEN 'Japan'
      WHEN ah.market = 'South Korea' THEN 'Korea'
      ELSE 'Other'
    END AS region
  FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
    ON ah.account_id = ap.id
  LEFT JOIN (
    -- Purchases (belongings) in the last 3 years
    SELECT
      account_c AS account_id,
      COUNT(*) AS nb_purchases_last_3y
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_c_filtered`
    WHERE DATE(COALESCE(warranty_activation_date_c, purchase_date_c))
          BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR) AND CURRENT_DATE()
    GROUP BY account_c
  ) bm
    ON ap.id = bm.account_id
  WHERE ah.photo_date = TIMESTAMP("2025-08-01 00:00:00+00")
    AND ah.life_time_segment != 'Prospect'
    -- Active client = at least one purchase in the last 3 years
    AND bm.account_id IS NOT NULL
)

-- Region-level counts + Global roll-up
SELECT 'All Regions' AS region,
       COUNT(DISTINCT account_id) AS nb_active_clients_last_3y
FROM region_mapping

UNION ALL

SELECT region,
       COUNT(DISTINCT account_id) AS nb_active_clients_last_3y
FROM region_mapping
GROUP BY region

-- enforce custom order
ORDER BY CASE region
  WHEN 'All Regions'   THEN 0
  WHEN 'Europe'        THEN 1
  WHEN 'North America' THEN 2
  WHEN 'Middle East'   THEN 3
  WHEN 'APAC'          THEN 4
  WHEN 'China'         THEN 5
  WHEN 'Japan'         THEN 6
  WHEN 'Korea'         THEN 7
  WHEN 'Other'         THEN 8
  ELSE 9 END;
