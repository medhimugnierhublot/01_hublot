WITH
-- Regional mapping from markets
region_mapping AS (
  SELECT
    ap.person_contact_id AS subscriber_key,
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

-- Unique Opens (Newsletters only)
opens AS (
  SELECT
    rm.region,
    COUNT(DISTINCT s.subscriber_key || '-' || s.send_id) AS nb_opens
  FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_event_mc_sent_date_joined` s
  JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_send_mc` sm
    ON s.send_id = sm.id
  JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_email_mc_joined_types_joined_journey` e
    ON CAST(e.id AS INT64) = sm.email_id
  JOIN region_mapping rm
    ON rm.subscriber_key = s.subscriber_key
  WHERE s.event_type = 'Open'
    AND e.type_c = 'Newsletter'
    AND s._sent_event_date > '2025-01-01'
  GROUP BY rm.region
),

-- Unique Clicks (Newsletters only)
clicks AS (
  SELECT
    rm.region,
    COUNT(DISTINCT s.subscriber_key || '-' || s.send_id) AS nb_clicks
  FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_event_mc_sent_date_joined` s
  JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_send_mc` sm
    ON s.send_id = sm.id
  JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_email_mc_joined_types_joined_journey` e
    ON CAST(e.id AS INT64) = sm.email_id
  JOIN region_mapping rm
    ON rm.subscriber_key = s.subscriber_key
  WHERE s.event_type = 'Click'
    AND e.type_c = 'Newsletter'
    AND s._sent_event_date > '2025-01-01'
  GROUP BY rm.region
),

-- Regional rollup
regional AS (
  SELECT
    o.region,
    o.nb_opens,
    COALESCE(c.nb_clicks, 0) AS nb_clicks,
    SAFE_DIVIDE(COALESCE(c.nb_clicks, 0), NULLIF(o.nb_opens, 0)) AS ctr_on_opens
  FROM opens o
  LEFT JOIN clicks c
    ON o.region = c.region
),

-- Global rollup
global AS (
  SELECT
    'Global' AS region,
    SUM(nb_opens) AS nb_opens,
    SUM(nb_clicks) AS nb_clicks,
    SAFE_DIVIDE(SUM(nb_clicks), NULLIF(SUM(nb_opens), 0)) AS ctr_on_opens
  FROM regional
),

unioned AS (
  SELECT * FROM regional
  UNION ALL
  SELECT * FROM global
)

-- Final pivot with Opens + Clicks + CTR
SELECT
  -- CTR %
  MAX(CASE WHEN region='Global' THEN ctr_on_opens END) AS Global_CTR_on_Opens,
  MAX(CASE WHEN region='Europe' THEN ctr_on_opens END) AS Europe_CTR_on_Opens,
  MAX(CASE WHEN region='North America' THEN ctr_on_opens END) AS North_America_CTR_on_Opens,
  MAX(CASE WHEN region='Middle East' THEN ctr_on_opens END) AS Middle_East_CTR_on_Opens,
  MAX(CASE WHEN region='APAC' THEN ctr_on_opens END) AS APAC_CTR_on_Opens,
  MAX(CASE WHEN region='China' THEN ctr_on_opens END) AS China_CTR_on_Opens,
  MAX(CASE WHEN region='Japan' THEN ctr_on_opens END) AS Japan_CTR_on_Opens,
  MAX(CASE WHEN region='Korea' THEN ctr_on_opens END) AS Korea_CTR_on_Opens,

  -- Numerator = Clicks
  MAX(CASE WHEN region='Global' THEN nb_clicks END) AS Global_Clicks,
  MAX(CASE WHEN region='Europe' THEN nb_clicks END) AS Europe_Clicks,
  MAX(CASE WHEN region='North America' THEN nb_clicks END) AS North_America_Clicks,
  MAX(CASE WHEN region='Middle East' THEN nb_clicks END) AS Middle_East_Clicks,
  MAX(CASE WHEN region='APAC' THEN nb_clicks END) AS APAC_Clicks,
  MAX(CASE WHEN region='China' THEN nb_clicks END) AS China_Clicks,
  MAX(CASE WHEN region='Japan' THEN nb_clicks END) AS Japan_Clicks,
  MAX(CASE WHEN region='Korea' THEN nb_clicks END) AS Korea_Clicks,

  -- Denominator = Opens
  MAX(CASE WHEN region='Global' THEN nb_opens END) AS Global_Opens,
  MAX(CASE WHEN region='Europe' THEN nb_opens END) AS Europe_Opens,
  MAX(CASE WHEN region='North America' THEN nb_opens END) AS North_America_Opens,
  MAX(CASE WHEN region='Middle East' THEN nb_opens END) AS Middle_East_Opens,
  MAX(CASE WHEN region='APAC' THEN nb_opens END) AS APAC_Opens,
  MAX(CASE WHEN region='China' THEN nb_opens END) AS China_Opens,
  MAX(CASE WHEN region='Japan' THEN nb_opens END) AS Japan_Opens,
  MAX(CASE WHEN region='Korea' THEN nb_opens END) AS Korea_Opens
FROM unioned;
