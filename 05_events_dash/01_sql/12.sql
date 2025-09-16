WITH campaign_base AS (
  SELECT
    c.id AS campaign_or_subcampaign_id,
    c.name AS campaign_or_subcampaign_name,
    c.parent_id,
    CASE WHEN c.parent_id IS NULL THEN c.id ELSE c.parent_id END AS campaign_id,
    CASE WHEN c.parent_id IS NULL THEN c.name ELSE cp.name END AS campaign_name,
    CASE WHEN c.parent_id IS NULL THEN NULL ELSE c.id END AS subcampaign_id,
    CASE WHEN c.parent_id IS NULL THEN NULL ELSE c.name END AS subcampaign_name,
    FORMAT_DATE('%Y-%m-%d', c.end_date) AS campaign_end_date,
    a.name AS boutique,
    c.territory_c AS territory,
    ab.territory_c AS fallback_territory
  FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_salesforce.campaign` c
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_salesforce.campaign` cp ON c.parent_id = cp.id
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_salesforce.account` a ON a.id = c.boutique_c
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_boutiques` ab ON ab.id = c.boutique_c
  WHERE c.end_date >= '2021-01-01' AND c.id IS NOT NULL
),


campaign_members AS (
  SELECT
    cm.id AS member_id,
    cm.campaign_id,
    cm.contact_id,
    cm.status AS member_status,
    cm.has_responded,
    cm.participated_c,
    ap.id AS account_id,
    ap.macro_segment,
    ap.type AS client_type
  FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_salesforce.campaign_member` cm
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap ON cm.contact_id = ap.person_contact_id
  WHERE NOT cm.is_deleted
),


wishlist_dedup AS (
  SELECT
    DISTINCT
    cm.campaign_id,
    cm.account_id,
    FORMAT('%s: %s', FORMAT_DATE('%Y-%m-%d', w.created_date), w.collection_c) AS entry
  FROM campaign_members cm
  JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_wishlist_c_filtered` w ON cm.account_id = w.account_c
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_product_2_filtered` p ON w.product_c = p.id
  WHERE DATE(w.created_date) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY) AND CURRENT_DATE()
),


sales_dedup AS (
  SELECT
    DISTINCT
    cm.campaign_id,
    cm.account_id,
    FORMAT('%s: CHF%.0f', FORMAT_DATE('%Y-%m-%d', COALESCE(b.warranty_activation_date_c, b.purchase_date_c)), b.retail_price_chf_c) AS entry
  FROM campaign_members cm
  JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_c_filtered` b ON cm.account_id = b.account_c
  WHERE b.is_watch_c = TRUE
    AND DATE(COALESCE(b.warranty_activation_date_c, b.purchase_date_c)) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY) AND CURRENT_DATE()
)


SELECT
  cb.campaign_id,
  cb.campaign_name,
  cb.subcampaign_id,
  cb.subcampaign_name,
  cb.boutique,
  COALESCE(cb.territory, cb.fallback_territory) AS territory,
  cb.campaign_end_date,


  cm.account_id,
  ANY_VALUE(cm.client_type) AS client_type,
  ANY_VALUE(cm.macro_segment) AS macro_segment,
  COUNT(DISTINCT cm.member_id) AS member_count,


  -- KPIs based on deduplicated sale dates per campaign/account
  COUNT(DISTINCT CASE WHEN DATE_DIFF(COALESCE(b.purchase_date), PARSE_DATE('%Y-%m-%d', cb.campaign_end_date), DAY) BETWEEN 0 AND 30 THEN b.purchase_date END) AS converted_1m,
  COUNT(DISTINCT CASE WHEN DATE_DIFF(COALESCE(b.purchase_date), PARSE_DATE('%Y-%m-%d', cb.campaign_end_date), DAY) BETWEEN 0 AND 90 THEN b.purchase_date END) AS converted_3m,
  COUNT(DISTINCT CASE WHEN DATE_DIFF(COALESCE(b.purchase_date), PARSE_DATE('%Y-%m-%d', cb.campaign_end_date), DAY) BETWEEN 0 AND 180 THEN b.purchase_date END) AS converted_6m,


  SUM(CASE WHEN DATE_DIFF(COALESCE(b.purchase_date), PARSE_DATE('%Y-%m-%d', cb.campaign_end_date), DAY) BETWEEN 0 AND 30 THEN b.retail_price_chf_c ELSE 0 END) AS revenue_1m,
  SUM(CASE WHEN DATE_DIFF(COALESCE(b.purchase_date), PARSE_DATE('%Y-%m-%d', cb.campaign_end_date), DAY) BETWEEN 0 AND 90 THEN b.retail_price_chf_c ELSE 0 END) AS revenue_3m,
  SUM(CASE WHEN DATE_DIFF(COALESCE(b.purchase_date), PARSE_DATE('%Y-%m-%d', cb.campaign_end_date), DAY) BETWEEN 0 AND 180 THEN b.retail_price_chf_c ELSE 0 END) AS revenue_6m,


  COUNT(DISTINCT CASE WHEN DATE_DIFF(w.created_date, PARSE_DATE('%Y-%m-%d', cb.campaign_end_date), DAY) BETWEEN 0 AND 30 THEN w.created_date END) AS wishes_1m,
  COUNT(DISTINCT CASE WHEN DATE_DIFF(w.created_date, PARSE_DATE('%Y-%m-%d', cb.campaign_end_date), DAY) BETWEEN 0 AND 90 THEN w.created_date END) AS wishes_3m,
  COUNT(DISTINCT CASE WHEN DATE_DIFF(w.created_date, PARSE_DATE('%Y-%m-%d', cb.campaign_end_date), DAY) BETWEEN 0 AND 180 THEN w.created_date END) AS wishes_6m,


  -- Deduplicated wishlist entries (joined from wishlist_dedup)
  (
    SELECT STRING_AGG(DISTINCT entry, '\n')
    FROM wishlist_dedup wd
    WHERE wd.account_id = cm.account_id AND wd.campaign_id = cb.campaign_id
  ) AS wishlist_activity,


  -- Deduplicated sales entries (joined from sales_dedup)
  (
    SELECT STRING_AGG(DISTINCT entry, '\n')
    FROM sales_dedup sd
    WHERE sd.account_id = cm.account_id AND sd.campaign_id = cb.campaign_id
  ) AS sales_details


FROM campaign_base cb
JOIN campaign_members cm
  ON cb.campaign_id = cm.campaign_id OR cb.subcampaign_id = cm.campaign_id
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_c_filtered` b
  ON cm.account_id = b.account_c
  AND b.is_watch_c = TRUE
  AND DATE(COALESCE(b.warranty_activation_date_c, b.purchase_date_c)) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY) AND CURRENT_DATE()
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_wishlist_c_filtered` w
  ON cm.account_id = w.account_c
  AND DATE(w.created_date) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY) AND CURRENT_DATE()


GROUP BY
  cb.campaign_id,
  cb.campaign_name,
  cb.subcampaign_id,
  cb.subcampaign_name,
  cb.boutique,
  cb.territory,
  cb.fallback_territory,
  cb.campaign_end_date,
  cm.account_id




