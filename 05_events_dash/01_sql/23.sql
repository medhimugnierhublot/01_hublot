-- Final query with fixed sales_details blocks
WITH campaign_base AS (
  SELECT
    c.id AS campaign_or_subcampaign_id,
    c.name AS campaign_or_subcampaign_name,
    c.parent_id,
    CASE WHEN c.parent_id IS NULL THEN c.id ELSE c.parent_id END AS campaign_id,
    CASE WHEN c.parent_id IS NULL THEN c.name ELSE cp.name END AS campaign_name,
    CASE WHEN c.parent_id IS NULL THEN NULL ELSE c.id END AS subcampaign_id,
    CASE WHEN c.parent_id IS NULL THEN NULL ELSE c.name END AS subcampaign_name,
    c.end_date AS subcampaign_end_date,
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
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
    ON cm.contact_id = ap.person_contact_id
  WHERE NOT cm.is_deleted
    AND ap.id = "0010X00004veSMlQAM"
    AND cm.campaign_id = '701J5000000PhynIAC'
),


sales_attributed AS (
  SELECT
    cm.member_id,
    cb.subcampaign_id,
    cb.subcampaign_end_date,
    COALESCE(b.warranty_activation_date_c, b.purchase_date_c) AS purchase_date,
    b.id AS sale_id,
    b.retail_price_chf_c,
    b.product_reference_c
  FROM campaign_members cm
  JOIN campaign_base cb ON cb.subcampaign_id = cm.campaign_id
  JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_c_filtered` b
    ON cm.account_id = b.account_c
  WHERE b.is_watch_c = TRUE
    AND DATE(COALESCE(b.warranty_activation_date_c, b.purchase_date_c))
        BETWEEN DATE(cb.subcampaign_end_date) AND DATE_ADD(DATE(cb.subcampaign_end_date), INTERVAL 90 DAY)
),


sales_aggregated AS (
  SELECT
    member_id,
    subcampaign_id,
    COUNT(DISTINCT CASE WHEN DATE_DIFF(DATE(purchase_date), DATE(subcampaign_end_date), DAY) BETWEEN 0 AND 30 THEN sale_id END) AS converted_1m,
    COUNT(DISTINCT CASE WHEN DATE_DIFF(DATE(purchase_date), DATE(subcampaign_end_date), DAY) BETWEEN 0 AND 90 THEN sale_id END) AS converted_3m,
    SUM(CASE WHEN DATE_DIFF(DATE(purchase_date), DATE(subcampaign_end_date), DAY) BETWEEN 0 AND 30 THEN retail_price_chf_c ELSE 0 END) AS revenue_1m,
    SUM(CASE WHEN DATE_DIFF(DATE(purchase_date), DATE(subcampaign_end_date), DAY) BETWEEN 0 AND 90 THEN retail_price_chf_c ELSE 0 END) AS revenue_3m,
    COUNT(DISTINCT CASE WHEN DATE_DIFF(DATE(purchase_date), DATE(subcampaign_end_date), DAY) BETWEEN 0 AND 30 THEN sale_id END) > 0 AS customers_1m,
    COUNT(DISTINCT CASE WHEN DATE_DIFF(DATE(purchase_date), DATE(subcampaign_end_date), DAY) BETWEEN 0 AND 90 THEN sale_id END) > 0 AS customers_3m
  FROM sales_attributed
  GROUP BY member_id, subcampaign_id
),


sales_details_1m AS (
  SELECT
    b.account_c,
    cb.subcampaign_id,
    STRING_AGG(
      FORMAT(
        '%s: %s:%s (CHF%s)',
        FORMAT_DATE('%Y-%m-%d', COALESCE(b.warranty_activation_date_c, b.purchase_date_c)),
        INITCAP(prd.collection_c),
        INITCAP(b.product_reference_c),
        FORMAT('%T', CAST(FLOOR(b.retail_price_chf_c) AS INT64))
      ),
      '\n'
    ) AS sales_details_1m
  FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_c_filtered` b
  JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_product_2_filtered` prd
    ON b.product_code_c = prd.product_code
  JOIN campaign_base cb
    ON DATE(COALESCE(b.warranty_activation_date_c, b.purchase_date_c)) BETWEEN DATE(cb.subcampaign_end_date) AND DATE_ADD(DATE(cb.subcampaign_end_date), INTERVAL 30 DAY)
  WHERE b.is_watch_c = TRUE
  GROUP BY b.account_c, cb.subcampaign_id
),


sales_details_3m AS (
  SELECT
    b.account_c,
    cb.subcampaign_id,
    STRING_AGG(
      FORMAT(
        '%s: %s:%s (CHF%s)',
        FORMAT_DATE('%Y-%m-%d', COALESCE(b.warranty_activation_date_c, b.purchase_date_c)),
        INITCAP(prd.collection_c),
        INITCAP(b.product_reference_c),
        FORMAT('%T', CAST(FLOOR(b.retail_price_chf_c) AS INT64))
      ),
      '\n'
    ) AS sales_details_3m
  FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_c_filtered` b
  JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_product_2_filtered` prd
    ON b.product_code_c = prd.product_code
  JOIN campaign_base cb
    ON DATE(COALESCE(b.warranty_activation_date_c, b.purchase_date_c)) BETWEEN DATE(cb.subcampaign_end_date) AND DATE_ADD(DATE(cb.subcampaign_end_date), INTERVAL 90 DAY)
  WHERE b.is_watch_c = TRUE
  GROUP BY b.account_c, cb.subcampaign_id
)


-- Final SELECT
SELECT
  cb.campaign_id,
  cb.campaign_name,
  cb.subcampaign_id,
  cb.subcampaign_name,
  cb.boutique,
  COALESCE(cb.territory, cb.fallback_territory) AS territory,
  DATE(cb.subcampaign_end_date) AS campaign_end_date,
  cm.member_id,
  cm.account_id,
  cm.contact_id,
  cm.client_type,
  cm.macro_segment,
  cm.member_status,
  cm.has_responded,
  cm.participated_c,
  IF(cm.client_type = 'Hublot Client', 1, 0) AS is_client,
  IF(cm.client_type = 'Prospect', 1, 0) AS is_prospect,
  IF(cm.macro_segment IS NOT NULL, 1, 0) AS has_macro_segment,
  IF(cm.member_status = 'Clicked', 1, 0) AS is_clicked,
  IF(cm.member_status = 'Hard Bounce', 1, 0) AS is_hard_bounce,
  IF(cm.member_status = 'Invited', 1, 0) AS is_invited,
  IF(cm.member_status = 'Opened', 1, 0) AS is_opened,
  IF(cm.member_status = 'Refused', 1, 0) AS is_refused,
  IF(cm.member_status = 'Responded', 1, 0) AS is_responded_status,
  IF(cm.member_status = 'Sent', 1, 0) AS is_sent,
  IF(cm.member_status = 'Soft Bounce', 1, 0) AS is_soft_bounce,
  IF(cm.has_responded, 1, 0) AS responded,
  IF(cm.participated_c = TRUE, 1, 0) AS has_participated,
  IFNULL(s.converted_1m, 0) AS converted_1m,
  IFNULL(s.converted_3m, 0) AS converted_3m,
  IFNULL(s.revenue_1m, 0) AS revenue_1m,
  IFNULL(s.revenue_3m, 0) AS revenue_3m,
  IF(s.customers_1m, 1, 0) AS customers_1m,
  IF(s.customers_3m, 1, 0) AS customers_3m,
  sd1m.sales_details_1m,
  sd3m.sales_details_3m
FROM campaign_members cm
JOIN campaign_base cb ON cb.subcampaign_id = cm.campaign_id
LEFT JOIN sales_aggregated s ON cm.member_id = s.member_id AND cb.subcampaign_id = s.subcampaign_id
LEFT JOIN sales_details_1m sd1m ON cm.account_id = sd1m.account_c AND cb.subcampaign_id = sd1m.subcampaign_id
LEFT JOIN sales_details_3m sd3m ON cm.account_id = sd3m.account_c AND cb.subcampaign_id = sd3m.subcampaign_id






