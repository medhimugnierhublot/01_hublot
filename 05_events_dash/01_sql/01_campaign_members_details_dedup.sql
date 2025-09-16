WITH campaign_base AS (
  SELECT
    c.id AS campaign_or_subcampaign_id,
    c.name AS campaign_or_subcampaign_name,
    c.parent_id,
    CASE WHEN c.parent_id IS NULL THEN c.id ELSE c.parent_id END AS campaign_id,
    CASE WHEN c.parent_id IS NULL THEN c.name ELSE cp.name END AS campaign_name,
    CASE WHEN c.parent_id IS NULL THEN NULL ELSE c.id END AS subcampaign_id,
    CASE WHEN c.parent_id IS NULL THEN NULL ELSE c.name END AS subcampaign_name,
    c.end_date,
    a.name AS boutique,
    c.territory_c AS territory,
    ab.territory_c AS fallback_territory
  FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_salesforce.campaign` c
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_salesforce.campaign` cp
    ON c.parent_id = cp.id
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_salesforce.account` a
    ON a.id = c.boutique_c
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_boutiques` ab
    ON ab.id = c.boutique_c
  WHERE 
    c.end_date >= '2021-01-01' AND 
    c.id IS NOT NULL
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
  WHERE 
    NOT cm.is_deleted
),

-- DEDUPLICATED Belonging (one purchase per client)
belonging AS (
  SELECT *
  FROM (
    SELECT
      account_c,
      COALESCE(warranty_activation_date_c, purchase_date_c) AS purchase_date,
      retail_price_chf_c,
      ROW_NUMBER() OVER (PARTITION BY account_c ORDER BY COALESCE(warranty_activation_date_c, purchase_date_c)) AS rn
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_c_filtered`
    WHERE is_watch_c = TRUE
  )
  WHERE rn = 1
),

campaign_status AS (
  SELECT
    cms.campaign_id,
    cms.label AS status_label
  FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_salesforce.campaign_member_status` cms
),

-- DEDUPLICATED Wishlist (one wishlist per client)
wishlist AS (
  SELECT 
  *
  FROM
  (
    SELECT
      wish.account_c,
      wish.created_date,
      -- wish.product_c,
      prd.collection_c,
      prd.name,
      ROW_NUMBER() OVER (PARTITION BY wish.account_c ORDER BY wish.created_date) AS rn
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_wishlist_c_filtered` wish
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_product_2_filtered` prd ON wish.product_c = prd.id
  )
  WHERE rn = 1
)

SELECT
  cb.campaign_id,
  cb.campaign_name,
  cb.subcampaign_id,
  cb.subcampaign_name,
  cb.boutique,
  COALESCE(cb.territory, cb.fallback_territory) AS territory,
  DATE(cb.end_date) AS campaign_end_date,

  cm.member_id,
  cm.account_id,
  cm.client_type,
  cm.macro_segment,
  cm.member_status,
  cm.has_responded,
  cm.participated_c,

  s.purchase_date,
  s.retail_price_chf_c,
  DATE_DIFF(DATE(s.purchase_date), DATE(cb.end_date), DAY) AS days_after_campaign,

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

  IF(DATE_DIFF(DATE(s.purchase_date), DATE(cb.end_date), DAY) <= 30, 1, 0) AS converted_1m,
  IF(DATE_DIFF(DATE(s.purchase_date), DATE(cb.end_date), DAY) <= 90, 1, 0) AS converted_3m,
  IF(DATE_DIFF(DATE(s.purchase_date), DATE(cb.end_date), DAY) <= 180, 1, 0) AS converted_6m,

  IF(DATE_DIFF(DATE(s.purchase_date), DATE(cb.end_date), DAY) <= 30, s.retail_price_chf_c, 0) AS revenue_1m,
  IF(DATE_DIFF(DATE(s.purchase_date), DATE(cb.end_date), DAY) <= 90, s.retail_price_chf_c, 0) AS revenue_3m,
  IF(DATE_DIFF(DATE(s.purchase_date), DATE(cb.end_date), DAY) <= 180, s.retail_price_chf_c, 0) AS revenue_6m,

  IF(cm.participated_c = TRUE AND DATE_DIFF(DATE(s.purchase_date), DATE(cb.end_date), DAY) <= 30, 1, 0) AS converted_1m_has_participated,
  IF(cm.participated_c = TRUE AND DATE_DIFF(DATE(s.purchase_date), DATE(cb.end_date), DAY) <= 90, 1, 0) AS converted_3m_has_participated,
  IF(cm.participated_c = TRUE AND DATE_DIFF(DATE(s.purchase_date), DATE(cb.end_date), DAY) <= 180, 1, 0) AS converted_6m_has_participated,

  IF(cm.participated_c = TRUE AND DATE_DIFF(DATE(s.purchase_date), DATE(cb.end_date), DAY) <= 30, s.retail_price_chf_c, 0) AS revenue_1m_has_participated,
  IF(cm.participated_c = TRUE AND DATE_DIFF(DATE(s.purchase_date), DATE(cb.end_date), DAY) <= 90, s.retail_price_chf_c, 0) AS revenue_3m_has_participated,
  IF(cm.participated_c = TRUE AND DATE_DIFF(DATE(s.purchase_date), DATE(cb.end_date), DAY) <= 180, s.retail_price_chf_c, 0) AS revenue_6m_has_participated,

  DATE(w.created_date) AS wishlist_created_date,

  IF(DATE_DIFF(DATE(w.created_date), DATE(cb.end_date), DAY) <= 30, 1, 0) AS wishes_1m,
  IF(DATE_DIFF(DATE(w.created_date), DATE(cb.end_date), DAY) <= 90, 1, 0) AS wishes_3m,
  IF(DATE_DIFF(DATE(w.created_date), DATE(cb.end_date), DAY) <= 180, 1, 0) AS wishes_6m,

  IF(cm.participated_c = TRUE AND DATE_DIFF(DATE(w.created_date), DATE(cb.end_date), DAY) <= 30, 1, 0) AS wishes_1m_has_participated,
  IF(cm.participated_c = TRUE AND DATE_DIFF(DATE(w.created_date), DATE(cb.end_date), DAY) <= 90, 1, 0) AS wishes_3m_has_participated,
  IF(cm.participated_c = TRUE AND DATE_DIFF(DATE(w.created_date), DATE(cb.end_date), DAY) <= 180, 1, 0) AS wishes_6m_has_participated

FROM campaign_base cb
LEFT JOIN campaign_members cm
  ON cb.subcampaign_id = cm.campaign_id OR cb.campaign_id = cm.campaign_id
LEFT JOIN campaign_status cs
  ON cm.campaign_id = cs.campaign_id
  AND cm.member_status = cs.status_label
LEFT JOIN belonging s
  ON cm.account_id = s.account_c
  AND DATE(s.purchase_date) BETWEEN DATE(cb.end_date) AND DATE_ADD(DATE(cb.end_date), INTERVAL 6 MONTH)
LEFT JOIN wishlist w
  ON cm.account_id = w.account_c
  AND DATE(w.created_date) BETWEEN DATE(cb.end_date) AND DATE_ADD(DATE(cb.end_date), INTERVAL 6 MONTH)