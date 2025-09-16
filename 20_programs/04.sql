WITH
  list_base AS (
    SELECT DISTINCT
      bout.name AS bout_name,
      bout.market AS bout_market,
      cl.id AS list_id,
      cl.business_entity_c AS business_entity,
      cl.name AS list_name,
      DATE(cl.created_date) AS created_date
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_client_list_joined` cl
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_boutiques` bout
      ON cl.business_entity_c = bout.id
    WHERE cl.list_type_c = 'store'
  ),

  client_list_members AS (
    SELECT
      cl.client_c,
      cl.client_list_c,
      ap.id AS account_id,
      ap.macro_segment,
      ap.status_pc,
      ap.type AS client_type
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_client_list_joined` cl
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
      ON cl.client_c = ap.id
    WHERE NOT cl.is_deleted AND cl.list_type_c = 'store'
  ),

  belonging AS (
    SELECT
      DISTINCT
      account_c,
      id AS sale_id,
      DATE(COALESCE(warranty_activation_date_c, purchase_date_c)) AS purchase_date,
      retail_price_chf_c,
      product_reference_c
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_c_filtered`
    WHERE is_watch_c = TRUE
  ),

  wishlist AS (
    SELECT
      wish.account_c,
      wish.id AS wishlist_id,
      DATE(wish.created_date) AS created_date,
      prd.collection_c
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_wishlist_c_filtered` wish
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_product_2_filtered` prd
      ON wish.product_c = prd.id
  ),


  outreaches AS (
    SELECT
      tsks.id AS outreach_id,
      ap.id AS account_id,
      DATE(tsks.created_date) AS created_date
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_task_filtered` tsks
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_contact_filtered` ctcs
      ON tsks.who_id = ctcs.id
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
      ON ctcs.id = ap.person_contact_id
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_user_with_markets` uswm
      ON tsks.owner_id = uswm.id
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_contact_relation_filtered` accrf
      ON ap.id = accrf.account_id
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_boutiques` bout
      ON accrf.account_id = bout.id
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_user_role_filtered` role
      ON uswm.user_role_id = role.id
    WHERE
      tsks.channel_c IN ('Call', 'Email', 'Kakao', 'Line', 'SMS', 'WeChat', 'WhatsApp') AND
      (LOWER(tsks.description) NOT LIKE '%bug%' OR LOWER(tsks.description) IS NULL) AND
      (LOWER(tsks.description) NOT LIKE '%internal%' OR LOWER(tsks.description) IS NULL) AND
      (
          LOWER(bout.name) NOT LIKE '%eboutique%'
          OR LOWER(bout.name) IS NULL
          OR bout.id = '0010X00004snyN3QAI'
      )
      AND (
        LOWER(role.name) LIKE '%boutique manager%' OR
        LOWER(role.name) LIKE '%sales associate%' OR
        uswm.id IN ('0050X000007hoVsQAI', '0056700000CeROxAAN', '0056700000EOFDeAAP')
      ) AND
      (bout.market != 'Greater China' OR bout.market IS NULL) AND
      (bout.status_c != 'Inactive' OR bout.status_c IS NULL)
  )

SELECT
  cl.created_date,
  CASE
    WHEN LOWER(cl.list_name) LIKE '%w&w prospects follow up%' THEN "W&W Prospects Follow up"
    ELSE cl.list_name
    END AS list_name,
  cl.bout_name,
  cl.bout_market,
  clm.macro_segment,
  clm.status_pc,
  COUNT(DISTINCT clm.account_id) AS nb_members,

  -- Outreaches
  COUNT(DISTINCT CASE WHEN DATE_DIFF(o.created_date, cl.created_date, DAY) BETWEEN 0 AND 30 THEN o.outreach_id END) AS outreaches_1m,
  COUNT(DISTINCT CASE WHEN DATE_DIFF(o.created_date, cl.created_date, DAY) BETWEEN 0 AND 90 THEN o.outreach_id END) AS outreaches_3m,

  -- ✅ Accounts reached out
  COUNT(DISTINCT CASE WHEN DATE_DIFF(o.created_date, cl.created_date, DAY) BETWEEN 0 AND 30 THEN clm.account_id END) AS outreached_accounts_1m,
  COUNT(DISTINCT CASE WHEN DATE_DIFF(o.created_date, cl.created_date, DAY) BETWEEN 0 AND 90 THEN clm.account_id END) AS outreached_accounts_3m,


  -- Wishes
  COUNT(DISTINCT CASE WHEN DATE_DIFF(w.created_date, cl.created_date, DAY) BETWEEN 0 AND 30 THEN w.wishlist_id END) AS wishes_1m,
  COUNT(DISTINCT CASE WHEN DATE_DIFF(w.created_date, cl.created_date, DAY) BETWEEN 0 AND 90 THEN w.wishlist_id END) AS wishes_3m,


  -- Customers
  COUNT(DISTINCT CASE WHEN DATE_DIFF(b.purchase_date, cl.created_date, DAY) BETWEEN 0 AND 30 THEN b.account_c END) AS customers_1m,
  COUNT(DISTINCT CASE WHEN DATE_DIFF(b.purchase_date, cl.created_date, DAY) BETWEEN 0 AND 90 THEN b.account_c END) AS customers_3m,


  -- Sales
  COUNT(DISTINCT CASE WHEN DATE_DIFF(b.purchase_date, cl.created_date, DAY) BETWEEN 0 AND 30 THEN b.sale_id END) AS sales_1m,
  COUNT(DISTINCT CASE WHEN DATE_DIFF(b.purchase_date, cl.created_date, DAY) BETWEEN 0 AND 90 THEN b.sale_id END) AS sales_3m,


  -- Revenue
  SUM(CASE WHEN DATE_DIFF(b.purchase_date, cl.created_date, DAY) BETWEEN 0 AND 30 THEN b.retail_price_chf_c ELSE 0 END) AS revenue_1m,
  SUM(CASE WHEN DATE_DIFF(b.purchase_date, cl.created_date, DAY) BETWEEN 0 AND 90 THEN b.retail_price_chf_c ELSE 0 END) AS revenue_3m


FROM
  client_list_members clm
LEFT JOIN list_base cl ON clm.client_list_c = cl.list_id
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_salesforce.account` bout ON bout.id = cl.business_entity
LEFT JOIN outreaches o ON 
    o.account_id = clm.account_id AND
    DATE(o.created_date) BETWEEN DATE(cl.created_date) AND DATE_ADD(DATE(cl.created_date), INTERVAL 90 DAY)
LEFT JOIN wishlist w ON 
    w.account_c = clm.account_id AND 
    DATE(w.created_date) BETWEEN DATE(cl.created_date) AND DATE_ADD(DATE(cl.created_date), INTERVAL 90 DAY)
LEFT JOIN belonging b ON 
    b.account_c = clm.account_id AND 
    DATE(b.purchase_date) BETWEEN DATE(cl.created_date) AND DATE_ADD(DATE(cl.created_date), INTERVAL 90 DAY)
WHERE
    (
    LOWER(cl.list_name) LIKE '%hpc - high potential clients%'
    OR LOWER(cl.list_name) LIKE '%w&w prospects follow up%'
    OR LOWER(cl.list_name) LIKE '%local prospects to convert%'
    )

GROUP BY 1,2,3,4,5,6