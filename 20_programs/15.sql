WITH


list_base AS (
  SELECT DISTINCT
    bout.name AS bout_name,
    bout.market AS bout_market,
    bout.billing_country,
    cl.id AS list_id,
    cl.business_entity_c AS business_entity,
    CASE
      WHEN LOWER(cl.name) LIKE '%w&w prospects follow up%' THEN "W&W Prospects Follow up"
      ELSE cl.name
    END AS list_name,
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
    ap.id AS account_id,
    ap.macro_segment,
    ap.status_pc,
    ap.type AS client_type
  FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_client_list_joined` cl
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
    ON cl.client_c = ap.id
  WHERE NOT cl.is_deleted AND cl.list_type_c = 'store'
),


sales_attributed AS (
  SELECT
    cm.client_c,
    cb.list_id,
    cb.created_date,
    COALESCE(b.warranty_activation_date_c, b.purchase_date_c) AS purchase_date,
    b.id AS sale_id,
    b.retail_price_chf_c,
    b.product_reference_c,
    b.product_code_c,
    prd.collection_c
  FROM client_list_members cm
  JOIN list_base cb ON cb.list_id = cm.client_list_c
  JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_c_filtered` b
    ON cm.account_id = b.account_c
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_product_2_filtered` prd
    ON b.product_code_c = prd.id
  WHERE b.is_watch_c = TRUE
    AND DATE(COALESCE(b.warranty_activation_date_c, b.purchase_date_c))
      BETWEEN DATE(cb.created_date) AND DATE_ADD(DATE(cb.created_date), INTERVAL 90 DAY)
),


sales_aggregated AS (
  SELECT
    client_c,
    list_id,
    COUNT(DISTINCT CASE WHEN DATE_DIFF(DATE(purchase_date), DATE(created_date), DAY) BETWEEN 0 AND 30 THEN sale_id END) AS converted_1m,
    COUNT(DISTINCT CASE WHEN DATE_DIFF(DATE(purchase_date), DATE(created_date), DAY) BETWEEN 0 AND 90 THEN sale_id END) AS converted_3m,
    SUM(CASE WHEN DATE_DIFF(DATE(purchase_date), DATE(created_date), DAY) BETWEEN 0 AND 30 THEN retail_price_chf_c ELSE 0 END) AS revenue_1m,
    SUM(CASE WHEN DATE_DIFF(DATE(purchase_date), DATE(created_date), DAY) BETWEEN 0 AND 90 THEN retail_price_chf_c ELSE 0 END) AS revenue_3m,
    COUNT(DISTINCT CASE WHEN DATE_DIFF(DATE(purchase_date), DATE(created_date), DAY) BETWEEN 0 AND 30 THEN sale_id END) > 0 AS customers_1m,
    COUNT(DISTINCT CASE WHEN DATE_DIFF(DATE(purchase_date), DATE(created_date), DAY) BETWEEN 0 AND 90 THEN sale_id END) > 0 AS customers_3m
  FROM sales_attributed
  GROUP BY client_c, list_id
),


wishes_aggregated AS (
  SELECT
    cm.client_c,
    cb.list_id,
    COUNTIF(DATE(w.created_date) BETWEEN cb.created_date AND DATE_ADD(cb.created_date, INTERVAL 30 DAY)) AS wishes_1m,
    COUNTIF(DATE(w.created_date) BETWEEN cb.created_date AND DATE_ADD(cb.created_date, INTERVAL 90 DAY)) AS wishes_3m
  FROM client_list_members cm
  JOIN list_base cb ON cb.list_id = cm.client_list_c
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_wishlist_c_filtered` w
    ON cm.account_id = w.account_c
  GROUP BY cm.client_c, cb.list_id
),


outreaches_attributed AS (
  SELECT
    tsks.id AS outreach_id,
    ap.id AS account_id,
    tsks.channel_c,
    DATE(tsks.created_date) AS created_date
  FROM client_list_members cm
  LEFT JOIN list_base cb ON cb.list_id = cm.client_list_c
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_task_filtered` tsks ON cm.account_id = tsks.account_id
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_contact_filtered` ctcs ON tsks.who_id = ctcs.id
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap ON ctcs.id = ap.person_contact_id
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_user_with_markets` uswm ON tsks.owner_id = uswm.id
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_contact_relation_filtered` accrf ON ap.id = accrf.account_id
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_boutiques` bout ON accrf.account_id = bout.id
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_user_role_filtered` role ON uswm.user_role_id = role.id
  WHERE
    DATE(tsks.created_date) BETWEEN DATE(cb.created_date) AND DATE_ADD(DATE(cb.created_date), INTERVAL 90 DAY)
    AND tsks.channel_c IN ('Call', 'Email', 'Kakao', 'Line', 'SMS', 'WeChat', 'WhatsApp')
    AND (LOWER(tsks.description) NOT LIKE '%bug%' OR tsks.description IS NULL)
    AND (LOWER(tsks.description) NOT LIKE '%internal%' OR tsks.description IS NULL)
    AND (LOWER(bout.name) NOT LIKE '%eboutique%' OR bout.name IS NULL OR bout.id = '0010X00004snyN3QAI')
    AND (
      LOWER(role.name) LIKE '%boutique manager%' OR
      LOWER(role.name) LIKE '%sales associate%' OR
      uswm.id IN ('0050X000007hoVsQAI', '0056700000CeROxAAN', '0056700000EOFDeAAP')
    )
    AND (bout.market != 'Greater China' OR bout.market IS NULL)
    AND (bout.status_c != 'Inactive' OR bout.status_c IS NULL)
),


outreaches_aggregated AS (
  SELECT
    cm.client_c,
    cb.list_id,
    COUNTIF(DATE(tsks.created_date) BETWEEN cb.created_date AND DATE_ADD(cb.created_date, INTERVAL 30 DAY)) AS outreaches_1m,
    COUNTIF(DATE(tsks.created_date) BETWEEN cb.created_date AND DATE_ADD(cb.created_date, INTERVAL 90 DAY)) AS outreaches_3m,
    COUNT(DISTINCT CASE WHEN DATE(tsks.created_date) BETWEEN cb.created_date AND DATE_ADD(cb.created_date, INTERVAL 30 DAY) THEN cm.client_c END) > 0 AS outreached_1m,
    COUNT(DISTINCT CASE WHEN DATE(tsks.created_date) BETWEEN cb.created_date AND DATE_ADD(cb.created_date, INTERVAL 90 DAY) THEN cm.client_c END) > 0 AS outreached_3m
  FROM outreaches_attributed tsks
  JOIN client_list_members cm ON cm.account_id = tsks.account_id
  JOIN list_base cb ON cb.list_id = cm.client_list_c
  GROUP BY cm.client_c, cb.list_id
),


offline_actions_attributed AS (
  SELECT
    tsks.id AS action_id,
    ap.id AS account_id,
    DATE(tsks.activity_date) AS activity_date
  FROM client_list_members cm
  LEFT JOIN list_base cb ON cb.list_id = cm.client_list_c
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_task_filtered` tsks ON cm.account_id = tsks.account_id
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_contact_filtered` ctcs ON tsks.who_id = ctcs.id
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap ON ctcs.id = ap.person_contact_id
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_boutiques` ab ON tsks.account_id = ab.id
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_user_with_markets` user ON tsks.owner_id = user.id
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_user_role_filtered` role ON user.user_role_id = role.id
  WHERE
    tsks.channel_c = 'Action'
    AND tsks.type_c IS NOT NULL
    AND DATE(tsks.activity_date) BETWEEN DATE(cb.created_date) AND DATE_ADD(cb.created_date, INTERVAL 90 DAY)
    AND (
      STRPOS(role.name, 'Boutique Manager') > 0 OR
      STRPOS(role.name, 'Sales Associate') > 0
    )
),


offline_actions_aggregated AS (
  SELECT
    cm.client_c,
    cb.list_id,
    COUNTIF(DATE(tsks.activity_date) BETWEEN cb.created_date AND DATE_ADD(cb.created_date, INTERVAL 30 DAY)) AS offline_actions_1m,
    COUNTIF(DATE(tsks.activity_date) BETWEEN cb.created_date AND DATE_ADD(cb.created_date, INTERVAL 90 DAY)) AS offline_actions_3m,
    COUNT(DISTINCT CASE WHEN DATE(tsks.activity_date) BETWEEN cb.created_date AND DATE_ADD(cb.created_date, INTERVAL 30 DAY) THEN cm.client_c END) > 0 AS offline_acted_1m,
    COUNT(DISTINCT CASE WHEN DATE(tsks.activity_date) BETWEEN cb.created_date AND DATE_ADD(cb.created_date, INTERVAL 90 DAY) THEN cm.client_c END) > 0 AS offline_acted_3m
  FROM offline_actions_attributed tsks
  JOIN client_list_members cm ON cm.account_id = tsks.account_id
  JOIN list_base cb ON cb.list_id = cm.client_list_c
  GROUP BY cm.client_c, cb.list_id
),


upgrades_detailed AS (
  SELECT
    cm.client_c,
    cm.account_id,
    cb.list_id,
    ah_1m.life_time_segment AS segment_1m,
    ah_3m.life_time_segment AS segment_3m
  FROM client_list_members cm
  JOIN list_base cb ON cb.list_id = cm.client_list_c


  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah_start
    ON cm.account_id = ah_start.account_id
    AND DATE(ah_start.photo_date) = DATE_TRUNC(cb.created_date, MONTH)


  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah_1m
    ON cm.account_id = ah_1m.account_id
    AND DATE(ah_1m.photo_date) = DATE_TRUNC(DATE_ADD(cb.created_date, INTERVAL 1 MONTH), MONTH)


  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah_3m
    ON cm.account_id = ah_3m.account_id
    AND DATE(ah_3m.photo_date) = DATE_TRUNC(DATE_ADD(cb.created_date, INTERVAL 3 MONTH), MONTH)


  WHERE ah_start.life_time_segment = 'One Timer'
)







SELECT
  cb.list_id,
  cb.list_name,
  cb.created_date,
  cb.bout_name,
  cb.bout_market,
  cb.billing_country,
  clm.client_c,
  clm.account_id,
  clm.client_type,
  clm.status_pc,
  clm.macro_segment,
  IFNULL(sa.converted_1m, 0) AS converted_1m,
  IFNULL(sa.converted_3m, 0) AS converted_3m,
  IFNULL(sa.revenue_1m, 0) AS revenue_1m,
  IFNULL(sa.revenue_3m, 0) AS revenue_3m,
  IFNULL(wa.wishes_1m, 0) AS wishes_1m,
  IFNULL(wa.wishes_3m, 0) AS wishes_3m,
  IF(sa.customers_1m, 1, 0) AS customers_1m,
  IF(sa.customers_3m, 1, 0) AS customers_3m,
  IFNULL(oa.outreaches_1m, 0) AS outreaches_1m,
  IFNULL(oa.outreaches_3m, 0) AS outreaches_3m,
  IF(oa.outreached_1m, 1, 0) AS outreached_1m,
  IF(oa.outreached_3m, 1, 0) AS outreached_3m,
  IFNULL(ofa.offline_actions_1m, 0) AS offline_actions_1m,
  IFNULL(ofa.offline_actions_3m, 0) AS offline_actions_3m,
  IF(ofa.offline_acted_1m, 1, 0) AS offline_acted_1m,
  IF(ofa.offline_acted_3m, 1, 0) AS offline_acted_3m,
  IF(ud.segment_1m IS NOT NULL AND ud.segment_1m != 'One Timer', 1, 0) AS upgraded_1m,
  IF(ud.segment_3m IS NOT NULL AND ud.segment_3m != 'One Timer', 1, 0) AS upgraded_3m,


IF(
  ud.segment_1m IS NOT NULL AND ud.segment_1m != 'One Timer',
  FORMAT('one_timer_to_%s', REPLACE(LOWER(ud.segment_1m), ' ', '_')),
  NULL
) AS upgraded_1m_details,




IF(
  ud.segment_3m IS NOT NULL AND ud.segment_3m != 'One Timer',
  FORMAT('one_timer_to_%s', REPLACE(LOWER(ud.segment_3m), ' ', '_')),
  NULL
) AS upgraded_3m_details,




FROM client_list_members clm
JOIN list_base cb ON cb.list_id = clm.client_list_c
LEFT JOIN sales_aggregated sa ON sa.client_c = clm.client_c AND sa.list_id = cb.list_id
LEFT JOIN wishes_aggregated wa ON wa.client_c = clm.client_c AND wa.list_id = cb.list_id
LEFT JOIN outreaches_aggregated oa ON oa.client_c = clm.client_c AND oa.list_id = cb.list_id
LEFT JOIN offline_actions_aggregated ofa ON ofa.client_c = clm.client_c AND ofa.list_id = cb.list_id
LEFT JOIN upgrades_detailed ud ON ud.client_c = clm.client_c AND ud.list_id = cb.list_id




