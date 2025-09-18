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


/* Sales attributed (raw) */
sales_attributed AS (
  SELECT
    cm.client_c,
    cb.list_id,
    cb.created_date,
    DATE(COALESCE(b.warranty_activation_date_c, b.purchase_date_c)) AS purchase_date,
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


/* Outreaches attributed */
outreaches_attributed AS (
  SELECT
    cm.client_c,
    cb.list_id,
    ap.id AS account_id,
    tsks.channel_c,
    DATE(tsks.created_date) AS created_date
  FROM client_list_members cm
  JOIN list_base cb ON cb.list_id = cm.client_list_c
  JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
    ON cm.client_c = ap.id
  JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_task_filtered` tsks
    ON ap.id = tsks.account_id
  WHERE DATE(tsks.created_date) BETWEEN cb.created_date AND DATE_ADD(cb.created_date, INTERVAL 90 DAY)
    AND tsks.channel_c IN ('Call','Email','Kakao','Line','SMS','WeChat','WhatsApp')
),


/* Offline actions attributed */
offline_actions_attributed AS (
  SELECT
    cm.client_c,
    cb.list_id,
    ap.id AS account_id,
    DATE(tsks.activity_date) AS activity_date,
    tsks.type
  FROM client_list_members cm
  JOIN list_base cb ON cb.list_id = cm.client_list_c
  JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
    ON cm.client_c = ap.id
  JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_task_filtered` tsks
    ON ap.id = tsks.account_id
  WHERE
    tsks.type IS NOT NULL AND
    tsks.record_type_id = "01267000000V6gLAAS" AND -- this identifies offline actions
    DATE(tsks.activity_date) BETWEEN cb.created_date AND DATE_ADD(cb.created_date, INTERVAL 90 DAY)
),


/* First touch (outreach OR offline) */
touch_events AS (
  SELECT client_c, list_id, created_date AS event_date
  FROM outreaches_attributed
  UNION ALL
  SELECT client_c, list_id, activity_date AS event_date
  FROM offline_actions_attributed
),


first_touch AS (
  SELECT
    te.client_c,
    te.list_id,
    MIN(te.event_date) AS first_touch_3m
  FROM touch_events te
  GROUP BY te.client_c, te.list_id
),


/* Sales aggregated AFTER first touch (3m only) */
sales_aggregated AS (
  SELECT
    sa.client_c,
    sa.list_id,
    COUNT(DISTINCT CASE
      WHEN sa.purchase_date BETWEEN lb.created_date AND DATE_ADD(lb.created_date, INTERVAL 90 DAY)
           AND ft.first_touch_3m IS NOT NULL
           AND sa.purchase_date >= ft.first_touch_3m
      THEN sa.sale_id END) AS converted_3m,
    SUM(CASE
      WHEN sa.purchase_date BETWEEN lb.created_date AND DATE_ADD(lb.created_date, INTERVAL 90 DAY)
           AND ft.first_touch_3m IS NOT NULL
           AND sa.purchase_date >= ft.first_touch_3m
      THEN sa.retail_price_chf_c ELSE 0 END) AS revenue_3m
  FROM sales_attributed sa
  JOIN list_base lb USING(list_id)
  LEFT JOIN first_touch ft ON sa.client_c=ft.client_c AND sa.list_id=ft.list_id
  GROUP BY sa.client_c, sa.list_id
),


/* ---- DETAIL STRINGS (3m only) ---- */
sales_details_3m AS (
  SELECT
    client_c, list_id,
    STRING_AGG(
      FORMAT('%s: CHF%.0f (%s, %s)',
        FORMAT_DATE('%Y-%m-%d', purchase_date),
        retail_price_chf_c,
        INITCAP(collection_c),
        INITCAP(product_reference_c)
      ),
      '\n' ORDER BY purchase_date ASC
    ) AS sales_details_3m
  FROM sales_attributed
  GROUP BY client_c, list_id
),


wishlist_details_3m AS (
  SELECT
    wish.account_c AS client_c, cb.list_id,
    STRING_AGG(
      FORMAT('%s: %s, %s',
        FORMAT_DATE('%Y-%m-%d', DATE(wish.created_date)),
        INITCAP(prd.collection_c),
        INITCAP(prd.product_code)
      ),
      '\n' ORDER BY DATE(wish.created_date) ASC
    ) AS wishlist_details_3m
  FROM list_base cb
  JOIN client_list_members cm ON cm.client_list_c=cb.list_id
  JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_wishlist_c_filtered` wish
    ON cm.account_id = wish.account_c
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_product_2_filtered` prd
    ON wish.product_c = prd.id
  WHERE DATE(wish.created_date) BETWEEN cb.created_date AND DATE_ADD(cb.created_date, INTERVAL 3 MONTH)
  GROUP BY wish.account_c, cb.list_id
),


outreaches_details_3m AS (
  SELECT
    client_c, list_id,
    STRING_AGG(
      FORMAT('%s (%s)',
        FORMAT_DATE('%Y-%m-%d', created_date),
        channel_c
      ),
      '\n' ORDER BY created_date ASC
    ) AS outreaches_details_3m
  FROM outreaches_attributed
  GROUP BY client_c, list_id
),


offline_actions_details_3m AS (
  SELECT
    client_c,
    list_id,
    STRING_AGG(
      FORMAT('%s (%s)',
        FORMAT_DATE('%Y-%m-%d', activity_date),
        LOWER(type)
      ),
      '\n' ORDER BY activity_date ASC
    ) AS offline_actions_details_3m
  FROM offline_actions_attributed
  GROUP BY client_c, list_id
),


/* Upgrades (3m only, inferred) */
upgrades_detailed AS (
  SELECT
    cm.client_c,
    cm.account_id,
    cb.list_id,
    ah_start.life_time_segment AS segment_start,
    ah_3m.life_time_segment AS segment_3m
  FROM client_list_members cm
  JOIN list_base cb ON cb.list_id = cm.client_list_c
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah_start
    ON cm.account_id = ah_start.account_id
    AND DATE(ah_start.photo_date) = DATE_TRUNC(cb.created_date, MONTH)
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah_3m
    ON cm.account_id = ah_3m.account_id
    AND DATE(ah_3m.photo_date) = DATE_TRUNC(DATE_ADD(cb.created_date, INTERVAL 3 MONTH), MONTH)
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


  IFNULL(sa.converted_3m,0) AS converted_3m,
  IFNULL(sa.revenue_3m,0) AS revenue_3m,


  sd.sales_details_3m,
  wd.wishlist_details_3m,
  od.outreaches_details_3m,
  ofd.offline_actions_details_3m,


CASE
    WHEN ud.segment_3m IS NULL THEN 0
    WHEN ud.segment_start = 'Prospect' AND ud.segment_3m != 'Prospect' THEN 1
    WHEN ud.segment_start = 'One Timer' AND ud.segment_3m != 'One Timer' THEN 1
    WHEN ud.segment_start = 'Loyal' AND ud.segment_3m IN ('VIC','VVIC') THEN 1
    WHEN ud.segment_start = 'VIC' AND ud.segment_3m = 'VVIC' THEN 1
    ELSE 0
END AS upgraded_3m,

CASE
  WHEN ud.segment_3m IS NULL THEN NULL
  WHEN ud.segment_start = 'Prospect' AND ud.segment_3m != 'Prospect'
    THEN FORMAT('prospect_to_%s', LOWER(REPLACE(ud.segment_3m, ' ', '_')))
  WHEN ud.segment_start = 'One Timer' AND ud.segment_3m != 'One Timer'
    THEN FORMAT('one_timer_to_%s', LOWER(REPLACE(ud.segment_3m, ' ', '_')))
  WHEN ud.segment_start = 'Loyal' AND ud.segment_3m IN ('VIC','VVIC')
    THEN FORMAT('loyal_to_%s', LOWER(REPLACE(ud.segment_3m, ' ', '_')))
  WHEN ud.segment_start = 'VIC' AND ud.segment_3m = 'VVIC'
    THEN 'vic_to_vvic'
  ELSE NULL
END AS upgraded_3m_details


FROM client_list_members clm
JOIN list_base cb ON cb.list_id = clm.client_list_c
LEFT JOIN sales_aggregated sa ON sa.client_c=clm.client_c AND sa.list_id=cb.list_id
LEFT JOIN sales_details_3m sd ON sd.client_c=clm.client_c AND sd.list_id=cb.list_id
LEFT JOIN wishlist_details_3m wd ON wd.client_c=clm.client_c AND wd.list_id=cb.list_id
LEFT JOIN outreaches_details_3m od ON od.client_c=clm.client_c AND od.list_id=cb.list_id
LEFT JOIN offline_actions_details_3m ofd ON ofd.client_c=clm.client_c AND ofd.list_id=cb.list_id
LEFT JOIN upgrades_detailed ud ON ud.client_c=clm.client_c AND ud.list_id=cb.list_id
;