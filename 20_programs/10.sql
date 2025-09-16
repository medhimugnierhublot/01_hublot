WITH


list_base AS (
  SELECT DISTINCT
    bout.name AS bout_name,
    bout.market AS bout_market,
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


sales_details_agg AS (
  SELECT
    client_c,
    list_id,
    STRING_AGG(
      FORMAT('%s: CHF%.0f (%s, %s)', FORMAT_DATE('%Y-%m-%d', purchase_date), retail_price_chf_c, INITCAP(collection_c), INITCAP(product_reference_c)),
      '\n' ORDER BY purchase_date DESC
    ) AS sales_details
  FROM sales_attributed
  GROUP BY client_c, list_id
),


wishlist_details_agg AS (
  SELECT
    cm.account_id,
    cb.list_id,
    STRING_AGG(
      FORMAT('%s: %s, %s', FORMAT_DATE('%Y-%m-%d', w.created_date), INITCAP(prd.collection_c), INITCAP(prd.product_code)),
      '\n' ORDER BY w.created_date DESC
    ) AS wishlist_details_3m,
    STRING_AGG(
      FORMAT('%s: %s, %s', FORMAT_DATE('%Y-%m-%d', w.created_date), INITCAP(prd.collection_c), INITCAP(prd.product_code)),
      '\n' ORDER BY w.created_date DESC
    ) FILTER (WHERE DATE(w.created_date) BETWEEN cb.created_date AND DATE_ADD(cb.created_date, INTERVAL 1 MONTH)) AS wishlist_details_1m
  FROM client_list_members cm
  JOIN list_base cb ON cb.list_id = cm.client_list_c
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_wishlist_c_filtered` w ON cm.account_id = w.account_c
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_product_2_filtered` prd ON w.product_c = prd.id
  WHERE DATE(w.created_date) BETWEEN cb.created_date AND DATE_ADD(cb.created_date, INTERVAL 3 MONTH)
  GROUP BY cm.account_id, cb.list_id
),


outreaches_details_agg AS (
  SELECT
    tsks.account_id,
    cb.list_id,
    STRING_AGG(
      FORMAT('%s (%s)', FORMAT_DATE('%Y-%m-%d', tsks.created_date), tsks.channel_c),
      '\n' ORDER BY tsks.created_date DESC
    ) AS outreaches_details_3m,
    STRING_AGG(
      FORMAT('%s (%s)', FORMAT_DATE('%Y-%m-%d', tsks.created_date), tsks.channel_c),
      '\n' ORDER BY tsks.created_date DESC
    ) FILTER (WHERE DATE(tsks.created_date) BETWEEN cb.created_date AND DATE_ADD(cb.created_date, INTERVAL 1 MONTH)) AS outreaches_details_1m
  FROM outreaches_attributed tsks
  JOIN list_base cb ON TRUE  -- tsks already joined via cb in earlier CTE
  WHERE DATE(tsks.created_date) BETWEEN cb.created_date AND DATE_ADD(cb.created_date, INTERVAL 3 MONTH)
  GROUP BY tsks.account_id, cb.list_id
)


SELECT
  cb.list_id,
  cb.list_name,
  cb.created_date,
  cb.bout_name,
  cb.bout_market,
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
  sda.sales_details,
  wda.wishlist_details_1m,
  wda.wishlist_details_3m,
  oda.outreaches_details_1m,
  oda.outreaches_details_3m
FROM client_list_members clm
JOIN list_base cb ON cb.list_id = clm.client_list_c
LEFT JOIN sales_aggregated sa ON sa.client_c = clm.client_c AND sa.list_id = cb.list_id
LEFT JOIN wishes_aggregated wa ON wa.client_c = clm.client_c AND wa.list_id = cb.list_id
LEFT JOIN outreaches_aggregated oa ON oa.client_c = clm.client_c AND oa.list_id = cb.list_id
LEFT JOIN sales_details_agg sda ON sda.client_c = clm.client_c AND sda.list_id = cb.list_id
LEFT JOIN wishlist_details_agg wda ON wda.account_id = clm.account_id AND wda.list_id = cb.list_id
LEFT JOIN outreaches_details_agg oda ON oda.account_id = clm.account_id AND oda.list_id = cb.list_id;




