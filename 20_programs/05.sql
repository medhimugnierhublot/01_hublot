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
      b.product_reference_c
    FROM client_list_members cm
    JOIN list_base cb ON cb.list_id = cm.client_list_c
    JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_c_filtered` b
      ON cm.account_id = b.account_c
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
      SUM(CASE WHEN DATE_DIFF(DATE(purchase_date), DATE(created_date), DAY) BETWEEN 0 AND 90 THEN retail_price_chf_c ELSE 0 END) AS revenue_3m
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
  )


SELECT
  lb.list_id,
  lb.list_name,
  lb.created_date,
  lb.bout_name,
  lb.bout_market,
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
  IFNULL(wa.wishes_3m, 0) AS wishes_3m
FROM client_list_members clm
JOIN list_base lb ON lb.list_id = clm.client_list_c
LEFT JOIN sales_aggregated sa ON sa.client_c = clm.client_c AND sa.list_id = lb.list_id
LEFT JOIN wishes_aggregated wa ON wa.client_c = clm.client_c AND wa.list_id = lb.list_id