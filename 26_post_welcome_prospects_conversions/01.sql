WITH
-- Opens (target journey + timeframe)
opens_raw AS (
  SELECT
    p.id AS account_id,
    p.market AS current_person_market,
    o.subscriber_key,
    CAST(CAST(o._sent_event_date AS DATETIME) AS DATE) AS open_date,
    e.name AS email_name,
    e.journey_name,
    e.type_c AS email_type
  FROM `hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_event_mc_sent_date_joined` o
  JOIN `hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_send_mc` s
    ON o.send_id = s.id
  JOIN `hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_email_mc_joined_types_joined_journey` e
    ON CAST(e.id AS INT64) = s.email_id
  JOIN `hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` p
    ON p.person_contact_id = o.subscriber_key
  WHERE o.event_type = 'Open'
    AND e.type_c = 'Customer Journey'
    AND STARTS_WITH(e.journey_name, 'Omnichannel Welcome Prospect')
    AND CAST(CAST(o._sent_event_date AS DATETIME) AS DATE)
        BETWEEN DATE('2023-10-20') AND DATE('2025-09-17')
),

-- Purchases (dedup per watch)
purchases_raw AS (
  SELECT
    bel.account_c AS account_id,
    bel.serial_number_c AS serial_number,
    COALESCE(CAST(bel.warranty_activation_date_c AS DATE),
             CAST(bel.purchase_date_c AS DATE)) AS purchase_date,
    bel.retail_price_chf_c AS price_chf,
    bel.product_reference_c,
    bel.product_code_c,
    prd.collection_c
  FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_c_filtered` bel
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_product_2_filtered` prd
    ON bel.product_code_c = prd.id
),
purchases AS (
  SELECT account_id, serial_number,
         MIN(purchase_date) AS purchase_date,
         MAX(price_chf)     AS price_chf,
         ANY_VALUE(product_reference_c) AS product_reference_c,
         ANY_VALUE(product_code_c) AS product_code_c,
         ANY_VALUE(collection_c) AS collection_c
  FROM purchases_raw
  GROUP BY account_id, serial_number
),

-- Metrics/status/segment at open
metrics_at_open AS (
  SELECT
    o.account_id, o.open_date,
    o.current_person_market, o.email_name, o.journey_name, o.email_type,
    MAX(p.purchase_date) AS last_purchase_date,
    SUM(p.price_chf) AS sum_spending,
    COUNT(DISTINCT p.serial_number) AS nb_watches
  FROM opens_raw o
  LEFT JOIN purchases p
    ON p.account_id = o.account_id
   AND p.purchase_date <= o.open_date
  GROUP BY o.account_id, o.open_date,
           o.current_person_market, o.email_name, o.journey_name, o.email_type
),
status_at_open AS (
  SELECT m.*,
    CASE
      WHEN last_purchase_date IS NULL THEN 'Prospect'
      WHEN last_purchase_date >= DATE_SUB(m.open_date, INTERVAL 4 YEAR) THEN 'Active'
      WHEN last_purchase_date >= DATE_SUB(m.open_date, INTERVAL 8 YEAR) THEN 'Sleeping'
      ELSE 'Inactive'
    END AS status_at_open
  FROM metrics_at_open m
),
classified_at_open AS (
  SELECT s.*,
    CASE
      WHEN IFNULL(s.nb_watches,0) = 0 THEN 'Prospect'
      WHEN IFNULL(s.sum_spending,0) < 100000 AND IFNULL(s.nb_watches,0) = 1 THEN 'One Timer'
      WHEN IFNULL(s.sum_spending,0) < 100000 AND IFNULL(s.nb_watches,0) > 1 THEN 'Loyal'
      WHEN IFNULL(s.sum_spending,0) >= 100000 AND IFNULL(s.sum_spending,0) < 200000 THEN 'VIC'
      WHEN IFNULL(s.sum_spending,0) >= 200000 THEN 'VVIC'
      ELSE 'Prospect'
    END AS segment_at_open
  FROM status_at_open s
),

-- Sellouts and attribution (last-touch within 90d)
sellouts AS (
  SELECT
    CAST(sellout.sellout_date_c AS DATE) AS sellout_date,
    sellout.qty_c,
    sellout.qty_c * sellout.PP_CHF AS revenue_CHF,
    b.account_c AS account_id
  FROM `hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_sellout_with_valo` sellout
  JOIN `hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_current` b
    ON b.serial_number_c = sellout.serial_c
),
last_touch_opens AS (
  SELECT
    c.account_id, c.journey_name, c.open_date,
    so.sellout_date, so.qty_c, so.revenue_CHF
  FROM classified_at_open c
  JOIN sellouts so
    ON so.account_id = c.account_id
   AND so.sellout_date >  c.open_date
   AND so.sellout_date <= DATE_ADD(c.open_date, INTERVAL 90 DAY)
  QUALIFY ROW_NUMBER() OVER (
            PARTITION BY so.account_id, so.sellout_date
            ORDER BY c.open_date DESC
          ) = 1
),

-- All purchases (not just attributed) within 90d after open
sales_details AS (
  SELECT
    ca.account_id, ca.journey_name, ca.open_date,
    STRING_AGG(
      FORMAT('%s: CHF%.0f (%s, %s)',
        FORMAT_DATE('%Y-%m-%d', p.purchase_date),
        p.price_chf,
        INITCAP(p.collection_c),
        INITCAP(p.product_reference_c)
      ),
      '\n' ORDER BY p.purchase_date ASC
    ) AS sales_details
  FROM classified_at_open ca
  JOIN purchases p
    ON ca.account_id = p.account_id
   AND p.purchase_date > ca.open_date
   AND p.purchase_date <= DATE_ADD(ca.open_date, INTERVAL 90 DAY)
  GROUP BY ca.account_id, ca.journey_name, ca.open_date
),

-- Wishlist details
wishlist_details AS (
  SELECT
    ca.account_id, ca.journey_name, ca.open_date,
    STRING_AGG(
      FORMAT('%s: %s, %s',
        FORMAT_DATE('%Y-%m-%d', DATE(wish.created_date)),
        INITCAP(prd.collection_c),
        INITCAP(prd.product_code)
      ),
      '\n' ORDER BY DATE(wish.created_date) ASC
    ) AS wishlist_details
  FROM classified_at_open ca
  JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_wishlist_c_filtered` wish
    ON ca.account_id = wish.account_c
   AND DATE(wish.created_date) > ca.open_date
   AND DATE(wish.created_date) <= DATE_ADD(ca.open_date, INTERVAL 90 DAY)
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_product_2_filtered` prd
    ON wish.product_c = prd.id
  GROUP BY ca.account_id, ca.journey_name, ca.open_date
),

-- Outreaches details
outreaches_details AS (
  SELECT
    ca.account_id, ca.journey_name, ca.open_date,
    STRING_AGG(
      FORMAT('%s (%s)',
        FORMAT_DATE('%Y-%m-%d', DATE(tsks.created_date)),
        tsks.channel_c
      ),
      '\n' ORDER BY tsks.created_date ASC
    ) AS outreaches_details
  FROM classified_at_open ca
  JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_task_filtered` tsks
    ON ca.account_id = tsks.account_id
   AND DATE(tsks.created_date) > ca.open_date
   AND DATE(tsks.created_date) <= DATE_ADD(ca.open_date, INTERVAL 90 DAY)
  WHERE tsks.channel_c IN ('Call','Email','Kakao','Line','SMS','WeChat','WhatsApp')
  GROUP BY ca.account_id, ca.journey_name, ca.open_date
),

-- Offline actions details
offline_actions_details AS (
  SELECT
    ca.account_id, ca.journey_name, ca.open_date,
    STRING_AGG(
      FORMAT('%s (%s)',
        FORMAT_DATE('%Y-%m-%d', DATE(tsks.activity_date)),
        LOWER(tsks.type)
      ),
      '\n' ORDER BY tsks.activity_date ASC
    ) AS offline_actions_details
  FROM classified_at_open ca
  JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_task_filtered` tsks
    ON ca.account_id = tsks.account_id
   AND DATE(tsks.activity_date) > ca.open_date
   AND DATE(tsks.activity_date) <= DATE_ADD(ca.open_date, INTERVAL 90 DAY)
  WHERE tsks.type IS NOT NULL
    AND tsks.record_type_id = "01267000000V6gLAAS"
  GROUP BY ca.account_id, ca.journey_name, ca.open_date
)

-- Final
SELECT
  ca.account_id,
  ca.open_date AS sent_date,   -- precise open date of the campaign
  ca.email_name AS name,
  ca.journey_name,
  ca.email_type,
  ca.segment_at_open,
  ca.status_at_open,
  ca.current_person_market,
  IF(cv.account_id IS NOT NULL, 1, 0) AS converted,
  sd.sales_details,
  wd.wishlist_details,
  od.outreaches_details,
  ofd.offline_actions_details
FROM classified_at_open ca
LEFT JOIN (SELECT DISTINCT account_id, journey_name, open_date FROM last_touch_opens) cv
  ON ca.account_id = cv.account_id
 AND ca.journey_name = cv.journey_name
 AND ca.open_date = cv.open_date
LEFT JOIN sales_details sd
  ON ca.account_id = sd.account_id AND ca.journey_name = sd.journey_name AND ca.open_date = sd.open_date
LEFT JOIN wishlist_details wd
  ON ca.account_id = wd.account_id AND ca.journey_name = wd.journey_name AND ca.open_date = wd.open_date
LEFT JOIN outreaches_details od
  ON ca.account_id = od.account_id AND ca.journey_name = od.journey_name AND ca.open_date = od.open_date
LEFT JOIN offline_actions_details ofd
  ON ca.account_id = ofd.account_id AND ca.journey_name = ofd.journey_name AND ca.open_date = ofd.open_date
WHERE sd.sales_details IS NOT NULL
   OR wd.wishlist_details IS NOT NULL
   OR od.outreaches_details IS NOT NULL
   OR ofd.offline_actions_details IS NOT NULL
   OR cv.account_id IS NOT NULL
ORDER BY ca.open_date DESC, ca.email_name, ca.account_id;
