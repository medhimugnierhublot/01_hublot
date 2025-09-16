-- StandardSQL
WITH
/* 1) Opens joined to accounts, with exact open date */
opens_raw AS (
  SELECT
    p.id AS account_id,
    p.market AS current_person_market,
    o.subscriber_key,
    CAST(CAST(o._sent_event_date AS DATETIME) AS DATE) AS open_date,
    DATE_TRUNC(CAST(CAST(o._sent_event_date AS DATETIME) AS DATE), MONTH) AS month_start_date,
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
    AND e.type_c IN ('Newsletter','Customer Journey')
    -- AND p.id = '001J500000A53SaIAJ'  -- optional focus on one account
),


/* 2) Purchases from belongings (your spec) – include serial for dedup */
purchases_raw AS (
  SELECT
    bel.account_c AS account_id,
    bel.serial_number_c AS serial_number,
    COALESCE(CAST(bel.warranty_activation_date_c AS DATE),
             CAST(bel.purchase_date_c            AS DATE)) AS purchase_date,
    bel.retail_price_chf_c AS price_chf
  FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_c_filtered` bel
),


/* 3) Deduplicate per (account, serial): earliest purchase_date, max non-null price */
purchases AS (
  SELECT
    account_id,
    serial_number,
    MIN(purchase_date) AS purchase_date,   -- first activation/purchase for that watch
    MAX(price_chf)     AS price_chf        -- one consolidated price per watch
  FROM purchases_raw
  GROUP BY account_id, serial_number
),


/* 4) Lifetime-to-date metrics at open (<= open_date, same-day counts) */
metrics_at_open AS (
  SELECT
    o.account_id,
    o.subscriber_key,
    o.open_date,
    o.month_start_date,
    o.current_person_market,
    o.email_name,
    o.journey_name,
    o.email_type,
    MAX(p.purchase_date)                                         AS last_purchase_date,
    SUM(p.price_chf)                                             AS sum_spending,
    COUNT(DISTINCT p.serial_number)                              AS nb_watches
  FROM opens_raw o
  LEFT JOIN purchases p
    ON p.account_id   = o.account_id
   AND p.purchase_date <= o.open_date
  GROUP BY
    o.account_id, o.subscriber_key, o.open_date, o.month_start_date,
    o.current_person_market, o.email_name, o.journey_name, o.email_type
),


/* 5) Status at open (4y / 8y windows relative to open_date) */
status_at_open AS (
  SELECT
    m.*,
    CASE
      WHEN last_purchase_date IS NULL THEN 'Prospect'
      WHEN last_purchase_date >= DATE_SUB(m.open_date, INTERVAL 4 YEAR) THEN 'Active'
      WHEN last_purchase_date >= DATE_SUB(m.open_date, INTERVAL 8 YEAR) THEN 'Sleeping'
      ELSE 'Inactive'
    END AS status_at_open
  FROM metrics_at_open m
),


/* 6) Segment at open (lifetime-to-date rules, matching your Python) */
classified_at_open AS (
  SELECT
    s.*,
    IFNULL(s.sum_spending, 0) AS sum_spending_ltd,
    IFNULL(s.nb_watches,   0) AS nb_watches_ltd,
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


/* 7) Monthly openings aggregation */
openings_agg AS (
  SELECT
    CAST(month_start_date AS TIMESTAMP) AS month_start_date,
    email_name AS name,
    journey_name,
    email_type,
    segment_at_open,
    status_at_open,
    current_person_market,
    COUNT(DISTINCT subscriber_key) AS unique_open_count
  FROM classified_at_open
  GROUP BY
    month_start_date, name, journey_name, email_type,
    segment_at_open, status_at_open, current_person_market
),


/* 8) Sellouts for ROI (0–90 days after open, last-touch attribution) */
sellouts AS (
  SELECT
    CAST(sellout.sellout_date_c AS DATE) AS sellout_date,
    sellout.qty_c,
    sellout.qty_c * sellout.PP_CHF AS revenue_CHF,
    b.account_c AS account_id
  FROM `hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_sellout_with_valo` AS sellout
  JOIN `hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_current` AS b
    ON b.serial_number_c = sellout.serial_c
  JOIN `hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_product_2_filtered` AS pr
    ON pr.id = sellout.product_c
),


/* 9) Attribute each sellout to the last open within prior 0–90 days */
last_touch_opens AS (
  SELECT
    c.account_id,
    c.open_date,
    c.month_start_date,
    c.email_name AS name,
    c.journey_name,
    c.email_type,
    c.segment_at_open,
    c.status_at_open,
    c.current_person_market,
    so.sellout_date,
    so.qty_c,
    so.revenue_CHF
  FROM classified_at_open c
  JOIN sellouts so
    ON so.account_id   = c.account_id
   AND so.sellout_date >  c.open_date
   AND so.sellout_date <  DATE_ADD(c.open_date, INTERVAL 90 DAY)
  QUALIFY ROW_NUMBER() OVER (
            PARTITION BY so.account_id, so.sellout_date
            ORDER BY c.open_date DESC
          ) = 1
),


/* 10) Monthly conversions aggregation (+ list of converting accounts) */
conversions_agg AS (
  SELECT
    CAST(month_start_date AS TIMESTAMP) AS month_start_date,
    name,
    segment_at_open,
    status_at_open,
    current_person_market,
    SUM(qty_c) AS sold_qty,
    SUM(revenue_CHF) AS revenue_CHF,
    COUNT(DISTINCT account_id) AS converted_people_count,
    ARRAY_AGG(DISTINCT account_id) AS account_ids
  FROM last_touch_opens
  GROUP BY
    month_start_date, name, segment_at_open, status_at_open, current_person_market
)


/* 11) Final output */
SELECT
  o.month_start_date,
  o.name,
  o.journey_name,
  o.email_type,
  o.segment_at_open,
  o.status_at_open,
  o.current_person_market,
  o.unique_open_count,
  c.sold_qty,
  c.revenue_CHF,
  c.converted_people_count,
  c.account_ids
FROM openings_agg o
LEFT JOIN conversions_agg c
  USING (month_start_date, name, segment_at_open, status_at_open, current_person_market)
ORDER BY o.month_start_date DESC, o.name;




