SELECT * FROM (SELECT 
  CAST(month_start_date AS TIMESTAMP) AS month_start_date,
  emails.name,
  emails.journey_name,
  emails.type_c AS email_type,
  COALESCE(account_history.person_type, "Prospect") AS person_type,
  people.market AS current_person_market,
  COUNT(open_events.subscriber_key) AS unique_open_count
FROM `hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_email_mc_joined_types_joined_journey` AS emails
LEFT JOIN `hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_send_mc` AS sends ON sends.email_id = CAST(emails.id AS INT64)
LEFT JOIN 
  (
    SELECT DISTINCT 
      send_id,
      subscriber_key, 
      CAST(CAST(_sent_event_date AS DATETIME) AS DATE) AS _sent_event_date,
      DATE_TRUNC(CAST(CAST(_sent_event_date AS DATETIME) AS DATE), MONTH) AS month_start_date
    FROM `hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_event_mc_sent_date_joined`
    WHERE event_type="Open"
  ) AS open_events
  ON open_events.send_id=sends.id
LEFT JOIN `hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` AS people ON people.person_contact_id=open_events.subscriber_key
LEFT JOIN (SELECT *, IF(segment_c IS NULL OR segment_c = "Prospect", NULL, "Hublot Client") AS person_type FROM `hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset`) AS account_history ON open_events.month_start_date = CAST(account_history.photo_date AS DATE) AND people.id=account_history.account_id
WHERE 
  (emails.type_c="Newsletter"
  OR emails.type_c="Customer Journey")
  -- Filter by last touch point for CRM openings
GROUP BY 
  emails.name,
  emails.journey_name,
  emails.type_c,
  month_start_date,
  account_history.person_type,
  people.market)

LEFT JOIN

(SELECT 
  CAST(month_start_date AS TIMESTAMP) AS month_start_date,
  emails.name,
  COALESCE(account_history.person_type, "Prospect") AS person_type,
  people.market AS current_person_market,
  SUM(sellout.qty_c) AS sold_qty,
  SUM(sellout.qty_c * sellout.PP_CHF) AS revenue_CHF,
  COUNT(DISTINCT people.id) AS converted_people_count
FROM `hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_email_mc_joined_types_joined_journey` AS emails
LEFT JOIN `hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_send_mc` AS sends ON sends.email_id = CAST(emails.id AS INT64)
LEFT JOIN 
  (
    SELECT DISTINCT 
      send_id, 
      subscriber_key, 
      CAST(CAST(_sent_event_date AS DATETIME) AS DATE) AS _sent_event_date,
      DATE_TRUNC(CAST(CAST(_sent_event_date AS DATETIME) AS DATE), MONTH) AS month_start_date
    FROM `hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_event_mc_sent_date_joined`
    WHERE event_type="Open"
  ) AS open_events
  ON open_events.send_id=sends.id
LEFT JOIN `hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` AS people ON people.person_contact_id=open_events.subscriber_key
LEFT JOIN `hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_current` AS belongings ON belongings.account_c=people.id
LEFT JOIN `hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_sellout_with_valo` AS sellout ON sellout.serial_c=belongings.serial_number_c
LEFT JOIN `hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_product_2_filtered` AS products ON products.id=sellout.product_c
LEFT JOIN (SELECT *, IF(segment_c IS NULL OR segment_c = "Prospect", NULL, "Hublot Client") AS person_type FROM `hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset`) AS account_history ON open_events.month_start_date = CAST(account_history.photo_date AS DATE) AND people.id=account_history.account_id
WHERE 
  (emails.type_c="Newsletter"
  OR emails.type_c="Customer Journey")
  AND (CAST(sellout.sellout_date_c AS DATE) > open_events._sent_event_date)
  AND (CAST(sellout.sellout_date_c AS DATE) < DATE_ADD(open_events._sent_event_date, INTERVAL 90 DAY))
  -- Filter by last touch point for CRM openings
  AND open_events._sent_event_date >= 
      (
        SELECT 
          MAX(CAST(CAST(_sent_event_date AS DATETIME) AS DATE))
        FROM `hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_event_mc_sent_date_joined`
        WHERE 
          subscriber_key=open_events.subscriber_key 
          AND (CAST(_sent_event_date AS TIMESTAMP) < sellout.sellout_date_c)
          AND event_type="Open"
      )
GROUP BY 
  emails.type_c,
  emails.name,
  month_start_date,
  account_history.person_type,
  people.market) USING(month_start_date, name, person_type, current_person_market)
ORDER BY
  month_start_date DESC