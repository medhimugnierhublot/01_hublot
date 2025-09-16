-- Openers only
SELECT
  emails.journey_name,
  emails.name,
  people.id AS account_id,
  'unique_open' AS type,
  0 AS sold_qty,
  0.0 AS revenue_CHF,
  DATE(sends.send_date) AS send_date,
  NULL AS sellout_details
FROM `hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_email_mc_joined_types_joined_journey` AS emails
LEFT JOIN `hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_send_mc` AS sends
  ON sends.email_id = CAST(emails.id AS INT64)
LEFT JOIN (
    SELECT DISTINCT
      send_id,
      subscriber_key,
      DATE_TRUNC(DATE(TIMESTAMP(_sent_event_date)), MONTH) AS month_start_date
    FROM `hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_event_mc_sent_date_joined`
    WHERE event_type = "Open"
) AS open_events
  ON open_events.send_id = sends.id
LEFT JOIN `hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` AS people
  ON people.person_contact_id = open_events.subscriber_key
WHERE emails.journey_name IN (
  'Omnichannel Post Purchase Franchise',
  'Omnichannel Post Purchase Online',
  'Omnichannel Welcome Prospect - Boutique',
  'Omnichannel Welcome Prospect - Online'
)
AND people.id IS NOT NULL


UNION ALL


-- Converters only
SELECT
  emails.journey_name,
  emails.name,
  people.id AS account_id,
  'converted_people' AS type,
  SUM(sellout.qty_c) AS sold_qty,
  SUM(sellout.qty_c * sellout.PP_CHF) AS revenue_CHF,
  DATE(sends.send_date) AS send_date,
STRING_AGG(
  FORMAT('%s:%.2f', CAST(DATE(sellout.sellout_date_c) AS STRING), sellout.PP_CHF),
  ','
) AS sellout_details

FROM `hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_email_mc_joined_types_joined_journey` AS emails
LEFT JOIN `hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_send_mc` AS sends
  ON sends.email_id = CAST(emails.id AS INT64)
LEFT JOIN (
    SELECT DISTINCT
      send_id,
      subscriber_key,
      DATE(TIMESTAMP(_sent_event_date)) AS _sent_event_date,
      DATE_TRUNC(DATE(TIMESTAMP(_sent_event_date)), MONTH) AS month_start_date
    FROM `hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_event_mc_sent_date_joined`
    WHERE event_type = "Open"
) AS open_events
  ON open_events.send_id = sends.id
LEFT JOIN `hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` AS people
  ON people.person_contact_id = open_events.subscriber_key
LEFT JOIN `hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_current` AS belongings
  ON belongings.account_c = people.id
LEFT JOIN `hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_sellout_with_valo` AS sellout
  ON sellout.serial_c = belongings.serial_number_c
WHERE emails.journey_name IN (
  'Omnichannel Post Purchase Franchise',
  'Omnichannel Post Purchase Online',
  'Omnichannel Welcome Prospect - Boutique',
  'Omnichannel Welcome Prospect - Online'
)
AND DATE(sellout.sellout_date_c) > open_events._sent_event_date
AND DATE(sellout.sellout_date_c) < DATE_ADD(open_events._sent_event_date, INTERVAL 90 DAY)
AND open_events._sent_event_date >= (
  SELECT MAX(DATE(TIMESTAMP(_sent_event_date)))
  FROM `hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_event_mc_sent_date_joined`
  WHERE subscriber_key = open_events.subscriber_key
    AND DATE(TIMESTAMP(_sent_event_date)) < DATE(sellout.sellout_date_c)
    AND event_type = "Open"
)
AND people.id IS NOT NULL
GROUP BY
  emails.journey_name,
  emails.name,
  people.id,
  sends.send_date