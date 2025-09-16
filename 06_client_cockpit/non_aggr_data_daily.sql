CREATE OR REPLACE TABLE `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_client_cockpit_test_medhi` AS
WITH created_accounts_sub AS (
  SELECT
    DATE_TRUNC(DATE(cal.calendar_history_date), MONTH) AS calendar_month,
    pb.name AS pb_name,
    pb.market AS pb_market,
    sa.name AS sa_name,
    pos.market AS pos_market,
    pos.name AS pos_name,

    COUNT(DISTINCT ap.id) AS created_accounts

  FROM
    `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_calendar_history` cal
  JOIN
    `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
    ON DATE(ap.created_date) >= DATE_TRUNC(DATE(cal.calendar_history_date), MONTH)
    AND DATE(ap.created_date) < DATE_ADD(DATE_TRUNC(DATE(cal.calendar_history_date), MONTH), INTERVAL 1 MONTH)
  LEFT JOIN
    `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_external_pos` pos
    ON ap.primary_external_pos_c = pos.id
  LEFT JOIN
    `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_primary_boutiques` pb
    ON ap.primary_boutique_c = pb.id
  LEFT JOIN
    `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_distinct_sa_joined_users` sa
    ON ap.sales_person_new_c = sa.sales_person_new_c
  WHERE ap.created_date IS NOT NULL
  GROUP BY calendar_month, pb_name, pb_market, sa_name, pos_market, pos_name
),

snapshot_metrics_sub AS (
  SELECT
    DATE_TRUNC(DATE(cal.calendar_history_date), MONTH) AS calendar_month,
    pb.name AS pb_name,
    pb.market AS pb_market,
    sa.name AS sa_name,
    pos.market AS pos_market,
    pos.name AS pos_name,

    ah.market AS ah_market,
    ah.macro_segment AS ah_macro_segment,

    -- Contactability logic
    (
      (ap.email_syntax_quality = 1 AND ap.marketing_consent = 1)
      OR
      (ap.phone_syntax_quality = 1 AND ap.marketing_consent = 1)
    ) AS ap_is_contactable_LVMH,

    -- Created Clients (non-Prospects)
    COUNT(DISTINCT IF(
      ah.macro_segment IS NOT NULL AND ah.macro_segment != "Prospect",
      ap.id,
      NULL
    )) AS created_clients,

    -- Active Clients
    COUNT(DISTINCT IF(
      ah.macro_segment IN ("One Timer", "Loyal", "VIC", "VVIC"),
      ap.id,
      NULL
    )) AS created_clients_active,

    -- Contactable Clients
    COUNT(DISTINCT IF(
      (
        (ap.email_syntax_quality = 1 AND ap.marketing_consent = 1)
        OR
        (ap.phone_syntax_quality = 1 AND ap.marketing_consent = 1)
      )
      AND ah.macro_segment IS NOT NULL
      AND ah.macro_segment != "Prospect",
      ap.id,
      NULL
    )) AS created_clients_contactable,

    -- Local Clients
    COUNT(DISTINCT IF(
      ah.billing_country_code = pb.billing_country_code
      AND ah.macro_segment IS NOT NULL
      AND ah.macro_segment != "Prospect",
      ap.id,
      NULL
    )) AS created_clients_local

  FROM
    `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_calendar_history` cal
  JOIN
    `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah
    ON DATE(ah.photo_date) >= DATE_TRUNC(DATE(cal.calendar_history_date), MONTH)
    AND DATE(ah.photo_date) < DATE_ADD(DATE_TRUNC(DATE(cal.calendar_history_date), MONTH), INTERVAL 1 MONTH)
  JOIN
    `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
    ON ah.account_id = ap.id
  LEFT JOIN
    `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_external_pos` pos
    ON ap.primary_external_pos_c = pos.id
  LEFT JOIN
    `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_primary_boutiques` pb
    ON ah.primary_boutique_c = pb.id
  LEFT JOIN
    `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_distinct_sa_joined_users` sa
    ON ah.sales_person_new_c = sa.sales_person_new_c
  WHERE ap.created_date IS NOT NULL
  GROUP BY calendar_month, pb_name, pb_market, sa_name, pos_market, pos_name,
           ah_market, ah_macro_segment, ap_is_contactable_LVMH
)

-- Final Join
SELECT
  s.calendar_month,
  s.pb_name,
  s.pb_market,
  s.sa_name,
  s.pos_market,
  s.pos_name,
  s.ah_market,
  s.ah_macro_segment,
  s.ap_is_contactable_LVMH,

  c.created_accounts,
  s.created_clients,
  s.created_clients_active,
  s.created_clients_contactable,
  s.created_clients_local

FROM snapshot_metrics_sub s
LEFT JOIN created_accounts_sub c
  ON s.calendar_month = c.calendar_month
  AND s.pb_name = c.pb_name
  AND s.pb_market = c.pb_market
  AND s.sa_name = c.sa_name
  AND s.pos_market = c.pos_market
  AND s.pos_name = c.pos_name


