WITH
  outreaches AS (
    SELECT
      ap.id AS account_id,
      DATE(tsks.created_date) AS created_date
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_task_filtered` tsks
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_contact_filtered` ctcs
      ON tsks.who_id = ctcs.id
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
      ON ctcs.id = ap.person_contact_id
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_user_with_markets` uswm
      ON tsks.owner_id = uswm.id
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_contact_user` ctcu
      ON uswm.id = ctcu.salesforce_user_c
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_contact_relation_filtered` accrf
      ON ctcu.id = accrf.contact_id
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_boutiques` bout
      ON accrf.account_id = bout.id
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_user_role_filtered` role
      ON uswm.user_role_id = role.id
    WHERE
      tsks.channel_c IN ('Call', 'Email', 'Kakao', 'Line', 'SMS', 'WeChat', 'WhatsApp')
      AND LOWER(tsks.description) NOT LIKE '%bug%'
      AND LOWER(tsks.description) NOT LIKE '%internal%'
      AND (
        (bout.boutique_type_2_c = 'DOS' AND LOWER(bout.name) NOT LIKE '%eboutique%')
        OR bout.id = '0010X00004snyN3QAI'
      )
      AND (
        LOWER(role.name) LIKE '%boutique manager%'
        OR LOWER(role.name) LIKE '%sales associate%'
        OR uswm.id IN ('0050X000007hoVsQAI', '0056700000CeROxAAN', '0056700000EOFDeAAP')
      )
      AND (bout.market != 'Greater China' OR bout.market IS NULL)
      AND (bout.status_c != 'Inactive' OR bout.status_c IS NULL)
  ),

  wishlist AS (
    SELECT
      wish.account_c AS account_id,
      wish.id AS wishlist_id,
      wish.created_date
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_wishlist_c_filtered` wish
  )

SELECT
    DATE(cal.calendar_history_date) AS calendar_history_date,
    bout.market AS primary_dos_market,
    bout.name AS primary_dos_name,
    bout.y_2_store_id_c AS store_id,
    pos.market AS primary_pos_market,
    pos.name AS primary_pos_name,
    sa.name AS sa_name,
    CASE
    WHEN (ap.email_syntax_quality = 1 AND ap.marketing_consent = 1)
        OR (ap.phone_syntax_quality = 1 AND ap.marketing_consent = 1)
    THEN True
    ELSE False
    END AS account_contactability,
    ah.market AS account_market,
    ah.life_time_segment AS account_segment,
    ah.status AS account_status,
    COUNT(DISTINCT ap.id) AS created_accounts,
    COUNT(DISTINCT IF(DATE(ap.created_date) = DATE(cal.calendar_history_date) AND ah.life_time_segment = 'Loyal', ap.id, NULL)) AS created_accounts_loyal,
    COUNT(DISTINCT IF(DATE(ap.created_date) = DATE(cal.calendar_history_date) AND ah.life_time_segment = 'One Timer', ap.id, NULL)) AS created_accounts_one_timer,
    COUNT(DISTINCT IF(DATE(ap.created_date) = DATE(cal.calendar_history_date) AND ah.life_time_segment = 'Prospect', ap.id, NULL)) AS created_accounts_prospect,
    COUNT(DISTINCT IF(DATE(ap.created_date) = DATE(cal.calendar_history_date) AND ah.life_time_segment = 'VIC', ap.id, NULL)) AS created_accounts_vic,
    COUNT(DISTINCT IF(DATE(ap.created_date) = DATE(cal.calendar_history_date) AND ah.life_time_segment = 'VVIC', ap.id, NULL)) AS created_accounts_vvic,

    -- All newly purchasing accounts on that day
    COUNT(DISTINCT IF(
        DATE(ap.first_purchase_date_c) = DATE(cal.calendar_history_date),
        ap.id,
        NULL
    )) AS created_clients,
    -- Of those, how many are local
    COUNT(DISTINCT IF(
        DATE(ap.first_purchase_date_c) = DATE(cal.calendar_history_date)
        AND ah.billing_country_code = bout.billing_country_code,
        ap.id,
        NULL
    )) AS created_clients_local,
    COUNT(DISTINCT IF(ah.billing_country_code = bout.billing_country_code, ap.id, NULL)) AS local_accounts,
    COUNT(DISTINCT IF(ah.is_hublotista_v_2_c = 'Yes', ap.id, NULL)) AS hublotista_accounts,
    COUNT(DISTINCT IF(
        ah.is_hublotista_v_2_c = 'Yes'
        AND DATE(ap.first_purchase_date_c) = DATE(cal.calendar_history_date),
        ap.id,
        NULL
        )) AS created_clients_hublotistas,
    COUNT(DISTINCT IF(
    (ap.email_syntax_quality = 1 AND ap.marketing_consent = 1)
    OR (ap.phone_syntax_quality = 1 AND ap.marketing_consent = 1),
    ap.id, NULL)) AS lvmh_contactable,
    COUNT(DISTINCT o.account_id) AS outreached_accounts,
    COUNT(DISTINCT wish.wishlist_id) AS total_wishes
FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_primary_boutiques` bout
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah
  ON bout.id = ah.primary_boutique_c
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
  ON ah.account_id = ap.id
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_external_pos` pos
  ON ap.primary_external_pos_c = pos.id
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_distinct_sa_joined_users` sa
  ON ah.sales_person_new_c = sa.sales_person_new_c
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_calendar_history` cal
  ON DATE(ap.created_date) = DATE(cal.calendar_history_date)
LEFT JOIN outreaches o
  ON ah.account_id = o.account_id
  AND DATE(o.created_date) = DATE(cal.calendar_history_date)
LEFT JOIN wishlist wish
  ON ap.id = wish.account_id
  AND DATE(wish.created_date) = DATE(cal.calendar_history_date)

GROUP BY
1,2,3,4,5,6,7,8,9,10,11