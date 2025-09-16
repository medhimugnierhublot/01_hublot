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
            wish.account_c,
            wish.id AS wishlist_id,
            wish.created_date,
            prd.collection_c
        FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_wishlist_c_filtered` wish
        LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_product_2_filtered` prd
            ON wish.product_c = prd.id
    )

SELECT 
    DATE(cal.calendar_history_date) AS calendar_history_date,
    bout.market AS primary_dos_market,
    bout.name as primary_dos_name,
    bout.y_2_store_id_c AS store_id,
    pos.market AS primary_pos_market,
    pos.name AS primary_pos_name,
    sa.name AS sa_name,
    CASE
        WHEN
            (ap.email_syntax_quality = 1 AND ap.marketing_consent = 1)
            OR
            (ap.phone_syntax_quality = 1 AND ap.marketing_consent = 1)
        THEN 1
        ELSE 0
    END AS is_contactable_LVMH,
    ah.market AS account_market,
    ah.life_time_segment AS account_life_time_segment,
    ah.status AS account_status,
    DATE(ap.created_date) AS account_created_date,
    ap.first_purchase_date_c AS account_first_purchase_date,
    IF(
    ah.billing_country_code = bout.billing_country_code, 
    'Local', 
    'Non-Local'
    ) AS is_local,
    ah.is_hublotista_v_2_c AS is_hublotista,
FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_primary_boutiques` bout
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah
    ON bout.id = ah.primary_boutique_c
LEFT  JOIN
  `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
    ON ah.account_id = ap.id
LEFT JOIN
   `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_external_pos` pos
    ON ap.primary_external_pos_c = pos.id
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_distinct_sa_joined_users` sa
    ON ah.sales_person_new_c=sa.sales_person_new_c
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_calendar_history` cal
    ON DATE(ap.created_date) = DATE(cal.calendar_history_date)
LEFT JOIN outreaches o
    ON ah.account_id = o.account_id