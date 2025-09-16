SELECT
    ap.id AS account_id,
    DATE(activity_date) AS activity_date
FROM
    `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_task_filtered` tsks
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
    -- is_outreach filter
    tsks.channel_c IN ('Call', 'Email', 'Kakao', 'Line', 'SMS', 'WeChat', 'WhatsApp')
    AND (LOWER(tsks.description) NOT LIKE '%bug%' AND LOWER(tsks.description) NOT LIKE '%internal%')


    -- is_fusion_scope filter
    AND (
        (bout.boutique_type_2_c = 'DOS' AND LOWER(bout.name) NOT LIKE '%eboutique%')
        OR bout.id = '0010X00004snyN3QAI'
    )


    -- is_fusion_user filter
    AND (
        LOWER(role.name) LIKE '%boutique manager%'
        OR LOWER(role.name) LIKE '%sales associate%'
        OR uswm.id IN ('0050X000007hoVsQAI', '0056700000CeROxAAN', '0056700000EOFDeAAP')
    )


    -- existing filters
    AND (bout.market != 'Greater China' OR bout.market IS NULL)
    AND (bout.status_c != 'Inactive' OR bout.status_c IS NULL)