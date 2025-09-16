WITH
    campaign_base AS (
    SELECT
        c.id AS campaign_or_subcampaign_id,
        c.name AS campaign_or_subcampaign_name,
        c.parent_id,
        CASE WHEN c.parent_id IS NULL THEN c.id ELSE c.parent_id END AS campaign_id,
        CASE WHEN c.parent_id IS NULL THEN c.name ELSE cp.name END AS campaign_name,
        CASE WHEN c.parent_id IS NULL THEN NULL ELSE c.id END AS subcampaign_id,
        CASE WHEN c.parent_id IS NULL THEN NULL ELSE c.name END AS subcampaign_name,
        c.end_date,
        a.name AS boutique,
        c.territory_c AS territory,
        ab.territory_c AS fallback_territory
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_salesforce.campaign` c
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_salesforce.campaign` cp ON c.parent_id = cp.id
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_salesforce.account` a ON a.id = c.boutique_c
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_boutiques` ab ON ab.id = c.boutique_c
    WHERE c.end_date >= '2021-01-01' AND c.id IS NOT NULL
    ),
    campaign_members AS (
        SELECT
            cm.id AS member_id,
            cm.campaign_id,
            cm.contact_id,
            cm.status AS member_status,
            cm.has_responded,
            cm.participated_c,
            ap.id AS account_id,
            ap.macro_segment,
            ap.type AS client_type
        FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_salesforce.campaign_member` cm
        LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
            ON cm.contact_id = ap.person_contact_id
        WHERE 
            NOT cm.is_deleted
        ),
    belonging AS (
        SELECT
            account_c,
            id AS sale_id,
            COALESCE(warranty_activation_date_c, purchase_date_c) AS purchase_date,
            retail_price_chf_c,
            product_reference_c
        FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_c_filtered`
        WHERE 
            is_watch_c = TRUE
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
    --     ,
    -- outreaches AS (
    --     SELECT
    --         tsks.id AS outreach_id,
    --         ap.id AS account_id,
    --         DATE(tsks.created_date) AS created_date
    --     FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_task_filtered` tsks
    --     LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_contact_filtered` ctcs
    --         ON tsks.who_id = ctcs.id
    --     LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
    --         ON ctcs.id = ap.person_contact_id
    --     LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_user_with_markets` uswm
    --         ON tsks.owner_id = uswm.id
    --     LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_contact_user` ctcu
    --         ON uswm.id = ctcu.salesforce_user_c
    --     LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_contact_relation_filtered` accrf
    --         ON ctcu.id = accrf.contact_id
    --     LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_boutiques` bout
    --         ON accrf.account_id = bout.id
    --     LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_user_role_filtered` role
    --         ON uswm.user_role_id = role.id
    --     WHERE
    --         tsks.channel_c IN ('Call', 'Email', 'Kakao', 'Line', 'SMS', 'WeChat', 'WhatsApp')
    --         AND LOWER(tsks.description) NOT LIKE '%bug%' OR LOWER(tsks.description) IS NULL
    --         AND LOWER(tsks.description) NOT LIKE '%internal%' OR LOWER(tsks.description) IS NULL
    --         AND (
    --         (bout.boutique_type_2_c = 'DOS' AND LOWER(bout.name) NOT LIKE '%eboutique%' OR LOWER(bout.name) IS NULL)
    --         OR bout.id = '0010X00004snyN3QAI'
    --         )
    --         AND (
    --         LOWER(role.name) LIKE '%boutique manager%'
    --         OR LOWER(role.name) LIKE '%sales associate%'
    --         OR uswm.id IN ('0050X000007hoVsQAI', '0056700000CeROxAAN', '0056700000EOFDeAAP')
    --         )
    --         AND (bout.market != 'Greater China' OR bout.market IS NULL)
    --         AND (bout.status_c != 'Inactive' OR bout.status_c IS NULL)
    --     )


    SELECT
    cb.campaign_id,
    cb.campaign_name,
    cb.subcampaign_id,
    cb.subcampaign_name,
    cb.boutique,
    COALESCE(cb.territory, cb.fallback_territory) AS territory,
    DATE(cb.end_date) AS campaign_end_date,
    cm.member_id,
    cm.account_id,
    cm.contact_id,
    cm.client_type,
    cm.macro_segment,
    cm.member_status,
    cm.has_responded,
    cm.participated_c,
    IF(cm.client_type = 'Hublot Client', 1, 0) AS is_client,
    IF(cm.client_type = 'Prospect', 1, 0) AS is_prospect,
    IF(cm.macro_segment IS NOT NULL, 1, 0) AS has_macro_segment,
    -- Email interaction flags
    IF(cm.member_status = 'Clicked', 1, 0) AS is_clicked,
    IF(cm.member_status = 'Hard Bounce', 1, 0) AS is_hard_bounce,
    IF(cm.member_status = 'Invited', 1, 0) AS is_invited,
    IF(cm.member_status = 'Opened', 1, 0) AS is_opened,
    IF(cm.member_status = 'Refused', 1, 0) AS is_refused,
    IF(cm.member_status = 'Responded', 1, 0) AS is_responded_status,
    IF(cm.member_status = 'Sent', 1, 0) AS is_sent,
    IF(cm.member_status = 'Soft Bounce', 1, 0) AS is_soft_bounce,
    IF(cm.has_responded, 1, 0) AS responded,
    IF(cm.participated_c = TRUE, 1, 0) AS has_participated,
    -- COUNT(DISTINCT CASE WHEN DATE(o.created_date) BETWEEN DATE_SUB(DATE(cb.end_date), INTERVAL 90 DAY) AND DATE_SUB(DATE(cb.end_date), INTERVAL 1 DAY) THEN o.created_date END) AS outreaches_3m_prior, 
    -- COUNT(DISTINCT CASE WHEN cm.participated_c = TRUE AND DATE(o.created_date) BETWEEN DATE_SUB(DATE(cb.end_date), INTERVAL 90 DAY) AND DATE_SUB(DATE(cb.end_date), INTERVAL 1 DAY) THEN o.created_date END) AS outreaches_3m_prior_has_participated,
    -- Sales KPIs: count of unique sale IDs
    COUNT(DISTINCT CASE WHEN DATE_DIFF(DATE(b.purchase_date), DATE(cb.end_date), DAY) BETWEEN 0 AND 30 THEN b.sale_id END) AS converted_1m,
    COUNT(DISTINCT CASE WHEN DATE_DIFF(DATE(b.purchase_date), DATE(cb.end_date), DAY) BETWEEN 0 AND 90 THEN b.sale_id END) AS converted_3m,
    -- Revenue by window
    SUM(CASE WHEN DATE_DIFF(DATE(b.purchase_date), DATE(cb.end_date), DAY) BETWEEN 0 AND 30 THEN b.retail_price_chf_c ELSE 0 END) AS revenue_1m,
    SUM(CASE WHEN DATE_DIFF(DATE(b.purchase_date), DATE(cb.end_date), DAY) BETWEEN 0 AND 90 THEN b.retail_price_chf_c ELSE 0 END) AS revenue_3m,
    -- Participated + conversion
    COUNT(DISTINCT CASE WHEN cm.participated_c = TRUE AND DATE_DIFF(DATE(b.purchase_date), DATE(cb.end_date), DAY) BETWEEN 0 AND 30 THEN b.sale_id END) AS converted_1m_has_participated,
    COUNT(DISTINCT CASE WHEN cm.participated_c = TRUE AND DATE_DIFF(DATE(b.purchase_date), DATE(cb.end_date), DAY) BETWEEN 0 AND 90 THEN b.sale_id END) AS converted_3m_has_participated,
    SUM(CASE WHEN cm.participated_c = TRUE AND DATE_DIFF(DATE(b.purchase_date), DATE(cb.end_date), DAY) BETWEEN 0 AND 30 THEN b.retail_price_chf_c ELSE 0 END) AS revenue_1m_has_participated,
    SUM(CASE WHEN cm.participated_c = TRUE AND DATE_DIFF(DATE(b.purchase_date), DATE(cb.end_date), DAY) BETWEEN 0 AND 90 THEN b.retail_price_chf_c ELSE 0 END) AS revenue_3m_has_participated,
    -- Wishlist KPIs
    COUNT(DISTINCT CASE WHEN DATE_DIFF(DATE(w.created_date), DATE(cb.end_date), DAY) BETWEEN 0 AND 30 THEN w.wishlist_id END) AS wishes_1m,
    COUNT(DISTINCT CASE WHEN DATE_DIFF(DATE(w.created_date), DATE(cb.end_date), DAY) BETWEEN 0 AND 90 THEN w.wishlist_id END) AS wishes_3m,
    SUM(CASE WHEN cm.participated_c = TRUE AND DATE_DIFF(DATE(w.created_date), DATE(cb.end_date), DAY) BETWEEN 0 AND 30 THEN 1 ELSE 0 END) AS wishes_1m_has_participated,
    SUM(CASE WHEN cm.participated_c = TRUE AND DATE_DIFF(DATE(w.created_date), DATE(cb.end_date), DAY) BETWEEN 0 AND 90 THEN 1 ELSE 0 END) AS wishes_3m_has_participated,
    -- New: customers (unique accounts) with at least one sale
    COUNT(DISTINCT CASE WHEN DATE_DIFF(DATE(b.purchase_date), DATE(cb.end_date), DAY) BETWEEN 0 AND 30 THEN b.account_c END) AS customers_1m,
    COUNT(DISTINCT CASE WHEN DATE_DIFF(DATE(b.purchase_date), DATE(cb.end_date), DAY) BETWEEN 0 AND 90 THEN b.account_c END) AS customers_3m,

        -- Deduplicated wishlist_activity per member + campaign
    (
        SELECT IFNULL(
        STRING_AGG(
            FORMAT('%s: %s', FORMAT_DATE('%Y-%m-%d', created_date), collection),
            '\n'
        ),
        ''
        )
        FROM (
        SELECT DISTINCT
            w.created_date,
            w.collection_c AS collection
        FROM wishlist w
        WHERE w.account_c = cm.account_id
            AND DATE(w.created_date) BETWEEN DATE(cb.end_date) AND DATE_ADD(DATE(cb.end_date), INTERVAL 3 MONTH)
        )
    ) AS wishlist_activity,

    -- Deduplicated sales_details per member + campaign
    (
        SELECT IFNULL(
        STRING_AGG(
            FORMAT('%s: %s (CHF%.0f)', FORMAT_DATE('%Y-%m-%d', purchase_date),INITCAP(product_reference_c), retail_price_chf_c),
            '\n'
        ),
        ''
        )
        FROM (
        SELECT DISTINCT
            COALESCE(warranty_activation_date_c, purchase_date_c) AS purchase_date,
            retail_price_chf_c,
            product_reference_c
        FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_c_filtered`
        WHERE account_c = cm.account_id
            AND is_watch_c = TRUE
            AND DATE(COALESCE(warranty_activation_date_c, purchase_date_c)) BETWEEN DATE(cb.end_date) AND DATE_ADD(DATE(cb.end_date), INTERVAL 3 MONTH)
        )
    ) AS sales_details


    FROM campaign_base cb
    LEFT JOIN campaign_members cm
    ON cb.subcampaign_id = cm.campaign_id
    LEFT JOIN belonging b
    ON cm.account_id = b.account_c
    AND DATE(b.purchase_date) BETWEEN DATE(cb.end_date) AND DATE_ADD(DATE(cb.end_date), INTERVAL 90 DAY)
    LEFT JOIN wishlist w
    ON cm.account_id = w.account_c
    AND DATE(w.created_date) BETWEEN DATE(cb.end_date) AND DATE_ADD(DATE(cb.end_date), INTERVAL 90 DAY)
    -- LEFT JOIN outreaches o
    -- ON cm.account_id = o.account_id
    -- AND DATE(o.created_date) BETWEEN DATE(cb.end_date) AND DATE_ADD(DATE(cb.end_date), INTERVAL 90 DAY)


    GROUP BY
    cb.campaign_id,
    cb.campaign_name,
    cb.subcampaign_id,
    cb.subcampaign_name,
    cb.boutique,
    cb.territory,
    cb.fallback_territory,
    cb.end_date,
    cm.member_id,
    cm.account_id,
    cm.contact_id,
    cm.client_type,
    cm.macro_segment,
    cm.member_status,
    cm.has_responded,
    cm.participated_c