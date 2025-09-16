
WITH sellout_max AS (
    SELECT
        account_c,
        MAX(COALESCE(purchase_date_c, warranty_activation_date_c)) AS max_sellout_date
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_c_filtered`
    WHERE is_watch_c = TRUE
    GROUP BY account_c
),

task_count AS (
    SELECT
        task.account_id,
        COUNT(task.id) AS nb_offline_actions
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_task_filtered` task
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_boutiques` ab
        ON task.account_id = ab.id
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_user_with_markets` user
        ON task.owner_id = user.id
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_user_role_filtered` role
        ON task.owner_id = role.id
    WHERE
        task.channel_c = 'Action'
        AND task.type_c IS NOT NULL
        AND (
            (ab.boutique_type_2_c = 'DOS' AND STRPOS(ab.name, 'eBoutique') = 0)
            OR ab.id = '0010X00004snyN3QAI'
        )
        AND (
            STRPOS(role.name, 'Boutique Manager') > 0
            OR STRPOS(role.name, 'Sales Associate') > 0
            OR user.id IN (
                '0050X000007hoVsQAI',
                '0056700000CeROxAAN',
                '0056700000EOFDeAAP'
            )
        )
        AND DATE(task.activity_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
    GROUP BY task.account_id
),

belonging_summary AS (
  SELECT
    account_c,
    MAX(COALESCE(warranty_activation_date_c, purchase_date_c)) AS last_purchase_date,
    COUNT(*) AS nb_purchase,
    SAFE_CAST(SUM(retail_price_chf_c) AS INT64) AS total_purchase_chf
  FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_c_filtered`
  WHERE 
    is_watch_c = TRUE AND
    active_c
  GROUP BY account_c
)

SELECT
    acc.id AS acc_id,
    acc.macro_segment AS acc_macro_segment,
    acc.status_pc AS status,

    CASE
        WHEN
            (acc.email_syntax_quality = 1 AND acc.marketing_consent = 1)
            OR
            (acc.phone_syntax_quality = 1 AND acc.marketing_consent = 1)
        THEN 1
        ELSE 0
    END AS is_contactable_LVMH,

    CASE
        WHEN acc.permission_to_contact_pc THEN 1
        ELSE 0
    END AS is_permission_to_contact_pc,

    acc.billing_country_code AS acc_country_code,
    acc.billing_country AS acc_country,
    acc.billing_state AS acc_state,
    bout.billing_country_code AS bout_country_code,
    bout.billing_country AS bout_country,
    bout.billing_state AS bout_state,
    bout.name AS bout_name,

    CAST(acc.last_dos_purchase_date_c AS DATE) AS acc_last_dos_purchase_date_c,
    CAST(sellout_max.max_sellout_date AS DATE) AS max_sellout_date,

    COALESCE(CAST(acc.last_dos_purchase_date_c AS DATE), CAST(sellout_max.max_sellout_date AS DATE)) AS last_purchase_date_c,

    acc.market AS acc_market,

    NULL AS city_to_bout_distance_km,

    CASE
        WHEN COALESCE(CAST(acc.last_dos_purchase_date_c AS DATE), CAST(sellout_max.max_sellout_date AS DATE)) < DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
             OR COALESCE(CAST(acc.last_dos_purchase_date_c AS DATE), CAST(sellout_max.max_sellout_date AS DATE)) IS NULL
        THEN 1
        ELSE 0
    END AS has_not_purchased_in_12m,

    CASE
        WHEN COALESCE(CAST(acc.last_dos_purchase_date_c AS DATE), CAST(sellout_max.max_sellout_date AS DATE)) < DATE_SUB(CURRENT_DATE(), INTERVAL 18 MONTH)
             OR COALESCE(CAST(acc.last_dos_purchase_date_c AS DATE), CAST(sellout_max.max_sellout_date AS DATE)) IS NULL
        THEN 1
        ELSE 0
    END AS has_not_purchased_in_18m,

    CASE
        WHEN acc.macro_segment NOT IN ('Loyal', 'VIC/VVIC') THEN 1
        ELSE 0
    END AS is_not_a_top_client,

    CASE
        WHEN acc.macro_segment = 'Prospect'
             AND CAST(acc.created_date AS DATE) < DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
        THEN 1
        ELSE 0
    END AS is_a_more_than_6m_old_prospect,

    COALESCE(task_count.nb_offline_actions, 0) AS nb_offline_actions_last_12m,
    bout.market AS bout_market,

    CASE
        WHEN acc.market IN (
            'Central Europe', 'Eastern Europe & Scandinavia', 'France & BeLux',
            'Iberia', 'Italy', 'Switzerland', 'UK'
        ) THEN acc.market
        ELSE NULL
    END AS is_acc_european_market,

    CASE
        WHEN bout.market IN (
            'Central Europe', 'Eastern Europe & Scandinavia', 'France & BeLux',
            'Iberia', 'Italy', 'Switzerland', 'UK'
        ) THEN bout.market
        ELSE NULL
    END AS is_bout_european_market,

    CASE
        WHEN acc.market IN (
            'Central Europe', 'Eastern Europe & Scandinavia', 'France & BeLux',
            'Iberia', 'Italy', 'Switzerland', 'UK'
        )
        AND bout.market IN (
            'Central Europe', 'Eastern Europe & Scandinavia', 'France & BeLux',
            'Iberia', 'Italy', 'Switzerland', 'UK'
        )
        THEN 1
        ELSE 0
    END AS is_acc_and_bout_in_european_market,

    acc.billing_city AS acc_city,
    acc.first_name AS acc_first_name,
    acc.last_name AS acc_last_name,

    -- New fields from belonging_summary
    bs.last_purchase_date,
    bs.nb_purchase,
    bs.total_purchase_chf,
    pos.market AS pos_market

FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` acc

LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_primary_boutiques` bout
    ON acc.primary_boutique_c = bout.primary_boutique_c

LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_external_pos` pos
ON acc.primary_external_pos_c = pos.id

LEFT JOIN sellout_max
    ON acc.id = sellout_max.account_c

LEFT JOIN task_count
    ON acc.id = task_count.account_id

LEFT JOIN belonging_summary bs
    ON acc.id = bs.account_c

WHERE 
    acc.billing_country_code <> bout.billing_country_code AND 
    (pos.market IS NULL OR pos.market!='MEA')