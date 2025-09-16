WITH purchases_ranked AS (
    SELECT
        ap.id,
        ap.created_date,
        COALESCE(bel.warranty_activation_date_c, bel.purchase_date_c) AS purchase_date,
        ROW_NUMBER() OVER (
            PARTITION BY ap.id 
            ORDER BY COALESCE(bel.warranty_activation_date_c, bel.purchase_date_c) ASC
        ) AS rn
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_c_filtered` bel
        ON ap.id = bel.account_c
    WHERE bel.is_watch_c
),
pivoted AS (
    SELECT
        id,
        MAX(CASE WHEN rn = 1 THEN purchase_date END) AS first_purchase,
        MAX(CASE WHEN rn = 2 THEN purchase_date END) AS second_purchase,
        MAX(CASE WHEN rn = 3 THEN purchase_date END) AS third_purchase,
        MAX(CASE WHEN rn = 1 THEN created_date END) AS created_date  -- created_date is only relevant for first purchase
    FROM purchases_ranked
    GROUP BY id
)

SELECT
    id,
    DATE_DIFF(first_purchase, created_date, DAY) AS created_to_first_p_days,
    DATE_DIFF(second_purchase, first_purchase, DAY) AS first_p_to_second_p_days,
    DATE_DIFF(third_purchase, second_purchase, DAY) AS second_p_to_third_p_days
FROM pivoted
WHERE first_purchase IS NOT NULL  -- ensures at least two purchases


-- avg(first_p_to_second_p_days):550
-- avg(second_p_to_third_p_days):380