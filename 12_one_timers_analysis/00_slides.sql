-- ONE TIMER - HISTORIQUE​

-- Top 5 billing countries​
SELECT
billing_country,
COUNT(distinct id) as distinct_ids
FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
WHERE macro_segment = 'One Timer'
GROUP BY 1
ORDER BY 2 DESC

-- Gender Split: dashboard

-- Average age 45 y.o  
SELECT
avg(age_of_the_client_c)
FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
WHERE macro_segment = 'One Timer'

-- Average order value
WITH 
one_timers_all_time AS 
    (
    SELECT
        DISTINCT ap.id
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap 
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah
        ON ah.account_id = ap.id
    WHERE
        ah.photo_date = "2025-05-01 00:00:00 UTC" AND
        ah.life_time_segment = 'One Timer' AND 
        ap.primary_boutique_c IS NOT NULL
    )
SELECT
avg(avg_retail_price_chf_c)
FROM
  (
  SELECT
      bec.id,
      avg(crbf.retail_price_chf_c) as avg_retail_price_chf_c
  FROM one_timers_all_time bec
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_c_filtered` crbf
  ON  bec.id=crbf.account_c
  GROUP BY 1
  )

-- Avg nb of straps
SELECT 
*
FROM `hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_product_2_filtered`
WHERE 
category_c = 'Strap'

WITH 
one_timers_all_time AS 
    (
    SELECT
        DISTINCT ap.id
        -- DISTINCT ap.id
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap 
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah
        ON ah.account_id = ap.id
    WHERE
        ah.photo_date = "2025-05-01 00:00:00 UTC" AND
        ah.life_time_segment = 'One Timer' AND 
        ap.primary_boutique_c IS NOT NULL
    )

SELECT
-- crbf.*
-- crbf.is_watch_c,
-- crbf.product_code_c,
-- count(distinct bec.id)
-- avg(avg_retail_price_chf_c)

pf.category_c,
count(*)
-- count(*)
  FROM one_timers_all_time bec
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_c_prepared` crbf ON bec.id=crbf.account_c
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_sellout_with_valo` sell ON crbf.serial_number_c = sell.serial_c
  LEFT JOIN `hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_product_2_filtered` pf ON sell.product_c = pf.id
WHERE  pf.category_c != 'Watch' or pf.category_c IS NULL
GROUP BY 1
ORDER BY 2 DESC
-- Clients since 5,1 years on average​
SELECT
avg(date_diff(current_date(),DATE(first_purchase_date_c),YEAR))
FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
WHERE 
macro_segment = 'One Timer'

-- product collections
WITH ordered_data AS (
    SELECT
        bel.account_c AS account_id,
        COALESCE(bel.warranty_activation_date_c, bel.purchase_date_c) AS purchase_date,
        bel.retail_price_chf_c,
        bel.product_reference_c,
        ROW_NUMBER() OVER (
            PARTITION BY bel.account_c 
            ORDER BY COALESCE(bel.warranty_activation_date_c, bel.purchase_date_c) ASC
        ) AS rn
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_c_filtered` bel
        ON ap.id = bel.account_c
    WHERE bel.is_watch_c
),
fp_details AS
(
SELECT
    account_id,
    purchase_date AS first_purchase_date,
    retail_price_chf_c AS first_retail_price_chf,
    product_reference_c AS first_product_reference
FROM ordered_data
WHERE rn = 1
)

SELECT
first_product_reference,
count(distinct account_id)
from fp_details
group by 1
order by 2 desc

-- Avg Order Value​


-- One Timers DOS

    -- Became One Timers
WITH 
one_timers_dos_prev_period AS 
    (
    SELECT
        DISTINCT ap.id
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap 
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah
    ON ah.account_id = ap.id
    WHERE 
        ah.photo_date = '2024-01-01 00:00:00 UTC' AND
        ah.life_time_segment = 'One Timer' AND 
        ap.primary_boutique_c IS NOT NULL
    )
SELECT
    COUNT(DISTINCT ap.id)
FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap 
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah
    ON ah.account_id = ap.id
LEFT JOIN one_timers_dos_prev_period USING(id)
WHERE
    one_timers_dos_prev_period.id IS NULL AND
    ah.photo_date = "2025-01-01 00:00:00 UTC" AND
    DATE(ap.created_date) < DATE(2025,1,1) AND
    DATE(ap.created_date) >= DATE(2024,1,1) AND
    ah.life_time_segment = 'One Timer' AND 
    ap.primary_boutique_c IS NOT NULL
    -- Total One Timers
SELECT
COUNT(DISTINCT ap.id)
FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap 
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah
ON ah.account_id = ap.id
WHERE 
ah.photo_date = '2025-06-01 00:00:00 UTC' AND
ah.life_time_segment = 'One Timer' AND 
ap.primary_boutique_c IS NOT NULL

    -- Total sellout value
WITH 
one_timers_dos_prev_period AS 
    (
    SELECT
        DISTINCT ap.id
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap 
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah
    ON ah.account_id = ap.id
    WHERE 
        ah.photo_date = '2024-01-01 00:00:00 UTC' AND
        ah.life_time_segment = 'One Timer' AND 
        ap.primary_boutique_c IS NOT NULL
    ),
became_one_timers_during_p AS 
    (
    SELECT
        DISTINCT ap.id
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap 
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah
        ON ah.account_id = ap.id
    LEFT JOIN one_timers_dos_prev_period USING(id)
    WHERE
        one_timers_dos_prev_period.id IS NULL AND
        ah.photo_date = "2025-01-01 00:00:00 UTC" AND
        DATE(ap.created_date) < DATE(2025,1,1) AND
        DATE(ap.created_date) >= DATE(2024,1,1) AND
        ah.life_time_segment = 'One Timer' AND 
        ap.primary_boutique_c IS NOT NULL
        )
SELECT
sum(avg_retail_price_chf_c)
FROM
  (
  SELECT
      bec.id,
      avg(crbf.retail_price_chf_c) as avg_retail_price_chf_c
  FROM became_one_timers_during_p bec
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_c_filtered` crbf
  ON  bec.id=crbf.account_c
  GROUP BY 1
  )

    -- Share in Total Sellout Value​
        -- cf client cockpit (tab segment_count_vs_revenue)

CLIENTS_REPORTING_belonging_c_filtered[retail_price_chf_c]

    -- Avg Order Value​
WITH 
one_timers_dos_prev_period AS 
    (
    SELECT
        DISTINCT ap.id
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap 
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah
    ON ah.account_id = ap.id
    WHERE 
        ah.photo_date = '2024-01-01 00:00:00 UTC' AND
        ah.life_time_segment = 'One Timer' AND 
        ap.primary_boutique_c IS NOT NULL
    ),
became_one_timers_during_p AS 
    (
    SELECT
        DISTINCT ap.id
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap 
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah
        ON ah.account_id = ap.id
    LEFT JOIN one_timers_dos_prev_period USING(id)
    WHERE
        one_timers_dos_prev_period.id IS NULL AND
        ah.photo_date = "2025-01-01 00:00:00 UTC" AND
        DATE(ap.created_date) < DATE(2025,1,1) AND
        DATE(ap.created_date) >= DATE(2024,1,1) AND
        ah.life_time_segment = 'One Timer' AND 
        ap.primary_boutique_c IS NOT NULL
    )
SELECT
avg(avg_retail_price_chf_c)
FROM
  (
  SELECT
      bec.id,
      avg(crbf.retail_price_chf_c) as avg_retail_price_chf_c
  FROM became_one_timers_during_p bec
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_c_filtered` crbf
  ON  bec.id=crbf.account_c
  GROUP BY 1
  )

    -- Gender Split (M/F)​
        -- cf client cockpit

    -- Avg Age​
WITH 
one_timers_dos_prev_period AS 
    (
    SELECT
        DISTINCT ap.id
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap 
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah
    ON ah.account_id = ap.id
    WHERE 
        ah.photo_date = '2025-01-01 00:00:00 UTC' AND
        ah.life_time_segment = 'One Timer' AND 
        ap.primary_boutique_c IS NOT NULL
    )
SELECT
    avg(ap.age_of_the_client_c)
FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap 
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah
    ON ah.account_id = ap.id
LEFT JOIN one_timers_dos_prev_period USING(id)
WHERE
    one_timers_dos_prev_period.id IS NULL AND
    ah.photo_date = "2025-05-01 00:00:00 UTC" AND
    ah.life_time_segment = 'One Timer' AND 
    ap.primary_boutique_c IS NOT NULL

    -- Time to First Purchase (months)​
WITH 
one_timers_dos_prev_period AS 
    (
    SELECT
        DISTINCT ap.id
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap 
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah
    ON ah.account_id = ap.id
    WHERE 
        ah.photo_date = '2025-01-01 00:00:00 UTC' AND
        ah.life_time_segment = 'One Timer' AND 
        ap.primary_boutique_c IS NOT NULL
    )
SELECT
    avg(DATE_DIFF(DATE(first_dos_purchase_date_c),DATE(created_date), DAY))
FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap 
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah
    ON ah.account_id = ap.id
LEFT JOIN one_timers_dos_prev_period USING(id)
WHERE
    one_timers_dos_prev_period.id IS NULL AND
    ah.photo_date = "2025-05-01 00:00:00 UTC" AND
    ah.life_time_segment = 'One Timer' AND 
    ap.primary_boutique_c IS NOT NULL

-- REPURCHASE RATE & TIMING – DOS DATA​
    -- ...converted to Loyal/VIC/VVIC clients ​in 2024
WITH 
one_timers_dos_prev_period AS 
    (
    SELECT
        DISTINCT ap.id
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap 
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah ON ah.account_id = ap.id
    WHERE 
        ah.photo_date = '2024-01-01 00:00:00 UTC' AND
        ah.life_time_segment = 'One Timer' AND 
        ap.primary_boutique_c IS NOT NULL
    )
SELECT
    COUNT(DISTINCT ap.id)
FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap 
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah
    ON ah.account_id = ap.id
LEFT JOIN one_timers_dos_prev_period USING(id)
WHERE
    one_timers_dos_prev_period.id IS NOT NULL AND
    ah.photo_date = "2025-01-01 00:00:00 UTC" AND
    ah.life_time_segment != 'One Timer' AND 
    ap.primary_boutique_c IS NOT NULL

-- repurchase rate for VICs
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
    WHERE 
        bel.is_watch_c AND
        ap.macro_segment IN ('VIC','VVIC')

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
avg(second_p_to_third_p_days)
FROM
(
SELECT
    id,
    DATE_DIFF(first_purchase, created_date, DAY) AS created_to_first_p_days,
    DATE_DIFF(second_purchase, first_purchase, DAY) AS first_p_to_second_p_days,
    DATE_DIFF(third_purchase, second_purchase, DAY) AS second_p_to_third_p_days
FROM pivoted
WHERE first_purchase IS NOT NULL  -- ensures at least two purchases
)

-- ONE TIMER DEEP DIVE ANALYSIS
-- Funnel
SELECT
    COUNT(DISTINCT ap.id)
FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap 
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah
ON ah.account_id = ap.id
WHERE 
    ah.photo_date = '2025-06-01 00:00:00 UTC' AND
    ah.life_time_segment = 'One Timer' AND
    ap.primary_boutique_c IS NOT NULL AND
    ah.status = 'Active' AND
    ap.permission_to_contact_pc AND
    ap.number_of_wishlists_c>0 AND
    ap.total_own_price_c>=50000

-- interactions before second purchase (one timers who became repeaters in 2024)
-- Comparison of metrics across two populations: turned_repeaters_in_2024 vs not


WITH
  -- Base population: One timers in 2024
  one_timers_dos_prev_period AS (
    SELECT DISTINCT ap.id
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah
      ON ah.account_id = ap.id
    WHERE ah.photo_date = '2024-01-01 00:00:00 UTC'
      AND ah.life_time_segment = 'One Timer'
      AND ap.primary_boutique_c IS NOT NULL
  ),


  -- Last purchase in 2024 per account
  last_purchase AS (
    SELECT
      account_c AS account_id,
      MAX(COALESCE(warranty_activation_date_c, purchase_date_c)) AS last_purchase_date
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_c_filtered`
    WHERE EXTRACT(YEAR FROM COALESCE(warranty_activation_date_c, purchase_date_c)) = 2024
    GROUP BY account_c
  ),


  -- Campaign interaction metrics before last purchase
  campaign_metrics AS (
    SELECT
      ap.id AS account_id,
      COUNT(DISTINCT cm.campaign_id) AS nb_campaigns,
      COUNT(DISTINCT CASE WHEN cm.participated_c THEN cm.campaign_id END) AS nb_campaigns_participated,
      COUNT(DISTINCT CASE WHEN cm.status = 'Opened' THEN cm.campaign_id END) AS nb_campaigns_opened,
      COUNT(DISTINCT CASE WHEN cm.status = 'Clicked' THEN cm.campaign_id END) AS nb_campaigns_clicked
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_salesforce.campaign_member` cm
    JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
      ON cm.contact_id = ap.person_contact_id
    JOIN last_purchase lp ON ap.id = lp.account_id
    WHERE NOT cm.is_deleted
      AND DATE(cm.created_date) < DATE(lp.last_purchase_date)
    GROUP BY ap.id
  ),


  -- Wishlist metrics before last purchase
  wishlist_metrics AS (
    SELECT
      wish.account_c AS account_id,
      COUNT(DISTINCT wish.id) AS nb_wishes_added
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_wishlist_c_filtered` wish
    JOIN last_purchase lp ON wish.account_c = lp.account_id
    WHERE DATE(wish.created_date) < DATE(lp.last_purchase_date)
    GROUP BY wish.account_c
  ),


  -- Belonging counts in 2024
  belonging_metrics AS (
    SELECT
      account_c AS account_id,
      COUNTIF(is_watch_c = TRUE) AS nb_belonging_watches_2024,
      COUNTIF(is_watch_c = FALSE) AS nb_belonging_not_watches_2024
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_c_filtered`
    WHERE EXTRACT(YEAR FROM COALESCE(warranty_activation_date_c, purchase_date_c)) = 2024
    GROUP BY account_c
  ),


  -- Outreach activity before last purchase
  outreach_metrics AS (
    SELECT
      o.account_id,
      COUNT(*) AS nb_outreaches_received
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_task_filtered` o
    JOIN last_purchase lp ON o.account_id = lp.account_id
    WHERE DATE(o.created_date) < DATE(lp.last_purchase_date)
    GROUP BY o.account_id
  ),


  full_population AS (
    SELECT
      otp.id AS account_id,
      CASE
        WHEN ah.photo_date = '2025-01-01 00:00:00 UTC' AND ah.life_time_segment != 'One Timer'
        THEN TRUE ELSE FALSE
      END AS turned_repeaters_in_2024,
      cmx.nb_campaigns,
      cmx.nb_campaigns_participated,
      cmx.nb_campaigns_opened,
      cmx.nb_campaigns_clicked,
      wm.nb_wishes_added,
      bm.nb_belonging_watches_2024,
      bm.nb_belonging_not_watches_2024,
      om.nb_outreaches_received
    FROM one_timers_dos_prev_period otp
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
      ON otp.id = ap.id
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah
      ON ah.account_id = ap.id AND ah.photo_date = '2025-01-01 00:00:00 UTC'
    LEFT JOIN last_purchase lp ON ap.id = lp.account_id
    LEFT JOIN campaign_metrics cmx ON ap.id = cmx.account_id
    LEFT JOIN wishlist_metrics wm ON ap.id = wm.account_id
    LEFT JOIN belonging_metrics bm ON ap.id = bm.account_id
    LEFT JOIN outreach_metrics om ON ap.id = om.account_id
  )


SELECT
  turned_repeaters_in_2024,
  COUNT(*) AS nb_accounts,
  AVG(nb_campaigns) AS avg_nb_campaigns,
  AVG(nb_campaigns_participated) AS avg_nb_campaigns_participated,
  AVG(nb_campaigns_opened) AS avg_nb_campaigns_opened,
  AVG(nb_campaigns_clicked) AS avg_nb_campaigns_clicked,
  AVG(nb_wishes_added) AS avg_nb_wishes_added,
  AVG(nb_belonging_watches_2024) AS avg_nb_belonging_watches_2024,
  AVG(nb_belonging_not_watches_2024) AS avg_nb_belonging_not_watches_2024,
  AVG(nb_outreaches_received) AS avg_nb_outreaches_received
FROM full_population
GROUP BY turned_repeaters_in_2024




