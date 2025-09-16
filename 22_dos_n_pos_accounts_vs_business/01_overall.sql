-- ================================
-- Transactions YTD to snapshot (2025-01-01 .. 2025-07-01)
-- ================================
WITH tx_2025 AS (
  SELECT
    bel.account_c AS account_id,
    SUM(bel.retail_price_chf_c) AS total_amount_chf
  FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_c_filtered` bel
  WHERE DATE(COALESCE(bel.warranty_activation_date_c, bel.purchase_date_c))
        BETWEEN DATE('2025-01-01') AND DATE('2025-09-01')
  GROUP BY bel.account_c
),


-- ================================
-- Snapshot of accounts (July 1, 2025)
-- ================================
snapshot_accounts AS (
  SELECT
    ap.id AS account_id,
    ah.life_time_segment,
    ah.status,
    ap.gender_pc,
    (
      (IFNULL(ap.email_syntax_quality, 0) = 1 AND IFNULL(ap.marketing_consent, 0) = 1)
      OR
      (IFNULL(ap.phone_syntax_quality, 0) = 1 AND IFNULL(ap.marketing_consent, 0) = 1)
    ) AS is_contactable_lvmh,
    pbout.market AS dos_market,
    pos.market   AS pos_market
  FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
  JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah
    ON ap.id = ah.account_id
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_primary_boutiques` pbout
    ON ah.primary_boutique_c = pbout.id
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_external_pos` pos
    ON ap.primary_external_pos_c = pos.id
  WHERE ah.photo_date = TIMESTAMP("2025-09-01 00:00:00+00")
),


-- ================================
-- Accounts grouped by market (DOS first, then POS, else Unknown)
-- ================================
segmented AS (
  SELECT
    COALESCE(dos_market, pos_market, 'Unknown') AS market,
    life_time_segment,
    COUNT(DISTINCT s.account_id) AS nb_accounts_total,
    COUNT(DISTINCT CASE WHEN s.is_contactable_lvmh THEN s.account_id END) AS nb_accounts_contactable,
    SAFE_DIVIDE(
      COUNT(DISTINCT CASE WHEN s.is_contactable_lvmh THEN s.account_id END),
      COUNT(DISTINCT s.account_id)
    ) AS pct_contactable_in_segment,
    COUNT(DISTINCT CASE WHEN s.status = 'Active'   THEN s.account_id END) AS nb_accounts_status_active,
    COUNT(DISTINCT CASE WHEN s.status = 'Inactive' THEN s.account_id END) AS nb_accounts_status_inactive,
    COUNT(DISTINCT CASE WHEN s.status = 'Sleeping' THEN s.account_id END) AS nb_accounts_status_sleeping,
    COUNT(DISTINCT CASE WHEN s.gender_pc = 'Male' THEN s.account_id END) AS nb_accounts_male,
    SUM(tx.total_amount_chf) AS revenue_2025
  FROM snapshot_accounts s
  LEFT JOIN tx_2025 tx
    ON s.account_id = tx.account_id
  GROUP BY COALESCE(dos_market, pos_market, 'Unknown'), life_time_segment
),


-- ================================
-- Add global totals
-- ================================
global AS (
  SELECT
    'All Markets' AS market,
    life_time_segment,
    SUM(nb_accounts_total) AS nb_accounts_total,
    SAFE_DIVIDE(SUM(nb_accounts_contactable), SUM(nb_accounts_total)) AS pct_contactable_in_segment,
    SUM(nb_accounts_status_active) AS nb_accounts_status_active,
    SUM(nb_accounts_status_inactive) AS nb_accounts_status_inactive,
    SUM(nb_accounts_status_sleeping) AS nb_accounts_status_sleeping,
    SAFE_DIVIDE(SUM(nb_accounts_male), SUM(nb_accounts_total)) AS pct_male_in_segment,
    SUM(revenue_2025) AS revenue_2025
  FROM segmented
  GROUP BY life_time_segment
)


-- ================================
-- Final output
-- ================================
SELECT
  market,
  life_time_segment,
  nb_accounts_total,
  pct_contactable_in_segment,
  nb_accounts_status_active,
  nb_accounts_status_inactive,
  nb_accounts_status_sleeping,
  pct_male_in_segment,
  SAFE_DIVIDE(revenue_2025, SUM(revenue_2025) OVER (PARTITION BY market)) AS share_in_revenue_2025
FROM (
  -- Global totals (already % male)
  SELECT
    market,
    life_time_segment,
    nb_accounts_total,
    pct_contactable_in_segment,
    nb_accounts_status_active,
    nb_accounts_status_inactive,
    nb_accounts_status_sleeping,
    pct_male_in_segment,
    revenue_2025
  FROM global


  UNION ALL


  -- Segmented rows (compute % male here)
  SELECT
    market,
    life_time_segment,
    nb_accounts_total,
    pct_contactable_in_segment,
    nb_accounts_status_active,
    nb_accounts_status_inactive,
    nb_accounts_status_sleeping,
    SAFE_DIVIDE(nb_accounts_male, NULLIF(nb_accounts_total, 0)) AS pct_male_in_segment,
    revenue_2025
  FROM segmented
)
ORDER BY
  market,
  CASE life_time_segment
    WHEN 'VVIC' THEN 1
    WHEN 'VIC' THEN 2
    WHEN 'Loyal' THEN 3
    WHEN 'One Timer' THEN 4
    WHEN 'Prospect' THEN 5
    ELSE 6
  END;




