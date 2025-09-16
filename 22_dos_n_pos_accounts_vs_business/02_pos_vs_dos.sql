WITH tx_until_photo AS (
  SELECT
    bel.account_c AS account_id,
    bel.retail_price_chf_c AS amount_chf
  FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_c_filtered` bel
  WHERE DATE(COALESCE(bel.warranty_activation_date_c, bel.purchase_date_c)) <= DATE('2025-09-01')
),




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
    pbout.market AS dos_market,   -- DOS market (primary boutique)
    pos.market   AS pos_market    -- POS market (external POS)
  FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
  JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah
    ON ap.id = ah.account_id
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_primary_boutiques` pbout
    ON ah.primary_boutique_c = pbout.id
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_external_pos` pos
    ON ap.primary_external_pos_c = pos.id
  WHERE ah.photo_date = TIMESTAMP("2025-09-01 00:00:00+00")
),




-- DOS metrics
dos_metrics AS (
  SELECT
    s.dos_market AS market,
    s.life_time_segment,
    COUNT(DISTINCT s.account_id) AS nb_accounts_dos,
    SUM(t.amount_chf) AS revenue_dos,


    -- new DOS cols
    COUNT(DISTINCT CASE WHEN s.status = 'Active'   THEN s.account_id END) AS nb_accounts_status_active_dos,
    COUNT(DISTINCT CASE WHEN s.status = 'Inactive' THEN s.account_id END) AS nb_accounts_status_inactive_dos,
    COUNT(DISTINCT CASE WHEN s.status = 'Sleeping' THEN s.account_id END) AS nb_accounts_status_sleeping_dos,
    SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN s.is_contactable_lvmh THEN s.account_id END),
                COUNT(DISTINCT s.account_id)) AS pct_contactable_in_segment_dos,
    SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN s.gender_pc = 'Male' THEN s.account_id END),
                COUNT(DISTINCT s.account_id)) AS pct_male_in_segment_dos


  FROM snapshot_accounts s
  LEFT JOIN tx_until_photo t
    ON t.account_id = s.account_id
  WHERE s.dos_market IS NOT NULL
  GROUP BY s.dos_market, s.life_time_segment
),




-- POS metrics
pos_metrics AS (
  SELECT
    s.pos_market AS market,
    s.life_time_segment,
    COUNT(DISTINCT s.account_id) AS nb_accounts_pos,
    SUM(t.amount_chf) AS revenue_pos,


    -- new POS cols
    COUNT(DISTINCT CASE WHEN s.status = 'Active'   THEN s.account_id END) AS nb_accounts_status_active_pos,
    COUNT(DISTINCT CASE WHEN s.status = 'Inactive' THEN s.account_id END) AS nb_accounts_status_inactive_pos,
    COUNT(DISTINCT CASE WHEN s.status = 'Sleeping' THEN s.account_id END) AS nb_accounts_status_sleeping_pos,
    SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN s.is_contactable_lvmh THEN s.account_id END),
                COUNT(DISTINCT s.account_id)) AS pct_contactable_in_segment_pos,
    SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN s.gender_pc = 'Male' THEN s.account_id END),
                COUNT(DISTINCT s.account_id)) AS pct_male_in_segment_pos


  FROM snapshot_accounts s
  LEFT JOIN tx_until_photo t
    ON t.account_id = s.account_id
  WHERE s.pos_market IS NOT NULL
  GROUP BY s.pos_market, s.life_time_segment
)




-- Final consolidated view (DOS + POS only)
SELECT
  COALESCE(d.market, p.market) AS market,
  COALESCE(d.life_time_segment, p.life_time_segment) AS life_time_segment,


  -- DOS
  d.nb_accounts_dos,
  SAFE_DIVIDE(d.nb_accounts_dos, SUM(d.nb_accounts_dos) OVER (PARTITION BY d.market)) AS share_in_accounts_dos,
  SAFE_DIVIDE(d.revenue_dos, SUM(d.revenue_dos) OVER (PARTITION BY d.market)) AS share_in_revenue_dos,
  d.nb_accounts_status_active_dos,
  d.nb_accounts_status_inactive_dos,
  d.nb_accounts_status_sleeping_dos,
  d.pct_contactable_in_segment_dos,
  d.pct_male_in_segment_dos,


  -- POS
  p.nb_accounts_pos,
  SAFE_DIVIDE(p.nb_accounts_pos, SUM(p.nb_accounts_pos) OVER (PARTITION BY p.market)) AS share_in_accounts_pos,
  SAFE_DIVIDE(p.revenue_pos, SUM(p.revenue_pos) OVER (PARTITION BY p.market)) AS share_in_revenue_pos,
  p.nb_accounts_status_active_pos,
  p.nb_accounts_status_inactive_pos,
  p.nb_accounts_status_sleeping_pos,
  p.pct_contactable_in_segment_pos,
  p.pct_male_in_segment_pos


FROM dos_metrics d
FULL OUTER JOIN pos_metrics p
  ON d.market = p.market
 AND d.life_time_segment = p.life_time_segment
ORDER BY
  market,
  CASE COALESCE(d.life_time_segment, p.life_time_segment)
    WHEN 'VVIC' THEN 1
    WHEN 'VIC' THEN 2
    WHEN 'Loyal' THEN 3
    WHEN 'One Timer' THEN 4
    WHEN 'Prospect' THEN 5
    ELSE 6
  END;