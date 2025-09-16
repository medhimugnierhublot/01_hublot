WITH
belonging AS (
  SELECT
    b.account_c,
    b.id AS sale_id,
    COALESCE(b.warranty_activation_date_c, b.purchase_date_c) AS purchase_date,
    b.retail_price_chf_c,
    b.product_reference_c,
    b.active_c,
    ab.name,
    ab.boutique_name_c,
    ab.billing_country AS boutique_country
  FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_c_filtered` b
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_boutiques` ab
    ON b.boutique_of_purchase_c = ab.id
  WHERE is_watch_c = TRUE
),


boutique_stats AS (
  SELECT
    account_c,
    boutique_name_c,
    boutique_country,
    COUNT(*) AS nb_belongings,
    SUM(retail_price_chf_c) AS portfolio_chf
  FROM belonging
  GROUP BY account_c, boutique_name_c, boutique_country
),


boutique_summary AS (
  SELECT
    account_c,
    ARRAY_TO_STRING(
      ARRAY_AGG(
        FORMAT('%s (%s): %d – CHF %.2f',
          boutique_name_c,
          boutique_country,
          nb_belongings,
          portfolio_chf
        )
      ), '; '
    ) AS unique_boutiques
  FROM boutique_stats
  GROUP BY account_c
)


SELECT
  ap.id,
  ap.name,
  ah.life_time_segment,
  ah.status,
  ap.permission_to_contact_pc,
  ap.billing_country,
  ap.billing_state,
  ap.billing_city,
  bout.name AS primary_dos_name,
  bout.billing_country AS primary_dos_country,
  pos.name AS primary_pos_name,
  pos.billing_country AS primary_pos_country,
  sa.name AS sa_name,
  COUNT(DISTINCT b.sale_id) AS total_belongings,
  SUM(b.retail_price_chf_c) AS total_portfolio_chf,
  COUNT(DISTINCT IF(b.active_c = TRUE, b.sale_id, NULL)) AS active_belongings,
  SUM(IF(b.active_c = TRUE, b.retail_price_chf_c, 0)) AS active_portfolio_chf,
  bs.unique_boutiques


FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah
  ON ah.account_id = ap.id
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_primary_boutiques` bout
  ON ap.primary_boutique_c = bout.primary_boutique_c
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_external_pos` pos
  ON ap.primary_external_pos_c = pos.id
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_distinct_sa_joined_users` sa
  ON ah.sales_person_new_c = sa.sales_person_new_c
LEFT JOIN belonging b
  ON ap.id = b.account_c
LEFT JOIN boutique_summary bs
  ON ap.id = bs.account_c


WHERE
  ah.photo_date = '2025-06-01 00:00:00 UTC'
  AND ap.billing_country_code <> bout.billing_country_code
  AND ah.life_time_segment IN ('Loyal', 'VIC', 'VVIC')
  -- AND bout.name IN 
  --     (
    --   'Hublot Geneva Boutique'
    --   'Hublot Ginza Boutique'
    --   'Hublot Las Vegas Forum Boutique'
    --   'Hublot New York 5th Avenue Boutique'
    --   'Hublot Paris Vendôme Boutique'
  --     'Hublot Zurich Boutique'
  --     )
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13, bs.unique_boutiques