SELECT
  FORMAT_DATE('%Y-%m', DATE(ap.created_date)) AS created_month,
  DATE(ah.photo_date) AS photo_date,
  bout.market as primary_dos_market,
  bout.name as primary_dos_name,
  bout.y_2_store_id_c AS store_id,
  pos.market AS primary_pos_market,
  pos.name AS primary_pos_name,
  sa.name AS sa_name,
  uwm.is_active,


    CASE
        WHEN (ap.email_syntax_quality = 1 AND ap.marketing_consent = 1)
        OR (ap.phone_syntax_quality = 1 AND ap.marketing_consent = 1)
        THEN TRUE ELSE FALSE
    END AS account_contactability,


    ah.market AS account_market,
    ah.life_time_segment AS account_segment,
    ah.status AS account_status,



  COUNT(DISTINCT IF(
    DATE(ap.created_date) BETWEEN DATE_TRUNC(DATE_SUB(DATE(ah.photo_date), INTERVAL 1 MONTH), MONTH)
                             AND LAST_DAY(DATE_SUB(DATE(ah.photo_date), INTERVAL 1 MONTH)),
    ah.account_id, NULL
  )) AS created_accounts,


  COUNT(DISTINCT IF(
    ah.life_time_segment = 'Prospect'
    AND DATE(ap.created_date) BETWEEN DATE_TRUNC(DATE_SUB(DATE(ah.photo_date), INTERVAL 1 MONTH), MONTH)
                                 AND LAST_DAY(DATE_SUB(DATE(ah.photo_date), INTERVAL 1 MONTH)),
    ah.account_id, NULL
  )) AS created_prospects,


  COUNT(DISTINCT IF(
    ah.life_time_segment = 'One Timer'
    AND DATE(ap.created_date) BETWEEN DATE_TRUNC(DATE_SUB(DATE(ah.photo_date), INTERVAL 1 MONTH), MONTH)
                                 AND LAST_DAY(DATE_SUB(DATE(ah.photo_date), INTERVAL 1 MONTH)),
    ah.account_id, NULL
  )) AS created_one_timers,


  COUNT(DISTINCT IF(
    ah.life_time_segment = 'Loyal'
    AND DATE(ap.created_date) BETWEEN DATE_TRUNC(DATE_SUB(DATE(ah.photo_date), INTERVAL 1 MONTH), MONTH)
                                 AND LAST_DAY(DATE_SUB(DATE(ah.photo_date), INTERVAL 1 MONTH)),
    ah.account_id, NULL
  )) AS created_loyal,


  COUNT(DISTINCT IF(
    ah.life_time_segment = 'VIC'
    AND DATE(ap.created_date) BETWEEN DATE_TRUNC(DATE_SUB(DATE(ah.photo_date), INTERVAL 1 MONTH), MONTH)
                                 AND LAST_DAY(DATE_SUB(DATE(ah.photo_date), INTERVAL 1 MONTH)),
    ah.account_id, NULL
  )) AS created_vics,


  COUNT(DISTINCT IF(
    ah.life_time_segment = 'VVIC'
    AND DATE(ap.created_date) BETWEEN DATE_TRUNC(DATE_SUB(DATE(ah.photo_date), INTERVAL 1 MONTH), MONTH)
                                 AND LAST_DAY(DATE_SUB(DATE(ah.photo_date), INTERVAL 1 MONTH)),
    ah.account_id, NULL
  )) AS created_vvics,

      COUNT(DISTINCT IF(
        ah.billing_country_code = bout.billing_country_code 
        AND DATE(ap.created_date) BETWEEN DATE_TRUNC(DATE_SUB(DATE(ah.photo_date), INTERVAL 1 MONTH), MONTH)
            AND LAST_DAY(DATE_SUB(DATE(ah.photo_date), INTERVAL 1 MONTH)),
        ah.account_id,NULL
  )) AS created_locals,

      COUNT(DISTINCT IF(
        ah.is_hublotista_v_2_c = 'Yes' 
        AND DATE(ap.created_date) BETWEEN DATE_TRUNC(DATE_SUB(DATE(ah.photo_date), INTERVAL 1 MONTH), MONTH)
            AND LAST_DAY(DATE_SUB(DATE(ah.photo_date), INTERVAL 1 MONTH)),
        ah.account_id,NULL
  )) AS created_hublotistas,

    COUNT(DISTINCT IF(
      (
      (ap.email_syntax_quality = 1 AND ap.marketing_consent = 1)
      OR (ap.phone_syntax_quality = 1 AND ap.marketing_consent = 1)
      )        
      AND DATE(ap.created_date) BETWEEN DATE_TRUNC(DATE_SUB(DATE(ah.photo_date), INTERVAL 1 MONTH), MONTH)
        AND LAST_DAY(DATE_SUB(DATE(ah.photo_date), INTERVAL 1 MONTH)),
        ah.account_id,NULL
  )) AS created_lvmh_contactable

FROM
  `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah
LEFT JOIN
  `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
  ON
    ah.account_id = ap.id
LEFT JOIN 
`hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_primary_boutiques` bout
  ON 
  bout.id = ah.primary_boutique_c
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_external_pos` pos
ON ap.primary_external_pos_c = pos.id
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_distinct_sa_joined_users` sa
  ON ah.sales_person_new_c = sa.sales_person_new_c
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_user_with_markets` uwm
    ON sa.sales_person_new_c = uwm.id
GROUP BY
1,2,3,4,5,6,7,8,9,10,11,12