SELECT
    DATE(ah.photo_date) AS photo_date,
    ah.account_id,
    ah.status AS account_status,
    ah.status_LM AS account_status_LM,
    bout.id AS primary_dos_id,
    bout.market AS primary_dos_market,
    bout.name AS primary_dos_name,
    pos.id AS primary_pos_id,
    pos.market AS primary_pos_market,
    pos.name AS primary_pos_name,
    sa.name AS sa_name,
        CASE
        WHEN (ap.email_syntax_quality = 1 AND ap.marketing_consent = 1)
        OR (ap.phone_syntax_quality = 1 AND ap.marketing_consent = 1)
        THEN True
        ELSE False
        END AS account_contactability,
    ah.market AS account_market,
    ah.life_time_segment AS account_segment,
FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
  ON ah.account_id = ap.id
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_primary_boutiques` bout
ON bout.id = ah.primary_boutique_c
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_external_pos` pos
  ON ap.primary_external_pos_c = pos.id
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_distinct_sa_joined_users` sa
  ON ah.sales_person_new_c = sa.sales_person_new_c