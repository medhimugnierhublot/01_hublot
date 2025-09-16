SELECT
  COUNT(DISTINCT ah.account_id)
FROM
  `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah
LEFT JOIN  
  `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
  ON ah.account_id = ap.id 
-- LEFT JOIN bounced_accounts ba 
--   ON ap.person_contact_id=ba.subscriber_key
-- LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_event_mc_sent_date_joined` ev
--   ON ap.person_contact_id = ev.subscriber_key
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_contact_filtered` cf
  ON ap.person_contact_id = cf.id
WHERE
-- LVMH CONTACTABLE (is_contactable_lvmh)

  -- is_contactable_lvmh condition: marketing consent 
  ah.opt_in_c

  -- is_contactable_lvmh condition: at least one form of valid contact (email or phone)
  AND (
      ah.email_syntax_quality= 1 OR
      ah.phone_syntax_quality= 1
  )

-- NEWLETTER CONTACTABLE (is_newsletter_contactable)

  -- -- is_newsletter_contactable: must have an email
  AND ah.person_email IS NOT NULL

  -- -- is_newsletter_contactable: has not opted out of email OR is null
  AND (
    NOT ah.person_has_opted_out_of_email 
    OR ah.person_has_opted_out_of_email IS NULL
    )

  -- is_newsletter_contactable: explicitly opted in to newsletter
  AND ah.newsletter_opt_in_c

  -- is_newsletter_contactable: has not indicated they don't want to be contacted
  AND (
    NOT ah.do_not_wish_to_be_contacted_c 
    OR ah.do_not_wish_to_be_contacted_c IS NULL
    )

  -- is_newsletter_contactable: not excluded by country restriction
AND 
  (
  ah.billing_country_code <> 'CN' 
  OR ah.billing_country_code is NULL
  )

  -- is_newsletter_contactable: email has not bounced (from related contact record)
  AND (
    NOT cf.is_email_bounced 
    OR cf.is_email_bounced IS NULL)

AND
  ah.photo_date = 	'2025-05-01 00:00:00 UTC'