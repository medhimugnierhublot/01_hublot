SELECT
  COUNT(DISTINCT ah.account_id)
FROM
  `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
LEFT JOIN  
  `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah
  ON ap.id = ah.account_id
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_event_mc_sent_date_joined` ev
  ON ap.person_contact_id = ev.subscriber_key
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_contact_filtered` cf
  ON ap.person_contact_id = cf.id
WHERE


-- EXCLUDING ACCOUNTS NOT IN ACCOUNTS_HISTORY (current month) > just to find same number than on CRM_DASHBOARDS cockpit
  ah.account_id IS NOT NULL


-- LVMH CONTACTABLE (is_contactable_lvmh)

  -- is_contactable_lvmh condition: marketing consent must be given
  AND ap.marketing_consent = 1


  -- is_contactable_lvmh condition: at least one form of valid contact (email or phone)
  AND (
    ap.email_syntax_quality = 1 OR
    ap.phone_syntax_quality = 1
  )

-- NEWLETTER CONTACTABLE (is_newsletter_contactable)

  -- -- is_newsletter_contactable: must have an email
  AND ap.person_email IS NOT NULL


  -- -- is_newsletter_contactable: has not opted out of email OR is null
  AND (
    NOT ap.person_has_opted_out_of_email 
    OR ap.person_has_opted_out_of_email IS NULL
    )


  -- is_newsletter_contactable: explicitly opted in to newsletter
  AND ap.newsletter_opt_in_c


  -- is_newsletter_contactable: has not indicated they don't want to be contacted
  AND (
    NOT ap.do_not_wish_to_be_contacted_c 
    OR ap.do_not_wish_to_be_contacted_c IS NULL
    )


  -- is_newsletter_contactable: email has not bounced (from related contact record)
  AND (
    NOT cf.is_email_bounced 
    OR cf.is_email_bounced IS NULL)


  -- is_newsletter_contactable: no recent customer journey block
  AND (
    NOT (
      DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S%Ez', ap.last_entry_in_customer_journey_c)) > CURRENT_DATE()
    )
    OR ap.last_entry_in_customer_journey_c IS NULL
  )

  -- is_newsletter_contactable: not excluded by country restriction
AND 
  (
  ap.person_mailing_country_code <> 'CN' 
  OR ap.person_mailing_country_code is NULL
  )

  -- is_newsletter_contactable: no reason recorded for not sending
  AND ap.not_sent_reason_first IS NULL