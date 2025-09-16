SELECT
  bout.market as bout_market,
  bout.name as bout_name,
  bout.billing_country_code as bout_billing_country_code,
  bout.billing_state as bout_billing_state,
  acc.billing_country_code as acc_country,
  acc.primary_boutique_c as acc_prim_boutique,
  acc.id AS acc_id
FROM
  `CLIENTS_REPORTING_account_people` acc
LEFT JOIN 
  `CLIENTS_REPORTING_account_primary_boutiques` bout
ON
  acc.primary_boutique_c=bout.primary_boutique_c