SELECT 
      acc.billing_state AS acc_state,
      bout.billing_state AS bout_state,
      COUNT(acc.id) AS num_clients
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` acc
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_primary_boutiques` bout
    ON acc.primary_boutique_c = bout.primary_boutique_c
    WHERE acc.billing_country_code = 'US' 
    AND bout.billing_country_code = 'US'
    AND acc.billing_state <> bout.billing_state
    GROUP BY acc.billing_state, bout.billing_state
    ORDER BY num_clients DESC;