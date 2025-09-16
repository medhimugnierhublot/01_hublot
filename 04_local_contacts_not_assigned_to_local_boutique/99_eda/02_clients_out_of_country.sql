SELECT 
      acc.billing_country_code AS acc_country,
      bout.billing_country_code AS bout_country,
      COUNT(acc.id) AS num_clients
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` acc
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_primary_boutiques` bout
    ON acc.primary_boutique_c = bout.primary_boutique_c
    WHERE acc.billing_country_code <> bout.billing_country_code
    GROUP BY acc.billing_country_code, bout.billing_country_code
    ORDER BY num_clients DESC;