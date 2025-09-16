SELECT 
      acc.billing_country_code AS country, 
      COUNT(acc.id) AS num_clients
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` acc
    GROUP BY acc.billing_country_code
    ORDER BY num_clients DESC;