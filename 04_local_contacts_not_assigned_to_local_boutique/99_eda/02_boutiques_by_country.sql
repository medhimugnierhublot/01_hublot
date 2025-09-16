SELECT 
      bout.billing_country_code AS country, 
      COUNT(DISTINCT bout.name) AS num_boutiques
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_primary_boutiques` bout
    GROUP BY bout.billing_country_code
    ORDER BY num_boutiques DESC;