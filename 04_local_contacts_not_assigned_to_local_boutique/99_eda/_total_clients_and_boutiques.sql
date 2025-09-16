SELECT 
      COUNT(DISTINCT acc.id) AS total_clients, 
      COUNT(DISTINCT bout.name) AS total_boutiques
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` acc
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_primary_boutiques` bout
    ON acc.primary_boutique_c = bout.primary_boutique_c;