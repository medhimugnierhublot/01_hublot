SELECT 
      acc.segment, 
      COUNT(acc.id) AS num_clients
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` acc
    WHERE acc.segment IN ('Inactive', 'Sleeping', 'One Timer')
    GROUP BY acc.segment
    ORDER BY num_clients DESC;