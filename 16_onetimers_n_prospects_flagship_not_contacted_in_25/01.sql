SELECT
    pos.id,
    dos.id,
    COUNT(distinct ap.id)
FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah
  ON ah.account_id = ap.id
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_primary_boutiques` bout
  ON ap.primary_boutique_c = bout.primary_boutique_c
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_external_pos` pos
  ON ap.primary_external_pos_c = pos.id
WHERE 
    dos.id IN (
        "001b000003jJWGQAA4",
        "001b000003H1oNNAAZ",
        "001b0000004DrIYAA0",
        "001b000000lIEApAAO",
        "001b000000XKVhhAAH",
        "0010X000048vFUxQAM",
        "0010X00004soT6mQAE",
        "0010X00004gBM89QAG"
        )
