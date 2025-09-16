WITH
  list_base AS
  (
  SELECT
    DISTINCT
    bout.name as bout_name,
    cl.id as list_id,
    cl.business_entity_c as business_entity,
    cl.name AS list_name,
    DATE(cl.created_date) AS created_date
  FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_client_list_joined` cl
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_boutiques` bout ON cl.business_entity_c=bout.id
  WHERE
    cl.list_type_c = 'store'
  ),
  client_list_members AS (
    SELECT
      cl.client_c,
      cl.client_list_c,
      ap.id AS account_id,
      ap.macro_segment,
      ap.type AS client_type
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_client_list_joined` cl
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
      ON cl.client_c = ap.id
    WHERE 
      NOT cl.is_deleted AND 
      cl.list_type_c = 'store'
    ),
  belonging AS (
    SELECT
      account_c,
      id AS sale_id,
      COALESCE(warranty_activation_date_c, purchase_date_c) AS purchase_date,
      retail_price_chf_c,
      product_reference_c
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_c_filtered`
    WHERE 
      is_watch_c = TRUE
    ),
    wishlist AS (
      SELECT
          wish.account_c,
          wish.id AS wishlist_id,
          wish.created_date,
          prd.collection_c
      FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_wishlist_c_filtered` wish
      LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_product_2_filtered` prd
          ON wish.product_c = prd.id
    ),
    outreaches AS (
        SELECT
            ap.id AS account_id,
            DATE(tsks.created_date) AS created_date
        FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_task_filtered` tsks
        LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_contact_filtered` ctcs
            ON tsks.who_id = ctcs.id
        LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
            ON ctcs.id = ap.person_contact_id
        LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_user_with_markets` uswm
            ON tsks.owner_id = uswm.id
        LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_contact_user` ctcu
            ON uswm.id = ctcu.salesforce_user_c
        LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_contact_relation_filtered` accrf
            ON ctcu.id = accrf.contact_id
        LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_boutiques` bout
            ON accrf.account_id = bout.id
        LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_user_role_filtered` role
            ON uswm.user_role_id = role.id
        WHERE
            tsks.channel_c IN (
              'Call', 
              'Email', 
              'Kakao', 
              'Line', 
              'SMS', 
              'WeChat', 
              'WhatsApp'
              )
            AND 
              LOWER(tsks.description) NOT LIKE '%bug%' OR 
              LOWER(tsks.description) IS NULL
            AND 
              LOWER(tsks.description) NOT LIKE '%internal%' OR 
              LOWER(tsks.description) IS NULL
            AND (
              (
              bout.boutique_type_2_c = 'DOS' AND 
              LOWER(bout.name) NOT LIKE '%eboutique%' OR 
              LOWER(bout.name) IS NULL 
              )
            OR bout.id = '0010X00004snyN3QAI'
            )
            AND (
              LOWER(role.name) LIKE '%boutique manager%'
              OR LOWER(role.name) LIKE '%sales associate%'
              OR uswm.id IN (
                '0050X000007hoVsQAI', 
                '0056700000CeROxAAN', 
                '0056700000EOFDeAAP'
                )
              )
            AND (
              bout.market != 'Greater China' OR 
              bout.market IS NULL
            )
            AND (
              bout.status_c != 'Inactive' OR 
              bout.status_c IS NULL
              )
        )

SELECT
  cl.bout_name,
  cl.list_name,
  COUNT(distinct clm.account_id) as nb_members,
FROM
  client_list_members clm
LEFT JOIN
  list_base cl
  ON clm.client_list_c = cl.list_id
LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_salesforce.account` bout
  ON bout.id=cl.business_entity
WHERE 
    (
    LOWER(cl.list_name) LIKE '%hpc - high potential clients%'
    OR LOWER(cl.list_name) LIKE '%w&w prospects follow up%' 
    OR LOWER(cl.list_name) LIKE '%local prospects to convert%'
    ) 
  AND
    bout_name IN
      (
      'Hublot Geneva Boutique',
      'Hublot Ginza Boutique',
      'Hublot Las Vegas Forum Boutique',
      'Hublot New York 5th Avenue Boutique',
      'Hublot Paris Vendôme Boutique',
      'Hublot Zurich Boutique'
      )
GROUP BY 1,2
ORDER BY 3 DESC