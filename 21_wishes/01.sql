WITH
-- Step 1: Wishlist with product info and computed wish_class
wishlist AS (
  SELECT
    wish.account_c,
    wish.id AS wish_wishlist_id,
    DATE(wish.created_date) AS wish_created_date,  -- cast to DATE
    wish.name AS wish_name,
    wish.product_c AS wish_product_c,
    CASE 
      WHEN LOWER(prd.collection_c) LIKE '%bang%' OR '%techframe%' or '%king power%' THEN 'Big Bang'
      WHEN LOWER(prd.collection_c) LIKE '%fusion%' THEN 'Classic Fusion'
      WHEN LOWER(prd.collection_c) LIKE '%collection%' THEN 'Exceptional TimePieces'
      ELSE NULL
      END AS collection,
      prd.collection_c AS subcollection,
    CASE
      WHEN DATE_DIFF(CURRENT_DATE(), DATE(wish.created_date), DAY) <= 90 THEN '1. HOT'
      WHEN DATE_DIFF(CURRENT_DATE(), DATE(wish.created_date), DAY) <= 180 THEN '2. WARM'
      ELSE '3. COLD'
    END AS wish_class, 
    CASE
      WHEN profile_id IN 
        (
        '00e67000001AOKjAAO',
        '00e67000001AOKkAAO'
        ) THEN "Account" 
        WHEN profile_id = '00e0X0000010YCbQAM' THEN 'SA'
        ELSE NULL END AS created_by
  FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_wishlist_c_filtered` wish
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_product_2_filtered` prd
    ON wish.product_c = prd.id
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_user_with_markets` usrs
    ON wish.created_by_id = usrs.id
),


-- Step 2: Account and associated metadata
account_base AS (
  SELECT
    ap.id AS account_id,
    ah.market AS account_market,
    ah.life_time_segment AS account_segment,
    ah.status AS account_status,
    DATE(ap.created_date) AS account_created_date,  -- cast to DATE
    ap.email_syntax_quality AS account_email_syntax_quality,
    ap.phone_syntax_quality AS account_phone_syntax_quality,
    ap.marketing_consent AS account_marketing_consent,
    ah.is_hublotista_v_2_c AS account_is_hublotista,
    ah.billing_country_code AS account_billing_country,
    bout.billing_country_code AS dos_billing_country,
    IF(ah.billing_country_code = bout.billing_country_code, 'Local', 'Non-Local') AS account_is_local,


    -- DOS (Boutique) info
    bout.name AS dos_name,
    bout.market AS dos_market,
    bout.y_2_store_id_c AS dos_store_id,


    -- Sales associate info
    sa.name AS sa_name,


    -- POS info
    pos.name AS pos_name,
    pos.market AS pos_market
  FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_history_dataset` ah
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
    ON ah.account_id = ap.id
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_primary_boutiques` bout
    ON ah.primary_boutique_c = bout.id
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_external_pos` pos
    ON ap.primary_external_pos_c = pos.id
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_distinct_sa_joined_users` sa
    ON ah.sales_person_new_c = sa.sales_person_new_c
)


-- Step 3: Final dataset with clean field names and date fields in DATE format
SELECT
  w.wish_created_date,
  w.wish_wishlist_id,
  w.wish_name,
  w.wish_product_c,
  w.wish_collection_c,
  w.wish_class,


  ab.dos_name AS dos_name,
  ab.dos_market AS dos_market,
  ab.dos_store_id AS dos_store_id,


  ab.sa_name AS sa_name,


  ab.pos_name AS pos_name,
  ab.pos_market AS pos_market,


  ab.account_market,
  ab.account_segment,
  ab.account_status,
  ab.account_created_date,
  ab.account_is_hublotista,
  ab.account_is_local,


  CASE
    WHEN (ab.account_email_syntax_quality = 1 AND ab.account_marketing_consent = 1)
      OR (ab.account_phone_syntax_quality = 1 AND ab.account_marketing_consent = 1)
    THEN 1
    ELSE 0
  END AS account_permission_to_contact


FROM wishlist w
LEFT JOIN account_base ab
  ON w.account_c = ab.account_id