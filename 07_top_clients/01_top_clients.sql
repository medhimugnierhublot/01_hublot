WITH top_clients AS (
  SELECT
    Account_ID,
    string_field_1,
    Person_Account__Contact_ID_18,
    Parent_Account__Account_ID,
    UniqueID,
    Account_External_Id,
    Account_Name,
    Segment,
    Billing_Country__text_only_,
    Primary_DOS_Boutique__Account_Name,
    Primary_Sales_Associate__Full_Name,
    Primary_External_POS__Account_Name,
    Last_purchase_date,
    Number_of_Belongings,
    Total_Own_Price_CHF,
    Total_Number_of_Belonging,
    Total_Retail_Price_CHF,
    Person_Account__Permission_To_Contact,
    Global_Opt_In,
    Person_Account__Email,
    Person_Account__Mobile,
    Hublotista
  FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis_dataiku_dev.top_clients_list_2025_04_14_14_12_26_jap_compliant`
),

top_clients_with_accounts AS (
  SELECT
    tc.*,
    ap.id AS account_c
  FROM top_clients tc
  LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_account_people` ap
    ON LOWER(tc.Person_Account__Email) = LOWER(ap.person_email)
  WHERE ap.id IS NOT NULL
),

belonging AS (
  SELECT
    account_c,
    COALESCE(warranty_activation_date_c, purchase_date_c) AS purchase_date,
    retail_price_chf_c
  FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_c_filtered`
  WHERE is_watch_c = TRUE
  AND account_c = '0016700005r6shn'
)

SELECT
  tc.Account_ID,
  tc.string_field_1,
  tc.Person_Account__Contact_ID_18,
  tc.Parent_Account__Account_ID,
  tc.UniqueID,
  tc.Account_External_Id,
  tc.Account_Name,
  tc.Segment,
  tc.Billing_Country__text_only_,
  tc.Primary_DOS_Boutique__Account_Name,
  tc.Primary_Sales_Associate__Full_Name,
  tc.Primary_External_POS__Account_Name,
  tc.Last_purchase_date,
  tc.Number_of_Belongings,
  tc.Total_Own_Price_CHF,
  tc.Total_Number_of_Belonging,
  tc.Total_Retail_Price_CHF,
  tc.Person_Account__Permission_To_Contact,
  tc.Global_Opt_In,
  tc.Person_Account__Email,
  tc.Person_Account__Mobile,
  tc.Hublotista,

  -- Purchase Aggregates for 2022
  COUNTIF(EXTRACT(YEAR FROM b.purchase_date) = 2022) AS number_of_purchases_2022,
  SAFE_CAST(SUM(IF(EXTRACT(YEAR FROM b.purchase_date) = 2022, b.retail_price_chf_c, 0)) AS INT64) AS total_value_2022_chf,
  SAFE_CAST(AVG(IF(EXTRACT(YEAR FROM b.purchase_date) = 2022, b.retail_price_chf_c, NULL)) AS INT64) AS avg_yearly_value_2022_chf,

  -- Purchase Aggregates for 2023
  COUNTIF(EXTRACT(YEAR FROM b.purchase_date) = 2023) AS number_of_purchases_2023,
  SAFE_CAST(SUM(IF(EXTRACT(YEAR FROM b.purchase_date) = 2023, b.retail_price_chf_c, 0)) AS INT64) AS total_value_2023_chf,
  SAFE_CAST(AVG(IF(EXTRACT(YEAR FROM b.purchase_date) = 2023, b.retail_price_chf_c, NULL)) AS INT64) AS avg_yearly_value_2023_chf,

  -- Purchase Aggregates for 2024
  COUNTIF(EXTRACT(YEAR FROM b.purchase_date) = 2024) AS number_of_purchases_2024,
  SAFE_CAST(SUM(IF(EXTRACT(YEAR FROM b.purchase_date) = 2024, b.retail_price_chf_c, 0)) AS INT64) AS total_value_2024_chf,
  SAFE_CAST(AVG(IF(EXTRACT(YEAR FROM b.purchase_date) = 2024, b.retail_price_chf_c, NULL)) AS INT64) AS avg_yearly_value_2024_chf,

  -- Purchase Aggregates for 2025
  COUNTIF(EXTRACT(YEAR FROM b.purchase_date) = 2025) AS number_of_purchases_2025,
  SAFE_CAST(SUM(IF(EXTRACT(YEAR FROM b.purchase_date) = 2025, b.retail_price_chf_c, 0)) AS INT64) AS total_value_2025_chf,
  SAFE_CAST(AVG(IF(EXTRACT(YEAR FROM b.purchase_date) = 2025, b.retail_price_chf_c, NULL)) AS INT64) AS avg_yearly_value_2025_chf

FROM top_clients_with_accounts tc
LEFT JOIN belonging b ON tc.account_c = b.account_c
GROUP BY
  tc.Account_ID,
  tc.string_field_1,
  tc.Person_Account__Contact_ID_18,
  tc.Parent_Account__Account_ID,
  tc.UniqueID,
  tc.Account_External_Id,
  tc.Account_Name,
  tc.Segment,
  tc.Billing_Country__text_only_,
  tc.Primary_DOS_Boutique__Account_Name,
  tc.Primary_Sales_Associate__Full_Name,
  tc.Primary_External_POS__Account_Name,
  tc.Last_purchase_date,
  tc.Number_of_Belongings,
  tc.Total_Own_Price_CHF,
  tc.Total_Number_of_Belonging,
  tc.Total_Retail_Price_CHF,
  tc.Person_Account__Permission_To_Contact,
  tc.Global_Opt_In,
  tc.Person_Account__Email,
  tc.Person_Account__Mobile,
  tc.Hublotista

ORDER BY tc.Account_ID;