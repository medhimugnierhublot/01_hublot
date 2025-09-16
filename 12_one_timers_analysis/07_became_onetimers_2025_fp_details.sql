WITH ordered_data AS (
    SELECT
        bel.account_c AS account_id,
        COALESCE(bel.warranty_activation_date_c, bel.purchase_date_c) AS purchase_date,
        bel.retail_price_chf_c,
        bel.product_reference_c,
        ROW_NUMBER() OVER (
            PARTITION BY bel.account_c 
            ORDER BY COALESCE(bel.warranty_activation_date_c, bel.purchase_date_c) ASC
        ) AS rn
    FROM `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_first_purchase_last_purchase.DATA_ANALYSIS_ONE_TIMERS_05_became_onetimers_2025_active_fp_in_DOS` ot
    LEFT JOIN `hub-prd-atomdt-prj-pltfrm-12b.hub_prd_atomdtp_bqd_crm_client_analysis.CLIENTS_REPORTING_belonging_c_filtered` bel
        ON ot.account_id = bel.account_c
    WHERE bel.is_watch_c
)

SELECT
    account_id,
    purchase_date AS first_purchase_date,
    retail_price_chf_c AS first_retail_price_chf,
    product_reference_c AS first_product_reference
FROM ordered_data
WHERE rn = 1

-- FIRST PRODUCT RETAIL PRICE
    -- avg_first_retail_price_chf: 15.4K

-- FIRST PRODUCT PRODUCT REF
    -- first_product_reference  nb_first_purchase
    -- CLASSIC FUSION TITANIUM	115
    -- CLASSIC FUSION TITANIUM RACING GREY	92
    -- CLASSIC FUSION BLACK MAGIC	79
    -- CLASSIC FUSION TITANIUM BLUE	47
    -- CLASSIC FUSION CHRONOGRAPH BLACK MAGIC	42
    -- CLASSIC FUSION CHRONOGRAPH TITANIUM	35
    -- CLASSIC FUSION AEROFUSION CHRONOGRAPH BLACK MAGIC	35
    -- CLASSIC FUSION TITANIUM DIAMONDS	33
    -- CLASSIC FUSION CHRONOGRAPH TITANIUM RACING GREY	33
    -- CLASSIC FUSION TITANIUM KING GOLD	29
    -- CLASSIC FUSION CHRONOGRAPH CERAMIC BLUE	27
    -- CLASSIC FUSION TITANIUM GREEN	26
    -- BIG BANG MECA-10 TITANIUM	24
    -- CLASSIC FUSION AEROFUSION CHRONOGRAPH TITANIUM	23
    -- BIG BANG STEEL CERAMIC	19
    -- BIG BANG UNICO BLACK MAGIC	18
    -- CLASSIC FUSION CHRONOGRAPH TITANIUM BLUE	18
    -- CLASSIC FUSION TITANIUM RACING GREY DIAMONDS	17
    -- BIG BANG UNICO BERLUTI NERO GRIGIO CERAMIC	17