-- 1st layer, select all resource from dim_product
WITH dim_product__source AS(
  SELECT *
  FROM `vit-lam-data.wide_world_importers.warehouse__stock_items`
)

-- 2nd layer, select specific fields and perform CAST
, dim_product__rename_recast AS (
  SELECT 
      CAST(stock_item_id AS INT) AS product_key
    , CAST(stock_item_name AS STRING) AS product_name
    , CAST(brand AS STRING) AS brand_name
    , CAST(size AS STRING) AS size_name
    , CAST(is_chiller_stock AS BOOLEAN) AS is_chiller_stock_boolean
    , CAST(tags AS STRING) AS tags
    , CAST(lead_time_days AS INT) AS lead_time_days
    , CAST(typical_weight_per_unit AS NUMERIC) AS typical_weight_per_unit
    , CAST(recommended_retail_price AS NUMERIC) AS recommended_retail_price
    , CAST(unit_price AS NUMERIC) AS unit_price
    , CAST(tax_rate AS NUMERIC) AS tax_rate
    , CAST(quantity_per_outer AS INT) AS quantity_per_outer
    , CAST(supplier_id AS INT) AS supplier_key
    , CAST(color_id AS INT) AS color_key
    , CAST(unit_package_id AS INT) AS unit_package_type_key
    , CAST(outer_package_id AS INT) AS outer_package_type_key
  FROM dim_product__source
)

, dim_product__convert_boolean AS (
  SELECT 
    *
    , CASE 
        WHEN is_chiller_stock_boolean IS TRUE THEN 'Chiller Stock'
        WHEN is_chiller_stock_boolean IS FALSE THEN 'Not Chiller Stock'
        WHEN is_chiller_stock_boolean IS NULL THEN 'Undefined'
        ELSE 'Invalid' END
      AS is_chiller_stock
  FROM dim_product__rename_recast
)

, dim_product__handle_null AS (
  SELECT
    product_key
    , product_name
    , IFNULL(brand_name,'Undefined') AS brand_name
    , IFNULL(size_name, 'Undefined') AS size_name
    , is_chiller_stock
    , IFNULL(tags, 'Undefined') AS tags
    , lead_time_days
    , typical_weight_per_unit
    , recommended_retail_price
    , unit_price
    , tax_rate
    , quantity_per_outer
    , IFNULL(supplier_key, 0) AS supplier_key
    , IFNULL(color_key, 0) AS color_key
    , IFNULL(unit_package_type_key, 0) AS unit_package_type_key
    , IFNULL(outer_package_type_key, 0) AS outer_package_type_key
  FROM dim_product__convert_boolean
)

, dim_product__add_undefined AS (
  SELECT
    product_key
    , product_name
    , brand_name
    , size_name
    , is_chiller_stock
    , tags
    , lead_time_days
    , typical_weight_per_unit
    , recommended_retail_price
    , unit_price
    , tax_rate
    , quantity_per_outer
    , supplier_key
    , color_key
    , unit_package_type_key
    , outer_package_type_key
  FROM dim_product__handle_null

  UNION ALL
  SELECT
    0 AS product_key
    , 'Undefined' AS product_name
    , 'Undefined' AS brand_name
    , 'Undefined' AS size_name
    , 'Undefined' AS is_chiller_stock
    , 'Undefined' AS tags
    , NULL AS lead_time_days
    , NULL AS typical_weight_per_unit
    , NULL AS recommended_retail_price
    , NULL AS unit_price
    , NULL AS tax_rate
    , NULL AS quantity_per_outer
    , 0 AS supplier_key
    , 0 AS color_key
    , 0 AS unit_package_type_key
    , 0 AS outer_package_type_key
  
  UNION ALL
  SELECT
    -1 AS product_key
    , 'Invalid' AS product_name
    , 'Invalid' AS brand_name
    , 'Invalid' AS size_name
    , 'Invalid' AS is_chiller_stock
    , 'Invalid' AS tags
    , NULL AS lead_time_days
    , NULL AS typical_weight_per_unit
    , NULL AS recommended_retail_price
    , NULL AS unit_price
    , NULL AS tax_rate
    , NULL AS quantity_per_outer
    , -1 AS supplier_key
    , -1 AS color_key
    , -1 AS unit_package_type_key
    , -1 AS outer_package_type_key
)

SELECT
  dim_product.product_key
  , dim_product.product_name
  , dim_product.brand_name
  , dim_product.size_name
  , dim_product.is_chiller_stock
  , dim_product.tags
  , dim_product.lead_time_days
  , dim_product.typical_weight_per_unit
  , dim_product.recommended_retail_price
  , dim_product.unit_price
  , dim_product.tax_rate
  , dim_product.quantity_per_outer
  , dim_product.supplier_key
  , IFNULL(dim_supplier.supplier_name, 'Invalid') AS supplier_name -- replace null inside joining tables
  , dim_supplier.payment_days AS payment_days
  , IFNULL(dim_supplier.supplier_category_key, -1) AS supplier_category_key
  , IFNULL(dim_supplier.supplier_category_name, 'Invalid') AS supplier_category_name
  , IFNULL(dim_supplier.primary_contact_person_key, -1) AS primary_contact_person_key
  , IFNULL(dim_supplier.primary_contact_full_name, 'Invalid') AS primary_contact_full_name
  , IFNULL(dim_supplier.alternate_contact_person_key, -1) AS alternate_contact_person_key
  , IFNULL(dim_supplier.alternate_contact_full_name, 'Invalid') AS alternate_contact_full_name
  , IFNULL(dim_supplier.delivery_method_key, -1) AS delivery_method_key
  , IFNULL(dim_supplier.delivery_method_name, 'Invalid') AS delivery_method_name
  , IFNULL(dim_supplier.delivery_city_key, -1) AS delivery_city_key
  , IFNULL(dim_supplier.city_name, 'Invalid') AS delivery_city_name
  , IFNULL(dim_supplier.state_province_key, -1) AS delivery_state_province_key
  , IFNULL(dim_supplier.state_province_name, 'Invalid') AS delivery_state_province_name
  , IFNULL(dim_supplier.sales_territory, 'Invalid') AS delivery_sales_territory
  , IFNULL(dim_supplier.country_key, -1) AS delivery_country_key
  , IFNULL(dim_supplier.country_name, 'Invalid') AS delivery_country_name
  , IFNULL(dim_supplier.country_type, 'Invalid') AS delivery_country_type
  , IFNULL(dim_supplier.continent_name, 'Invalid') AS delivery_continent_name
  , IFNULL(dim_supplier.region_name, 'Invalid') AS delivery_region_name
  , IFNULL(dim_supplier.subregion_name, 'Invalid') AS delivery_subregion_name
  , dim_product.color_key
  , IFNULL(dim_color.color_name, 'Invalid') AS color_name
  , dim_product.unit_package_type_key
  , IFNULL(dim_package_type_unit.package_type_name, 'Invalid') AS unit_package_type_name
  , dim_product.outer_package_type_key
  , IFNULL(dim_package_type_outer.package_type_name, 'Invalid') AS outer_package_type_name
  , IFNULL(dim_external_product.product_category_key, -1) AS product_category_key
  , IFNULL(dim_product_category.product_category_name, 'Invalid') AS product_category_name
  , IFNULL(dim_product_category.parent_category_key, -1) AS parent_category_key
  , IFNULL(dim_parent_category.product_category_name, 'Invalid') AS parent_category_name
  , IFNULL(dim_product_category.category_level, -1) AS category_level
FROM dim_product__add_undefined AS dim_product

LEFT JOIN {{ ref('dim_supplier') }} AS dim_supplier
  ON dim_product.supplier_key = dim_supplier.supplier_key

LEFT JOIN {{ ref('stg_dim_color') }} AS dim_color
  ON dim_product.color_key = dim_color.color_key

LEFT JOIN {{ ref('dim_package_type') }} AS dim_package_type_unit
  ON dim_product.unit_package_type_key = dim_package_type_unit.package_type_key

LEFT JOIN {{ ref('dim_package_type') }} AS dim_package_type_outer
  ON dim_product.outer_package_type_key = dim_package_type_outer.package_type_key

LEFT JOIN {{ ref ('stg_dim_external_product') }} AS dim_external_product
  ON dim_product.product_key = dim_external_product.product_key

LEFT JOIN {{ ref('stg_dim_product_category') }} AS dim_product_category
  ON dim_external_product.product_category_key = dim_product_category.product_category_key

-- join với dim_product_category lần nữa để retrieve parent category key/name
LEFT JOIN {{ ref('stg_dim_product_category') }} AS dim_parent_category
  ON dim_product_category.parent_category_key = dim_parent_category.product_category_key