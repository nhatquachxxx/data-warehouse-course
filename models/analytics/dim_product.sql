-- 1st layer, select all resource from dim_product
WITH dim_product__source AS(
  SELECT *
  FROM `vit-lam-data.wide_world_importers.warehouse__stock_items`
)

-- 2nd layer, select specific fields and perform CAST
, dim_product__rename_recast AS (
  SELECT 
      CAST(stock_item_id AS INT) AS product_key
    , CAST(stock_item_name AS string) AS product_name
    , CAST(supplier_id AS INT) AS supplier_key
    , CAST(brand AS string) AS brand_name
    , CAST(is_chiller_stock AS BOOLEAN) AS is_chiller_stock_boolean
  FROM dim_product__source
)

, dim_product__convert_boolean AS (
  SELECT 
    *
    , CASE 
        WHEN is_chiller_stock_boolean IS TRUE THEN 'Chiller Stock'
        WHEN is_chiller_stock_boolean IS FALSE THEN 'Not Chiller Stock'
        ELSE 'Undefined' END
      AS is_chiller_stock
  FROM dim_product__rename_recast
)

SELECT
  dim_product.product_key
  , dim_product.product_name
  , dim_product.brand_name
  , dim_product.supplier_key
  , dim_supplier.supplier_name
  , dim_product.is_chiller_stock
FROM dim_product__convert_boolean AS dim_product
LEFT JOIN {{ ref('dim_supplier') }} AS dim_supplier
  ON dim_product.supplier_key = dim_supplier.supplier_key