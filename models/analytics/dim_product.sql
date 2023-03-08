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
  FROM dim_product__source
)

SELECT
  dim_product.product_key
  , dim_product.product_name
  , dim_product.brand_name
  , dim_product.supplier_key
  , dim_supplier.supplier_name
FROM dim_product__rename_recast AS dim_product
LEFT JOIN {{ ref('dim_supplier') }} AS dim_supplier
  ON dim_product.supplier_key = dim_supplier.supplier_key