-- 1st layer, select all resource from dim_prodct
WITH dim_product__source AS(
  SELECT *
  FROM `vit-lam-data.wide_world_importers.warehouse__stock_items`
)

-- 2nd layer, select specific fields and perform CAST
, dim_product__rename_recast AS (
  SELECT 
      CAST(stock_item_id AS INT) AS product_key
    , CAST(stock_item_name AS string)AS product_name
    , CAST(brand AS string) AS brand_name
  FROM dim_product__source
)

SELECT 
  * 
FROM dim_product__rename_recast