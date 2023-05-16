WITH dim_external_product__source AS (
  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.external__stock_item`
)

, dim_external_product__rename_recast AS (
  SELECT
    CAST(stock_item_id AS INT) AS product_key
    , CAST(category_id AS INT) AS category_key
  FROM dim_external_product__source
)

, dim_external_product__handle_null AS (
  SELECT
    IFNULL(product_key, 0) AS product_key
    , IFNULL(category_key, 0) AS category_key
  FROM dim_external_product__rename_recast
)

SELECT
  product_key
  , category_key
FROM dim_external_product__handle_null
