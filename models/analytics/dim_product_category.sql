WITH dim_product_category__source AS (
  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.external__categories`
)

, dim_product_category__rename_recast AS (
  SELECT
    CAST(category_id AS INT) AS product_category_key
    , CAST(category_name AS STRING) AS product_category_name
    , CAST(parent_category_id AS INT) AS parent_category_key
    , CAST(category_level AS INT) AS category_level
  FROM dim_product_category__source
)

, dim_product_category__handle_null AS (
  SELECT
    product_category_key
    , IFNULL(product_category_name, 'Undefined') AS product_category_name
    , IFNULL(parent_category_key, 0) AS parent_category_key
    , IFNULL(category_level, 0) AS category_level
  FROM dim_product_category__rename_recast
)

, dim_product_category__add_undefined AS (
  SELECT
    product_category_key
    , product_category_name
    , parent_category_key
    , category_level
  FROM dim_product_category__handle_null

  UNION ALL
  SELECT
    0 AS product_category_key
    , 'Undefined' AS product_category_name
    , 0 AS parent_category_key
    , 0 AS category_level
  
  UNION ALL
  SELECT
    -1 AS product_category_key
    , 'Invalid' AS product_category_name
    , -1 AS parent_category_key
    , -1 AS category_level
)

SELECT
  *
FROM dim_product_category__add_undefined