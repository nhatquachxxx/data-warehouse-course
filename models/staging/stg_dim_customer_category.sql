WITH dim_customer_category__source AS(
  SELECT 
    *
  FROM `vit-lam-data.wide_world_importers.sales__customer_categories`
)

, dim_customer_category__rename_recast AS (
  SELECT
    CAST(customer_category_id AS INT) AS customer_category_key
    , CAST(customer_category_name AS STRING) AS customer_category_name
  FROM dim_customer_category__source
)

,dim_customer_category__add_undefined AS (
  SELECT
    customer_category_key
    , customer_category_name
  FROM dim_customer_category__rename_recast

  UNION ALL
  SELECT
    0 AS customer_category_key
    , 'Undefined' AS customer_category_name
)

SELECT
  customer_category_key
  , customer_category_name
FROM dim_customer_category__add_undefined