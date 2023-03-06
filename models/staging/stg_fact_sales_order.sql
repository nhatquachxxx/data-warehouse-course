WITH stg_fact_sales_order__source AS (
  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.sales__orders`
)

, stg_fact_sales_order__rename_recast AS (
  SELECT
    CAST(order_id AS INT) AS sales_order_key
    , CAST(customer_id AS INT) AS customer_key
  FROM stg_fact_sales_order__source
)
SELECT
  sales_order_key
  , customer_key
FROM stg_fact_sales_order__rename_recast