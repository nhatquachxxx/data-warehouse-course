WITH stg_fact_sales_order__source AS (
  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.sales__orders`
)

, stg_fact_sales_order__rename_recast AS (
  SELECT
    CAST(order_id AS INT) AS sales_order_key
    , CAST(is_undersupply_backordered AS BOOLEAN) AS is_undersupply_backordered_boolean
    , CAST(backorder_order_id AS INT) AS backorder_order_key
    , CAST(customer_purchase_order_number AS INT) AS customer_purchase_order_number
    , CAST(customer_id AS INT) AS customer_key
    , CAST(picked_by_person_id AS INT) AS picked_by_person_key
    , CAST(salesperson_person_id AS INT) AS salesperson_person_key
    , CAST(contact_person_id AS INT) AS contact_person_key
    , CAST(order_date AS DATE) AS order_date
    , CAST(expected_delivery_date AS DATE) AS expected_delivery_date
    , CAST(picking_completed_when AS DATE) AS order_picking_completed_when
  FROM stg_fact_sales_order__source
)

, stg_fact_sales_order__convert_boolean AS (
  SELECT
    *
    , CASE
      WHEN is_undersupply_backordered_boolean IS TRUE THEN 'Backordered'
      WHEN is_undersupply_backordered_boolean IS FALSE THEN 'Not Backordered'
      WHEN is_undersupply_backordered_boolean IS NULL THEN 'Undefined'
      ELSE 'Invalid' END
    AS is_undersupply_backordered
  FROM stg_fact_sales_order__rename_recast
)

, stg_fact_sales_order__handle_nul AS (
  SELECT
    sales_order_key
    , is_undersupply_backordered
    , backorder_order_key
    , customer_purchase_order_number
    , customer_key
    , IFNULL(picked_by_person_key, 0) AS picked_by_person_key
    , salesperson_person_key
    , contact_person_key
    , order_date
    , expected_delivery_date
    , order_picking_completed_when
  FROM stg_fact_sales_order__convert_boolean
)