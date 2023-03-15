WITH fact_sales_order_line__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.sales__order_lines`
)

, fact_sales_order_line__rename_recast AS (
  SELECT 
    CAST(order_line_id AS INT) AS sales_order_line_key
    , CAST(order_id AS INT) AS sales_order_key
    , CAST(stock_item_id AS INT) AS product_key
    , CAST(quantity AS INT) AS quantity
    , CAST(unit_price AS NUMERIC) AS unit_price
    , CAST(tax_rate AS NUMERIC) AS tax_rate
  FROM fact_sales_order_line__source
)

, fact_sales_order_line__calculated_measure AS (
  SELECT 
    sales_order_line_key
    , sales_order_key
    , product_key
    , quantity
    , unit_price
    , tax_rate
    , quantity * unit_price AS gross_amount
    , unit_price * quantity * tax_rate AS tax_amount
    , (quantity * unit_price) - (unit_price * quantity * tax_rate) AS net_amount -- net_amount = gross_amount - tax_amount
  FROM fact_sales_order_line__rename_recast
)

SELECT
  fact_order_line.sales_order_line_key
  , fact_order_line.sales_order_key
  , fact_order.customer_key
  , IFNULL(fact_order.picked_by_person_key, -1) AS picked_by_person_key
  , fact_order_line.product_key
  , fact_order_line.quantity
  , fact_order_line.unit_price
  , fact_order_line.tax_rate
  , fact_order.order_date
  , fact_order_line.gross_amount
  , fact_order_line.tax_amount
  , fact_order_line.net_amount
FROM fact_sales_order_line__calculated_measure AS fact_order_line
LEFT JOIN {{ ref('stg_fact_sales_order') }} AS fact_order
  ON fact_order_line.sales_order_key = fact_order.sales_order_key