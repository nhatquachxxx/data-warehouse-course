-- Code Layering with CTE
-- Layer 1: select all from source table
-- Layer 2: Select necessary fields & apply cast
-- Layer 3: perform calculated measures
-- Code Layering with CTE

WITH fact_sales_order_line__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.sales__order_lines`
)

, fact_sales_order_line__rename_cast AS (
  SELECT 
    CAST(order_line_id AS INT) AS sales_order_line_key
    , CAST(stock_item_id AS INT) AS product_key
    , CAST(quantity AS INT) AS quantity
    , CAST(unit_price AS NUMERIC) AS unit_price
    , CAST(tax_rate AS NUMERIC) AS tax_rate
  FROM fact_sales_order_line__source
)

, fact_sales_order_line__calculated_measure AS (
  SELECT 
    sales_order_line_key
    , product_key
    , quantity
    , unit_price
    , tax_rate
    , quantity * unit_price AS gross_amount
    , unit_price * quantity *tax_rate AS tax_amount
    , (quantity * unit_price) - (unit_price * quantity * tax_rate) AS net_amount -- net_amount = gross_amount - tax_amount
  FROM fact_sales_order_line__rename_cast
)

SELECT 
  sales_order_line_key
  , product_key
  , quantity
  , unit_price
  , tax_rate
  , gross_amount
  , tax_amount
  , net_amount
FROM fact_sales_order_line__calculated_measure