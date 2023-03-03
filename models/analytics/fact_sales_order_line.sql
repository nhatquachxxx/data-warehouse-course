SELECT 
  CAST(order_line_id AS INT) AS sales_order_line_key
  , CAST(stock_item_id AS INT) AS product_key
  , CAST(quantity AS INT) AS quantity
  , CAST(unit_price AS NUMERIC) AS unit_price
  , CAST(tax_rate AS NUMERIC) AS tax_rate
  , CAST(quantity AS INT) * CAST(unit_price AS NUMERIC) AS gross_amount
  , CAST(unit_price AS NUMERIC) * CAST(unit_price AS NUMERIC) * CAST(tax_rate AS NUMERIC) / 100 AS tax_amount
  , (CAST(quantity AS INT) * CAST(unit_price AS NUMERIC)) - (CAST(quantity AS INT) * CAST(unit_price AS NUMERIC) * CAST(tax_rate AS NUMERIC) / 100) AS net_amount
FROM `vit-lam-data.wide_world_importers.sales__order_lines`