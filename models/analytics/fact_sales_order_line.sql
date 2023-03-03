SELECT 
  order_line_id AS sales_order_line_key
  , quantity
  , unit_price
  , tax_rate
  , quantity*unit_price AS gross_amount
  , quantity * unit_price * tax_rate / 100 AS tax_amount
  , (quantity*unit_price) - (quantity * unit_price * tax_rate / 100) AS net_amount
FROM `vit-lam-data.wide_world_importers.sales__order_lines`