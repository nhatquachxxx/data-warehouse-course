SELECT 
  CAST(stock_item_id AS INT) AS product_key
  , CAST(stock_item_name AS string)AS product_name
  , CAST(brand AS string) AS brand_name
FROM `vit-lam-data.wide_world_importers.warehouse__stock_items`
