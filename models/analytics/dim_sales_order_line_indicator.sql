WITH dim_sales_order_line_indicator__generate AS (
  SELECT
    TRUE AS is_undersupply_backordered_boolean
    ,'Undersupply Backordered' AS is_undersupply_backordered

  UNION ALL
  SELECT
    FALSE AS is_undersupply_backordered_boolean
    , 'Not Undersupply Backordered' AS is_undersupply_backordered
)

SELECT
  is_undersupply_backordered_boolean
  , is_undersupply_backordered
  , package_type_key
  , package_type_name
FROM dim_sales_order_line_indicator__generate
CROSS JOIN {{ ref('dim_package_type') }} AS dim_package_type