WITH fact_purchase_order_line__source AS (
  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.purchasing__purchase_order_lines`
)

, fact_purchase_order_line__rename_recast AS (
  SELECT
    CAST(purchase_order_line_id AS INT) AS purchase_order_line_key
    , CAST(purchase_order_id AS INT) AS purchase_order_key
    , CAST(is_order_line_finalized AS BOOLEAN) AS is_order_line_finalized_boolean
    , CAST(stock_item_id AS INT) AS product_key
    , CAST(package_type_id AS INT) AS package_type_key
    , CAST(ordered_outers AS INT) AS ordered_outers
    , CAST(received_outers AS INT) AS received_outers
    , CAST(expected_unit_price_per_outer AS NUMERIC) AS expected_unit_price_per_outer
    , CAST(description AS string) AS description
    , CAST(last_receipt_date AS DATE) AS last_receipt_date
  FROM fact_purchase_order_line__source
)

, fact_purchase_order_line__convert_boolean AS (
  SELECT
    *
    , CASE
        WHEN is_order_line_finalized_boolean IS TRUE THEN 'Finalized Order Line'
        WHEN is_order_line_finalized_boolean IS FALSE THEN 'Not Finalized Order Line'
        WHEN is_order_line_finalized_boolean IS NULL THEN 'Undefined'
        ELSE 'Invalid' END
    AS is_order_line_finalized
  FROM fact_purchase_order_line__rename_recast
)

, fact_purchase_order_line__handle_null AS (
  SELECT
    purchase_order_line_key
    , IFNULL(purchase_order_key, 0) AS purchase_order_key
    , is_order_line_finalized
    , IFNULL(product_key, 0) AS product_key
    , IFNULL(package_type_key, 0) AS package_type_key
    , ordered_outers
    , received_outers
    , expected_unit_price_per_outer
    , IFNULL(description, 'Undefined') AS description
    , last_receipt_date
  FROM fact_purchase_order_line__convert_boolean
)

, fact_purchase_order_line__calculate_measure AS (
  SELECT
    *
    , ordered_outers - received_outers AS remaining_outers
  FROM fact_purchase_order_line__handle_null
)

SELECT
  fact_line.purchase_order_line_key
  , fact_line.purchase_order_key
  , fact_header.is_order_finalized
  , fact_line.is_order_line_finalized
  , fact_line.product_key
  , fact_line.package_type_key
  , fact_header.supplier_key
  , fact_header.delivery_method_key
  , fact_header.contact_person_key
  , fact_line.description
  , fact_header.supplier_reference
  , fact_header.comments
  , fact_line.ordered_outers
  , fact_line.received_outers
  , fact_line.remaining_outers
  , fact_line.expected_unit_price_per_outer
  , fact_line.last_receipt_date
  , fact_header.order_date
  , fact_header.expected_delivery_date
FROM fact_purchase_order_line__calculate_measure AS fact_line
LEFT JOIN {{ ref('stg_fact_purchase_order') }} AS fact_header
  ON fact_line.purchase_order_key = fact_header.purchase_order_key