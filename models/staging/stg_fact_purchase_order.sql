WITH fact_purchase_order__source AS (
  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.purchasing__purchase_orders`
)

, fact_purchase_order__rename_recast AS (
  SELECT
    CAST(purchase_order_id AS INT) AS purchase_order_key
    , CAST(is_order_finalized AS BOOLEAN) AS is_order_finalized_boolean
    , CAST(supplier_id AS INT) AS supplier_key
    , CAST(delivery_method_id AS INT) AS delivery_method_key
    , CAST(contact_person_id AS INT) AS contact_person_key
    , CAST(supplier_reference AS STRING) AS supplier_reference
    , CAST(comments AS STRING) AS comments
    , CAST(order_date AS DATE) AS order_date
    , CAST(expected_delivery_date AS DATE) AS expected_delivery_date
  FROM fact_purchase_order__source
)

, fact_purchase_order__convert_boolean AS (
  SELECT
    *
    , CASE
        WHEN is_order_finalized_boolean IS TRUE THEN 'Finalized Order'
        WHEN is_order_finalized_boolean IS FALSE THEN 'Not Finalized Order'
        WHEN is_order_finalized_boolean IS NULL THEN 'Undefined'
        ELSE 'Invalid' END
      AS is_order_finalized
  FROM fact_purchase_order__rename_recast
)

, fact_purchase_order__handle_null AS (
  SELECT
    purchase_order_key
    , is_order_finalized
    , IFNULL(supplier_key, 0) AS supplier_key
    , IFNULL(delivery_method_key, 0) AS delivery_method_key
    , IFNULL(contact_person_key, 0) AS contact_person_key
    , IFNULL(supplier_reference, 'Undefined') AS supplier_reference
    , IFNULL(comments, 'Undefined') AS comments
    , order_date
    , expected_delivery_date
  FROM fact_purchase_order__convert_boolean
)

, fact_purchase_order__add_undefined AS (
  SELECT
    purchase_order_key
    , is_order_finalized
    , supplier_key
    , delivery_method_key
    , contact_person_key
    , supplier_reference
    , comments
    , order_date
    , expected_delivery_date
  FROM fact_purchase_order__handle_null

  UNION ALL
  SELECT
    0 AS purchase_order_key
    , 'Undefined' AS is_order_finalized
    , 0 AS supplier_key
    , 0 AS delivery_method_key
    , 0 AS contact_person_key
    , 'Undefined' AS supplier_reference
    , 'Undefined' AS comments
    , NULL AS order_date
    , NULL AS expected_delivery_date

  UNION ALL
  SELECT
    -1 AS purchase_order_key
    , 'Invalid' AS is_order_finalized
    , -1 AS supplier_key
    , -1 AS delivery_method_key
    , -1 AS contact_person_key
    , 'Invalid' AS supplier_reference
    , 'Invalid' AS comments
    , NULL AS order_date
    , NULL AS expected_delivery_date
)

SELECT
  purchase_order_key
  , is_order_finalized
  , supplier_key
  , delivery_method_key
  , contact_person_key
  , supplier_reference
  , comments
  , order_date
  , expected_delivery_date
FROM fact_purchase_order__add_undefined