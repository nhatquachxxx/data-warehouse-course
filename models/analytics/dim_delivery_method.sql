WITH dim_delivery_method__source AS (
  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.application__delivery_methods`
)

, dim_delivery_method__rename_recast AS (
  SELECT
    CAST(delivery_method_id AS INT) AS delivery_method_key
    , CAST(delivery_method_name AS STRING) AS delivery_method_name
  FROM dim_delivery_method__source
)

, dim_delivery_method__add_undefined AS (
  SELECT
    delivery_method_key
    , delivery_method_name
  FROM dim_delivery_method__rename_recast

  UNION ALL
  SELECT
    0 AS delivery_method_key
    , 'Undefined' AS delivery_method_name
)

SELECT
  delivery_method_key
  , delivery_method_name
FROM dim_delivery_method__add_undefined 