WITH dim_supplier__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.purchasing__suppliers`
)

, dim_supplier__rename_recast AS (
  SELECT
    CAST(supplier_id AS INT) AS supplier_key
    , CAST(supplier_name AS STRING) AS supplier_name
  FROM dim_supplier__source
)

SELECT
  supplier_key
  , supplier_name
FROM dim_supplier__rename_recast
