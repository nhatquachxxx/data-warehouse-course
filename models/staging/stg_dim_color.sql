WITH dim_color__source AS (
  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.warehouse__colors`
)

, dim_color__rename_recast AS (
  SELECT
    CAST(color_id AS INT) AS color_key
    , CAST(color_name AS STRING) AS color_name
  FROM dim_color__source
)

, dim_color__add_undefined AS (
  SELECT
    color_key
    , color_name
  FROM dim_color__rename_recast

  UNION ALL
  SELECT
    0 AS color_key
    , 'Undefined' AS color_name
)

SELECT
  color_key
  , color_name
FROM dim_color__add_undefined