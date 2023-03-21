WITH dim_package_type__source AS (
  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.warehouse__package_types`
)

, dim_package_type__recast_rename AS (
  SELECT
    CAST(package_type_id AS INT) AS package_type_key
    , CAST(package_type_name AS STRING) AS package_type_name
  FROM dim_package_type__source
)

SELECT
  package_type_key
  , package_type_name
FROM dim_package_type__recast_rename