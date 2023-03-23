WITH dim_location__source AS (
  SELECT 
    *
  FROM `vit-lam-data.wide_world_importers.application__cities`
)

, dim_location__rename_recast AS (
  SELECT
    CAST(city_id AS INT) AS city_key
    , CAST(city_name AS STRING) AS city_name
    , CAST(state_province_id AS INT) AS state_province_key
  FROM dim_location__source
)

, dim_location__handle_null AS (
  SELECT
    city_key
    , city_name
    , IFNULL(state_province_key, 0) AS state_province_key
  FROM dim_location__rename_recast
)

, dim_location__add_undefined AS (
  SELECT
    city_key
    , city_name
    , state_province_key
  FROM dim_location__handle_null

  UNION ALL
  SELECT
    0 AS city_key
    , 'Undefined' AS city_name
    , 0 AS state_province_key
)

  SELECT
    dim_location.city_key
    , dim_location.city_name
    , dim_location.state_province_key
    , IFNULL(dim_state_province.state_province_name, 'Invalid') AS state_province_name
    , IFNULL(dim_state_province.sales_territory, 'Invalid') AS sales_territory
    , dim_state_province.country_key AS country_key
    , IFNULL(dim_state_province.country_name, 'Invalid') AS country_name
    , IFNULL(dim_state_province.country_type, 'Invalid') AS country_type
    , IFNULL(dim_state_province.continent_name, 'Invalid') AS continent_name
    , IFNULL(dim_state_province.region_name, 'Invalid') AS region_name
    , IFNULL(dim_state_province.subregion_name, 'Invalid') AS subregion_name
  FROM dim_location__add_undefined AS dim_location
  LEFT JOIN {{ ref('stg_dim_state_province')}} AS dim_state_province
    ON dim_location.state_province_key = dim_state_province.state_province_key