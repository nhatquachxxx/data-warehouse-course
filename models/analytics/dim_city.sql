WITH dim_city__source AS (
  SELECT 
    *
  FROM `vit-lam-data.wide_world_importers.application__cities`
)

, dim_city__rename_recast AS (
  SELECT
    CAST(city_id AS INT) AS city_key
    , CAST(city_name AS STRING) AS city_name
    , CAST(state_province_id AS INT) AS state_province_key
  FROM dim_city__source
)

, dim_city__handle_null AS (
  SELECT
    city_key
    , city_name
    , IFNULL(state_province_key, 0) AS state_province_key
  FROM dim_city__rename_recast
)

, dim_city__add_undefined AS (
  SELECT
    city_key
    , city_name
    , state_province_key
  FROM dim_city__handle_null

  UNION ALL
  SELECT
    0 AS city_key
    , 'Undefined' AS city_name
    , 0 AS state_province_key

  UNION ALL
  SELECT
    -1 AS city_key
    , 'Invalid' AS city_name
    , -1 AS state_province_key
)

  SELECT
    dim_city.city_key
    , dim_city.city_name
    , dim_city.state_province_key
    , IFNULL(dim_state_province.state_province_name, 'Invalid') AS state_province_name
    , IFNULL(dim_state_province.sales_territory, 'Invalid') AS sales_territory
    , IFNULL(dim_state_province.country_key, -1) AS country_key
    , IFNULL(dim_state_province.country_name, 'Invalid') AS country_name
    , IFNULL(dim_state_province.country_type, 'Invalid') AS country_type
    , IFNULL(dim_state_province.continent_name, 'Invalid') AS continent_name
    , IFNULL(dim_state_province.region_name, 'Invalid') AS region_name
    , IFNULL(dim_state_province.subregion_name, 'Invalid') AS subregion_name
  FROM dim_city__add_undefined AS dim_city
  LEFT JOIN {{ ref('stg_dim_state_province') }} AS dim_state_province
    ON dim_city.state_province_key = dim_state_province.state_province_key