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

SELECT
  dim_location.city_key
  , dim_location.city_name
  , dim_location.state_province_key
  , dim_state_province.state_province_name
  , dim_state_province.sales_territory
  , dim_state_province.country_key
  , dim_state_province.country_name
  , dim_state_province.country_type
  , dim_state_province.continent_name
  , dim_state_province.region_name
  , dim_state_province.subregion_name
FROM dim_location__rename_recast AS dim_location
LEFT JOIN {{ ref('stg_dim_state_province')}} AS dim_state_province
  ON dim_location.state_province_key = dim_state_province.state_province_key