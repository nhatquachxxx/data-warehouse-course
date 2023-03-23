WITH dim_state_province__source AS (
  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.application__state_provinces`
)

, dim_state_province__rename_recast AS (
  SELECT
    CAST(state_province_id AS INT) AS state_province_key
    , CAST(state_province_name AS STRING) AS state_province_name
    , CAST(sales_territory AS STRING) AS sales_territory
    , CAST(country_id AS INT) AS country_key
  FROM dim_state_province__source
)

, dim_state_province__handle_null AS (
  SELECT
    state_province_key
    , state_province_name
    , IFNULL(sales_territory, 'Undefined') AS sales_territory
    , IFNULL(country_key, 0) AS country_key
  FROM dim_state_province__rename_recast
)

, dim_state_province__add_undefined AS (
  SELECT
    state_province_key
    , state_province_name
    , sales_territory
    , country_key
  FROM dim_state_province__handle_null

  UNION ALL
  SELECT
  0 AS state_province_key
  , 'Undefined' AS state_province_name
  , 'Undefined' AS sales_territory
  , 0 AS country_key
)

SELECT
    dim_state_province.state_province_key
    , dim_state_province.state_province_name
    , dim_state_province.sales_territory
    , dim_state_province.country_key
    , IFNULL(dim_country.country_name, 'Invalid') AS country_name
    , IFNULL(dim_country.country_type, 'Invalid') AS country_type
    , IFNULL(dim_country.continent_name, 'Invalid') AS continent_name
    , IFNULL(dim_country.region_name, 'Invalid') AS region_name
    , IFNULL(dim_country.subregion_name, 'Invalid') AS subregion_name
FROM dim_state_province__add_undefined AS dim_state_province
LEFT JOIN {{ ref('stg_dim_country') }} AS dim_country
  ON dim_state_province.country_key = dim_country.country_key