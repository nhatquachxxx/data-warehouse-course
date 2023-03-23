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

SELECT
    dim_state_province.state_province_key
    , dim_state_province.state_province_name
    , dim_state_province.sales_territory
    , dim_state_province.country_key
    , dim_country.country_name
    , dim_country.country_type
    , dim_country.continent_name
    , dim_country.region_name
    , dim_country.subregion_name
FROM dim_state_province__rename_recast AS dim_state_province
LEFT JOIN {{ ref('stg_dim_country') }} AS dim_country
  ON dim_state_province.country_key = dim_country.country_key