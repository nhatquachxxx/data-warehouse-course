WITH dim_country__resource AS (
  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.application__countries`
)

, dim_country__rename_recast AS (
  SELECT
    CAST(country_id AS INT) AS country_key
    , CAST(country_name AS STRING) AS country_name
    , CAST(country_type AS STRING) AS country_type
    , CAST(continent AS STRING) AS continent_name
    , CAST(region AS STRING) AS region_name
    , CAST(subregion AS STRING) AS subregion_name
  FROM dim_country__resource
)

SELECT
  country_key
  , country_name
  , country_type
  , continent_name
  , region_name
  , subregion_name
FROM dim_country__rename_recast