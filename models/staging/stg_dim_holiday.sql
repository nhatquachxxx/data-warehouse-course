WITH dim_holiday__source AS (
  SELECT
    *
  FROM `atomic-oven-374400.wide_world_importers_dwh.dim_holiday_source`
)

, dim_holiday__rename_recast AS (
  SELECT
    CAST(holiday_date AS date) AS holiday_date
    , CAST(holiday_name AS string) AS holiday_name
    , CAST(holiday_type AS string) AS holiday_type
    , CAST(country_name AS string) AS country_name
    , CAST(description AS string) AS description
  FROM dim_holiday__source
)

SELECT
  holiday_date
  , CASE
      WHEN holiday_name = 'Tet holiday' THEN 'Tet Holiday'
      ELSE holiday_name END
    AS holiday_name
  , holiday_type
  , country_name
  , description
FROM dim_holiday__rename_recast