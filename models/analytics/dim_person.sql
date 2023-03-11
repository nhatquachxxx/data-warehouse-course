WITH dim_person__source AS(
  SELECT 
    *
  FROM `vit-lam-data.wide_world_importers.application__people`
)

, dim_person__rename_recast AS (
  SELECT
    CAST(person_id AS INT) AS person_key
    , CAST(full_name AS STRING) AS full_name
  FROM dim_person__source
)

, dim_person__add_undefined_value AS (
  SELECT
    person_key
    , full_name
  FROM dim_person__rename_recast
  
  UNION ALL

  SELECT
    0 AS person_key
    , 'Undefined' AS full_name
)

SELECT
  person_key
  , full_name
FROM dim_person__add_undefined_value