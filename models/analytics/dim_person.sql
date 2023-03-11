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

SELECT
  person_key
  , full_name
FROM dim_person__rename_recast