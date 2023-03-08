WITH dim_buying_group__source AS (
  SELECT 
    *
  FROM `vit-lam-data.wide_world_importers.sales__buying_groups`
)

, dim_buying_group__recast_rename AS (
  SELECT
    CAST(buying_group_id AS INT) AS buying_group_key
    , CAST(buying_group_name AS STRING) AS buying_group_name
  FROM dim_buying_group__source
)

SELECT 
  buying_group_key
  , buying_group_name
FROM dim_buying_group__recast_rename