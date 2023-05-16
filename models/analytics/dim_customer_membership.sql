WITH dim_customer_membership__source AS (
  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.external__customer_membership`
)

, dim_customer_membership__rename_recast AS (
  SELECT
    CAST(customer_id AS INT) AS customer_id
    , CAST(membership AS STRING) AS membership
    , CAST(begin_effective_date AS DATE) AS begin_effective_date
    , CAST(end_effective_date AS DATE) AS end_effective_date
  FROM dim_customer_membership__source
)

, dim_customer_membership__handle_null AS (
  SELECT 
    customer_id
    , IFNULL(membership, 'Undefined') AS membership
    , begin_effective_date
    , end_effective_date
  FROM dim_customer_membership__rename_recast
)

, dim_customer_membership__add_undefine AS (
  SELECT
   customer_id
    , membership
    , begin_effective_date
    , end_effective_date
  FROM dim_customer_membership__handle_null

  UNION ALL
  SELECT
    0 AS customer_id
    , 'Undefined' AS membership
    , NULL AS begin_effective_date
    , NULL AS end_effective_date

  UNION ALL
  SELECT
    -1 AS customer_id
    , 'Invalid' AS membership
    , NULL AS begin_effective_date
    , NULL AS end_effective_date
)

SELECT
  customer_id
  , membership
  , begin_effective_date
  , end_effective_date
FROM dim_customer_membership__add_undefine