WITH dim_customer__source AS (
  SELECT 
    *
  FROM `vit-lam-data.wide_world_importers.sales__customers`
)

, dim_customer__rename_recast AS (
  SELECT
    CAST(customer_id AS INT) AS customer_key
    , CAST(customer_name AS STRING) AS customer_name
    , CAST(is_on_credit_hold AS BOOLEAN) AS is_on_credit_hold_boolean
    , CAST(customer_category_id AS INT) AS customer_category_key
    , CAST(buying_group_id AS INT) AS buying_group_key

  FROM dim_customer__source
)

, dim_customer__convert_boolean AS (
  SELECT
    *
    , CASE
        WHEN is_on_credit_hold_boolean IS TRUE THEN 'On Credit Hold'
        WHEN is_on_credit_hold_boolean IS FALSE THEN 'Not On Credit Hold'
        WHEN is_on_credit_hold_boolean IS NULL THEN 'Undefined'
        ELSE 'Invalid' END
      AS is_on_credit_hold -- Separate between Undefined (null) and Invalid data
  FROM dim_customer__rename_recast
)

SELECT 
  dim_customer.customer_key
  , dim_customer.customer_name
  , dim_customer.is_on_credit_hold
  , dim_customer.customer_category_key
  , IFNULL(dim_customer_category.customer_category_name, 'Invalid') AS customer_category_name
  , dim_customer.buying_group_key
  , IFNULL(dim_buying_group.buying_group_name, 'Invalid') AS buying_group_name
FROM dim_customer__convert_boolean AS dim_customer
LEFT JOIN {{ ref('stg_dim_customer_category') }} AS dim_customer_category
  ON dim_customer.customer_category_key = dim_customer_category.customer_category_key
LEFT JOIN {{ ref('stg_dim_buying_group') }} AS dim_buying_group
  ON dim_customer.buying_group_key = dim_buying_group.buying_group_key