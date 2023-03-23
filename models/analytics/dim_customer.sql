WITH dim_customer__source AS (
  SELECT 
    *
  FROM `vit-lam-data.wide_world_importers.sales__customers`
)

, dim_customer__rename_recast AS (
  SELECT
    CAST(customer_id AS INT) AS customer_key
    , CAST(customer_name AS STRING) AS customer_name
    , CAST(is_statement_sent AS BOOLEAN) AS is_statement_sent_boolean
    , CAST(is_on_credit_hold AS BOOLEAN) AS is_on_credit_hold_boolean
    , CAST(payment_days AS INT) AS payment_days
    , CAST(credit_limit AS NUMERIC) AS credit_limit
    , CAST(standard_discount_percentage AS NUMERIC) AS standard_discount_percentage
    , CAST(bill_to_customer_id AS INT) AS bill_to_customer_key
    , CAST(customer_category_id AS INT) AS customer_category_key
    , CAST(buying_group_id AS INT) AS buying_group_key
    , CAST(primary_contact_person_id AS INT) AS primary_contact_person_key
    , CAST(alternate_contact_person_id AS INT) AS alternate_contact_person_key
    , CAST(delivery_method_id AS INT) AS delivery_method_key
    , CAST(delivery_city_id AS INT) AS delivery_city_key
    , CAST(account_open_date AS DATE) AS account_open_date
  FROM dim_customer__source
)

, dim_customer__convert_boolean AS (
  SELECT
    *
    , CASE
        WHEN is_statement_sent_boolean IS TRUE THEN 'Statement Sent'
        WHEN is_statement_sent_boolean IS FALSE THEN 'Statement Not Sent'
        WHEN is_statement_sent_boolean IS NULL THEN 'Undefined'
        ELSE 'Invalid' END
      AS is_statement_sent
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
LEFT JOIN {{ ref('dim_person') }} AS dim_person_primary
  ON dim_customer.primary_contact_person_key = dim_person_primary.person_key
LEFT JOIN {{ ref('dim_delivery_method') }} AS dim_delivery_method
  ON dim_customer.delivery_method_key = dim_delivery_method.delivery_method_key
LEFT JOIN {{ ref('dim_location') }} AS dim_location
  ON dim_customer.delivery_city_key = dim_location.city_key