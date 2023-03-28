WITH dim_person__source AS(
  SELECT 
    *
  FROM `vit-lam-data.wide_world_importers.application__people`
)

, dim_person__rename_recast AS (
  SELECT
    CAST(person_id AS INT) AS person_key
    , CAST(full_name AS STRING) AS full_name
    , CAST(preferred_name AS STRING) AS preferred_name
    , CAST(is_permitted_to_logon AS BOOLEAN) AS is_permitted_to_logon_boolean
    , CAST(is_system_user AS BOOLEAN) AS is_system_user_boolean
    , CAST(is_employee AS BOOLEAN) AS is_employee_boolean
    , CAST(is_salesperson AS BOOLEAN) AS is_salesperson_boolean
  FROM dim_person__source
)

, dim_person__convert_boolean AS (
  SELECT
    person_key
    , full_name
    , IFNULL(preferred_name, 'Undefined') AS preferred_name
    , CASE
        WHEN is_permitted_to_logon_boolean IS TRUE THEN 'Permitted to Logon'
        WHEN is_permitted_to_logon_boolean IS FALSE THEN ' Not Permitted to Logon'
        WHEN is_permitted_to_logon_boolean IS NULL THEN 'Undefined'
        ELSE 'Invalid'
      END AS is_permitted_to_logon
    , CASE
        WHEN is_system_user_boolean IS TRUE THEN 'System User'
        WHEN is_system_user_boolean IS FALSE THEN ' Not System User'
        WHEN is_system_user_boolean IS NULL THEN 'Undefined'
        ELSE 'Invalid'
      END AS is_system_user
    , CASE
        WHEN is_employee_boolean IS TRUE THEN 'Employee'
        WHEN is_employee_boolean IS FALSE THEN ' Not Employee'
        WHEN is_employee_boolean IS NULL THEN 'Undefined'
        ELSE 'Invalid'
      END AS is_employee
    , CASE
        WHEN is_salesperson_boolean IS TRUE THEN 'Salesperson'
        WHEN is_salesperson_boolean IS FALSE THEN ' Not Salesperson'
        WHEN is_salesperson_boolean IS NULL THEN 'Undefined'
        ELSE 'Invalid'
      END AS is_salesperson
  FROM dim_person__rename_recast
)

, dim_person__add_undefined AS (
  SELECT
    person_key
    , full_name
    , preferred_name
    , is_permitted_to_logon
    , is_system_user
    , is_employee
    , is_salesperson
  FROM dim_person__convert_boolean
  
  UNION ALL
  SELECT
    0 AS person_key
    , 'Undefined' AS full_name
    , 'Undefined' AS preferred_name
    , 'Undefined' AS is_permitted_to_logon
    , 'Undefined' AS is_system_user
    , 'Undefined' AS is_employee
    , 'Undefined' AS is_salesperson

  UNION ALL
  SELECT
    -1 AS person_key
    , 'Invalid' AS full_name
    , 'Invalid' AS preferred_name
    , 'Invalid' AS is_permitted_to_logon
    , 'Invalid' AS is_system_user
    , 'Invalid' AS is_employee
    , 'Invalid' AS is_salesperson
)

SELECT
    person_key
  , full_name
  , preferred_name
  , is_permitted_to_logon
  , is_system_user
  , is_employee
  , is_salesperson
FROM dim_person__add_undefined