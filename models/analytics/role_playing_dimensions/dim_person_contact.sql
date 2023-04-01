SELECT
    person_key AS contact_person_key
  , full_name AS contact_full_name
  , preferred_name AS contact_preferred_name
  , is_system_user
  , is_employee
  , is_salesperson
FROM {{ ref('dim_person') }}