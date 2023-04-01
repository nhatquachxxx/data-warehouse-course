SELECT
  person_key AS picked_by_person_key
  , full_name AS picked_by_full_name
  , preferred_name AS picked_by_preferred_name
  , is_employee
  , is_salesperson
FROM {{ ref('dim_person') }}
WHERE is_employee = 'Employee'
  OR person_key IN (0, -1)