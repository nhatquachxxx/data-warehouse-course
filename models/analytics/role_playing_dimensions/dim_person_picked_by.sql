SELECT
  person_key AS picked_by_person_key
  , full_name AS picked_by_full_name
  , preferred_name AS picked_by_preferred_name
FROM {{ ref('dim_person') }}