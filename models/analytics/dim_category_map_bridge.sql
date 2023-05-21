WITH dim_category_map_bridge__generate AS (
  SELECT
    *
  FROM {{ ref('dim_category') }}
  -- WHERE category_name IN ('Food', 'Biscuits', 'Candy', 'Butter Biscuits', 'Cheese Biscuits', 'Cookies Biscuits')
)

/* Created from category_key & parent_category_key in dim_category. This one does not require to add level columns in dim hierarchy */
  SELECT --depth distance = 0
    category_name AS parent_category
    , category_name AS child_category
  FROM dim_category_map_bridge__generate

  UNION ALL
  SELECT -- depth distance = 1
    parent_category_name AS parent_category
    , category_name AS child_category
  FROM dim_category_map_bridge__generate
  WHERE parent_category_key <> 0

  UNION ALL
  SELECT -- depth distance = 2
    parent.parent_category_name AS parent_category
    , child.category_name AS child_category
  FROM dim_category_map_bridge__generate AS child
  LEFT JOIN dim_category_map_bridge__generate AS parent
    ON child.parent_category_name = parent.category_name
  WHERE parent.parent_category_key <> 0

  UNION ALL
  SELECT --depth distance = 3
    parent.parent_category_name AS parrent_category
    , child.category_name AS child_category
  FROM dim_category_map_bridge__generate AS child
  LEFT JOIN dim_category_map_bridge__generate AS intermediate
    ON child.parent_category_name = intermediate.category_name
  LEFT JOIN dim_category_map_bridge__generate AS parent
    ON intermediate.parent_category_name = parent.category_name
  WHERE parent.parent_category_key <> 0

/* Using created category_level columns in dim_category */
-- SELECT
--   category_level_1_key AS parent_key
--   , category_level_1_name AS parent_name
--   , category_key AS child_key
--   , category_name AS child_name
-- FROM dim_category_map_bridge__generate
-- WHERE category_level_1_key <> 0

-- UNION ALL
-- SELECT
--   category_level_2_key AS parent_key
--   , category_level_2_name AS parent_name
--   , category_key AS child_key
--   , category_name AS child_name
-- FROM dim_category_map_bridge__generate
-- WHERE category_level_2_key <> 0

-- UNION ALL
-- SELECT
--   category_level_3_key AS parent_key
--   , category_level_3_name AS parent_name
--   , category_key AS child_key
--   , category_name AS child_name
-- FROM dim_category_map_bridge__generate
-- WHERE category_level_3_key <> 0

-- UNION ALL
-- SELECT
--   category_level_4_key AS parent_key
--   , category_level_4_name AS parent_name
--   , category_key AS child_key
--   , category_name AS child_name
-- FROM dim_category_map_bridge__generate
-- WHERE category_level_4_key <> 0
-- ORDER BY parent_key
