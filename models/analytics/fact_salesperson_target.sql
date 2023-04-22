WITH fact_salesperson_target__source AS (
  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.external__salesperson_target`
)

, fact_salesperson_target__rename_recast AS (
  SELECT
    CAST(year_month AS DATE) AS year_month
    , CAST(salesperson_person_id AS INT) AS salesperson_person_key
    , CAST(target_revenue AS NUMERIC) AS target_gross_amount
  FROM fact_salesperson_target__source
)

, fact_salesperson_target__handle_null AS (
  SELECT
    year_month
    , IFNULL(salesperson_person_key, 0) AS salesperson_person_key
    , target_gross_amount
  FROM fact_salesperson_target__rename_recast
)

, fact_salesperson_target__aggregate_gross AS (
  SELECT
    salesperson_person_key
    , DATE_TRUNC(order_date, MONTH) AS order_year_month
    , SUM(gross_amount) AS gross_amount
  FROM {{ ref('fact_sales_order_line') }}
  GROUP BY salesperson_person_key, order_year_month
)

, fact_salesperson_target__sales_join AS (
  SELECT
    year_month -- Vì dùng USING nên BQ sẽ tự COALESCE ở những Join Keys
    , salesperson_person_key
    , COALESCE(fact_target.target_gross_amount, 0) AS target_gross_amount
    , COALESCE(fact_sales.gross_amount, 0) AS gross_amount
  FROM fact_salesperson_target__handle_null AS fact_target
  FULL OUTER JOIN fact_salesperson_target__aggregate_gross AS fact_sales
    USING(year_month, salesperson_person_key)
)

, fact_salesperson_target__calculate_achievement AS (
  SELECT
    year_month
    , salesperson_person_key
    , target_gross_amount
    , gross_amount
    , gross_amount / NULLIF(target_gross_amount, 0) AS achievement_percent -- Rào NULLIF để tránh trường hợp chia cho 0
  FROM fact_salesperson_target__sales_join
)

  SELECT
    year_month
    , salesperson_person_key
    , target_gross_amount
    , gross_amount
    , achievement_percent
    , CONCAT( ROUND( achievement_percent * 100, 2 ), '%' ) AS achievement_percent_formatted
    , CASE
        WHEN achievement_percent >= 0.8 THEN 'Achieved'
        WHEN achievement_percent < 0.8 THEN 'Not Achieved'
        ELSE 'Invalid' END
    AS is_achieved
  FROM fact_salesperson_target__calculate_achievement