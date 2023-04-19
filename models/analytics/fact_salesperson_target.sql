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
    COALESCE(fact_target.year_month, fact_sales.order_year_month) AS year_month
    , COALESCE(fact_target.salesperson_person_key, fact_sales.salesperson_person_key) AS salesperson_person_key
    , fact_target.target_gross_amount
    , fact_sales.gross_amount
  FROM fact_salesperson_target__handle_null AS fact_target
  FULL OUTER JOIN fact_salesperson_target__aggregate_gross AS fact_sales
    ON fact_target.salesperson_person_key = fact_sales.salesperson_person_key
    AND fact_target.year_month = fact_sales.order_year_month
)

, fact_salesperson_target__calculate_achievement AS (
  SELECT
    year_month
    , salesperson_person_key
    , target_gross_amount
    , gross_amount
    , gross_amount / target_gross_amount AS achievement_percent
  FROM fact_salesperson_target__sales_join
)

  SELECT
    year_month
    , salesperson_person_key
    , target_gross_amount
    , gross_amount
    ,  CONCAT( ROUND( achievement_percent * 100, 2 ), '%' ) AS achievement_percent
    , CASE
        WHEN achievement_percent >= 0.8 THEN 'Achieved'
        WHEN achievement_percent < 0.8 THEN 'Not Achieved'
        ELSE 'Invalid' END
    AS is_achieved
  FROM fact_salesperson_target__calculate_achievement