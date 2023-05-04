-- Periodic Snapshot by Customer & Order Month
WITH fact_customer_snapshot_by_month__aggregate_sales AS (
  SELECT
    customer_key
    , DATE_TRUNC(order_date, MONTH) AS year_month
    , SUM(gross_amount) AS sales_amount
    , COUNT( DISTINCT sales_order_key ) AS sales_order_quantity
    , SUM(quantity) AS item_quantity
  FROM {{ ref('fact_sales_order_line') }}
  GROUP BY customer_key, year_month
)

, fact_customer_snapshot_by_month__generate_densed AS (
  SELECT
    customer_key
    , year_month
  FROM {{ ref('dim_customer_attribute') }} AS dim_customer
  CROSS JOIN (
    SELECT DISTINCT DATE_TRUNC(date, MONTH) AS year_month FROM {{ ref('dim_date') }} ) 
  AS dim_date
  WHERE year_month BETWEEN start_month AND end_month
)

, fact_customer_snapshot_by_month__dense_snapshot AS (
  SELECT
    customer_key
    , year_month
    , sales.sales_amount
    , sales.sales_order_quantity
    , sales.item_quantity
  FROM fact_customer_snapshot_by_month__generate_densed AS densed
  LEFT JOIN fact_customer_snapshot_by_month__aggregate_sales AS sales
  USING(customer_key, year_month)
)

, fact_customer_snapshot_by_month__calculate_measures AS (
  SELECT
    *
    , SUM(sales_amount) OVER(PARTITION BY customer_key ORDER BY year_month) AS lifetime_sales_amount
    , SUM(sales_order_quantity) OVER(PARTITION BY customer_key ORDER BY year_month) AS lifetime_sales_order_quantity
    , SUM(item_quantity) OVER(PARTITION BY customer_key ORDER BY year_month) AS lifetime_item_quantity
    , LAG(sales_amount, 1) OVER(PARTITION BY customer_key ORDER BY year_month) AS lm_sales_amount
    , SUM(sales_amount) OVER(PARTITION BY customer_key ORDER BY year_month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS l12m_sales_amount
  FROM fact_customer_snapshot_by_month__dense_snapshot
)

, fact_customer_snapshot_by_month__calculate_percentile AS (
  SELECT
    *
    , PERCENT_RANK() OVER(PARTITION BY year_month ORDER BY sales_amount) AS monetary_percentile
    , PERCENT_RANK() OVER(PARTITION BY year_month ORDER BY lifetime_sales_amount) AS lifetime_monetary_percentile
    , PERCENT_RANK() OVER(PARTITION BY year_month ORDER BY lm_sales_amount) AS lm_monetary_percentile
    , PERCENT_RANK() OVER(PARTITION BY year_month ORDER BY l12m_sales_amount) AS l12m_monetary_percentile
  FROM fact_customer_snapshot_by_month__calculate_measures
)

, fact_customer_snapshot_by_month__segmentation AS (
  SELECT
    *
    , CASE
        WHEN monetary_percentile BETWEEN 0.8 AND 1 THEN 'High'
        WHEN monetary_percentile BETWEEN 0.5 AND 0.8 THEN 'Medium'
        ELSE 'Low' END
    AS monetary_segment
    , CASE
        WHEN lifetime_monetary_percentile BETWEEN 0.8 AND 1 THEN 'High'
        WHEN lifetime_monetary_percentile BETWEEN 0.5 AND 0.8 THEN 'Medium'
        ELSE 'Low' END
    AS lifetime_monetary_segment
    , CASE
        WHEN lm_monetary_percentile BETWEEN 0.8 AND 1 THEN 'High'
        WHEN lm_monetary_percentile BETWEEN 0.5 AND 0.8 THEN 'Medium'
        ELSE 'Low' END
    AS lm_monetary_segment
    , CASE
        WHEN l12m_monetary_percentile BETWEEN 0.8 AND 1 THEN 'High'
        WHEN l12m_monetary_percentile BETWEEN 0.5 AND 0.8 THEN 'Medium'
        ELSE 'Low' END
    AS l12m_monetary_segment
  FROM fact_customer_snapshot_by_month__calculate_percentile
)

SELECT
  customer_key
  , year_month
  , sales_amount
  , sales_order_quantity
  , item_quantity
  , monetary_percentile
  , monetary_segment
  , lifetime_sales_amount
  , lifetime_sales_order_quantity
  , lifetime_item_quantity
  , lifetime_monetary_percentile
  , lifetime_monetary_segment
  , lm_sales_amount
  , lm_monetary_percentile
  , lm_monetary_segment
  , l12m_sales_amount
  , l12m_monetary_percentile
  , l12m_monetary_segment
FROM fact_customer_snapshot_by_month__segmentation