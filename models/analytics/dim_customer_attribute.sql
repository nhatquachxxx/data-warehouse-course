WITH dim_customer_attribute__aggregate_metric AS (
  SELECT
    customer_key
    , MIN( DATE_TRUNC(order_date, MONTH) ) AS start_month
    , MAX( DATE_TRUNC(order_date, MONTH) ) AS end_month
    , SUM(gross_amount) AS lifetime_monetary_amount
    , COUNT(DISTINCT sales_order_key) AS lifetime_sales_order_quantity
    , SUM(quantity) AS lifetime_item_quantity
    , SUM( 
        CASE
          WHEN FORMAT_DATE('%Y%m', order_date) = '201604' THEN gross_amount
          ELSE 0 END)
    AS lastmonth_monetary_amount
    , COUNT( DISTINCT
        CASE
          WHEN FORMAT_DATE('%Y%m', order_date) = '201604' THEN sales_order_key
          ELSE NULL END)
    AS lastmonth_sales_order_quantity
    , SUM (
        CASE
          WHEN FORMAT_DATE('%Y%m', order_date) = '201604' THEN quantity
          ELSE 0 END)
    AS lastmonth_item_quantity
  FROM {{ ref('fact_sales_order_line') }}
  GROUP BY customer_key
)

, dim_customer_attribute__calculate_percentile AS (
  SELECT
    customer_key
    , start_month
    , end_month
    , lifetime_monetary_amount
    , lifetime_sales_order_quantity
    , lastmonth_monetary_amount
    , lastmonth_sales_order_quantity
    , lastmonth_item_quantity
    , PERCENT_RANK() OVER(ORDER BY lifetime_monetary_amount ASC) AS lifetime_monetary_percentile
    , PERCENT_RANK() OVER(ORDER BY lastmonth_monetary_amount ASC) AS lastmonth_monetary_percentile
  FROM dim_customer_attribute__aggregate_metric
)

, dim_customer_attribute__segmentation AS (
  SELECT
    *
    , CASE
        WHEN lifetime_monetary_percentile < 1 AND lifetime_monetary_percentile > 0.8 THEN 'High'
        WHEN lifetime_monetary_percentile <= 0.5 THEN 'Low'
        ELSE 'Medium' END
    AS lifetime_monetary_segment
    , CASE
        WHEN lastmonth_monetary_percentile < 1 AND lastmonth_monetary_percentile > 0.8 THEN 'High'
        WHEN lastmonth_monetary_percentile <= 0.5 THEN 'Low'
        ELSE 'Medium' END
    AS lastmonth_monetary_segment
  FROM dim_customer_attribute__calculate_percentile
)

SELECT
  *
FROM dim_customer_attribute__segmentation