WITH dim_date__generate AS (
  SELECT
    *
  FROM UNNEST(GENERATE_DATE_ARRAY('2010-01-01','2030-12-31',interval 1 day)) AS date
)

, dim_date__extract_components AS (
  SELECT
    date
    , FORMAT_DATE('%A',date) AS day_of_week
    , FORMAT_DATE('%a',date) AS day_of_week_short
    , EXTRACT(WEEK(MONDAY) FROM date) AS week_number
    , FORMAT_DATE('%Y/W%W',date) AS year_week
    , FORMAT_DATE('%Y/M%m',date) AS year_month
    , EXTRACT(MONTH FROM date) AS month
    , FORMAT_DATE('%B',date) AS month_name
    , FORMAT_DATE('%Y/Q%Q',date) AS year_quarter
    , EXTRACT(QUARTER FROM date) AS quarter
    , EXTRACT(YEAR FROM date) AS year
  FROM dim_date__generate
)

SELECT
  date
  , day_of_week
  , day_of_week_short
  , CASE 
      WHEN day_of_week_short IN ('Sat', 'Sun') THEN 'Weekend'
      WHEN day_of_week_short IN ('Mon', 'Tue', 'Wed', 'Thu', 'Fri') THEN 'Weekday'
      ELSE 'Invalid' END
    AS is_weekday_or_weekend
  , week_number
  , year_week
  , month_name
  , month
  , year_month
  , quarter
  , year_quarter
  , year
FROM dim_date__extract_components