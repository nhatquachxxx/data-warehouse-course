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
  dim_date.date
  , dim_date.day_of_week
  , dim_date.day_of_week_short
  , CASE
      WHEN dim_date.day_of_week_short IN ('Sat', 'Sun') THEN 'Weekend'
      WHEN dim_date.day_of_week_short IN ('Mon', 'Tue', 'Wed', 'Thu', 'Fri') THEN 'Weekday'
      ELSE 'Invalid' END
    AS is_weekday_or_weekend
  , dim_date.week_number
  , dim_date.year_week
  , dim_date.month_name
  , dim_date.month
  , dim_date.year_month
  , dim_date.quarter
  , dim_date.year_quarter
  , dim_date.year
  , CASE
      WHEN dim_holiday.holiday_date IS NOT NULL THEN 'Holiday'
      WHEN dim_holiday.holiday_date IS NULL THEN 'Not Holiday'
      ELSE 'Invalid' END
    AS is_holiday
  , IFNULL(dim_holiday.holiday_name, 'Undefined') AS holiday_name
FROM dim_date__extract_components AS dim_date
LEFT JOIN {{ ref('stg_dim_holiday') }} AS dim_holiday
  ON dim_date.date = dim_holiday.holiday_date