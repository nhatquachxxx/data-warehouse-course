WITH dim_date AS (
  SELECT
    *
  FROM {{ ref('dim_date') }}
)

--Retrieve Current Date
SELECT
  date AS date_for_reporting
  , 'Current Year' AS comparing_type
  , date AS date_for_joining
FROM dim_date

--Retrieve Last Year Date
UNION ALL
SELECT
  date AS date_for_reporting
  , 'Last Year' AS comparing_type
  , DATE_SUB(date, INTERVAL 1 year) AS date_for_joining
FROM dim_date
ORDER BY date_for_reporting