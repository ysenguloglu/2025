CREATE OR REPLACE TABLE f`your-project-id.your-dataset.table-id` AS
WITH date_range AS (
    SELECT
        DATE '2010-01-01' + INTERVAL day_num DAY AS full_date
    FROM
        UNNEST(GENERATE_ARRAY(0, 365 * 30)) AS day_num
)
SELECT
    FORMAT_DATE('%Y-%m-%d', full_date) AS date_id,
    DATE(FORMAT_DATE('%Y-%m-%d', full_date)) AS full_date,
    EXTRACT(DAY FROM full_date) AS day,
    EXTRACT(DAYOFWEEK FROM full_date) AS day_of_week,
    FORMAT_DATE('%A', full_date) AS day_name,
    CASE
        WHEN EXTRACT(DAYOFWEEK FROM full_date) IN (1, 7) THEN 1
        ELSE 0
    END AS is_weekend,
    EXTRACT(WEEK FROM full_date) AS week_of_year,
    EXTRACT(MONTH FROM full_date) AS month,
    FORMAT_DATE('%B', full_date) AS month_name,
    EXTRACT(QUARTER FROM full_date) AS quarter,
    EXTRACT(YEAR FROM full_date) AS year,
    FORMAT_DATE('%G', full_date) AS iso_year
FROM
    date_range;