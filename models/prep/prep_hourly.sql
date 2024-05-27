WITH hourly_data AS (
    SELECT * 
    FROM {{ref('staging_forecast_hour')}}
),
add_features AS (
    SELECT *
		, timestamp::DATE AS date -- only time (hours:minutes:seconds) as TIME data type
		, timestamp::TIME AS time -- only time (hours:minutes:seconds) as TIME data type
        , TO_CHAR(timestamp,'HH24:MI') as hour -- time (hours:minutes) as TEXT data type
        , TO_CHAR(timestamp, 'FMmonth') AS monthname -- month name as a text
        , TO_CHAR(timestamp, 'FMday') AS weekday -- weekday name as text        
        , DATE_PART('day', timestamp) AS date_day
		, DATE_PART('month', timestamp) AS date_month
		, DATE_PART('year', timestamp) AS date_year
		, DATE_PART('week', timestamp) AS cw
),
add_more_features AS (
    SELECT *
		, CASE 
			WHEN time BETWEEN '00:00:00' AND '05:59:00' THEN 'night'
			WHEN time BETWEEN '06:00:00' AND '18:00:00' THEN 'day'
			WHEN time BETWEEN '18:00:00' AND '23:59:00' THEN 'evening'
		END
)
SELECT *
FROM add_more_features