WITH daily_data AS (
    SELECT * 
    FROM {{ref('staging_daily')}}
),
add_features AS (
    SELECT *
		, DATE_PART('day', date) AS date_day
		, DATE_PART('month', date) AS date_month
		, DATE_PART('year', date) AS date_year
		, DATE_PART('week', date) AS cw
		, TO_CHAR(date, 'FMmonth') AS monthname
		, TO_CHAR(date, 'FMday') AS weekday
    FROM staging_daily
    ORDER BY date
),
add_more_features AS (
    SELECT *
		, (CASE 
			WHEN monthname in ('december','january','february') THEN 'winter'
			WHEN monthname in ('march','april','may') THEN 'spring'
            WHEN monthname in ('june','july','august') THEN 'summer'
            WHEN monthname in ('september','october','november') THEN 'autumn'
		END) AS season
		FROM add_features
)
SELECT *
FROM add_more_features