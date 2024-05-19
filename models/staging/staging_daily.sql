WITH daily_raw AS (
    SELECT
            airport_code,
            station_id,
            JSON_ARRAY_ELEMENTS(extracted_data -> 'data') AS json_data
    FROM {{source("staging", "dbt.test.weather_daily_raw")}}
),
daily_data AS (
    SELECT  airport_code,
            station_id,
            json_data->>'date' AS date,
            (json_data->>'tavg')::float AS tavg,
            (json_data->>'tmin')::float AS tmin,
            (json_data->>'tmax')::float AS tmax,
            (json_data->>'prcp')::float AS prcp,
            (json_data->>'snow')::float AS snow,
            (json_data->>'wdir')::float AS wdir,
            (json_data->>'wspd')::float AS wspd,
            (json_data->>'wpgt')::float AS wpgt,
            (json_data->>'pres')::float AS pres,
            (json_data->>'tsun')::float AS tsun
    FROM daily_raw
)
SELECT * 
FROM daily_data