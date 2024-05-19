WITH daily_raw AS (
    SELECT
            airport_code,
            station_id,
            JSON_ARRAY_ELEMENTS(extracted_data -> 'data') AS json_data
    FROM {{source("staging", "weather_daily_raw")}}
),
daily_data AS (
    SELECT  airport_code,
            station_id,
            json_data->>'date'::DATE AS date,
            (json_data->>'tavg')::FLOAT AS tavg,
            (json_data->>'tmin')::FLOAT AS tmin,
            (json_data->>'tmax')::FLOAT AS tmax,
            (json_data->>'prcp')::FLOAT AS prcp,
            (json_data->>'snow')::FLOAT AS snow,
            (json_data->>'wdir')::FLOAT AS wdir,
            (json_data->>'wspd')::FLOAT AS wspd,
            (json_data->>'wpgt')::FLOAT AS wpgt,
            (json_data->>'pres')::FLOAT AS pres,
            (json_data->>'tsun')::FLOAT AS tsun
    FROM daily_raw
)
SELECT * 
FROM daily_data