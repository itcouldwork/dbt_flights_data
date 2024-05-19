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
            (json_data->>'date')::DATE AS date,
            (json_data->>'tavg')::FLOAT AS avg_temp_c,
            (json_data->>'tmin')::FLOAT AS min_temp_c,
            (json_data->>'tmax')::FLOAT AS max_temp_c,
            (json_data->>'prcp')::FLOAT AS precipitation_mm,
            ((json_data->>'snow')::FLOAT)::INTEGER AS max_snow_mm,
            ((json_data->>'wdir')::FLOAT)::INTEGER AS avg_wind_direction,
            (json_data->>'wspd')::FLOAT AS avg_wind_speed_kmh,
            (json_data->>'wpgt')::FLOAT AS wind_peakgust_kmh,
            (json_data->>'pres')::FLOAT AS avg_pressure_hpa,
            (json_data->>'tsun')::INTEGER AS sunshine_min
    FROM daily_raw
)
SELECT * 
FROM daily_data