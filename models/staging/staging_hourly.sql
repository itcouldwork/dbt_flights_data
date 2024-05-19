WITH hourly_raw AS (
    SELECT
            airport_code,
            station_id,
            JSON_ARRAY_ELEMENTS(extracted_data -> 'data') AS json_data
    FROM {{source("staging", "weather_hourly_raw")}}
),
daily_data AS (
    SELECT  
            airport_code
            ,station_id
            ,(json_data->>'time')::TIMESTAMP AS timestamp	
            ,(json_data->>'temp')::FLOAT AS temp
            ,(json_data->>'dwpt')::FLOAT AS dwpt
            ,(json_data->>'rhum')::INTEGER AS rhum
            ,(json_data->>'prcp')::FLOAT AS prcp
            ,(json_data->>'snow')::INTEGER AS snow
            ,(json_data->>'wdir') AS wdir
            ,(json_data->>'wspd')::FLOAT AS wspd
            ,(json_data->>'wpgt')::FLOAT AS wpgt
            ,(json_data->>'pres')::FLOAT AS pres
            ,(json_data->>'tsun')::INTEGER AS tsun
            ,(json_data->>'coco')::INTEGER AS coco
    FROM hourly_raw
)
SELECT * 
FROM hourly_data