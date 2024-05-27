WITH airports_regions_join AS (
    SELECT * 
    FROM {{source("staging_flights", "airports")}}
    LEFT JOIN {{source("staging_flights", "regions")}}
    USING (country)
),
airports_reorder AS (
    SELECT faa
            ,name
            ,city
            ,country
            ,region
            ,lat
            ,lon
            ,alt
            ,tz
            ,dst
    FROM airports_regions_join
)
SELECT * FROM airports_reorder