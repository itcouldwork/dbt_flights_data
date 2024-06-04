WITH flights_stats AS (
	SELECT TO_CHAR(flight_date, 'YYYY-MM') AS flight_month
		   ,origin
		   ,dest
		   ,COUNT(flight_number) AS n_flights
		   ,COUNT(DISTINCT tail_number) AS nunique_tails
		   ,COUNT(DISTINCT airline) AS nunique_airlines
		   ,AVG(actual_elapsed_time_interval) AS avg_actual_elapsed_time 
		   ,STDDEV(EXTRACT(EPOCH FROM actual_elapsed_time_interval))::INTEGER * ('1 second'::INTERVAL) AS sd_actual_elapsed_time 
		   ,AVG(arr_delay_interval) AS avg_arr_delay
		   ,MIN(arr_delay_interval) AS min_arr_delay
		   ,MAX(arr_delay_interval) AS max_arr_delay
		   ,SUM(cancelled) AS total_cancelled
		   ,SUM(diverted) AS total_diverted
	FROM {{ref('prep_flights')}}
	GROUP BY (flight_month, origin, dest)
),
add_names AS (
	SELECT o.city AS origin_city
			,d.city AS dest_city
			,o.name AS origin_name
			,d.name AS dest_name
			,f.*
	FROM flights_stats f
	LEFT JOIN {{ref('prep_airports')}} o
		ON origin=o.faa
	LEFT JOIN {{ref('prep_airports')}} d
		ON dest=d.faa
)
SELECT *
FROM add_names
ORDER BY (origin, dest) DESC;