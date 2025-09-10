SELECT
date_trunc('week', last_trip_at) AS Week,
ROUND(AVG(avg_distance_km), 3) AS "Average Distance Km",
ROUND(AVG(avg_network_duration_min), 2) AS "Average Netwrok Duration"
FROM smartlogistics.routes_summary
GROUP BY 1
ORDER BY 1;
