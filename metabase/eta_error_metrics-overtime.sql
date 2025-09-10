SELECT
    toStartOfWeek(last_trip_at) AS week,
    avg(avg_eta_error_min) AS avg_eta_error_min,
    avg(avg_abs_eta_error_min) AS avg_abs_eta_error_min
FROM smartlogistics.routes_summary
GROUP BY week
ORDER BY week;
